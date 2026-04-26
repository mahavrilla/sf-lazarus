// Package watermark persists per-object sync watermarks in DynamoDB.
// The watermark is set to job_start minus WatermarkBuffer, not to the max
// record timestamp, so that records in flight during the previous Bulk API
// cursor are re-queried on the next sync cycle.
package watermark

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	// WatermarkBuffer is subtracted from job start time when computing the
	// watermark. The resulting overlap window ensures records that were in
	// flight during the previous job's Bulk API cursor are re-queried.
	WatermarkBuffer = 5 * time.Minute

	attrObject       = "object"
	attrWatermark    = "watermark"
	attrLastJobStart = "last_job_start"
	attrLastJobID    = "last_job_id"
)

// Record is the DynamoDB item shape for one managed Salesforce object.
type Record struct {
	Object       string
	Watermark    time.Time
	LastJobStart time.Time
	LastJobID    string
}

// Store reads and writes watermarks backed by a DynamoDB table.
//
// Table schema (create once before running):
//
//	aws dynamodb create-table \
//	  --table-name lazarus-watermarks-v2 \
//	  --attribute-definitions AttributeName=object,AttributeType=S \
//	  --key-schema AttributeName=object,KeyType=HASH \
//	  --billing-mode PAY_PER_REQUEST
type Store struct {
	client    *dynamodb.Client
	tableName string
}

// NewStore constructs a Store for the given DynamoDB table.
func NewStore(cfg aws.Config, tableName string) *Store {
	return &Store{
		client:    dynamodb.NewFromConfig(cfg),
		tableName: tableName,
	}
}

// Get returns the current watermark for objectName.
// Returns (zero time, false, nil) when the object has never been synced —
// the caller should treat that as a full-load with no lower bound.
func (s *Store) Get(ctx context.Context, objectName string) (time.Time, bool, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]ddbTypes.AttributeValue{
			attrObject: &ddbTypes.AttributeValueMemberS{Value: objectName},
		},
	})
	if err != nil {
		return time.Time{}, false, fmt.Errorf("get watermark %s: %w", objectName, err)
	}
	if out.Item == nil {
		return time.Time{}, false, nil
	}

	v, ok := out.Item[attrWatermark].(*ddbTypes.AttributeValueMemberS)
	if !ok || v.Value == "" {
		return time.Time{}, false, nil
	}

	t, err := time.Parse(time.RFC3339, v.Value)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parse watermark for %s: %w", objectName, err)
	}
	return t, true, nil
}

// Advance records jobStart-WatermarkBuffer as the new watermark for object.
// It is called only after all batches for a job have been successfully written.
func (s *Store) Advance(ctx context.Context, object string, jobStart time.Time, jobID string) error {
	return s.put(ctx, object, Record{
		Object:       object,
		Watermark:    jobStart.Add(-WatermarkBuffer),
		LastJobStart: jobStart,
		LastJobID:    jobID,
	})
}

func (s *Store) put(ctx context.Context, object string, r Record) error {
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item: map[string]ddbTypes.AttributeValue{
			attrObject:       &ddbTypes.AttributeValueMemberS{Value: r.Object},
			attrWatermark:    &ddbTypes.AttributeValueMemberS{Value: r.Watermark.UTC().Format(time.RFC3339)},
			attrLastJobStart: &ddbTypes.AttributeValueMemberS{Value: r.LastJobStart.UTC().Format(time.RFC3339)},
			attrLastJobID:    &ddbTypes.AttributeValueMemberS{Value: r.LastJobID},
		},
	})
	if err != nil {
		return fmt.Errorf("set watermark %s: %w", object, err)
	}
	return nil
}
