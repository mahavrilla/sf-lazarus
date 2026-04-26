package syncstate

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	pkAttr        = "pk"
	watermarkAttr = "watermark"
)

// DynamoStore implements Store using a DynamoDB table.
//
// Table schema (create once before running):
//
//	aws dynamodb create-table \
//	  --table-name lazarus-watermarks \
//	  --attribute-definitions AttributeName=pk,AttributeType=S \
//	  --key-schema AttributeName=pk,KeyType=HASH \
//	  --billing-mode PAY_PER_REQUEST
type DynamoStore struct {
	client    *dynamodb.Client
	tableName string
}

// NewDynamoStore constructs a DynamoStore for the given table.
func NewDynamoStore(cfg aws.Config, tableName string) *DynamoStore {
	return &DynamoStore{
		client:    dynamodb.NewFromConfig(cfg),
		tableName: tableName,
	}
}

// GetWatermark returns the stored watermark for objectName, or "" for a
// first-time run.
func (s *DynamoStore) GetWatermark(ctx context.Context, objectName string) (string, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]ddbTypes.AttributeValue{
			pkAttr: &ddbTypes.AttributeValueMemberS{Value: objectName},
		},
	})
	if err != nil {
		return "", fmt.Errorf("get watermark %s: %w", objectName, err)
	}
	if out.Item == nil {
		return "", nil
	}
	v, ok := out.Item[watermarkAttr].(*ddbTypes.AttributeValueMemberS)
	if !ok {
		return "", nil
	}
	return v.Value, nil
}

// SetWatermark durably records watermark for objectName.
func (s *DynamoStore) SetWatermark(ctx context.Context, objectName, watermark string) error {
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item: map[string]ddbTypes.AttributeValue{
			pkAttr:        &ddbTypes.AttributeValueMemberS{Value: objectName},
			watermarkAttr: &ddbTypes.AttributeValueMemberS{Value: watermark},
		},
	})
	if err != nil {
		return fmt.Errorf("set watermark %s: %w", objectName, err)
	}
	return nil
}
