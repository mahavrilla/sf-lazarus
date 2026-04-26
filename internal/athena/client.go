// Package athena provides a client for submitting and polling Athena queries.
package athena

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenaTypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
)

const (
	pollInitial    = 2 * time.Second
	pollMax        = 30 * time.Second
	pollMultiplier = 1.5
)

// Config holds connection parameters for the Athena client.
type Config struct {
	// Database is the Glue/Athena database to query against.
	Database string
	// Workgroup is the Athena workgroup. Defaults to "primary" when empty.
	Workgroup string
	// OutputLocation is the S3 URI where Athena writes query results,
	// e.g. "s3://my-bucket/athena-results/".
	OutputLocation string
	// AWSConfig is the resolved AWS SDK configuration.
	AWSConfig aws.Config
}

// Client submits queries to Athena, polls for completion, and returns results.
type Client struct {
	client *athena.Client
	cfg    Config
}

// NewClient constructs a Client from cfg.
func NewClient(cfg Config) *Client {
	return &Client{
		client: athena.NewFromConfig(cfg.AWSConfig),
		cfg:    cfg,
	}
}

// Execute submits query, polls until it completes, and returns all result rows
// as a slice of column-name → value maps. Column names are taken from the
// result set metadata returned by Athena. DDL and DML statements that produce
// no rows return an empty (non-nil) slice.
func (c *Client) Execute(ctx context.Context, query string) ([]map[string]string, error) {
	execID, err := c.start(ctx, query)
	if err != nil {
		return nil, err
	}
	if err := c.poll(ctx, execID); err != nil {
		return nil, err
	}
	return c.results(ctx, execID)
}

// start submits the query and returns the execution ID.
func (c *Client) start(ctx context.Context, query string) (string, error) {
	wg := c.cfg.Workgroup
	if wg == "" {
		wg = "primary"
	}

	out, err := c.client.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		QueryExecutionContext: &athenaTypes.QueryExecutionContext{
			Database: aws.String(c.cfg.Database),
		},
		WorkGroup: aws.String(wg),
		ResultConfiguration: &athenaTypes.ResultConfiguration{
			OutputLocation: aws.String(c.cfg.OutputLocation),
		},
	})
	if err != nil {
		return "", fmt.Errorf("start query execution: %w", err)
	}
	return aws.ToString(out.QueryExecutionId), nil
}

// poll blocks until the query reaches a terminal state.
func (c *Client) poll(ctx context.Context, execID string) error {
	interval := pollInitial

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}

		out, err := c.client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(execID),
		})
		if err != nil {
			return fmt.Errorf("get query execution %s: %w", execID, err)
		}

		switch out.QueryExecution.Status.State {
		case athenaTypes.QueryExecutionStateSucceeded:
			return nil
		case athenaTypes.QueryExecutionStateFailed:
			reason := aws.ToString(out.QueryExecution.Status.StateChangeReason)
			return fmt.Errorf("query %s failed: %s", execID, reason)
		case athenaTypes.QueryExecutionStateCancelled:
			return fmt.Errorf("query %s cancelled", execID)
		}

		next := time.Duration(float64(interval) * pollMultiplier)
		interval = time.Duration(math.Min(float64(next), float64(pollMax)))
	}
}

// results fetches all result pages and returns rows as column-name → value maps.
// The first row of the first page is the column header and is not included in
// the output. Subsequent pages carry data rows only.
func (c *Client) results(ctx context.Context, execID string) ([]map[string]string, error) {
	var out []map[string]string
	var columns []string
	var nextToken *string

	for {
		page, err := c.client.GetQueryResults(ctx, &athena.GetQueryResultsInput{
			QueryExecutionId: aws.String(execID),
			NextToken:        nextToken,
		})
		if err != nil {
			return nil, fmt.Errorf("get query results %s: %w", execID, err)
		}

		dataRows := page.ResultSet.Rows
		if columns == nil {
			if len(dataRows) == 0 {
				break
			}
			for _, d := range dataRows[0].Data {
				columns = append(columns, aws.ToString(d.VarCharValue))
			}
			dataRows = dataRows[1:]
		}

		out = append(out, parseRows(columns, dataRows)...)

		if page.NextToken == nil {
			break
		}
		nextToken = page.NextToken
	}

	if out == nil {
		out = []map[string]string{}
	}
	return out, nil
}

// parseRows converts a slice of Athena result rows into column-name → value
// maps using the provided column order. Extracted as a pure function for
// testability.
func parseRows(columns []string, rows []athenaTypes.Row) []map[string]string {
	out := make([]map[string]string, 0, len(rows))
	for _, row := range rows {
		m := make(map[string]string, len(columns))
		for j, datum := range row.Data {
			if j < len(columns) {
				m[columns[j]] = aws.ToString(datum.VarCharValue)
			}
		}
		out = append(out, m)
	}
	return out
}
