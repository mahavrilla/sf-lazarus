package salesforce

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
)

// JobStatus is the current state of a Bulk API 2.0 ingest job.
type JobStatus struct {
	// Status is the Salesforce job state, e.g. "JobComplete", "Failed".
	Status string
	// RecordsProcessed is the number of records Salesforce successfully processed.
	RecordsProcessed int
	// Errors collects job-level error information. Per-record failures are
	// reported via RecordsProcessed vs the original record count.
	Errors []string
}

// ingestJob is the subset of the Bulk API 2.0 ingest job response we need.
type ingestJob struct {
	ID                     string `json:"id"`
	State                  string `json:"state"`
	NumberRecordsProcessed int    `json:"numberRecordsProcessed"`
	NumberRecordsFailed    int    `json:"numberRecordsFailed"`
	ErrorMessage           string `json:"errorMessage"`
}

// BulkUpsert creates a Bulk API 2.0 ingest job with operation=upsert and
// externalIdFieldName=Id, uploads records as CSV, and closes the job for
// processing. It returns the job ID for status polling via GetJobStatus.
func (c *Client) BulkUpsert(ctx context.Context, object string, records []Record, log *slog.Logger) (string, error) {
	if len(records) == 0 {
		return "", fmt.Errorf("bulk upsert %s: no records provided", object)
	}

	job, err := c.createIngestJob(ctx, object)
	if err != nil {
		return "", fmt.Errorf("create ingest job for %s: %w", object, err)
	}
	log.Info("ingest job created",
		slog.String("object", object),
		slog.String("job_id", job.ID),
		slog.Int("records", len(records)),
	)

	if err := c.uploadCSV(ctx, job.ID, records); err != nil {
		return "", fmt.Errorf("upload CSV for %s (job %s): %w", object, job.ID, err)
	}

	if err := c.closeIngestJob(ctx, job.ID); err != nil {
		return "", fmt.Errorf("close ingest job %s: %w", job.ID, err)
	}
	log.Info("ingest job submitted",
		slog.String("object", object),
		slog.String("job_id", job.ID),
	)

	return job.ID, nil
}

// GetJobStatus returns the current status of a Bulk API 2.0 ingest job.
// The caller is responsible for polling until Status reaches a terminal state
// (JobComplete, Failed, or Aborted).
func (c *Client) GetJobStatus(ctx context.Context, jobID string) (JobStatus, error) {
	var job ingestJob
	if err := c.getJSON(ctx, "/jobs/ingest/"+jobID, &job); err != nil {
		return JobStatus{}, fmt.Errorf("get job status %s: %w", jobID, err)
	}

	status := JobStatus{
		Status:           job.State,
		RecordsProcessed: job.NumberRecordsProcessed,
	}
	if job.NumberRecordsFailed > 0 {
		status.Errors = append(status.Errors,
			fmt.Sprintf("%d record(s) failed", job.NumberRecordsFailed))
	}
	if job.ErrorMessage != "" {
		status.Errors = append(status.Errors, job.ErrorMessage)
	}

	return status, nil
}

// createIngestJob opens a new Bulk API 2.0 upsert job for object.
func (c *Client) createIngestJob(ctx context.Context, object string) (ingestJob, error) {
	body := map[string]string{
		"object":              object,
		"operation":           "upsert",
		"externalIdFieldName": "Id",
		"contentType":         "CSV",
		"lineEnding":          "LF",
	}
	var job ingestJob
	if err := c.postJSON(ctx, "/jobs/ingest", body, &job); err != nil {
		return ingestJob{}, err
	}
	if job.ID == "" {
		return ingestJob{}, fmt.Errorf("create ingest job returned empty ID")
	}
	return job, nil
}

// uploadCSV converts records to CSV and PUTs them to the job batches endpoint.
func (c *Client) uploadCSV(ctx context.Context, jobID string, records []Record) error {
	data, err := recordsToCSV(records)
	if err != nil {
		return fmt.Errorf("encode records as CSV: %w", err)
	}

	url := c.endpoint("/jobs/ingest/" + jobID + "/batches")
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/csv")

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("upload CSV HTTP %d", resp.StatusCode)
	}
	return nil
}

// closeIngestJob signals to Salesforce that all data has been uploaded.
func (c *Client) closeIngestJob(ctx context.Context, jobID string) error {
	return c.patchJSON(ctx, "/jobs/ingest/"+jobID,
		map[string]string{"state": "UploadComplete"}, nil)
}

// recordsToCSV encodes records as a CSV byte slice. Column headers are derived
// from the union of all keys across all records and sorted alphabetically for
// deterministic output. Missing values for a given record are written as empty.
func recordsToCSV(records []Record) ([]byte, error) {
	// Collect all column names across all records.
	colSet := make(map[string]struct{})
	for _, r := range records {
		for k := range r {
			colSet[k] = struct{}{}
		}
	}
	cols := make([]string, 0, len(colSet))
	for k := range colSet {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	if err := w.Write(cols); err != nil {
		return nil, err
	}

	row := make([]string, len(cols))
	for _, rec := range records {
		for i, col := range cols {
			row[i] = rec[col]
		}
		if err := w.Write(row); err != nil {
			return nil, err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
