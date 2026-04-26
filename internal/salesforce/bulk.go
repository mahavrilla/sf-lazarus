package salesforce

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"
)

const (
	recordChannelBuffer = 100

	pollInitial    = 2 * time.Second
	pollMax        = 30 * time.Second
	pollMultiplier = 1.5

	// jobComplete and jobFailed are the terminal Bulk API 2.0 job states.
	jobComplete = "JobComplete"
	jobFailed   = "Failed"
	jobAborted  = "Aborted"

	// sfLocatorDone is the Sforce-Locator value that signals no more pages.
	sfLocatorDone = "null"
)

// BulkQueryParams carries the inputs needed to build and execute a query job.
type BulkQueryParams struct {
	// Object is the Salesforce sObject API name, e.g. "Opportunity".
	Object string
	// Fields is the ordered list of field names to include in SELECT.
	Fields []string
	// WatermarkField is the datetime field used for incremental filtering.
	WatermarkField string
	// Watermark is the exclusive lower bound. Empty means no WHERE clause
	// (full load).
	Watermark string
}

// bulkJob is the subset of the Bulk API 2.0 job representation we care about.
type bulkJob struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

// BulkQuerier is the interface that wraps BulkQuery, useful for testing.
type BulkQuerier interface {
	BulkQuery(ctx context.Context, p BulkQueryParams, log *slog.Logger) (<-chan Record, <-chan error)
}

// BulkQuery creates a Bulk API 2.0 query job, polls until it completes, then
// streams all result pages as individual Records on the returned channel.
//
// The error channel carries at most one error and is closed when the goroutine
// exits. The record channel is always closed before the error channel.
// Callers must drain or abandon the record channel; abandoning requires
// cancelling ctx to unblock the goroutine.
func (c *Client) BulkQuery(ctx context.Context, p BulkQueryParams, log *slog.Logger) (<-chan Record, <-chan error) {
	records := make(chan Record, recordChannelBuffer)
	errCh := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errCh)

		if err := c.runBulkQuery(ctx, p, records, log); err != nil {
			errCh <- err
		}
	}()

	return records, errCh
}

// runBulkQuery is the synchronous body of the BulkQuery goroutine.
func (c *Client) runBulkQuery(ctx context.Context, p BulkQueryParams, records chan<- Record, log *slog.Logger) error {
	soql := buildSOQL(p)
	log.Info("starting bulk query",
		slog.String("object", p.Object),
		slog.String("watermark_field", p.WatermarkField),
		slog.String("watermark", p.Watermark),
	)

	job, err := c.createQueryJob(ctx, soql)
	if err != nil {
		return fmt.Errorf("create query job for %s: %w", p.Object, err)
	}
	log.Info("bulk job created", slog.String("object", p.Object), slog.String("job_id", job.ID))

	// Always attempt cleanup, even on early return.
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.deleteResource(cleanupCtx, "/jobs/query/"+job.ID); err != nil {
			log.Warn("delete bulk job failed", slog.String("job_id", job.ID), slog.Any("error", err))
		}
	}()

	if err := c.pollJobComplete(ctx, job.ID, log); err != nil {
		return fmt.Errorf("poll job %s for %s: %w", job.ID, p.Object, err)
	}
	log.Info("bulk job complete", slog.String("object", p.Object), slog.String("job_id", job.ID))

	n, err := c.streamResults(ctx, job.ID, records, log)
	if err != nil {
		return fmt.Errorf("stream results for %s (job %s): %w", p.Object, job.ID, err)
	}
	log.Info("bulk query finished",
		slog.String("object", p.Object),
		slog.Int("records_read", n),
	)
	return nil
}

// buildSOQL constructs the SOQL query string from BulkQueryParams.
func buildSOQL(p BulkQueryParams) string {
	selectClause := strings.Join(p.Fields, ", ")
	q := fmt.Sprintf("SELECT %s FROM %s", selectClause, p.Object)
	if p.WatermarkField != "" && p.Watermark != "" {
		q += fmt.Sprintf(" WHERE %s > %s", p.WatermarkField, p.Watermark)
	}
	return q
}

// createQueryJob submits a new Bulk API 2.0 query job and returns its ID.
func (c *Client) createQueryJob(ctx context.Context, soql string) (bulkJob, error) {
	body := map[string]string{
		"operation":       "queryAll",
		"query":           soql,
		"contentType":     "CSV",
		"columnDelimiter": "COMMA",
		"lineEnding":      "LF",
	}
	var job bulkJob
	if err := c.postJSON(ctx, "/jobs/query", body, &job); err != nil {
		return bulkJob{}, err
	}
	if job.ID == "" {
		return bulkJob{}, fmt.Errorf("create job returned empty ID")
	}
	return job, nil
}

// pollJobComplete blocks until the job reaches JobComplete, Failed, or Aborted,
// or until ctx is cancelled.
func (c *Client) pollJobComplete(ctx context.Context, jobID string, log *slog.Logger) error {
	interval := pollInitial
	path := "/jobs/query/" + jobID

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}

		var job bulkJob
		if err := c.getJSON(ctx, path, &job); err != nil {
			return fmt.Errorf("poll job status: %w", err)
		}

		log.Debug("bulk job poll", slog.String("job_id", jobID), slog.String("state", job.State))

		switch job.State {
		case jobComplete:
			return nil
		case jobFailed, jobAborted:
			return fmt.Errorf("job %s entered terminal state %q", jobID, job.State)
		}

		// Advance the backoff interval, capped at pollMax.
		next := time.Duration(float64(interval) * pollMultiplier)
		interval = time.Duration(math.Min(float64(next), float64(pollMax)))
	}
}

// streamResults fetches all result pages and sends each row to records.
// It returns the total number of records sent.
func (c *Client) streamResults(ctx context.Context, jobID string, records chan<- Record, log *slog.Logger) (int, error) {
	locator := ""
	total := 0

	for {
		n, nextLocator, err := c.streamPage(ctx, jobID, locator, records)
		if err != nil {
			return total, err
		}
		total += n
		log.Debug("bulk page streamed",
			slog.String("job_id", jobID),
			slog.Int("page_records", n),
			slog.String("next_locator", nextLocator),
		)

		if nextLocator == "" || nextLocator == sfLocatorDone {
			break
		}
		locator = nextLocator
	}
	return total, nil
}

// streamPage fetches one result page and sends its rows to records.
// It returns the row count for this page and the Sforce-Locator for the next.
func (c *Client) streamPage(ctx context.Context, jobID, locator string, records chan<- Record) (int, string, error) {
	url := c.endpoint("/jobs/query/" + jobID + "/results")
	if locator != "" {
		url += "?locator=" + locator
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.accessToken)
	req.Header.Set("Accept", "text/csv")

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return 0, "", fmt.Errorf("get results HTTP %d: %s", resp.StatusCode, raw)
	}

	// Read the locator from headers before consuming the body.
	nextLocator := resp.Header.Get("Sforce-Locator")

	n, err := parseCSVPage(ctx, resp.Body, records)
	if err != nil {
		return n, "", err
	}
	return n, nextLocator, nil
}

// parseCSVPage reads a Salesforce CSV response body line-by-line and sends
// each data row as a Record. It returns the number of data rows sent.
func parseCSVPage(ctx context.Context, r io.Reader, records chan<- Record) (int, error) {
	reader := csv.NewReader(r)
	reader.ReuseRecord = false // each record is sent to a channel; don't reuse

	// First row is the column header.
	headers, err := reader.Read()
	if err == io.EOF {
		return 0, nil // empty result set
	}
	if err != nil {
		return 0, fmt.Errorf("read CSV header: %w", err)
	}

	count := 0
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, fmt.Errorf("read CSV row %d: %w", count+1, err)
		}

		rec := make(Record, len(headers))
		for i, h := range headers {
			if i < len(row) {
				rec[h] = row[i]
			}
		}

		select {
		case records <- rec:
			count++
		case <-ctx.Done():
			return count, ctx.Err()
		}
	}
	return count, nil
}
