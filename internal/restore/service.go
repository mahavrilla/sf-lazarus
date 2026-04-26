// Package restore queries the raw Iceberg table to retrieve last-known-good
// Salesforce record state, used as input for point-in-time restore operations.
package restore

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"time"
)

// sfIDPattern accepts any non-empty alphanumeric string as a record ID.
// The goal is SQL injection prevention: Salesforce IDs never contain quotes,
// spaces, or special characters. Length is not enforced here.
var sfIDPattern = regexp.MustCompile(`^[a-zA-Z0-9]+$`)

// Querier is the subset of the Athena client the restore service needs.
// Matches the signature of (*athena.Client).Execute.
type Querier interface {
	Execute(ctx context.Context, query string) ([]map[string]string, error)
}

// Options controls what LastGoodState queries.
type Options struct {
	// ObjectType is the Salesforce sObject API name, e.g. "Lead".
	ObjectType string
	// RecordID is the Salesforce record ID to look up. When empty, the last
	// good state of every record in the table is returned.
	RecordID string
	// Before is the point-in-time anchor. Only states with
	// systemmodstamp < Before are considered.
	Before time.Time
}

// Service retrieves the last-known-good state of Salesforce records from the
// raw append-only Iceberg table.
type Service struct {
	athena Querier
	log    *slog.Logger
}

// NewService constructs a Service with the given Athena querier and logger.
func NewService(athena Querier, log *slog.Logger) *Service {
	return &Service{athena: athena, log: log}
}

// LastGoodState returns the last state of matching records whose systemmodstamp
// is strictly before opts.Before.
//
// When opts.RecordID is set, at most one record is returned (the most recent
// version before the cutoff). When opts.RecordID is empty, the most recent
// version of every record in the table before the cutoff is returned.
//
func (s *Service) LastGoodState(ctx context.Context, opts Options) ([]map[string]string, error) {
	if opts.ObjectType == "" {
		return nil, fmt.Errorf("restore: ObjectType is required")
	}
	if opts.Before.IsZero() {
		return nil, fmt.Errorf("restore: Before is required")
	}
	if opts.RecordID != "" && !sfIDPattern.MatchString(opts.RecordID) {
		return nil, fmt.Errorf("restore: RecordID %q is not a valid Salesforce ID", opts.RecordID)
	}

	table := tableFor(opts.ObjectType)
	before := opts.Before.UTC().Format(time.RFC3339)

	var query string
	if opts.RecordID != "" {
		query = singleRecordQuery(table, opts.RecordID, before)
	} else {
		query = fullObjectQuery(table, before)
	}

	s.log.Debug("restore query",
		slog.String("object", opts.ObjectType),
		slog.String("record_id", opts.RecordID),
		slog.String("before", before),
	)

	rows, err := s.athena.Execute(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("restore last good state for %s: %w", opts.ObjectType, err)
	}

	// The full-object query adds a synthetic rn column from ROW_NUMBER(). Strip
	// it so callers receive clean record maps regardless of which path ran.
	for _, row := range rows {
		delete(row, "rn")
	}

	s.log.Debug("restore query complete",
		slog.String("object", opts.ObjectType),
		slog.Int("records", len(rows)),
	)

	return rows, nil
}

// tableFor returns the Glue catalog table name for a Salesforce object.
// Tables are named after the object directly (e.g. "Lead", "Account"),
// matching what the sync pipeline creates via GlueWriter.
func tableFor(objectType string) string {
	return objectType
}

// singleRecordQuery returns the most recent state of one record before before.
func singleRecordQuery(table, recordID, before string) string {
	return fmt.Sprintf(
		`SELECT *
FROM %s
WHERE id = '%s'
  AND systemmodstamp < '%s'
ORDER BY systemmodstamp DESC
LIMIT 1`,
		table, recordID, before,
	)
}

// fullObjectQuery returns the most recent state of every record before before.
// Uses ROW_NUMBER() OVER (PARTITION BY id) because Athena (Presto/Trino) does
// not support PostgreSQL's DISTINCT ON syntax used in the design document.
func fullObjectQuery(table, before string) string {
	return fmt.Sprintf(
		`WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY systemmodstamp DESC) AS rn
    FROM %s
    WHERE systemmodstamp < '%s'
)
SELECT * FROM ranked WHERE rn = 1`,
		table, before,
	)
}
