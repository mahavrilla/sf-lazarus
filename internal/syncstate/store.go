// Package syncstate persists per-object watermarks between backfill runs.
package syncstate

import "context"

// Store reads and writes the last successfully committed watermark for each
// managed Salesforce object.
type Store interface {
	// GetWatermark returns the last committed watermark for objectName.
	// Returns ("", nil) when the object has never been synced (full load).
	GetWatermark(ctx context.Context, objectName string) (string, error)

	// SetWatermark durably records watermark as the new high-water mark for
	// objectName. It is called only after a successful WriteBatch.
	SetWatermark(ctx context.Context, objectName string, watermark string) error
}
