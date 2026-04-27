// Package backfill orchestrates the end-to-end pipeline that queries
// Salesforce objects via the Bulk API and writes them to Apache Iceberg.
package backfill

import (
	"context"
	"fmt"
	"log/slog"

	"golang.org/x/sync/errgroup"

	"github.com/mahavrilla/sf-lazarus/internal/iceberg"
	"github.com/mahavrilla/sf-lazarus/internal/salesforce"
	"github.com/mahavrilla/sf-lazarus/internal/syncstate"
)

const defaultBatchSize = 5000

// SalesforceClient is the subset of the Salesforce client API the runner needs.
type SalesforceClient interface {
	Describe(ctx context.Context, objectName string) ([]salesforce.Field, error)
	BulkQuery(ctx context.Context, p salesforce.BulkQueryParams, log *slog.Logger) (<-chan salesforce.Record, <-chan error)
}

// Encoder serialises a mapped Salesforce record into the protobuf bytes that
// become EncodedRecord.Data. The concrete implementation lives in
// internal/schema.
type Encoder interface {
	Encode(record map[string]string, cols []iceberg.Column) ([]byte, error)
}

// Config holds tunables for the Runner.
type Config struct {
	// BatchSize is the number of encoded records accumulated before each
	// WriteBatch call. Defaults to 5000 when zero or negative.
	BatchSize int
}

func (c Config) batchSize() int {
	if c.BatchSize > 0 {
		return c.BatchSize
	}
	return defaultBatchSize
}

// Runner orchestrates the backfill for all registered objects.
type Runner struct {
	cfg     Config
	reg     *iceberg.Registry
	sf      SalesforceClient
	ice     iceberg.Writer
	state   syncstate.Store
	encoder Encoder
	log     *slog.Logger
}

// NewRunner constructs a Runner with all required dependencies.
func NewRunner(
	cfg Config,
	reg *iceberg.Registry,
	sf SalesforceClient,
	ice iceberg.Writer,
	state syncstate.Store,
	encoder Encoder,
	log *slog.Logger,
) *Runner {
	return &Runner{
		cfg:     cfg,
		reg:     reg,
		sf:      sf,
		ice:     ice,
		state:   state,
		encoder: encoder,
		log:     log,
	}
}

// RunAll runs the pipeline for every registered object concurrently, with a
// maximum of 5 in-flight objects to stay within the Salesforce Bulk API limit.
// It returns the first error encountered; all in-flight objects are cancelled.
func (r *Runner) RunAll(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(5)

	for _, obj := range r.reg.All() {
		obj := obj
		g.Go(func() error {
			return r.runOne(ctx, obj)
		})
	}
	return g.Wait()
}

// runOne executes the full pipeline for a single object:
//
//  1. Describe the SF object and compute the desired Iceberg schema.
//  2. Bootstrap the Iceberg table (no-op if it already exists).
//  3. Detect and apply schema drift.
//  4. Load the last watermark.
//  5. Stream records from the Bulk API.
//  6. Encode, accumulate into fixed-size batches, and write to Iceberg.
//  7. Advance the watermark after each successful WriteBatch.
func (r *Runner) runOne(ctx context.Context, obj iceberg.ObjectConfig) error {
	log := r.log.With(slog.String("object", obj.Name))

	// ── 1. Describe ──────────────────────────────────────────────────────────
	sfFields, err := r.sf.Describe(ctx, obj.Name)
	if err != nil {
		return fmt.Errorf("describe %s: %w", obj.Name, err)
	}
	desired := iceberg.ColumnsFromDescribe(sfFields, obj.ExcludeFields)

	// intentionallyDropped is the set of SF field names that exist in the org
	// but are deliberately omitted from desired (compound types, excludeFields).
	// These must not trigger the removed-column policy.
	intentionallyDropped := intentionallyDroppedFields(sfFields, desired)

	// ── 2. Bootstrap table ───────────────────────────────────────────────────
	if err := r.ice.EnsureTable(ctx, obj, desired, obj.Overflow); err != nil {
		return fmt.Errorf("ensure table %s: %w", obj.Name, err)
	}

	// ── 3. Detect and apply schema drift ─────────────────────────────────────
	current, err := r.ice.GetSchema(ctx, obj.Name)
	if err != nil {
		return fmt.Errorf("get schema %s: %w", obj.Name, err)
	}

	drift := iceberg.DetectDrift(current, desired)
	drift.RemovedColumns = rejectIntentional(drift.RemovedColumns, intentionallyDropped)

	if len(drift.RemovedColumns) > 0 {
		switch obj.RemovedFieldPolicy {
		case iceberg.PolicyHalt:
			return fmt.Errorf("object %s: halting — columns removed from SF schema: %v",
				obj.Name, columnNames(drift.RemovedColumns))
		default: // tombstone: existing column stays; new rows will have nulls
			log.Warn("columns removed from SF schema; existing Iceberg column retained",
				slog.Any("removed", columnNames(drift.RemovedColumns)),
			)
		}
	}

	if len(drift.TypeChanges) > 0 {
		log.Warn("column type changes detected; Iceberg type unchanged",
			slog.Any("changes", drift.TypeChanges),
		)
	}

	if len(drift.NewColumns) > 0 {
		if obj.Overflow {
			// New fields are not promoted — they will spill to _overflow.
			log.Info("overflow mode: new SF columns will spill to _overflow",
				slog.Any("new_columns", columnNames(drift.NewColumns)),
			)
		} else {
			if err := r.ice.EvolveSchema(ctx, obj.Name, drift.NewColumns); err != nil {
				return fmt.Errorf("evolve schema %s: %w", obj.Name, err)
			}
			log.Info("schema evolved", slog.Int("new_columns", len(drift.NewColumns)))
		}
	}

	// SchemaManager tracks the columns used for encoding. In overflow mode new
	// columns are intentionally excluded so SpillToOverflow routes them to _overflow.
	sm := iceberg.NewSchemaManager(current)
	if !obj.Overflow {
		sm.Prepare(drift)
	}

	// ── 4. Load watermark ────────────────────────────────────────────────────
	watermark, err := r.state.GetWatermark(ctx, obj.Name)
	if err != nil {
		return fmt.Errorf("get watermark %s: %w", obj.Name, err)
	}
	log.Info("starting query",
		slog.String("watermark", watermark),
		slog.Int("batch_size", r.cfg.batchSize()),
	)

	// ── 5. Stream from Bulk API ───────────────────────────────────────────────
	params := salesforce.BulkQueryParams{
		Object:         obj.Name,
		Fields:         queryFields(sm.Columns(), obj.WatermarkField),
		WatermarkField: obj.WatermarkField,
		Watermark:      watermark,
	}
	sfRecords, sfErrCh := r.sf.BulkQuery(ctx, params, log)

	// ── 6 & 7. Encode, batch, write, advance watermark ────────────────────────
	batch := make([]iceberg.EncodedRecord, 0, r.cfg.batchSize())
	batchMaxWatermark := ""

	for sfRec := range sfRecords {
		// Track the per-batch max before any transformation.
		if obj.WatermarkField != "" {
			if wv := sfRec[obj.WatermarkField]; wv > batchMaxWatermark {
				batchMaxWatermark = wv
			}
		}

		// Partition mapped fields from overflow.
		mapped, overflow, err := iceberg.SpillToOverflow(map[string]string(sfRec), sm.FieldIndex())
		if err != nil {
			return fmt.Errorf("spill overflow for %s: %w", obj.Name, err)
		}

		// Encode mapped fields to protobuf.
		data, err := r.encoder.Encode(mapped, sm.Columns())
		if err != nil {
			return fmt.Errorf("encode record for %s: %w", obj.Name, err)
		}

		batch = append(batch, iceberg.EncodedRecord{Data: data, Overflow: overflow})

		if len(batch) >= r.cfg.batchSize() {
			if err := r.writeBatch(ctx, obj.Name, batch, batchMaxWatermark, log); err != nil {
				return err
			}
			batch = batch[:0]
			batchMaxWatermark = ""
		}
	}

	// The record channel is always closed before the error channel.
	if err := <-sfErrCh; err != nil {
		return fmt.Errorf("bulk query for %s: %w", obj.Name, err)
	}

	// Flush any records that did not fill a complete batch.
	if len(batch) > 0 {
		if err := r.writeBatch(ctx, obj.Name, batch, batchMaxWatermark, log); err != nil {
			return err
		}
	}

	log.Info("object complete")
	return nil
}

// writeBatch writes the batch to Iceberg and, on success, advances the
// watermark. Watermark is only advanced when batchMaxWatermark is non-empty
// (i.e. a WatermarkField is configured and the batch had records).
func (r *Runner) writeBatch(
	ctx context.Context,
	objectName string,
	batch []iceberg.EncodedRecord,
	batchMaxWatermark string,
	log *slog.Logger,
) error {
	if err := r.ice.WriteBatch(ctx, objectName, batch); err != nil {
		return fmt.Errorf("write batch for %s: %w", objectName, err)
	}

	log.Info("batch written",
		slog.Int("batch_size", len(batch)),
		slog.String("watermark", batchMaxWatermark),
	)

	if batchMaxWatermark != "" {
		if err := r.state.SetWatermark(ctx, objectName, batchMaxWatermark); err != nil {
			return fmt.Errorf("set watermark for %s: %w", objectName, err)
		}
	}
	return nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

// queryFields builds the SELECT field list, ensuring WatermarkField and
// IsDeleted are always present. IsDeleted is required to capture soft-deletes
// from the queryAll operation. WatermarkField is required to compute the batch
// max even when it is not a mapped Iceberg column.
func queryFields(cols []iceberg.Column, watermarkField string) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	hasWatermark := watermarkField == ""
	hasIsDeleted := false
	for _, n := range names {
		if n == watermarkField {
			hasWatermark = true
		}
		if n == "IsDeleted" {
			hasIsDeleted = true
		}
	}
	if !hasWatermark {
		names = append(names, watermarkField)
	}
	if !hasIsDeleted {
		names = append(names, "IsDeleted")
	}
	return names
}

// columnNames extracts the Name field from a slice of columns.
func columnNames(cols []iceberg.Column) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

// intentionallyDroppedFields returns the set of SF field names that are
// present in the raw describe response but absent from the desired column list
// (e.g. compound address fields, explicit excludeFields).
func intentionallyDroppedFields(sfFields []salesforce.Field, desired []iceberg.Column) map[string]struct{} {
	desiredSet := make(map[string]struct{}, len(desired))
	for _, c := range desired {
		desiredSet[c.Name] = struct{}{}
	}
	dropped := make(map[string]struct{})
	for _, f := range sfFields {
		if _, ok := desiredSet[f.Name]; !ok {
			dropped[f.Name] = struct{}{}
		}
	}
	return dropped
}

// rejectIntentional filters out columns whose names appear in the
// intentionally-dropped set, returning only genuine schema removals.
func rejectIntentional(removed []iceberg.Column, intentional map[string]struct{}) []iceberg.Column {
	out := removed[:0]
	for _, c := range removed {
		if _, ok := intentional[c.Name]; !ok {
			out = append(out, c)
		}
	}
	return out
}
