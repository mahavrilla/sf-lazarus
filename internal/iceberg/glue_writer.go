package iceberg

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	gluecatalog "github.com/apache/iceberg-go/catalog/glue"
	"github.com/aws/aws-sdk-go-v2/aws"

	// Register S3 (and GCS/Azure) file I/O so iceberg-go can read/write cloud URIs.
	_ "github.com/apache/iceberg-go/io/gocloud"
)

const overflowColumnName = "_overflow"

// GlueWriterConfig holds the configuration for a GlueWriter.
type GlueWriterConfig struct {
	// Database is the Glue database (namespace) that holds all managed tables.
	Database string
	// Bucket is the S3 bucket where Iceberg data and metadata are stored.
	Bucket string
	// KeyPrefix is the S3 key prefix for all tables, e.g. "iceberg/".
	KeyPrefix string
	// AWSConfig is the resolved AWS SDK configuration.
	AWSConfig aws.Config
}

// GlueWriter implements Writer using AWS Glue as the Iceberg catalog and S3 as storage.
type GlueWriter struct {
	cat   *gluecatalog.Catalog
	cfg   GlueWriterConfig
	codec Codec
	log   *slog.Logger

	mu          sync.RWMutex
	schemaCache map[string][]Column // objectName → current columns
}

// NewGlueWriter constructs a GlueWriter. codec is used to decode protobuf record bytes
// before writing to Parquet; pass nil only when WriteBatch will not be called.
func NewGlueWriter(cfg GlueWriterConfig, codec Codec, log *slog.Logger) *GlueWriter {
	cat := gluecatalog.NewCatalog(
		gluecatalog.WithAwsConfig(cfg.AWSConfig),
		gluecatalog.WithAwsProperties(gluecatalog.AwsProperties{
			gluecatalog.Region: cfg.AWSConfig.Region,
		}),
	)
	return &GlueWriter{
		cat:         cat,
		cfg:         cfg,
		codec:       codec,
		log:         log,
		schemaCache: make(map[string][]Column),
	}
}

// EnsureTable creates the Iceberg table in Glue if it does not already exist.
// If withOverflow is true, a _overflow STRING column is appended to store unmapped fields.
func (w *GlueWriter) EnsureTable(ctx context.Context, obj ObjectConfig, cols []Column, withOverflow bool) error {
	ident := gluecatalog.TableIdentifier(w.cfg.Database, obj.Name)

	exists, err := w.cat.CheckTableExists(ctx, ident)
	if err != nil {
		return fmt.Errorf("check table exists %s: %w", obj.Name, err)
	}
	if exists {
		w.cacheSchema(obj.Name, cols)
		return nil
	}

	allCols := cols
	if withOverflow {
		allCols = append(append([]Column{}, cols...), Column{Name: overflowColumnName, Type: ColumnTypeString})
	}

	schema := columnsToIcebergSchema(allCols, 1)
	location := w.tableLocation(obj.Name)

	opts := []catalog.CreateTableOpt{
		catalog.WithLocation(location),
	}
	if len(obj.PartitionBy) > 0 {
		spec := buildPartitionSpec(schema, obj.PartitionBy)
		if spec != nil {
			opts = append(opts, catalog.WithPartitionSpec(spec))
		}
	}

	if _, err := w.cat.CreateTable(ctx, ident, schema, opts...); err != nil {
		return fmt.Errorf("create table %s: %w", obj.Name, err)
	}

	w.cacheSchema(obj.Name, allCols)
	w.log.Info("created iceberg table",
		slog.String("object", obj.Name),
		slog.String("location", location),
		slog.Int("columns", len(allCols)),
	)
	return nil
}

// GetSchema returns the current column set for objectName from the Glue catalog.
func (w *GlueWriter) GetSchema(ctx context.Context, objectName string) ([]Column, error) {
	ident := gluecatalog.TableIdentifier(w.cfg.Database, objectName)
	tbl, err := w.cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, fmt.Errorf("load table %s: %w", objectName, err)
	}

	cols := icebergSchemaToColumns(tbl.Schema())
	w.cacheSchema(objectName, cols)
	return cols, nil
}

// EvolveSchema adds newCols to the live Iceberg table schema via a transaction.
func (w *GlueWriter) EvolveSchema(ctx context.Context, objectName string, newCols []Column) error {
	if len(newCols) == 0 {
		return nil
	}

	ident := gluecatalog.TableIdentifier(w.cfg.Database, objectName)
	tbl, err := w.cat.LoadTable(ctx, ident)
	if err != nil {
		return fmt.Errorf("load table %s: %w", objectName, err)
	}

	tx := tbl.NewTransaction()
	us := tx.UpdateSchema(false, false)
	for _, col := range newCols {
		us.AddColumn(
			[]string{col.Name},
			columnTypeToIceberg(col.Type),
			"",    // doc
			false, // all new SF columns are optional
			nil,   // no default value
		)
	}
	if err := us.Commit(); err != nil {
		return fmt.Errorf("stage schema update for %s: %w", objectName, err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit schema evolution for %s: %w", objectName, err)
	}

	existing, _ := w.getCachedSchema(objectName)
	w.cacheSchema(objectName, append(existing, newCols...))

	w.log.Info("evolved schema",
		slog.String("object", objectName),
		slog.Int("new_columns", len(newCols)),
	)
	return nil
}

// WriteBatch decodes records and appends them to the Iceberg table in a single transaction.
func (w *GlueWriter) WriteBatch(ctx context.Context, objectName string, records []EncodedRecord) error {
	if w.codec == nil {
		return fmt.Errorf("no codec configured for GlueWriter")
	}
	if len(records) == 0 {
		return nil
	}

	cols, err := w.getCachedSchema(objectName)
	if err != nil {
		return fmt.Errorf("get schema for %s: %w", objectName, err)
	}

	// Separate data columns from the overflow column.
	hasOverflow := false
	dataCols := make([]Column, 0, len(cols))
	for _, c := range cols {
		if c.Name == overflowColumnName {
			hasOverflow = true
		} else {
			dataCols = append(dataCols, c)
		}
	}

	decoded := make([]map[string]any, len(records))
	for i, rec := range records {
		fields, err := w.codec.Decode(rec.Data, dataCols)
		if err != nil {
			return fmt.Errorf("decode record %d for %s: %w", i, objectName, err)
		}
		decoded[i] = fields
	}

	rdr, err := buildRecordReader(cols, hasOverflow, decoded, records)
	if err != nil {
		return fmt.Errorf("build record reader for %s: %w", objectName, err)
	}
	defer rdr.Release()

	ident := gluecatalog.TableIdentifier(w.cfg.Database, objectName)
	tbl, err := w.cat.LoadTable(ctx, ident)
	if err != nil {
		return fmt.Errorf("load table %s: %w", objectName, err)
	}

	if _, err := tbl.Append(ctx, rdr, nil); err != nil {
		return fmt.Errorf("append batch to %s: %w", objectName, err)
	}

	w.log.Info("wrote batch",
		slog.String("object", objectName),
		slog.Int("records_written", len(records)),
	)
	return nil
}

// ─── schema helpers ──────────────────────────────────────────────────────────

func (w *GlueWriter) tableLocation(objectName string) string {
	return fmt.Sprintf("s3://%s/%s%s", w.cfg.Bucket, w.cfg.KeyPrefix, objectName)
}

func (w *GlueWriter) cacheSchema(objectName string, cols []Column) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.schemaCache[objectName] = cols
}

func (w *GlueWriter) getCachedSchema(objectName string) ([]Column, error) {
	w.mu.RLock()
	cols, ok := w.schemaCache[objectName]
	w.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("schema for %s not loaded; call EnsureTable or GetSchema first", objectName)
	}
	return cols, nil
}

// columnsToIcebergSchema converts []Column to an *iceberg.Schema.
// fieldIDStart is the first field ID to assign.
func columnsToIcebergSchema(cols []Column, fieldIDStart int) *iceberg.Schema {
	fields := make([]iceberg.NestedField, len(cols))
	id := fieldIDStart
	for i, c := range cols {
		fields[i] = iceberg.NestedField{
			Type:     columnTypeToIceberg(c.Type),
			ID:       id,
			Name:     c.Name,
			Required: c.Required,
		}
		id++
	}
	return iceberg.NewSchema(0, fields...)
}

// icebergSchemaToColumns converts an *iceberg.Schema back to []Column.
func icebergSchemaToColumns(s *iceberg.Schema) []Column {
	fields := s.Fields()
	cols := make([]Column, len(fields))
	for i, f := range fields {
		cols[i] = Column{
			Name:     f.Name,
			Type:     icebergTypeToColumnType(f.Type),
			Required: f.Required,
		}
	}
	return cols
}

// columnTypeToIceberg maps a ColumnType to the corresponding iceberg.PrimitiveType.
func columnTypeToIceberg(ct ColumnType) iceberg.Type {
	switch ct {
	case ColumnTypeLong:
		return iceberg.PrimitiveTypes.Int64
	case ColumnTypeInt:
		return iceberg.PrimitiveTypes.Int32
	case ColumnTypeDouble:
		return iceberg.PrimitiveTypes.Float64
	case ColumnTypeFloat:
		return iceberg.PrimitiveTypes.Float32
	case ColumnTypeBoolean:
		return iceberg.PrimitiveTypes.Bool
	case ColumnTypeDate:
		return iceberg.PrimitiveTypes.Date
	case ColumnTypeTimestamp:
		return iceberg.PrimitiveTypes.TimestampTz
	case ColumnTypeBinary:
		return iceberg.PrimitiveTypes.Binary
	default: // ColumnTypeString and anything unrecognised
		return iceberg.PrimitiveTypes.String
	}
}

// icebergTypeToColumnType is the reverse of columnTypeToIceberg.
func icebergTypeToColumnType(t iceberg.Type) ColumnType {
	switch t {
	case iceberg.PrimitiveTypes.Int64:
		return ColumnTypeLong
	case iceberg.PrimitiveTypes.Int32:
		return ColumnTypeInt
	case iceberg.PrimitiveTypes.Float64:
		return ColumnTypeDouble
	case iceberg.PrimitiveTypes.Float32:
		return ColumnTypeFloat
	case iceberg.PrimitiveTypes.Bool:
		return ColumnTypeBoolean
	case iceberg.PrimitiveTypes.Date:
		return ColumnTypeDate
	case iceberg.PrimitiveTypes.Timestamp, iceberg.PrimitiveTypes.TimestampTz:
		return ColumnTypeTimestamp
	case iceberg.PrimitiveTypes.Binary:
		return ColumnTypeBinary
	default:
		return ColumnTypeString
	}
}

// buildPartitionSpec returns an iceberg.PartitionSpec from the given partition
// configs. Supported transforms: "day", "hour", "month", "year", "identity"
// (default). Columns not found in the schema are skipped.
func buildPartitionSpec(schema *iceberg.Schema, partitionBy []PartitionConfig) *iceberg.PartitionSpec {
	fields := make([]iceberg.PartitionField, 0, len(partitionBy))
	for i, p := range partitionBy {
		f, ok := schema.FindFieldByName(p.Field)
		if !ok {
			continue
		}
		fields = append(fields, iceberg.PartitionField{
			SourceID:  f.ID,
			FieldID:   1000 + i,
			Name:      p.Field,
			Transform: partitionTransform(p.Transform),
		})
	}
	if len(fields) == 0 {
		return nil
	}
	spec := iceberg.NewPartitionSpec(fields...)
	return &spec
}

// partitionTransform maps a transform name to the corresponding iceberg.Transform.
func partitionTransform(name string) iceberg.Transform {
	switch name {
	case "day":
		return iceberg.DayTransform{}
	case "hour":
		return iceberg.HourTransform{}
	case "month":
		return iceberg.MonthTransform{}
	case "year":
		return iceberg.YearTransform{}
	default:
		return iceberg.IdentityTransform{}
	}
}

// ─── Arrow conversion ────────────────────────────────────────────────────────

// columnsToArrowSchema builds an Arrow schema from []Column.
func columnsToArrowSchema(cols []Column) *arrow.Schema {
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		fields[i] = arrow.Field{
			Name:     c.Name,
			Type:     columnTypeToArrow(c.Type),
			Nullable: !c.Required,
		}
	}
	return arrow.NewSchema(fields, nil)
}

// columnTypeToArrow maps a ColumnType to an Arrow data type.
func columnTypeToArrow(ct ColumnType) arrow.DataType {
	switch ct {
	case ColumnTypeLong:
		return arrow.PrimitiveTypes.Int64
	case ColumnTypeInt:
		return arrow.PrimitiveTypes.Int32
	case ColumnTypeDouble:
		return arrow.PrimitiveTypes.Float64
	case ColumnTypeFloat:
		return arrow.PrimitiveTypes.Float32
	case ColumnTypeBoolean:
		return arrow.FixedWidthTypes.Boolean
	case ColumnTypeDate:
		return arrow.FixedWidthTypes.Date32
	case ColumnTypeTimestamp:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	case ColumnTypeBinary:
		return arrow.BinaryTypes.Binary
	default:
		return arrow.BinaryTypes.String
	}
}

// buildRecordReader converts decoded field maps into an Arrow RecordReader.
// If hasOverflow is true, the last column in cols is overflowColumnName and its
// value comes from EncodedRecord.Overflow rather than the decoded map.
func buildRecordReader(cols []Column, hasOverflow bool, decoded []map[string]any, raw []EncodedRecord) (array.RecordReader, error) {
	schema := columnsToArrowSchema(cols)
	pool := memory.NewGoAllocator()
	bldr := array.NewRecordBuilder(pool, schema)
	defer bldr.Release()

	overflowIdx := -1
	if hasOverflow {
		overflowIdx = len(cols) - 1
	}

	for ri, row := range decoded {
		for ci, col := range cols {
			fb := bldr.Field(ci)
			if ci == overflowIdx {
				if raw[ri].Overflow != nil {
					fb.(*array.StringBuilder).Append(string(raw[ri].Overflow))
				} else {
					fb.AppendNull()
				}
				continue
			}
			v, ok := row[col.Name]
			if !ok {
				fb.AppendNull()
				continue
			}
			if err := appendValue(fb, col.Type, v); err != nil {
				return nil, fmt.Errorf("column %s record %d: %w", col.Name, ri, err)
			}
		}
	}

	rec := bldr.NewRecord()
	defer rec.Release()

	rdr, err := array.NewRecordReader(schema, []arrow.Record{rec})
	if err != nil {
		return nil, fmt.Errorf("new record reader: %w", err)
	}
	return rdr, nil
}
