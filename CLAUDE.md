# Lazarus — Iceberg Backfill Implementation
 
## Project context
 
Lazarus is a Go monorepo using the `cmd/` pattern. Each `cmd/` produces an independent binary.
The backfill binary bulk-queries Salesforce objects and writes them to Apache Iceberg via the Glue catalog on S3.
 
## Repository layout (relevant parts)
 
```
lazarus/
├── cmd/backfill/main.go           # entry point — thin wiring only
├── config/objects.yaml            # manifest of all managed SF objects
└── internal/
    ├── iceberg/
    │   ├── doc.go                 # package overview and call order
    │   ├── registry.go            # ObjectConfig + Registry (loads objects.yaml)
    │   ├── schema.go              # SF field → Iceberg column type mapping
    │   ├── drift.go               # DriftReport, DetectDrift (pure, no I/O)
    │   ├── overflow.go            # SpillToOverflow, MappedFieldIndex
    │   └── writer.go              # Writer interface, SchemaManager, EncodedRecord
    ├── salesforce/
    │   ├── client.go              # Auth + REST client
    │   ├── bulk.go                # BulkQuery — returns <-chan Record
    │   └── describe.go            # Describe → []salesforce.Field
    ├── schema/
    │   ├── registry.go            # Protobuf schema registry client
    │   └── proto.go               # Encode(record) → []byte
    ├── syncstate/
    │   └── store.go               # GetWatermark / SetWatermark (DynamoDB or S3)
    └── backfill/
        └── run.go                 # Runner — orchestrates one object end to end
```
 
## Key types
 
```go
// internal/iceberg/registry.go
type ObjectConfig struct {
    Name               string
    PrimaryKey         string
    WatermarkField     string
    PartitionBy        []string
    Overflow           bool
    RemovedFieldPolicy RemovedFieldPolicy // "tombstone" | "halt"
    ExcludeFields      []string
}
 
// internal/iceberg/writer.go
type Writer interface {
    EnsureTable(ctx context.Context, obj ObjectConfig, cols []Column, withOverflow bool) error
    GetSchema(ctx context.Context, objectName string) ([]Column, error)
    EvolveSchema(ctx context.Context, objectName string, newCols []Column) error
    WriteBatch(ctx context.Context, objectName string, records []EncodedRecord) error
}
 
type EncodedRecord struct {
    Data     []byte // Protobuf-encoded mapped fields
    Overflow []byte // JSON blob of unmapped fields, nil if none
}
 
// internal/iceberg/drift.go
type DriftReport struct {
    NewColumns     []Column
    RemovedColumns []Column
    TypeChanges    []TypeChange
}
```
 
## Backfill call chain
 
```
LoadRegistry → reg.All() → for each object:
  sf.Describe → ColumnsFromDescribe → SchemaManager.Prepare →
  syncstate.GetWatermark → sf.BulkQuery (returns chan Record) →
  encode + spill overflow → ice.WriteBatch →
  syncstate.SetWatermark(MAX(WatermarkField))
```
 
## Design constraints
 
- `BulkQuery` returns `<-chan salesforce.Record`, not a slice — do not load all records into memory
- Batch records into fixed-size chunks (configurable, default 5000) before calling `WriteBatch`
- Watermark advances **only** after a successful `WriteBatch` — at-least-once guarantee
- Run all objects concurrently via `errgroup`, max 5 concurrent (SF Bulk API limit)
- All errors wrapped with `fmt.Errorf("context: %w", err)` — no naked errors
- Structured logging via `log/slog` throughout — key fields: `object`, `watermark`, `batch_size`, `records_written`
- `Writer` is an interface — provide a concrete `GlueWriter` implementation using the AWS SDK
## What needs to be implemented
 
### 1. `internal/iceberg/glue_writer.go`
 
Concrete implementation of the `Writer` interface backed by:
 
- AWS Glue Data Catalog (table bootstrap + schema evolution via `AddColumns`)
- S3 (Parquet file writes)
- Use `github.com/apache/iceberg-go` if it has Glue catalog support, otherwise write Parquet directly via `github.com/parquet-go/parquet-go` and manage Iceberg metadata manually
### 2. `internal/salesforce/bulk.go`
 
Implement `BulkQuery`:
 
- Use Salesforce Bulk API 2.0
- Build SOQL: `SELECT {mapped fields} FROM {object} WHERE {watermark_field} > {watermark}`
- Stream results as `chan salesforce.Record` — do not buffer the entire result set
- Handle job creation → poll until complete → stream results → close job
### 3. `internal/backfill/run.go`
 
Implement `Runner`:
 
- `RunAll(ctx)` — `errgroup` with limit 5 over `reg.All()`
- `runOne(ctx, ObjectConfig)` — full pipeline for one object
- Fixed-size batch accumulation from the `BulkQuery` channel
- Watermark computed as `MAX(record[obj.WatermarkField])` across the batch
### 4. `cmd/backfill/main.go`
 
Wire everything:
 
- Load `config/objects.yaml` via `iceberg.LoadRegistry`
- Accept `--object` flag to run a single object (useful for debugging)
- Accept `--dry-run` flag to describe + drift check without writing
- Use `log/slog` with JSON handler for structured output
## Coding standards
 
- Composition over inheritance — use interfaces, no embedding for behavior
- No global state — pass dependencies explicitly via constructors
- Each file does one thing — if a file needs a second responsibility, split it
- Self-documenting names — no abbreviations except well-known ones (`ctx`, `cfg`, `err`)
- Tests alongside source as `_test.go` — `drift.go` is pure and should be fully unit tested