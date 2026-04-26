package iceberg

import "context"

// EncodedRecord holds one Salesforce record after schema-aware encoding.
type EncodedRecord struct {
	Data     []byte // protobuf-encoded mapped fields
	Overflow []byte // JSON blob of unmapped fields; nil when none
}

// Codec decodes a protobuf-encoded record into a field map keyed by column name.
// Implementations live in internal/schema.
type Codec interface {
	Decode(data []byte, cols []Column) (map[string]any, error)
}

// SchemaManager tracks the active column set for one object and absorbs drift.
type SchemaManager struct {
	cols     []Column
	fieldIdx MappedFieldIndex
}

// NewSchemaManager initialises a SchemaManager for the given columns.
func NewSchemaManager(cols []Column) *SchemaManager {
	return &SchemaManager{
		cols:     cols,
		fieldIdx: NewMappedFieldIndex(cols),
	}
}

// Columns returns the current column set.
func (m *SchemaManager) Columns() []Column { return m.cols }

// FieldIndex returns the name→index mapping for the current columns.
func (m *SchemaManager) FieldIndex() MappedFieldIndex { return m.fieldIdx }

// Prepare merges newly discovered columns from a DriftReport into the active set.
// It is called after DetectDrift and before encoding begins.
func (m *SchemaManager) Prepare(drift DriftReport) {
	m.cols = append(m.cols, drift.NewColumns...)
	m.fieldIdx = NewMappedFieldIndex(m.cols)
}

// Writer is the interface the Iceberg/Glue layer must satisfy.
type Writer interface {
	// EnsureTable creates the Iceberg table if it does not exist.
	// withOverflow adds a _overflow STRING column to hold unmapped fields.
	EnsureTable(ctx context.Context, obj ObjectConfig, cols []Column, withOverflow bool) error

	// GetSchema returns the current column set for objectName from the catalog.
	GetSchema(ctx context.Context, objectName string) ([]Column, error)

	// EvolveSchema adds newCols to the existing table schema.
	EvolveSchema(ctx context.Context, objectName string, newCols []Column) error

	// WriteBatch appends a batch of encoded records to the Iceberg table.
	WriteBatch(ctx context.Context, objectName string, records []EncodedRecord) error
}
