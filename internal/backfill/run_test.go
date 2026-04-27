package backfill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/mahavrilla/sf-lazarus/internal/iceberg"
	"github.com/mahavrilla/sf-lazarus/internal/salesforce"
)

// ── unit tests for pure helpers ──────────────────────────────────────────────

func TestQueryFields_watermarkAlreadyPresent(t *testing.T) {
	// IsDeleted is always appended; watermark already present so no extra field.
	cols := []iceberg.Column{
		{Name: "Id"},
		{Name: "LastModifiedDate"},
		{Name: "Amount"},
	}
	got := queryFields(cols, "LastModifiedDate")
	if len(got) != 4 {
		t.Fatalf("expected 4 fields (+ IsDeleted), got %d: %v", len(got), got)
	}
}

func TestQueryFields_watermarkMissing(t *testing.T) {
	// Both watermark and IsDeleted are appended.
	cols := []iceberg.Column{{Name: "Id"}, {Name: "Name"}}
	got := queryFields(cols, "SystemModstamp")
	if len(got) != 4 {
		t.Fatalf("expected 4 fields (+ watermark + IsDeleted), got %d: %v", len(got), got)
	}
}

func TestQueryFields_noWatermark(t *testing.T) {
	// Only IsDeleted is appended when watermark is empty.
	cols := []iceberg.Column{{Name: "Id"}}
	got := queryFields(cols, "")
	if len(got) != 2 {
		t.Fatalf("expected 2 fields (Id + IsDeleted), got %d", len(got))
	}
	if got[1] != "IsDeleted" {
		t.Errorf("expected IsDeleted appended, got %q", got[1])
	}
}

func TestQueryFields_isDeletedAlreadyPresent(t *testing.T) {
	// IsDeleted already in schema — no duplicate.
	cols := []iceberg.Column{{Name: "Id"}, {Name: "IsDeleted"}}
	got := queryFields(cols, "SystemModstamp")
	count := 0
	for _, f := range got {
		if f == "IsDeleted" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("IsDeleted appears %d times, want 1: %v", count, got)
	}
}

func TestConfigBatchSize_default(t *testing.T) {
	c := Config{}
	if c.batchSize() != defaultBatchSize {
		t.Errorf("expected default %d, got %d", defaultBatchSize, c.batchSize())
	}
}

func TestConfigBatchSize_custom(t *testing.T) {
	c := Config{BatchSize: 100}
	if c.batchSize() != 100 {
		t.Errorf("expected 100, got %d", c.batchSize())
	}
}

func TestConfigBatchSize_negative(t *testing.T) {
	c := Config{BatchSize: -1}
	if c.batchSize() != defaultBatchSize {
		t.Errorf("expected default for negative, got %d", c.batchSize())
	}
}

// ── fakes ────────────────────────────────────────────────────────────────────

type fakeSF struct {
	describeResult []salesforce.Field
	records        []salesforce.Record
	describeErr    error
	queryErr       error
}

func (f *fakeSF) Describe(_ context.Context, _ string) ([]salesforce.Field, error) {
	return f.describeResult, f.describeErr
}

func (f *fakeSF) BulkQuery(_ context.Context, _ salesforce.BulkQueryParams, _ *slog.Logger) (<-chan salesforce.Record, <-chan error) {
	ch := make(chan salesforce.Record, len(f.records)+1)
	errCh := make(chan error, 1)
	for _, r := range f.records {
		ch <- r
	}
	close(ch)
	if f.queryErr != nil {
		errCh <- f.queryErr
	}
	close(errCh)
	return ch, errCh
}

type fakeWriter struct {
	schema    []iceberg.Column
	batches   [][]iceberg.EncodedRecord
	ensureErr error
	writeErr  error
	evolveErr error
}

func (f *fakeWriter) EnsureTable(_ context.Context, _ iceberg.ObjectConfig, cols []iceberg.Column, _ bool) error {
	if f.schema == nil {
		f.schema = append([]iceberg.Column{}, cols...)
	}
	return f.ensureErr
}

func (f *fakeWriter) GetSchema(_ context.Context, _ string) ([]iceberg.Column, error) {
	return append([]iceberg.Column{}, f.schema...), nil
}

func (f *fakeWriter) EvolveSchema(_ context.Context, _ string, newCols []iceberg.Column) error {
	f.schema = append(f.schema, newCols...)
	return f.evolveErr
}

func (f *fakeWriter) WriteBatch(_ context.Context, _ string, records []iceberg.EncodedRecord) error {
	if f.writeErr != nil {
		return f.writeErr
	}
	cp := make([]iceberg.EncodedRecord, len(records))
	copy(cp, records)
	f.batches = append(f.batches, cp)
	return nil
}

type fakeState struct {
	watermarks map[string]string
	getErr     error
	setErr     error
}

func newFakeState() *fakeState { return &fakeState{watermarks: make(map[string]string)} }

func (s *fakeState) GetWatermark(_ context.Context, name string) (string, error) {
	return s.watermarks[name], s.getErr
}

func (s *fakeState) SetWatermark(_ context.Context, name, wm string) error {
	if s.setErr != nil {
		return s.setErr
	}
	s.watermarks[name] = wm
	return nil
}

type fakeEncoder struct{}

func (e *fakeEncoder) Encode(record map[string]string, _ []iceberg.Column) ([]byte, error) {
	return []byte(fmt.Sprintf("%v", record)), nil
}

func makeRunner(cfg Config, sf SalesforceClient, ice iceberg.Writer, state *fakeState) *Runner {
	return NewRunner(cfg, iceberg.NewRegistry(nil), sf, ice, state, &fakeEncoder{}, slog.Default())
}

// ── tests ────────────────────────────────────────────────────────────────────

func TestRunOne_watermarkAdvancesPerBatch(t *testing.T) {
	// 3 records, batch size 2 → first flush at records 1-2, second flush at record 3.
	fields := []salesforce.Field{
		{Name: "Id", Type: "id", Nillable: false},
		{Name: "SystemModstamp", Type: "datetime", Nillable: false},
	}
	records := []salesforce.Record{
		{"Id": "001", "SystemModstamp": "2024-01-01T00:00:00.000+0000"},
		{"Id": "002", "SystemModstamp": "2024-01-02T00:00:00.000+0000"},
		{"Id": "003", "SystemModstamp": "2024-01-03T00:00:00.000+0000"},
	}

	sf := &fakeSF{describeResult: fields, records: records}
	ice := &fakeWriter{}
	state := newFakeState()

	r := makeRunner(Config{BatchSize: 2}, sf, ice, state)
	obj := iceberg.ObjectConfig{Name: "Account", WatermarkField: "SystemModstamp"}

	if err := r.runOne(context.Background(), obj); err != nil {
		t.Fatalf("runOne: %v", err)
	}

	if len(ice.batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(ice.batches))
	}
	if len(ice.batches[0]) != 2 {
		t.Errorf("first batch: expected 2, got %d", len(ice.batches[0]))
	}
	if len(ice.batches[1]) != 1 {
		t.Errorf("second batch: expected 1, got %d", len(ice.batches[1]))
	}

	got := state.watermarks["Account"]
	want := "2024-01-03T00:00:00.000+0000"
	if got != want {
		t.Errorf("final watermark: got %q, want %q", got, want)
	}
}

func TestRunOne_watermarkNotAdvancedOnWriteFailure(t *testing.T) {
	fields := []salesforce.Field{
		{Name: "Id", Type: "id"},
		{Name: "Ts", Type: "datetime"},
	}
	records := []salesforce.Record{
		{"Id": "001", "Ts": "2024-06-01T00:00:00.000+0000"},
		{"Id": "002", "Ts": "2024-06-02T00:00:00.000+0000"},
	}

	sf := &fakeSF{describeResult: fields, records: records}
	ice := &fakeWriter{writeErr: errors.New("s3 timeout")}
	state := newFakeState()
	state.watermarks["Obj"] = "2024-01-01T00:00:00.000+0000"

	r := makeRunner(Config{BatchSize: 10}, sf, ice, state)

	err := r.runOne(context.Background(), iceberg.ObjectConfig{
		Name:           "Obj",
		WatermarkField: "Ts",
	})
	if err == nil {
		t.Fatal("expected error from WriteBatch, got nil")
	}
	if got := state.watermarks["Obj"]; got != "2024-01-01T00:00:00.000+0000" {
		t.Errorf("watermark advanced despite write failure: %q", got)
	}
}

func TestRunOne_haltPolicyOnRemovedColumn(t *testing.T) {
	// Iceberg has ExtraCol; SF no longer includes it.
	ice := &fakeWriter{
		schema: []iceberg.Column{
			{Name: "Id", Type: iceberg.ColumnTypeString},
			{Name: "ExtraCol", Type: iceberg.ColumnTypeString},
		},
	}
	sf := &fakeSF{
		describeResult: []salesforce.Field{{Name: "Id", Type: "id"}},
	}

	r := makeRunner(Config{BatchSize: 10}, sf, ice, newFakeState())

	err := r.runOne(context.Background(), iceberg.ObjectConfig{
		Name:               "Obj",
		RemovedFieldPolicy: iceberg.PolicyHalt,
	})
	if err == nil {
		t.Fatal("expected halt error for removed column, got nil")
	}
}

func TestRunOne_newColumnEvolvesSchema(t *testing.T) {
	// Iceberg has only Id; SF now also has NewField.
	ice := &fakeWriter{
		schema: []iceberg.Column{{Name: "Id", Type: iceberg.ColumnTypeString}},
	}
	fields := []salesforce.Field{
		{Name: "Id", Type: "id"},
		{Name: "NewField", Type: "string", Nillable: true},
	}
	sf := &fakeSF{
		describeResult: fields,
		records:        []salesforce.Record{{"Id": "001", "NewField": "hello"}},
	}

	r := makeRunner(Config{BatchSize: 10}, sf, ice, newFakeState())

	if err := r.runOne(context.Background(), iceberg.ObjectConfig{Name: "Obj"}); err != nil {
		t.Fatalf("runOne: %v", err)
	}
	// EvolveSchema should have added NewField.
	found := false
	for _, c := range ice.schema {
		if c.Name == "NewField" {
			found = true
		}
	}
	if !found {
		t.Error("expected NewField to be added via EvolveSchema")
	}
}

func TestRunOne_bulkQueryError(t *testing.T) {
	fields := []salesforce.Field{{Name: "Id", Type: "id"}}
	sf := &fakeSF{describeResult: fields, queryErr: errors.New("SF job failed")}

	r := makeRunner(Config{BatchSize: 10}, sf, &fakeWriter{}, newFakeState())

	err := r.runOne(context.Background(), iceberg.ObjectConfig{Name: "Obj"})
	if err == nil {
		t.Fatal("expected error from BulkQuery, got nil")
	}
}

func TestRunOne_zeroRecords(t *testing.T) {
	// Empty result set — no writes, watermark unchanged.
	fields := []salesforce.Field{{Name: "Id", Type: "id"}, {Name: "Ts", Type: "datetime"}}
	sf := &fakeSF{describeResult: fields, records: nil}
	ice := &fakeWriter{}
	state := newFakeState()
	state.watermarks["Obj"] = "2024-01-01T00:00:00.000+0000"

	r := makeRunner(Config{BatchSize: 10}, sf, ice, state)

	if err := r.runOne(context.Background(), iceberg.ObjectConfig{
		Name:           "Obj",
		WatermarkField: "Ts",
	}); err != nil {
		t.Fatalf("runOne: %v", err)
	}
	if len(ice.batches) != 0 {
		t.Errorf("expected 0 writes for empty result set, got %d", len(ice.batches))
	}
	if got := state.watermarks["Obj"]; got != "2024-01-01T00:00:00.000+0000" {
		t.Errorf("watermark changed for zero-record result: %q", got)
	}
}

func TestRunAll_runsAllObjects(t *testing.T) {
	objects := make([]iceberg.ObjectConfig, 7)
	for i := range objects {
		objects[i] = iceberg.ObjectConfig{Name: fmt.Sprintf("Obj%d", i)}
	}

	fields := []salesforce.Field{{Name: "Id", Type: "id"}}
	sf := &fakeSF{describeResult: fields}
	ice := &fakeWriter{}
	state := newFakeState()
	reg := iceberg.NewRegistry(objects)

	r := NewRunner(Config{BatchSize: 10}, reg, sf, ice, state, &fakeEncoder{}, slog.Default())
	if err := r.RunAll(context.Background()); err != nil {
		t.Fatalf("RunAll: %v", err)
	}
	// Each object with no records should produce 0 batches total.
	if len(ice.batches) != 0 {
		t.Errorf("expected 0 batches for all-empty objects, got %d", len(ice.batches))
	}
}
