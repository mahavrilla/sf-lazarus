package iceberg

import (
	"testing"
)

func TestDetectDrift_noChange(t *testing.T) {
	cols := []Column{
		{Name: "Id", Type: ColumnTypeString},
		{Name: "Amount", Type: ColumnTypeDouble},
	}
	report := DetectDrift(cols, cols)
	if !report.IsEmpty() {
		t.Fatalf("expected empty report, got %+v", report)
	}
}

func TestDetectDrift_newColumn(t *testing.T) {
	current := []Column{{Name: "Id", Type: ColumnTypeString}}
	desired := []Column{
		{Name: "Id", Type: ColumnTypeString},
		{Name: "CreatedDate", Type: ColumnTypeTimestamp},
	}
	report := DetectDrift(current, desired)
	if len(report.NewColumns) != 1 {
		t.Fatalf("expected 1 new column, got %d", len(report.NewColumns))
	}
	if report.NewColumns[0].Name != "CreatedDate" {
		t.Fatalf("unexpected new column name: %s", report.NewColumns[0].Name)
	}
	if len(report.RemovedColumns) != 0 {
		t.Fatalf("expected no removed columns")
	}
}

func TestDetectDrift_removedColumn(t *testing.T) {
	current := []Column{
		{Name: "Id", Type: ColumnTypeString},
		{Name: "OldField", Type: ColumnTypeString},
	}
	desired := []Column{{Name: "Id", Type: ColumnTypeString}}
	report := DetectDrift(current, desired)
	if len(report.RemovedColumns) != 1 {
		t.Fatalf("expected 1 removed column, got %d", len(report.RemovedColumns))
	}
	if report.RemovedColumns[0].Name != "OldField" {
		t.Fatalf("unexpected removed column name: %s", report.RemovedColumns[0].Name)
	}
}

func TestDetectDrift_typeChange(t *testing.T) {
	current := []Column{{Name: "Score", Type: ColumnTypeInt}}
	desired := []Column{{Name: "Score", Type: ColumnTypeLong}}
	report := DetectDrift(current, desired)
	if len(report.TypeChanges) != 1 {
		t.Fatalf("expected 1 type change, got %d", len(report.TypeChanges))
	}
	tc := report.TypeChanges[0]
	if tc.Name != "Score" || tc.OldType != ColumnTypeInt || tc.NewType != ColumnTypeLong {
		t.Fatalf("unexpected type change: %+v", tc)
	}
}
