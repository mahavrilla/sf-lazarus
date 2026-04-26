package salesforce

import (
	"context"
	"strings"
	"testing"
)

func TestRecordsToCSV_columnOrder(t *testing.T) {
	records := []Record{
		{"Id": "001", "Name": "Acme", "Amount": "500"},
		{"Id": "002", "Name": "Globex", "Amount": "750"},
	}
	data, err := recordsToCSV(records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines (header + 2 rows), got %d:\n%s", len(lines), string(data))
	}

	// Headers must be sorted alphabetically.
	if lines[0] != "Amount,Id,Name" {
		t.Errorf("header line = %q, want %q", lines[0], "Amount,Id,Name")
	}
	if lines[1] != "500,001,Acme" {
		t.Errorf("row 0 = %q, want %q", lines[1], "500,001,Acme")
	}
	if lines[2] != "750,002,Globex" {
		t.Errorf("row 1 = %q, want %q", lines[2], "750,002,Globex")
	}
}

func TestRecordsToCSV_missingFieldWrittenAsEmpty(t *testing.T) {
	// Second record has no Amount field.
	records := []Record{
		{"Id": "001", "Amount": "100"},
		{"Id": "002"},
	}
	data, err := recordsToCSV(records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	// header: Amount,Id; row0: 100,001; row1: ,002
	if lines[2] != ",002" {
		t.Errorf("missing field row = %q, want %q", lines[2], ",002")
	}
}

func TestRecordsToCSV_quotedValues(t *testing.T) {
	records := []Record{
		{"Id": "001", "Desc": "has, comma"},
		{"Id": "002", "Desc": "has \"quotes\""},
	}
	data, err := recordsToCSV(records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Round-trip through csv.Reader to verify encoding is valid.
	r := strings.NewReader(string(data))
	ch := make(chan Record, 10)
	n, err := parseCSVPage(context.Background(), r, ch)
	if err != nil {
		t.Fatalf("round-trip parse error: %v", err)
	}
	if n != 2 {
		t.Fatalf("round-trip: expected 2 records, got %d", n)
	}
	close(ch)
	recs := make([]Record, 0, 2)
	for rec := range ch {
		recs = append(recs, rec)
	}
	if recs[0]["Desc"] != "has, comma" {
		t.Errorf("quoted comma: got %q", recs[0]["Desc"])
	}
	if recs[1]["Desc"] != "has \"quotes\"" {
		t.Errorf("quoted quotes: got %q", recs[1]["Desc"])
	}
}

func TestRecordsToCSV_singleRecord(t *testing.T) {
	records := []Record{{"Id": "001", "Status": "New"}}
	data, err := recordsToCSV(records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
}

func TestGetJobStatus_errorsPopulated(t *testing.T) {
	job := ingestJob{
		State:                  jobFailed,
		NumberRecordsProcessed: 5,
		NumberRecordsFailed:    2,
		ErrorMessage:           "invalid field",
	}

	status := JobStatus{
		Status:           job.State,
		RecordsProcessed: job.NumberRecordsProcessed,
	}
	if job.NumberRecordsFailed > 0 {
		status.Errors = append(status.Errors,
			"2 record(s) failed")
	}
	if job.ErrorMessage != "" {
		status.Errors = append(status.Errors, job.ErrorMessage)
	}

	if status.Status != jobFailed {
		t.Errorf("Status = %q, want %q", status.Status, jobFailed)
	}
	if status.RecordsProcessed != 5 {
		t.Errorf("RecordsProcessed = %d, want 5", status.RecordsProcessed)
	}
	if len(status.Errors) != 2 {
		t.Errorf("len(Errors) = %d, want 2", len(status.Errors))
	}
}

func TestGetJobStatus_noErrors(t *testing.T) {
	job := ingestJob{
		State:                  jobComplete,
		NumberRecordsProcessed: 10,
	}

	status := JobStatus{
		Status:           job.State,
		RecordsProcessed: job.NumberRecordsProcessed,
	}

	if len(status.Errors) != 0 {
		t.Errorf("expected no errors, got %v", status.Errors)
	}
}
