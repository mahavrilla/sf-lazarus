package salesforce

import (
	"context"
	"strings"
	"testing"
)

func TestBuildSOQL_withWatermark(t *testing.T) {
	p := BulkQueryParams{
		Object:         "Opportunity",
		Fields:         []string{"Id", "Name", "Amount", "LastModifiedDate"},
		WatermarkField: "LastModifiedDate",
		Watermark:      "2024-01-01T00:00:00.000+0000",
	}
	got := buildSOQL(p)
	want := "SELECT Id, Name, Amount, LastModifiedDate FROM Opportunity WHERE LastModifiedDate > 2024-01-01T00:00:00.000+0000"
	if got != want {
		t.Errorf("buildSOQL mismatch\ngot:  %s\nwant: %s", got, want)
	}
}

func TestBuildSOQL_noWatermark(t *testing.T) {
	p := BulkQueryParams{
		Object: "Account",
		Fields: []string{"Id", "Name"},
	}
	got := buildSOQL(p)
	want := "SELECT Id, Name FROM Account"
	if got != want {
		t.Errorf("buildSOQL mismatch\ngot:  %s\nwant: %s", got, want)
	}
}

func TestBuildSOQL_watermarkFieldSetButValueEmpty(t *testing.T) {
	p := BulkQueryParams{
		Object:         "Account",
		Fields:         []string{"Id"},
		WatermarkField: "SystemModstamp",
		Watermark:      "", // initial load
	}
	got := buildSOQL(p)
	// No WHERE clause when Watermark is empty.
	if strings.Contains(got, "WHERE") {
		t.Errorf("expected no WHERE clause for empty watermark, got: %s", got)
	}
}

func TestParseCSVPage_happyPath(t *testing.T) {
	csv := "Id,Name,Amount\n001,Acme,100.0\n002,Globex,200.0\n"
	records := make(chan Record, 10)

	n, err := parseCSVPage(context.Background(), strings.NewReader(csv), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 records, got %d", n)
	}
	close(records)

	var got []Record
	for r := range records {
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 records in channel, got %d", len(got))
	}
	if got[0]["Name"] != "Acme" {
		t.Errorf("got[0][Name] = %q, want %q", got[0]["Name"], "Acme")
	}
	if got[1]["Amount"] != "200.0" {
		t.Errorf("got[1][Amount] = %q, want %q", got[1]["Amount"], "200.0")
	}
}

func TestParseCSVPage_empty(t *testing.T) {
	records := make(chan Record, 10)
	n, err := parseCSVPage(context.Background(), strings.NewReader(""), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 records, got %d", n)
	}
}

func TestParseCSVPage_headerOnly(t *testing.T) {
	records := make(chan Record, 10)
	n, err := parseCSVPage(context.Background(), strings.NewReader("Id,Name\n"), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 records for header-only CSV, got %d", n)
	}
}

func TestParseCSVPage_quotedFields(t *testing.T) {
	csv := "Id,Description\n001,\"has, comma\"\n002,\"has\nnewline\"\n"
	records := make(chan Record, 10)

	n, err := parseCSVPage(context.Background(), strings.NewReader(csv), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 records, got %d", n)
	}
	close(records)

	recs := make([]Record, 0, 2)
	for r := range records {
		recs = append(recs, r)
	}
	if recs[0]["Description"] != "has, comma" {
		t.Errorf("quoted comma not handled: %q", recs[0]["Description"])
	}
	if recs[1]["Description"] != "has\nnewline" {
		t.Errorf("quoted newline not handled: %q", recs[1]["Description"])
	}
}

func TestParseCSVPage_contextCancelled(t *testing.T) {
	// Build a CSV with 3 rows but cancel the context immediately.
	csv := "Id,Name\n001,A\n002,B\n003,C\n"
	records := make(chan Record) // unbuffered so the goroutine blocks

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before we start

	_, err := parseCSVPage(ctx, strings.NewReader(csv), records)
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
}
