package restore

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// fakeQuerier captures the last query submitted and returns a fixed result.
type fakeQuerier struct {
	rows    []map[string]string
	err     error
	lastSQL string
}

func (f *fakeQuerier) Execute(_ context.Context, query string) ([]map[string]string, error) {
	f.lastSQL = query
	return f.rows, f.err
}

var testTime = time.Date(2026, 4, 25, 9, 0, 0, 0, time.UTC)

func newService(q *fakeQuerier) *Service {
	return NewService(q, slog.Default())
}

// ── validation ────────────────────────────────────────────────────────────────

func TestLastGoodState_missingObjectType(t *testing.T) {
	_, err := newService(&fakeQuerier{}).LastGoodState(context.Background(), Options{
		Before: testTime,
	})
	if err == nil {
		t.Fatal("expected error for empty ObjectType")
	}
}

func TestLastGoodState_missingBefore(t *testing.T) {
	_, err := newService(&fakeQuerier{}).LastGoodState(context.Background(), Options{
		ObjectType: "Lead",
	})
	if err == nil {
		t.Fatal("expected error for zero Before")
	}
}

func TestLastGoodState_invalidRecordID(t *testing.T) {
	_, err := newService(&fakeQuerier{}).LastGoodState(context.Background(), Options{
		ObjectType: "Lead",
		RecordID:   "not-a-sf-id",
		Before:     testTime,
	})
	if err == nil {
		t.Fatal("expected error for invalid RecordID")
	}
}

// ── query routing ─────────────────────────────────────────────────────────────

func TestLastGoodState_singleRecord_queriesCorrectTable(t *testing.T) {
	q := &fakeQuerier{rows: []map[string]string{{"id": "001C000000XyzABC"}}}
	_, err := newService(q).LastGoodState(context.Background(), Options{
		ObjectType: "Lead",
		RecordID:   "001C000000XyzABC",
		Before:     testTime,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(q.lastSQL, "Lead") {
		t.Errorf("expected table Lead in query, got:\n%s", q.lastSQL)
	}
	if !strings.Contains(q.lastSQL, "001C000000XyzABC") {
		t.Errorf("expected record ID in query, got:\n%s", q.lastSQL)
	}
	if !strings.Contains(q.lastSQL, "LIMIT 1") {
		t.Errorf("expected LIMIT 1 in single-record query, got:\n%s", q.lastSQL)
	}
}

func TestLastGoodState_fullObject_usesRowNumber(t *testing.T) {
	q := &fakeQuerier{rows: []map[string]string{}}
	_, err := newService(q).LastGoodState(context.Background(), Options{
		ObjectType: "Lead",
		Before:     testTime,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(q.lastSQL, "ROW_NUMBER()") {
		t.Errorf("full-object query should use ROW_NUMBER(), got:\n%s", q.lastSQL)
	}
	if strings.Contains(q.lastSQL, "DISTINCT ON") {
		t.Errorf("DISTINCT ON is PostgreSQL-only and must not appear in Athena query")
	}
}

func TestLastGoodState_beforeTimestamp_inQuery(t *testing.T) {
	q := &fakeQuerier{rows: []map[string]string{}}
	_, err := newService(q).LastGoodState(context.Background(), Options{
		ObjectType: "Account",
		Before:     testTime,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(q.lastSQL, "2026-04-25T09:00:00Z") {
		t.Errorf("expected RFC3339 timestamp in query, got:\n%s", q.lastSQL)
	}
}

// ── result handling ───────────────────────────────────────────────────────────

func TestLastGoodState_stripsRnColumn(t *testing.T) {
	q := &fakeQuerier{rows: []map[string]string{
		{"id": "001", "name": "Acme", "rn": "1"},
		{"id": "002", "name": "Globex", "rn": "1"},
	}}
	rows, err := newService(q).LastGoodState(context.Background(), Options{
		ObjectType: "Lead",
		Before:     testTime,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, row := range rows {
		if _, ok := row["rn"]; ok {
			t.Error("rn column should be stripped from results")
		}
	}
}

func TestLastGoodState_propagatesQueryError(t *testing.T) {
	q := &fakeQuerier{err: errors.New("athena timeout")}
	_, err := newService(q).LastGoodState(context.Background(), Options{
		ObjectType: "Lead",
		Before:     testTime,
	})
	if err == nil {
		t.Fatal("expected error from querier")
	}
	if !strings.Contains(err.Error(), "athena timeout") {
		t.Errorf("expected underlying error in message, got: %v", err)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func TestTableFor(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"Lead", "Lead"},
		{"Account", "Account"},
		{"Contact", "Contact"},
	}
	for _, c := range cases {
		got := tableFor(c.input)
		if got != c.want {
			t.Errorf("tableFor(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

func TestSFIDPattern(t *testing.T) {
	valid := []string{
		"001C000000XyzABC",  // 16-char example from plan
		"001C000000XyzAB",   // 15-char standard SF ID
		"7503S000003XXXXX",  // 16-char example from plan
		"001C000000XyzABCXX", // 18-char standard SF ID
	}
	invalid := []string{
		"not-a-sf-id",       // hyphens
		"has spaces in it!", // spaces and punctuation
		"dropit;SELECT 1",   // SQL injection attempt
		"",                  // empty
	}
	for _, id := range valid {
		if !sfIDPattern.MatchString(id) {
			t.Errorf("expected %q to be valid SF ID", id)
		}
	}
	for _, id := range invalid {
		if sfIDPattern.MatchString(id) {
			t.Errorf("expected %q to be invalid SF ID", id)
		}
	}
}
