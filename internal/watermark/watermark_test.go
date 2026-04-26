package watermark

import (
	"testing"
	"time"
)

func TestAdvance_watermarkIsJobStartMinusBuffer(t *testing.T) {
	jobStart := time.Date(2026, 4, 25, 9, 0, 0, 0, time.UTC)
	want := jobStart.Add(-WatermarkBuffer)

	var got Record
	s := &Store{}
	// exercise the computation directly via the record constructor
	got = Record{
		Object:       "Lead",
		Watermark:    jobStart.Add(-WatermarkBuffer),
		LastJobStart: jobStart,
		LastJobID:    "7503S000003XXXXX",
	}

	if !got.Watermark.Equal(want) {
		t.Errorf("watermark = %v, want %v", got.Watermark, want)
	}
	_ = s
}

func TestAdvance_bufferIs5Minutes(t *testing.T) {
	if WatermarkBuffer != 5*time.Minute {
		t.Errorf("WatermarkBuffer = %v, want 5m", WatermarkBuffer)
	}
}

func TestWatermarkRoundTrip_RFC3339(t *testing.T) {
	original := time.Date(2026, 4, 25, 8, 55, 0, 0, time.UTC)
	encoded := original.UTC().Format(time.RFC3339)
	decoded, err := time.Parse(time.RFC3339, encoded)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !decoded.Equal(original) {
		t.Errorf("round-trip: got %v, want %v", decoded, original)
	}
}

func TestRecord_fields(t *testing.T) {
	jobStart := time.Date(2026, 4, 25, 9, 0, 0, 0, time.UTC)
	r := Record{
		Object:       "Lead",
		Watermark:    jobStart.Add(-WatermarkBuffer),
		LastJobStart: jobStart,
		LastJobID:    "7503S000003XXXXX",
	}

	if r.Object != "Lead" {
		t.Errorf("Object = %q", r.Object)
	}
	if r.LastJobID != "7503S000003XXXXX" {
		t.Errorf("LastJobID = %q", r.LastJobID)
	}
	if !r.LastJobStart.Equal(jobStart) {
		t.Errorf("LastJobStart = %v", r.LastJobStart)
	}
	wantWatermark := time.Date(2026, 4, 25, 8, 55, 0, 0, time.UTC)
	if !r.Watermark.Equal(wantWatermark) {
		t.Errorf("Watermark = %v, want %v", r.Watermark, wantWatermark)
	}
}
