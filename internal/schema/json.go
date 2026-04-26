// Package schema handles encoding and decoding of Salesforce field maps for
// the backfill pipeline.
//
// JSONCodec is a development-grade implementation that encodes records as JSON.
// Replace with a protobuf implementation backed by a schema registry for
// production use.
package schema

import (
	"encoding/json"
	"fmt"

	"github.com/mahavrilla/sf-lazarus/internal/iceberg"
)

// JSONCodec implements both backfill.Encoder and iceberg.Codec using JSON.
//
// Encode produces a JSON object whose keys are field names and values are
// the raw Salesforce strings. Decode reverses that into a map[string]any
// that the GlueWriter's Arrow conversion can consume.
type JSONCodec struct{}

// Encode serialises the mapped Salesforce fields as a JSON object.
// The cols parameter is accepted for interface compatibility; the JSON
// encoding does not rely on column type information.
func (c *JSONCodec) Encode(record map[string]string, _ []iceberg.Column) ([]byte, error) {
	b, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("json encode record: %w", err)
	}
	return b, nil
}

// Decode deserialises protobuf/JSON bytes back into a map[string]any for
// the GlueWriter's Arrow builder. Values are returned as strings (the
// GlueWriter's coerce helpers handle the final type conversion).
func (c *JSONCodec) Decode(data []byte, _ []iceberg.Column) (map[string]any, error) {
	// Unmarshal as map[string]any; JSON strings remain strings, which is
	// correct — the coerce helpers in glue_writer.go accept string inputs.
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("json decode record: %w", err)
	}
	return m, nil
}
