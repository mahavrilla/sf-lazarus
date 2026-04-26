package iceberg

import (
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// The coerce helpers convert map[string]any values (from proto decode) to the
// concrete Go types that Arrow builders expect.  Values may arrive as their
// native Go type or as a string (e.g. from a JSON-decoded Salesforce payload).

func toString(v any) (string, error) {
	switch s := v.(type) {
	case string:
		return s, nil
	case []byte:
		return string(s), nil
	case fmt.Stringer:
		return s.String(), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

func toInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int32:
		return int64(n), nil
	case int:
		return int64(n), nil
	case float64:
		return int64(n), nil
	case string:
		return strconv.ParseInt(n, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toInt32(v any) (int32, error) {
	n, err := toInt64(v)
	if err != nil {
		return 0, err
	}
	return int32(n), nil
}

func toFloat64(v any) (float64, error) {
	switch f := v.(type) {
	case float64:
		return f, nil
	case float32:
		return float64(f), nil
	case int64:
		return float64(f), nil
	case int:
		return float64(f), nil
	case string:
		return strconv.ParseFloat(f, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func toFloat32(v any) (float32, error) {
	f, err := toFloat64(v)
	return float32(f), err
}

func toBool(v any) (bool, error) {
	switch b := v.(type) {
	case bool:
		return b, nil
	case string:
		return strconv.ParseBool(b)
	case int64:
		return b != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v)
	}
}

// toDate32 converts a value to an Arrow Date32 (days since Unix epoch).
// Accepts time.Time, int32 (days), or a "YYYY-MM-DD" string.
func toDate32(v any) (arrow.Date32, error) {
	switch d := v.(type) {
	case arrow.Date32:
		return d, nil
	case int32:
		return arrow.Date32(d), nil
	case time.Time:
		return arrow.Date32(d.Unix() / 86400), nil
	case string:
		t, err := time.Parse("2006-01-02", d)
		if err != nil {
			return 0, fmt.Errorf("parse date %q: %w", d, err)
		}
		return arrow.Date32(t.Unix() / 86400), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to Date32", v)
	}
}

// toTimestamp converts a value to an Arrow Timestamp in microseconds.
// Accepts time.Time, int64 (microseconds), or RFC3339 string.
func toTimestamp(v any) (arrow.Timestamp, error) {
	switch ts := v.(type) {
	case arrow.Timestamp:
		return ts, nil
	case int64:
		return arrow.Timestamp(ts), nil
	case time.Time:
		return arrow.Timestamp(ts.UnixMicro()), nil
	case string:
		t, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			// Try Salesforce datetime format.
			t, err = time.Parse("2006-01-02T15:04:05.000+0000", ts)
			if err != nil {
				return 0, fmt.Errorf("parse timestamp %q: %w", ts, err)
			}
		}
		return arrow.Timestamp(t.UnixMicro()), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to Timestamp", v)
	}
}

func toBytes(v any) ([]byte, error) {
	switch b := v.(type) {
	case []byte:
		return b, nil
	case string:
		return []byte(b), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to []byte", v)
	}
}
