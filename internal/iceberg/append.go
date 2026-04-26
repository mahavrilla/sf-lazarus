package iceberg

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// appendValue appends a single decoded value to the appropriate Arrow builder.
func appendValue(fb array.Builder, ct ColumnType, v any) error {
	if v == nil {
		fb.AppendNull()
		return nil
	}
	// Salesforce represents null non-string fields as empty strings.
	if s, ok := v.(string); ok && s == "" && ct != ColumnTypeString {
		fb.AppendNull()
		return nil
	}

	switch ct {
	case ColumnTypeString:
		b, ok := fb.(*array.StringBuilder)
		if !ok {
			return fmt.Errorf("type mismatch for string column: got %T", fb)
		}
		s, err := toString(v)
		if err != nil {
			return err
		}
		b.Append(s)

	case ColumnTypeLong:
		b, ok := fb.(*array.Int64Builder)
		if !ok {
			return fmt.Errorf("type mismatch for long column: got %T", fb)
		}
		n, err := toInt64(v)
		if err != nil {
			return err
		}
		b.Append(n)

	case ColumnTypeInt:
		b, ok := fb.(*array.Int32Builder)
		if !ok {
			return fmt.Errorf("type mismatch for int column: got %T", fb)
		}
		n, err := toInt32(v)
		if err != nil {
			return err
		}
		b.Append(n)

	case ColumnTypeDouble:
		b, ok := fb.(*array.Float64Builder)
		if !ok {
			return fmt.Errorf("type mismatch for double column: got %T", fb)
		}
		f, err := toFloat64(v)
		if err != nil {
			return err
		}
		b.Append(f)

	case ColumnTypeFloat:
		b, ok := fb.(*array.Float32Builder)
		if !ok {
			return fmt.Errorf("type mismatch for float column: got %T", fb)
		}
		f, err := toFloat32(v)
		if err != nil {
			return err
		}
		b.Append(f)

	case ColumnTypeBoolean:
		b, ok := fb.(*array.BooleanBuilder)
		if !ok {
			return fmt.Errorf("type mismatch for boolean column: got %T", fb)
		}
		bv, err := toBool(v)
		if err != nil {
			return err
		}
		b.Append(bv)

	case ColumnTypeDate:
		b, ok := fb.(*array.Date32Builder)
		if !ok {
			return fmt.Errorf("type mismatch for date column: got %T", fb)
		}
		d, err := toDate32(v)
		if err != nil {
			return err
		}
		b.Append(d)

	case ColumnTypeTimestamp:
		b, ok := fb.(*array.TimestampBuilder)
		if !ok {
			return fmt.Errorf("type mismatch for timestamp column: got %T", fb)
		}
		ts, err := toTimestamp(v)
		if err != nil {
			return err
		}
		b.Append(ts)

	case ColumnTypeBinary:
		b, ok := fb.(*array.BinaryBuilder)
		if !ok {
			return fmt.Errorf("type mismatch for binary column: got %T", fb)
		}
		bs, err := toBytes(v)
		if err != nil {
			return err
		}
		b.Append(bs)

	default:
		return fmt.Errorf("unsupported column type %q", ct)
	}
	return nil
}
