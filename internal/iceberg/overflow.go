package iceberg

import (
	"encoding/json"
	"fmt"
)

// MappedFieldIndex maps lowercase column names to their index in a []Column slice.
type MappedFieldIndex map[string]int

// NewMappedFieldIndex builds an index from a column slice.
func NewMappedFieldIndex(cols []Column) MappedFieldIndex {
	idx := make(MappedFieldIndex, len(cols))
	for i, c := range cols {
		idx[c.Name] = i
	}
	return idx
}

// SpillToOverflow partitions a Salesforce record into mapped and overflow parts.
//
// mapped contains only the fields that have a corresponding column in idx.
// overflow is a JSON-encoded map of all remaining fields; it is nil when empty.
func SpillToOverflow(record map[string]string, idx MappedFieldIndex) (mapped map[string]string, overflow []byte, err error) {
	mapped = make(map[string]string, len(idx))
	var extra map[string]string

	for k, v := range record {
		if _, ok := idx[k]; ok {
			mapped[k] = v
		} else {
			if extra == nil {
				extra = make(map[string]string)
			}
			extra[k] = v
		}
	}

	if len(extra) == 0 {
		return mapped, nil, nil
	}

	overflow, err = json.Marshal(extra)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal overflow fields: %w", err)
	}
	return mapped, overflow, nil
}
