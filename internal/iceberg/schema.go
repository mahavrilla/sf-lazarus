package iceberg

import (
	"github.com/mahavrilla/sf-lazarus/internal/salesforce"
)

// ColumnType is the Iceberg primitive type for a table column.
type ColumnType string

const (
	ColumnTypeString    ColumnType = "string"
	ColumnTypeLong      ColumnType = "long"
	ColumnTypeInt       ColumnType = "int"
	ColumnTypeDouble    ColumnType = "double"
	ColumnTypeFloat     ColumnType = "float"
	ColumnTypeBoolean   ColumnType = "boolean"
	ColumnTypeDate      ColumnType = "date"
	ColumnTypeTimestamp ColumnType = "timestamp"
	ColumnTypeBinary    ColumnType = "binary"
)

// Column is one column in an Iceberg table.
type Column struct {
	Name     string
	Type     ColumnType
	Required bool
}

// compoundSFTypes are Salesforce field types that the Bulk API 2.0 cannot
// query. Their scalar sub-fields (e.g. BillingStreet, BillingCity) appear
// separately in the describe response and are queryable individually.
var compoundSFTypes = map[string]struct{}{
	"address":      {},
	"location":     {},
	"complexvalue": {},
}

// sfTypeToColumnType maps Salesforce field types to Iceberg column types.
// Unmapped types default to string.
var sfTypeToColumnType = map[string]ColumnType{
	"id":              ColumnTypeString,
	"reference":       ColumnTypeString,
	"string":          ColumnTypeString,
	"textarea":        ColumnTypeString,
	"picklist":        ColumnTypeString,
	"multipicklist":   ColumnTypeString,
	"combobox":        ColumnTypeString,
	"email":           ColumnTypeString,
	"phone":           ColumnTypeString,
	"url":             ColumnTypeString,
	"encryptedstring": ColumnTypeString,
	"int":             ColumnTypeLong,
	"double":          ColumnTypeDouble,
	"currency":        ColumnTypeDouble,
	"percent":         ColumnTypeDouble,
	"boolean":         ColumnTypeBoolean,
	"date":            ColumnTypeDate,
	"datetime":        ColumnTypeTimestamp,
	"base64":          ColumnTypeBinary,
}

// ColumnsFromDescribe converts Salesforce field metadata to Iceberg columns,
// skipping any field whose name appears in excludeFields.
func ColumnsFromDescribe(fields []salesforce.Field, excludeFields []string) []Column {
	excluded := make(map[string]struct{}, len(excludeFields))
	for _, f := range excludeFields {
		excluded[f] = struct{}{}
	}

	cols := make([]Column, 0, len(fields))
	for _, f := range fields {
		if _, skip := excluded[f.Name]; skip {
			continue
		}
		if _, compound := compoundSFTypes[f.Type]; compound {
			continue
		}
		colType, ok := sfTypeToColumnType[f.Type]
		if !ok {
			colType = ColumnTypeString
		}
		cols = append(cols, Column{
			Name:     f.Name,
			Type:     colType,
			Required: !f.Nillable,
		})
	}
	return cols
}
