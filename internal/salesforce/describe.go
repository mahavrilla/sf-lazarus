package salesforce

import (
	"context"
	"fmt"
)

// Field is a single field returned by the Salesforce Describe API.
type Field struct {
	Name      string
	Type      string // e.g. "string", "double", "boolean", "datetime", "date", "int", "id", "reference", "base64"
	Length    int
	Precision int
	Scale     int
	Nillable  bool
}

// Describer fetches field metadata for a Salesforce object.
type Describer interface {
	Describe(ctx context.Context, objectName string) ([]Field, error)
}

// sfDescribeResponse is the relevant slice of the /sobjects/{obj}/describe payload.
type sfDescribeResponse struct {
	Fields []sfField `json:"fields"`
}

type sfField struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Length    int    `json:"length"`
	Precision int    `json:"precision"`
	Scale     int    `json:"scale"`
	Nillable  bool   `json:"nillable"`
}

// Describe fetches metadata for objectName and returns its field list.
func (c *Client) Describe(ctx context.Context, objectName string) ([]Field, error) {
	path := fmt.Sprintf("/sobjects/%s/describe", objectName)
	var resp sfDescribeResponse
	if err := c.getJSON(ctx, path, &resp); err != nil {
		return nil, fmt.Errorf("describe %s: %w", objectName, err)
	}

	fields := make([]Field, len(resp.Fields))
	for i, f := range resp.Fields {
		fields[i] = Field{
			Name:      f.Name,
			Type:      f.Type,
			Length:    f.Length,
			Precision: f.Precision,
			Scale:     f.Scale,
			Nillable:  f.Nillable,
		}
	}
	return fields, nil
}
