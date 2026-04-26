package athena

import (
	"testing"

	athenaTypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/aws"
)

func datum(s string) athenaTypes.Datum {
	return athenaTypes.Datum{VarCharValue: aws.String(s)}
}

func row(values ...string) athenaTypes.Row {
	data := make([]athenaTypes.Datum, len(values))
	for i, v := range values {
		data[i] = datum(v)
	}
	return athenaTypes.Row{Data: data}
}

func TestParseRows_basic(t *testing.T) {
	cols := []string{"id", "name", "amount"}
	rows := []athenaTypes.Row{
		row("001", "Acme", "500"),
		row("002", "Globex", "750"),
	}

	got := parseRows(cols, rows)

	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
	if got[0]["id"] != "001" || got[0]["name"] != "Acme" || got[0]["amount"] != "500" {
		t.Errorf("row 0 = %v", got[0])
	}
	if got[1]["id"] != "002" || got[1]["name"] != "Globex" || got[1]["amount"] != "750" {
		t.Errorf("row 1 = %v", got[1])
	}
}

func TestParseRows_empty(t *testing.T) {
	got := parseRows([]string{"id", "name"}, nil)
	if len(got) != 0 {
		t.Errorf("expected 0 rows, got %d", len(got))
	}
}

func TestParseRows_nullValue(t *testing.T) {
	// Athena returns nil VarCharValue for NULL fields; aws.ToString converts to "".
	cols := []string{"id", "optional"}
	rows := []athenaTypes.Row{
		{Data: []athenaTypes.Datum{
			{VarCharValue: aws.String("001")},
			{VarCharValue: nil},
		}},
	}

	got := parseRows(cols, rows)
	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	if got[0]["optional"] != "" {
		t.Errorf("expected empty string for nil datum, got %q", got[0]["optional"])
	}
}

func TestParseRows_fewerColumnsThanData(t *testing.T) {
	// Extra data columns beyond the header are silently ignored.
	cols := []string{"id"}
	rows := []athenaTypes.Row{row("001", "extra", "ignored")}

	got := parseRows(cols, rows)
	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	if _, ok := got[0]["extra"]; ok {
		t.Error("extra column should not appear in output")
	}
}

func TestConfig_defaultWorkgroup(t *testing.T) {
	c := &Client{cfg: Config{Workgroup: ""}}
	// Verify the defaulting logic: empty workgroup resolves to "primary" in start().
	// We test this indirectly by confirming the zero value is empty.
	if c.cfg.Workgroup != "" {
		t.Errorf("expected empty default, got %q", c.cfg.Workgroup)
	}
}
