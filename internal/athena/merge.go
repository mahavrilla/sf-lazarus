package athena

import (
	"context"
	"fmt"
	"strings"
)

// MergeCurrentState merges the latest state of each record from rawTable into
// currentTable using Athena MERGE INTO on Iceberg. Rows are matched on "id";
// all other columns in the provided list are updated or inserted.
//
// columns must be the full column list from the raw table schema (lowercase,
// as returned by GlueWriter.GetSchema). The synthetic ROW_NUMBER column is
// never included in the output.
func (c *Client) MergeCurrentState(ctx context.Context, rawTable, currentTable string, columns []string) error {
	colList := strings.Join(columns, ", ")

	setClauses := make([]string, 0, len(columns))
	insertVals := make([]string, 0, len(columns))
	for _, col := range columns {
		insertVals = append(insertVals, "s."+col)
		if col != "id" {
			setClauses = append(setClauses, fmt.Sprintf("t.%s = s.%s", col, col))
		}
	}

	query := fmt.Sprintf(`MERGE INTO "%s"."%s" AS t
USING (
  WITH ranked AS (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY systemmodstamp DESC) AS rn
    FROM "%s"."%s"
  )
  SELECT %s FROM ranked WHERE rn = 1
) AS s ON t.id = s.id
WHEN MATCHED AND s.isdeleted = 'true'  THEN DELETE
WHEN MATCHED AND s.isdeleted = 'false' THEN UPDATE SET
  %s
WHEN NOT MATCHED AND s.isdeleted = 'false' THEN INSERT (%s) VALUES (%s)`,
		c.cfg.Database, currentTable,
		c.cfg.Database, rawTable,
		colList,
		strings.Join(setClauses, ",\n  "),
		colList,
		strings.Join(insertVals, ", "),
	)

	_, err := c.Execute(ctx, query)
	if err != nil {
		return fmt.Errorf("merge %s into %s: %w", rawTable, currentTable, err)
	}
	return nil
}
