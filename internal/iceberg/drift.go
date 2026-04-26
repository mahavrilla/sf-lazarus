package iceberg

// TypeChange records a column whose Iceberg type changed between schema versions.
type TypeChange struct {
	Name    string
	OldType ColumnType
	NewType ColumnType
}

// DriftReport summarises the difference between a current and a desired schema.
type DriftReport struct {
	NewColumns     []Column
	RemovedColumns []Column
	TypeChanges    []TypeChange
}

// IsEmpty reports whether the report contains no changes.
func (d DriftReport) IsEmpty() bool {
	return len(d.NewColumns) == 0 && len(d.RemovedColumns) == 0 && len(d.TypeChanges) == 0
}

// DetectDrift compares current (live Iceberg schema) against desired (from Salesforce Describe).
// It is pure and performs no I/O.
func DetectDrift(current, desired []Column) DriftReport {
	currentByName := make(map[string]Column, len(current))
	for _, c := range current {
		currentByName[c.Name] = c
	}
	desiredByName := make(map[string]Column, len(desired))
	for _, c := range desired {
		desiredByName[c.Name] = c
	}

	var report DriftReport

	for _, d := range desired {
		c, exists := currentByName[d.Name]
		if !exists {
			report.NewColumns = append(report.NewColumns, d)
			continue
		}
		if c.Type != d.Type {
			report.TypeChanges = append(report.TypeChanges, TypeChange{
				Name:    d.Name,
				OldType: c.Type,
				NewType: d.Type,
			})
		}
	}

	for _, c := range current {
		if _, exists := desiredByName[c.Name]; !exists {
			report.RemovedColumns = append(report.RemovedColumns, c)
		}
	}

	return report
}
