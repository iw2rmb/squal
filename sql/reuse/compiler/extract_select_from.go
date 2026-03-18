package compiler

import "strings"

func (qd *QueryDecomposer) extractSelectComponent(sql string) *QueryComponent {
	// Use pre-compiled regex for better performance
	matches := selectPattern.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return nil
	}

	selectClause := matches[1]
	columns := []string{}

	// Parse columns (simplified - doesn't handle all cases)
	parts := strings.Split(selectClause, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		// Remove aliases for now
		if strings.Contains(part, " as ") {
			part = strings.Split(part, " as ")[0]
		}
		if strings.Contains(part, " AS ") {
			part = strings.Split(part, " AS ")[0]
		}
		// Extract column name
		if !strings.Contains(part, "(") { // Not an aggregation
			// Handle table.column format
			if strings.Contains(part, ".") {
				part = strings.Split(part, ".")[1]
			}
			columns = append(columns, strings.TrimSpace(part))
		}
	}

	if len(columns) == 0 {
		return nil
	}

	component := &QueryComponent{
		Type:      SELECT_COMPONENT,
		Columns:   columns,
		Signature: qd.generateSignature("SELECT", columns),
	}

	return component
}

func (qd *QueryDecomposer) extractFromComponent(sql string) *QueryComponent {
	// Use pre-compiled regex
	matches := fromPattern.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return nil
	}

	fromClause := strings.TrimSpace(matches[1])

	// Parse multiple tables (comma-separated)
	var tables []string

	// Check if it's multiple tables separated by commas
	if strings.Contains(fromClause, ",") {
		tableMatches := multiTablePattern.FindAllStringSubmatch(fromClause, -1)
		for _, match := range tableMatches {
			if len(match) >= 2 {
				table := strings.TrimSpace(match[1])
				if table != "" {
					tables = append(tables, table)
				}
			}
		}
	} else {
		// Single table, handle alias
		table := fromClause
		if strings.Contains(table, " ") {
			table = strings.Split(table, " ")[0]
		}
		tables = []string{table}
	}

	if len(tables) == 0 {
		return nil
	}

	component := &QueryComponent{
		Type:      FROM_COMPONENT,
		Tables:    tables,
		Signature: qd.generateSignature("FROM", tables),
	}

	return component
}
