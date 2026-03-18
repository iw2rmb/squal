package compiler

import (
	"regexp"
	"strings"
)

func (qd *QueryDecomposer) extractGroupByComponent(sql string) *QueryComponent {
	// Use pre-compiled regex
	matches := groupByPattern.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return nil
	}

	groupByClause := matches[1]
	columns := []string{}

	// Parse GROUP BY columns
	parts := strings.Split(groupByClause, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		// Handle table.column format
		if strings.Contains(part, ".") {
			part = strings.Split(part, ".")[1]
		}
		columns = append(columns, part)
	}

	if len(columns) == 0 {
		return nil
	}

	component := &QueryComponent{
		Type:      GROUP_BY_COMPONENT,
		Columns:   columns,
		Signature: qd.generateSignature("GROUP_BY", columns),
	}

	return component
}

func (qd *QueryDecomposer) extractAggregationComponents(sql string) []QueryComponent {
	// Use pre-compiled regex
	matches := aggPattern.FindAllStringSubmatch(sql, -1)

	if len(matches) == 0 {
		return []QueryComponent{}
	}

	var aggregations []AggExpr
	var signatures []string

	for _, match := range matches {
		if len(match) > 1 {
			function := strings.ToUpper(match[1])
			// Extract column from function call
			columnRegex := regexp.MustCompile(`\(([^)]+)\)`)
			colMatch := columnRegex.FindStringSubmatch(match[0])
			column := ""
			if len(colMatch) > 1 {
				column = strings.TrimSpace(colMatch[1])
				// Remove DISTINCT if present
				column = strings.TrimPrefix(column, "DISTINCT ")
				// Handle table.column
				if strings.Contains(column, ".") {
					column = strings.Split(column, ".")[1]
				}
			}

			alias := ""
			if len(match) > 2 {
				alias = match[2]
			}

			agg := AggExpr{
				Function: function,
				Column:   column,
				Alias:    alias,
			}

			aggregations = append(aggregations, agg)
			signatures = append(signatures, function+"_"+column)
		}
	}

	if len(aggregations) == 0 {
		return []QueryComponent{}
	}

	// Create single component with all aggregations
	component := QueryComponent{
		Type:         AGG_COMPONENT,
		Aggregations: aggregations,
		Signature:    qd.generateSignature("AGG", signatures),
	}

	return []QueryComponent{component}
}

func (qd *QueryDecomposer) extractOrderByComponent(sql string) *QueryComponent {
	// Use pre-compiled regex
	matches := orderByPattern.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return nil
	}

	orderByClause := strings.TrimSpace(matches[1])

	// Parse ORDER BY columns
	var columns []string
	columnParts := strings.Split(orderByClause, ",")
	for _, part := range columnParts {
		part = strings.TrimSpace(part)
		// Remove DESC/ASC
		part = strings.Fields(part)[0]
		if part != "" {
			columns = append(columns, part)
		}
	}

	component := &QueryComponent{
		Type:      ORDER_BY_COMPONENT,
		Columns:   columns,
		Signature: qd.generateSignature("ORDER_BY", columns),
	}

	return component
}

func (qd *QueryDecomposer) extractLimitComponent(sql string) *QueryComponent {
	// Use pre-compiled regex
	matches := limitPattern.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return nil
	}

	limit := matches[1]
	var offset string
	if len(matches) >= 3 && matches[2] != "" {
		offset = matches[2]
	}

	values := []string{limit}
	if offset != "" {
		values = append(values, offset)
	}

	component := &QueryComponent{
		Type:      LIMIT_COMPONENT,
		Signature: qd.generateSignature("LIMIT", values),
	}

	return component
}
