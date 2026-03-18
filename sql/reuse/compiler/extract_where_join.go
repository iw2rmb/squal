package compiler

import (
	"regexp"
	"strings"
)

func (qd *QueryDecomposer) extractWhereComponent(sql string) *QueryComponent {
	// Use pre-compiled regex
	matches := wherePattern.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return nil
	}

	whereClause := matches[1]

	// Enhanced filter parsing to handle complex conditions
	filters := []FilterExpr{}

	// Parse multiple patterns for better condition extraction
	condMatches := conditionPattern.FindAllStringSubmatch(whereClause, -1)
	for _, match := range condMatches {
		if len(match) >= 3 {
			column := strings.TrimSpace(match[1])
			operator := strings.TrimSpace(match[2])
			value := ""
			if len(match) >= 4 {
				value = strings.Trim(strings.TrimSpace(match[3]), "'\"")
			}

			filters = append(filters, FilterExpr{
				Column:   column,
				Operator: operator,
				Value:    value,
			})
		}
	}

	// Additional parsing for BETWEEN conditions
	betweenPattern := regexp.MustCompile(`(?i)(\w+(?:\.\w+)?)\s+BETWEEN\s+([^A]+?)\s+AND\s+([^A]+?)(?:\s+AND|\s+OR|$)`)
	betweenMatches := betweenPattern.FindAllStringSubmatch(whereClause, -1)
	for _, match := range betweenMatches {
		if len(match) >= 4 {
			filters = append(filters, FilterExpr{
				Column:   strings.TrimSpace(match[1]),
				Operator: "BETWEEN",
				Value:    strings.TrimSpace(match[2]) + " AND " + strings.TrimSpace(match[3]),
			})
		}
	}

	// Additional parsing for IS NULL conditions
	isNullPattern := regexp.MustCompile(`(?i)(\w+(?:\.\w+)?)\s+IS\s+(NOT\s+)?NULL`)
	isNullMatches := isNullPattern.FindAllStringSubmatch(whereClause, -1)
	for _, match := range isNullMatches {
		if len(match) >= 2 {
			operator := "IS NULL"
			if len(match) >= 3 && strings.TrimSpace(match[2]) != "" {
				operator = "IS NOT NULL"
			}
			filters = append(filters, FilterExpr{
				Column:   strings.TrimSpace(match[1]),
				Operator: operator,
				Value:    "",
			})
		}
	}

	// If no filters parsed, create a generic one
	if len(filters) == 0 {
		filters = append(filters, FilterExpr{
			Column:   "generic",
			Operator: "=",
			Value:    whereClause,
		})
	}

	component := &QueryComponent{
		Type:      WHERE_COMPONENT,
		Filters:   filters,
		Signature: qd.generateSignature("WHERE", []string{whereClause}),
	}

	return component
}

func (qd *QueryDecomposer) extractJoinComponent(sql string) *QueryComponent {
	// Use pre-compiled regex
	matches := joinPattern.FindAllStringSubmatch(sql, -1)
	if len(matches) == 0 {
		return nil
	}

	// Collect all joined tables
	tables := []string{}
	for _, match := range matches {
		if len(match) >= 3 {
			table := strings.TrimSpace(match[2])
			if table != "" {
				tables = append(tables, table)
			}
		}
	}

	if len(tables) < 2 {
		return nil
	}

	component := &QueryComponent{
		Type:      JOIN_COMPONENT,
		Tables:    tables,
		Signature: qd.generateSignature("JOIN", tables),
	}

	return component
}
