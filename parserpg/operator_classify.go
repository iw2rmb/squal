//go:build cgo
// +build cgo

package parserpg

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func (p *PGQueryParser) isAggregateFunction(name string) bool {
	aggregates := map[string]bool{
		"count": true, "sum": true, "avg": true, "min": true, "max": true,
		"array_agg": true, "string_agg": true, "json_agg": true, "jsonb_agg": true,
		"bool_and": true, "bool_or": true, "every": true,
		"stddev": true, "stddev_pop": true, "stddev_samp": true,
		"variance": true, "var_pop": true, "var_samp": true,
	}
	return aggregates[strings.ToLower(name)]
}

func (p *PGQueryParser) isArrayFunction(name string) bool {
	arrayFuncs := map[string]bool{
		"array_agg": true, "array_append": true, "array_cat": true, "array_prepend": true,
		"array_remove": true, "array_replace": true, "array_length": true, "array_dims": true,
		"array_lower": true, "array_upper": true, "unnest": true,
	}
	return arrayFuncs[strings.ToLower(name)]
}

func (p *PGQueryParser) isJSONBOperator(aExpr *pg_query.A_Expr) bool {
	if aExpr == nil {
		return false
	}
	for _, nameNode := range aExpr.Name {
		if str := nameNode.GetString_(); str != nil {
			switch str.Sval {
			case "->", "->>", "#>", "#>>", "@>", "<@", "?", "?|", "?&", "||", "-", "#-":
				return true
			}
		}
	}
	return false
}

func (p *PGQueryParser) isILikeOperator(aExpr *pg_query.A_Expr) bool {
	if aExpr == nil {
		return false
	}
	for _, nameNode := range aExpr.Name {
		if str := nameNode.GetString_(); str != nil {
			if strings.ToLower(str.Sval) == "~~*" || strings.ToLower(str.Sval) == "ilike" {
				return true
			}
		}
	}
	return false
}

func (p *PGQueryParser) stringInSlice(str string, slice []string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}
