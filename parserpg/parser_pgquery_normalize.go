//go:build cgo
// +build cgo

package parserpg

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// NormalizeQuery normalizes a SQL query for consistent caching.
// This uses pg_query_go's normalization which produces a canonical representation
// regardless of formatting, whitespace, or case variations.
func (p *PGQueryParser) NormalizeQuery(sql string) (string, error) {
	result, err := pg_query.Normalize(sql)
	if err != nil {
		return "", err
	}
	return result, nil
}
