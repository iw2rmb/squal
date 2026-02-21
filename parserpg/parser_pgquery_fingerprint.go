//go:build cgo
// +build cgo

package parserpg

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// GenerateFingerprint generates a unique fingerprint for a SQL query.
// The fingerprint is stable across queries that differ only in literal values,
// making it useful for grouping similar queries.
func (p *PGQueryParser) GenerateFingerprint(sql string) (string, error) {
	result, err := pg_query.Fingerprint(sql)
	if err != nil {
		return "", err
	}
	return result, nil
}
