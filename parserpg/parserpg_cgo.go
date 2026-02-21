//go:build cgo
// +build cgo

package parserpg

import pgquery "github.com/pganalyze/pg_query_go/v6"

// PGQueryParser is a PostgreSQL parser implementation backed by pg_query.
type PGQueryParser struct{}

// NewPGQueryParser creates the parser when CGO is enabled.
func NewPGQueryParser() (*PGQueryParser, error) {
	return &PGQueryParser{}, nil
}

// Keep a direct symbol reference so pg_query linkage remains part of the CGO build.
var _ = pgquery.Parse
