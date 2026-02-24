//go:build cgo
// +build cgo

package parserpg

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// forEachSelectStmt parses sql and calls fn for every top-level SelectStmt.
func forEachSelectStmt(sql string, fn func(sel *pg_query.SelectStmt)) error {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return err
	}
	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}
		if sel := stmt.Stmt.GetSelectStmt(); sel != nil {
			fn(sel)
		}
	}
	return nil
}
