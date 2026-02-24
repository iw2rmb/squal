//go:build cgo
// +build cgo

package parserpg

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/iw2rmb/squal/parser"
)

// ExtractDistinctSpec analyzes DISTINCT usage in a query.
// It distinguishes between SELECT DISTINCT and COUNT(DISTINCT col).
//
// What: Reports distinct-ness at query and aggregate level.
// How: Looks at SelectStmt.DistinctClause and FuncCall with AggDistinct.
func (p *PGQueryParser) ExtractDistinctSpec(sql string) (*parser.DistinctSpec, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}
	return p.extractDistinctSpecFromTree(tree), nil
}

// extractDistinctSpecFromTree analyzes DISTINCT usage from a pre-parsed tree.
func (p *PGQueryParser) extractDistinctSpecFromTree(tree *pg_query.ParseResult) *parser.DistinctSpec {
	spec := &parser.DistinctSpec{Columns: []string{}, CountColumns: []string{}}
	forEachSelectStmtFromTree(tree, func(sel *pg_query.SelectStmt) {
		if len(sel.DistinctClause) > 0 {
			spec.HasDistinct = true
			if sel.TargetList != nil {
				for _, target := range sel.TargetList {
					rt := target.GetResTarget()
					if rt == nil || rt.Val == nil {
						continue
					}
					if rt.Val.GetColumnRef() != nil {
						cr := rt.Val.GetColumnRef()
						if len(cr.Fields) > 0 {
							if cr.Fields[0].GetAStar() != nil {
								spec.Columns = append(spec.Columns, "*")
								break
							}
							var colName string
							if len(cr.Fields) == 1 {
								if s := cr.Fields[0].GetString_(); s != nil {
									colName = s.Sval
								}
							} else {
								if s := cr.Fields[len(cr.Fields)-1].GetString_(); s != nil {
									colName = s.Sval
								}
							}
							if colName != "" {
								spec.Columns = append(spec.Columns, colName)
							}
						}
					}
				}
			}
		}
		if sel.TargetList != nil {
			for _, target := range sel.TargetList {
				rt := target.GetResTarget()
				if rt == nil || rt.Val == nil {
					continue
				}
				fc := rt.Val.GetFuncCall()
				if fc == nil {
					continue
				}
				if len(fc.Funcname) > 0 {
					fname := ""
					if s := fc.Funcname[0].GetString_(); s != nil {
						fname = strings.ToUpper(s.Sval)
					}
					if fname == "COUNT" && fc.AggDistinct && len(fc.Args) > 0 {
						spec.HasCountDistinct = true
						arg := fc.Args[0]
						if cr := arg.GetColumnRef(); cr != nil && len(cr.Fields) > 0 {
							var colName string
							if len(cr.Fields) == 1 {
								if s := cr.Fields[0].GetString_(); s != nil {
									colName = s.Sval
								}
							} else {
								if s := cr.Fields[len(cr.Fields)-1].GetString_(); s != nil {
									colName = s.Sval
								}
							}
							if colName != "" {
								spec.CountColumns = append(spec.CountColumns, colName)
							}
						}
					}
				}
			}
		}
	})
	return spec
}
