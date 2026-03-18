//go:build cgo
// +build cgo

package parserpg

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/iw2rmb/squall/parser"
)

// ExtractJSONPaths extracts all JSON/JSONB path operations from a query.
// It walks the AST to find JSON operators (->, ->>, #>, #>>) and extracts
// the column name, path components, operator, and result type.
//
// What: Structured capture of JSON path usage for cache keys and invalidation.
// How: Recognizes A_Expr nodes with JSON path operators and decodes path tokens.
func (p *PGQueryParser) ExtractJSONPaths(sql string) ([]parser.JSONPath, error) {
	var paths []parser.JSONPath
	var tableName string
	err := forEachSelectStmt(sql, func(sel *pg_query.SelectStmt) {
		if len(sel.FromClause) > 0 {
			if rt := sel.FromClause[0].GetRangeVar(); rt != nil {
				if rt.Relname != "" {
					tableName = rt.Relname
				}
			}
		}
		for _, target := range sel.TargetList {
			if rt := target.GetResTarget(); rt != nil && rt.Val != nil {
				p.extractJSONPathsFromNode(rt.Val, tableName, &paths)
			}
		}
		if sel.WhereClause != nil {
			p.extractJSONPathsFromNode(sel.WhereClause, tableName, &paths)
		}
	})
	if err != nil {
		return nil, err
	}
	return normalizeJSONPaths(sql, paths), nil
}

func (p *PGQueryParser) extractJSONPathsFromNode(node *pg_query.Node, tableName string, paths *[]parser.JSONPath) {
	if node == nil {
		return
	}
	if ae := node.GetAExpr(); ae != nil {
		op := ""
		if len(ae.Name) > 0 {
			if s := ae.Name[0].GetString_(); s != nil {
				op = s.Sval
			}
		}
		if op == "->" || op == "->>" || op == "#>" || op == "#>>" {
			if jsonPath := p.extractJSONPathFromAExpr(ae, tableName); jsonPath != nil {
				*paths = append(*paths, *jsonPath)
			}
			return
		}
		p.extractJSONPathsFromNode(ae.Lexpr, tableName, paths)
		p.extractJSONPathsFromNode(ae.Rexpr, tableName, paths)
		return
	}
	if be := node.GetBoolExpr(); be != nil {
		for _, arg := range be.Args {
			p.extractJSONPathsFromNode(arg, tableName, paths)
		}
		return
	}
	if fc := node.GetFuncCall(); fc != nil {
		for _, arg := range fc.Args {
			p.extractJSONPathsFromNode(arg, tableName, paths)
		}
		return
	}
}

func (p *PGQueryParser) extractJSONPathFromAExpr(ae *pg_query.A_Expr, tableName string) *parser.JSONPath {
	if ae == nil || len(ae.Name) == 0 {
		return nil
	}
	var operator string
	if s := ae.Name[0].GetString_(); s != nil {
		operator = s.Sval
	}
	baseCol, path := p.extractJSONBase(ae.Lexpr)
	if baseCol == "" {
		baseCol, path = p.extractJSONBase(ae.Rexpr)
	}
	if baseCol == "" {
		return nil
	}
	return &parser.JSONPath{Table: tableName, Column: baseCol, Path: path, Operator: operator, IsText: operator == "->>" || operator == "#>>"}
}

func (p *PGQueryParser) extractJSONBase(node *pg_query.Node) (string, []string) {
	if node == nil {
		return "", nil
	}
	// Left side could be chained json ops; unwind until ColumnRef
	if ae := node.GetAExpr(); ae != nil {
		// Prefer left side chain
		if col, path := p.extractJSONBase(ae.Lexpr); col != "" {
			// Append right as path token when possible
			if ac := ae.Rexpr.GetAConst(); ac != nil {
				if s := ac.GetSval(); s != nil {
					return col, append(path, s.Sval)
				}
			}
			if list := ae.Rexpr.GetList(); list != nil {
				tokens := make([]string, 0, len(list.Items))
				for _, it := range list.Items {
					if ac := it.GetAConst(); ac != nil {
						if s := ac.GetSval(); s != nil {
							tokens = append(tokens, s.Sval)
						}
					}
				}
				return col, append(path, tokens...)
			}
			return col, path
		}
		// Or right side chain
		if col, path := p.extractJSONBase(ae.Rexpr); col != "" {
			return col, path
		}
	}
	if cr := node.GetColumnRef(); cr != nil {
		if len(cr.Fields) > 0 {
			if s := cr.Fields[len(cr.Fields)-1].GetString_(); s != nil {
				return s.Sval, nil
			}
		}
	}
	return "", nil
}
