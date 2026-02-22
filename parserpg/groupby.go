//go:build cgo
// +build cgo

package parserpg

import (
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/iw2rmb/squal/parser"
)

// ExtractGroupBy returns ordered GROUP BY items from a query.
// It resolves positional references (GROUP BY 1) to the corresponding SELECT target
// alias or column, and provides the raw expression kind for each item.
//
// What: Canonicalizes GROUP BY items into structured items.
// How: Reads pg_query AST, resolves aliases/positions, renders simple expr markers.
func (p *PGQueryParser) ExtractGroupBy(sql string) ([]parser.GroupItem, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}
	items := []parser.GroupItem{}
	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}
		sel := stmt.Stmt.GetSelectStmt()
		if sel == nil || sel.GroupClause == nil {
			continue
		}
		for _, groupNode := range sel.GroupClause {
			if groupNode == nil {
				continue
			}
			if item := p.extractGroupItem(groupNode, sel.TargetList); item != nil {
				items = append(items, *item)
			}
		}
	}
	return items, nil
}

// extractGroupItem extracts a single GroupItem from a GROUP BY node.
func (p *PGQueryParser) extractGroupItem(node *pg_query.Node, targetList []*pg_query.Node) *parser.GroupItem {
	if node == nil {
		return nil
	}
	// Positional reference
	if ac := node.GetAConst(); ac != nil {
		if ival := ac.GetIval(); ival != nil {
			pos := int(ival.Ival)
			if pos >= 1 && targetList != nil && pos <= len(targetList) {
				rt := targetList[pos-1].GetResTarget()
				if rt == nil {
					return &parser.GroupItem{Kind: "positional", Position: pos}
				}
				if rt.Name != "" {
					return &parser.GroupItem{Kind: "positional", Position: pos, Alias: rt.Name}
				}
				if rt.Val != nil {
					if cr := rt.Val.GetColumnRef(); cr != nil {
						col, table := p.extractColumnFromRef(cr)
						return &parser.GroupItem{Kind: "positional", Position: pos, Column: col, Table: table}
					}
					return &parser.GroupItem{Kind: "positional", Position: pos, RawExpr: p.nodeToExprString(rt.Val)}
				}
			}
			return &parser.GroupItem{Kind: "positional", Position: pos}
		}
	}

	// Column reference
	if cr := node.GetColumnRef(); cr != nil {
		col, table := p.extractColumnFromRef(cr)
		if col != "" {
			if alias := p.findMatchingAlias(col, table, targetList); alias != "" {
				return &parser.GroupItem{Kind: "alias", Alias: alias}
			}
			return &parser.GroupItem{Kind: "column", Column: col, Table: table}
		}
	}

	// Function call
	if fc := node.GetFuncCall(); fc != nil {
		return &parser.GroupItem{Kind: "function", RawExpr: p.nodeToExprString(node)}
	}

	// Complex expression
	return &parser.GroupItem{Kind: "expression", RawExpr: p.nodeToExprString(node)}
}

// extractColumnFromRef extracts column and table names from a ColumnRef node.
func (p *PGQueryParser) extractColumnFromRef(cr *pg_query.ColumnRef) (column, table string) {
	if cr == nil || len(cr.Fields) == 0 {
		return "", ""
	}
	if len(cr.Fields) == 1 {
		if s := cr.Fields[0].GetString_(); s != nil {
			return s.Sval, ""
		}
		return "", ""
	}
	if s := cr.Fields[0].GetString_(); s != nil {
		table = s.Sval
	}
	if s := cr.Fields[len(cr.Fields)-1].GetString_(); s != nil {
		column = s.Sval
	}
	return column, table
}

// findMatchingAlias returns the alias of a SELECT target that matches a given column/table.
func (p *PGQueryParser) findMatchingAlias(column, table string, targetList []*pg_query.Node) string {
	for _, target := range targetList {
		rt := target.GetResTarget()
		if rt == nil || rt.Val == nil || rt.Name == "" {
			continue
		}
		if cr := rt.Val.GetColumnRef(); cr != nil {
			col, tbl := p.extractColumnFromRef(cr)
			if col == column && tbl == table {
				return rt.Name
			}
		}
	}
	return ""
}

// nodeToExprString produces a compact printable representation of a node (used for group-by expr markers).
func (p *PGQueryParser) nodeToExprString(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	if fc := node.GetFuncCall(); fc != nil {
		if len(fc.Funcname) > 0 {
			if s := fc.Funcname[0].GetString_(); s != nil {
				name := strings.ToLower(s.Sval)
				switch name {
				case "date_trunc":
					return "date_trunc(...)"
				case "extract":
					return "extract(...)"
				}
				return name + "(...)"
			}
		}
	}
	if ac := node.GetAConst(); ac != nil {
		if sv := ac.GetSval(); sv != nil {
			return "'" + sv.Sval + "'"
		}
		if ival := ac.GetIval(); ival != nil {
			return strconv.FormatInt(int64(ival.Ival), 10)
		}
		if fval := ac.GetFval(); fval != nil {
			return fval.Fval
		}
	}
	if cr := node.GetColumnRef(); cr != nil {
		if len(cr.Fields) == 1 {
			if s := cr.Fields[0].GetString_(); s != nil {
				return s.Sval
			}
		} else {
			var parts []string
			for _, f := range cr.Fields {
				if s := f.GetString_(); s != nil {
					parts = append(parts, s.Sval)
				}
			}
			return strings.Join(parts, ".")
		}
	}
	return "<expr>"
}
