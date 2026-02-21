//go:build cgo
// +build cgo

package parserpg

import (
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/iw2rmb/sql/core"
	"github.com/iw2rmb/sql/parser"
)

// extractJoinConditionsFromFromClause walks FROM/JOIN nodes and appends join relationships
// to metadata.JoinConditions.
//
// What: Captures join types and key columns for dependency graph and invalidation.
// How: Recurses the JoinExpr tree, maps join types, and scrapes ON/USING details.
func (p *PGQueryParser) extractJoinConditionsFromFromClause(node *pg_query.Node, metadata *parser.QueryMetadata) {
	if node == nil || metadata == nil {
		return
	}
	if j := node.GetJoinExpr(); j != nil {
		// Recurse first for nested joins
		p.extractJoinConditionsFromFromClause(j.Larg, metadata)
		p.extractJoinConditionsFromFromClause(j.Rarg, metadata)

		// Join type mapping
		var jt core.JoinType
		switch j.Jointype {
		case pg_query.JoinType_JOIN_LEFT:
			jt = core.JoinTypeLeft
		case pg_query.JoinType_JOIN_RIGHT:
			jt = core.JoinTypeRight
		case pg_query.JoinType_JOIN_FULL:
			jt = core.JoinTypeFull
		case pg_query.JoinType_JOIN_INNER:
			fallthrough
		default:
			jt = core.JoinTypeInner
		}

		ltab, lalias := p.baseAndAlias(j.Larg)
		rtab, ralias := p.baseAndAlias(j.Rarg)

		var lcol, rcol string
		if j.Quals != nil {
			if lc, rc, ok := p.firstEqualityColumns(j.Quals); ok {
				if lc.table != "" {
					if lalias != "" && lc.table == lalias {
						// keep base
					} else {
						ltab = lc.table
					}
				}
				if rc.table != "" {
					if ralias != "" && rc.table == ralias {
						// keep base
					} else {
						rtab = rc.table
					}
				}
				lcol, rcol = lc.column, rc.column
			}
		}
		if lcol == "" && rcol == "" && len(j.UsingClause) > 0 {
			if s := j.UsingClause[0].GetString_(); s != nil {
				lcol, rcol = s.Sval, s.Sval
			}
		}

		jc := parser.JoinCondition{
			Type:        jt,
			LeftTable:   ltab,
			RightTable:  rtab,
			LeftColumn:  lcol,
			RightColumn: rcol,
			LeftAlias:   lalias,
			RightAlias:  ralias,
		}
		if j.Quals != nil {
			if on := p.onExprSummary(j.Quals); on != "" {
				jc.OnExpr = on
			}
		}
		metadata.JoinConditions = append(metadata.JoinConditions, jc)
		return
	}
}

// baseAndAlias returns the base table name and alias (if present) for a FROM item.
func (p *PGQueryParser) baseAndAlias(node *pg_query.Node) (string, string) {
	if node == nil {
		return "", ""
	}
	if rv := node.GetRangeVar(); rv != nil {
		base := rv.Relname
		alias := ""
		if rv.Alias != nil && rv.Alias.Aliasname != "" {
			alias = rv.Alias.Aliasname
		}
		return base, alias
	}
	if rs := node.GetRangeSubselect(); rs != nil {
		alias := ""
		if rs.Alias != nil && rs.Alias.Aliasname != "" {
			alias = rs.Alias.Aliasname
		}
		return "", alias
	}
	if j := node.GetJoinExpr(); j != nil {
		if b, a := p.baseAndAlias(j.Rarg); b != "" || a != "" {
			return b, a
		}
		return p.baseAndAlias(j.Larg)
	}
	return "", ""
}

// onExprSummary returns a compact human-readable string for a JOIN ON expression.
func (p *PGQueryParser) onExprSummary(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	if be := node.GetBoolExpr(); be != nil {
		if be.Boolop == pg_query.BoolExprType_AND_EXPR {
			parts := make([]string, 0, len(be.Args))
			for _, a := range be.Args {
				if s := p.onExprSummary(a); s != "" {
					parts = append(parts, s)
				}
			}
			return strings.Join(parts, " AND ")
		}
		if be.Boolop == pg_query.BoolExprType_NOT_EXPR && len(be.Args) > 0 {
			if s := p.onExprSummary(be.Args[0]); s != "" {
				return "NOT (" + s + ")"
			}
		}
		if be.Boolop == pg_query.BoolExprType_OR_EXPR {
			parts := make([]string, 0, len(be.Args))
			for _, a := range be.Args {
				if s := p.onExprSummary(a); s != "" {
					parts = append(parts, s)
				}
			}
			if len(parts) > 0 {
				return strings.Join(parts, " OR ")
			}
		}
		return ""
	}
	if ae := node.GetAExpr(); ae != nil {
		op := ""
		if len(ae.Name) > 0 {
			if s := ae.Name[0].GetString_(); s != nil {
				op = s.Sval
			}
		}
		ls := p.exprNodeToString(ae.Lexpr)
		rs := p.exprNodeToString(ae.Rexpr)
		if op == "" {
			return strings.TrimSpace(ls + " " + rs)
		}
		return strings.TrimSpace(ls + " " + op + " " + rs)
	}
	if nt := node.GetNullTest(); nt != nil {
		col := p.exprNodeToString(nt.Arg)
		if nt.Nulltesttype == pg_query.NullTestType_IS_NULL {
			return col + " IS NULL"
		}
		if nt.Nulltesttype == pg_query.NullTestType_IS_NOT_NULL {
			return col + " IS NOT NULL"
		}
	}
	return p.nodeToExprString(node)
}

// exprNodeToString converts a general node (column ref, const, func) to a compact string.
func (p *PGQueryParser) exprNodeToString(node *pg_query.Node) string {
	if node == nil {
		return ""
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
		return ""
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
	return p.nodeToExprString(node)
}

// firstEqualityColumns finds the first A_Expr "=" between two column references within node.
func (p *PGQueryParser) firstEqualityColumns(node *pg_query.Node) (colNames, colNames, bool) {
	if node == nil {
		return colNames{}, colNames{}, false
	}
	if be := node.GetBoolExpr(); be != nil {
		for _, arg := range be.Args {
			if l, r, ok := p.firstEqualityColumns(arg); ok {
				return l, r, ok
			}
		}
		return colNames{}, colNames{}, false
	}
	if ae := node.GetAExpr(); ae != nil {
		op := ""
		if len(ae.Name) > 0 {
			if s := ae.Name[0].GetString_(); s != nil {
				op = s.Sval
			}
		}
		if op == "=" {
			if l, okL := p.columnRefToNames(ae.Lexpr); okL {
				if r, okR := p.columnRefToNames(ae.Rexpr); okR {
					return l, r, true
				}
			}
		}
	}
	return colNames{}, colNames{}, false
}
