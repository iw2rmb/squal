//go:build cgo
// +build cgo

package parserpg

import (
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/iw2rmb/squal/parser"
)

// ExtractCaseAggregates parses SELECT targets and extracts SUM/COUNT with CASE expressions.
// Supported patterns:
//   - SUM(CASE WHEN <pred> THEN <col|const> ELSE <const> END)
//   - COUNT(CASE WHEN <pred> THEN 1 END)
//
// What: Detects CASE-in-aggregate patterns for incremental strategies.
// How: Walks SELECT targets; for FuncCall nodes tries to parse a CASE shape.
func (p *PGQueryParser) ExtractCaseAggregates(sql string) ([]parser.AggCase, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}
	out := []parser.AggCase{}
	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}
		sel := stmt.Stmt.GetSelectStmt()
		if sel == nil || sel.TargetList == nil {
			continue
		}
		for _, target := range sel.TargetList {
			rt := target.GetResTarget()
			if rt == nil || rt.Val == nil {
				continue
			}
			if fc := rt.Val.GetFuncCall(); fc != nil {
				if ac, ok := p.parseCaseAggregateFromFunc(fc); ok {
					ac.Alias = rt.Name
					out = append(out, *ac)
				}
			}
		}
	}
	return out, nil
}

// ExtractAggregateCompositions flattens +/- expressions over SUM/COUNT function calls in SELECT targets.
func (p *PGQueryParser) ExtractAggregateCompositions(sql string) ([]parser.AggComposition, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}
	out := []parser.AggComposition{}
	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}
		sel := stmt.Stmt.GetSelectStmt()
		if sel == nil || sel.TargetList == nil {
			continue
		}
		for _, target := range sel.TargetList {
			rt := target.GetResTarget()
			if rt == nil || rt.Val == nil || rt.Name == "" {
				continue
			}
			terms, ok := p.parseAggTerms(rt.Val, +1)
			if ok && len(terms) > 0 {
				out = append(out, parser.AggComposition{Alias: rt.Name, Terms: terms})
			}
		}
	}
	return out, nil
}

// ExtractAggregates returns all aggregate functions (COUNT/SUM/AVG/MIN/MAX) from SELECT targets.
// Supported aggregate functions: COUNT, SUM, AVG, MIN, MAX.
// For COUNT(*), Column will be "*". DISTINCT is detected and reported.
func (p *PGQueryParser) ExtractAggregates(sql string) ([]parser.Aggregate, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}
	var aggregates []parser.Aggregate
	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}
		sel := stmt.Stmt.GetSelectStmt()
		if sel == nil || sel.TargetList == nil {
			continue
		}
		for _, target := range sel.TargetList {
			rt := target.GetResTarget()
			if rt == nil || rt.Val == nil {
				continue
			}
			if agg := p.extractAggregateFromNode(rt.Val, rt.Name); agg != nil {
				aggregates = append(aggregates, *agg)
			}
		}
	}
	return aggregates, nil
}

// extractAggregateFromNode extracts a single aggregate from a node if present.
func (p *PGQueryParser) extractAggregateFromNode(node *pg_query.Node, alias string) *parser.Aggregate {
	if node == nil {
		return nil
	}
	if fc := node.GetFuncCall(); fc != nil {
		if len(fc.Funcname) == 0 {
			return nil
		}
		fname := ""
		if s := fc.Funcname[0].GetString_(); s != nil {
			fname = strings.ToUpper(s.Sval)
		}
		switch fname {
		case "COUNT", "SUM", "AVG", "MIN", "MAX":
			agg := &parser.Aggregate{Func: fname, Alias: alias}
			agg.Distinct = fc.AggDistinct
			if len(fc.Args) == 0 {
				if fname == "COUNT" {
					agg.Column = "*"
					return agg
				}
				return nil
			}
			arg := fc.Args[0]
			if arg.GetAStar() != nil {
				agg.Column = "*"
				return agg
			}
			if cr := arg.GetColumnRef(); cr != nil && len(cr.Fields) > 0 {
				if len(cr.Fields) == 1 {
					if s := cr.Fields[0].GetString_(); s != nil {
						agg.Column = s.Sval
					}
				} else {
					if s := cr.Fields[0].GetString_(); s != nil {
						agg.Table = s.Sval
					}
					if s := cr.Fields[len(cr.Fields)-1].GetString_(); s != nil {
						agg.Column = s.Sval
					}
				}
				return agg
			}
			return agg
		}
	}
	return nil
}

// parseAggTerms recursively parses an expression into aggregate terms with signs.
func (p *PGQueryParser) parseAggTerms(node *pg_query.Node, sign int) ([]parser.AggTerm, bool) {
	if node == nil {
		return nil, false
	}
	if ae := node.GetAExpr(); ae != nil {
		op := ""
		if len(ae.Name) > 0 {
			if s := ae.Name[0].GetString_(); s != nil {
				op = s.Sval
			}
		}
		if op == "+" || op == "-" {
			left, okL := p.parseAggTerms(ae.Lexpr, sign)
			right, okR := p.parseAggTerms(ae.Rexpr, ifThen(op == "+", sign, -sign))
			if okL && okR {
				return append(left, right...), true
			}
			if okL {
				return left, true
			}
			if okR {
				return right, true
			}
		}
		return nil, false
	}
	if fc := node.GetFuncCall(); fc != nil {
		if ac, ok := p.parseCaseAggregateFromFunc(fc); ok {
			term := parser.AggTerm{Sign: sign, Func: ac.Func, Case: ac}
			return []parser.AggTerm{term}, true
		}
		if len(fc.Funcname) > 0 {
			fname := ""
			if s := fc.Funcname[0].GetString_(); s != nil {
				fname = strings.ToUpper(s.Sval)
			}
			if fname == "SUM" && len(fc.Args) == 1 {
				if cr := fc.Args[0].GetColumnRef(); cr != nil && len(cr.Fields) > 0 {
					col := ""
					if s := cr.Fields[len(cr.Fields)-1].GetString_(); s != nil {
						col = s.Sval
					}
					if col != "" {
						return []parser.AggTerm{{Sign: sign, Func: "SUM", Column: col}}, true
					}
				}
			}
		}
	}
	return nil, false
}

func ifThen(cond bool, a, b int) int {
	if cond {
		return a
	}
	return b
}

func (p *PGQueryParser) parseCaseAggregateFromFunc(fc *pg_query.FuncCall) (*parser.AggCase, bool) {
	if fc == nil || len(fc.Funcname) == 0 {
		return nil, false
	}
	fname := ""
	if s := fc.Funcname[0].GetString_(); s != nil {
		fname = strings.ToUpper(s.Sval)
	}
	if fname != "SUM" && fname != "COUNT" {
		return nil, false
	}
	if len(fc.Args) == 0 {
		return nil, false
	}
	ce := fc.Args[0].GetCaseExpr()
	if ce == nil || len(ce.Args) == 0 {
		return nil, false
	}
	when := ce.Args[0].GetCaseWhen()
	if when == nil {
		return nil, false
	}
	conds := p.parseCaseConditions(when.Expr)
	if len(conds) == 0 {
		return nil, false
	}
	ac := &parser.AggCase{Func: fname, Conditions: conds}
	if tc, ok, isConst, constVal := p.parseThen(when.Result); ok {
		if isConst {
			ac.ThenConst = &constVal
		} else {
			ac.ThenColumn = tc
		}
	} else {
		return nil, false
	}
	if fname == "SUM" {
		if ce.Defresult == nil {
			zero := 0.0
			ac.ElseConst = &zero
		} else {
			if val, ok := p.constToFloat(ce.Defresult); ok {
				ac.ElseConst = &val
			} else {
				zero := 0.0
				ac.ElseConst = &zero
			}
		}
		if ac.ThenConst != nil && *ac.ThenConst == 1 {
			ac.Func = "COUNT"
		}
	}
	return ac, true
}

func (p *PGQueryParser) parseCaseConditions(node *pg_query.Node) []parser.AggCondition {
	if node == nil {
		return nil
	}
	if be := node.GetBoolExpr(); be != nil && be.Boolop == pg_query.BoolExprType_AND_EXPR {
		all := []parser.AggCondition{}
		for _, arg := range be.Args {
			all = append(all, p.parseCaseConditions(arg)...)
		}
		return all
	}
	if ae := node.GetAExpr(); ae != nil {
		op := ""
		if len(ae.Name) > 0 {
			if s := ae.Name[0].GetString_(); s != nil {
				op = strings.ToUpper(s.Sval)
			}
		}
		if op == "AND" {
			left := p.parseCaseConditions(ae.Lexpr)
			right := p.parseCaseConditions(ae.Rexpr)
			if len(left) > 0 || len(right) > 0 {
				return append(left, right...)
			}
		}
		if ae.Kind == pg_query.A_Expr_Kind_AEXPR_IN || (ae.Kind == pg_query.A_Expr_Kind_AEXPR_OP && op == "IN") {
			if col, ok := p.columnRefToNames(ae.Lexpr); ok {
				vals := []interface{}{}
				if list := ae.Rexpr.GetList(); list != nil {
					for _, it := range list.Items {
						if v, ok2 := p.constNodeToValue(it); ok2 {
							vals = append(vals, v)
						}
					}
				} else if arr := ae.Rexpr.GetArrayExpr(); arr != nil {
					for _, el := range arr.Elements {
						if v, ok2 := p.constNodeToValue(el); ok2 {
							vals = append(vals, v)
						}
					}
				} else if arr2 := ae.Rexpr.GetAArrayExpr(); arr2 != nil {
					for _, el := range arr2.Elements {
						if v, ok2 := p.constNodeToValue(el); ok2 {
							vals = append(vals, v)
						}
					}
				}
				if len(vals) > 0 {
					return []parser.AggCondition{{Kind: "in", Column: parser.ColumnRef{Table: col.table, Column: col.column}, Values: vals}}
				}
			}
			return nil
		}
		if op == "=" {
			if col, ok := p.columnRefToNames(ae.Lexpr); ok {
				if v, ok2 := p.constNodeToValue(ae.Rexpr); ok2 {
					return []parser.AggCondition{{Kind: "eq", Column: parser.ColumnRef{Table: col.table, Column: col.column}, Value: v}}
				}
			}
			if col, ok := p.columnRefToNames(ae.Rexpr); ok {
				if v, ok2 := p.constNodeToValue(ae.Lexpr); ok2 {
					return []parser.AggCondition{{Kind: "eq", Column: parser.ColumnRef{Table: col.table, Column: col.column}, Value: v}}
				}
			}
		}
	}
	if sae := node.GetScalarArrayOpExpr(); sae != nil {
		if len(sae.Args) == 2 {
			if col, ok := p.columnRefToNames(sae.Args[0]); ok {
				vals := []interface{}{}
				if arr := sae.Args[1].GetArrayExpr(); arr != nil {
					for _, el := range arr.Elements {
						if v, ok2 := p.constNodeToValue(el); ok2 {
							vals = append(vals, v)
						}
					}
				} else if arr2 := sae.Args[1].GetAArrayExpr(); arr2 != nil {
					for _, el := range arr2.Elements {
						if v, ok2 := p.constNodeToValue(el); ok2 {
							vals = append(vals, v)
						}
					}
				}
				if len(vals) > 0 {
					return []parser.AggCondition{{Kind: "in", Column: parser.ColumnRef{Table: col.table, Column: col.column}, Values: vals}}
				}
			}
		}
	}
	return nil
}

func (p *PGQueryParser) parseThen(node *pg_query.Node) (col string, ok bool, isConst bool, constVal float64) {
	if node == nil {
		return "", false, false, 0
	}
	if cr := node.GetColumnRef(); cr != nil {
		if len(cr.Fields) > 0 {
			if s := cr.Fields[len(cr.Fields)-1].GetString_(); s != nil {
				return s.Sval, true, false, 0
			}
		}
	}
	if v, ok := p.constNodeToValue(node); ok {
		switch t := v.(type) {
		case int64:
			return "", true, true, float64(t)
		case string:
			if f, err := strconv.ParseFloat(t, 64); err == nil {
				return "", true, true, f
			}
		}
	}
	return "", false, false, 0
}

func (p *PGQueryParser) constToFloat(node *pg_query.Node) (float64, bool) {
	if node == nil {
		return 0, false
	}
	if c := node.GetAConst(); c != nil {
		if v := c.GetFval(); v != nil {
			if f, err := strconv.ParseFloat(v.Fval, 64); err == nil {
				return f, true
			}
		}
		if v := c.GetIval(); v != nil {
			return float64(v.Ival), true
		}
		if v := c.GetSval(); v != nil {
			if f, err := strconv.ParseFloat(v.Sval, 64); err == nil {
				return f, true
			}
		}
	}
	return 0, false
}
