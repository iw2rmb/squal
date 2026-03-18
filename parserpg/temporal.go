//go:build cgo
// +build cgo

package parserpg

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/iw2rmb/squall/parser"
)

// ExtractTemporalOps analyzes temporal operations in a query using the PostgreSQL AST.
// It detects NOW()/CURRENT_TIMESTAMP, DATE_TRUNC, and time-based WHERE ranges.
//
// What: Surfaces time references for incremental scheduling and windowing.
// How: Scans select targets and WHERE clause, recognizing common temporal shapes.
func (p *PGQueryParser) ExtractTemporalOps(sql string) (*parser.TemporalOps, error) {
	ops := &parser.TemporalOps{HasNow: false, HasDateTrunc: false, WhereRanges: []parser.TimeRange{}}
	err := forEachSelectStmt(sql, func(sel *pg_query.SelectStmt) {
		for _, target := range sel.TargetList {
			if resTarget := target.GetResTarget(); resTarget != nil {
				p.checkExprForTemporal(resTarget.Val, ops)
			}
		}
		if sel.GroupClause != nil {
			for _, groupItem := range sel.GroupClause {
				p.checkExprForTemporal(groupItem, ops)
			}
		}
		if sel.WhereClause != nil {
			p.extractTimeRanges(sel.WhereClause, ops)
		}
	})
	if err != nil {
		return nil, err
	}
	return ops, nil
}

func (p *PGQueryParser) checkExprForTemporal(node *pg_query.Node, ops *parser.TemporalOps) {
	if node == nil || ops == nil {
		return
	}
	if fc := node.GetFuncCall(); fc != nil {
		if len(fc.Funcname) > 0 {
			if s := fc.Funcname[0].GetString_(); s != nil {
				name := strings.ToLower(s.Sval)
				if name == "now" {
					ops.HasNow = true
				}
				if name == "date_trunc" {
					ops.HasDateTrunc = true
				}
			}
		}
		for _, arg := range fc.Args {
			p.checkExprForTemporal(arg, ops)
		}
		return
	}
	if svf := node.GetSqlvalueFunction(); svf != nil {
		if svf.Op == pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP ||
			svf.Op == pg_query.SQLValueFunctionOp_SVFOP_CURRENT_DATE ||
			svf.Op == pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIME {
			ops.HasNow = true
		}
		return
	}
	if sub := node.GetSubLink(); sub != nil {
		if sub.Subselect != nil {
			// best effort: look into subselect SELECT targets
			if sel := sub.Subselect.GetSelectStmt(); sel != nil {
				for _, t := range sel.TargetList {
					if rt := t.GetResTarget(); rt != nil {
						p.checkExprForTemporal(rt.Val, ops)
					}
				}
			}
		}
		return
	}
}

func (p *PGQueryParser) extractTimeRanges(node *pg_query.Node, ops *parser.TemporalOps) {
	if node == nil || ops == nil {
		return
	}
	if be := node.GetBoolExpr(); be != nil {
		// Traverse all args (AND/OR), aggregate ranges
		for _, arg := range be.Args {
			p.extractTimeRanges(arg, ops)
		}
		return
	}
	if ae := node.GetAExpr(); ae != nil {
		op := ""
		if len(ae.Name) > 0 {
			if s := ae.Name[0].GetString_(); s != nil {
				op = s.Sval
			}
		}
		switch strings.ToUpper(op) {
		case ">", ">=", "<", "<=":
			if tr := p.extractComparisonTimeRange(ae, op); tr != nil {
				ops.WhereRanges = append(ops.WhereRanges, *tr)
			}
			if p.hasNowInNode(ae.Lexpr) || p.hasNowInNode(ae.Rexpr) {
				ops.HasNow = true
			}
		case "BETWEEN":
			if tr := p.extractBetweenTimeRange(ae); tr != nil {
				ops.WhereRanges = append(ops.WhereRanges, *tr)
			}
		}
		return
	}
}

// hasNowInNode detects NOW() or CURRENT_* in the subtree.
func (p *PGQueryParser) hasNowInNode(node *pg_query.Node) bool {
	if node == nil {
		return false
	}
	if p.isNowFunction(node) {
		return true
	}
	if ae := node.GetAExpr(); ae != nil {
		return p.hasNowInNode(ae.Lexpr) || p.hasNowInNode(ae.Rexpr)
	}
	if be := node.GetBoolExpr(); be != nil {
		for _, a := range be.Args {
			if p.hasNowInNode(a) {
				return true
			}
		}
	}
	if fc := node.GetFuncCall(); fc != nil {
		for _, a := range fc.Args {
			if p.hasNowInNode(a) {
				return true
			}
		}
	}
	if svf := node.GetSqlvalueFunction(); svf != nil {
		return svf.Op == pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP ||
			svf.Op == pg_query.SQLValueFunctionOp_SVFOP_CURRENT_DATE ||
			svf.Op == pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIME
	}
	return false
}

func (p *PGQueryParser) extractComparisonTimeRange(ae *pg_query.A_Expr, op string) *parser.TimeRange {
	if ae == nil {
		return nil
	}
	column, table := "", ""
	if cr := ae.Lexpr.GetColumnRef(); cr != nil {
		column, table = p.extractColumnFromRef(cr)
	} else if cr := ae.Rexpr.GetColumnRef(); cr != nil {
		column, table = p.extractColumnFromRef(cr)
		switch op {
		case ">":
			op = "<"
		case "<":
			op = ">"
		case ">=":
			op = "<="
		case "<=":
			op = ">="
		}
	}
	if column == "" {
		return nil
	}
	rexpr := ae.Rexpr
	if ae.Rexpr.GetColumnRef() != nil {
		rexpr = ae.Lexpr
	}
	if rexprAe := rexpr.GetAExpr(); rexprAe != nil {
		rexprOp := ""
		if len(rexprAe.Name) > 0 {
			if s := rexprAe.Name[0].GetString_(); s != nil {
				rexprOp = s.Sval
			}
		}
		if rexprOp == "-" {
			if fc := rexprAe.Lexpr.GetFuncCall(); fc != nil {
				funcName := ""
				if len(fc.Funcname) > 0 {
					if s := fc.Funcname[0].GetString_(); s != nil {
						funcName = strings.ToLower(s.Sval)
					}
				}
				if funcName == "now" {
					if interval := p.extractInterval(rexprAe.Rexpr); interval != "" {
						return &parser.TimeRange{Column: column, Table: table, Kind: "now_minus_interval", Interval: interval, Operator: op}
					}
				}
			}
		}
	}
	if ac := rexpr.GetAConst(); ac != nil {
		if sval := ac.GetSval(); sval != nil {
			timestamp := sval.Sval
			if strings.Contains(timestamp, "-") || strings.Contains(timestamp, ":") {
				return &parser.TimeRange{Column: column, Table: table, Kind: "greater_than", Operator: op, StartTime: timestamp}
			}
		}
	}
	return nil
}

// extractBetweenTimeRange extracts time ranges from BETWEEN expressions.
func (p *PGQueryParser) extractBetweenTimeRange(ae *pg_query.A_Expr) *parser.TimeRange {
	if ae == nil {
		return nil
	}
	column, table := "", ""
	if cr := ae.Lexpr.GetColumnRef(); cr != nil {
		column, table = p.extractColumnFromRef(cr)
	}
	if column == "" {
		return nil
	}
	if list := ae.Rexpr.GetList(); list != nil && len(list.Items) == 2 {
		startTime, endTime := "", ""
		if ac := list.Items[0].GetAConst(); ac != nil {
			if sval := ac.GetSval(); sval != nil {
				startTime = sval.Sval
			}
		}
		if ac := list.Items[1].GetAConst(); ac != nil {
			if sval := ac.GetSval(); sval != nil {
				endTime = sval.Sval
			}
		}
		if startTime != "" && endTime != "" {
			return &parser.TimeRange{Column: column, Table: table, Kind: "between", StartTime: startTime, EndTime: endTime}
		}
	}
	return nil
}

// extractInterval extracts the interval string from an INTERVAL expression node.
func (p *PGQueryParser) extractInterval(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	if tc := node.GetTypeCast(); tc != nil {
		if ac := tc.Arg.GetAConst(); ac != nil {
			if sval := ac.GetSval(); sval != nil {
				return sval.Sval
			}
		}
	}
	if ac := node.GetAConst(); ac != nil {
		if sval := ac.GetSval(); sval != nil {
			return sval.Sval
		}
	}
	return ""
}

// extractNowMinusInterval checks if a node represents NOW() - INTERVAL 'duration'.
func (p *PGQueryParser) extractNowMinusInterval(node *pg_query.Node) (interval, referenceSQL string) {
	if node == nil {
		return "", ""
	}
	ae := node.GetAExpr()
	if ae == nil {
		return "", ""
	}
	op := ""
	if len(ae.Name) > 0 {
		if s := ae.Name[0].GetString_(); s != nil {
			op = s.Sval
		}
	}
	if op != "-" {
		return "", ""
	}
	if !p.isNowFunction(ae.Lexpr) {
		return "", ""
	}
	intervalStr := p.extractInterval(ae.Rexpr)
	if intervalStr == "" {
		return "", ""
	}
	nowFunc := p.getNowFunctionName(ae.Lexpr)
	refSQL := nowFunc + " - INTERVAL '" + intervalStr + "'"
	return intervalStr, refSQL
}

func (p *PGQueryParser) isNowFunction(node *pg_query.Node) bool {
	if node == nil {
		return false
	}
	if fc := node.GetFuncCall(); fc != nil {
		if len(fc.Funcname) > 0 {
			if s := fc.Funcname[len(fc.Funcname)-1].GetString_(); s != nil {
				return strings.ToLower(s.Sval) == "now"
			}
		}
	}
	if svf := node.GetSqlvalueFunction(); svf != nil {
		return svf.Op == pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP ||
			svf.Op == pg_query.SQLValueFunctionOp_SVFOP_CURRENT_DATE ||
			svf.Op == pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIME
	}
	return false
}

func (p *PGQueryParser) getNowFunctionName(node *pg_query.Node) string {
	if node == nil {
		return "NOW()"
	}
	if fc := node.GetFuncCall(); fc != nil {
		if len(fc.Funcname) > 0 {
			if s := fc.Funcname[len(fc.Funcname)-1].GetString_(); s != nil {
				return strings.ToUpper(s.Sval) + "()"
			}
		}
	}
	if svf := node.GetSqlvalueFunction(); svf != nil {
		switch svf.Op {
		case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP:
			return "CURRENT_TIMESTAMP"
		case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_DATE:
			return "CURRENT_DATE"
		case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIME:
			return "CURRENT_TIME"
		}
	}
	return "NOW()"
}

func (p *PGQueryParser) swapOperator(op string) string {
	switch op {
	case ">":
		return "<"
	case ">=":
		return "<="
	case "<":
		return ">"
	case "<=":
		return ">="
	default:
		return op
	}
}
