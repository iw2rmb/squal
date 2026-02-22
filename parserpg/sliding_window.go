//go:build cgo
// +build cgo

package parserpg

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/iw2rmb/squal/parser"
)

// DetectSlidingWindow scans WHERE for sliding windows like: column > NOW() - INTERVAL 'X'.
// What: Enables efficient periodic refresh for time-bounded queries.
// How: Uses comparison recognition + interval parsing via parser.ParseInterval.
func (p *PGQueryParser) DetectSlidingWindow(sql string) (*parser.SlidingWindowInfo, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}
	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}
		selectStmt := stmt.Stmt.GetSelectStmt()
		if selectStmt == nil || selectStmt.WhereClause == nil {
			continue
		}
		if info := p.detectSlidingWindowInNode(selectStmt.WhereClause); info != nil {
			return info, nil
		}
	}
	return nil, nil
}

func (p *PGQueryParser) detectSlidingWindowInNode(node *pg_query.Node) *parser.SlidingWindowInfo {
	if node == nil {
		return nil
	}
	if be := node.GetBoolExpr(); be != nil {
		if be.Boolop == pg_query.BoolExprType_AND_EXPR {
			for _, arg := range be.Args {
				if info := p.detectSlidingWindowInNode(arg); info != nil {
					return info
				}
			}
		}
		return nil
	}
	if ae := node.GetAExpr(); ae != nil {
		op := ""
		if len(ae.Name) > 0 {
			if s := ae.Name[0].GetString_(); s != nil {
				op = s.Sval
			}
		}
		switch op {
		case ">", ">=", "<", "<=":
			return p.extractSlidingWindowFromComparison(ae, op)
		}
	}
	return nil
}

func (p *PGQueryParser) extractSlidingWindowFromComparison(ae *pg_query.A_Expr, op string) *parser.SlidingWindowInfo {
	if ae == nil {
		return nil
	}
	if colInfo := p.tryExtractSlidingWindow(ae.Lexpr, ae.Rexpr, op); colInfo != nil {
		return colInfo
	}
	swappedOp := p.swapOperator(op)
	if colInfo := p.tryExtractSlidingWindow(ae.Rexpr, ae.Lexpr, swappedOp); colInfo != nil {
		return colInfo
	}
	return nil
}

func (p *PGQueryParser) tryExtractSlidingWindow(colNode, exprNode *pg_query.Node, op string) *parser.SlidingWindowInfo {
	col, table := p.extractColumnFromRefNode(colNode)
	if col == "" {
		return nil
	}
	intervalStr, referenceSQL := p.extractNowMinusInterval(exprNode)
	if intervalStr == "" {
		return nil
	}
	dur, err := parser.ParseInterval(intervalStr)
	if err != nil {
		return nil
	}
	return &parser.SlidingWindowInfo{Enabled: true, Column: col, Table: table, Operator: op, Interval: dur, ReferenceSQL: referenceSQL}
}
