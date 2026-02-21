//go:build cgo
// +build cgo

package parserpg

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// ExtractTables extracts table names from a SQL query.
// Phase 3: robust implementation that walks the parse tree to find all table references.
func (p *PGQueryParser) ExtractTables(sql string) ([]string, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}

	tables := make(map[string]bool)
	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}
		p.extractTablesFromNode(stmt.Stmt, tables)
	}

	// Convert map to slice
	result := make([]string, 0, len(tables))
	for table := range tables {
		result = append(result, table)
	}
	return result, nil
}

// extractTablesFromNode recursively extracts table references from a node
func (p *PGQueryParser) extractTablesFromNode(node *pg_query.Node, tables map[string]bool) {
	if node == nil {
		return
	}

	// Handle SELECT statements
	if selectStmt := node.GetSelectStmt(); selectStmt != nil {
		// Process FROM clause
		for _, fromItem := range selectStmt.FromClause {
			p.extractTablesFromFromClause(fromItem, tables)
		}
		// Process CTEs (extract tables from CTE definitions, not the CTE names themselves)
		if selectStmt.WithClause != nil {
			for _, cte := range selectStmt.WithClause.Ctes {
				if commonTableExpr := cte.GetCommonTableExpr(); commonTableExpr != nil {
					if commonTableExpr.Ctequery != nil {
						p.extractTablesFromNode(commonTableExpr.Ctequery, tables)
					}
				}
			}
		}
		// Process WHERE clause for subqueries
		if selectStmt.WhereClause != nil {
			p.extractTablesFromExpression(selectStmt.WhereClause, tables)
		}
		return
	}

	// Handle INSERT statements
	if insertStmt := node.GetInsertStmt(); insertStmt != nil {
		if insertStmt.Relation != nil {
			if rangeVar := insertStmt.Relation; rangeVar != nil {
				tables[rangeVar.Relname] = true
			}
		}
		// Process SELECT in INSERT
		if insertStmt.SelectStmt != nil {
			p.extractTablesFromNode(insertStmt.SelectStmt, tables)
		}
		return
	}

	// Handle UPDATE statements
	if updateStmt := node.GetUpdateStmt(); updateStmt != nil {
		if updateStmt.Relation != nil {
			tables[updateStmt.Relation.Relname] = true
		}
		// Process FROM clause in UPDATE
		for _, fromItem := range updateStmt.FromClause {
			p.extractTablesFromFromClause(fromItem, tables)
		}
		return
	}

	// Handle DELETE statements
	if deleteStmt := node.GetDeleteStmt(); deleteStmt != nil {
		if deleteStmt.Relation != nil {
			tables[deleteStmt.Relation.Relname] = true
		}
		// Process USING clause in DELETE
		for _, usingItem := range deleteStmt.UsingClause {
			p.extractTablesFromFromClause(usingItem, tables)
		}
		return
	}
}

// extractTablesFromFromClause extracts table names from FROM/JOIN clauses
func (p *PGQueryParser) extractTablesFromFromClause(node *pg_query.Node, tables map[string]bool) {
	if node == nil {
		return
	}

	// Handle range variables (direct table references)
	if rangeVar := node.GetRangeVar(); rangeVar != nil {
		tables[rangeVar.Relname] = true
		return
	}

	// Handle subqueries in FROM
	if rangeSubselect := node.GetRangeSubselect(); rangeSubselect != nil {
		if rangeSubselect.Subquery != nil {
			p.extractTablesFromNode(rangeSubselect.Subquery, tables)
		}
		return
	}

	// Handle JOINs
	if joinExpr := node.GetJoinExpr(); joinExpr != nil {
		p.extractTablesFromFromClause(joinExpr.Larg, tables)
		p.extractTablesFromFromClause(joinExpr.Rarg, tables)
		return
	}
}

// extractTablesFromExpression extracts table names from expressions (for subqueries in WHERE, etc.)
func (p *PGQueryParser) extractTablesFromExpression(node *pg_query.Node, tables map[string]bool) {
	if node == nil {
		return
	}

	// Handle SubLink (subqueries)
	if subLink := node.GetSubLink(); subLink != nil {
		if subLink.Subselect != nil {
			p.extractTablesFromNode(subLink.Subselect, tables)
		}
		return
	}

	// Handle boolean expressions (AND, OR, NOT)
	if boolExpr := node.GetBoolExpr(); boolExpr != nil {
		for _, arg := range boolExpr.Args {
			p.extractTablesFromExpression(arg, tables)
		}
		return
	}

	// Handle A_Expr (comparisons, operators)
	if aExpr := node.GetAExpr(); aExpr != nil {
		if aExpr.Lexpr != nil {
			p.extractTablesFromExpression(aExpr.Lexpr, tables)
		}
		if aExpr.Rexpr != nil {
			p.extractTablesFromExpression(aExpr.Rexpr, tables)
		}
		return
	}
}
