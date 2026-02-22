//go:build cgo
// +build cgo

package parserpg

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/iw2rmb/squal/core"
	"github.com/iw2rmb/squal/parser"
)

// ExtractMetadata parses a SQL query and extracts metadata.
// Phase 3: enhanced with bucket detection, array ops, JSONB ops, and ILIKE support.
//
// What: High-level metadata pass (tables, flags, select columns, group by, joins, filters).
// How: Walks the pg_query AST, delegating to small helpers per concern to keep logic readable.
func (p *PGQueryParser) ExtractMetadata(sql string) (*parser.QueryMetadata, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}

	metadata := &parser.QueryMetadata{
		Tables:          []string{},
		Columns:         []string{},
		Operations:      []string{},
		Aggregations:    []string{},
		Filters:         make(map[string]string),
		GroupBy:         []string{},
		OrderBy:         []string{},
		SelectColumns:   []parser.ColumnRef{},
		DistinctColumns: []string{},
	}

	// Populate base tables using ExtractTables helper
	if tables, err := p.ExtractTables(sql); err == nil {
		metadata.Tables = tables
	}

	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}

		if selectStmt := stmt.Stmt.GetSelectStmt(); selectStmt != nil {
			if selectStmt.WithClause != nil {
				metadata.HasCTEs = true
				if selectStmt.WithClause.Recursive {
					metadata.IsRecursiveCTE = true
				}
			}

			if len(selectStmt.DistinctClause) > 0 {
				metadata.HasDistinct = true
			}

			if distinctSpec, err := p.ExtractDistinctSpec(sql); err == nil && distinctSpec.HasDistinct {
				metadata.DistinctColumns = distinctSpec.Columns
			}

			// Target list
			for _, target := range selectStmt.TargetList {
				if resTarget := target.GetResTarget(); resTarget != nil {
					p.checkExpressionForMetadata(resTarget.Val, metadata)
					if selectCol := p.extractSelectColumn(resTarget); selectCol != nil {
						metadata.SelectColumns = append(metadata.SelectColumns, *selectCol)
					}
				}
			}

			// GROUP BY: bucket detection + ordered GroupBy strings
			for _, groupItem := range selectStmt.GroupClause {
				p.checkGroupByForBuckets(groupItem, metadata)

				// Handle positional GROUP BY referencing date_trunc target
				if !metadata.HasTimeBucket {
					if ac := groupItem.GetAConst(); ac != nil {
						if ival := ac.GetIval(); ival != nil {
							pos := int(ival.Ival)
							if pos >= 1 && selectStmt.TargetList != nil && pos <= len(selectStmt.TargetList) {
								if rt := selectStmt.TargetList[pos-1].GetResTarget(); rt != nil && rt.Val != nil {
									if fc := rt.Val.GetFuncCall(); fc != nil {
										fname := ""
										if len(fc.Funcname) > 0 {
											if s := fc.Funcname[len(fc.Funcname)-1].GetString_(); s != nil {
												fname = strings.ToLower(s.Sval)
											}
										}
										if fname == "date_trunc" && len(fc.Args) >= 2 {
											bi := &parser.BucketInfo{Function: "date_trunc"}
											if a := fc.Args[0].GetAConst(); a != nil {
												if sv := a.GetSval(); sv != nil {
													bi.Interval = sv.Sval
												}
											}
											if c := fc.Args[1].GetColumnRef(); c != nil && len(c.Fields) > 0 {
												if s := c.Fields[len(c.Fields)-1].GetString_(); s != nil {
													bi.Column = s.Sval
												}
												if len(c.Fields) > 1 {
													if s := c.Fields[0].GetString_(); s != nil {
														bi.ColumnTable = s.Sval
													}
												}
											}
											metadata.HasTimeBucket = true
											metadata.BucketInfo = bi
										}
									}
								}
							}
						}
					}
				}
			}

			if groupItems, err := p.ExtractGroupBy(sql); err == nil {
				for _, item := range groupItems {
					switch item.Kind {
					case "column":
						if item.Table != "" {
							metadata.GroupBy = append(metadata.GroupBy, item.Table+"."+item.Column)
						} else {
							metadata.GroupBy = append(metadata.GroupBy, item.Column)
						}
					case "alias":
						metadata.GroupBy = append(metadata.GroupBy, item.Alias)
					case "positional":
						if item.Alias != "" {
							metadata.GroupBy = append(metadata.GroupBy, item.Alias)
						} else if item.Column != "" {
							if item.Table != "" {
								metadata.GroupBy = append(metadata.GroupBy, item.Table+"."+item.Column)
							} else {
								metadata.GroupBy = append(metadata.GroupBy, item.Column)
							}
						} else {
							metadata.GroupBy = append(metadata.GroupBy, "__expr_pos_")
						}
					case "function", "expression":
						if item.RawExpr != "" {
							metadata.GroupBy = append(metadata.GroupBy, item.RawExpr)
						} else {
							metadata.GroupBy = append(metadata.GroupBy, "<expr>")
						}
					}
				}
			}

			if selectStmt.WhereClause != nil {
				p.checkWhereClauseForOperations(selectStmt.WhereClause, metadata)
				p.extractWhereFilters(selectStmt.WhereClause, metadata)
			}

			// JOIN relationships
			for _, fromItem := range selectStmt.FromClause {
				p.extractJoinConditionsFromFromClause(fromItem, metadata)
			}
		}
	}

	return metadata, nil
}

// extractSelectColumn extracts column metadata from a SELECT target (ResTarget node).
// It identifies aggregate functions, column references, and aliases.
func (p *PGQueryParser) extractSelectColumn(rt *pg_query.ResTarget) *parser.ColumnRef {
	if rt == nil || rt.Val == nil {
		return nil
	}

	colRef := &parser.ColumnRef{Alias: rt.Name}

	if fc := rt.Val.GetFuncCall(); fc != nil {
		funcName := ""
		if len(fc.Funcname) > 0 {
			if s := fc.Funcname[0].GetString_(); s != nil {
				funcName = strings.ToLower(s.Sval)
			}
		}

		if p.isAggregateFunction(funcName) {
			colRef.IsAgg = true
			colRef.Column = strings.ToUpper(funcName)
			if len(fc.Args) > 0 {
				if fc.Args[0].GetAStar() != nil {
					colRef.Column = strings.ToUpper(funcName) + "(*)"
				} else if cr := fc.Args[0].GetColumnRef(); cr != nil {
					argCol, argTable := p.extractColumnFromRef(cr)
					if argCol != "" {
						if argTable != "" {
							colRef.Table = argTable
							colRef.Column = strings.ToUpper(funcName) + "(" + argTable + "." + argCol + ")"
						} else {
							colRef.Column = strings.ToUpper(funcName) + "(" + argCol + ")"
						}
					}
				}
			} else if funcName == "count" {
				colRef.Column = "COUNT(*)"
			}
			return colRef
		}
	}

	if cr := rt.Val.GetColumnRef(); cr != nil {
		col, table := p.extractColumnFromRef(cr)
		if col != "" {
			colRef.Column = col
			colRef.Table = table
			return colRef
		}
	}

	if rt.Name != "" {
		colRef.Column = rt.Name
		return colRef
	}
	return nil
}

// checkExpressionForMetadata inspects expressions for aggregates, windows, arrays, jsonb, and subqueries.
func (p *PGQueryParser) checkExpressionForMetadata(node *pg_query.Node, metadata *parser.QueryMetadata) {
	if node == nil || metadata == nil {
		return
	}

	if funcCall := node.GetFuncCall(); funcCall != nil {
		funcName := ""
		if len(funcCall.Funcname) > 0 {
			if str := funcCall.Funcname[0].GetString_(); str != nil {
				funcName = strings.ToLower(str.Sval)
			}
		}

		if funcCall.Over != nil {
			metadata.HasWindowFunctions = true
		}
		if p.isAggregateFunction(funcName) {
			metadata.IsAggregate = true
		}
		if p.isArrayFunction(funcName) {
			metadata.DatabaseType = "postgresql"
			metadata.HasDatabaseSpecificOps = true
			if !p.stringInSlice(funcName, metadata.DatabaseOperations) {
				metadata.DatabaseOperations = append(metadata.DatabaseOperations, funcName)
			}
		}
		for _, arg := range funcCall.Args {
			p.checkExpressionForMetadata(arg, metadata)
		}
	}

	if windowDef := node.GetWindowDef(); windowDef != nil {
		_ = windowDef
		metadata.HasWindowFunctions = true
	}
	if aggref := node.GetAggref(); aggref != nil {
		_ = aggref
		metadata.IsAggregate = true
	}

	if aExpr := node.GetAExpr(); aExpr != nil {
		if p.isJSONBOperator(aExpr) {
			metadata.DatabaseType = "postgresql"
			metadata.HasDatabaseSpecificOps = true
			if !p.stringInSlice("jsonb", metadata.DatabaseOperations) {
				metadata.DatabaseOperations = append(metadata.DatabaseOperations, "jsonb")
			}
		}
	}

	if subLink := node.GetSubLink(); subLink != nil {
		if subSelect := subLink.Subselect; subSelect != nil {
			if selectStmt := subSelect.GetSelectStmt(); selectStmt != nil && selectStmt.TargetList != nil {
				for _, target := range selectStmt.TargetList {
					if resTarget := target.GetResTarget(); resTarget != nil {
						p.checkExpressionForMetadata(resTarget.Val, metadata)
					}
				}
			}
		}
	}
}

// checkGroupByForBuckets detects date_trunc and other time bucketing in GROUP BY
func (p *PGQueryParser) checkGroupByForBuckets(node *pg_query.Node, metadata *parser.QueryMetadata) {
	if node == nil {
		return
	}
	if funcCall := node.GetFuncCall(); funcCall != nil {
		funcName := ""
		if len(funcCall.Funcname) > 0 {
			if str := funcCall.Funcname[0].GetString_(); str != nil {
				funcName = strings.ToLower(str.Sval)
			}
		}
		if funcName == "date_trunc" && len(funcCall.Args) >= 2 {
			metadata.HasTimeBucket = true
			bucketInfo := &parser.BucketInfo{Function: "date_trunc"}
			if firstArg := funcCall.Args[0].GetAConst(); firstArg != nil {
				if sval := firstArg.GetSval(); sval != nil {
					bucketInfo.Interval = sval.Sval
				}
			}
			if len(funcCall.Args) > 1 {
				if colRef := funcCall.Args[1].GetColumnRef(); colRef != nil && len(colRef.Fields) > 0 {
					lastField := colRef.Fields[len(colRef.Fields)-1]
					if str := lastField.GetString_(); str != nil {
						bucketInfo.Column = str.Sval
					}
					if len(colRef.Fields) > 1 {
						if str := colRef.Fields[0].GetString_(); str != nil {
							bucketInfo.ColumnTable = str.Sval
						}
					}
				}
			}
			metadata.BucketInfo = bucketInfo
		}
	}
}

// checkWhereClauseForOperations checks WHERE clause for ILIKE and JSONB ops.
func (p *PGQueryParser) checkWhereClauseForOperations(node *pg_query.Node, metadata *parser.QueryMetadata) {
	if node == nil {
		return
	}
	if aExpr := node.GetAExpr(); aExpr != nil {
		if p.isILikeOperator(aExpr) {
			metadata.DatabaseType = "postgresql"
			metadata.HasDatabaseSpecificOps = true
			if !p.stringInSlice("ilike", metadata.DatabaseOperations) {
				metadata.DatabaseOperations = append(metadata.DatabaseOperations, "ilike")
			}
		}
		if p.isJSONBOperator(aExpr) {
			metadata.DatabaseType = "postgresql"
			metadata.HasDatabaseSpecificOps = true
			if !p.stringInSlice("jsonb", metadata.DatabaseOperations) {
				metadata.DatabaseOperations = append(metadata.DatabaseOperations, "jsonb")
			}
		}
		if aExpr.Lexpr != nil {
			p.checkWhereClauseForOperations(aExpr.Lexpr, metadata)
		}
		if aExpr.Rexpr != nil {
			p.checkWhereClauseForOperations(aExpr.Rexpr, metadata)
		}
	}
	if boolExpr := node.GetBoolExpr(); boolExpr != nil {
		for _, arg := range boolExpr.Args {
			p.checkWhereClauseForOperations(arg, metadata)
		}
	}
}

// extractWhereFilters walks the WHERE clause to populate QueryMetadata.WhereConditions
// with simple predicates used by the predicate guard.
func (p *PGQueryParser) extractWhereFilters(node *pg_query.Node, metadata *parser.QueryMetadata) {
	if node == nil || metadata == nil {
		return
	}
	if be := node.GetBoolExpr(); be != nil {
		if be.Boolop == pg_query.BoolExprType_AND_EXPR {
			for _, arg := range be.Args {
				p.extractWhereFilters(arg, metadata)
			}
		}
		return
	}
	if ae := node.GetAExpr(); ae != nil {
		op := ""
		if len(ae.Name) > 0 {
			if s := ae.Name[0].GetString_(); s != nil {
				op = strings.ToUpper(s.Sval)
			}
		}
		if op == "=" {
			col, okCol := p.columnRefToNames(ae.Lexpr)
			val, okVal := p.constNodeToValue(ae.Rexpr)
			if !okCol || !okVal {
				col2, okCol2 := p.columnRefToNames(ae.Rexpr)
				val2, okVal2 := p.constNodeToValue(ae.Lexpr)
				if okCol2 && okVal2 {
					metadata.WhereConditions = append(metadata.WhereConditions, parser.FilterCondition{
						Column:   parser.ColumnRef{Table: col2.table, Column: col2.column},
						Operator: core.CompareOpEqual,
						Value:    val2,
					})
				}
				return
			}
			metadata.WhereConditions = append(metadata.WhereConditions, parser.FilterCondition{
				Column:   parser.ColumnRef{Table: col.table, Column: col.column},
				Operator: core.CompareOpEqual,
				Value:    val,
			})
			return
		}
		if op == "IN" {
			if col, ok := p.columnRefToNames(ae.Lexpr); ok {
				if list := ae.Rexpr.GetList(); list != nil {
					vals := make([]interface{}, 0, len(list.Items))
					for _, it := range list.Items {
						if v, ok := p.constNodeToValue(it); ok {
							vals = append(vals, v)
						}
					}
					if len(vals) > 0 {
						metadata.WhereConditions = append(metadata.WhereConditions, parser.FilterCondition{
							Column:   parser.ColumnRef{Table: col.table, Column: col.column},
							Operator: core.CompareOpIn,
							Value:    vals,
						})
					}
				}
			}
			return
		}
		return
	}
	if nt := node.GetNullTest(); nt != nil {
		if col, ok := p.columnRefToNames(nt.Arg); ok {
			if nt.Nulltesttype == pg_query.NullTestType_IS_NULL {
				metadata.WhereConditions = append(metadata.WhereConditions, parser.FilterCondition{
					Column:   parser.ColumnRef{Table: col.table, Column: col.column},
					Operator: core.CompareOpIs,
					Value:    nil,
				})
			} else if nt.Nulltesttype == pg_query.NullTestType_IS_NOT_NULL {
				metadata.WhereConditions = append(metadata.WhereConditions, parser.FilterCondition{
					Column:   parser.ColumnRef{Table: col.table, Column: col.column},
					Operator: core.CompareOpIsNot,
					Value:    nil,
				})
			}
		}
		return
	}
}

