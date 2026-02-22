package complete

import (
	"sort"
	"strings"

	"github.com/iw2rmb/squal/parser"
)

func (e *EngineImpl) buildContext(req Request) (completionContext, []Diagnostic) {
	if e.cfg.Parser == nil {
		return completionContext{ParseDegraded: true}, []Diagnostic{
			{
				Code:    ParseDegraded,
				Message: "parser dependency is not configured",
			},
		}
	}

	meta, err := e.cfg.Parser.ExtractMetadata(req.SQL)
	if err != nil {
		return completionContext{ParseDegraded: true}, []Diagnostic{
			{
				Code:    ParseDegraded,
				Message: "parser metadata extraction failed",
			},
		}
	}
	if meta == nil {
		return completionContext{ParseDegraded: true}, []Diagnostic{
			{
				Code:    ParseDegraded,
				Message: "parser metadata is unavailable",
			},
		}
	}

	return buildContext(meta, req.SQL, req.CursorByte), nil
}

func buildContext(meta *parser.QueryMetadata, sql string, cursor int) completionContext {
	if meta == nil {
		return completionContext{
			ActiveClause:  activeClauseAtCursor(sql, cursor),
			ParseDegraded: true,
		}
	}

	tables := sortedUniqueStrings(meta.Tables)

	knownTables := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		knownTables[strings.ToLower(table)] = struct{}{}
	}

	aliasesSet := make(map[string]struct{}, len(meta.JoinConditions)*2)
	aliasToTable := make(map[string]string, len(meta.JoinConditions)*2)
	ambiguousAliases := make(map[string]struct{})

	recordAliasBinding := func(alias, table string) {
		if alias == "" || table == "" {
			return
		}
		if _, ambiguous := ambiguousAliases[alias]; ambiguous {
			return
		}
		if existing, ok := aliasToTable[alias]; ok {
			if existing != table {
				delete(aliasToTable, alias)
				ambiguousAliases[alias] = struct{}{}
			}
			return
		}
		aliasToTable[alias] = table
	}

	aliasesFromQualifier := func(qualifier string) {
		if qualifier == "" {
			return
		}
		if _, ok := knownTables[strings.ToLower(qualifier)]; ok {
			return
		}
		aliasesSet[qualifier] = struct{}{}
	}

	for _, join := range meta.JoinConditions {
		if join.LeftAlias != "" {
			aliasesSet[join.LeftAlias] = struct{}{}
			recordAliasBinding(join.LeftAlias, join.LeftTable)
		}
		if join.RightAlias != "" {
			aliasesSet[join.RightAlias] = struct{}{}
			recordAliasBinding(join.RightAlias, join.RightTable)
		}
	}
	for _, col := range meta.SelectColumns {
		aliasesFromQualifier(col.Table)
	}
	for _, filter := range meta.WhereConditions {
		aliasesFromQualifier(filter.Column.Table)
	}

	projectionSet := make(map[string]struct{}, len(meta.SelectColumns))
	for _, col := range meta.SelectColumns {
		target := projectionTarget(col)
		if target != "" {
			projectionSet[target] = struct{}{}
		}
	}

	predicates := make([]predicateContext, 0, len(meta.WhereConditions))
	for _, filter := range meta.WhereConditions {
		predicates = append(predicates, predicateContext{
			Qualifier: filter.Column.Table,
			Column:    filter.Column.Column,
			Operator:  string(filter.Operator),
			IsParam:   filter.IsParam,
		})
	}
	sort.Slice(predicates, func(i, j int) bool {
		a, b := predicates[i], predicates[j]
		if a.Qualifier != b.Qualifier {
			return a.Qualifier < b.Qualifier
		}
		if a.Column != b.Column {
			return a.Column < b.Column
		}
		if a.Operator != b.Operator {
			return a.Operator < b.Operator
		}
		return !a.IsParam && b.IsParam
	})

	joins := make([]joinContext, 0, len(meta.JoinConditions))
	for _, join := range meta.JoinConditions {
		joins = append(joins, joinContext{
			Type:        string(join.Type),
			LeftTable:   join.LeftTable,
			RightTable:  join.RightTable,
			LeftColumn:  join.LeftColumn,
			RightColumn: join.RightColumn,
			LeftAlias:   join.LeftAlias,
			RightAlias:  join.RightAlias,
		})
	}
	sort.Slice(joins, func(i, j int) bool {
		a, b := joins[i], joins[j]
		if a.Type != b.Type {
			return a.Type < b.Type
		}
		if a.LeftTable != b.LeftTable {
			return a.LeftTable < b.LeftTable
		}
		if a.RightTable != b.RightTable {
			return a.RightTable < b.RightTable
		}
		if a.LeftColumn != b.LeftColumn {
			return a.LeftColumn < b.LeftColumn
		}
		if a.RightColumn != b.RightColumn {
			return a.RightColumn < b.RightColumn
		}
		if a.LeftAlias != b.LeftAlias {
			return a.LeftAlias < b.LeftAlias
		}
		return a.RightAlias < b.RightAlias
	})

	aliasBindings := make([]aliasBinding, 0, len(aliasToTable))
	for alias, table := range aliasToTable {
		aliasBindings = append(aliasBindings, aliasBinding{
			Alias: alias,
			Table: table,
		})
	}
	sort.Slice(aliasBindings, func(i, j int) bool {
		a, b := aliasBindings[i], aliasBindings[j]
		if a.Alias != b.Alias {
			return a.Alias < b.Alias
		}
		return a.Table < b.Table
	})

	return completionContext{
		ActiveClause:      activeClauseAtCursor(sql, cursor),
		Tables:            tables,
		Aliases:           sortedKeys(aliasesSet),
		AliasBindings:     aliasBindings,
		ProjectionTargets: sortedKeys(projectionSet),
		Predicates:        predicates,
		Joins:             joins,
	}
}

func projectionTarget(col parser.ColumnRef) string {
	if col.Alias != "" {
		return col.Alias
	}
	if col.Column == "" {
		return ""
	}
	if col.Table == "" {
		return col.Column
	}
	return col.Table + "." + col.Column
}

func sortedUniqueStrings(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		set[value] = struct{}{}
	}
	return sortedKeys(set)
}

func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
