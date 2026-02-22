package complete

import (
	"sort"
	"strconv"
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

	aliasesSet := make(map[string]struct{})
	aliasToTable := make(map[string]string)
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
		if predicates[i].Qualifier != predicates[j].Qualifier {
			return predicates[i].Qualifier < predicates[j].Qualifier
		}
		if predicates[i].Column != predicates[j].Column {
			return predicates[i].Column < predicates[j].Column
		}
		if predicates[i].Operator != predicates[j].Operator {
			return predicates[i].Operator < predicates[j].Operator
		}
		return strconv.FormatBool(predicates[i].IsParam) < strconv.FormatBool(predicates[j].IsParam)
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
		if joins[i].Type != joins[j].Type {
			return joins[i].Type < joins[j].Type
		}
		if joins[i].LeftTable != joins[j].LeftTable {
			return joins[i].LeftTable < joins[j].LeftTable
		}
		if joins[i].RightTable != joins[j].RightTable {
			return joins[i].RightTable < joins[j].RightTable
		}
		if joins[i].LeftColumn != joins[j].LeftColumn {
			return joins[i].LeftColumn < joins[j].LeftColumn
		}
		if joins[i].RightColumn != joins[j].RightColumn {
			return joins[i].RightColumn < joins[j].RightColumn
		}
		if joins[i].LeftAlias != joins[j].LeftAlias {
			return joins[i].LeftAlias < joins[j].LeftAlias
		}
		return joins[i].RightAlias < joins[j].RightAlias
	})

	aliasBindings := make([]aliasBinding, 0, len(aliasToTable))
	for alias, table := range aliasToTable {
		aliasBindings = append(aliasBindings, aliasBinding{
			Alias: alias,
			Table: table,
		})
	}
	sort.Slice(aliasBindings, func(i, j int) bool {
		if aliasBindings[i].Alias != aliasBindings[j].Alias {
			return aliasBindings[i].Alias < aliasBindings[j].Alias
		}
		return aliasBindings[i].Table < aliasBindings[j].Table
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
