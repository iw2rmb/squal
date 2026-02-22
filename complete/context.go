package complete

import (
	"sort"
	"strings"
)

type contextClause string

const (
	contextClauseUnknown contextClause = "unknown"
	contextClauseSelect  contextClause = "select"
	contextClauseFrom    contextClause = "from"
	contextClauseJoin    contextClause = "join"
	contextClauseWhere   contextClause = "where"
	contextClauseGroupBy contextClause = "group_by"
	contextClauseOrderBy contextClause = "order_by"
)

type completionContext struct {
	ActiveClause      contextClause
	Tables            []string
	Aliases           []string
	ProjectionTargets []string
	Predicates        []predicateContext
	Joins             []joinContext
	ParseDegraded     bool
}

type predicateContext struct {
	Qualifier string
	Column    string
	Operator  string
	IsParam   bool
}

type joinContext struct {
	Type        string
	LeftTable   string
	RightTable  string
	LeftColumn  string
	RightColumn string
	LeftAlias   string
	RightAlias  string
}

func activeClauseAtCursor(sql string, cursor int) contextClause {
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(sql) {
		cursor = len(sql)
	}

	prefix := strings.ToUpper(sql[:cursor])

	occurrences := []struct {
		clause contextClause
		pos    int
	}{
		{clause: contextClauseSelect, pos: strings.LastIndex(prefix, "SELECT")},
		{clause: contextClauseFrom, pos: strings.LastIndex(prefix, "FROM")},
		{clause: contextClauseJoin, pos: strings.LastIndex(prefix, "JOIN")},
		{clause: contextClauseWhere, pos: strings.LastIndex(prefix, "WHERE")},
		{clause: contextClauseGroupBy, pos: strings.LastIndex(prefix, "GROUP BY")},
		{clause: contextClauseOrderBy, pos: strings.LastIndex(prefix, "ORDER BY")},
	}

	sort.SliceStable(occurrences, func(i, j int) bool {
		return occurrences[i].pos > occurrences[j].pos
	})

	for _, candidate := range occurrences {
		if candidate.pos >= 0 {
			return candidate.clause
		}
	}

	return contextClauseUnknown
}
