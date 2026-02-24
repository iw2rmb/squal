package complete

import (
	"sort"
	"strings"
)

type contextClause string

const (
	contextClauseUnknown  contextClause = "unknown"
	contextClauseSelect   contextClause = "select"
	contextClauseFrom     contextClause = "from"
	contextClauseFromTail contextClause = "from_tail"
	contextClauseJoin     contextClause = "join"
	contextClauseWhere    contextClause = "where"
	contextClauseGroupBy  contextClause = "group_by"
	contextClauseOrderBy  contextClause = "order_by"
)

type completionContext struct {
	ActiveClause      contextClause
	Tables            []string
	Aliases           []string
	AliasBindings     []aliasBinding
	ProjectionTargets []string
	Predicates        []predicateContext
	Joins             []joinContext
	ParseDegraded     bool
}

type aliasBinding struct {
	Alias string
	Table string
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
			if candidate.clause == contextClauseFrom {
				return classifyFromClause(prefix, candidate.pos)
			}
			return candidate.clause
		}
	}

	return contextClauseUnknown
}

func classifyFromClause(prefix string, fromPos int) contextClause {
	fromStart := fromPos + len("FROM")
	if fromStart > len(prefix) {
		return contextClauseFrom
	}

	if fromClauseNeedsTable(prefix[fromStart:]) {
		return contextClauseFrom
	}
	return contextClauseFromTail
}

func fromClauseNeedsTable(tail string) bool {
	trimmed := strings.TrimSpace(tail)
	if trimmed == "" {
		return true
	}

	if hasTrailingWhitespace(tail) {
		last := trailingNonWhitespace(tail)
		return last == ','
	}

	tokenStart := len(trimmed)
	for tokenStart > 0 && !isFromClauseDelimiter(trimmed[tokenStart-1]) {
		tokenStart--
	}

	beforeToken := strings.TrimSpace(trimmed[:tokenStart])
	if beforeToken == "" {
		return true
	}
	return strings.HasSuffix(beforeToken, ",")
}

func hasTrailingWhitespace(value string) bool {
	if value == "" {
		return false
	}
	return isWhitespaceByte(value[len(value)-1])
}

func trailingNonWhitespace(value string) byte {
	for i := len(value) - 1; i >= 0; i-- {
		if !isWhitespaceByte(value[i]) {
			return value[i]
		}
	}
	return 0
}

func isFromClauseDelimiter(b byte) bool {
	return isWhitespaceByte(b) || b == ','
}

func isWhitespaceByte(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}
