package complete

import (
	"strings"
)

type contextClause string

const (
	contextClauseUnknown  contextClause = "unknown"
	contextClauseSelect   contextClause = "select"
	contextClauseFrom     contextClause = "from"
	contextClauseFromTail contextClause = "from_tail"
	contextClauseJoin     contextClause = "join"
	contextClauseJoinOn   contextClause = "join_on"
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

	prefix := sql[:cursor]
	lastClause := contextClauseUnknown
	lastFromPos := -1
	lastJoinPos := -1
	state := clauseScanDefault

	for i := 0; i < len(prefix); {
		switch state {
		case clauseScanSingleQuote:
			if prefix[i] == '\'' {
				if i+1 < len(prefix) && prefix[i+1] == '\'' {
					i += 2
					continue
				}
				state = clauseScanDefault
			}
			i++
			continue
		case clauseScanDoubleQuote:
			if prefix[i] == '"' {
				if i+1 < len(prefix) && prefix[i+1] == '"' {
					i += 2
					continue
				}
				state = clauseScanDefault
			}
			i++
			continue
		case clauseScanLineComment:
			if prefix[i] == '\n' || prefix[i] == '\r' {
				state = clauseScanDefault
			}
			i++
			continue
		case clauseScanBlockComment:
			if prefix[i] == '*' && i+1 < len(prefix) && prefix[i+1] == '/' {
				state = clauseScanDefault
				i += 2
				continue
			}
			i++
			continue
		}

		if prefix[i] == '\'' {
			state = clauseScanSingleQuote
			i++
			continue
		}
		if prefix[i] == '"' {
			state = clauseScanDoubleQuote
			i++
			continue
		}
		if prefix[i] == '-' && i+1 < len(prefix) && prefix[i+1] == '-' {
			state = clauseScanLineComment
			i += 2
			continue
		}
		if prefix[i] == '/' && i+1 < len(prefix) && prefix[i+1] == '*' {
			state = clauseScanBlockComment
			i += 2
			continue
		}

		if end, ok := matchTwoWordClause(prefix, i, "GROUP", "BY"); ok {
			lastClause = contextClauseGroupBy
			i = end
			continue
		}
		if end, ok := matchTwoWordClause(prefix, i, "ORDER", "BY"); ok {
			lastClause = contextClauseOrderBy
			i = end
			continue
		}
		if end, ok := matchSingleWordClause(prefix, i, "SELECT"); ok {
			lastClause = contextClauseSelect
			i = end
			continue
		}
		if end, ok := matchSingleWordClause(prefix, i, "FROM"); ok {
			lastClause = contextClauseFrom
			lastFromPos = i
			i = end
			continue
		}
		if end, ok := matchSingleWordClause(prefix, i, "JOIN"); ok {
			lastClause = contextClauseJoin
			lastJoinPos = i
			i = end
			continue
		}
		if end, ok := matchSingleWordClause(prefix, i, "ON"); ok {
			if lastJoinPos >= 0 && lastJoinPos < i {
				lastClause = contextClauseJoinOn
			}
			i = end
			continue
		}
		if end, ok := matchSingleWordClause(prefix, i, "WHERE"); ok {
			lastClause = contextClauseWhere
			i = end
			continue
		}

		i++
	}

	if lastClause == contextClauseFrom {
		return classifyFromClause(prefix, lastFromPos)
	}
	return lastClause
}

type clauseScanState uint8

const (
	clauseScanDefault clauseScanState = iota
	clauseScanSingleQuote
	clauseScanDoubleQuote
	clauseScanLineComment
	clauseScanBlockComment
)

func matchSingleWordClause(value string, start int, keyword string) (int, bool) {
	end := start + len(keyword)
	if end > len(value) {
		return 0, false
	}
	if !isClauseBoundaryBefore(value, start) || !isClauseBoundaryAfter(value, end) {
		return 0, false
	}
	if !equalFoldASCII(value[start:end], keyword) {
		return 0, false
	}
	return end, true
}

func matchTwoWordClause(value string, start int, first string, second string) (int, bool) {
	firstEnd, ok := matchSingleWordClause(value, start, first)
	if !ok {
		return 0, false
	}

	if firstEnd >= len(value) || !isWhitespaceByte(value[firstEnd]) {
		return 0, false
	}

	secondStart := firstEnd
	for secondStart < len(value) && isWhitespaceByte(value[secondStart]) {
		secondStart++
	}

	secondEnd := secondStart + len(second)
	if secondEnd > len(value) {
		return 0, false
	}
	if !isClauseBoundaryAfter(value, secondEnd) {
		return 0, false
	}
	if !equalFoldASCII(value[secondStart:secondEnd], second) {
		return 0, false
	}
	return secondEnd, true
}

func isClauseBoundaryBefore(value string, start int) bool {
	if start <= 0 {
		return true
	}
	return !isIdentifierByte(value[start-1])
}

func isClauseBoundaryAfter(value string, end int) bool {
	if end >= len(value) {
		return true
	}
	return !isIdentifierByte(value[end])
}

func equalFoldASCII(value string, expectedUpper string) bool {
	if len(value) != len(expectedUpper) {
		return false
	}
	for i := 0; i < len(value); i++ {
		if asciiUpper(value[i]) != expectedUpper[i] {
			return false
		}
	}
	return true
}

func asciiUpper(b byte) byte {
	if b >= 'a' && b <= 'z' {
		return b - ('a' - 'A')
	}
	return b
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
