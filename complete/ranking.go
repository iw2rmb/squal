package complete

import "strings"

type rankingContext struct {
	activeClause contextClause
	cursorPrefix string
}

type contextScoreKey struct {
	clause contextClause
	kind   CandidateKind
}

var contextScoreTable = map[contextScoreKey]float64{
	// SELECT
	{contextClauseSelect, CandidateKindColumn}:  50,
	{contextClauseSelect, CandidateKindTable}:   20,
	{contextClauseSelect, CandidateKindJoin}:    10,
	{contextClauseSelect, CandidateKindSchema}:  8,
	{contextClauseSelect, CandidateKindSnippet}: 6,
	{contextClauseSelect, CandidateKindKeyword}: 5,
	// FROM
	{contextClauseFrom, CandidateKindTable}:   50,
	{contextClauseFrom, CandidateKindSchema}:  24,
	{contextClauseFrom, CandidateKindJoin}:    12,
	{contextClauseFrom, CandidateKindColumn}:  6,
	{contextClauseFrom, CandidateKindSnippet}: 5,
	{contextClauseFrom, CandidateKindKeyword}: 4,
	// JOIN
	{contextClauseJoin, CandidateKindJoin}:    50,
	{contextClauseJoin, CandidateKindTable}:   30,
	{contextClauseJoin, CandidateKindColumn}:  12,
	{contextClauseJoin, CandidateKindSchema}:  6,
	{contextClauseJoin, CandidateKindSnippet}: 5,
	{contextClauseJoin, CandidateKindKeyword}: 4,
	// WHERE
	{contextClauseWhere, CandidateKindColumn}:  55,
	{contextClauseWhere, CandidateKindJoin}:    12,
	{contextClauseWhere, CandidateKindTable}:   8,
	{contextClauseWhere, CandidateKindSnippet}: 6,
	{contextClauseWhere, CandidateKindSchema}:  5,
	{contextClauseWhere, CandidateKindKeyword}: 4,
	// GROUP BY
	{contextClauseGroupBy, CandidateKindColumn}:  50,
	{contextClauseGroupBy, CandidateKindTable}:   8,
	{contextClauseGroupBy, CandidateKindJoin}:    6,
	{contextClauseGroupBy, CandidateKindSchema}:  5,
	{contextClauseGroupBy, CandidateKindSnippet}: 5,
	{contextClauseGroupBy, CandidateKindKeyword}: 4,
	// ORDER BY
	{contextClauseOrderBy, CandidateKindColumn}:  50,
	{contextClauseOrderBy, CandidateKindTable}:   8,
	{contextClauseOrderBy, CandidateKindJoin}:    6,
	{contextClauseOrderBy, CandidateKindSchema}:  5,
	{contextClauseOrderBy, CandidateKindSnippet}: 5,
	{contextClauseOrderBy, CandidateKindKeyword}: 4,
}

func contextScore(clause contextClause, kind CandidateKind) float64 {
	return contextScoreTable[contextScoreKey{clause, kind}]
}

func catalogScore(source CandidateSource) float64 {
	switch source {
	case CandidateSourceCatalog:
		return 20
	case CandidateSourceParser:
		return 12
	case CandidateSourceSnippet:
		return 8
	case CandidateSourceProvider:
		return 6
	default:
		return 0
	}
}

func populateRanking(candidate *Candidate, ctx rankingContext) {
	exactPrefix := hasExactPrefixMatch(candidate, ctx.cursorPrefix)
	candidate.SortKey.ExactPrefix = exactPrefix

	var prefixVal float64
	if exactPrefix {
		prefixVal = 30
	}
	var snippetVal float64
	if candidate.Kind == CandidateKindSnippet {
		snippetVal = -5
	}
	var providerVal float64
	if candidate.Source == CandidateSourceProvider {
		providerVal = 10
	}

	candidate.ScoreComponents = ScoreComponents{
		Context:  contextScore(ctx.activeClause, candidate.Kind),
		Catalog:  catalogScore(candidate.Source),
		Prefix:   prefixVal,
		Snippet:  snippetVal,
		Provider: providerVal,
	}
	candidate.Score = candidate.ScoreComponents.Context +
		candidate.ScoreComponents.Catalog +
		candidate.ScoreComponents.Prefix +
		candidate.ScoreComponents.Snippet +
		candidate.ScoreComponents.Provider
}

func candidateLess(left Candidate, right Candidate) bool {
	if left.Score != right.Score {
		return left.Score > right.Score
	}
	if left.SortKey.KindPriority != right.SortKey.KindPriority {
		return left.SortKey.KindPriority < right.SortKey.KindPriority
	}
	if left.SortKey.ExactPrefix != right.SortKey.ExactPrefix {
		return left.SortKey.ExactPrefix && !right.SortKey.ExactPrefix
	}
	if left.Kind != right.Kind {
		return left.Kind < right.Kind
	}
	if left.SortKey.LabelLexical != right.SortKey.LabelLexical {
		return left.SortKey.LabelLexical < right.SortKey.LabelLexical
	}
	if left.SortKey.InsertLexical != right.SortKey.InsertLexical {
		return left.SortKey.InsertLexical < right.SortKey.InsertLexical
	}
	if left.Label != right.Label {
		return left.Label < right.Label
	}
	if left.InsertText != right.InsertText {
		return left.InsertText < right.InsertText
	}
	if left.Source != right.Source {
		return left.Source < right.Source
	}
	return left.ID < right.ID
}

func candidateKindPriority(kind CandidateKind) int {
	switch kind {
	case CandidateKindSchema:
		return 10
	case CandidateKindTable:
		return 20
	case CandidateKindColumn:
		return 30
	case CandidateKindJoin:
		return 40
	case CandidateKindSnippet:
		return 50
	case CandidateKindKeyword:
		return 60
	default:
		return 100
	}
}

func cursorPrefixAt(sql string, cursor int) string {
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(sql) {
		cursor = len(sql)
	}

	start := cursor
	for start > 0 && isIdentifierByte(sql[start-1]) {
		start--
	}
	if start == cursor {
		return ""
	}
	return strings.ToLower(sql[start:cursor])
}

func hasExactPrefixMatch(candidate *Candidate, prefix string) bool {
	if prefix == "" {
		return false
	}

	return hasIdentifierPrefix(candidate.SortKey.LabelLexical, prefix) ||
		hasIdentifierPrefix(candidate.SortKey.InsertLexical, prefix)
}

func hasIdentifierPrefix(value string, prefix string) bool {
	start := -1
	for i := 0; i <= len(value); i++ {
		if i < len(value) && isIdentifierByte(value[i]) {
			if start < 0 {
				start = i
			}
			continue
		}

		if start >= 0 {
			if strings.HasPrefix(value[start:i], prefix) {
				return true
			}
			start = -1
		}
	}

	return false
}

func isIdentifierByte(b byte) bool {
	return (b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') ||
		b == '_' ||
		b == '$'
}

func candidateID(candidate Candidate) string {
	return string(candidate.Kind) + ":" + candidate.SortKey.LabelLexical + ":" + candidate.SortKey.InsertLexical
}
