package complete

import (
	"strings"
	"unicode/utf8"

	"github.com/iw2rmb/squal/core"
)

func planSingleEdit(req Request, accepted Candidate) (EditPlan, []Diagnostic) {
	if accepted.Kind == "" || accepted.InsertText == "" {
		return conflictPlan(
			replacementSpanForCursor(req.SQL, req.CursorByte),
			EditPlanValidation{InBounds: true, NonOverlapping: true},
			"accepted candidate is invalid",
		)
	}

	if !utf8.ValidString(req.SQL[:req.CursorByte]) {
		span := core.Span{StartByte: req.CursorByte, EndByte: req.CursorByte}
		return conflictPlan(
			span,
			EditPlanValidation{InBounds: false, NonOverlapping: false},
			"cursor byte offset must align to utf-8 rune boundary",
		)
	}

	span := replacementSpanForCursor(req.SQL, req.CursorByte)
	edit := core.TextEdit{
		Span:    span,
		NewText: adaptInsertTextForTrailingToken(req.SQL, span, accepted.InsertText),
	}
	changeSet := core.TextChangeSet{Edits: []core.TextEdit{edit}}
	validation := EditPlanValidation{
		InBounds:       edit.IsValid(len(req.SQL)),
		NonOverlapping: changeSet.Validate(len(req.SQL)),
	}
	if !validation.InBounds || !validation.NonOverlapping {
		return conflictPlan(span, validation, "planned edit set is invalid or overlapping")
	}

	return EditPlan{
		Edits:           []core.TextEdit{edit},
		ReplacementSpan: span,
		Validation:      validation,
	}, nil
}

func conflictPlan(span core.Span, validation EditPlanValidation, message string) (EditPlan, []Diagnostic) {
	return EditPlan{
			Edits:           []core.TextEdit{},
			ReplacementSpan: span,
			Validation:      validation,
		}, []Diagnostic{
			{
				Code:    EditConflict,
				Message: message,
			},
		}
}

func replacementSpanForCursor(sql string, cursor int) core.Span {
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(sql) {
		cursor = len(sql)
	}

	start := cursor
	for start > 0 {
		r, size := utf8.DecodeLastRuneInString(sql[:start])
		if r == utf8.RuneError && size == 1 {
			break
		}
		if !isIdentifierRune(r) {
			break
		}
		start -= size
	}

	end := cursor
	for end < len(sql) {
		r, size := utf8.DecodeRuneInString(sql[end:])
		if r == utf8.RuneError && size == 1 {
			break
		}
		if !isIdentifierRune(r) {
			break
		}
		end += size
	}

	if start == end {
		return core.Span{StartByte: cursor, EndByte: cursor}
	}
	return core.Span{StartByte: start, EndByte: end}
}

func adaptInsertTextForTrailingToken(sql string, replacement core.Span, insert string) string {
	qualifier := qualifierBeforeSpan(sql, replacement.StartByte)
	if qualifier == "" {
		return insert
	}

	qualifierPrefix := strings.ToLower(qualifier) + "."
	if !strings.HasPrefix(strings.ToLower(insert), qualifierPrefix) {
		return insert
	}

	trimmed := insert[len(qualifier)+1:]
	if trimmed == "" {
		return insert
	}
	return trimmed
}

func qualifierBeforeSpan(sql string, tokenStart int) string {
	if tokenStart <= 0 || tokenStart > len(sql) {
		return ""
	}
	if sql[tokenStart-1] != '.' {
		return ""
	}

	qualifierEnd := tokenStart - 1
	qualifierStart := qualifierEnd
	for qualifierStart > 0 {
		r, size := utf8.DecodeLastRuneInString(sql[:qualifierStart])
		if r == utf8.RuneError && size == 1 {
			break
		}
		if !isIdentifierRune(r) {
			break
		}
		qualifierStart -= size
	}
	if qualifierStart == qualifierEnd {
		return ""
	}
	return sql[qualifierStart:qualifierEnd]
}

func isIdentifierRune(r rune) bool {
	return (r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9') ||
		r == '_' ||
		r == '$'
}
