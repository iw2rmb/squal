package core

import "sort"

// Span is a byte-addressed half-open range [StartByte, EndByte) over SQL text.
type Span struct {
	StartByte int `json:"start_byte"`
	EndByte   int `json:"end_byte"`
}

// IsValid reports whether the span is within [0, sqlLen] and non-inverted.
func (s Span) IsValid(sqlLen int) bool {
	if sqlLen < 0 {
		return false
	}
	if s.StartByte < 0 || s.EndByte < 0 {
		return false
	}
	if s.StartByte > s.EndByte {
		return false
	}
	return s.EndByte <= sqlLen
}

// Len returns the number of bytes covered by the span.
func (s Span) Len() int {
	return s.EndByte - s.StartByte
}

// TextEdit describes a replacement over a byte-addressed span.
type TextEdit struct {
	Span    Span   `json:"span"`
	NewText string `json:"new_text"`
}

// IsValid reports whether the edit span is valid for the given SQL length.
func (e TextEdit) IsValid(sqlLen int) bool {
	return e.Span.IsValid(sqlLen)
}

// TextChangeSet is an ordered set of text edits.
type TextChangeSet struct {
	Edits []TextEdit `json:"edits"`
}

// Canonicalize returns a copy with edits sorted in deterministic order.
func (c TextChangeSet) Canonicalize() TextChangeSet {
	canonical := TextChangeSet{
		Edits: append([]TextEdit(nil), c.Edits...),
	}
	sort.SliceStable(canonical.Edits, func(i, j int) bool {
		left := canonical.Edits[i]
		right := canonical.Edits[j]
		if left.Span.StartByte != right.Span.StartByte {
			return left.Span.StartByte < right.Span.StartByte
		}
		if left.Span.EndByte != right.Span.EndByte {
			return left.Span.EndByte < right.Span.EndByte
		}
		return left.NewText < right.NewText
	})
	return canonical
}

// Validate reports whether all edits are valid and non-overlapping.
func (c TextChangeSet) Validate(sqlLen int) bool {
	canonical := c.Canonicalize()
	previousEnd := 0
	for i, edit := range canonical.Edits {
		if !edit.IsValid(sqlLen) {
			return false
		}
		if i > 0 && edit.Span.StartByte < previousEnd {
			return false
		}
		previousEnd = edit.Span.EndByte
	}
	return true
}
