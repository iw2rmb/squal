package core

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestSpanIsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		span   Span
		sqlLen int
		want   bool
	}{
		{
			name:   "valid range",
			span:   Span{StartByte: 1, EndByte: 3},
			sqlLen: 10,
			want:   true,
		},
		{
			name:   "zero-length at end",
			span:   Span{StartByte: 10, EndByte: 10},
			sqlLen: 10,
			want:   true,
		},
		{
			name:   "negative start",
			span:   Span{StartByte: -1, EndByte: 1},
			sqlLen: 10,
			want:   false,
		},
		{
			name:   "inverted range",
			span:   Span{StartByte: 5, EndByte: 4},
			sqlLen: 10,
			want:   false,
		},
		{
			name:   "past sql length",
			span:   Span{StartByte: 0, EndByte: 11},
			sqlLen: 10,
			want:   false,
		},
		{
			name:   "negative sql length",
			span:   Span{StartByte: 0, EndByte: 0},
			sqlLen: -1,
			want:   false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.span.IsValid(tt.sqlLen); got != tt.want {
				t.Fatalf("Span.IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTextChangeSetValidateNonOverlapping(t *testing.T) {
	t.Parallel()

	changeSet := TextChangeSet{
		Edits: []TextEdit{
			{Span: Span{StartByte: 5, EndByte: 7}, NewText: "x"},
			{Span: Span{StartByte: 0, EndByte: 2}, NewText: "y"},
			{Span: Span{StartByte: 7, EndByte: 7}, NewText: "z"},
		},
	}
	if !changeSet.Validate(20) {
		t.Fatal("TextChangeSet.Validate() = false, want true")
	}
}

func TestTextChangeSetValidateOverlappingRejected(t *testing.T) {
	t.Parallel()

	changeSet := TextChangeSet{
		Edits: []TextEdit{
			{Span: Span{StartByte: 0, EndByte: 4}, NewText: "a"},
			{Span: Span{StartByte: 3, EndByte: 5}, NewText: "b"},
		},
	}
	if changeSet.Validate(20) {
		t.Fatal("TextChangeSet.Validate() = true, want false")
	}
}

func TestTextChangeSetCanonicalizeDeterministic(t *testing.T) {
	t.Parallel()

	changeSet := TextChangeSet{
		Edits: []TextEdit{
			{Span: Span{StartByte: 3, EndByte: 5}, NewText: "z"},
			{Span: Span{StartByte: 1, EndByte: 2}, NewText: "a"},
			{Span: Span{StartByte: 1, EndByte: 2}, NewText: "0"},
		},
	}
	got := changeSet.Canonicalize()
	want := TextChangeSet{
		Edits: []TextEdit{
			{Span: Span{StartByte: 1, EndByte: 2}, NewText: "0"},
			{Span: Span{StartByte: 1, EndByte: 2}, NewText: "a"},
			{Span: Span{StartByte: 3, EndByte: 5}, NewText: "z"},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Canonicalize() = %#v, want %#v", got, want)
	}
}

func TestEditDTOJSONStable(t *testing.T) {
	t.Parallel()

	changeSet := TextChangeSet{
		Edits: []TextEdit{
			{
				Span:    Span{StartByte: 1, EndByte: 4},
				NewText: "abc",
			},
		},
	}

	raw, err := json.Marshal(changeSet)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	const wantJSON = `{"edits":[{"span":{"start_byte":1,"end_byte":4},"new_text":"abc"}]}`
	if string(raw) != wantJSON {
		t.Fatalf("json.Marshal() = %s, want %s", raw, wantJSON)
	}

	var decoded TextChangeSet
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, changeSet) {
		t.Fatalf("round-trip mismatch: got %#v want %#v", decoded, changeSet)
	}
}
