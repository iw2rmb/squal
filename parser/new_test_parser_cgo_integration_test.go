//go:build cgo
// +build cgo

package parser_test

import (
	"testing"

	"github.com/iw2rmb/squall/parser"
	"github.com/iw2rmb/squall/parserpg"
)

func TestNewTestParserUsesParserPGFactory(t *testing.T) {
	got := parser.NewTestParser()
	if got == nil {
		t.Fatal("parser.NewTestParser() returned nil")
	}
	if _, ok := got.(*parserpg.PGQueryParser); !ok {
		t.Fatalf("parser.NewTestParser() returned %T, want *parserpg.PGQueryParser", got)
	}
}
