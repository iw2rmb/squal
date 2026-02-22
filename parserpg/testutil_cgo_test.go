//go:build cgo
// +build cgo

package parserpg

import "testing"

func newCGOParser(tb testing.TB) *PGQueryParser {
	tb.Helper()
	p, err := NewPGQueryParser()
	if err != nil {
		tb.Fatalf("NewPGQueryParser() unexpected error: %v", err)
	}
	return p
}
