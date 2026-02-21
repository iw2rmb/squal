//go:build cgo
// +build cgo

package parserpg

import "testing"

func TestNewPGQueryParserCGO(t *testing.T) {
	p, err := NewPGQueryParser()
	if err != nil {
		t.Fatalf("NewPGQueryParser() unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("NewPGQueryParser() returned nil parser")
	}
}
