//go:build !cgo
// +build !cgo

package parserpg

import (
	"errors"
	"testing"
)

func TestNewPGQueryParserNoCGO(t *testing.T) {
	p, err := NewPGQueryParser()
	if p != nil {
		t.Fatal("NewPGQueryParser() returned parser without CGO")
	}
	if !errors.Is(err, ErrCGODisabled) {
		t.Fatalf("NewPGQueryParser() error mismatch: got %v, want %v", err, ErrCGODisabled)
	}
}
