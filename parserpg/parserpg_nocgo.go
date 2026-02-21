//go:build !cgo
// +build !cgo

package parserpg

// PGQueryParser is unavailable when CGO is disabled.
type PGQueryParser struct{}

// NewPGQueryParser returns an explicit error when CGO is unavailable.
func NewPGQueryParser() (*PGQueryParser, error) {
	return nil, ErrCGODisabled
}
