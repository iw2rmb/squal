package parserpg

import "errors"

// ErrCGODisabled indicates parserpg cannot be constructed without CGO.
var ErrCGODisabled = errors.New("parserpg: cgo is required")
