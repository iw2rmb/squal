package routing

import (
	"testing"

	"github.com/iw2rmb/squal/sql/internal/boundary"
)

func TestImportBoundary(t *testing.T) {
	t.Parallel()
	boundary.AssertNoImportPathPrefixes(
		t,
		".",
		"github.com/iw2rmb/mill/internal",
		"github.com/iw2rmb/mill/internal/mill/cache",
	)
}
