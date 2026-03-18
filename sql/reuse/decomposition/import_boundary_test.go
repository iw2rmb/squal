package decomposition

import (
	"testing"

	"github.com/iw2rmb/squall/sql/internal/boundary"
)

func TestImportBoundary(t *testing.T) {
	t.Parallel()
	boundary.AssertNoImportPathPrefixes(t, ".", "github.com/iw2rmb/mill/internal")
}
