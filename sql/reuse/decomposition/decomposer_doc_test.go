package decomposition

import (
	"go/doc"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// Test that the package documentation added in decomposer.go is visible
// via go/doc and contains the intended sections.
func TestPackageDocMentionsPlanAndFailure(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse dir: %v", err)
	}
	p, ok := pkgs["decomposition"]
	if !ok {
		t.Fatalf("package decomposition not found")
	}
	dp := doc.New(p, "github.com/iw2rmb/squall/sql/reuse/decomposition", 0)
	docText := strings.ToLower(dp.Doc)

	if !strings.Contains(docText, "plan structure") {
		t.Fatalf("package doc missing 'Plan structure' section: %q", dp.Doc)
	}
	if !strings.Contains(docText, "failure behavior") {
		t.Fatalf("package doc missing 'Failure behavior' section: %q", dp.Doc)
	}
}
