package decomposition

import (
	"go/doc"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// Test that package docs include subquery extractor caveats.
func TestSubqueryExtractorDocsMentionHeuristicsAndLimitations(t *testing.T) {
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

	// Key phrases that must appear in the package docs after adding the file intro.
	mustContain := []string{
		"heuristics-first",
		"parser relationship",
		"known limitations",
		"false positives",
	}

	for _, phrase := range mustContain {
		if !strings.Contains(docText, phrase) {
			t.Fatalf("package doc missing phrase %q in: %q", phrase, dp.Doc)
		}
	}
}
