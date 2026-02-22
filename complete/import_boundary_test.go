package complete

import (
	goparser "go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestImportBoundary(t *testing.T) {
	t.Parallel()

	goFiles, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}

	allowed := map[string]bool{
		"github.com/iw2rmb/squal/core":   true,
		"github.com/iw2rmb/squal/parser": true,
	}
	forbiddenSuffix := "parser" + "pg"

	for _, file := range goFiles {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}

		fset := token.NewFileSet()
		node, err := goparser.ParseFile(fset, file, nil, goparser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s failed: %v", file, err)
		}

		for _, imp := range node.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatalf("unquote import path in %s failed: %v", file, err)
			}

			if strings.HasPrefix(path, "github.com/iw2rmb/squal/") && !allowed[path] {
				t.Fatalf("%s imports forbidden sql package %q", file, path)
			}
			if strings.HasSuffix(path, "/"+forbiddenSuffix) || path == forbiddenSuffix {
				t.Fatalf("%s imports forbidden package %q", file, path)
			}
		}
	}
}

func TestFilesPresent(t *testing.T) {
	t.Parallel()

	required := []string{
		"doc.go",
		"types.go",
		"engine.go",
		"diagnostics.go",
	}
	for _, name := range required {
		if _, err := os.Stat(name); err != nil {
			t.Fatalf("required skeleton file missing: %s (%v)", name, err)
		}
	}
}
