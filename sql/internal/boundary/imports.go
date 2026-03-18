package boundary

import (
	goparser "go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// AssertNoImportPathPrefixes verifies package-local non-test files do not import forbidden prefixes.
func AssertNoImportPathPrefixes(t *testing.T, packageDir string, forbiddenPrefixes ...string) {
	t.Helper()

	goFiles, err := filepath.Glob(filepath.Join(packageDir, "*.go"))
	if err != nil {
		t.Fatalf("glob failed in %s: %v", packageDir, err)
	}
	if len(goFiles) == 0 {
		t.Fatalf("no go files found in %s", packageDir)
	}

	forbidden := slices.Clone(forbiddenPrefixes)
	slices.Sort(forbidden)

	checkedFiles := 0
	for _, file := range goFiles {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		checkedFiles++

		fset := token.NewFileSet()
		node, err := goparser.ParseFile(fset, file, nil, goparser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse imports in %s failed: %v", file, err)
		}

		for _, imp := range node.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatalf("unquote import path in %s failed: %v", file, err)
			}

			for _, prefix := range forbidden {
				if strings.HasPrefix(path, prefix) {
					t.Fatalf("%s imports forbidden package path %q (prefix %q)", file, path, prefix)
				}
			}
		}
	}

	if checkedFiles == 0 {
		t.Fatalf("no non-test go files found in %s", packageDir)
	}
}
