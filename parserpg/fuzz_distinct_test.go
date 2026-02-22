//go:build cgo
// +build cgo

package parserpg

import "testing"

// FuzzExtractDistinctSpec validates that the DISTINCT extractor never panics
// on arbitrary SQL inputs. It allows errors from the underlying parser.
func FuzzExtractDistinctSpec(f *testing.F) {
	p := newCGOParser(f)
	for _, seed := range []string{
		"SELECT 1",
		"SELECT DISTINCT * FROM t",
		"SELECT DISTINCT col FROM t",
		"SELECT DISTINCT a, b FROM t",
		"SELECT COUNT(DISTINCT col) FROM t",
		"SELECT DISTINCT ON (user_id) user_id, created_at FROM t ORDER BY user_id, created_at DESC",
		"WITH x AS (SELECT 1) SELECT DISTINCT * FROM x",
		"SELECT DISTINCT lower(email) FROM users",
		"SELECT COUNT(DISTINCT u.id) FROM users u",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, sql string) {
		// No panics; either a spec is returned or an error.
		_, _ = p.ExtractDistinctSpec(sql)
	})
}
