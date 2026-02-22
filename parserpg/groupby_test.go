//go:build cgo
// +build cgo

package parserpg

import (
	"testing"

	"github.com/iw2rmb/squal/parser"
)

func TestPGQueryParser_ExtractGroupBy(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name    string
		sql     string
		want    []parser.GroupItem
		wantErr bool
	}{
		{name: "no GROUP BY", sql: "SELECT id, name FROM users", want: []parser.GroupItem{}},
		{name: "GROUP BY single column", sql: "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id", want: []parser.GroupItem{{Kind: "column", Column: "user_id"}}},
		{name: "GROUP BY multiple columns", sql: "SELECT user_id, product_id, COUNT(*) FROM orders GROUP BY user_id, product_id", want: []parser.GroupItem{{Kind: "column", Column: "user_id"}, {Kind: "column", Column: "product_id"}}},
		{name: "GROUP BY with table qualifier", sql: "SELECT o.user_id, COUNT(*) FROM orders o GROUP BY o.user_id", want: []parser.GroupItem{{Kind: "column", Column: "user_id", Table: "o"}}},
		{name: "GROUP BY positional (1)", sql: "SELECT user_id, COUNT(*) FROM orders GROUP BY 1", want: []parser.GroupItem{{Kind: "positional", Position: 1, Column: "user_id"}}},
		{name: "GROUP BY positional with alias", sql: "SELECT user_id AS uid, COUNT(*) FROM orders GROUP BY 1", want: []parser.GroupItem{{Kind: "positional", Position: 1, Alias: "uid"}}},
		{name: "GROUP BY multiple positional", sql: "SELECT user_id, product_id, COUNT(*) FROM orders GROUP BY 1, 2", want: []parser.GroupItem{{Kind: "positional", Position: 1, Column: "user_id"}, {Kind: "positional", Position: 2, Column: "product_id"}}},
		{name: "GROUP BY function (date_trunc)", sql: "SELECT date_trunc('hour', created_at) AS hour, COUNT(*) FROM orders GROUP BY date_trunc('hour', created_at)", want: []parser.GroupItem{{Kind: "function", RawExpr: "date_trunc(...)"}}},
		{name: "GROUP BY mixed column and function", sql: "SELECT user_id, date_trunc('day', created_at) AS day, COUNT(*) FROM orders GROUP BY user_id, date_trunc('day', created_at)", want: []parser.GroupItem{{Kind: "column", Column: "user_id"}, {Kind: "function", RawExpr: "date_trunc(...)"}}},
		{name: "GROUP BY alias reference", sql: "SELECT user_id * 2 AS double_id, COUNT(*) FROM orders GROUP BY double_id", want: []parser.GroupItem{{Kind: "alias", Alias: "double_id"}}},
		{name: "GROUP BY positional referencing function", sql: "SELECT date_trunc('hour', created_at), COUNT(*) FROM orders GROUP BY 1", want: []parser.GroupItem{{Kind: "positional", Position: 1, RawExpr: "date_trunc(...)"}}},
		{name: "GROUP BY with qualified and unqualified columns", sql: "SELECT u.id, o.user_id, COUNT(*) FROM orders o JOIN users u ON o.user_id = u.id GROUP BY u.id, o.user_id", want: []parser.GroupItem{{Kind: "column", Column: "id", Table: "u"}, {Kind: "column", Column: "user_id", Table: "o"}}},
		{name: "GROUP BY with ROLLUP", sql: "SELECT category, product_id, COUNT(*) FROM orders GROUP BY ROLLUP(category, product_id)", want: []parser.GroupItem{{Kind: "expression", RawExpr: "<expr>"}}},
		{name: "GROUP BY with CUBE", sql: "SELECT category, product_id, COUNT(*) FROM orders GROUP BY CUBE(category, product_id)", want: []parser.GroupItem{{Kind: "expression", RawExpr: "<expr>"}}},
		{name: "GROUP BY column matching an alias name", sql: "SELECT status AS status, COUNT(*) FROM orders GROUP BY status", want: []parser.GroupItem{{Kind: "column", Column: "status"}}},
		{name: "GROUP BY with EXTRACT function", sql: "SELECT EXTRACT(year FROM created_at) AS year, COUNT(*) FROM orders GROUP BY EXTRACT(year FROM created_at)", want: []parser.GroupItem{{Kind: "function", RawExpr: "extract(...)"}}},
		{name: "GROUP BY three columns ordered", sql: "SELECT a, b, c, COUNT(*) FROM t GROUP BY a, b, c", want: []parser.GroupItem{{Kind: "column", Column: "a"}, {Kind: "column", Column: "b"}, {Kind: "column", Column: "c"}}},
		{name: "GROUP BY with complex expression", sql: "SELECT (amount * 1.1) AS adjusted, COUNT(*) FROM orders GROUP BY (amount * 1.1)", want: []parser.GroupItem{{Kind: "expression", RawExpr: "<expr>"}}},
		{name: "invalid SQL", sql: "SELECT FROM WHERE", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			items, err := p.ExtractGroupBy(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ExtractGroupBy() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if items == nil {
				t.Fatal("nil items")
			}
			if len(items) != len(tt.want) {
				t.Fatalf("got %d want %d\nGot:%+v\nWant:%+v", len(items), len(tt.want), items, tt.want)
			}
		})
	}
}
