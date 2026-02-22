//go:build cgo
// +build cgo

package parserpg

import (
	"testing"
)

func TestPGQueryParser_ExtractTables(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name       string
		sql        string
		wantTables []string
		wantErr    bool
	}{
		{name: "simple SELECT", sql: "SELECT * FROM users", wantTables: []string{"users"}},
		{name: "SELECT with JOIN", sql: "SELECT * FROM users u JOIN orders o ON u.id = o.user_id", wantTables: []string{"users", "orders"}},
		{name: "SELECT with multiple JOINs", sql: "SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id", wantTables: []string{"users", "orders", "products"}},
		{name: "SELECT with subquery", sql: "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)", wantTables: []string{"users", "orders"}},
		{name: "SELECT with CTE", sql: "WITH recent_orders AS (SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '1 day') SELECT * FROM recent_orders", wantTables: []string{"orders", "recent_orders"}},
		{name: "INSERT with SELECT", sql: "INSERT INTO archive SELECT * FROM users WHERE active = false", wantTables: []string{"archive", "users"}},
		{name: "UPDATE with FROM", sql: "UPDATE users u SET email = o.email FROM other_users o WHERE u.id = o.id", wantTables: []string{"users", "other_users"}},
		{name: "DELETE with USING", sql: "DELETE FROM users u USING old_users o WHERE u.id = o.id", wantTables: []string{"users", "old_users"}},
		{name: "invalid SQL", sql: "SELECT FROM", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tables, err := p.ExtractTables(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ExtractTables() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if tables == nil {
				t.Fatal("ExtractTables() returned nil slice")
			}
			// order-independent check
			got := map[string]bool{}
			for _, tbl := range tables {
				got[tbl] = true
			}
			for _, w := range tt.wantTables {
				if !got[w] {
					t.Fatalf("missing table %q, got %v", w, tables)
				}
			}
			if len(tables) != len(tt.wantTables) {
				t.Fatalf("got %d tables, want %d: %v vs %v", len(tables), len(tt.wantTables), tables, tt.wantTables)
			}
		})
	}
}
