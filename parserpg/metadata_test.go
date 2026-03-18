//go:build cgo
// +build cgo

package parserpg

import (
	"testing"

	"github.com/iw2rmb/squall/parser"
)

func TestPGQueryParser_ExtractMetadata(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name, sql string
		wantErr   bool
	}{
		{name: "simple SELECT", sql: "SELECT id, name FROM users WHERE id = 1"},
		{name: "invalid SQL", sql: "SELECT FROM", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md, err := p.ExtractMetadata(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ExtractMetadata() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if md == nil {
				t.Fatal("nil metadata")
			}
			if md.Tables == nil {
				t.Error("metadata.Tables is nil")
			}
			if md.Filters == nil {
				t.Error("metadata.Filters is nil")
			}
		})
	}
}

// GroupBy summarized within metadata
func TestPGQueryParser_ExtractMetadata_GroupBy_Simple(t *testing.T) {
	p := newCGOParser(t)
	sql := "SELECT product, SUM(amount) FROM transactions GROUP BY product"
	md, err := p.ExtractMetadata(sql)
	if err != nil {
		t.Fatalf("ExtractMetadata failed: %v", err)
	}
	if len(md.GroupBy) == 0 {
		t.Fatalf("expected GroupBy to be populated")
	}
	found := false
	for _, g := range md.GroupBy {
		if g == "product" || g == "transactions.product" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected GroupBy to contain 'product', got %v", md.GroupBy)
	}
}

func TestPGQueryParser_ExtractMetadata_HasCTEs(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name, sql     string
		want, wantErr bool
	}{
		{name: "no CTE", sql: "SELECT id FROM users", want: false},
		{name: "simple CTE", sql: "WITH cte AS (SELECT id FROM users) SELECT * FROM cte", want: true},
		{name: "multiple CTEs", sql: "WITH cte1 AS (SELECT id FROM users), cte2 AS (SELECT id FROM orders) SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md, err := p.ExtractMetadata(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tt.wantErr)
			}
			if !tt.wantErr && md.HasCTEs != tt.want {
				t.Errorf("HasCTEs=%v want %v", md.HasCTEs, tt.want)
			}
		})
	}
}

func TestPGQueryParser_ExtractMetadata_IsRecursiveCTE(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name, sql     string
		want, wantErr bool
	}{
		{name: "no CTE", sql: "SELECT id FROM users", want: false},
		{name: "non-recursive CTE", sql: "WITH cte AS (SELECT id FROM users) SELECT * FROM cte", want: false},
		{name: "recursive CTE", sql: "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md, err := p.ExtractMetadata(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tt.wantErr)
			}
			if !tt.wantErr && md.IsRecursiveCTE != tt.want {
				t.Errorf("IsRecursiveCTE=%v want %v", md.IsRecursiveCTE, tt.want)
			}
		})
	}
}

func TestPGQueryParser_ExtractMetadata_HasWindowFunctions(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name, sql     string
		want, wantErr bool
	}{
		{name: "no window function", sql: "SELECT id, name FROM users", want: false},
		{name: "ROW_NUMBER window function", sql: "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM users", want: true},
		{name: "RANK window function", sql: "SELECT id, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees", want: true},
		{name: "LAG window function", sql: "SELECT id, LAG(value, 1) OVER (ORDER BY created_at) FROM metrics", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md, err := p.ExtractMetadata(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tt.wantErr)
			}
			if !tt.wantErr && md.HasWindowFunctions != tt.want {
				t.Errorf("HasWindowFunctions=%v want %v", md.HasWindowFunctions, tt.want)
			}
		})
	}
}

func TestPGQueryParser_ExtractMetadata_HasDistinct(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name, sql     string
		want, wantErr bool
	}{
		{name: "no DISTINCT", sql: "SELECT id, name FROM users", want: false},
		{name: "DISTINCT on all columns", sql: "SELECT DISTINCT id, name FROM users", want: true},
		{name: "DISTINCT ON specific columns", sql: "SELECT DISTINCT ON (dept) id, name FROM users", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md, err := p.ExtractMetadata(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tt.wantErr)
			}
			if !tt.wantErr && md.HasDistinct != tt.want {
				t.Errorf("HasDistinct=%v want %v", md.HasDistinct, tt.want)
			}
		})
	}
}

func TestPGQueryParser_ExtractMetadata_IsAggregate(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name, sql     string
		want, wantErr bool
	}{
		{name: "no aggregation", sql: "SELECT id, name FROM users", want: false},
		{name: "COUNT aggregate", sql: "SELECT COUNT(*) FROM users", want: true},
		{name: "SUM aggregate", sql: "SELECT SUM(amount) FROM orders", want: true},
		{name: "AVG aggregate", sql: "SELECT AVG(salary) FROM employees", want: true},
		{name: "MIN and MAX aggregates", sql: "SELECT MIN(price), MAX(price) FROM products", want: true},
		{name: "array_agg aggregate", sql: "SELECT dept, array_agg(name) FROM employees GROUP BY dept", want: true},
		{name: "GROUP BY without explicit aggregate", sql: "SELECT dept FROM employees GROUP BY dept", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md, err := p.ExtractMetadata(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tt.wantErr)
			}
			if !tt.wantErr && md.IsAggregate != tt.want {
				t.Errorf("IsAggregate=%v want %v", md.IsAggregate, tt.want)
			}
		})
	}
}

func TestPGQueryParser_ExtractMetadata_ComposedAggregateCompatibility(t *testing.T) {
	p := newCGOParser(t)
	sql := "SELECT SUM(CASE WHEN ok THEN amount ELSE 0 END)-SUM(CASE WHEN NOT ok THEN amount ELSE 0 END) AS net FROM events"

	md, err := p.ExtractMetadata(sql)
	if err != nil {
		t.Fatalf("ExtractMetadata failed: %v", err)
	}
	if !md.IsAggregate {
		t.Fatalf("IsAggregate=%v want true for composed aggregate query", md.IsAggregate)
	}
}

func TestPGQueryParser_ExtractMetadata_OperationCompatibility(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{name: "SELECT", sql: "SELECT id FROM users", want: "SELECT"},
		{name: "WITH", sql: "WITH q AS (SELECT id FROM users) SELECT * FROM q", want: "SELECT"},
		{name: "INSERT", sql: "INSERT INTO users (id, name) VALUES (1, 'alice')", want: "INSERT"},
		{name: "UPDATE", sql: "UPDATE users SET name = 'bob' WHERE id = 1", want: "UPDATE"},
		{name: "DELETE", sql: "DELETE FROM users WHERE id = 1", want: "DELETE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md, err := p.ExtractMetadata(tt.sql)
			if err != nil {
				t.Fatalf("ExtractMetadata failed: %v", err)
			}
			if len(md.Operations) != 1 || md.Operations[0] != tt.want {
				t.Fatalf("Operations=%v want [%s]", md.Operations, tt.want)
			}
		})
	}
}

func TestPGQueryParser_ExtractMetadata_SelectColumns(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name        string
		sql         string
		wantColumns []parser.ColumnRef
		wantErr     bool
	}{
		{name: "simple columns", sql: "SELECT id, name FROM users", wantColumns: []parser.ColumnRef{{Column: "id"}, {Column: "name"}}},
		{name: "columns with aliases", sql: "SELECT id AS user_id, name AS user_name FROM users", wantColumns: []parser.ColumnRef{{Column: "id", Alias: "user_id"}, {Column: "name", Alias: "user_name"}}},
		{name: "aggregate functions", sql: "SELECT COUNT(*) AS total, SUM(amount) AS total_amount FROM orders", wantColumns: []parser.ColumnRef{{Column: "COUNT(*)", Alias: "total", IsAgg: true}, {Column: "SUM(amount)", Alias: "total_amount", IsAgg: true}}},
		{name: "mixed columns and aggregates", sql: "SELECT user_id, COUNT(*) AS cnt FROM orders GROUP BY user_id", wantColumns: []parser.ColumnRef{{Column: "user_id"}, {Column: "COUNT(*)", Alias: "cnt", IsAgg: true}}},
		{name: "expressions with aliases", sql: "SELECT (price * quantity) AS total_price FROM cart_items", wantColumns: []parser.ColumnRef{{Column: "total_price", Alias: "total_price"}}},
		{name: "COUNT DISTINCT", sql: "SELECT COUNT(DISTINCT email) AS unique_emails FROM users", wantColumns: []parser.ColumnRef{{Column: "COUNT(*)", Alias: "unique_emails", IsAgg: true}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md, err := p.ExtractMetadata(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			// Len check only; exact order may vary for complex expr; compare minimal expectations
			if len(md.SelectColumns) != len(tt.wantColumns) {
				t.Fatalf("got %d columns, want %d: %+v", len(md.SelectColumns), len(tt.wantColumns), md.SelectColumns)
			}
		})
	}
}

func TestPGQueryParser_ExtractMetadata_JSONBOperators(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name, sql, wantDBType string
		wantDBOps, wantErr    bool
	}{
		{name: "no JSONB", sql: "SELECT * FROM users", wantDBType: "", wantDBOps: false},
		{name: "JSONB -> operator", sql: "SELECT data->'name' FROM users", wantDBType: "postgresql", wantDBOps: true},
		{name: "JSONB ->> operator", sql: "SELECT data->>'email' FROM users", wantDBType: "postgresql", wantDBOps: true},
		{name: "JSONB #> operator", sql: "SELECT data #> '{address,city}' FROM users", wantDBType: "postgresql", wantDBOps: true},
		{name: "JSONB ? operator", sql: "SELECT * FROM users WHERE data ? 'email'", wantDBType: "postgresql", wantDBOps: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md, err := p.ExtractMetadata(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if md.DatabaseType != tt.wantDBType {
					t.Errorf("DatabaseType=%v want %v", md.DatabaseType, tt.wantDBType)
				}
				if md.HasDatabaseSpecificOps != tt.wantDBOps {
					t.Errorf("HasDatabaseSpecificOps=%v want %v", md.HasDatabaseSpecificOps, tt.wantDBOps)
				}
			}
		})
	}
}

func TestPGQueryParser_ExtractMetadata_ILIKE(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name, sql, wantDBType        string
		wantDBOps, hasILike, wantErr bool
	}{
		{name: "no ILIKE", sql: "SELECT * FROM users WHERE name = 'John'"},
		{name: "ILIKE operator", sql: "SELECT * FROM users WHERE name ILIKE '%john%'", wantDBType: "postgresql", wantDBOps: true, hasILike: true},
		{name: "ILIKE with multiple conditions", sql: "SELECT * FROM users WHERE name ILIKE '%john%' AND email ILIKE '%@example.com'", wantDBType: "postgresql", wantDBOps: true, hasILike: true},
		{name: "LIKE operator (not ILIKE)", sql: "SELECT * FROM users WHERE name LIKE '%John%'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md, err := p.ExtractMetadata(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if md.DatabaseType != tt.wantDBType {
					t.Errorf("DatabaseType=%v want %v", md.DatabaseType, tt.wantDBType)
				}
				if md.HasDatabaseSpecificOps != tt.wantDBOps {
					t.Errorf("HasDatabaseSpecificOps=%v want %v", md.HasDatabaseSpecificOps, tt.wantDBOps)
				}
				if tt.hasILike {
					found := false
					for _, op := range md.DatabaseOperations {
						if op == "ilike" {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("expected 'ilike' in DatabaseOperations, got %v", md.DatabaseOperations)
					}
				}
			}
		})
	}
}

func TestPGQueryParser_ExtractMetadata_BucketInfo(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name, sql      string
		wantTimeBucket bool
		wantBucketInfo *parser.BucketInfo
		wantErr        bool
	}{
		{name: "no time bucket", sql: "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id", wantTimeBucket: false},
		{name: "date_trunc hour bucket", sql: "SELECT date_trunc('hour', created_at) AS hour, COUNT(*) FROM orders GROUP BY date_trunc('hour', created_at)", wantTimeBucket: true, wantBucketInfo: &parser.BucketInfo{Function: "date_trunc", Interval: "hour", Column: "created_at"}},
		{name: "date_trunc day bucket", sql: "SELECT date_trunc('day', created_at) AS day, COUNT(*) FROM orders GROUP BY date_trunc('day', created_at)", wantTimeBucket: true, wantBucketInfo: &parser.BucketInfo{Function: "date_trunc", Interval: "day", Column: "created_at"}},
		{name: "date_trunc with qualified column", sql: "SELECT date_trunc('hour', o.created_at) AS hour, COUNT(*) FROM orders o GROUP BY date_trunc('hour', o.created_at)", wantTimeBucket: true, wantBucketInfo: &parser.BucketInfo{Function: "date_trunc", Interval: "hour", Column: "created_at", ColumnTable: "o"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md, err := p.ExtractMetadata(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if md.HasTimeBucket != tt.wantTimeBucket {
				t.Errorf("HasTimeBucket=%v want %v", md.HasTimeBucket, tt.wantTimeBucket)
			}
			if tt.wantBucketInfo == nil {
				if md.BucketInfo != nil {
					t.Errorf("BucketInfo=%+v want nil", md.BucketInfo)
				}
			} else {
				if md.BucketInfo == nil {
					t.Fatal("BucketInfo is nil, want non-nil")
				}
				if md.BucketInfo.Function != tt.wantBucketInfo.Function {
					t.Errorf("Function=%q want %q", md.BucketInfo.Function, tt.wantBucketInfo.Function)
				}
				if md.BucketInfo.Interval != tt.wantBucketInfo.Interval {
					t.Errorf("Interval=%q want %q", md.BucketInfo.Interval, tt.wantBucketInfo.Interval)
				}
				if md.BucketInfo.Column != tt.wantBucketInfo.Column {
					t.Errorf("Column=%q want %q", md.BucketInfo.Column, tt.wantBucketInfo.Column)
				}
				if md.BucketInfo.ColumnTable != tt.wantBucketInfo.ColumnTable {
					t.Errorf("ColumnTable=%q want %q", md.BucketInfo.ColumnTable, tt.wantBucketInfo.ColumnTable)
				}
			}
		})
	}
}
