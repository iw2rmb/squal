//go:build cgo
// +build cgo

package parserpg

import (
	"testing"

	"github.com/iw2rmb/squall/parser"
)

func TestPGQueryParser_ExtractAggregates(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name    string
		sql     string
		want    []parser.Aggregate
		wantErr bool
	}{
		{name: "COUNT(*)", sql: "SELECT COUNT(*) FROM users", want: []parser.Aggregate{{Func: "COUNT", Column: "*", Alias: "", Distinct: false}}},
		{name: "COUNT(*) with alias", sql: "SELECT COUNT(*) AS total FROM users", want: []parser.Aggregate{{Func: "COUNT", Column: "*", Alias: "total", Distinct: false}}},
		{name: "COUNT(column)", sql: "SELECT COUNT(id) FROM users", want: []parser.Aggregate{{Func: "COUNT", Column: "id", Alias: "", Distinct: false}}},
		{name: "COUNT(DISTINCT column)", sql: "SELECT COUNT(DISTINCT email) FROM users", want: []parser.Aggregate{{Func: "COUNT", Column: "email", Alias: "", Distinct: true}}},
		{name: "SUM(column)", sql: "SELECT SUM(amount) FROM transactions", want: []parser.Aggregate{{Func: "SUM", Column: "amount", Alias: "", Distinct: false}}},
		{name: "SUM with alias", sql: "SELECT SUM(amount) AS total_amount FROM transactions", want: []parser.Aggregate{{Func: "SUM", Column: "amount", Alias: "total_amount", Distinct: false}}},
		{name: "AVG(column)", sql: "SELECT AVG(price) FROM products", want: []parser.Aggregate{{Func: "AVG", Column: "price", Alias: "", Distinct: false}}},
		{name: "MIN and MAX", sql: "SELECT MIN(price), MAX(price) FROM products", want: []parser.Aggregate{{Func: "MIN", Column: "price", Alias: "", Distinct: false}, {Func: "MAX", Column: "price", Alias: "", Distinct: false}}},
		{name: "multiple aggregates with aliases", sql: "SELECT COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount) AS avg_amt FROM transactions", want: []parser.Aggregate{{Func: "COUNT", Column: "*", Alias: "cnt", Distinct: false}, {Func: "SUM", Column: "amount", Alias: "total", Distinct: false}, {Func: "AVG", Column: "amount", Alias: "avg_amt", Distinct: false}}},
		{name: "aggregate with table qualifier", sql: "SELECT COUNT(t.id) FROM transactions t", want: []parser.Aggregate{{Func: "COUNT", Column: "id", Table: "t", Alias: "", Distinct: false}}},
		{name: "SUM(DISTINCT column)", sql: "SELECT SUM(DISTINCT amount) FROM transactions", want: []parser.Aggregate{{Func: "SUM", Column: "amount", Alias: "", Distinct: true}}},
		{name: "aggregates with GROUP BY", sql: "SELECT category, COUNT(*), SUM(amount) FROM transactions GROUP BY category", want: []parser.Aggregate{{Func: "COUNT", Column: "*", Alias: "", Distinct: false}, {Func: "SUM", Column: "amount", Alias: "", Distinct: false}}},
		{name: "aggregate with CASE expression", sql: "SELECT SUM(CASE WHEN status = 'active' THEN amount ELSE 0 END) AS active_total FROM transactions", want: []parser.Aggregate{{Func: "SUM", Column: "", Alias: "active_total", Distinct: false}}},
		{name: "no aggregates", sql: "SELECT id, name FROM users", want: []parser.Aggregate{}},
		{name: "invalid SQL", sql: "SELECT FROM WHERE", wantErr: true},
		{name: "mixed columns and aggregates", sql: "SELECT user_id, COUNT(*), MAX(created_at) FROM orders GROUP BY user_id", want: []parser.Aggregate{{Func: "COUNT", Column: "*"}, {Func: "MAX", Column: "created_at"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.ExtractAggregates(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ExtractAggregates() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d want %d\nGot:%+v\nWant:%+v", len(got), len(tt.want), got, tt.want)
			}
		})
	}
}

func TestPGQueryParser_ExtractCaseAggregates(t *testing.T) {
	p := newCGOParser(t)
	floatPtr := func(f float64) *float64 { return &f }
	tests := []struct {
		name    string
		sql     string
		want    []parser.AggCase
		wantErr bool
	}{
		{name: "no CASE aggregates", sql: "SELECT id, name FROM users", want: []parser.AggCase{}},
		{name: "SUM(CASE WHEN) with column", sql: "SELECT SUM(CASE WHEN status = 'active' THEN amount ELSE 0 END) AS active_sum FROM transactions", want: []parser.AggCase{{Func: "SUM", Alias: "active_sum", Conditions: []parser.AggCondition{{Kind: "eq", Column: parser.ColumnRef{Column: "status"}, Value: "active"}}, ThenColumn: "amount", ElseConst: floatPtr(0)}}},
		{name: "SUM(CASE WHEN) with const", sql: "SELECT SUM(CASE WHEN type = 'deposit' THEN 1 ELSE 0 END) AS deposit_count FROM transactions", want: []parser.AggCase{{Func: "COUNT", Alias: "deposit_count", Conditions: []parser.AggCondition{{Kind: "eq", Column: parser.ColumnRef{Column: "type"}, Value: "deposit"}}, ThenConst: floatPtr(1), ElseConst: floatPtr(0)}}},
		{name: "COUNT(CASE WHEN)", sql: "SELECT COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending_count FROM orders", want: []parser.AggCase{{Func: "COUNT", Alias: "pending_count", Conditions: []parser.AggCondition{{Kind: "eq", Column: parser.ColumnRef{Column: "status"}, Value: "pending"}}, ThenConst: floatPtr(1)}}},
		{name: "multiple CASE aggregates", sql: "SELECT SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END) AS credits, SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END) AS debits FROM transactions", want: []parser.AggCase{{Func: "SUM", Alias: "credits", Conditions: []parser.AggCondition{{Kind: "eq", Column: parser.ColumnRef{Column: "type"}, Value: "credit"}}, ThenColumn: "amount", ElseConst: floatPtr(0)}, {Func: "SUM", Alias: "debits", Conditions: []parser.AggCondition{{Kind: "eq", Column: parser.ColumnRef{Column: "type"}, Value: "debit"}}, ThenColumn: "amount", ElseConst: floatPtr(0)}}},
		{name: "CASE with numeric condition", sql: "SELECT SUM(CASE WHEN priority = 1 THEN amount ELSE 0 END) AS high_priority FROM orders", want: []parser.AggCase{{Func: "SUM", Alias: "high_priority", Conditions: []parser.AggCondition{{Kind: "eq", Column: parser.ColumnRef{Column: "priority"}, Value: int64(1)}}, ThenColumn: "amount", ElseConst: floatPtr(0)}}},
		{name: "CASE with AND conditions", sql: "SELECT SUM(CASE WHEN status = 'active' AND region = 'US' THEN amount ELSE 0 END) AS us_active FROM sales", want: []parser.AggCase{{Func: "SUM", Alias: "us_active", Conditions: []parser.AggCondition{{Kind: "eq", Column: parser.ColumnRef{Column: "status"}, Value: "active"}, {Kind: "eq", Column: parser.ColumnRef{Column: "region"}, Value: "US"}}, ThenColumn: "amount", ElseConst: floatPtr(0)}}},
		{name: "CASE aggregate without alias", sql: "SELECT SUM(CASE WHEN active = true THEN 1 ELSE 0 END) FROM users", want: []parser.AggCase{{Func: "COUNT", Alias: "", Conditions: []parser.AggCondition{{Kind: "eq", Column: parser.ColumnRef{Column: "active"}, Value: true}}, ThenConst: floatPtr(1), ElseConst: floatPtr(0)}}},
		{name: "CASE with table-qualified column", sql: "SELECT SUM(CASE WHEN t.status = 'complete' THEN t.amount ELSE 0 END) AS completed FROM transactions t", want: []parser.AggCase{{Func: "SUM", Alias: "completed", Conditions: []parser.AggCondition{{Kind: "eq", Column: parser.ColumnRef{Table: "t", Column: "status"}, Value: "complete"}}, ThenColumn: "amount", ElseConst: floatPtr(0)}}},
		{name: "invalid SQL", sql: "SELECT FROM WHERE", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.ExtractCaseAggregates(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ExtractCaseAggregates() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d want %d\nGot:%+v\nWant:%+v", len(got), len(tt.want), got, tt.want)
			}
		})
	}
}
