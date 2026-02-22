//go:build cgo
// +build cgo

package parserpg

import (
	"testing"

	"github.com/iw2rmb/sql/parser"
)

func TestPGQueryParser_ExtractTemporalOps(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name             string
		sql              string
		wantHasNow       bool
		wantHasDateTrunc bool
		wantRangeCount   int
		wantRanges       []parser.TimeRange
		wantErr          bool
	}{
		{name: "no temporal ops", sql: "SELECT id, name FROM users", wantHasNow: false, wantHasDateTrunc: false, wantRangeCount: 0},
		{name: "NOW() in SELECT", sql: "SELECT NOW(), id FROM users", wantHasNow: true, wantRangeCount: 0},
		{name: "CURRENT_TIMESTAMP in SELECT", sql: "SELECT CURRENT_TIMESTAMP, id FROM users", wantHasNow: true, wantRangeCount: 0},
		{name: "DATE_TRUNC in SELECT", sql: "SELECT DATE_TRUNC('hour', created_at) FROM events", wantHasDateTrunc: true},
		{name: "DATE_TRUNC in GROUP BY", sql: "SELECT DATE_TRUNC('day', created_at) AS day, COUNT(*) FROM events GROUP BY DATE_TRUNC('day', created_at)", wantHasDateTrunc: true},
		{name: "NOW() - INTERVAL in WHERE", sql: "SELECT * FROM events WHERE created_at > NOW() - INTERVAL '1 day'", wantHasNow: true, wantRangeCount: 1, wantRanges: []parser.TimeRange{{Column: "created_at", Kind: "now_minus_interval", Interval: "1 day", Operator: ">"}}},
		{name: "NOW() - INTERVAL with 7 days", sql: "SELECT * FROM events WHERE created_at >= NOW() - INTERVAL '7 days'", wantHasNow: true, wantRangeCount: 1, wantRanges: []parser.TimeRange{{Column: "created_at", Kind: "now_minus_interval", Interval: "7 days", Operator: ">="}}},
		{name: "BETWEEN with timestamps", sql: "SELECT * FROM events WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'", wantRangeCount: 1, wantRanges: []parser.TimeRange{{Column: "created_at", Kind: "between", StartTime: "2024-01-01", EndTime: "2024-12-31"}}},
		{name: "direct timestamp comparison", sql: "SELECT * FROM events WHERE created_at > '2024-01-01'", wantRangeCount: 1, wantRanges: []parser.TimeRange{{Column: "created_at", Kind: "greater_than", Operator: ">", StartTime: "2024-01-01"}}},
		{name: "multiple temporal ops", sql: "SELECT DATE_TRUNC('hour', created_at), NOW() FROM events WHERE created_at > NOW() - INTERVAL '1 hour'", wantHasNow: true, wantHasDateTrunc: true, wantRangeCount: 1, wantRanges: []parser.TimeRange{{Column: "created_at", Kind: "now_minus_interval", Interval: "1 hour", Operator: ">"}}},
		{name: "NOW() - INTERVAL with AND conditions", sql: "SELECT * FROM events WHERE created_at > NOW() - INTERVAL '1 day' AND status = 'active'", wantHasNow: true, wantRangeCount: 1, wantRanges: []parser.TimeRange{{Column: "created_at", Kind: "now_minus_interval", Interval: "1 day", Operator: ">"}}},
		{name: "table-qualified column", sql: "SELECT * FROM events e WHERE e.created_at > NOW() - INTERVAL '1 day'", wantHasNow: true, wantRangeCount: 1, wantRanges: []parser.TimeRange{{Column: "created_at", Table: "e", Kind: "now_minus_interval", Interval: "1 day", Operator: ">"}}},
		{name: "multiple time ranges with OR", sql: "SELECT * FROM events WHERE created_at > '2024-01-01' OR updated_at > '2024-06-01'", wantRangeCount: 2},
		{name: "invalid SQL", sql: "SELECT FROM WHERE", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.ExtractTemporalOps(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ExtractTemporalOps() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got.HasNow != tt.wantHasNow {
				t.Errorf("HasNow=%v want %v", got.HasNow, tt.wantHasNow)
			}
			if got.HasDateTrunc != tt.wantHasDateTrunc {
				t.Errorf("HasDateTrunc=%v want %v", got.HasDateTrunc, tt.wantHasDateTrunc)
			}
			if len(got.WhereRanges) != tt.wantRangeCount {
				t.Fatalf("WhereRanges=%d want %d: %+v", len(got.WhereRanges), tt.wantRangeCount, got.WhereRanges)
			}
		})
	}
}
