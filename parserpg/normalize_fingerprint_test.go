//go:build cgo
// +build cgo

package parserpg

import (
	"strings"
	"testing"
)

func TestPGQueryParser_NormalizeQuery(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name, sql string
		wantErr   bool
	}{
		{name: "simple SELECT", sql: "SELECT id, name FROM users WHERE id = 1"},
		{name: "SELECT with different whitespace", sql: "SELECT   id,name   FROM users  WHERE id=1"},
		{name: "SELECT with different case", sql: "select ID, NAME from USERS where ID = 1"},
		{name: "SELECT with literals", sql: "SELECT * FROM users WHERE name = 'John'"},
		{name: "invalid SQL", sql: "SELECT FROM", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			normalized, err := p.NormalizeQuery(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NormalizeQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && normalized == "" {
				t.Fatal("NormalizeQuery() returned empty string for valid SQL")
			}
		})
	}
}

func TestPGQueryParser_NormalizeQuery_Stability(t *testing.T) {
	p := newCGOParser(t)
	queries := []string{
		"SELECT id, name FROM users WHERE id = 1",
		"SELECT id, name FROM users WHERE id = 999",
		"SELECT id, name FROM users WHERE id = 42",
	}
	var normalized []string
	for _, sql := range queries {
		result, err := p.NormalizeQuery(sql)
		if err != nil {
			t.Fatalf("NormalizeQuery(%q) failed: %v", sql, err)
		}
		normalized = append(normalized, result)
	}
	for i := 1; i < len(normalized); i++ {
		if normalized[i] != normalized[0] {
			t.Errorf("Normalized query %d differs from query 0:\n  [0]: %s\n  [%d]: %s", i, normalized[0], i, normalized[i])
		}
	}
	if !strings.Contains(normalized[0], "$1") {
		t.Errorf("Expected normalized query to contain placeholder $1, got: %s", normalized[0])
	}
}

func TestPGQueryParser_GenerateFingerprint(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name, sql string
		wantErr   bool
	}{
		{name: "simple SELECT", sql: "SELECT id, name FROM users WHERE id = 1"},
		{name: "SELECT with string literal", sql: "SELECT * FROM users WHERE name = 'John'"},
		{name: "SELECT with numeric literal", sql: "SELECT * FROM orders WHERE total > 100"},
		{name: "invalid SQL", sql: "SELECT FROM", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fingerprint, err := p.GenerateFingerprint(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("GenerateFingerprint() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && fingerprint == "" {
				t.Fatal("GenerateFingerprint() returned empty string for valid SQL")
			}
		})
	}
}

func TestPGQueryParser_GenerateFingerprint_Stability(t *testing.T) {
	p := newCGOParser(t)
	queryPairs := []struct {
		name, sql1, sql2 string
		same             bool
	}{
		{name: "different numeric literals", sql1: "SELECT * FROM users WHERE id = 1", sql2: "SELECT * FROM users WHERE id = 999", same: true},
		{name: "different string literals", sql1: "SELECT * FROM users WHERE name = 'Alice'", sql2: "SELECT * FROM users WHERE name = 'Bob'", same: true},
		{name: "different columns", sql1: "SELECT id FROM users WHERE id = 1", sql2: "SELECT name FROM users WHERE id = 1", same: false},
		{name: "different tables", sql1: "SELECT * FROM users WHERE id = 1", sql2: "SELECT * FROM orders WHERE id = 1", same: false},
	}
	for _, tc := range queryPairs {
		t.Run(tc.name, func(t *testing.T) {
			fp1, err := p.GenerateFingerprint(tc.sql1)
			if err != nil {
				t.Fatalf("fp1: %v", err)
			}
			fp2, err := p.GenerateFingerprint(tc.sql2)
			if err != nil {
				t.Fatalf("fp2: %v", err)
			}
			if tc.same && fp1 != fp2 {
				t.Errorf("Expected same fingerprint, got\nSQL1:%s\nSQL2:%s\nFP1:%s\nFP2:%s", tc.sql1, tc.sql2, fp1, fp2)
			}
			if !tc.same && fp1 == fp2 {
				t.Errorf("Expected different fingerprints, got one: %s", fp1)
			}
		})
	}
}
