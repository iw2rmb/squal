//go:build cgo
// +build cgo

package parserpg

import (
	"reflect"
	"testing"

	"github.com/iw2rmb/squal/parser"
)

func TestPGQueryParser_ExtractJSONPaths(t *testing.T) {
	p := newCGOParser(t)
	tests := []struct {
		name    string
		sql     string
		want    []parser.JSONPath
		wantErr bool
	}{
		{name: "no JSON paths", sql: "SELECT id, name FROM users", want: []parser.JSONPath{}},
		{name: "single level -> operator", sql: "SELECT profile->'name' FROM users", want: []parser.JSONPath{{Table: "users", Column: "profile", Path: []string{"name"}, Operator: "->", IsText: false}}},
		{name: "single level ->> operator (text)", sql: "SELECT profile->>'email' FROM users", want: []parser.JSONPath{{Table: "users", Column: "profile", Path: []string{"email"}, Operator: "->>", IsText: true}}},
		{name: "chained -> operators", sql: "SELECT profile->'settings'->'theme' FROM users", want: []parser.JSONPath{{Table: "users", Column: "profile", Path: []string{"settings", "theme"}, Operator: "->", IsText: false}}},
		{name: "chained with ->> at end", sql: "SELECT profile->'settings'->>'theme' FROM users", want: []parser.JSONPath{{Table: "users", Column: "profile", Path: []string{"settings", "theme"}, Operator: "->>", IsText: true}}},
		{name: "deep path #> operator", sql: "SELECT profile #> '{settings,notifications,email}' FROM users", want: []parser.JSONPath{{Table: "users", Column: "profile", Path: []string{"settings", "notifications", "email"}, Operator: "#>", IsText: false}}},
		{name: "deep path #>> operator (text)", sql: "SELECT profile #>> '{settings,theme}' FROM users", want: []parser.JSONPath{{Table: "users", Column: "profile", Path: []string{"settings", "theme"}, Operator: "#>>", IsText: true}}},
		{name: "JSON path in WHERE clause", sql: "SELECT * FROM users WHERE profile->>'status' = 'active'", want: []parser.JSONPath{{Table: "users", Column: "profile", Path: []string{"status"}, Operator: "->>", IsText: true}}},
		{name: "multiple JSON paths", sql: "SELECT profile->'name', profile->'email', data->>'type' FROM users", want: []parser.JSONPath{{Table: "users", Column: "profile", Path: []string{"name"}, Operator: "->", IsText: false}, {Table: "users", Column: "profile", Path: []string{"email"}, Operator: "->", IsText: false}, {Table: "users", Column: "data", Path: []string{"type"}, Operator: "->>", IsText: true}}},
		{name: "JSON path with AND in WHERE", sql: "SELECT * FROM users WHERE profile->>'status' = 'active' AND data->'verified' = 'true'::jsonb", want: []parser.JSONPath{{Table: "users", Column: "profile", Path: []string{"status"}, Operator: "->>", IsText: true}, {Table: "users", Column: "data", Path: []string{"verified"}, Operator: "->", IsText: false}}},
		{name: "nested path with multiple levels", sql: "SELECT profile->'address'->'city'->>'name' FROM users", want: []parser.JSONPath{{Table: "users", Column: "profile", Path: []string{"address", "city", "name"}, Operator: "->>", IsText: true}}},
		{name: "JSON path with alias", sql: "SELECT profile->>'email' AS user_email FROM users", want: []parser.JSONPath{{Table: "users", Column: "profile", Path: []string{"email"}, Operator: "->>", IsText: true}}},
		{name: "invalid SQL", sql: "SELECT FROM WHERE", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.ExtractJSONPaths(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ExtractJSONPaths() error = %v, wantErr %v", err, tt.wantErr)
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

func TestPGQueryParser_ExtractJSONPaths_CompatibilityNormalization(t *testing.T) {
	p := newCGOParser(t)

	tests := []struct {
		name string
		sql  string
		want parser.JSONPath
	}{
		{
			name: "arrow chain includes terminal text segment",
			sql:  "SELECT profile->'settings'->>'theme' FROM users",
			want: parser.JSONPath{
				Table:    "users",
				Column:   "profile",
				Path:     []string{"settings", "theme"},
				Operator: "->>",
				IsText:   true,
			},
		},
		{
			name: "hash path list is expanded",
			sql:  "SELECT profile #>> '{settings,theme}' FROM users",
			want: parser.JSONPath{
				Table:    "users",
				Column:   "profile",
				Path:     []string{"settings", "theme"},
				Operator: "#>>",
				IsText:   true,
			},
		},
		{
			name: "where clause arrow chain is normalized",
			sql:  "SELECT * FROM users WHERE profile->'settings'->>'theme' = 'dark'",
			want: parser.JSONPath{
				Table:    "users",
				Column:   "profile",
				Path:     []string{"settings", "theme"},
				Operator: "->>",
				IsText:   true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.ExtractJSONPaths(tt.sql)
			if err != nil {
				t.Fatalf("ExtractJSONPaths() error = %v", err)
			}
			found := false
			for _, path := range got {
				if reflect.DeepEqual(path, tt.want) {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("expected normalized path %+v in %v", tt.want, got)
			}
		})
	}
}
