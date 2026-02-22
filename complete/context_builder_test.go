package complete

import (
	"reflect"
	"testing"

	"github.com/iw2rmb/sql/core"
	"github.com/iw2rmb/sql/parser"
)

func TestBuildContext(t *testing.T) {
	t.Parallel()

	meta := &parser.QueryMetadata{
		Tables: []string{"users", "orders", "users"},
		SelectColumns: []parser.ColumnRef{
			{Table: "o", Column: "total"},
			{Table: "u", Column: "email", Alias: "customer_email"},
			{Column: "status"},
		},
		WhereConditions: []parser.FilterCondition{
			{
				Column:   parser.ColumnRef{Table: "o", Column: "status"},
				Operator: core.CompareOpEqual,
				IsParam:  true,
			},
			{
				Column:   parser.ColumnRef{Table: "u", Column: "email"},
				Operator: core.CompareOpLike,
				IsParam:  false,
			},
		},
		JoinConditions: []parser.JoinCondition{
			{
				Type:        core.JoinTypeInner,
				LeftTable:   "orders",
				RightTable:  "users",
				LeftColumn:  "user_id",
				RightColumn: "id",
				LeftAlias:   "o",
				RightAlias:  "u",
			},
		},
	}

	sql := "SELECT u.email, o.total FROM orders o JOIN users u ON o.user_id = u.id WHERE o.status = $1"
	cursor := len(sql)

	gotA := buildContext(meta, sql, cursor)
	gotB := buildContext(meta, sql, cursor)
	if !reflect.DeepEqual(gotA, gotB) {
		t.Fatalf("buildContext() is not deterministic:\nA=%#v\nB=%#v", gotA, gotB)
	}

	want := completionContext{
		ActiveClause:      contextClauseWhere,
		Tables:            []string{"orders", "users"},
		Aliases:           []string{"o", "u"},
		AliasBindings:     []aliasBinding{{Alias: "o", Table: "orders"}, {Alias: "u", Table: "users"}},
		ProjectionTargets: []string{"customer_email", "o.total", "status"},
		Predicates: []predicateContext{
			{Qualifier: "o", Column: "status", Operator: string(core.CompareOpEqual), IsParam: true},
			{Qualifier: "u", Column: "email", Operator: string(core.CompareOpLike), IsParam: false},
		},
		Joins: []joinContext{
			{
				Type:        string(core.JoinTypeInner),
				LeftTable:   "orders",
				RightTable:  "users",
				LeftColumn:  "user_id",
				RightColumn: "id",
				LeftAlias:   "o",
				RightAlias:  "u",
			},
		},
	}
	if !reflect.DeepEqual(gotA, want) {
		t.Fatalf("buildContext() = %#v, want %#v", gotA, want)
	}
}

func TestParseDegraded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		parser parser.Parser
	}{
		{
			name:   "nil parser dependency",
			parser: nil,
		},
		{
			name:   "parser returns error",
			parser: failedParserStub(),
		},
		{
			name:   "parser returns nil metadata",
			parser: nilMetadataParserStub(),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			engine := NewEngine(Config{Parser: tc.parser})
			version, err := engine.InitCatalog(catalogSnapshotVariantA())
			if err != nil {
				t.Fatalf("InitCatalog() error = %v", err)
			}

			resp, err := engine.Complete(Request{
				SQL:            "select 1",
				CursorByte:     len("select 1"),
				CatalogVersion: version,
			})
			if err != nil {
				t.Fatalf("Complete() error = %v", err)
			}
			if len(resp.Candidates) == 0 {
				t.Fatalf("Complete() candidates = %d, want >0 fallback candidates", len(resp.Candidates))
			}
			if len(resp.Diagnostics) != 1 {
				t.Fatalf("Complete() diagnostics = %#v, want exactly one", resp.Diagnostics)
			}
			if resp.Diagnostics[0].Code != ParseDegraded {
				t.Fatalf("Complete() diagnostics[0].Code = %q, want %q", resp.Diagnostics[0].Code, ParseDegraded)
			}
		})
	}
}

func TestAmbiguousContext(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{
		Parser: &parserStub{
			metadata: &parser.QueryMetadata{
				Tables: []string{"orders", "users"},
				JoinConditions: []parser.JoinCondition{
					{
						Type:        core.JoinTypeInner,
						LeftTable:   "orders",
						RightTable:  "users",
						LeftColumn:  "user_id",
						RightColumn: "id",
						LeftAlias:   "o",
						RightAlias:  "u",
					},
				},
			},
		},
	})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	resp, err := engine.Complete(Request{
		SQL:            "select o.id from orders o join users u on o.user_id = u.id where ",
		CursorByte:     len("select o.id from orders o join users u on o.user_id = u.id where "),
		CatalogVersion: version,
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}

	for _, diag := range resp.Diagnostics {
		if diag.Code == AmbiguousContext {
			t.Fatalf("Complete() diagnostics unexpectedly contain %q: %#v", AmbiguousContext, resp.Diagnostics)
		}
	}
}
