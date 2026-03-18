package complete

import (
	"reflect"
	"testing"

	"github.com/iw2rmb/squall/core"
	"github.com/iw2rmb/squall/parser"
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
		name       string
		parser     parser.MetadataExtractor
		sql        string
		wantClause contextClause
	}{
		{
			name:       "nil parser dependency",
			parser:     nil,
			sql:        "select 1 where ",
			wantClause: contextClauseWhere,
		},
		{
			name:       "parser returns error",
			parser:     failedParserStub(),
			sql:        "select * from ",
			wantClause: contextClauseFrom,
		},
		{
			name:       "parser returns nil metadata",
			parser:     nilMetadataParserStub(),
			sql:        "select * from orders ",
			wantClause: contextClauseFromTail,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			engine := NewEngine(Config{Parser: tc.parser})
			impl, ok := engine.(*EngineImpl)
			if !ok {
				t.Fatalf("engine concrete type = %T, want *EngineImpl", engine)
			}

			ctx, diags := impl.buildContext(Request{
				SQL:        tc.sql,
				CursorByte: len(tc.sql),
			})
			if !ctx.ParseDegraded {
				t.Fatalf("buildContext() ParseDegraded = %v, want true", ctx.ParseDegraded)
			}
			if ctx.ActiveClause != tc.wantClause {
				t.Fatalf("buildContext() ActiveClause = %q, want %q", ctx.ActiveClause, tc.wantClause)
			}
			if len(diags) != 1 || diags[0].Code != ParseDegraded {
				t.Fatalf("buildContext() diagnostics = %#v, want one %q diagnostic", diags, ParseDegraded)
			}

			version, err := engine.InitCatalog(catalogSnapshotVariantA())
			if err != nil {
				t.Fatalf("InitCatalog() error = %v", err)
			}

			resp, err := engine.Complete(Request{
				SQL:            tc.sql,
				CursorByte:     len(tc.sql),
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

func TestActiveClauseAtCursorFromTail(t *testing.T) {
	t.Parallel()

	sql := "select * from orders "
	got := activeClauseAtCursor(sql, len(sql))
	if got != contextClauseFromTail {
		t.Fatalf("activeClauseAtCursor() = %q, want %q", got, contextClauseFromTail)
	}
}

func TestActiveClauseAtCursorFromNeedsTable(t *testing.T) {
	t.Parallel()

	sql := "select * from "
	got := activeClauseAtCursor(sql, len(sql))
	if got != contextClauseFrom {
		t.Fatalf("activeClauseAtCursor() = %q, want %q", got, contextClauseFrom)
	}
}

func TestActiveClauseAtCursorFromAfterCommaNeedsTable(t *testing.T) {
	t.Parallel()

	sql := "select * from orders, "
	got := activeClauseAtCursor(sql, len(sql))
	if got != contextClauseFrom {
		t.Fatalf("activeClauseAtCursor() = %q, want %q", got, contextClauseFrom)
	}
}

func TestActiveClauseAtCursorJoinOn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want contextClause
	}{
		{
			name: "join on clause after on keyword",
			sql:  "select o.id from orders o join customers c on ",
			want: contextClauseJoinOn,
		},
		{
			name: "join on clause stays active in predicate tail",
			sql:  "select o.id from orders o join customers c on o.customer_id = c.id and ",
			want: contextClauseJoinOn,
		},
		{
			name: "where clause overrides join on",
			sql:  "select o.id from orders o join customers c on o.customer_id = c.id where ",
			want: contextClauseWhere,
		},
		{
			name: "join clause before on stays join",
			sql:  "select o.id from orders o join customers c ",
			want: contextClauseJoin,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := activeClauseAtCursor(tc.sql, len(tc.sql))
			if got != tc.want {
				t.Fatalf("activeClauseAtCursor() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestActiveClauseIgnoresQuotedKeywords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want contextClause
	}{
		{
			name: "single-quoted literal does not set from",
			sql:  "select 'from' as s where ",
			want: contextClauseWhere,
		},
		{
			name: "double-quoted identifier does not set join",
			sql:  `select "join" from t `,
			want: contextClauseFromTail,
		},
		{
			name: "escaped quote keeps literal scanning intact",
			sql:  "select 'it''s where' from t ",
			want: contextClauseFromTail,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := activeClauseAtCursor(tc.sql, len(tc.sql))
			if got != tc.want {
				t.Fatalf("activeClauseAtCursor() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestActiveClauseIgnoresCommentKeywords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want contextClause
	}{
		{
			name: "line comment does not set where",
			sql:  "select 1 -- where\nfrom orders ",
			want: contextClauseFromTail,
		},
		{
			name: "block comment does not set group by",
			sql:  "select 1 /* group by */ from orders ",
			want: contextClauseFromTail,
		},
		{
			name: "commented clause before real where keeps where",
			sql:  "select 1 /* where */ where ",
			want: contextClauseWhere,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := activeClauseAtCursor(tc.sql, len(tc.sql))
			if got != tc.want {
				t.Fatalf("activeClauseAtCursor() = %q, want %q", got, tc.want)
			}
		})
	}
}
