package core

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestCatalogValidation(t *testing.T) {
	t.Parallel()

	valid := CatalogSchema{
		Name: "public",
		Tables: []CatalogTable{
			{
				Schema: "public",
				Name:   "orders",
				Columns: []CatalogColumn{
					{Name: "id", Type: "bigint", Nullable: false},
					{Name: "customer_id", Type: "bigint", Nullable: false},
				},
				PrimaryKey: []string{"id"},
				ForeignKeys: []CatalogForeignKey{
					{
						Name:       "orders_customer_fk",
						Columns:    []string{"customer_id"},
						RefSchema:  "public",
						RefTable:   "customers",
						RefColumns: []string{"id"},
					},
				},
			},
		},
	}

	if !valid.IsValid() {
		t.Fatal("CatalogSchema.IsValid() = false, want true")
	}

	tests := []struct {
		name   string
		schema CatalogSchema
	}{
		{
			name: "empty schema name",
			schema: CatalogSchema{
				Name: "",
			},
		},
		{
			name: "empty table name",
			schema: CatalogSchema{
				Name: "public",
				Tables: []CatalogTable{
					{
						Name: "",
					},
				},
			},
		},
		{
			name: "schema mismatch on table",
			schema: CatalogSchema{
				Name: "public",
				Tables: []CatalogTable{
					{
						Schema: "other",
						Name:   "orders",
					},
				},
			},
		},
		{
			name: "empty column name",
			schema: CatalogSchema{
				Name: "public",
				Tables: []CatalogTable{
					{
						Name: "orders",
						Columns: []CatalogColumn{
							{Name: ""},
						},
					},
				},
			},
		},
		{
			name: "foreign key length mismatch",
			schema: CatalogSchema{
				Name: "public",
				Tables: []CatalogTable{
					{
						Name: "orders",
						ForeignKeys: []CatalogForeignKey{
							{
								Columns:    []string{"customer_id", "region_id"},
								RefTable:   "customers",
								RefColumns: []string{"id"},
							},
						},
					},
				},
			},
		},
		{
			name: "foreign key empty ref table",
			schema: CatalogSchema{
				Name: "public",
				Tables: []CatalogTable{
					{
						Name: "orders",
						ForeignKeys: []CatalogForeignKey{
							{
								Columns:    []string{"customer_id"},
								RefColumns: []string{"id"},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.schema.IsValid() {
				t.Fatal("CatalogSchema.IsValid() = true, want false")
			}
		})
	}
}

func TestCatalogCanonicalizeDeterministic(t *testing.T) {
	t.Parallel()

	in := []CatalogSchema{
		{
			Name: "z_schema",
			Tables: []CatalogTable{
				{
					Schema: "z_schema",
					Name:   "z_table",
					Columns: []CatalogColumn{
						{Name: "b_col", Type: "text", Nullable: true},
						{Name: "a_col", Type: "integer", Nullable: false},
					},
					ForeignKeys: []CatalogForeignKey{
						{
							Name:       "fk_b",
							Columns:    []string{"b_col"},
							RefSchema:  "x_schema",
							RefTable:   "x_table",
							RefColumns: []string{"id"},
						},
						{
							Name:       "fk_a",
							Columns:    []string{"a_col"},
							RefSchema:  "y_schema",
							RefTable:   "y_table",
							RefColumns: []string{"id"},
						},
					},
				},
			},
		},
		{
			Name: "a_schema",
			Tables: []CatalogTable{
				{
					Schema: "a_schema",
					Name:   "b_table",
				},
				{
					Schema: "a_schema",
					Name:   "a_table",
				},
			},
		},
	}

	got := CanonicalizeSchemas(in)
	gotAgain := CanonicalizeSchemas(in)
	if !reflect.DeepEqual(got, gotAgain) {
		t.Fatalf("CanonicalizeSchemas() is not deterministic: %#v vs %#v", got, gotAgain)
	}

	if got[0].Name != "a_schema" || got[1].Name != "z_schema" {
		t.Fatalf("schema ordering mismatch: %#v", got)
	}
	if got[0].Tables[0].Name != "a_table" || got[0].Tables[1].Name != "b_table" {
		t.Fatalf("table ordering mismatch: %#v", got[0].Tables)
	}
	if got[1].Tables[0].Columns[0].Name != "a_col" || got[1].Tables[0].Columns[1].Name != "b_col" {
		t.Fatalf("column ordering mismatch: %#v", got[1].Tables[0].Columns)
	}
	if got[1].Tables[0].ForeignKeys[0].Name != "fk_a" || got[1].Tables[0].ForeignKeys[1].Name != "fk_b" {
		t.Fatalf("foreign key ordering mismatch: %#v", got[1].Tables[0].ForeignKeys)
	}
}

func TestCatalogDTOJSONStable(t *testing.T) {
	t.Parallel()

	schemas := []CatalogSchema{
		{
			Name: "public",
			Tables: []CatalogTable{
				{
					Schema: "public",
					Name:   "orders",
					Columns: []CatalogColumn{
						{Name: "id", Type: "bigint", Nullable: false},
					},
					PrimaryKey: []string{"id"},
					ForeignKeys: []CatalogForeignKey{
						{
							Name:       "orders_customer_fk",
							Columns:    []string{"customer_id"},
							RefSchema:  "public",
							RefTable:   "customers",
							RefColumns: []string{"id"},
						},
					},
				},
			},
		},
	}

	raw, err := json.Marshal(schemas)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var decoded []CatalogSchema
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if !reflect.DeepEqual(decoded, schemas) {
		t.Fatalf("round-trip mismatch: got %#v want %#v", decoded, schemas)
	}
}
