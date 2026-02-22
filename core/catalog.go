package core

import "sort"

// CatalogSchema is a schema node in a catalog graph.
type CatalogSchema struct {
	Name   string         `json:"name"`
	Tables []CatalogTable `json:"tables"`
}

// IsValid reports whether the schema and nested table graph are structurally valid.
func (s CatalogSchema) IsValid() bool {
	if s.Name == "" {
		return false
	}
	for _, table := range s.Tables {
		if !table.IsValid() {
			return false
		}
		if table.Schema != "" && table.Schema != s.Name {
			return false
		}
	}
	return true
}

// CatalogTable is a table node in a catalog graph.
type CatalogTable struct {
	Schema      string              `json:"schema"`
	Name        string              `json:"name"`
	Columns     []CatalogColumn     `json:"columns"`
	PrimaryKey  []string            `json:"primary_key"`
	ForeignKeys []CatalogForeignKey `json:"foreign_keys"`
}

// IsValid reports whether the table and nested keys are structurally valid.
func (t CatalogTable) IsValid() bool {
	if t.Name == "" {
		return false
	}
	for _, col := range t.Columns {
		if !col.IsValid() {
			return false
		}
	}
	for _, col := range t.PrimaryKey {
		if col == "" {
			return false
		}
	}
	for _, fk := range t.ForeignKeys {
		if !fk.IsValid() {
			return false
		}
	}
	return true
}

// CatalogColumn is a column node in a catalog graph.
type CatalogColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// IsValid reports whether the column is structurally valid.
func (c CatalogColumn) IsValid() bool {
	return c.Name != ""
}

// CatalogForeignKey is a directed table-edge with composite key support.
type CatalogForeignKey struct {
	Name       string   `json:"name"`
	Columns    []string `json:"columns"`
	RefSchema  string   `json:"ref_schema"`
	RefTable   string   `json:"ref_table"`
	RefColumns []string `json:"ref_columns"`
}

// IsValid reports whether the foreign key is structurally valid.
func (f CatalogForeignKey) IsValid() bool {
	if f.RefTable == "" {
		return false
	}
	if len(f.Columns) == 0 || len(f.RefColumns) == 0 {
		return false
	}
	if len(f.Columns) != len(f.RefColumns) {
		return false
	}
	for _, col := range f.Columns {
		if col == "" {
			return false
		}
	}
	for _, col := range f.RefColumns {
		if col == "" {
			return false
		}
	}
	return true
}

// CanonicalizeSchemas returns a deterministic deep-copy ordering for schemas/tables/columns/FKs.
func CanonicalizeSchemas(in []CatalogSchema) []CatalogSchema {
	out := make([]CatalogSchema, 0, len(in))
	for _, schema := range in {
		copiedSchema := CatalogSchema{
			Name:   schema.Name,
			Tables: make([]CatalogTable, 0, len(schema.Tables)),
		}

		for _, table := range schema.Tables {
			copiedTable := CatalogTable{
				Schema:      table.Schema,
				Name:        table.Name,
				Columns:     append([]CatalogColumn(nil), table.Columns...),
				PrimaryKey:  append([]string(nil), table.PrimaryKey...),
				ForeignKeys: make([]CatalogForeignKey, len(table.ForeignKeys)),
			}

			for i, fk := range table.ForeignKeys {
				copiedTable.ForeignKeys[i] = CatalogForeignKey{
					Name:       fk.Name,
					Columns:    append([]string(nil), fk.Columns...),
					RefSchema:  fk.RefSchema,
					RefTable:   fk.RefTable,
					RefColumns: append([]string(nil), fk.RefColumns...),
				}
			}

			sort.SliceStable(copiedTable.Columns, func(i, j int) bool {
				left := copiedTable.Columns[i]
				right := copiedTable.Columns[j]
				if left.Name != right.Name {
					return left.Name < right.Name
				}
				if left.Type != right.Type {
					return left.Type < right.Type
				}
				return !left.Nullable && right.Nullable
			})

			sort.SliceStable(copiedTable.ForeignKeys, func(i, j int) bool {
				left := copiedTable.ForeignKeys[i]
				right := copiedTable.ForeignKeys[j]
				if left.Name != right.Name {
					return left.Name < right.Name
				}
				if left.RefSchema != right.RefSchema {
					return left.RefSchema < right.RefSchema
				}
				if left.RefTable != right.RefTable {
					return left.RefTable < right.RefTable
				}
				if joinedColumns(left.Columns) != joinedColumns(right.Columns) {
					return joinedColumns(left.Columns) < joinedColumns(right.Columns)
				}
				return joinedColumns(left.RefColumns) < joinedColumns(right.RefColumns)
			})

			copiedSchema.Tables = append(copiedSchema.Tables, copiedTable)
		}

		sort.SliceStable(copiedSchema.Tables, func(i, j int) bool {
			left := copiedSchema.Tables[i]
			right := copiedSchema.Tables[j]
			if left.Schema != right.Schema {
				return left.Schema < right.Schema
			}
			return left.Name < right.Name
		})

		out = append(out, copiedSchema)
	}

	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})

	return out
}

func joinedColumns(columns []string) string {
	if len(columns) == 0 {
		return ""
	}
	n := 0
	for _, col := range columns {
		n += len(col) + 1
	}
	joined := make([]byte, 0, n)
	for i, col := range columns {
		if i > 0 {
			joined = append(joined, 0)
		}
		joined = append(joined, col...)
	}
	return string(joined)
}
