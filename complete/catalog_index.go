package complete

import (
	"sort"
	"strings"

	"github.com/iw2rmb/squal/core"
)

type catalogTableEntry struct {
	Schema string
	Table  core.CatalogTable
}

type catalogIndex struct {
	schemas         []string
	tables          []catalogTableEntry
	byQualified     map[string]int
	byName          map[string][]int
	searchPathOrder map[string]int
	searchPathSet   map[string]struct{}
}

type tableBinding struct {
	qualifier string
	entry     catalogTableEntry
	isAlias   bool
}

type visibleTable struct {
	entry     catalogTableEntry
	qualifier string
}

func buildCatalogIndex(snapshot CatalogSnapshot) catalogIndex {
	idx := catalogIndex{
		schemas:         make([]string, 0, len(snapshot.Schemas)),
		tables:          make([]catalogTableEntry, 0),
		byQualified:     make(map[string]int),
		byName:          make(map[string][]int),
		searchPathOrder: make(map[string]int, len(snapshot.SearchPath)),
		searchPathSet:   make(map[string]struct{}, len(snapshot.SearchPath)),
	}

	for i, schema := range snapshot.SearchPath {
		name := strings.ToLower(schema)
		if _, exists := idx.searchPathOrder[name]; !exists {
			idx.searchPathOrder[name] = i
		}
		idx.searchPathSet[name] = struct{}{}
	}

	for _, schema := range snapshot.Schemas {
		idx.schemas = append(idx.schemas, schema.Name)
		for _, table := range schema.Tables {
			entry := catalogTableEntry{Schema: schema.Name, Table: table}
			position := len(idx.tables)
			idx.tables = append(idx.tables, entry)

			qualifiedKey := tableKey(schema.Name, table.Name)
			idx.byQualified[qualifiedKey] = position

			nameKey := strings.ToLower(table.Name)
			idx.byName[nameKey] = append(idx.byName[nameKey], position)
		}
	}

	return idx
}

func resolveQualifiedTable(schema string, table string, idx catalogIndex) (catalogTableEntry, bool) {
	position, ok := idx.byQualified[tableKey(schema, table)]
	if !ok {
		return catalogTableEntry{}, false
	}
	return idx.tables[position], true
}

func resolveTableRef(ref string, idx catalogIndex) (catalogTableEntry, bool) {
	trimmed := strings.TrimSpace(ref)
	if trimmed == "" {
		return catalogTableEntry{}, false
	}

	if strings.Contains(trimmed, ".") {
		parts := strings.SplitN(trimmed, ".", 2)
		if len(parts) == 2 {
			return resolveQualifiedTable(parts[0], parts[1], idx)
		}
	}

	candidates := idx.byName[strings.ToLower(trimmed)]
	if len(candidates) == 0 {
		return catalogTableEntry{}, false
	}
	if len(candidates) == 1 {
		return idx.tables[candidates[0]], true
	}

	best := candidates[0]
	bestRank := searchPathRank(idx.tables[best].Schema, idx.searchPathOrder)
	for _, candidate := range candidates[1:] {
		candidateEntry := idx.tables[candidate]
		candidateRank := searchPathRank(candidateEntry.Schema, idx.searchPathOrder)
		bestEntry := idx.tables[best]

		if candidateRank < bestRank {
			best = candidate
			bestRank = candidateRank
			continue
		}
		if candidateRank > bestRank {
			continue
		}
		if candidateEntry.Schema < bestEntry.Schema {
			best = candidate
			continue
		}
		if candidateEntry.Schema == bestEntry.Schema && candidateEntry.Table.Name < bestEntry.Table.Name {
			best = candidate
		}
	}

	return idx.tables[best], true
}

func searchPathRank(schema string, order map[string]int) int {
	if rank, ok := order[strings.ToLower(schema)]; ok {
		return rank
	}
	return len(order) + 1
}

func qualifiedTableInsert(schema string, table string, searchPathSet map[string]struct{}) string {
	if schema == "" {
		return table
	}
	if _, ok := searchPathSet[strings.ToLower(schema)]; ok {
		return table
	}
	return schema + "." + table
}

func tableKey(schema string, table string) string {
	if schema == "" {
		return strings.ToLower(table)
	}
	return strings.ToLower(schema) + "." + strings.ToLower(table)
}

func gatherTableBindings(idx catalogIndex, ctx completionContext) []tableBinding {
	bindings := make([]tableBinding, 0, len(ctx.Tables)+len(ctx.AliasBindings))
	seen := make(map[string]struct{})

	add := func(qualifier string, entry catalogTableEntry, isAlias bool) {
		if qualifier == "" {
			return
		}
		key := strings.ToLower(qualifier) + "\x00" + tableKey(entry.Schema, entry.Table.Name)
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		bindings = append(bindings, tableBinding{
			qualifier: qualifier,
			entry:     entry,
			isAlias:   isAlias,
		})
	}

	for _, table := range ctx.Tables {
		entry, ok := resolveTableRef(table, idx)
		if !ok {
			continue
		}
		add(entry.Table.Name, entry, false)
	}

	for _, alias := range ctx.AliasBindings {
		entry, ok := resolveTableRef(alias.Table, idx)
		if !ok {
			continue
		}
		add(alias.Alias, entry, true)
	}

	sort.Slice(bindings, func(i, j int) bool {
		if bindings[i].qualifier != bindings[j].qualifier {
			return bindings[i].qualifier < bindings[j].qualifier
		}
		if bindings[i].entry.Schema != bindings[j].entry.Schema {
			return bindings[i].entry.Schema < bindings[j].entry.Schema
		}
		if bindings[i].entry.Table.Name != bindings[j].entry.Table.Name {
			return bindings[i].entry.Table.Name < bindings[j].entry.Table.Name
		}
		return !bindings[i].isAlias && bindings[j].isAlias
	})

	return bindings
}

func visibleTablesByIdentity(bindings []tableBinding) map[string]visibleTable {
	visible := make(map[string]visibleTable)
	for _, binding := range bindings {
		key := tableKey(binding.entry.Schema, binding.entry.Table.Name)
		existing, exists := visible[key]
		if !exists {
			visible[key] = visibleTable{entry: binding.entry, qualifier: binding.qualifier}
			continue
		}

		existingAlias := existing.qualifier != existing.entry.Table.Name
		currentAlias := binding.qualifier != binding.entry.Table.Name
		replace := false
		if currentAlias && !existingAlias {
			replace = true
		}
		if currentAlias == existingAlias && binding.qualifier < existing.qualifier {
			replace = true
		}
		if replace {
			visible[key] = visibleTable{entry: binding.entry, qualifier: binding.qualifier}
		}
	}
	return visible
}

func joinCondition(leftQualifier string, leftColumns []string, rightQualifier string, rightColumns []string) string {
	if leftQualifier == "" || rightQualifier == "" || len(leftColumns) == 0 || len(leftColumns) != len(rightColumns) {
		return ""
	}

	parts := make([]string, 0, len(leftColumns))
	for i := 0; i < len(leftColumns); i++ {
		if leftColumns[i] == "" || rightColumns[i] == "" {
			return ""
		}
		parts = append(parts, leftQualifier+"."+leftColumns[i]+" = "+rightQualifier+"."+rightColumns[i])
	}
	return strings.Join(parts, " AND ")
}
