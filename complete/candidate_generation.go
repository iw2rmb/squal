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

type candidateSet struct {
	items []Candidate
	seen  map[string]struct{}
}

type rankingContext struct {
	activeClause contextClause
	cursorPrefix string
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

func generateCandidates(ctx completionContext, catalog CatalogSnapshot, req Request) []Candidate {
	idx := buildCatalogIndex(catalog)
	out := newCandidateSet()

	addSchemaCandidates(out, idx)
	addTableCandidates(out, idx)
	addColumnCandidates(out, idx, ctx)
	addJoinCandidates(out, idx, ctx)
	if req.IncludeSnippets {
		addSnippetCandidates(out)
	}

	out.applyRanking(rankingContext{
		activeClause: ctx.ActiveClause,
		cursorPrefix: cursorPrefixAt(req.SQL, req.CursorByte),
	})

	return out.finalize(req.MaxCandidates)
}

func buildCatalogIndex(snapshot CatalogSnapshot) catalogIndex {
	canonical := canonicalizeCatalogSnapshot(snapshot)
	idx := catalogIndex{
		schemas:         make([]string, 0, len(canonical.Schemas)),
		tables:          make([]catalogTableEntry, 0),
		byQualified:     make(map[string]int),
		byName:          make(map[string][]int),
		searchPathOrder: make(map[string]int),
		searchPathSet:   make(map[string]struct{}),
	}

	for i, schema := range canonical.SearchPath {
		name := strings.ToLower(schema)
		if _, exists := idx.searchPathOrder[name]; !exists {
			idx.searchPathOrder[name] = i
		}
		idx.searchPathSet[name] = struct{}{}
	}

	for _, schema := range canonical.Schemas {
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

func newCandidateSet() *candidateSet {
	return &candidateSet{
		items: []Candidate{},
		seen:  make(map[string]struct{}),
	}
}

func (s *candidateSet) add(candidate Candidate) {
	if candidate.Kind == "" || candidate.Label == "" || candidate.InsertText == "" {
		return
	}

	candidate.SortKey.KindPriority = candidateKindPriority(candidate.Kind)
	candidate.SortKey.LabelLexical = strings.ToLower(candidate.Label)
	candidate.SortKey.InsertLexical = strings.ToLower(candidate.InsertText)
	if candidate.ID == "" {
		candidate.ID = candidateID(candidate)
	}

	key := strings.Join([]string{
		string(candidate.Kind),
		candidate.SortKey.LabelLexical,
		candidate.SortKey.InsertLexical,
		string(candidate.Source),
	}, "\x00")
	if _, exists := s.seen[key]; exists {
		return
	}
	s.seen[key] = struct{}{}
	s.items = append(s.items, candidate)
}

func (s *candidateSet) applyRanking(ctx rankingContext) {
	for i := range s.items {
		populateRanking(&s.items[i], ctx)
	}
}

func populateRanking(candidate *Candidate, ctx rankingContext) {
	exactPrefix := hasExactPrefixMatch(candidate, ctx.cursorPrefix)
	candidate.SortKey.ExactPrefix = exactPrefix

	candidate.ScoreComponents = ScoreComponents{
		Context:  contextScore(ctx.activeClause, candidate.Kind),
		Catalog:  catalogScore(candidate.Source),
		Prefix:   prefixScore(exactPrefix),
		Snippet:  snippetScore(candidate.Kind),
		Provider: providerScore(candidate.Source),
	}
	candidate.Score = candidate.ScoreComponents.Context +
		candidate.ScoreComponents.Catalog +
		candidate.ScoreComponents.Prefix +
		candidate.ScoreComponents.Snippet +
		candidate.ScoreComponents.Provider
}

func (s *candidateSet) finalize(max int) []Candidate {
	sort.Slice(s.items, func(i, j int) bool { return candidateLess(s.items[i], s.items[j]) })

	if max > 0 && len(s.items) > max {
		return append([]Candidate(nil), s.items[:max]...)
	}
	return append([]Candidate(nil), s.items...)
}

func candidateLess(left Candidate, right Candidate) bool {
	if left.Score != right.Score {
		return left.Score > right.Score
	}
	if left.SortKey.KindPriority != right.SortKey.KindPriority {
		return left.SortKey.KindPriority < right.SortKey.KindPriority
	}
	if left.SortKey.ExactPrefix != right.SortKey.ExactPrefix {
		return left.SortKey.ExactPrefix && !right.SortKey.ExactPrefix
	}
	if left.Kind != right.Kind {
		return left.Kind < right.Kind
	}
	if left.SortKey.LabelLexical != right.SortKey.LabelLexical {
		return left.SortKey.LabelLexical < right.SortKey.LabelLexical
	}
	if left.SortKey.InsertLexical != right.SortKey.InsertLexical {
		return left.SortKey.InsertLexical < right.SortKey.InsertLexical
	}
	if left.Label != right.Label {
		return left.Label < right.Label
	}
	if left.InsertText != right.InsertText {
		return left.InsertText < right.InsertText
	}
	if left.Source != right.Source {
		return left.Source < right.Source
	}
	return left.ID < right.ID
}

func addSchemaCandidates(out *candidateSet, idx catalogIndex) {
	for _, schema := range idx.schemas {
		out.add(Candidate{
			ID:         "schema:" + strings.ToLower(schema),
			Label:      schema,
			InsertText: schema,
			Kind:       CandidateKindSchema,
			Source:     CandidateSourceCatalog,
		})
	}
}

func addTableCandidates(out *candidateSet, idx catalogIndex) {
	for _, entry := range idx.tables {
		insert := qualifiedTableInsert(entry.Schema, entry.Table.Name, idx.searchPathSet)
		out.add(Candidate{
			ID:         "table:" + strings.ToLower(entry.Schema) + "." + strings.ToLower(entry.Table.Name),
			Label:      entry.Schema + "." + entry.Table.Name,
			InsertText: insert,
			Kind:       CandidateKindTable,
			Source:     CandidateSourceCatalog,
		})
	}
}

func addColumnCandidates(out *candidateSet, idx catalogIndex, ctx completionContext) {
	bindings := gatherTableBindings(idx, ctx)
	if len(bindings) == 0 {
		return
	}

	for _, binding := range bindings {
		for _, column := range binding.entry.Table.Columns {
			value := binding.qualifier + "." + column.Name
			out.add(Candidate{
				ID:         "column:" + strings.ToLower(binding.qualifier) + "." + strings.ToLower(column.Name),
				Label:      value,
				InsertText: value,
				Kind:       CandidateKindColumn,
				Source:     CandidateSourceCatalog,
			})
		}
	}
}

func addJoinCandidates(out *candidateSet, idx catalogIndex, ctx completionContext) {
	bindings := gatherTableBindings(idx, ctx)
	if len(bindings) == 0 {
		return
	}

	visible := visibleTablesByIdentity(bindings)
	if len(visible) == 0 {
		return
	}

	visibleKeys := make([]string, 0, len(visible))
	for key := range visible {
		visibleKeys = append(visibleKeys, key)
	}
	sort.Strings(visibleKeys)

	for _, key := range visibleKeys {
		current := visible[key]

		for _, fk := range current.entry.Table.ForeignKeys {
			if len(fk.Columns) == 0 || len(fk.Columns) != len(fk.RefColumns) || fk.RefTable == "" {
				continue
			}

			targetSchema := fk.RefSchema
			if targetSchema == "" {
				targetSchema = current.entry.Schema
			}
			target, ok := resolveQualifiedTable(targetSchema, fk.RefTable, idx)
			if !ok {
				target, ok = resolveTableRef(fk.RefTable, idx)
				if !ok {
					continue
				}
			}

			targetKey := tableKey(target.Schema, target.Table.Name)
			if _, alreadyVisible := visible[targetKey]; alreadyVisible {
				continue
			}

			on := joinCondition(current.qualifier, fk.Columns, target.Table.Name, fk.RefColumns)
			if on == "" {
				continue
			}
			addJoinCandidate(out, idx, target.Schema, target.Table.Name, on)
		}
	}

	for _, source := range idx.tables {
		sourceKey := tableKey(source.Schema, source.Table.Name)
		if _, alreadyVisible := visible[sourceKey]; alreadyVisible {
			continue
		}

		for _, fk := range source.Table.ForeignKeys {
			if len(fk.Columns) == 0 || len(fk.Columns) != len(fk.RefColumns) || fk.RefTable == "" {
				continue
			}

			targetSchema := fk.RefSchema
			if targetSchema == "" {
				targetSchema = source.Schema
			}
			targetKey := tableKey(targetSchema, fk.RefTable)
			boundTarget, visibleTarget := visible[targetKey]
			if !visibleTarget {
				continue
			}

			on := joinCondition(source.Table.Name, fk.Columns, boundTarget.qualifier, fk.RefColumns)
			if on == "" {
				continue
			}
			addJoinCandidate(out, idx, source.Schema, source.Table.Name, on)
		}
	}
}

func addJoinCandidate(out *candidateSet, idx catalogIndex, schema string, table string, on string) {
	joinTable := qualifiedTableInsert(schema, table, idx.searchPathSet)
	out.add(Candidate{
		ID:         "join:" + strings.ToLower(schema) + "." + strings.ToLower(table) + ":" + strings.ToLower(on),
		Label:      "JOIN " + schema + "." + table + " ON " + on,
		InsertText: "JOIN " + joinTable + " ON " + on,
		Kind:       CandidateKindJoin,
		Source:     CandidateSourceCatalog,
	})
}

func addSnippetCandidates(out *candidateSet) {
	snippets := []struct {
		id     string
		label  string
		insert string
	}{
		{
			id:     "snippet:select_from",
			label:  "SELECT ... FROM ...",
			insert: "SELECT * FROM ",
		},
		{
			id:     "snippet:where",
			label:  "WHERE ...",
			insert: "WHERE ",
		},
		{
			id:     "snippet:join_on",
			label:  "JOIN ... ON ...",
			insert: "JOIN  ON ",
		},
	}

	for _, snippet := range snippets {
		out.add(Candidate{
			ID:         snippet.id,
			Label:      snippet.label,
			InsertText: snippet.insert,
			Kind:       CandidateKindSnippet,
			Source:     CandidateSourceSnippet,
		})
	}
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

func candidateKindPriority(kind CandidateKind) int {
	switch kind {
	case CandidateKindSchema:
		return 10
	case CandidateKindTable:
		return 20
	case CandidateKindColumn:
		return 30
	case CandidateKindJoin:
		return 40
	case CandidateKindSnippet:
		return 50
	case CandidateKindKeyword:
		return 60
	default:
		return 100
	}
}

func contextScore(clause contextClause, kind CandidateKind) float64 {
	switch clause {
	case contextClauseSelect:
		switch kind {
		case CandidateKindColumn:
			return 50
		case CandidateKindTable:
			return 20
		case CandidateKindJoin:
			return 10
		case CandidateKindSchema:
			return 8
		case CandidateKindSnippet:
			return 6
		case CandidateKindKeyword:
			return 5
		}
	case contextClauseFrom:
		switch kind {
		case CandidateKindTable:
			return 50
		case CandidateKindSchema:
			return 24
		case CandidateKindJoin:
			return 12
		case CandidateKindColumn:
			return 6
		case CandidateKindSnippet:
			return 5
		case CandidateKindKeyword:
			return 4
		}
	case contextClauseJoin:
		switch kind {
		case CandidateKindJoin:
			return 50
		case CandidateKindTable:
			return 30
		case CandidateKindColumn:
			return 12
		case CandidateKindSchema:
			return 6
		case CandidateKindSnippet:
			return 5
		case CandidateKindKeyword:
			return 4
		}
	case contextClauseWhere:
		switch kind {
		case CandidateKindColumn:
			return 55
		case CandidateKindJoin:
			return 12
		case CandidateKindTable:
			return 8
		case CandidateKindSchema:
			return 5
		case CandidateKindSnippet:
			return 6
		case CandidateKindKeyword:
			return 4
		}
	case contextClauseGroupBy, contextClauseOrderBy:
		switch kind {
		case CandidateKindColumn:
			return 50
		case CandidateKindTable:
			return 8
		case CandidateKindSchema:
			return 5
		case CandidateKindSnippet:
			return 5
		case CandidateKindKeyword:
			return 4
		case CandidateKindJoin:
			return 6
		}
	}
	return 0
}

func catalogScore(source CandidateSource) float64 {
	switch source {
	case CandidateSourceCatalog:
		return 20
	case CandidateSourceParser:
		return 12
	case CandidateSourceSnippet:
		return 8
	case CandidateSourceProvider:
		return 6
	default:
		return 0
	}
}

func prefixScore(exact bool) float64 {
	if exact {
		return 30
	}
	return 0
}

func snippetScore(kind CandidateKind) float64 {
	if kind == CandidateKindSnippet {
		return -5
	}
	return 0
}

func providerScore(source CandidateSource) float64 {
	if source == CandidateSourceProvider {
		return 10
	}
	return 0
}

func cursorPrefixAt(sql string, cursor int) string {
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(sql) {
		cursor = len(sql)
	}

	start := cursor
	for start > 0 && isIdentifierByte(sql[start-1]) {
		start--
	}
	if start == cursor {
		return ""
	}
	return strings.ToLower(sql[start:cursor])
}

func hasExactPrefixMatch(candidate *Candidate, prefix string) bool {
	if prefix == "" {
		return false
	}

	return hasIdentifierPrefix(candidate.SortKey.LabelLexical, prefix) ||
		hasIdentifierPrefix(candidate.SortKey.InsertLexical, prefix)
}

func hasIdentifierPrefix(value string, prefix string) bool {
	start := -1
	for i := 0; i <= len(value); i++ {
		if i < len(value) && isIdentifierByte(value[i]) {
			if start < 0 {
				start = i
			}
			continue
		}

		if start >= 0 {
			if strings.HasPrefix(value[start:i], prefix) {
				return true
			}
			start = -1
		}
	}

	return false
}

func isIdentifierByte(b byte) bool {
	return (b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') ||
		b == '_' ||
		b == '$'
}

func candidateID(candidate Candidate) string {
	return string(candidate.Kind) + ":" + candidate.SortKey.LabelLexical + ":" + candidate.SortKey.InsertLexical
}

func tableKey(schema string, table string) string {
	if schema == "" {
		return strings.ToLower(table)
	}
	return strings.ToLower(schema) + "." + strings.ToLower(table)
}
