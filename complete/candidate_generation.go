package complete

import (
	"sort"
	"strings"
)

type candidateSet struct {
	items []Candidate
	seen  map[string]struct{}
}

func generateCandidates(ctx completionContext, catalog CatalogSnapshot, req Request) []Candidate {
	idx := buildCatalogIndex(catalog)
	out := newCandidateSet()
	cursorPrefix := cursorPrefixAt(req.SQL, req.CursorByte)

	// For SELECT without visible source tables, return final-form projection
	// candidates only, so users can complete a runnable query in one accept.
	if ctx.ActiveClause == contextClauseSelect && len(gatherTableBindings(idx, ctx)) == 0 {
		if cursorPrefix == "" {
			addSelectStarFromCandidates(out, idx)
		} else {
			addSelectColumnFromCandidates(out, idx, cursorPrefix)
		}
		out.applyRanking(rankingContext{
			activeClause: ctx.ActiveClause,
			cursorPrefix: cursorPrefix,
		})
		return out.finalize(req.MaxCandidates)
	}

	addSchemaCandidates(out, idx)
	addTableCandidates(out, idx)
	addColumnCandidates(out, idx, ctx)
	addJoinCandidates(out, idx, ctx)
	if req.IncludeSnippets {
		addSnippetCandidates(out)
	}

	out.applyRanking(rankingContext{
		activeClause: ctx.ActiveClause,
		cursorPrefix: cursorPrefix,
	})

	return out.finalize(req.MaxCandidates)
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

func (s *candidateSet) finalize(max int) []Candidate {
	sort.Slice(s.items, func(i, j int) bool { return candidateLess(s.items[i], s.items[j]) })

	if max > 0 && len(s.items) > max {
		return append([]Candidate(nil), s.items[:max]...)
	}
	return append([]Candidate(nil), s.items...)
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

func addSelectStarFromCandidates(out *candidateSet, idx catalogIndex) {
	for _, entry := range idx.tables {
		tableLabel := entry.Schema + "." + entry.Table.Name
		tableInsert := qualifiedTableInsert(entry.Schema, entry.Table.Name, idx.searchPathSet)
		out.add(Candidate{
			ID:         "select:star-from:" + strings.ToLower(tableLabel),
			Label:      "* FROM " + tableLabel,
			InsertText: "* FROM " + tableInsert,
			Kind:       CandidateKindSnippet,
			Source:     CandidateSourceCatalog,
		})
	}
}

func addSelectColumnFromCandidates(out *candidateSet, idx catalogIndex, prefix string) {
	lowerPrefix := strings.ToLower(strings.TrimSpace(prefix))
	if lowerPrefix == "" {
		return
	}

	for _, entry := range idx.tables {
		tableLabel := entry.Schema + "." + entry.Table.Name
		tableInsert := qualifiedTableInsert(entry.Schema, entry.Table.Name, idx.searchPathSet)
		for _, column := range entry.Table.Columns {
			if !strings.HasPrefix(strings.ToLower(column.Name), lowerPrefix) {
				continue
			}
			label := column.Name + " FROM " + tableLabel
			out.add(Candidate{
				ID:         "select:column-from:" + strings.ToLower(tableLabel) + "." + strings.ToLower(column.Name),
				Label:      label,
				InsertText: column.Name + " FROM " + tableInsert,
				Kind:       CandidateKindColumn,
				Source:     CandidateSourceCatalog,
			})
		}
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
