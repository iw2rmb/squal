package graph

// FindAffectedQueries finds queries affected by a table change.
func (g *QueryGraph) FindAffectedQueries(table TableName, operation string) []QueryID {
	g.mu.RLock()
	defer g.mu.RUnlock()

	affected := make(map[QueryID]bool)

	if queries, ok := g.tableIndex[table]; ok {
		for _, queryID := range queries {
			affected[queryID] = true
			g.findDependentsRecursive(queryID, affected)
		}
	}

	result := make([]QueryID, 0, len(affected))
	for id := range affected {
		result = append(result, id)
	}

	return result
}

func (g *QueryGraph) findDependentsRecursive(queryID QueryID, visited map[QueryID]bool) {
	if node, ok := g.nodes[queryID]; ok {
		for _, depID := range node.Dependents {
			if !visited[depID] {
				visited[depID] = true
				g.findDependentsRecursive(depID, visited)
			}
		}
	}
}

// FindQueriesByTable finds all queries that depend on a specific table.
func (g *QueryGraph) FindQueriesByTable(table TableName) []QueryID {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if queries, ok := g.tableIndex[table]; ok {
		result := make([]QueryID, len(queries))
		copy(result, queries)
		return result
	}

	return []QueryID{}
}
