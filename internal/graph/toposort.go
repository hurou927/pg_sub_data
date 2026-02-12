package graph

import "fmt"

// TopoResult holds the result of topological sorting.
type TopoResult struct {
	// Order is the topological order (parents before children).
	Order []string
	// HasCycle is true if the graph contains a cycle.
	HasCycle bool
	// CycleTables lists tables involved in cycles (if any).
	CycleTables []string
}

// TopoSort performs Kahn's algorithm on the given set of tables within the graph.
// Returns tables in dependency order: parents first, then children.
func TopoSort(g *Graph, tables []string) TopoResult {
	tableSet := make(map[string]bool, len(tables))
	for _, t := range tables {
		tableSet[t] = true
	}

	// Compute in-degree for each table within this subset
	// In-degree = number of parent edges within the subset
	inDegree := make(map[string]int, len(tables))
	for _, t := range tables {
		inDegree[t] = 0
	}

	// Build local parent map (only edges within the subset)
	localParents := make(map[string][]string)
	localChildren := make(map[string][]string)
	for _, t := range tables {
		for _, p := range g.Parents[t] {
			if tableSet[p] {
				localParents[t] = append(localParents[t], p)
				localChildren[p] = append(localChildren[p], t)
				inDegree[t]++
			}
		}
	}

	// Initialize queue with zero in-degree nodes (roots)
	var queue []string
	for _, t := range tables {
		if inDegree[t] == 0 {
			queue = append(queue, t)
		}
	}

	var order []string
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		order = append(order, node)

		for _, child := range localChildren[node] {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	result := TopoResult{Order: order}

	if len(order) < len(tables) {
		result.HasCycle = true
		for _, t := range tables {
			if inDegree[t] > 0 {
				result.CycleTables = append(result.CycleTables, t)
			}
		}
	}

	return result
}

// TopoSortAll performs topological sort across all tables in the graph.
func TopoSortAll(g *Graph) TopoResult {
	var all []string
	for name := range g.Tables {
		all = append(all, name)
	}
	return TopoSort(g, all)
}

// ValidateCycles checks for cycles and returns a descriptive error if found.
func ValidateCycles(result TopoResult) error {
	if !result.HasCycle {
		return nil
	}
	return fmt.Errorf("circular dependency detected among tables: %v", result.CycleTables)
}
