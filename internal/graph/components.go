package graph

// Component represents a connected component of tables.
type Component struct {
	Tables []string
}

// FindComponents detects connected components using undirected BFS.
func FindComponents(g *Graph) []Component {
	visited := make(map[string]bool)
	var components []Component

	for name := range g.Tables {
		if visited[name] {
			continue
		}
		comp := bfs(g, name, visited)
		components = append(components, Component{Tables: comp})
	}

	return components
}

func bfs(g *Graph, start string, visited map[string]bool) []string {
	queue := []string{start}
	visited[start] = true
	var result []string

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		result = append(result, node)

		for neighbor := range g.Adjacency[node] {
			if !visited[neighbor] {
				visited[neighbor] = true
				queue = append(queue, neighbor)
			}
		}
	}

	return result
}
