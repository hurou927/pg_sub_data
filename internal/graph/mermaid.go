package graph

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// WriteMermaid writes the graph in Mermaid format to w.
// Each connected component is a subgraph.
func WriteMermaid(w io.Writer, g *Graph) error {
	components := FindComponents(g)

	// Sort components for deterministic output
	for i := range components {
		sort.Strings(components[i].Tables)
	}
	sort.Slice(components, func(i, j int) bool {
		if len(components[i].Tables) == 0 {
			return true
		}
		if len(components[j].Tables) == 0 {
			return false
		}
		return components[i].Tables[0] < components[j].Tables[0]
	})

	fmt.Fprintln(w, "graph TD")

	for i, comp := range components {
		fmt.Fprintf(w, "    subgraph component_%d\n", i+1)

		tableSet := make(map[string]bool, len(comp.Tables))
		for _, t := range comp.Tables {
			tableSet[t] = true
		}

		// Collect edges for this component
		edgesWritten := make(map[string]bool)
		for _, edge := range g.Edges {
			if !tableSet[edge.ChildTable] {
				continue
			}
			label := strings.Join(edge.FK.ChildColumns, ", ")
			edgeKey := fmt.Sprintf("%s-->%s:%s", mermaidID(edge.ChildTable), mermaidID(edge.ParentTable), label)
			if edgesWritten[edgeKey] {
				continue
			}
			edgesWritten[edgeKey] = true
			fmt.Fprintf(w, "        %s -->|%s| %s\n",
				mermaidID(edge.ChildTable), label, mermaidID(edge.ParentTable))
		}

		// Write self-referential edges
		for _, t := range comp.Tables {
			if selfRefs, ok := g.SelfRefs[t]; ok {
				for _, fk := range selfRefs {
					label := strings.Join(fk.ChildColumns, ", ")
					fmt.Fprintf(w, "        %s -->|%s| %s\n",
						mermaidID(t), label, mermaidID(t))
				}
			}
		}

		// Write standalone nodes (roots with no edges in this component)
		for _, t := range comp.Tables {
			if !hasEdge(g, t, tableSet) {
				fmt.Fprintf(w, "        %s\n", mermaidID(t))
			}
		}

		fmt.Fprintln(w, "    end")
		if i < len(components)-1 {
			fmt.Fprintln(w)
		}
	}

	return nil
}

// WriteText writes a text summary of the graph to w.
func WriteText(w io.Writer, g *Graph) error {
	components := FindComponents(g)

	// Sort for deterministic output
	for i := range components {
		sort.Strings(components[i].Tables)
	}
	sort.Slice(components, func(i, j int) bool {
		if len(components[i].Tables) == 0 {
			return true
		}
		if len(components[j].Tables) == 0 {
			return false
		}
		return components[i].Tables[0] < components[j].Tables[0]
	})

	fmt.Fprintf(w, "Tables: %d\n", len(g.Tables))
	fmt.Fprintf(w, "Foreign Keys: %d\n", len(g.Edges)+countSelfRefs(g))
	fmt.Fprintf(w, "Connected Components: %d\n\n", len(components))

	topoResult := TopoSortAll(g)
	if topoResult.HasCycle {
		fmt.Fprintf(w, "WARNING: Circular dependencies detected: %v\n\n", topoResult.CycleTables)
	}

	// Warn about tables without PKs
	var noPKTables []string
	for name, tbl := range g.Tables {
		if tbl.PrimaryKey == nil {
			noPKTables = append(noPKTables, name)
		}
	}
	if len(noPKTables) > 0 {
		sort.Strings(noPKTables)
		fmt.Fprintf(w, "WARNING: Tables without primary key: %v\n\n", noPKTables)
	}

	// Self-referencing tables
	if len(g.SelfRefs) > 0 {
		var selfRefTables []string
		for t := range g.SelfRefs {
			selfRefTables = append(selfRefTables, t)
		}
		sort.Strings(selfRefTables)
		fmt.Fprintf(w, "Self-referencing tables: %v\n\n", selfRefTables)
	}

	roots := g.Roots()
	sort.Strings(roots)
	fmt.Fprintf(w, "Root tables (no FK parents): %v\n\n", roots)

	for i, comp := range components {
		fmt.Fprintf(w, "=== Component %d (%d tables) ===\n", i+1, len(comp.Tables))

		topoComp := TopoSort(g, comp.Tables)
		if topoComp.HasCycle {
			fmt.Fprintf(w, "  Topological order (partial, has cycle):\n")
		} else {
			fmt.Fprintf(w, "  Topological order:\n")
		}
		for j, t := range topoComp.Order {
			tbl := g.Tables[t]
			pkInfo := "no PK"
			if tbl.PrimaryKey != nil {
				pkInfo = fmt.Sprintf("PK: %s", strings.Join(tbl.PrimaryKey.Columns, ", "))
			}
			fkCount := 0
			for _, fk := range tbl.ForeignKeys {
				if !fk.IsSelfRef {
					fkCount++
				}
			}
			fmt.Fprintf(w, "    %d. %s (%d cols, %s, %d FKs)\n",
				j+1, t, len(tbl.Columns), pkInfo, fkCount)
		}
		if topoComp.HasCycle {
			fmt.Fprintf(w, "  Cycle tables: %v\n", topoComp.CycleTables)
		}
		fmt.Fprintln(w)
	}

	return nil
}

// mermaidID converts a schema.table name to a Mermaid-safe node ID.
func mermaidID(fullName string) string {
	return strings.ReplaceAll(fullName, ".", "_")
}

func hasEdge(g *Graph, table string, componentTables map[string]bool) bool {
	// Check if this table appears as child or parent in any edge within the component
	for _, edge := range g.Edges {
		if edge.ChildTable == table && componentTables[edge.ParentTable] {
			return true
		}
		if edge.ParentTable == table && componentTables[edge.ChildTable] {
			return true
		}
	}
	// Check self-referential
	if _, ok := g.SelfRefs[table]; ok {
		return true
	}
	return false
}

func countSelfRefs(g *Graph) int {
	count := 0
	for _, fks := range g.SelfRefs {
		count += len(fks)
	}
	return count
}
