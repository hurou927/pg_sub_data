package graph

import (
	"github.com/hurou927/db-sub-data/internal/schema"
)

// Edge represents a directed edge from child to parent (FK direction).
type Edge struct {
	FK          schema.ForeignKey
	ChildTable  string // schema.table
	ParentTable string // schema.table
}

// Graph is a directed graph built from FK relationships.
type Graph struct {
	// Tables maps full name -> table
	Tables map[string]*schema.Table

	// Edges are non-self-referential FK edges (child → parent)
	Edges []Edge

	// SelfRefs holds self-referential FKs, keyed by table full name
	SelfRefs map[string][]schema.ForeignKey

	// Children maps parent full name → list of child full names
	Children map[string][]string

	// Parents maps child full name → list of parent full names
	Parents map[string][]string

	// adjacency for undirected connectivity
	Adjacency map[string]map[string]bool
}

// Build constructs a directed graph from introspected tables.
// Tables in excludeSet are skipped. FKs referencing tables outside
// the known set are ignored.
func Build(tables map[string]*schema.Table, excludeSet map[string]bool) *Graph {
	g := &Graph{
		Tables:    make(map[string]*schema.Table),
		SelfRefs:  make(map[string][]schema.ForeignKey),
		Children:  make(map[string][]string),
		Parents:   make(map[string][]string),
		Adjacency: make(map[string]map[string]bool),
	}

	// Filter excluded tables
	for name, tbl := range tables {
		if excludeSet[tbl.Name] {
			continue
		}
		g.Tables[name] = tbl
		g.Adjacency[name] = make(map[string]bool)
	}

	// Build edges
	for name, tbl := range g.Tables {
		for _, fk := range tbl.ForeignKeys {
			parentKey := fk.ParentSchema + "." + fk.ParentTable
			if _, ok := g.Tables[parentKey]; !ok {
				continue // parent table not in scope
			}

			if fk.IsSelfRef {
				g.SelfRefs[name] = append(g.SelfRefs[name], fk)
				continue
			}

			edge := Edge{
				FK:          fk,
				ChildTable:  name,
				ParentTable: parentKey,
			}
			g.Edges = append(g.Edges, edge)
			g.Children[parentKey] = append(g.Children[parentKey], name)
			g.Parents[name] = append(g.Parents[name], parentKey)
			g.Adjacency[name][parentKey] = true
			g.Adjacency[parentKey][name] = true
		}
	}

	return g
}

// Roots returns tables that have no outgoing FK edges (no parents).
func (g *Graph) Roots() []string {
	var roots []string
	for name := range g.Tables {
		if len(g.Parents[name]) == 0 {
			roots = append(roots, name)
		}
	}
	return roots
}
