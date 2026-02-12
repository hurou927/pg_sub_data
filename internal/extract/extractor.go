package extract

import (
	"context"
	"fmt"
	"io"
	"log"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hurou927/db-sub-data/internal/config"
	"github.com/hurou927/db-sub-data/internal/graph"
	"github.com/hurou927/db-sub-data/internal/output"
	"github.com/hurou927/db-sub-data/internal/schema"
)

// Extractor orchestrates the subset extraction process.
type Extractor struct {
	pool    *pgxpool.Pool
	cfg     *config.Config
	g       *graph.Graph
	verbose bool
	dryRun  bool

	// collected holds extracted rows per table (full name → rows)
	collected map[string][][]any
	// collectedPKs holds PK values per table for child lookups
	collectedPKs map[string][][]any
}

// New creates a new Extractor.
func New(pool *pgxpool.Pool, cfg *config.Config, g *graph.Graph, verbose, dryRun bool) *Extractor {
	return &Extractor{
		pool:         pool,
		cfg:          cfg,
		g:            g,
		verbose:      verbose,
		dryRun:       dryRun,
		collected:    make(map[string][][]any),
		collectedPKs: make(map[string][][]any),
	}
}

// Extract performs the extraction and writes the output.
func (e *Extractor) Extract(ctx context.Context, w io.Writer) error {
	// Build root table lookup: table name → WHERE clause
	rootWhere := make(map[string]string)
	for _, r := range e.cfg.Roots {
		rootWhere[r.Table] = r.Where
	}

	// Get topological order
	topoResult := graph.TopoSortAll(e.g)
	if topoResult.HasCycle {
		log.Printf("WARNING: Circular dependencies detected: %v", topoResult.CycleTables)
		log.Printf("Tables in cycles will be handled with session_replication_role = 'replica'")
	}

	// Process tables in topological order (parents first)
	order := topoResult.Order
	// Append cycle tables at the end
	if topoResult.HasCycle {
		order = append(order, topoResult.CycleTables...)
	}

	for _, tableName := range order {
		tbl, ok := e.g.Tables[tableName]
		if !ok {
			continue
		}

		if where, isRoot := rootWhere[tbl.Name]; isRoot {
			if err := e.extractRoot(ctx, tbl, where); err != nil {
				return fmt.Errorf("extracting root %s: %w", tableName, err)
			}
		} else if len(e.g.Parents[tableName]) > 0 {
			if err := e.extractChild(ctx, tbl); err != nil {
				return fmt.Errorf("extracting child %s: %w", tableName, err)
			}
		}
		// Tables with no parents and not a root: skip (isolated or no config)

		// Handle self-referencing FKs
		if selfRefs, ok := e.g.SelfRefs[tableName]; ok && len(selfRefs) > 0 {
			if err := e.extractSelfRef(ctx, tbl, selfRefs); err != nil {
				return fmt.Errorf("extracting self-ref %s: %w", tableName, err)
			}
		}
	}

	if e.dryRun {
		return nil
	}

	// Write output in topological order
	cw := output.NewWriter(w)
	if err := cw.WriteHeader(); err != nil {
		return err
	}

	for _, tableName := range order {
		tbl, ok := e.g.Tables[tableName]
		if !ok {
			continue
		}
		rows := e.collected[tableName]
		if err := cw.WriteTableData(tbl, rows); err != nil {
			return fmt.Errorf("writing %s: %w", tableName, err)
		}
	}

	return cw.WriteFooter()
}

func (e *Extractor) extractRoot(ctx context.Context, table *schema.Table, where string) error {
	query := buildRootQuery(table, where)

	if e.verbose || e.dryRun {
		fmt.Printf("[root] %s: %s\n", table.FullName(), query)
	}
	if e.dryRun {
		return nil
	}

	rows, err := e.pool.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return err
		}
		e.addRow(table, values)
	}

	if e.verbose {
		fmt.Printf("  -> %d rows\n", len(e.collected[table.FullName()]))
	}
	return rows.Err()
}

func (e *Extractor) extractChild(ctx context.Context, table *schema.Table) error {
	query, args := buildChildQuery(table, nil, e.collectedPKs)
	if query == "" {
		return nil
	}

	if e.verbose || e.dryRun {
		fmt.Printf("[child] %s: %s\n", table.FullName(), query)
		if e.dryRun {
			fmt.Printf("  args: %v\n", args)
		}
	}
	if e.dryRun {
		return nil
	}

	rows, err := e.pool.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return err
		}
		e.addRow(table, values)
	}

	if e.verbose {
		fmt.Printf("  -> %d rows\n", len(e.collected[table.FullName()]))
	}
	return rows.Err()
}

func (e *Extractor) extractSelfRef(ctx context.Context, table *schema.Table, selfRefs []schema.ForeignKey) error {
	seedPKs := e.collectedPKs[table.FullName()]
	if len(seedPKs) == 0 {
		return nil
	}

	for _, fk := range selfRefs {
		extraRows, err := fetchSelfRefRows(ctx, e.pool, table, fk, seedPKs, e.verbose || e.dryRun)
		if err != nil {
			return err
		}

		// Add new rows (avoid duplicates by PK)
		existing := e.pkSet(table)
		for _, row := range extraRows {
			pkVals := e.extractPK(table, row)
			key := fmt.Sprintf("%v", pkVals)
			if !existing[key] {
				e.addRow(table, row)
				existing[key] = true
			}
		}

		if e.verbose {
			fmt.Printf("  [self-ref] %s: total %d rows after recursive\n",
				table.FullName(), len(e.collected[table.FullName()]))
		}
	}
	return nil
}

func (e *Extractor) addRow(table *schema.Table, values []any) {
	fullName := table.FullName()
	e.collected[fullName] = append(e.collected[fullName], values)

	// Extract and store PK values for child lookups
	pkVals := e.extractPK(table, values)
	if pkVals != nil {
		e.collectedPKs[fullName] = append(e.collectedPKs[fullName], pkVals)
	}
}

func (e *Extractor) extractPK(table *schema.Table, values []any) []any {
	if table.PrimaryKey == nil {
		return nil
	}

	pkIdxs := e.pkColumnIndexes(table)
	pk := make([]any, len(pkIdxs))
	for i, idx := range pkIdxs {
		if idx < len(values) {
			pk[i] = values[idx]
		}
	}
	return pk
}

func (e *Extractor) pkColumnIndexes(table *schema.Table) []int {
	if table.PrimaryKey == nil {
		return nil
	}
	colIdx := make(map[string]int)
	for i, col := range table.Columns {
		colIdx[col.Name] = i
	}
	idxs := make([]int, len(table.PrimaryKey.Columns))
	for i, col := range table.PrimaryKey.Columns {
		idxs[i] = colIdx[col]
	}
	return idxs
}

func (e *Extractor) pkSet(table *schema.Table) map[string]bool {
	set := make(map[string]bool)
	for _, row := range e.collected[table.FullName()] {
		pk := e.extractPK(table, row)
		set[fmt.Sprintf("%v", pk)] = true
	}
	return set
}

// CollectedSummary returns a summary of collected rows for reporting.
func (e *Extractor) CollectedSummary() []string {
	var lines []string
	keys := make([]string, 0, len(e.collected))
	for k := range e.collected {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		lines = append(lines, fmt.Sprintf("  %s: %d rows", k, len(e.collected[k])))
	}
	return lines
}
