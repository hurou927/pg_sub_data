package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/hurou927/db-sub-data/internal/db"
	"github.com/hurou927/db-sub-data/internal/extract"
	"github.com/hurou927/db-sub-data/internal/graph"
	"github.com/hurou927/db-sub-data/internal/schema"
)

var (
	outputPath string
	dryRun     bool
	verbose    bool
)

var extractCmd = &cobra.Command{
	Use:   "extract",
	Short: "Extract a data subset preserving FK dependencies",
	Long:  `Extracts data starting from root tables, following FK dependencies in topological order, and outputs in pg_dump-compatible COPY format.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		pool, err := db.NewPool(ctx, &cfg.Connection)
		if err != nil {
			return fmt.Errorf("connecting to database: %w", err)
		}
		defer pool.Close()

		if err := cfg.ValidateForExtract(); err != nil {
			return err
		}

		tables, err := schema.Introspect(ctx, pool, cfg.Schemas)
		if err != nil {
			return fmt.Errorf("introspecting schema: %w", err)
		}

		g := graph.Build(tables, cfg.ExcludeSet(), cfg.VirtualRelations)

		// Validate that all root tables exist in the graph
		for _, root := range cfg.Roots {
			found := false
			for _, tbl := range g.Tables {
				if tbl.Name == root.Table {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("root table %q not found in schema", root.Table)
			}
		}

		extractor := extract.New(pool, cfg, g, verbose, dryRun)

		// Determine output destination
		outPath := outputPath
		if outPath == "" {
			outPath = cfg.Output
		}

		var w *os.File
		if dryRun || outPath == "" || outPath == "-" {
			w = os.Stdout
		} else {
			w, err = os.Create(outPath)
			if err != nil {
				return fmt.Errorf("creating output file: %w", err)
			}
			defer w.Close()
		}

		if err := extractor.Extract(ctx, w); err != nil {
			return err
		}

		if !dryRun {
			summary := extractor.CollectedSummary()
			fmt.Fprintln(os.Stderr, "Extraction complete:")
			for _, line := range summary {
				fmt.Fprintln(os.Stderr, line)
			}
			if outPath != "" && outPath != "-" {
				fmt.Fprintf(os.Stderr, "Output written to: %s\n", outPath)
			}
		}

		return nil
	},
}

func init() {
	extractCmd.Flags().StringVar(&outputPath, "output", "", "output file path (overrides config)")
	extractCmd.Flags().BoolVar(&dryRun, "dry-run", false, "show queries without executing")
	extractCmd.Flags().BoolVar(&verbose, "verbose", false, "show detailed progress")
	rootCmd.AddCommand(extractCmd)
}
