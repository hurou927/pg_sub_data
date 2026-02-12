package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/hurou927/db-sub-data/internal/db"
	"github.com/hurou927/db-sub-data/internal/graph"
	"github.com/hurou927/db-sub-data/internal/schema"
)

var analyzeFormat string

var analyzeCmd = &cobra.Command{
	Use:   "analyze",
	Short: "Analyze FK dependency graph and output structure",
	Long:  `Connects to the database, introspects the schema, builds an FK dependency graph, and outputs it in the specified format.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		pool, err := db.NewPool(ctx, &cfg.Connection)
		if err != nil {
			return fmt.Errorf("connecting to database: %w", err)
		}
		defer pool.Close()

		tables, err := schema.Introspect(ctx, pool, cfg.Schemas)
		if err != nil {
			return fmt.Errorf("introspecting schema: %w", err)
		}

		g := graph.Build(tables, nil)

		switch analyzeFormat {
		case "mermaid":
			return graph.WriteMermaid(os.Stdout, g)
		case "text":
			return graph.WriteText(os.Stdout, g)
		default:
			return fmt.Errorf("unknown format: %s (supported: mermaid, text)", analyzeFormat)
		}
	},
}

func init() {
	analyzeCmd.Flags().StringVar(&analyzeFormat, "format", "mermaid", "output format: mermaid or text")
	rootCmd.AddCommand(analyzeCmd)
}
