package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/hurou927/db-sub-data/internal/config"
)

var (
	cfgPath string
	cfg     *config.Config
)

var rootCmd = &cobra.Command{
	Use:   "db-sub-data",
	Short: "Extract a subset of PostgreSQL data preserving FK dependencies",
	Long: `db-sub-data connects to a PostgreSQL database, builds an FK dependency graph,
and extracts a consistent subset of data starting from specified root tables.
The output is in pg_dump-compatible COPY format.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if cfgPath == "" {
			return fmt.Errorf("--config is required")
		}
		var err error
		cfg, err = config.Load(cfgPath)
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgPath, "config", "", "path to YAML config file (required)")
}

// Execute runs the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
