package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/strata-spec/openstrata/internal/inference"
)

// newInitCmd creates the strata init command.
func newInitCmd() *cobra.Command {
	var schema string
	var enableLogMining bool
	var strataMDPath string
	var refresh bool

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize semantic model files from a Postgres database",
		RunE: func(cmd *cobra.Command, args []string) error {
			if dbFlag == "" {
				return fmt.Errorf("--db is required")
			}

			maxTables, _ := cmd.Flags().GetInt("max-tables")

			provider, err := inference.ProviderFromString(llmFlag)
			if err != nil {
				return err
			}

			cfg := inference.Config{
				DSN:             dbFlag,
				Schema:          schema,
				EnableLogMining: enableLogMining,
				StrataMDPath:    strataMDPath,
				LLM:             provider,
				Progress:        inference.NewStderrProgress(os.Stderr),
				MaxTables:       maxTables,
			}

			ctx := context.Background()
			if refresh {
				err = inference.Refresh(ctx, cfg)
			} else {
				err = inference.Init(ctx, cfg)
			}
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return err
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&schema, "schema", "public", "Postgres schema name")
	cmd.Flags().BoolVar(&enableLogMining, "enable-log-mining", false, "Enable pg_stat_statements mining")
	cmd.Flags().StringVar(&strataMDPath, "strata-md", "./strata.md", "Path to strata.md")
	cmd.Flags().BoolVar(&refresh, "refresh", false, "Re-run inference and merge with corrections")
	cmd.Flags().Int("max-tables", 0, "Abort if the schema has more than this many tables. 0 = no limit.")

	return cmd
}
