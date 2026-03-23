package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/strata-spec/openstrata/internal/inference"
	"github.com/strata-spec/openstrata/internal/localconfig"
)

// newInitCmd creates the strata init command.
func newInitCmd() *cobra.Command {
	var schema string
	var enableLogMining bool
	var strataMDPath string
	var refresh bool
	var skipProfiling bool
	var profileTimeout int

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize semantic model files from a Postgres database",
		RunE: func(cmd *cobra.Command, args []string) error {
			if dbFlag == "" {
				return fmt.Errorf("--db is required")
			}

			outputDir := "."

			maxTables, _ := cmd.Flags().GetInt("max-tables")
			tablesRaw, _ := cmd.Flags().GetString("tables")
			var tables []string
			if tablesRaw != "" {
				for _, t := range strings.Split(tablesRaw, ",") {
					t = strings.TrimSpace(t)
					if t != "" {
						tables = append(tables, t)
					}
				}
			}

			provider, err := inference.ProviderFromString(llmFlag)
			if err != nil {
				return wrapCommandError("init", err)
			}

			cfg := inference.Config{
				DSN:                dbFlag,
				Schema:             schema,
				EnableLogMining:    enableLogMining,
				StrataMDPath:       strataMDPath,
				LLM:                provider,
				Progress:           inference.NewStderrProgress(os.Stderr),
				MaxTables:          maxTables,
				Tables:             tables,
				SkipProfiling:      skipProfiling,
				ProfileTimeoutSecs: profileTimeout,
			}

			ctx := context.Background()
			if refresh {
				err = inference.Refresh(ctx, cfg)
			} else {
				err = inference.Init(ctx, cfg)
			}
			if err != nil {
				return wrapCommandError("init", err)
			}

			lc := localconfig.Config{
				DB:     dbFlag,
				Schema: schema,
			}
			if err := localconfig.Write(outputDir, lc); err != nil {
				fmt.Fprintf(os.Stderr, "⚠  could not write .strata: %v\n", err)
			}
			if err := localconfig.EnsureGitignored(outputDir); err != nil {
				fmt.Fprintf(os.Stderr, "⚠  could not update .gitignore: %v\n", err)
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&schema, "schema", "public", "Postgres schema name")
	cmd.Flags().BoolVar(&enableLogMining, "enable-log-mining", false, "Enable pg_stat_statements mining")
	cmd.Flags().StringVar(&strataMDPath, "strata-md", "./strata.md", "Path to strata.md")
	cmd.Flags().BoolVar(&refresh, "refresh", false, "Re-run inference and merge with corrections")
	cmd.Flags().BoolVar(&skipProfiling, "skip-profiling", false,
		"Skip Stage 3 sample profiling entirely. Columns will have "+
			"cardinality_category: unknown and no example values. "+
			"Useful for remote databases with high latency.")
	cmd.Flags().IntVar(&profileTimeout, "profile-timeout", 30,
		"Per-table timeout in seconds for sample profiling. "+
			"0 = no limit. Tables that exceed the timeout are skipped "+
			"with cardinality_category: unknown.")
	cmd.Flags().Int("max-tables", 0, "Abort if the schema has more than this many tables. 0 = no limit.")
	cmd.Flags().String("tables", "",
		"Comma-separated list of table names to process. "+
			"When set, only these tables pass through the LLM pipeline. "+
			"FK introspection runs on the full schema first. "+
			"Example: --tables orders,users,products")

	return cmd
}

// isMultilineError returns true if err's message contains a newline.
func isMultilineError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "\n")
}

func wrapCommandError(command string, err error) error {
	if err == nil {
		return nil
	}
	if isMultilineError(err) {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return fmt.Errorf("%s failed", command)
	}
	return fmt.Errorf("%s failed: %w", command, err)
}
