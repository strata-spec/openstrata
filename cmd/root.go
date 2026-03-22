// Package cmd provides the CLI commands for Strata.
package cmd

import (
	"os"

	"github.com/spf13/cobra"
	appversion "github.com/strata-spec/openstrata/internal/version"
)

var (
	// Global persistent flags
	dbFlag  string
	llmFlag string
)

// rootCmd represents the base command when called without any subcommands.
var rootCmd = newRootCmd()

// newRootCmd builds and configures the root command and its subcommands.
func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "strata",
		Short:   "Strata - PostgreSQL semantic layer inference and serving",
		Long:    "Strata connects to PostgreSQL, infers a semantic layer, and outputs SMIF documents.",
		Version: appversion.Version,
	}

	cmd.PersistentFlags().StringVar(&dbFlag, "db", "", "Postgres connection string")
	cmd.PersistentFlags().StringVar(&llmFlag, "llm", "anthropic", "LLM provider: anthropic (default) or openai")

	cmd.AddCommand(newInitCmd())
	cmd.AddCommand(newServeCmd())
	cmd.AddCommand(newValidateCmd())
	cmd.AddCommand(newCorrectCmd())

	return cmd
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
