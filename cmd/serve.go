package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/cobra"
	"github.com/strata-spec/openstrata/internal/localconfig"
	internalmcp "github.com/strata-spec/openstrata/internal/mcp"
	"github.com/strata-spec/openstrata/internal/postgres"
)

// newServeCmd creates the strata serve command.
func newServeCmd() *cobra.Command {
	var semanticPath string
	var correctionsPath string
	var port int

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the MCP server",
		RunE: func(cmd *cobra.Command, args []string) error {
			dbDSN, _ := cmd.Flags().GetString("db")
			if dbDSN == "" {
				lc, err := localconfig.Read(filepath.Dir(semanticPath))
				if err == nil && lc != nil && lc.DB != "" {
					dbDSN = lc.DB
					fmt.Fprintf(os.Stderr, "  Using DB from .strata\n")
				}
			}

			if dbDSN != "" {
				cfg, err := pgconn.ParseConfig(dbDSN)
				if err != nil {
					return wrapCommandError("serve", fmt.Errorf("parse --db: %w", err))
				}
				dbHost := cfg.Host
				pgxPool, err := postgres.Connect(context.Background(), dbDSN)
				if err != nil {
					return wrapCommandError("serve", fmt.Errorf("connect database: %w", err))
				}
				defer pgxPool.Close()

				serverInstance, err := internalmcp.New(semanticPath, correctionsPath, pgxPool)
				if err != nil {
					return wrapCommandError("serve", err)
				}

				fmt.Fprintf(cmd.OutOrStdout(), "Strata MCP server starting on port %d\n", port)
				fmt.Fprintf(cmd.OutOrStdout(), "  Semantic model: %s (%d models)\n", semanticPath, serverInstance.ModelCount())
				fmt.Fprintf(cmd.OutOrStdout(), "  Database: %s [connected]\n", dbHost)
				fmt.Fprintf(cmd.OutOrStdout(), "  Corrections: %s\n", correctionsPath)

				return wrapCommandError("serve", serverInstance.Start(port))
			}

			serverInstance, err := internalmcp.New(semanticPath, correctionsPath, nil)
			if err != nil {
				return wrapCommandError("serve", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Strata MCP server starting on port %d\n", port)
			fmt.Fprintf(cmd.OutOrStdout(), "  Semantic model: %s (%d models)\n", semanticPath, serverInstance.ModelCount())
			fmt.Fprintln(cmd.OutOrStdout(), "  Database: not connected (run_semantic_sql disabled)")
			fmt.Fprintf(cmd.OutOrStdout(), "  Corrections: %s\n", correctionsPath)
			return wrapCommandError("serve", serverInstance.Start(port))
		},
	}

	cmd.Flags().StringVar(&semanticPath, "semantic", "./semantic.yaml", "Path to semantic.yaml")
	cmd.Flags().StringVar(&correctionsPath, "corrections", "./corrections.yaml", "Path to corrections.yaml")
	cmd.Flags().IntVar(&port, "port", 3333, "MCP server port")

	return cmd
}
