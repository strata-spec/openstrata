package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/strata-spec/openstrata/internal/overlay"
	"github.com/strata-spec/openstrata/internal/smif"
)

// newValidateCmd creates the strata validate command.
func newValidateCmd() *cobra.Command {
	var semanticPath string
	var correctionsPath string

	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate semantic.yaml and corrections.yaml against SMIF rules",
		RunE: func(cmd *cobra.Command, args []string) error {
			semantic, err := smif.ReadYAML(semanticPath)
			if err != nil {
				return fmt.Errorf("read semantic yaml: %w", err)
			}

			var corrections *smif.CorrectionsFile
			if _, err := os.Stat(correctionsPath); err == nil {
				loaded, loadErr := overlay.LoadCorrections(correctionsPath)
				if loadErr != nil {
					return fmt.Errorf("read corrections yaml: %w", loadErr)
				}
				corrections = loaded
			} else if !os.IsNotExist(err) {
				return fmt.Errorf("stat corrections yaml: %w", err)
			}

			musts, shoulds := smif.Validate(smif.ValidationDoc{Semantic: semantic, Corrections: corrections})

			for _, v := range musts {
				fmt.Fprintf(os.Stderr, "ERROR: %s %s: %s\n", v.RuleID, v.Path, v.Message)
			}
			for _, v := range shoulds {
				fmt.Fprintf(os.Stderr, "WARN:  %s %s: %s\n", v.RuleID, v.Path, v.Message)
			}

			if len(musts) > 0 {
				return fmt.Errorf("validation failed with %d MUST violation(s)", len(musts))
			}

			fmt.Fprintln(cmd.OutOrStdout(), "✓ semantic.yaml is valid SMIF 0.1.0")
			return nil
		},
	}

	cmd.Flags().StringVar(&semanticPath, "semantic", "./semantic.yaml", "Path to semantic.yaml")
	cmd.Flags().StringVar(&correctionsPath, "corrections", "./corrections.yaml", "Path to corrections.yaml")

	return cmd
}
