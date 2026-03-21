package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/strata-spec/openstrata/internal/overlay"
	"github.com/strata-spec/openstrata/internal/smif"
)

// newCorrectCmd creates the strata correct command.
func newCorrectCmd() *cobra.Command {
	var semanticPath string
	var correctionsPath string
	var correctionJSON string

	cmd := &cobra.Command{
		Use:   "correct",
		Short: "Apply a correction record",
		RunE: func(cmd *cobra.Command, args []string) error {
			semantic, err := smif.ReadYAML(semanticPath)
			if err != nil {
				return fmt.Errorf("read semantic yaml: %w", err)
			}

			correction, err := buildCorrection(cmd, correctionJSON)
			if err != nil {
				return err
			}

			if err := validateCorrectionTarget(semantic, correction); err != nil {
				fmt.Fprintln(cmd.ErrOrStderr(), err.Error())
				return err
			}

			if err := overlay.AppendCorrection(correctionsPath, semantic.SMIFVersion, correction); err != nil {
				return fmt.Errorf("append correction: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "✓ Correction recorded: %s\n", correction.CorrectionID)
			return nil
		},
	}

	cmd.Flags().StringVar(&semanticPath, "semantic", "./semantic.yaml", "Path to semantic.yaml")
	cmd.Flags().StringVar(&correctionsPath, "corrections", "./corrections.yaml", "Path to corrections.yaml")
	cmd.Flags().StringVar(&correctionJSON, "json", "", "Correction record as a JSON string")

	return cmd
}

func buildCorrection(cmd *cobra.Command, correctionJSON string) (smif.Correction, error) {
	if strings.TrimSpace(correctionJSON) != "" {
		var c smif.Correction
		if err := json.Unmarshal([]byte(correctionJSON), &c); err != nil {
			return smif.Correction{}, fmt.Errorf("parse --json correction: %w", err)
		}
		if strings.TrimSpace(c.TargetType) == "" || strings.TrimSpace(c.TargetID) == "" || strings.TrimSpace(c.CorrectionType) == "" || strings.TrimSpace(fmt.Sprint(c.NewValue)) == "" {
			return smif.Correction{}, fmt.Errorf("--json requires target_type, target_id, correction_type, and new_value")
		}
		if strings.TrimSpace(c.CorrectionID) == "" {
			c.CorrectionID = "corr_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
		}
		c.Timestamp = time.Now().UTC().Format(time.RFC3339)
		c.Source = "user_defined"
		c.Status = "approved"
		return c, nil
	}

	scanner := bufio.NewScanner(cmd.InOrStdin())
	read := func(prompt string) (string, error) {
		fmt.Fprint(cmd.OutOrStdout(), prompt)
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return "", err
			}
			return "", fmt.Errorf("input cancelled")
		}
		return strings.TrimSpace(scanner.Text()), nil
	}

	targetType, err := read("Target type (domain/model/column/relationship/metric): ")
	if err != nil {
		return smif.Correction{}, err
	}
	targetID, err := read("Target ID: ")
	if err != nil {
		return smif.Correction{}, err
	}
	correctionType, err := read("Correction type: ")
	if err != nil {
		return smif.Correction{}, err
	}
	newValue, err := read("New value: ")
	if err != nil {
		return smif.Correction{}, err
	}
	reason, err := read("Reason (optional): ")
	if err != nil {
		return smif.Correction{}, err
	}

	if targetType == "" || targetID == "" || correctionType == "" || newValue == "" {
		return smif.Correction{}, fmt.Errorf("target_type, target_id, correction_type, and new_value are required")
	}

	id := strings.ReplaceAll(uuid.NewString(), "-", "")
	if len(id) > 8 {
		id = id[:8]
	}

	return smif.Correction{
		CorrectionID:   "corr_" + id,
		TargetType:     targetType,
		TargetID:       targetID,
		CorrectionType: correctionType,
		NewValue:       newValue,
		Reason:         reason,
		Source:         "user_defined",
		Status:         "approved",
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func validateCorrectionTarget(semantic *smif.SemanticModel, correction smif.Correction) error {
	doc := smif.ValidationDoc{
		Semantic: semantic,
		Corrections: &smif.CorrectionsFile{
			SMIFVersion: semantic.SMIFVersion,
			Corrections: []smif.Correction{correction},
		},
	}

	for _, rule := range smif.AllRules() {
		if rule.ID != "V-042" {
			continue
		}
		violations := rule.Check(&doc)
		if len(violations) > 0 {
			return fmt.Errorf("target_id does not resolve: %s", violations[0].Message)
		}
		break
	}

	return nil
}
