package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	mcp "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/strata-spec/openstrata/internal/overlay"
	"github.com/strata-spec/openstrata/internal/smif"
)

type ReloadableServer interface {
	Reload() error
	CorrectionsPath() string
	SMIFVersion() string
}

// RecordCorrection returns the record_correction MCP tool definition and handler.
func RecordCorrection(s ReloadableServer, getModel func() *smif.SemanticModel) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool(
		"record_correction",
		mcp.WithDescription("Record a human correction to the semantic model"),
		mcp.WithString("target_type", mcp.Required()),
		mcp.WithString("target_id", mcp.Required()),
		mcp.WithString("correction_type", mcp.Required()),
		mcp.WithString("new_value", mcp.Required()),
		mcp.WithString("reason"),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		_ = ctx
		targetType, _ := request.Params.Arguments["target_type"].(string)
		targetID, _ := request.Params.Arguments["target_id"].(string)
		correctionType, _ := request.Params.Arguments["correction_type"].(string)
		newValue, _ := request.Params.Arguments["new_value"].(string)
		reason, _ := request.Params.Arguments["reason"].(string)

		if strings.TrimSpace(targetType) == "" || strings.TrimSpace(targetID) == "" || strings.TrimSpace(correctionType) == "" || strings.TrimSpace(newValue) == "" {
			return nil, fmt.Errorf("target_type, target_id, correction_type, and new_value are required")
		}

		correction := buildCorrection(targetType, targetID, correctionType, newValue, reason)

		model := getModel()
		if !targetResolves(model, correction.TargetType, correction.TargetID) {
			return mcp.NewToolResultError("target_id does not resolve to a defined object"), nil
		}

		smifVersion := ""
		if model != nil {
			smifVersion = model.SMIFVersion
		}
		if smifVersion == "" {
			smifVersion = s.SMIFVersion()
		}

		if err := overlay.AppendCorrection(s.CorrectionsPath(), smifVersion, correction); err != nil {
			return nil, err
		}
		if err := s.Reload(); err != nil {
			return nil, err
		}

		payload := map[string]any{
			"correction_id": correction.CorrectionID,
			"status":        "applied",
			"message":       "Correction recorded and model reloaded.",
		}

		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		return mcp.NewToolResultText(string(b)), nil
	}

	return tool, handler
}

func buildCorrection(targetType, targetID, correctionType, newValue, reason string) overlay.Correction {
	raw := strings.ReplaceAll(uuid.NewString(), "-", "")
	if len(raw) > 8 {
		raw = raw[:8]
	}

	return overlay.Correction{
		CorrectionID:   "corr_" + raw,
		TargetType:     targetType,
		TargetID:       targetID,
		CorrectionType: correctionType,
		NewValue:       newValue,
		Reason:         reason,
		Source:         "user_defined",
		Status:         "approved",
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
	}
}

func targetResolves(model *smif.SemanticModel, targetType, targetID string) bool {
	if model == nil {
		return false
	}
	switch targetType {
	case "domain":
		return true
	case "model":
		for _, m := range model.Models {
			if m.ModelID == targetID {
				return true
			}
		}
		return false
	case "column":
		parts := strings.SplitN(targetID, ".", 2)
		if len(parts) != 2 {
			return false
		}
		for _, m := range model.Models {
			if m.ModelID != parts[0] {
				continue
			}
			for _, c := range m.Columns {
				if c.Name == parts[1] {
					return true
				}
			}
		}
		return false
	case "relationship":
		for _, r := range model.Relationships {
			if r.RelationshipID == targetID {
				return true
			}
		}
		return false
	case "metric":
		for _, m := range model.Metrics {
			if m.Name == targetID {
				return true
			}
		}
		return false
	default:
		return false
	}
}
