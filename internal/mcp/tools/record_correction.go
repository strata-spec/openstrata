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

		targetType = strings.TrimSpace(targetType)
		targetID = strings.TrimSpace(targetID)
		correctionType = strings.TrimSpace(correctionType)
		newValue = strings.TrimSpace(newValue)

		if missingField := firstMissingRequiredField(targetType, targetID, correctionType, newValue); missingField != "" {
			return toolJSONError(
				fmt.Sprintf("missing required field: %s", missingField),
				"Required fields: target_type, target_id, correction_type, new_value",
			)
		}

		correction := buildCorrection(targetType, targetID, correctionType, newValue, reason)

		model := getModel()
		if !targetResolves(model, correction.TargetType, correction.TargetID) {
			return toolJSONError(
				fmt.Sprintf("target not found: %s", correction.TargetID),
				"Check target_type and target_id match a defined object in the semantic model. Column targets use format model_id.column_name",
			)
		}

		smifVersion := ""
		if model != nil {
			smifVersion = model.SMIFVersion
		}
		if smifVersion == "" {
			smifVersion = s.SMIFVersion()
		}

		if err := overlay.AppendCorrection(s.CorrectionsPath(), smifVersion, correction); err != nil {
			return toolJSONError(
				fmt.Sprintf("failed to write correction: %s", err.Error()),
				"Check that corrections.yaml is writable and not locked by another process",
			)
		}
		if err := s.Reload(); err != nil {
			return toolJSONError(
				fmt.Sprintf("correction written but model reload failed: %s", err.Error()),
				"Restart strata serve to pick up the correction",
			)
		}

		payload := map[string]any{
			"correction_id": correction.CorrectionID,
			"status":        "applied",
			"message":       "Correction recorded and model reloaded.",
		}

		b, err := json.Marshal(payload)
		if err != nil {
			return toolJSONError(
				fmt.Sprintf("failed to serialize correction response: %s", err.Error()),
				"Retry the operation; if this persists, inspect server logs",
			)
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
	targetType = strings.TrimSpace(targetType)
	targetID = strings.TrimSpace(targetID)

	switch targetType {
	case "domain":
		return true
	case "model":
		if model == nil {
			return false
		}
		for _, m := range model.Models {
			if m.ModelID == targetID {
				return true
			}
		}
		return false
	case "column":
		if model == nil {
			return false
		}
		parts := strings.SplitN(targetID, ".", 2)
		if len(parts) != 2 {
			return false
		}
		modelID := strings.TrimSpace(parts[0])
		columnName := strings.TrimSpace(parts[1])
		if modelID == "" || columnName == "" {
			return false
		}
		for _, m := range model.Models {
			if m.ModelID != modelID {
				continue
			}
			for _, c := range m.Columns {
				if c.Name == columnName {
					return true
				}
			}
		}
		return false
	case "relationship":
		if model == nil {
			return false
		}
		for _, r := range model.Relationships {
			if r.RelationshipID == targetID {
				return true
			}
		}
		return false
	case "metric":
		if model == nil {
			return false
		}
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

func firstMissingRequiredField(targetType, targetID, correctionType, newValue string) string {
	if targetType == "" {
		return "target_type"
	}
	if targetID == "" {
		return "target_id"
	}
	if correctionType == "" {
		return "correction_type"
	}
	if newValue == "" {
		return "new_value"
	}
	return ""
}

func toolJSONError(msg, hint string) (*mcp.CallToolResult, error) {
	return mcp.NewToolResultText(fmt.Sprintf(
		`{"error": %q, "hint": %q}`,
		msg, hint,
	)), nil
}
