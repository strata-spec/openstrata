package tools

import (
	"context"
	"encoding/json"
	"fmt"

	mcp "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/strata-spec/openstrata/internal/smif"
)

// GetModel returns the get_model MCP tool definition and handler.
func GetModel(getModel func() *smif.SemanticModel) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool(
		"get_model",
		mcp.WithDescription("Get full detail for a single model including all columns and relationships"),
		mcp.WithString("model_id", mcp.Description("Model identifier"), mcp.Required()),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		_ = ctx
		modelID, _ := request.Params.Arguments["model_id"].(string)
		if modelID == "" {
			return nil, fmt.Errorf("model_id is required")
		}

		doc := getModel()
		if doc == nil {
			return nil, fmt.Errorf("model '%s' not found", modelID)
		}

		var selected *smif.Model
		for i := range doc.Models {
			if doc.Models[i].ModelID == modelID {
				selected = &doc.Models[i]
				break
			}
		}
		if selected == nil {
			return nil, fmt.Errorf("model '%s' not found", modelID)
		}
		if selected.Suppressed {
			return nil, fmt.Errorf("model '%s' is suppressed", modelID)
		}

		colSet := make(map[string]struct{})
		columns := make([]smif.Column, 0, len(selected.Columns))
		for _, c := range selected.Columns {
			if c.Suppressed {
				continue
			}
			columns = append(columns, c)
			colSet[c.Name] = struct{}{}
		}

		relationships := make([]smif.Relationship, 0)
		for _, r := range doc.Relationships {
			if r.Suppressed {
				continue
			}
			if r.FromModel == modelID {
				if _, ok := colSet[r.FromColumn]; !ok {
					continue
				}
			}
			if r.ToModel == modelID {
				if _, ok := colSet[r.ToColumn]; !ok {
					continue
				}
			}
			if r.FromModel == modelID || r.ToModel == modelID {
				relationships = append(relationships, r)
			}
		}

		metrics := make([]smif.Metric, 0)
		for _, mt := range doc.Metrics {
			if mt.Suppressed {
				continue
			}
			if metricReferencesModel(mt, modelID) {
				metrics = append(metrics, mt)
			}
		}

		result := struct {
			Model         smif.Model          `json:"model"`
			Relationships []smif.Relationship `json:"relationships"`
			Metrics       []smif.Metric       `json:"metrics"`
		}{
			Model: smif.Model{
				ModelID:        selected.ModelID,
				Name:           selected.Name,
				Label:          selected.Label,
				Grain:          selected.Grain,
				Description:    selected.Description,
				Suppressed:     selected.Suppressed,
				PhysicalSource: selected.PhysicalSource,
				PrimaryKey:     selected.PrimaryKey,
				DDLFingerprint: selected.DDLFingerprint,
				RowCountEst:    selected.RowCountEst,
				Columns:        columns,
				Provenance:     selected.Provenance,
				XProperties:    selected.XProperties,
			},
			Relationships: relationships,
			Metrics:       metrics,
		}

		b, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		return mcp.NewToolResultText(string(b)), nil
	}

	return tool, handler
}

func metricReferencesModel(metric smif.Metric, modelID string) bool {
	if metric.DefaultTimeDimension != nil && metric.DefaultTimeDimension.Model == modelID {
		return true
	}
	for _, d := range metric.ValidDimensions {
		if d.Model == modelID {
			return true
		}
	}
	for _, d := range metric.InvalidDimensions {
		if d.Model == modelID {
			return true
		}
	}
	return false
}
