package tools

import (
	"context"
	"encoding/json"
	"sort"

	mcp "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/strata-spec/openstrata/internal/smif"
)

// ListModels returns the list_models MCP tool definition and handler.
func ListModels(getModel func() *smif.SemanticModel) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool(
		"list_models",
		mcp.WithDescription("List all models in the semantic layer"),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		_ = ctx
		_ = request

		model := getModel()
		if model == nil {
			return mcp.NewToolResultText("[]"), nil
		}

		type modelRow struct {
			ModelID     string `json:"model_id"`
			Name        string `json:"name"`
			Label       string `json:"label"`
			Description string `json:"description"`
			Grain       string `json:"grain"`
			ColumnCount int    `json:"column_count"`
			Suppressed  bool   `json:"suppressed"`
		}

		rows := make([]modelRow, 0, len(model.Models))
		for _, m := range model.Models {
			if m.Suppressed {
				continue
			}
			rows = append(rows, modelRow{
				ModelID:     m.ModelID,
				Name:        m.Name,
				Label:       m.Label,
				Description: m.Description,
				Grain:       m.Grain,
				ColumnCount: len(m.Columns),
				Suppressed:  m.Suppressed,
			})
		}

		sort.Slice(rows, func(i, j int) bool {
			return rows[i].ModelID < rows[j].ModelID
		})

		b, err := json.Marshal(rows)
		if err != nil {
			return nil, err
		}
		return mcp.NewToolResultText(string(b)), nil
	}

	return tool, handler
}
