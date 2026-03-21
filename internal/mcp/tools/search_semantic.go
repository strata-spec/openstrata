package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	mcp "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/strata-spec/openstrata/internal/smif"
)

// SearchSemantic returns the search_semantic MCP tool definition and handler.
func SearchSemantic(getModel func() *smif.SemanticModel) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool(
		"search_semantic",
		mcp.WithDescription("Search for models, columns, or concepts by name or description"),
		mcp.WithString("query", mcp.Description("Search query"), mcp.Required()),
	)

	type scoredResult struct {
		Score int
		Item  map[string]any
	}

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		_ = ctx
		query, _ := request.Params.Arguments["query"].(string)
		query = strings.TrimSpace(query)
		if query == "" {
			return nil, fmt.Errorf("query is required")
		}
		qLower := strings.ToLower(query)

		model := getModel()
		if model == nil {
			return mcp.NewToolResultText("[]"), nil
		}

		results := make([]scoredResult, 0)
		for _, m := range model.Models {
			if m.Suppressed {
				continue
			}
			if score, field, snippet, ok := rankMatch(qLower, m.Name, m.Label, m.Description); ok {
				results = append(results, scoredResult{
					Score: score,
					Item: map[string]any{
						"type":        "model",
						"model_id":    m.ModelID,
						"name":        m.Name,
						"label":       m.Label,
						"match_field": field,
						"snippet":     snippet,
					},
				})
			}

			for _, c := range m.Columns {
				if c.Suppressed {
					continue
				}
				if score, field, snippet, ok := rankMatch(qLower, c.Name, c.Label, c.Description); ok {
					results = append(results, scoredResult{
						Score: score,
						Item: map[string]any{
							"type":        "column",
							"model_id":    m.ModelID,
							"column_name": c.Name,
							"label":       c.Label,
							"match_field": field,
							"snippet":     snippet,
						},
					})
				}
			}
		}

		for _, c := range model.Concepts {
			if score, field, snippet, ok := rankMatch(qLower, "", c.Label, c.Description); ok {
				results = append(results, scoredResult{
					Score: score,
					Item: map[string]any{
						"type":        "concept",
						"concept_id":  c.ConceptID,
						"label":       c.Label,
						"match_field": field,
						"snippet":     snippet,
					},
				})
			}
		}

		sort.SliceStable(results, func(i, j int) bool {
			if results[i].Score != results[j].Score {
				return results[i].Score > results[j].Score
			}
			iType, _ := results[i].Item["type"].(string)
			jType, _ := results[j].Item["type"].(string)
			if iType != jType {
				return iType < jType
			}
			iLabel, _ := results[i].Item["label"].(string)
			jLabel, _ := results[j].Item["label"].(string)
			return iLabel < jLabel
		})

		if len(results) > 20 {
			results = results[:20]
		}

		out := make([]map[string]any, 0, len(results))
		for _, r := range results {
			out = append(out, r.Item)
		}

		b, err := json.Marshal(out)
		if err != nil {
			return nil, err
		}

		return mcp.NewToolResultText(string(b)), nil
	}

	return tool, handler
}

func rankMatch(queryLower, name, label, description string) (score int, field, snippet string, ok bool) {
	nameLower := strings.ToLower(name)
	labelLower := strings.ToLower(label)
	descLower := strings.ToLower(description)

	if nameLower == queryLower {
		return 300, "name", name, true
	}
	if strings.Contains(labelLower, queryLower) {
		return 200, "label", label, true
	}
	if strings.Contains(nameLower, queryLower) {
		return 150, "name", name, true
	}
	if strings.Contains(descLower, queryLower) {
		return 100, "description", description, true
	}
	return 0, "", "", false
}
