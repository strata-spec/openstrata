package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode"

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
		Score     int
		FieldRank int
		Item      map[string]any
	}

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		_ = ctx
		query, _ := request.Params.Arguments["query"].(string)
		query = strings.TrimSpace(query)
		if query == "" {
			return nil, fmt.Errorf("query is required")
		}
		tokens := tokeniseQuery(query)
		if len(tokens) == 0 {
			return mcp.NewToolResultText("[]"), nil
		}

		model := getModel()
		if model == nil {
			return mcp.NewToolResultText("[]"), nil
		}

		results := make([]scoredResult, 0)
		for _, m := range model.Models {
			if m.Suppressed {
				continue
			}
			score := scoreCandidate(tokens, m.Name, m.Label, m.Description)
			if score > 0 {
				field, snippet := bestMatchField(tokens, m.Name, m.Label, m.Description)
				results = append(results, scoredResult{
					Score:     score,
					FieldRank: matchFieldRank(field),
					Item: map[string]any{
						"type":        "model",
						"model_id":    m.ModelID,
						"name":        m.Name,
						"label":       m.Label,
						"score":       score,
						"match_field": field,
						"snippet":     snippet,
					},
				})
			}

			for _, c := range m.Columns {
				if c.Suppressed {
					continue
				}
				score := scoreCandidate(tokens, c.Name, c.Label, c.Description)
				if score > 0 {
					field, snippet := bestMatchField(tokens, c.Name, c.Label, c.Description)
					results = append(results, scoredResult{
						Score:     score,
						FieldRank: matchFieldRank(field),
						Item: map[string]any{
							"type":        "column",
							"model_id":    m.ModelID,
							"column_name": c.Name,
							"label":       c.Label,
							"score":       score,
							"match_field": field,
							"snippet":     snippet,
						},
					})
				}
			}
		}

		for _, c := range model.Concepts {
			score := scoreCandidate(tokens, c.ConceptID, c.Label, c.Description)
			if score > 0 {
				field, snippet := bestMatchField(tokens, c.ConceptID, c.Label, c.Description)
				results = append(results, scoredResult{
					Score:     score,
					FieldRank: matchFieldRank(field),
					Item: map[string]any{
						"type":        "concept",
						"concept_id":  c.ConceptID,
						"label":       c.Label,
						"score":       score,
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
			if results[i].FieldRank != results[j].FieldRank {
				return results[i].FieldRank < results[j].FieldRank
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

func tokeniseQuery(query string) []string {
	raw := strings.FieldsFunc(
		strings.ToLower(query),
		func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsDigit(r)
		},
	)

	seen := make(map[string]bool)
	tokens := make([]string, 0, len(raw))
	for _, t := range raw {
		if len(t) < 2 {
			continue
		}
		if seen[t] {
			continue
		}
		seen[t] = true
		tokens = append(tokens, t)
	}

	return tokens
}

func scoreCandidate(tokens []string, fields ...string) int {
	score := 0
	combined := strings.ToLower(strings.Join(fields, " "))
	for _, token := range tokens {
		if strings.Contains(combined, token) {
			score++
		}
	}
	return score
}

func bestMatchField(tokens []string, name, label, description string) (field, snippet string) {
	nameScore := scoreCandidate(tokens, name)
	labelScore := scoreCandidate(tokens, label)
	descScore := scoreCandidate(tokens, description)

	if nameScore >= labelScore && nameScore >= descScore && nameScore > 0 {
		return "name", name
	}
	if labelScore >= nameScore && labelScore >= descScore && labelScore > 0 {
		return "label", label
	}
	if descScore > 0 {
		return "description", description
	}
	return "description", description
}

func matchFieldRank(field string) int {
	switch field {
	case "name":
		return 0
	case "label":
		return 1
	case "description":
		return 2
	default:
		return 3
	}
}
