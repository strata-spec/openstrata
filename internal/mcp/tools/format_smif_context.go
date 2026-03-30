package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	mcp "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/strata-spec/openstrata/internal/smif"
)

// FormatSMIFContext returns the format_smif_context MCP tool definition and handler.
//
// The tool performs a two-pass relevance filter to select the models most
// relevant to a natural-language question:
//
//   - Pass 1 (keyword match): tokenises the question and scores every
//     non-suppressed model against model_id, name, label, and description.
//     Any model with a score > 0 is included in the seed set.
//
//   - Pass 2 (one-hop expansion): for every model in the seed set, all
//     non-suppressed relationships are traversed. The model at the other
//     end of each relationship is added to the result set regardless of
//     whether it appeared in the question text. This ensures join-target
//     tables (e.g. `categories`, `jobs`) are never dropped from context
//     even when the user's question only mentions the driving table.
//
// The combined set is serialised to JSON and returned as the tool result.
func FormatSMIFContext(getModel func() *smif.SemanticModel) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool(
		"format_smif_context",
		mcp.WithDescription("Return the semantic model context most relevant to a question, including all join-target models"),
		mcp.WithString("question", mcp.Description("Natural-language question to answer"), mcp.Required()),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		_ = ctx

		question, _ := request.Params.Arguments["question"].(string)
		question = strings.TrimSpace(question)
		if question == "" {
			return nil, fmt.Errorf("question is required")
		}

		doc := getModel()
		if doc == nil {
			return mcp.NewToolResultText("{}"), nil
		}

		// -----------------------------------------------------------------
		// Pass 1: keyword match — select directly mentioned models.
		// -----------------------------------------------------------------
		tokens := tokeniseQuery(question)

		seedSet := make(map[string]struct{})
		for _, m := range doc.Models {
			if m.Suppressed {
				continue
			}
			if len(tokens) == 0 || scoreCandidate(tokens, m.ModelID, m.Name, m.Label, m.Description) > 0 {
				seedSet[m.ModelID] = struct{}{}
			}
		}

		// -----------------------------------------------------------------
		// Pass 2: one-hop expansion — include join targets of seed models.
		// -----------------------------------------------------------------
		resultSet := make(map[string]struct{}, len(seedSet))
		for id := range seedSet {
			resultSet[id] = struct{}{}
		}
		for _, r := range doc.Relationships {
			if r.Suppressed {
				continue
			}
			if _, ok := seedSet[r.FromModel]; ok {
				resultSet[r.ToModel] = struct{}{}
			}
			if _, ok := seedSet[r.ToModel]; ok {
				resultSet[r.FromModel] = struct{}{}
			}
		}

		log.Printf("format_smif_context: question=%q seed=%v expanded=%v",
			question, sortedKeys(seedSet), sortedKeys(resultSet))

		// -----------------------------------------------------------------
		// Build output: models, their columns, and relevant relationships.
		// -----------------------------------------------------------------
		type columnOut struct {
			Name        string `json:"name"`
			DataType    string `json:"data_type"`
			Role        string `json:"role"`
			Label       string `json:"label"`
			Description string `json:"description"`
		}
		type modelOut struct {
			ModelID     string      `json:"model_id"`
			Name        string      `json:"name"`
			Label       string      `json:"label"`
			Description string      `json:"description"`
			Grain       string      `json:"grain,omitempty"`
			Columns     []columnOut `json:"columns"`
		}
		type relOut struct {
			RelationshipID   string `json:"relationship_id"`
			FromModel        string `json:"from_model"`
			FromColumn       string `json:"from_column"`
			ToModel          string `json:"to_model"`
			ToColumn         string `json:"to_column"`
			RelationshipType string `json:"relationship_type"`
			JoinCondition    string `json:"join_condition"`
		}
		type output struct {
			Models        []modelOut `json:"models"`
			Relationships []relOut   `json:"relationships"`
		}

		models := make([]modelOut, 0, len(resultSet))
		for _, m := range doc.Models {
			if m.Suppressed {
				continue
			}
			if _, ok := resultSet[m.ModelID]; !ok {
				continue
			}
			cols := make([]columnOut, 0, len(m.Columns))
			for _, c := range m.Columns {
				if c.Suppressed {
					continue
				}
				cols = append(cols, columnOut{
					Name:        c.Name,
					DataType:    c.DataType,
					Role:        c.Role,
					Label:       c.Label,
					Description: c.Description,
				})
			}
			models = append(models, modelOut{
				ModelID:     m.ModelID,
				Name:        m.Name,
				Label:       m.Label,
				Description: m.Description,
				Grain:       m.Grain,
				Columns:     cols,
			})
		}

		rels := make([]relOut, 0)
		for _, r := range doc.Relationships {
			if r.Suppressed {
				continue
			}
			_, fromOK := resultSet[r.FromModel]
			_, toOK := resultSet[r.ToModel]
			if !fromOK || !toOK {
				continue
			}
			rels = append(rels, relOut{
				RelationshipID:   r.RelationshipID,
				FromModel:        r.FromModel,
				FromColumn:       r.FromColumn,
				ToModel:          r.ToModel,
				ToColumn:         r.ToColumn,
				RelationshipType: r.RelationshipType,
				JoinCondition:    r.JoinCondition,
			})
		}

		out := output{Models: models, Relationships: rels}
		b, err := json.Marshal(out)
		if err != nil {
			return nil, err
		}
		return mcp.NewToolResultText(string(b)), nil
	}

	return tool, handler
}

// sortedKeys returns the keys of a string set in sorted order, for logging.
func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// simple insertion sort — sets are small
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
