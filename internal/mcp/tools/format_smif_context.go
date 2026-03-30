package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"

	mcp "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/strata-spec/openstrata/internal/smif"
)

// stopwords is the set of SQL and question tokens that carry no semantic
// meaning when scoring models. filterTokens strips these before scoring.
var stopwords = map[string]bool{
	"show": true, "find": true, "get": true, "list": true, "count": true,
	"how": true, "many": true, "what": true, "which": true, "all": true,
	"the": true, "of": true, "for": true, "in": true, "with": true,
	"where": true, "is": true, "are": true, "have": true,
}

// filterTokens tokenises question via tokeniseQuery (which already strips
// single-character tokens and lowercases), then removes SQL/question
// stopwords. The returned slice is reused across Pass 1 and Pass 2.
func filterTokens(question string) []string {
	raw := tokeniseQuery(question)
	tokens := make([]string, 0, len(raw))
	for _, t := range raw {
		if !stopwords[t] {
			tokens = append(tokens, t)
		}
	}
	return tokens
}

// scoreModel returns the number of token matches across the model's
// model_id, name, label, and description fields.
func scoreModel(m smif.Model, tokens []string) int {
	return scoreCandidate(tokens, m.ModelID, m.Name, m.Label, m.Description)
}

// computeHubThreshold computes a dynamic hub threshold from the degree
// distribution of non-suppressed models:
//
//	degrees       = count of non-suppressed relationships in either direction
//	mean_degree   = mean(degrees.values())
//	stddev_degree = stddev(degrees.values())
//	hub_threshold = max(2, round(mean_degree + stddev_degree))
//
// The max(2, …) floor ensures that on a 3-table schema where all degrees
// are 1, no hubs exist and every model expands unconditionally.
func computeHubThreshold(models []smif.Model, rels []smif.Relationship) int {
	degrees := make(map[string]int)
	for _, m := range models {
		if !m.Suppressed {
			degrees[m.ModelID] = 0
		}
	}
	for _, r := range rels {
		if r.Suppressed {
			continue
		}
		if _, ok := degrees[r.FromModel]; ok {
			degrees[r.FromModel]++
		}
		if _, ok := degrees[r.ToModel]; ok {
			degrees[r.ToModel]++
		}
	}

	n := len(degrees)
	if n == 0 {
		return 2
	}

	total := 0
	for _, d := range degrees {
		total += d
	}
	mean := float64(total) / float64(n)

	variance := 0.0
	for _, d := range degrees {
		diff := float64(d) - mean
		variance += diff * diff
	}
	variance /= float64(n)
	stddev := math.Sqrt(variance)

	threshold := int(math.Round(mean + stddev))
	if threshold < 2 {
		return 2
	}
	return threshold
}

// FormatSMIFContext returns the format_smif_context MCP tool definition and handler.
//
// The tool performs a two-pass relevance filter to select the models most
// relevant to a natural-language question:
//
//   - Pass 1 (keyword match): tokenises the question, strips stopwords, and
//     scores every non-suppressed model against model_id, name, label, and
//     description. Any model with a score > 0 is included in the seed set.
//
//   - Pass 2 (hub-aware one-hop expansion): for every model in the seed set,
//     non-suppressed relationships are traversed. If the seed model's degree
//     is ≤ the dynamic hub threshold, all non-suppressed neighbours are added
//     unconditionally. If the seed model is a hub (degree > threshold), only
//     neighbours with a relevance score > 0 against the filtered tokens are
//     added. This prevents hub models from flooding the context with dozens of
//     unrelated tables.
//
// A relationship is emitted only when both endpoints are in the final context
// set. The combined set is serialised to JSON and returned as the tool result.
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
		// Uses filtered tokens (stopwords removed) for tighter seeding.
		// -----------------------------------------------------------------
		tokens := filterTokens(question)

		seedSet := make(map[string]struct{})
		for _, m := range doc.Models {
			if m.Suppressed {
				continue
			}
			if len(tokens) == 0 || scoreModel(m, tokens) > 0 {
				seedSet[m.ModelID] = struct{}{}
			}
		}

		// -----------------------------------------------------------------
		// Pass 2: hub-aware one-hop expansion.
		// Models with degree > hubThreshold are hubs; only neighbours that
		// score > 0 against the filtered tokens are added for hubs.
		// Non-hub seed models expand all non-suppressed neighbours
		// unconditionally (preserving q011/q012 behaviour).
		// -----------------------------------------------------------------
		hubThreshold := computeHubThreshold(doc.Models, doc.Relationships)

		// Build degree map (non-suppressed only) for hub detection.
		degrees := make(map[string]int)
		for _, m := range doc.Models {
			if !m.Suppressed {
				degrees[m.ModelID] = 0
			}
		}
		for _, r := range doc.Relationships {
			if r.Suppressed {
				continue
			}
			if _, ok := degrees[r.FromModel]; ok {
				degrees[r.FromModel]++
			}
			if _, ok := degrees[r.ToModel]; ok {
				degrees[r.ToModel]++
			}
		}

		// Build model lookup for O(1) neighbour scoring.
		modelByID := make(map[string]smif.Model, len(doc.Models))
		for _, m := range doc.Models {
			modelByID[m.ModelID] = m
		}

		resultSet := make(map[string]struct{}, len(seedSet))
		for id := range seedSet {
			resultSet[id] = struct{}{}
		}
		for id := range seedSet {
			isHub := degrees[id] > hubThreshold
			for _, r := range doc.Relationships {
				if r.Suppressed {
					continue
				}
				var neighbourID string
				if r.FromModel == id {
					neighbourID = r.ToModel
				} else if r.ToModel == id {
					neighbourID = r.FromModel
				} else {
					continue
				}
				neighbour, ok := modelByID[neighbourID]
				if !ok || neighbour.Suppressed {
					continue
				}
				if isHub && scoreModel(neighbour, tokens) == 0 {
					// Hub: skip neighbours not mentioned in the question.
					continue
				}
				resultSet[neighbourID] = struct{}{}
			}
		}

		log.Printf("format_smif_context: question=%q seed=%v expanded=%v hub_threshold=%d",
			question, sortedKeys(seedSet), sortedKeys(resultSet), hubThreshold)

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
