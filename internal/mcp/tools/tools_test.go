package tools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	mcp "github.com/mark3labs/mcp-go/mcp"
	"github.com/strata-spec/openstrata/internal/smif"
)

func TestListModelsExcludesSuppressed(t *testing.T) {
	model := fixtureModel()
	tool, handler := ListModels(func() *smif.SemanticModel { return model })
	if tool.Name != "list_models" {
		t.Fatalf("unexpected tool name %q", tool.Name)
	}

	res, err := handler(context.Background(), callRequest("list_models", nil))
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}

	var out []map[string]any
	if err := json.Unmarshal([]byte(extractText(t, res)), &out); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 model, got %d", len(out))
	}
	if out[0]["model_id"] != "orders" {
		t.Fatalf("expected orders model only, got %v", out[0]["model_id"])
	}
}

func TestGetModelNotFound(t *testing.T) {
	model := fixtureModel()
	_, handler := GetModel(func() *smif.SemanticModel { return model })

	_, err := handler(context.Background(), callRequest("get_model", map[string]any{"model_id": "missing"}))
	if err == nil {
		t.Fatalf("expected error for missing model")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not found error, got %v", err)
	}
}

func TestGetModelExcludesSuppressedColumns(t *testing.T) {
	model := fixtureModel()
	_, handler := GetModel(func() *smif.SemanticModel { return model })

	res, err := handler(context.Background(), callRequest("get_model", map[string]any{"model_id": "orders"}))
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}

	var out struct {
		Model smif.Model `json:"model"`
	}
	if err := json.Unmarshal([]byte(extractText(t, res)), &out); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	for _, c := range out.Model.Columns {
		if c.Name == "internal_note" {
			t.Fatalf("suppressed column should not be returned")
		}
	}
}

func TestSearchSemanticBasic(t *testing.T) {
	model := fixtureModel()
	_, handler := SearchSemantic(func() *smif.SemanticModel { return model })

	res, err := handler(context.Background(), callRequest("search_semantic", map[string]any{"query": "Orders"}))
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}

	var out []map[string]any
	if err := json.Unmarshal([]byte(extractText(t, res)), &out); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected at least one search result")
	}
	if out[0]["type"] != "model" || out[0]["model_id"] != "orders" {
		t.Fatalf("expected orders model first, got %v", out[0])
	}
}

func TestSearchSemanticCaseInsensitive(t *testing.T) {
	model := fixtureModel()
	_, handler := SearchSemantic(func() *smif.SemanticModel { return model })

	res, err := handler(context.Background(), callRequest("search_semantic", map[string]any{"query": "ORDER"}))
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}

	var out []map[string]any
	if err := json.Unmarshal([]byte(extractText(t, res)), &out); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected case-insensitive match")
	}
}

func TestRunSemanticSQLNoPool(t *testing.T) {
	model := fixtureModel()
	_, handler := RunSemanticSQL(func() *smif.SemanticModel { return model }, nil)

	res, err := handler(context.Background(), callRequest("run_semantic_sql", map[string]any{"sql": "SELECT * FROM orders"}))
	if err != nil {
		t.Fatalf("expected no protocol error, got %v", err)
	}
	text := extractText(t, res)
	if !strings.Contains(text, "requires a live database connection") {
		t.Fatalf("expected missing db message, got %s", text)
	}
}

func TestRunSemanticSQLRejectsDML(t *testing.T) {
	model := fixtureModel()
	_, handler := RunSemanticSQL(func() *smif.SemanticModel { return model }, &pgxpool.Pool{})

	res, err := handler(context.Background(), callRequest("run_semantic_sql", map[string]any{"sql": "DELETE FROM orders"}))
	if err != nil {
		t.Fatalf("expected tool-level rejection, got protocol error %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected tool-level error response")
	}
	if !strings.Contains(extractText(t, res), "read-only") {
		t.Fatalf("expected read-only message")
	}
}

func TestRecordCorrectionBuildsCorrectStruct(t *testing.T) {
	c := buildCorrection("column", "orders.status", "label_override", "Order Status", "clearer label")
	if c.CorrectionID == "" {
		t.Fatalf("expected correction_id to be generated")
	}
	if c.Source != "user_defined" {
		t.Fatalf("expected source user_defined, got %q", c.Source)
	}
	if c.Status != "approved" {
		t.Fatalf("expected status approved, got %q", c.Status)
	}
	ts, err := time.Parse(time.RFC3339, c.Timestamp)
	if err != nil {
		t.Fatalf("invalid timestamp: %v", err)
	}
	if time.Since(ts) > 5*time.Second {
		t.Fatalf("timestamp is not recent: %s", c.Timestamp)
	}
}

func callRequest(name string, args map[string]any) mcp.CallToolRequest {
	var req mcp.CallToolRequest
	req.Params.Name = name
	req.Params.Arguments = args
	return req
}

func extractText(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	if res == nil || len(res.Content) == 0 {
		t.Fatalf("missing tool content")
	}
	text, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content type %T", res.Content[0])
	}
	return text.Text
}

func fixtureModel() *smif.SemanticModel {
	return &smif.SemanticModel{
		SMIFVersion: "0.1.0",
		Domain:      smif.Domain{Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9, HumanReviewed: true}},
		Models: []smif.Model{
			{
				ModelID:        "orders",
				Name:           "orders",
				Label:          "Orders",
				Description:    "Order header records.",
				Grain:          "one row per order",
				PhysicalSource: smif.PhysicalSource{Schema: "public", Table: "orders"},
				Columns: []smif.Column{
					{Name: "status", Label: "Order Status", Description: "Current state of the order", Role: "dimension", DataType: "text", Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.8}},
					{Name: "internal_note", Label: "Internal Note", Description: "Internal-only notes", Role: "dimension", DataType: "text", Suppressed: true, Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.6}},
				},
				Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9},
			},
			{
				ModelID:        "hidden_model",
				Name:           "hidden_model",
				Label:          "Hidden",
				Description:    "Suppressed model.",
				Suppressed:     true,
				PhysicalSource: smif.PhysicalSource{Schema: "public", Table: "hidden_model"},
				Columns:        []smif.Column{{Name: "id", DataType: "int", Role: "identifier", Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9}}},
				Provenance:     smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9},
			},
		},
		Relationships: []smif.Relationship{{
			RelationshipID:   "rel_orders_self",
			FromModel:        "orders",
			FromColumn:       "status",
			ToModel:          "orders",
			ToColumn:         "status",
			RelationshipType: "one_to_one",
			JoinCondition:    "orders.status = orders.status",
			Provenance:       smif.Provenance{SourceType: "llm_inferred", Confidence: 0.8},
		}},
	}
}
