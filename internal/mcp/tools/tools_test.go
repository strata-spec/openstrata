package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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

func TestSearchSemanticMultiWord(t *testing.T) {
	model := fixtureModel()
	model.Models[0].Label = "Box Office Revenue"
	model.Models[0].Description = "Model for movies revenue and budget analytics"

	_, handler := SearchSemantic(func() *smif.SemanticModel { return model })

	res, err := handler(context.Background(), callRequest("search_semantic", map[string]any{"query": "revenue budget"}))
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}

	var out []map[string]any
	if err := json.Unmarshal([]byte(extractText(t, res)), &out); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected results for multi-word query")
	}
	if out[0]["type"] != "model" {
		t.Fatalf("expected model result first, got %v", out[0]["type"])
	}
	if out[0]["score"] != float64(2) {
		t.Fatalf("expected score 2 for two matched tokens, got %v", out[0]["score"])
	}
}

func TestSearchSemanticNoMatch(t *testing.T) {
	model := fixtureModel()
	_, handler := SearchSemantic(func() *smif.SemanticModel { return model })

	res, err := handler(context.Background(), callRequest("search_semantic", map[string]any{"query": "xyzzy impossible query"}))
	if err != nil {
		t.Fatalf("expected empty results, got error: %v", err)
	}

	var out []map[string]any
	if err := json.Unmarshal([]byte(extractText(t, res)), &out); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expected no results, got %d", len(out))
	}
}

func TestSearchSemanticSingleWordStillWorks(t *testing.T) {
	model := fixtureModel()
	model.Models[0].Label = "Revenue Facts"

	_, handler := SearchSemantic(func() *smif.SemanticModel { return model })

	res, err := handler(context.Background(), callRequest("search_semantic", map[string]any{"query": "revenue"}))
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}

	var out []map[string]any
	if err := json.Unmarshal([]byte(extractText(t, res)), &out); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected single-word results")
	}
}

func TestSearchSemanticLiveQueries(t *testing.T) {
	model := &smif.SemanticModel{
		SMIFVersion: "0.1.0",
		Domain:      smif.Domain{Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9}},
		Models: []smif.Model{
			{
				ModelID:        "movies",
				Name:           "movies",
				Label:          "Movies Revenue",
				Description:    "Tracks movies, revenue, and production budget.",
				Grain:          "one row per movie",
				PhysicalSource: smif.PhysicalSource{Schema: "public", Table: "movies"},
				DDLFingerprint: "movies_fingerprint",
				Columns: []smif.Column{
					{Name: "revenue", Label: "Revenue", Description: "Box office revenue in USD", Role: "measure", DataType: "numeric", Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9}},
					{Name: "budget", Label: "Budget", Description: "Production budget in USD", Role: "measure", DataType: "numeric", Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9}},
				},
				Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9},
			},
			{
				ModelID:        "casts",
				Name:           "casts",
				Label:          "Cast Assignments",
				Description:    "Movie cast and crew assignments by role.",
				Grain:          "one row per person per movie role",
				PhysicalSource: smif.PhysicalSource{Schema: "public", Table: "casts"},
				DDLFingerprint: "casts_fingerprint",
				Columns: []smif.Column{
					{Name: "crew_role", Label: "Crew Role", Description: "Crew assignment role", Role: "dimension", DataType: "text", Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.8}},
				},
				Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9},
			},
		},
	}

	_, handler := SearchSemantic(func() *smif.SemanticModel { return model })

	queries := []string{"movies revenue budget", "cast crew assignments", "revenue"}
	for _, q := range queries {
		res, err := handler(context.Background(), callRequest("search_semantic", map[string]any{"query": q}))
		if err != nil {
			t.Fatalf("query %q returned error: %v", q, err)
		}

		var out []map[string]any
		if err := json.Unmarshal([]byte(extractText(t, res)), &out); err != nil {
			t.Fatalf("query %q unmarshal response: %v", q, err)
		}
		if len(out) == 0 {
			t.Fatalf("query %q expected non-empty results", q)
		}

		switch q {
		case "cast crew assignments":
			foundCasts := false
			for _, item := range out {
				if item["type"] == "model" && item["model_id"] == "casts" {
					foundCasts = true
					break
				}
			}
			if !foundCasts {
				t.Fatalf("query %q expected casts model in results", q)
			}
		}

		t.Logf("query=%q results=%v", q, out)
	}
}

func TestTokeniseQuery(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{name: "basic words", query: "movies revenue budget", want: []string{"movies", "revenue", "budget"}},
		{name: "mixed case", query: "Box Office Revenue", want: []string{"box", "office", "revenue"}},
		{name: "short words", query: "a the of", want: []string{"the", "of"}},
		{name: "dedup", query: "revenue revenue", want: []string{"revenue"}},
		{name: "empty", query: "", want: []string{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tokeniseQuery(tc.query)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("tokeniseQuery(%q) = %v, want %v", tc.query, got, tc.want)
			}
		})
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

func TestRecordCorrectionValidationErrorsReturnStructuredToolText(t *testing.T) {
	server := &stubReloadableServer{}
	_, handler := RecordCorrection(server, func() *smif.SemanticModel { return fixtureModel() })

	res, err := handler(context.Background(), callRequest("record_correction", map[string]any{
		"target_type": "",
		"target_id":   "orders.status",
		"new_value":   "x",
	}))
	if err != nil {
		t.Fatalf("expected no protocol error, got %v", err)
	}
	text := extractText(t, res)
	if !strings.Contains(text, `"error": "missing required field: target_type"`) {
		t.Fatalf("unexpected response: %s", text)
	}
}

func TestRecordCorrectionTargetNotFoundReturnsStructuredToolText(t *testing.T) {
	server := &stubReloadableServer{}
	_, handler := RecordCorrection(server, func() *smif.SemanticModel { return fixtureModel() })

	res, err := handler(context.Background(), callRequest("record_correction", map[string]any{
		"target_type":     "column",
		"target_id":       "orders.missing",
		"correction_type": "description_override",
		"new_value":       "x",
	}))
	if err != nil {
		t.Fatalf("expected no protocol error, got %v", err)
	}
	text := extractText(t, res)
	if !strings.Contains(text, `"error": "target not found: orders.missing"`) {
		t.Fatalf("unexpected response: %s", text)
	}
}

func TestRecordCorrectionAppendFailureReturnsStructuredToolText(t *testing.T) {
	server := &stubReloadableServer{correctionsPath: filepath.Join(t.TempDir(), "missing", "corrections.yaml")}
	_, handler := RecordCorrection(server, func() *smif.SemanticModel { return fixtureModel() })

	res, err := handler(context.Background(), callRequest("record_correction", map[string]any{
		"target_type":     "column",
		"target_id":       "orders.status",
		"correction_type": "description_override",
		"new_value":       "x",
	}))
	if err != nil {
		t.Fatalf("expected no protocol error, got %v", err)
	}
	text := extractText(t, res)
	if !strings.Contains(text, `"error": "failed to write correction:`) {
		t.Fatalf("unexpected response: %s", text)
	}
}

func TestRecordCorrectionReloadFailureReturnsStructuredToolText(t *testing.T) {
	dir := t.TempDir()
	correctionsPath := filepath.Join(dir, "corrections.yaml")
	if err := os.WriteFile(correctionsPath, []byte("smif_version: \"0.1.0\"\ncorrections:\n"), 0o644); err != nil {
		t.Fatalf("write corrections file: %v", err)
	}

	server := &stubReloadableServer{
		correctionsPath: correctionsPath,
		reloadErr:       errors.New("boom"),
	}
	_, handler := RecordCorrection(server, func() *smif.SemanticModel { return fixtureModel() })

	res, err := handler(context.Background(), callRequest("record_correction", map[string]any{
		"target_type":     "column",
		"target_id":       "orders.status",
		"correction_type": "description_override",
		"new_value":       "x",
	}))
	if err != nil {
		t.Fatalf("expected no protocol error, got %v", err)
	}
	text := extractText(t, res)
	if !strings.Contains(text, `"error": "correction written but model reload failed: boom"`) {
		t.Fatalf("unexpected response: %s", text)
	}
}

func TestTargetResolvesCoverage(t *testing.T) {
	model := fixtureModel()
	model.Metrics = []smif.Metric{{Name: "gross_revenue"}}

	tests := []struct {
		name       string
		targetType string
		targetID   string
		want       bool
	}{
		{name: "domain always resolves", targetType: "domain", targetID: "domain", want: true},
		{name: "model resolves", targetType: "model", targetID: "orders", want: true},
		{name: "column resolves", targetType: "column", targetID: "orders.status", want: true},
		{name: "column malformed", targetType: "column", targetID: "orders", want: false},
		{name: "relationship resolves", targetType: "relationship", targetID: "rel_orders_self", want: true},
		{name: "metric resolves", targetType: "metric", targetID: "gross_revenue", want: true},
		{name: "unknown type", targetType: "unknown", targetID: "x", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := targetResolves(model, tc.targetType, tc.targetID); got != tc.want {
				t.Fatalf("targetResolves(%q, %q) = %v, want %v", tc.targetType, tc.targetID, got, tc.want)
			}
		})
	}
}

func TestRecordCorrectionSuccessShape(t *testing.T) {
	dir := t.TempDir()
	correctionsPath := filepath.Join(dir, "corrections.yaml")
	if err := os.WriteFile(correctionsPath, []byte("smif_version: \"0.1.0\"\ncorrections:\n"), 0o644); err != nil {
		t.Fatalf("write corrections file: %v", err)
	}

	server := &stubReloadableServer{correctionsPath: correctionsPath}
	_, handler := RecordCorrection(server, func() *smif.SemanticModel { return fixtureModel() })

	res, err := handler(context.Background(), callRequest("record_correction", map[string]any{
		"target_type":     "column",
		"target_id":       "orders.status",
		"correction_type": "description_override",
		"new_value":       "Order status text",
	}))
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal([]byte(extractText(t, res)), &out); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if out["status"] != "applied" {
		t.Fatalf("expected status applied, got %v", out["status"])
	}
	if id, ok := out["correction_id"].(string); !ok || !strings.HasPrefix(id, "corr_") {
		t.Fatalf("unexpected correction_id: %v", out["correction_id"])
	}
}

func TestRecordCorrectionLivePayloadColumn(t *testing.T) {
	dir := t.TempDir()
	correctionsPath := filepath.Join(dir, "corrections.yaml")
	if err := os.WriteFile(correctionsPath, []byte("smif_version: \"0.1.0\"\ncorrections:\n"), 0o644); err != nil {
		t.Fatalf("write corrections file: %v", err)
	}

	model := &smif.SemanticModel{
		SMIFVersion: "0.1.0",
		Domain:      smif.Domain{Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9}},
		Models: []smif.Model{{
			ModelID:        "movies",
			Name:           "movies",
			Label:          "Movies",
			Description:    "Movie facts",
			Grain:          "one row per movie",
			PhysicalSource: smif.PhysicalSource{Schema: "public", Table: "movies"},
			DDLFingerprint: "movies_fingerprint",
			Columns: []smif.Column{
				{Name: "revenue", Label: "Revenue", Description: "Revenue", Role: "measure", DataType: "numeric", Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9}},
			},
			Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9},
		}},
	}

	server := &stubReloadableServer{correctionsPath: correctionsPath}
	_, handler := RecordCorrection(server, func() *smif.SemanticModel { return model })

	res, err := handler(context.Background(), callRequest("record_correction", map[string]any{
		"target_type":     "column",
		"target_id":       "movies.revenue",
		"correction_type": "description_override",
		"new_value":       "Box office revenue in USD. Zero indicates unreported or unknown revenue, not a true zero. Do not aggregate with SUM without filtering zero values first.",
		"reason":          "Zero revenue is ambiguous",
	}))
	if err != nil {
		t.Fatalf("expected no protocol error, got %v", err)
	}

	text := extractText(t, res)
	if strings.Contains(text, "Tool execution failed") {
		t.Fatalf("unexpected tool execution failure text: %s", text)
	}

	var out map[string]any
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if out["status"] != "applied" {
		t.Fatalf("expected status applied, got %v", out["status"])
	}
	if _, ok := out["correction_id"].(string); !ok {
		t.Fatalf("missing correction_id: %v", out)
	}

	t.Logf("record_correction response=%v", out)
}

type stubReloadableServer struct {
	correctionsPath string
	smifVersion     string
	reloadErr       error
}

func (s *stubReloadableServer) Reload() error {
	if s.reloadErr != nil {
		return s.reloadErr
	}
	return nil
}

func (s *stubReloadableServer) CorrectionsPath() string {
	if strings.TrimSpace(s.correctionsPath) == "" {
		return filepath.Join(os.TempDir(), fmt.Sprintf("test-corrections-%d.yaml", time.Now().UnixNano()))
	}
	return s.correctionsPath
}

func (s *stubReloadableServer) SMIFVersion() string {
	if strings.TrimSpace(s.smifVersion) == "" {
		return "0.1.0"
	}
	return s.smifVersion
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
