package inference

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	coarsepkg "github.com/strata-spec/openstrata/internal/inference/coarse"
	finepkg "github.com/strata-spec/openstrata/internal/inference/fine"
	joinspkg "github.com/strata-spec/openstrata/internal/inference/joins"
	llmpkg "github.com/strata-spec/openstrata/internal/inference/llm"
	"github.com/strata-spec/openstrata/internal/postgres"
	"github.com/strata-spec/openstrata/internal/runlog"
	"github.com/strata-spec/openstrata/internal/smif"
	appversion "github.com/strata-spec/openstrata/internal/version"
)

func TestAssembleModelEnvelope(t *testing.T) {
	t.Parallel()

	cfg := Config{Schema: "public"}
	domain := &coarsepkg.DomainResult{Name: "commerce", Description: "Commerce domain"}
	tables := []postgres.TableInfo{
		{
			Name:       "orders",
			PrimaryKey: []string{"id"},
			Columns: []postgres.ColumnInfo{
				{Name: "id", DataType: "bigint", IsNullable: false},
			},
		},
	}
	profiles := map[string]postgres.ColumnProfile{
		"orders.id": {TableName: "orders", ColumnName: "id", CardinalityCategory: "high"},
	}
	tableResults := []coarsepkg.TableResult{{TableName: "orders", Description: "Orders table", Grain: "one row per order"}}
	grainConfirmations := []joinspkg.GrainConfirmation{{TableName: "orders", GrainStatement: "one row per order", Confirmed: true}}

	model, err := assembleModel(
		cfg,
		domain,
		tables,
		profiles,
		tableResults,
		nil,
		nil,
		grainConfirmations,
		nil,
		false,
		"deadbeef",
	)
	if err != nil {
		t.Fatalf("assembleModel returned error: %v", err)
	}

	if model.SMIFVersion != "0.1.0" {
		t.Fatalf("expected smif_version 0.1.0, got %s", model.SMIFVersion)
	}
	if len(model.Source.SchemaNames) != 1 || model.Source.SchemaNames[0] != "public" {
		t.Fatalf("expected source.schema_names [public], got %+v", model.Source.SchemaNames)
	}
	if len(model.Models) != 1 {
		t.Fatalf("expected one model, got %d", len(model.Models))
	}
	if strings.TrimSpace(model.Models[0].DDLFingerprint) == "" {
		t.Fatalf("expected non-empty ddl_fingerprint")
	}
}

func TestAssembleModelColumnProvenance(t *testing.T) {
	t.Parallel()

	cfg := Config{Schema: "public"}
	tables := []postgres.TableInfo{
		{
			Name: "orders",
			Columns: []postgres.ColumnInfo{
				{Name: "status", DataType: "text", Comment: "order state"},
			},
		},
	}
	fineResults := []finepkg.FinePassResult{
		{
			TableName: "orders",
			Columns: []finepkg.ColumnResult{
				{TableName: "orders", ColumnName: "status", Role: "dimension", Label: "Status", Description: "State", Difficulty: "self_evident"},
			},
		},
	}

	model, err := assembleModel(
		cfg,
		nil,
		tables,
		nil,
		nil,
		fineResults,
		nil,
		nil,
		nil,
		false,
		"deadbeef",
	)
	if err != nil {
		t.Fatalf("assembleModel returned error: %v", err)
	}

	if len(model.Models) != 1 || len(model.Models[0].Columns) != 1 {
		t.Fatalf("expected one model/column, got %+v", model.Models)
	}
	sourceType := model.Models[0].Columns[0].Provenance.SourceType
	if sourceType != "ddl_comment" {
		t.Fatalf("expected ddl_comment to win, got %s", sourceType)
	}
}

func TestAssembleModelNoFinePass(t *testing.T) {
	t.Parallel()

	cfg := Config{Schema: "public"}
	tables := []postgres.TableInfo{
		{
			Name: "orders",
			Columns: []postgres.ColumnInfo{
				{Name: "status", DataType: "text"},
			},
		},
	}

	model, err := assembleModel(
		cfg,
		nil,
		tables,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		false,
		"deadbeef",
	)
	if err != nil {
		t.Fatalf("assembleModel returned error: %v", err)
	}

	col := model.Models[0].Columns[0]
	if col.Role != "dimension" {
		t.Fatalf("expected default role dimension, got %s", col.Role)
	}
	if col.Provenance.SourceType != "schema_constraint" {
		t.Fatalf("expected schema_constraint source type, got %s", col.Provenance.SourceType)
	}
}

func TestAssembleModelValidValues(t *testing.T) {
	t.Parallel()

	cfg := Config{Schema: "public"}
	tables := []postgres.TableInfo{
		{
			Name: "movies",
			Columns: []postgres.ColumnInfo{
				{Name: "kind", DataType: "text"},
				{Name: "title", DataType: "text"},
				{Name: "year", DataType: "integer"},
			},
		},
	}
	// kind has low distinct count and valid values enumerated; title and year do not.
	profiles := map[string]postgres.ColumnProfile{
		"movies.kind": {
			TableName:           "movies",
			ColumnName:          "kind",
			DistinctCount:       3,
			CardinalityCategory: "low",
			ExampleValues:       []string{"movie", "short", "tvSeries"},
			ValidValues:         []string{"movie", "short", "tvSeries"},
		},
		"movies.title": {
			TableName:           "movies",
			ColumnName:          "title",
			DistinctCount:       50000,
			CardinalityCategory: "high",
			ExampleValues:       []string{"Inception"},
		},
		"movies.year": {
			TableName:           "movies",
			ColumnName:          "year",
			DistinctCount:       80,
			CardinalityCategory: "low",
			ExampleValues:       []string{"2000"},
		},
	}

	model, err := assembleModel(cfg, nil, tables, profiles, nil, nil, nil, nil, nil, false, "deadbeef")
	if err != nil {
		t.Fatalf("assembleModel returned error: %v", err)
	}

	if len(model.Models) != 1 {
		t.Fatalf("expected one model, got %d", len(model.Models))
	}

	colByName := make(map[string]smif.Column, len(model.Models[0].Columns))
	for _, c := range model.Models[0].Columns {
		colByName[c.Name] = c
	}

	kindCol, ok := colByName["kind"]
	if !ok {
		t.Fatalf("expected kind column in model")
	}
	if len(kindCol.ValidValues) != 3 {
		t.Fatalf("expected 3 valid_values for kind, got %v", kindCol.ValidValues)
	}
	for _, want := range []string{"movie", "short", "tvSeries"} {
		found := false
		for _, v := range kindCol.ValidValues {
			if v == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected %q in kind valid_values, got %v", want, kindCol.ValidValues)
		}
	}

	titleCol := colByName["title"]
	if len(titleCol.ValidValues) != 0 {
		t.Fatalf("expected no valid_values for high-cardinality title column, got %v", titleCol.ValidValues)
	}

	yearCol := colByName["year"]
	if len(yearCol.ValidValues) != 0 {
		t.Fatalf("expected no valid_values for non-text year column, got %v", yearCol.ValidValues)
	}
}

func TestWriteOutputsCreatesThreeFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	model := &smif.SemanticModel{
		SMIFVersion: "0.1.0",
		GeneratedAt: "2026-03-20T00:00:00Z",
		ToolVersion: fmt.Sprintf("strata/%s", appversion.Version),
		Source: smif.Source{
			Type:            "postgres",
			HostFingerprint: "deadbeef",
			SchemaNames:     []string{"public"},
		},
		Domain: smif.Domain{
			Name:        "test",
			Description: "test domain",
			Provenance: smif.Provenance{
				SourceType:    "llm_inferred",
				Confidence:    0.8,
				HumanReviewed: false,
			},
		},
		Models: []smif.Model{
			{
				ModelID:        "orders",
				Name:           "orders",
				Label:          "Orders",
				Description:    "Orders",
				PhysicalSource: smif.PhysicalSource{Schema: "public", Table: "orders"},
				DDLFingerprint: "sha256:abc",
				Columns: []smif.Column{
					{
						Name:                "id",
						DataType:            "bigint",
						Role:                "identifier",
						Label:               "ID",
						Description:         "id",
						CardinalityCategory: "high",
						Provenance:          smif.Provenance{SourceType: "schema_constraint", Confidence: 1.0},
					},
				},
				Provenance: smif.Provenance{SourceType: "schema_constraint", Confidence: 1.0},
			},
		},
	}

	if err := writeOutputs(model, dir); err != nil {
		t.Fatalf("writeOutputs returned error: %v", err)
	}

	yamlPath := filepath.Join(dir, "semantic.yaml")
	jsonPath := filepath.Join(dir, "semantic.json")
	correctionsPath := filepath.Join(dir, "corrections.yaml")

	if _, err := os.Stat(yamlPath); err != nil {
		t.Fatalf("semantic.yaml missing: %v", err)
	}
	if _, err := os.Stat(jsonPath); err != nil {
		t.Fatalf("semantic.json missing: %v", err)
	}
	if _, err := os.Stat(correctionsPath); err != nil {
		t.Fatalf("corrections.yaml missing: %v", err)
	}

	cb, err := os.ReadFile(correctionsPath)
	if err != nil {
		t.Fatalf("read corrections.yaml: %v", err)
	}
	content := string(cb)
	if !strings.Contains(content, "smif_version: \"0.1.0\"") {
		t.Fatalf("expected corrections.yaml to contain smif_version, got: %q", content)
	}
	if !strings.Contains(content, "corrections:") {
		t.Fatalf("expected corrections.yaml to contain corrections key, got: %q", content)
	}

	if _, err := smif.ReadYAML(yamlPath); err != nil {
		t.Fatalf("semantic.yaml not parseable: %v", err)
	}
}

func TestEmptyTableError(t *testing.T) {
	t.Parallel()

	err := checkTableCount("wrong_schema", makeTablesForCountTests(0), 0)
	if err == nil {
		t.Fatalf("expected empty-schema error, got nil")
	}
	if !strings.Contains(err.Error(), "no tables found in schema") {
		t.Fatalf("expected no-tables error, got: %v", err)
	}
}

func TestMaxTablesEnforced(t *testing.T) {
	t.Parallel()

	err := checkTableCount("public", makeTablesForCountTests(5), 3)
	if err == nil {
		t.Fatalf("expected max-tables error, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds --max-tables=3") {
		t.Fatalf("expected max-tables error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "Estimated LLM calls") {
		t.Fatalf("expected LLM calls estimate in error, got: %v", err)
	}
}

func TestMaxTablesZeroMeansNoLimit(t *testing.T) {
	t.Parallel()

	err := checkTableCount("public", makeTablesForCountTests(100), 0)
	if err != nil {
		t.Fatalf("expected no max-tables error when limit is 0, got: %v", err)
	}
}

func TestLargeSchemaWarning(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	tables := makeTablesForCountTests(21)
	err := checkTableCount("public", tables, 0)
	if err != nil {
		t.Fatalf("expected warning only, got error: %v", err)
	}

	warnLargeSchema(NewStderrProgress(&buf), tables)

	out := buf.String()
	if !strings.Contains(out, "⚠") {
		t.Fatalf("expected warning symbol in output, got: %q", out)
	}
	if !strings.Contains(out, "LLM calls") {
		t.Fatalf("expected LLM calls estimate in output, got: %q", out)
	}
}

func TestFilterTablesAllowlist(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{
		{Name: "users"},
		{Name: "orders"},
		{Name: "products"},
		{Name: "order_items"},
	}

	filtered, err := filterTables(tables, []string{"orders", "users"})
	if err != nil {
		t.Fatalf("filterTables returned error: %v", err)
	}
	if len(filtered) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(filtered))
	}
	if filtered[0].Name != "orders" || filtered[1].Name != "users" {
		t.Fatalf("expected [orders users], got [%s %s]", filtered[0].Name, filtered[1].Name)
	}
}

func TestFilterTablesEmpty(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{
		{Name: "users"},
		{Name: "orders"},
		{Name: "products"},
		{Name: "order_items"},
	}

	filtered, err := filterTables(tables, nil)
	if err != nil {
		t.Fatalf("filterTables returned error: %v", err)
	}
	if len(filtered) != 4 {
		t.Fatalf("expected 4 tables, got %d", len(filtered))
	}
}

func TestFilterTablesMissing(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{
		{Name: "users"},
		{Name: "orders"},
		{Name: "products"},
		{Name: "order_items"},
	}

	_, err := filterTables(tables, []string{"orders", "shipments"})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "shipments") {
		t.Fatalf("expected missing table name in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "Available tables") || !strings.Contains(err.Error(), "users") {
		t.Fatalf("expected available table list in error, got: %v", err)
	}
}

func TestFilterTablesCaseInsensitive(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{{Name: "Users"}}

	filtered, err := filterTables(tables, []string{"users"})
	if err != nil {
		t.Fatalf("filterTables returned error: %v", err)
	}
	if len(filtered) != 1 || filtered[0].Name != "Users" {
		t.Fatalf("expected case-insensitive table match, got %+v", filtered)
	}
}

func TestFilterTablesComposesWithMaxTables(t *testing.T) {
	t.Parallel()

	tables := makeTablesForCountTests(10)
	allowlist := []string{"table_0", "table_1", "table_2"}

	filtered, err := filterTables(tables, allowlist)
	if err != nil {
		t.Fatalf("filterTables returned error: %v", err)
	}
	if len(filtered) != 3 {
		t.Fatalf("expected 3 filtered tables, got %d", len(filtered))
	}

	err = checkTableCount("public", filtered, 5)
	if err != nil {
		t.Fatalf("expected no max-table error, got: %v", err)
	}
}

func TestFilterTablesComposesWithMaxTablesAbort(t *testing.T) {
	t.Parallel()

	tables := makeTablesForCountTests(10)
	allowlist := []string{"table_0", "table_1", "table_2", "table_3", "table_4", "table_5"}

	filtered, err := filterTables(tables, allowlist)
	if err != nil {
		t.Fatalf("filterTables returned error: %v", err)
	}
	if len(filtered) != 6 {
		t.Fatalf("expected 6 filtered tables, got %d", len(filtered))
	}

	err = checkTableCount("public", filtered, 5)
	if err == nil {
		t.Fatalf("expected max-table error, got nil")
	}
}

func makeTablesForCountTests(n int) []postgres.TableInfo {
	tables := make([]postgres.TableInfo, 0, n)
	for i := 0; i < n; i++ {
		tables = append(tables, postgres.TableInfo{
			Name: fmt.Sprintf("table_%d", i),
			Columns: []postgres.ColumnInfo{
				{Name: "id", DataType: "bigint"},
			},
		})
	}
	return tables
}

// stubLLMClient is a minimal LLMClient implementation for unit tests.
type stubLLMClient struct {
	model string
}

func (s *stubLLMClient) GenerateStructured(_ context.Context, _ string, _ []byte, _ any) (llmpkg.GenerateResult, error) {
	return llmpkg.GenerateResult{}, nil
}
func (s *stubLLMClient) Provider() string { return "stub" }
func (s *stubLLMClient) Model() string    { return s.model }

func TestStrataLogIncludesLLMModel(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "strata.log")

	rl, err := runlog.Open(logPath)
	if err != nil {
		t.Fatalf("open runlog: %v", err)
	}

	stub := &stubLLMClient{model: "deepseek-chat"}

	// Write the run_start entry exactly as pipeline.Init does.
	rl.Write(runlog.Entry{
		Event:    "run_start",
		LLMModel: stub.Model(),
		BaseURL:  "https://api.deepseek.com",
	})
	if err := rl.Close(); err != nil {
		t.Fatalf("close runlog: %v", err)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read strata.log: %v", err)
	}
	content := string(data)

	if !strings.Contains(content, `"event":"run_start"`) {
		t.Fatalf("expected run_start event in strata.log, got: %q", content)
	}
	if !strings.Contains(content, `"llm_model":"deepseek-chat"`) {
		t.Fatalf("expected llm_model in strata.log, got: %q", content)
	}
	if !strings.Contains(content, `"base_url":"https://api.deepseek.com"`) {
		t.Fatalf("expected base_url in strata.log, got: %q", content)
	}
}
