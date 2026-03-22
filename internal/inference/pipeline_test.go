package inference

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	coarsepkg "github.com/strata-spec/openstrata/internal/inference/coarse"
	finepkg "github.com/strata-spec/openstrata/internal/inference/fine"
	joinspkg "github.com/strata-spec/openstrata/internal/inference/joins"
	"github.com/strata-spec/openstrata/internal/postgres"
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
	if !strings.Contains(string(cb), "corrections: []") {
		t.Fatalf("expected corrections.yaml to contain corrections: []")
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
