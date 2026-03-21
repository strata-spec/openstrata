package inference

import (
	"strings"
	"testing"

	"github.com/strata-spec/openstrata/internal/postgres"
)

func TestBuildFinePassPrompt(t *testing.T) {
	t.Parallel()

	table := postgres.TableInfo{
		Name:       "orders",
		PrimaryKey: []string{"id"},
		Columns: []postgres.ColumnInfo{
			{Name: "id", DataType: "bigint", IsNullable: false},
			{Name: "user_id", DataType: "bigint", IsNullable: false, Comment: "customer foreign key"},
			{Name: "status", DataType: "text", IsNullable: false},
		},
		ForeignKeys: []postgres.FKConstraint{
			{
				ConstraintName: "fk_orders_user",
				FromColumns:    []string{"user_id"},
				ToTable:        "users",
				ToColumns:      []string{"id"},
			},
		},
	}

	profile := map[string]postgres.ColumnProfile{
		"status": {
			TableName:     "orders",
			ColumnName:    "status",
			DistinctCount: 4,
			NullCount:     0,
			ExampleValues: []string{"pending", "shipped"},
		},
	}

	tableResult := TableResult{TableName: "orders", Description: "Customer orders", Grain: "one row per order"}
	domain := &DomainResult{Name: "commerce", Description: "Storefront transactions"}

	promptNoStrata := buildFinePassPrompt(table, profile, tableResult, domain, "")
	if !strings.Contains(promptNoStrata, "[FK→") {
		t.Fatalf("expected FK marker in prompt")
	}
	if !strings.Contains(promptNoStrata, "pending, shipped") {
		t.Fatalf("expected example values in prompt")
	}
	if !strings.Contains(promptNoStrata, "[PK]") {
		t.Fatalf("expected PK marker in prompt")
	}
	if !strings.Contains(promptNoStrata, "// customer foreign key") {
		t.Fatalf("expected comment marker in prompt")
	}
	if strings.Contains(promptNoStrata, "<strata_md>") {
		t.Fatalf("did not expect <strata_md> block when strataMD is empty")
	}

	promptWithStrata := buildFinePassPrompt(table, profile, tableResult, domain, "use domain terms")
	if !strings.Contains(promptWithStrata, "<strata_md>") {
		t.Fatalf("expected <strata_md> block when strataMD is provided")
	}
}

func TestFinePassPostProcessing(t *testing.T) {
	t.Parallel()

	table := postgres.TableInfo{
		Name: "orders",
		Columns: []postgres.ColumnInfo{
			{Name: "id"},
			{Name: "status"},
			{Name: "created_at"},
		},
	}

	t.Run("padding and needs review and case-insensitive alignment", func(t *testing.T) {
		result := FinePassResult{
			TableName: "orders",
			Columns: []ColumnResult{
				{TableName: "orders", ColumnName: "ID", Role: "identifier", Label: "ID", Description: "Identifier", Difficulty: "self_evident"},
				{TableName: "orders", ColumnName: "status", Role: "dimension", Label: "Status", Description: "State", Difficulty: "ambiguous"},
			},
		}

		applyPostProcessing(&result, table)

		if len(result.Columns) != len(table.Columns) {
			t.Fatalf("expected %d columns after padding, got %d", len(table.Columns), len(result.Columns))
		}
		if result.Columns[0].ColumnName != "id" {
			t.Fatalf("expected case-insensitive alignment to canonical column name id, got %s", result.Columns[0].ColumnName)
		}
		if result.Columns[1].NeedsReview != true {
			t.Fatalf("expected ambiguous difficulty to set NeedsReview=true")
		}
		if result.Columns[0].NeedsReview != false {
			t.Fatalf("expected self_evident difficulty to set NeedsReview=false")
		}
		if result.Columns[2].Description != "Could not be inferred." {
			t.Fatalf("expected padded column description default, got %q", result.Columns[2].Description)
		}
	})

	t.Run("truncation and unknown column discard", func(t *testing.T) {
		result := FinePassResult{
			TableName: "orders",
			Columns: []ColumnResult{
				{TableName: "orders", ColumnName: "id", Role: "identifier", Label: "ID", Description: "Identifier", Difficulty: "self_evident"},
				{TableName: "orders", ColumnName: "status", Role: "dimension", Label: "Status", Description: "State", Difficulty: "context_dependent"},
				{TableName: "orders", ColumnName: "created_at", Role: "timestamp", Label: "Created At", Description: "Creation time", Difficulty: "self_evident"},
				{TableName: "orders", ColumnName: "unknown_column", Role: "dimension", Label: "Unknown", Description: "Bad", Difficulty: "domain_dependent"},
			},
		}

		applyPostProcessing(&result, table)

		if len(result.Columns) != len(table.Columns) {
			t.Fatalf("expected truncation/preservation to %d columns, got %d", len(table.Columns), len(result.Columns))
		}
		for _, col := range result.Columns {
			if col.ColumnName == "unknown_column" {
				t.Fatalf("expected unknown columns to be discarded")
			}
		}
	})
}
