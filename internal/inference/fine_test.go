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

func TestApplyPostProcessingOrdinalPatternCorrection(t *testing.T) {
	t.Parallel()

	// schema_version is an integer column with an ordinal/version suffix and no
	// PK or FK constraint. The LLM mistakenly returned identifier; post-processing
	// must correct it to dimension with confidence-lowering difficulty.
	t.Run("schema_version integer no constraint → dimension", func(t *testing.T) {
		table := postgres.TableInfo{
			Name: "migrations",
			Columns: []postgres.ColumnInfo{
				{Name: "schema_version", DataType: "integer"},
			},
		}
		result := FinePassResult{
			TableName: "migrations",
			Columns: []ColumnResult{
				{TableName: "migrations", ColumnName: "schema_version", Role: "identifier", Label: "Schema Version", Description: "Schema version number.", Difficulty: "self_evident"},
			},
		}

		applyPostProcessing(&result, table)

		if len(result.Columns) != 1 {
			t.Fatalf("expected 1 column, got %d", len(result.Columns))
		}
		col := result.Columns[0]
		if col.Role != "dimension" {
			t.Errorf("schema_version with no PK/FK constraint: expected role dimension, got %s", col.Role)
		}
		if col.NeedsReview != true {
			t.Errorf("schema_version corrected column: expected NeedsReview=true")
		}
		// Difficulty must not be self_evident so that confidence stays below 0.80.
		if col.Difficulty == "self_evident" {
			t.Errorf("schema_version corrected column: difficulty must not be self_evident (confidence would be 0.80, not < 0.80)")
		}
	})

	// movie_id is an integer column that is an FK target; the identifier role
	// assigned by the LLM is correct and must be preserved.
	t.Run("movie_id integer FK → identifier preserved", func(t *testing.T) {
		table := postgres.TableInfo{
			Name: "rentals",
			Columns: []postgres.ColumnInfo{
				{Name: "movie_id", DataType: "integer"},
			},
			ForeignKeys: []postgres.FKConstraint{
				{
					ConstraintName: "fk_rentals_movie",
					FromColumns:    []string{"movie_id"},
					ToTable:        "movies",
					ToColumns:      []string{"id"},
				},
			},
		}
		result := FinePassResult{
			TableName: "rentals",
			Columns: []ColumnResult{
				{TableName: "rentals", ColumnName: "movie_id", Role: "identifier", Label: "Movie ID", Description: "FK to movies.", Difficulty: "self_evident"},
			},
		}

		applyPostProcessing(&result, table)

		if len(result.Columns) != 1 {
			t.Fatalf("expected 1 column, got %d", len(result.Columns))
		}
		if result.Columns[0].Role != "identifier" {
			t.Errorf("movie_id with FK constraint: expected role identifier, got %s", result.Columns[0].Role)
		}
	})

	// display_order with a PK constraint (unusual but valid): identifier must be kept.
	t.Run("display_order integer PK → identifier preserved", func(t *testing.T) {
		table := postgres.TableInfo{
			Name:       "menu_items",
			PrimaryKey: []string{"display_order"},
			Columns: []postgres.ColumnInfo{
				{Name: "display_order", DataType: "bigint"},
			},
		}
		result := FinePassResult{
			TableName: "menu_items",
			Columns: []ColumnResult{
				{TableName: "menu_items", ColumnName: "display_order", Role: "identifier", Label: "Display Order", Description: "PK.", Difficulty: "self_evident"},
			},
		}

		applyPostProcessing(&result, table)

		if result.Columns[0].Role != "identifier" {
			t.Errorf("display_order with PK constraint: expected role identifier, got %s", result.Columns[0].Role)
		}
	})

	// sort_rank with a text data type should not be affected (pattern only fires on integer types).
	t.Run("sort_rank text type not corrected", func(t *testing.T) {
		table := postgres.TableInfo{
			Name: "products",
			Columns: []postgres.ColumnInfo{
				{Name: "sort_rank", DataType: "text"},
			},
		}
		result := FinePassResult{
			TableName: "products",
			Columns: []ColumnResult{
				{TableName: "products", ColumnName: "sort_rank", Role: "identifier", Label: "Sort Rank", Description: "Text rank.", Difficulty: "self_evident"},
			},
		}

		applyPostProcessing(&result, table)

		if result.Columns[0].Role != "identifier" {
			t.Errorf("sort_rank with text type: expected role identifier to be unchanged, got %s", result.Columns[0].Role)
		}
	})
}

func TestIsOrdinalPattern(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		want    bool
	}{
		{"schema_version", true},
		{"display_order", true},
		{"sort_rank", true},
		{"row_seq", true},
		{"row_index", true},
		{"movie_id", false},
		{"user_id", false},
		{"id", false},
		{"version", false}, // no underscore prefix – suffix check is exact
		{"order_id", false},
	}

	for _, tc := range cases {
		got := isOrdinalPattern(tc.name)
		if got != tc.want {
			t.Errorf("isOrdinalPattern(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestIsIntegerDataType(t *testing.T) {
	t.Parallel()

	intTypes := []string{"integer", "int", "int2", "int4", "int8", "bigint", "smallint", "serial", "bigserial", "smallserial", "INTEGER", "BIGINT"}
	for _, dt := range intTypes {
		if !isIntegerDataType(dt) {
			t.Errorf("isIntegerDataType(%q) = false, want true", dt)
		}
	}

	nonIntTypes := []string{"text", "varchar", "numeric", "float4", "float8", "boolean", "uuid", "timestamptz"}
	for _, dt := range nonIntTypes {
		if isIntegerDataType(dt) {
			t.Errorf("isIntegerDataType(%q) = true, want false", dt)
		}
	}
}

func TestBuildFinePassPromptOrdinalGuidance(t *testing.T) {
	t.Parallel()

	table := postgres.TableInfo{
		Name: "schema_info",
		Columns: []postgres.ColumnInfo{
			{Name: "schema_version", DataType: "integer"},
		},
	}
	prompt := buildFinePassPrompt(table, nil, TableResult{TableName: "schema_info"}, nil, "")

	if !strings.Contains(prompt, "version numbers, ordinals, or sort keys") {
		t.Fatalf("expected ordinal/version guidance in prompt, got:\n%s", prompt)
	}
}
