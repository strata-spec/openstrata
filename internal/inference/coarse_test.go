package inference

import (
	"strings"
	"testing"

	"github.com/strata-spec/openstrata/internal/postgres"
)

func TestBuildDomainPrompt(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{
		{Name: "users", Columns: []postgres.ColumnInfo{{Name: "id"}}, PrimaryKey: []string{"id"}},
		{Name: "orders", Columns: []postgres.ColumnInfo{{Name: "id"}, {Name: "user_id"}}, PrimaryKey: []string{"id"}},
	}

	withStrata := buildDomainPrompt(tables, "Business context")
	if !strings.Contains(withStrata, "<strata_md>") {
		t.Fatalf("expected <strata_md> block when strataMD is provided")
	}
	for _, tableName := range []string{"users", "orders"} {
		if !strings.Contains(withStrata, tableName) {
			t.Fatalf("expected table %q in prompt", tableName)
		}
	}

	withoutStrata := buildDomainPrompt(tables, "")
	if strings.Contains(withoutStrata, "<strata_md>") {
		t.Fatalf("did not expect <strata_md> block when strataMD is empty")
	}
	for _, tableName := range []string{"users", "orders"} {
		if !strings.Contains(withoutStrata, tableName) {
			t.Fatalf("expected table %q in prompt", tableName)
		}
	}
}

func TestBuildTablePrompt(t *testing.T) {
	t.Parallel()

	table := postgres.TableInfo{
		Name:       "order_items",
		PrimaryKey: []string{"id"},
		Columns: []postgres.ColumnInfo{
			{Name: "id", DataType: "bigint", IsNullable: false},
			{Name: "order_id", DataType: "bigint", IsNullable: false, Comment: "FK to orders"},
			{Name: "quantity", DataType: "int", IsNullable: true},
		},
		ForeignKeys: []postgres.FKConstraint{
			{
				ConstraintName: "fk_order_items_order",
				FromColumns:    []string{"order_id"},
				ToTable:        "orders",
				ToColumns:      []string{"id"},
			},
		},
	}
	prompt := buildTablePrompt(table, &DomainResult{Name: "commerce", Description: "Retail ops"}, "")

	if !strings.Contains(prompt, "[PK]") {
		t.Fatalf("expected PK marker in prompt")
	}
	if !strings.Contains(prompt, "[FK->orders.id]") {
		t.Fatalf("expected FK marker in prompt")
	}
	if !strings.Contains(prompt, "// FK to orders") {
		t.Fatalf("expected DDL comment marker in prompt")
	}
	if strings.Contains(prompt, "<strata_md>") {
		t.Fatalf("did not expect <strata_md> block when strataMD is empty")
	}
}
