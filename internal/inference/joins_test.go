package inference

import (
	"strings"
	"testing"

	coarse "github.com/strata-spec/openstrata/internal/inference/coarse"
	"github.com/strata-spec/openstrata/internal/postgres"
)

func TestInferJoinsFromFK(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{
		{
			Name: "users",
			Columns: []postgres.ColumnInfo{
				{Name: "id"},
			},
			PrimaryKey: []string{"id"},
		},
		{
			Name: "orders",
			Columns: []postgres.ColumnInfo{
				{Name: "id"},
				{Name: "user_id"},
			},
			PrimaryKey: []string{"id"},
			ForeignKeys: []postgres.FKConstraint{
				{
					ConstraintName: "fk_orders_user",
					FromColumns:    []string{"user_id"},
					ToTable:        "users",
					ToColumns:      []string{"id"},
				},
			},
		},
	}

	rels, err := InferJoins(tables, nil, "")
	if err != nil {
		t.Fatalf("InferJoins returned error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 relationship, got %d", len(rels))
	}

	r := rels[0]
	if r.SourceType != sourceSchemaConstraint {
		t.Fatalf("expected source_type=%s, got %s", sourceSchemaConstraint, r.SourceType)
	}
	if r.Confidence != 1.0 {
		t.Fatalf("expected confidence=1.0, got %v", r.Confidence)
	}
	if r.FromModel != "orders" || r.FromColumn != "user_id" {
		t.Fatalf("unexpected from side: %s.%s", r.FromModel, r.FromColumn)
	}
	if r.ToModel != "users" || r.ToColumn != "id" {
		t.Fatalf("unexpected to side: %s.%s", r.ToModel, r.ToColumn)
	}
	if !r.Preferred {
		t.Fatalf("expected preferred=true for only model pair relationship")
	}
	if r.JoinCondition != "orders.user_id = users.id" {
		t.Fatalf("unexpected join condition: %q", r.JoinCondition)
	}
}

func TestInferJoinsDeduplication(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{
		{
			Name:    "users",
			Columns: []postgres.ColumnInfo{{Name: "id"}},
		},
		{
			Name:    "orders",
			Columns: []postgres.ColumnInfo{{Name: "id"}, {Name: "user_id"}},
			ForeignKeys: []postgres.FKConstraint{
				{FromColumns: []string{"user_id"}, ToTable: "users", ToColumns: []string{"id"}},
			},
		},
	}
	usage := []postgres.UsageProfile{
		{TableName: "orders", ColumnName: "user_id", JoinCount: 15},
	}

	rels, err := InferJoins(tables, usage, "")
	if err != nil {
		t.Fatalf("InferJoins returned error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 relationship, got %d", len(rels))
	}
	if rels[0].SourceType != sourceSchemaConstraint {
		t.Fatalf("expected schema_constraint to win, got %s", rels[0].SourceType)
	}
}

func TestInferJoinsLogInferred(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{
		{
			Name:    "products",
			Columns: []postgres.ColumnInfo{{Name: "id"}},
		},
		{
			Name:    "order_items",
			Columns: []postgres.ColumnInfo{{Name: "id"}, {Name: "product_id"}},
		},
	}
	usage := []postgres.UsageProfile{
		{TableName: "order_items", ColumnName: "product_id", JoinCount: 12},
	}

	rels, err := InferJoins(tables, usage, "")
	if err != nil {
		t.Fatalf("InferJoins returned error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 relationship, got %d", len(rels))
	}

	r := rels[0]
	if r.SourceType != sourceLogInferred {
		t.Fatalf("expected source_type=%s, got %s", sourceLogInferred, r.SourceType)
	}
	if r.Confidence != 0.75 {
		t.Fatalf("expected confidence=0.75 for JoinCount >= 10, got %v", r.Confidence)
	}
	if r.FromModel != "order_items" || r.FromColumn != "product_id" || r.ToModel != "products" || r.ToColumn != "id" {
		t.Fatalf("unexpected relationship: %+v", r)
	}
}

func TestInferJoinsStrataMD(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{
		{Name: "shipments", Columns: []postgres.ColumnInfo{{Name: "warehouse_id"}}},
		{Name: "warehouses", Columns: []postgres.ColumnInfo{{Name: "id"}}},
	}

	strataMD := "## Canonical Joins\nshipments.warehouse_id = warehouses.id\n"
	rels, err := InferJoins(tables, nil, strataMD)
	if err != nil {
		t.Fatalf("InferJoins returned error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 relationship, got %d", len(rels))
	}
	if rels[0].SourceType != sourceUserDefined {
		t.Fatalf("expected user_defined source, got %s", rels[0].SourceType)
	}
	if rels[0].Confidence != 1.0 {
		t.Fatalf("expected confidence=1.0, got %v", rels[0].Confidence)
	}
}

func TestInferJoinsStrataMDOverridesFK(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{
		{Name: "users", Columns: []postgres.ColumnInfo{{Name: "id"}}},
		{
			Name:    "orders",
			Columns: []postgres.ColumnInfo{{Name: "user_id"}},
			ForeignKeys: []postgres.FKConstraint{
				{FromColumns: []string{"user_id"}, ToTable: "users", ToColumns: []string{"id"}},
			},
		},
	}
	strataMD := "## canonical joins\norders.user_id = users.id\n"

	rels, err := InferJoins(tables, nil, strataMD)
	if err != nil {
		t.Fatalf("InferJoins returned error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 relationship, got %d", len(rels))
	}
	if rels[0].SourceType != sourceUserDefined {
		t.Fatalf("expected user_defined to override FK, got %s", rels[0].SourceType)
	}
}

func TestConfirmGrains(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{
		{Name: "users", PrimaryKey: []string{"id"}},
		{Name: "orphans", PrimaryKey: nil},
		{Name: "user_products", PrimaryKey: []string{"user_id", "product_id"}},
		{Name: "daily_orders", PrimaryKey: []string{"user_id", "date"}},
	}
	results := []coarse.TableResult{
		{TableName: "users", Grain: "one row per user"},
		{TableName: "orphans", Grain: "one row per orphan"},
		{TableName: "user_products", Grain: "one row per user_id product_id combination"},
		{TableName: "daily_orders", Grain: "one row per order"},
	}

	confs := ConfirmGrains(tables, results)
	if len(confs) != 4 {
		t.Fatalf("expected 4 confirmations, got %d", len(confs))
	}

	if !confs[0].Confirmed {
		t.Fatalf("expected single-column PK to be confirmed")
	}
	if confs[1].Confirmed || !strings.Contains(strings.ToLower(confs[1].Note), "no primary key") {
		t.Fatalf("expected empty PK to be unconfirmed with no primary key note, got: %+v", confs[1])
	}
	if !confs[2].Confirmed {
		t.Fatalf("expected composite PK with all columns referenced in grain to be confirmed")
	}
	if confs[3].Confirmed {
		t.Fatalf("expected missing composite PK column mention to be unconfirmed")
	}
	if !strings.Contains(strings.ToLower(confs[3].Note), "date") {
		t.Fatalf("expected missing PK note to include date, got: %s", confs[3].Note)
	}
}

func TestRelationshipIDFormat(t *testing.T) {
	t.Parallel()

	single := relationshipID("Orders", "User_ID", "Users")
	if single != "orders_user_id_to_users" {
		t.Fatalf("unexpected single-column relationship ID: %s", single)
	}

	tables := []postgres.TableInfo{
		{Name: "users", Columns: []postgres.ColumnInfo{{Name: "id"}, {Name: "date"}}},
		{
			Name:    "orders",
			Columns: []postgres.ColumnInfo{{Name: "user_id"}, {Name: "date"}},
			ForeignKeys: []postgres.FKConstraint{
				{FromColumns: []string{"user_id", "date"}, ToTable: "users", ToColumns: []string{"id", "date"}},
			},
		},
	}

	rels, err := InferJoins(tables, nil, "")
	if err != nil {
		t.Fatalf("InferJoins returned error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 relationship, got %d", len(rels))
	}
	if rels[0].RelationshipID != "orders_user_id_date_to_users" {
		t.Fatalf("unexpected composite relationship ID: %s", rels[0].RelationshipID)
	}
}
