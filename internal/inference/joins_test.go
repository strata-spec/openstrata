package inference

import (
	"sort"
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

	rels, _, err := InferJoins(tables, nil, "", nil)
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

	rels, _, err := InferJoins(tables, usage, "", nil)
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

	rels, _, err := InferJoins(tables, usage, "", nil)
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

func TestCanonicalJoinsGetStrataMDSourceType(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{
		{Name: "shipments", Columns: []postgres.ColumnInfo{{Name: "warehouse_id"}}},
		{Name: "warehouses", Columns: []postgres.ColumnInfo{{Name: "id"}}},
	}

	strataMD := "## Canonical Joins\nshipments.warehouse_id = warehouses.id\n"
	rels, _, err := InferJoins(tables, nil, strataMD, nil)
	if err != nil {
		t.Fatalf("InferJoins returned error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 relationship, got %d", len(rels))
	}
	if rels[0].SourceType != sourceStrataMD {
		t.Fatalf("expected strata_md source, got %s", rels[0].SourceType)
	}
	if rels[0].Confidence != 0.95 {
		t.Fatalf("expected confidence=0.95, got %v", rels[0].Confidence)
	}
	if rels[0].SourceType == sourceUserDefined {
		t.Fatalf("canonical joins must not use user_defined provenance")
	}
}

// TestSchemaConstraintWinsOverStrataMDOnConfidence verifies that when the same
// four-part join (from_model, from_column, to_model, to_column) appears as both
// a schema_constraint (confidence 1.0) and a strata_md join (confidence 0.95),
// dedup keeps the schema_constraint because confidence is the primary criterion.
func TestSchemaConstraintWinsOverStrataMDOnConfidence(t *testing.T) {
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

	rels, _, err := InferJoins(tables, nil, strataMD, nil)
	if err != nil {
		t.Fatalf("InferJoins returned error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 relationship after dedup (same four-part key), got %d", len(rels))
	}
	// schema_constraint (1.0) beats strata_md (0.95) on confidence.
	if rels[0].SourceType != sourceSchemaConstraint {
		t.Fatalf("expected schema_constraint, got %s", rels[0].SourceType)
	}
	// Regression pin: if logic reverts to trust-first, strata_md (trust=3) beats
	// schema_constraint (trust=2) and returns Confidence=0.95, failing here.
	if rels[0].Confidence != 1.0 {
		t.Fatalf("expected confidence 1.0 (schema_constraint wins on confidence), got %f", rels[0].Confidence)
	}
}

func TestPreferredFlagSameModelPairDifferentColumns(t *testing.T) {
	t.Parallel()

	// Table a references b via two distinct column pairs.
	// Both relationships survive dedup (different four-part keys).
	// Exactly one must be preferred.
	tables := []postgres.TableInfo{
		{Name: "b", Columns: []postgres.ColumnInfo{{Name: "a_id"}, {Name: "a_code"}}},
		{
			Name:    "a",
			Columns: []postgres.ColumnInfo{{Name: "id"}, {Name: "code"}},
			ForeignKeys: []postgres.FKConstraint{
				{FromColumns: []string{"id"}, ToTable: "b", ToColumns: []string{"a_id"}},
				{FromColumns: []string{"code"}, ToTable: "b", ToColumns: []string{"a_code"}},
			},
		},
	}

	rels, _, err := InferJoins(tables, nil, "", nil)
	if err != nil {
		t.Fatalf("InferJoins returned error: %v", err)
	}

	var ab []InferredRelationship
	for _, r := range rels {
		if r.FromModel == "a" && r.ToModel == "b" {
			ab = append(ab, r)
		}
	}
	if len(ab) != 2 {
		t.Fatalf("expected 2 relationships for a→b (different column pairs), got %d", len(ab))
	}

	preferredCount := 0
	for _, r := range ab {
		if r.Preferred {
			preferredCount++
		}
	}
	if preferredCount != 1 {
		t.Fatalf("expected exactly 1 preferred for a→b, got %d", preferredCount)
	}
}

func TestPreferredFlagStrataMDAndFKSamePair(t *testing.T) {
	t.Parallel()

	// Same four-part key from both schema_constraint (confidence 1.0) and
	// strata_md (confidence 0.95). Dedup must keep exactly one.
	// Winner: schema_constraint, because confidence is the primary criterion
	// (1.0 > 0.95) even though strata_md has higher source trust.
	tables := []postgres.TableInfo{
		{Name: "b", Columns: []postgres.ColumnInfo{{Name: "id"}}},
		{
			Name:    "a",
			Columns: []postgres.ColumnInfo{{Name: "b_id"}},
			ForeignKeys: []postgres.FKConstraint{
				{FromColumns: []string{"b_id"}, ToTable: "b", ToColumns: []string{"id"}},
			},
		},
	}
	strataMD := "## Canonical Joins\na.b_id = b.id\n"

	rels, _, err := InferJoins(tables, nil, strataMD, nil)
	if err != nil {
		t.Fatalf("InferJoins returned error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 relationship after dedup (same four-part key), got %d", len(rels))
	}
	if rels[0].SourceType != sourceSchemaConstraint {
		t.Fatalf("expected schema_constraint to win on confidence (1.0 > 0.95), got %s", rels[0].SourceType)
	}
	if !rels[0].Preferred {
		t.Fatalf("expected the sole relationship to be preferred")
	}
}

func TestInferJoinsFiltersOutOfScopeModels(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{
		{Name: "users", Columns: []postgres.ColumnInfo{{Name: "id"}}},
		{Name: "products", Columns: []postgres.ColumnInfo{{Name: "id"}}},
		{
			Name:    "orders",
			Columns: []postgres.ColumnInfo{{Name: "user_id"}, {Name: "product_id"}},
			ForeignKeys: []postgres.FKConstraint{
				{FromColumns: []string{"user_id"}, ToTable: "users", ToColumns: []string{"id"}},
				{FromColumns: []string{"product_id"}, ToTable: "products", ToColumns: []string{"id"}},
			},
		},
	}

	rels, dropped, err := InferJoins(tables, nil, "", []string{"orders", "users"})
	if err != nil {
		t.Fatalf("InferJoins returned error: %v", err)
	}
	if dropped != 1 {
		t.Fatalf("expected dropped_count=1, got %d", dropped)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 in-scope relationship, got %d", len(rels))
	}
	if rels[0].ToModel != "users" {
		t.Fatalf("expected retained relationship to users, got to_model=%s", rels[0].ToModel)
	}
	for _, r := range rels {
		if r.ToModel == "products" {
			t.Fatalf("unexpected out-of-scope relationship retained: %+v", r)
		}
	}
}

func TestInferJoinsNoFilterWhenSelectedTablesEmpty(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{
		{Name: "users", Columns: []postgres.ColumnInfo{{Name: "id"}}},
		{Name: "products", Columns: []postgres.ColumnInfo{{Name: "id"}}},
		{
			Name:    "orders",
			Columns: []postgres.ColumnInfo{{Name: "user_id"}, {Name: "product_id"}},
			ForeignKeys: []postgres.FKConstraint{
				{FromColumns: []string{"user_id"}, ToTable: "users", ToColumns: []string{"id"}},
				{FromColumns: []string{"product_id"}, ToTable: "products", ToColumns: []string{"id"}},
			},
		},
	}

	rels, dropped, err := InferJoins(tables, nil, "", nil)
	if err != nil {
		t.Fatalf("InferJoins returned error: %v", err)
	}
	if dropped != 0 {
		t.Fatalf("expected dropped_count=0 without filter, got %d", dropped)
	}
	if len(rels) != 2 {
		t.Fatalf("expected both relationships retained, got %d", len(rels))
	}
}

func TestInferJoinsDropCountWarning(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{
		{Name: "users", Columns: []postgres.ColumnInfo{{Name: "id"}}},
		{Name: "products", Columns: []postgres.ColumnInfo{{Name: "id"}}},
		{
			Name:    "orders",
			Columns: []postgres.ColumnInfo{{Name: "user_id"}, {Name: "product_id"}},
			ForeignKeys: []postgres.FKConstraint{
				{FromColumns: []string{"user_id"}, ToTable: "users", ToColumns: []string{"id"}},
				{FromColumns: []string{"product_id"}, ToTable: "products", ToColumns: []string{"id"}},
			},
		},
	}

	_, dropped, err := InferJoins(tables, nil, "", []string{"orders", "users"})
	if err != nil {
		t.Fatalf("InferJoins returned error: %v", err)
	}
	if dropped == 0 {
		t.Fatalf("expected dropped_count > 0 when out-of-scope relationships are present")
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

	rels, _, err := InferJoins(tables, nil, "", nil)
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

func TestPreferredFlagAtMostOnePerModelPair(t *testing.T) {
	t.Parallel()

	tables := []postgres.TableInfo{
		{Name: "users", Columns: []postgres.ColumnInfo{{Name: "id"}}},
		{
			Name:    "orders",
			Columns: []postgres.ColumnInfo{{Name: "account_id"}, {Name: "owner_id"}, {Name: "user_id"}},
			ForeignKeys: []postgres.FKConstraint{
				{FromColumns: []string{"account_id"}, ToTable: "users", ToColumns: []string{"id"}},
			},
		},
	}
	usage := []postgres.UsageProfile{{TableName: "orders", ColumnName: "owner_id", JoinCount: 12}}
	strataMD := "## Canonical Joins\norders.user_id = users.id\n"

	rels, _, err := InferJoins(tables, usage, strataMD, nil)
	if err != nil {
		t.Fatalf("InferJoins returned error: %v", err)
	}

	preferredCount := 0
	for _, r := range rels {
		if r.FromModel == "orders" && r.ToModel == "users" && r.Preferred {
			preferredCount++
		}
	}
	if preferredCount != 1 {
		t.Fatalf("expected exactly one preferred relationship for orders->users, got %d", preferredCount)
	}
}

func TestPreferredFlagResetBeforeAssignment(t *testing.T) {
	t.Parallel()

	deduped := deduplicateRelationships([]InferredRelationship{
		{
			FromModel:  "orders",
			ToModel:    "users",
			FromColumn: "user_id",
			ToColumn:   "id",
			SourceType: sourceStrataMD,
			Confidence: 0.95,
			Preferred:  true,
		},
		{
			FromModel:  "orders",
			ToModel:    "users",
			FromColumn: "account_id",
			ToColumn:   "id",
			SourceType: sourceSchemaConstraint,
			Confidence: 1.0,
			Preferred:  true,
		},
	})

	markPreferred(deduped)

	preferredCount := 0
	for _, r := range deduped {
		if r.FromModel == "orders" && r.ToModel == "users" && r.Preferred {
			preferredCount++
		}
	}
	if preferredCount != 1 {
		t.Fatalf("expected at most one preferred relationship for orders->users, got %d", preferredCount)
	}
}

// TestPreferredFlagBidirectionalPair verifies that A→B and B→A are treated as
// the same model pair by markPreferred, matching the unordered key used by checkV022.
// Only one of the two directions should get Preferred=true.
func TestPreferredFlagBidirectionalPair(t *testing.T) {
	t.Parallel()

	rels := []InferredRelationship{
		{
			FromModel:  "orders",
			ToModel:    "users",
			FromColumn: "user_id",
			ToColumn:   "id",
			SourceType: sourceSchemaConstraint,
			Confidence: 1.0,
			Preferred:  false,
		},
		{
			FromModel:  "users",
			ToModel:    "orders",
			FromColumn: "id",
			ToColumn:   "user_id",
			SourceType: sourceLogInferred,
			Confidence: 0.7,
			Preferred:  false,
		},
	}

	markPreferred(rels)

	preferredCount := 0
	for _, r := range rels {
		if r.Preferred {
			preferredCount++
		}
	}
	if preferredCount != 1 {
		t.Fatalf("expected exactly 1 preferred relationship across bidirectional pair, got %d", preferredCount)
	}

	// The schema_constraint direction (higher confidence) must win.
	pairKey := func(from, to string) string {
		parts := []string{strings.ToLower(from), strings.ToLower(to)}
		sort.Strings(parts)
		return parts[0] + "|" + parts[1]
	}
	winner := ""
	for _, r := range rels {
		if r.Preferred {
			winner = pairKey(r.FromModel, r.ToModel)
		}
	}
	if winner == "" {
		t.Fatal("no preferred relationship found")
	}
	// Sanity: the winner key must equal the loser key (same unordered pair).
	for _, r := range rels {
		if !r.Preferred {
			loserKey := pairKey(r.FromModel, r.ToModel)
			if loserKey != winner {
				t.Errorf("winner key %q != loser key %q — pairs are not treated as the same model pair", winner, loserKey)
			}
		}
	}

	// The schema_constraint relationship must be preferred (confidence 1.0 beats 0.7).
	for _, r := range rels {
		if r.SourceType == sourceSchemaConstraint && !r.Preferred {
			t.Error("expected schema_constraint direction to be preferred (higher confidence), but it is not")
		}
		if r.SourceType == sourceLogInferred && r.Preferred {
			t.Error("log_inferred direction should NOT be preferred (lower confidence), but it is")
		}
	}
}
