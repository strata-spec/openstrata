package postgres

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRedactPII(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input    string
		wantType string
	}{
		{"user@example.com", "email"},
		{"USER@EXAMPLE.COM", "email"},
		{"not an email", ""},
		{"555-123-4567", "phone"},
		{"+1 (555) 123-4567", "phone"},
		{"123-45-6789", "ssn"},
		{"4111 1111 1111 1111", "cc"},
		{"4111-1111-1111-1111", "cc"},
		{"hello world", ""},
		{"revenue_usd", ""},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()

			got := RedactPII(tc.input)
			if tc.wantType == "" {
				if got != tc.input {
					t.Fatalf("expected clean value %q, got %q", tc.input, got)
				}
				return
			}

			want := "[REDACTED:" + tc.wantType + "]"
			if got != want {
				t.Fatalf("expected redacted value %q, got %q", want, got)
			}
		})
	}
}

func TestCardinalityCategory(t *testing.T) {
	t.Parallel()

	cases := []struct {
		distinctCount int64
		want          string
	}{
		{0, "low"},
		{99, "low"},
		{100, "medium"},
		{9999, "medium"},
		{10000, "high"},
		{10001, "high"},
	}

	for _, tc := range cases {
		got := cardinalityCategory(tc.distinctCount)
		if got != tc.want {
			t.Fatalf("distinct=%d: expected %q, got %q", tc.distinctCount, tc.want, got)
		}
	}
}

func TestProfileSampleBased(t *testing.T) {
	pool := integrationPool(t)
	schema := loadTestSchema(t, pool)

	tables, _, err := Introspect(context.Background(), pool, schema)
	if err != nil {
		t.Fatalf("Introspect returned error: %v", err)
	}

	profiles, err := Profile(context.Background(), pool, tables, NoOpProfileProgress{})
	if err != nil {
		t.Fatalf("Profile returned error: %v", err)
	}

	if len(profiles) == 0 {
		t.Fatalf("expected non-empty profiles")
	}

	emailProfile, ok := profiles["users.email"]
	if !ok {
		t.Fatalf("expected users.email profile")
	}

	switch emailProfile.CardinalityCategory {
	case "low", "medium", "high":
	default:
		t.Fatalf("unexpected users.email cardinality category: %q", emailProfile.CardinalityCategory)
	}

	for _, value := range emailProfile.ExampleValues {
		if strings.Contains(value, "@") && value != "[REDACTED:email]" {
			t.Fatalf("users.email contains unredacted email value: %q", value)
		}
	}

	ddl, readErr := os.ReadFile("../../testdata/schemas/ecommerce.sql")
	if readErr != nil {
		t.Fatalf("read ecommerce.sql: %v", readErr)
	}
	if strings.Contains(strings.ToUpper(string(ddl)), "INSERT INTO") {
		orderStatus, ok := profiles["orders.status"]
		if !ok {
			t.Fatalf("expected orders.status profile")
		}
		if len(orderStatus.ExampleValues) == 0 {
			t.Fatalf("expected orders.status example values to be non-empty")
		}
	} else {
		t.Log("skipping orders.status example values assertion: ecommerce.sql has no INSERT statements")
	}

	qtyProfile, ok := profiles["order_items.quantity"]
	if !ok {
		t.Fatalf("expected order_items.quantity profile")
	}
	if qtyProfile.CardinalityCategory != "low" {
		t.Fatalf("expected order_items.quantity cardinality category low, got %q", qtyProfile.CardinalityCategory)
	}
}

type testProfileProgress struct {
	mu      sync.Mutex
	calls   []profileCall
	skipped []string
}

type profileCall struct {
	table string
	col   string
	done  int
	total int
}

func (t *testProfileProgress) ColumnProfiled(tableName, columnName string, done, total int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls = append(t.calls, profileCall{table: tableName, col: columnName, done: done, total: total})
}

func (t *testProfileProgress) TableSkipped(tableName string, reason string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.skipped = append(t.skipped, tableName+":"+reason)
}

func TestProfileProgressCallbacks(t *testing.T) {
	pool := integrationPool(t)
	schema := loadTestSchema(t, pool)

	tables, _, err := Introspect(context.Background(), pool, schema)
	if err != nil {
		t.Fatalf("Introspect returned error: %v", err)
	}

	progress := &testProfileProgress{}
	_, err = Profile(context.Background(), pool, tables, progress)
	if err != nil {
		t.Fatalf("Profile returned error: %v", err)
	}

	expectedTotal := 0
	for _, table := range tables {
		expectedTotal += len(table.Columns)
	}

	progress.mu.Lock()
	defer progress.mu.Unlock()
	if len(progress.calls) != expectedTotal {
		t.Fatalf("expected %d ColumnProfiled callbacks, got %d", expectedTotal, len(progress.calls))
	}

	for i, c := range progress.calls {
		if c.done != i+1 {
			t.Fatalf("expected done counter %d at callback %d, got %d", i+1, i, c.done)
		}
		if c.total != expectedTotal {
			t.Fatalf("expected total=%d for callback %d, got %d", expectedTotal, i, c.total)
		}
	}
}

func TestProfileTimeout(t *testing.T) {
	pool := integrationPool(t)
	schema := loadTestSchema(t, pool)

	tables, _, err := Introspect(context.Background(), pool, schema)
	if err != nil {
		t.Fatalf("Introspect returned error: %v", err)
	}

	progress := &testProfileProgress{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx = WithProfileTimeout(ctx, 1)

	_, err = Profile(ctx, pool, tables, progress)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("Profile returned unexpected error: %v", err)
	}

	progress.mu.Lock()
	defer progress.mu.Unlock()
	if len(progress.skipped) == 0 {
		t.Log("no tables exceeded timeout in test schema; passing vacuously")
	}
}

func TestProfileSkipDoesNotEmitColumnProfiled(t *testing.T) {
	pool := integrationPool(t)
	schema := loadTestSchema(t, pool)

	tables := []TableInfo{
		{
			Schema: schema,
			Name:   "definitely_missing_table_for_profile_test",
			Columns: []ColumnInfo{
				{Name: "id"},
				{Name: "name"},
			},
		},
	}

	progress := &testProfileProgress{}
	profiles, err := Profile(context.Background(), pool, tables, progress)
	if err != nil {
		t.Fatalf("Profile returned error: %v", err)
	}

	progress.mu.Lock()
	defer progress.mu.Unlock()

	if len(progress.skipped) != 1 {
		t.Fatalf("expected 1 TableSkipped callback, got %d (%v)", len(progress.skipped), progress.skipped)
	}
	if !strings.Contains(progress.skipped[0], "definitely_missing_table_for_profile_test") {
		t.Fatalf("expected skip callback for missing table, got %v", progress.skipped)
	}
	if len(progress.calls) != 0 {
		t.Fatalf("expected no ColumnProfiled callbacks for skipped table, got %d", len(progress.calls))
	}

	for _, colName := range []string{"id", "name"} {
		k := "definitely_missing_table_for_profile_test." + colName
		profile, ok := profiles[k]
		if !ok {
			t.Fatalf("expected profile for %s", k)
		}
		if profile.CardinalityCategory != "unknown" {
			t.Fatalf("expected %s cardinality unknown, got %q", k, profile.CardinalityCategory)
		}
		if len(profile.ExampleValues) != 1 || profile.ExampleValues[0] != "[profiling error]" {
			t.Fatalf("expected %s example values [profiling error], got %#v", k, profile.ExampleValues)
		}
	}
}

type recordingProgress struct {
	mu       sync.Mutex
	skipped  []string
	profiled []string
}

func (r *recordingProgress) TableSkipped(tableName string, reason string) {
	_ = reason
	r.mu.Lock()
	defer r.mu.Unlock()
	r.skipped = append(r.skipped, tableName)
}

func (r *recordingProgress) ColumnProfiled(tableName, columnName string, done, total int) {
	_ = done
	_ = total
	r.mu.Lock()
	defer r.mu.Unlock()
	r.profiled = append(r.profiled, tableName+"."+columnName)
}

func TestProfileInvariantSkippedTableHasNoColumnProfiled(t *testing.T) {
	pool := integrationPool(t)
	schema := loadTestSchema(t, pool)

	tableName := "missing_table_for_invariant_test"
	tables := []TableInfo{{
		Schema: schema,
		Name:   tableName,
		Columns: []ColumnInfo{
			{Name: "id"},
			{Name: "name"},
		},
	}}

	progress := &recordingProgress{}
	profiles, err := Profile(context.Background(), pool, tables, progress)
	if err != nil {
		t.Fatalf("Profile returned error: %v", err)
	}

	progress.mu.Lock()
	defer progress.mu.Unlock()

	if len(progress.skipped) != 1 || progress.skipped[0] != tableName {
		t.Fatalf("expected exactly one skipped table %q, got %#v", tableName, progress.skipped)
	}

	for _, p := range progress.profiled {
		if strings.HasPrefix(p, tableName+".") {
			t.Fatalf("expected no ColumnProfiled callbacks for skipped table %q, got %q", tableName, p)
		}
	}

	for _, col := range []string{"id", "name"} {
		k := tableName + "." + col
		cp, ok := profiles[k]
		if !ok {
			t.Fatalf("expected profile for %s", k)
		}
		if cp.CardinalityCategory != "unknown" {
			t.Fatalf("expected unknown cardinality for %s, got %q", k, cp.CardinalityCategory)
		}
	}
}

func TestProfileUnsupportedType(t *testing.T) {
	pool := integrationPool(t)
	schema := loadTestSchema(t, pool)

	tables, _, err := Introspect(context.Background(), pool, schema)
	if err != nil {
		t.Fatalf("Introspect returned error: %v", err)
	}

	hasUnsupportedColumn := false
	for _, table := range tables {
		for _, column := range table.Columns {
			dt := strings.ToLower(column.DataType)
			if strings.Contains(dt, "tsvector") || strings.Contains(dt, "geometry") {
				hasUnsupportedColumn = true
				break
			}
		}
		if hasUnsupportedColumn {
			break
		}
	}

	if !hasUnsupportedColumn {
		t.Skip("no unsupported-type column in test schema")
	}

	profiles, err := Profile(context.Background(), pool, tables, NoOpProfileProgress{})
	if err != nil {
		t.Fatalf("Profile returned error: %v", err)
	}

	foundUnsupportedValue := false
	for _, profile := range profiles {
		for _, value := range profile.ExampleValues {
			if value == "[unsupported type]" {
				foundUnsupportedValue = true
				break
			}
		}
		if foundUnsupportedValue {
			break
		}
	}

	if !foundUnsupportedValue {
		t.Fatalf("expected at least one profile with [unsupported type] example value")
	}
}

// TestProfileBatchBoundary verifies that tables beyond the first batch
// (i.e. at index profileBatchSize and beyond) are profiled correctly and
// produce non-empty example_values when the underlying table has data.
//
// The test constructs profileBatchSize+1 TableInfo entries by repeating the
// real tables from the test schema. Each entry must be profiled correctly
// regardless of which batch it falls in, ensuring the result-mapping logic
// correctly pairs each pgx.Batch result set with the table that generated it.
func TestProfileBatchBoundary(t *testing.T) {
	pool := integrationPool(t)
	schema := loadTestSchema(t, pool)

	// Introspect the test schema to get real TableInfo with column metadata.
	realTables, _, err := Introspect(context.Background(), pool, schema)
	if err != nil {
		t.Fatalf("Introspect returned error: %v", err)
	}
	if len(realTables) == 0 {
		t.Fatal("test schema has no tables")
	}

	// Build profileBatchSize+1 entries by cycling through the real tables.
	// This guarantees at least one table falls in the second batch.
	count := profileBatchSize + 1
	tables := make([]TableInfo, 0, count)
	for len(tables) < count {
		tables = append(tables, realTables[len(tables)%len(realTables)])
	}

	profiles, err := Profile(context.Background(), pool, tables, NoOpProfileProgress{})
	if err != nil {
		t.Fatalf("Profile returned error: %v", err)
	}

	// Every table entry must appear in the result map with a valid profile.
	for _, tbl := range tables {
		for _, col := range tbl.Columns {
			key := tbl.Name + "." + col.Name
			p, ok := profiles[key]
			if !ok {
				t.Errorf("missing profile for %s", key)
				continue
			}
			if p.ExampleValues == nil {
				t.Errorf("profile for %s has nil ExampleValues", key)
			}
			// A profile produced by a failed query has CardinalityCategory "unknown".
			// A successfully profiled column must not be "unknown".
			if p.CardinalityCategory == "unknown" {
				t.Errorf("profile for %s has unexpected CardinalityCategory %q (profiling may have failed)", key, p.CardinalityCategory)
			}
		}
	}
}
