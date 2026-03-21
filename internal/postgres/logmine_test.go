package postgres

import (
	"context"
	"errors"
	"testing"
)

func TestExtractColumnUsage(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		query   string
		tables  []string
		columns []string
	}{
		{
			name:    "simple select",
			query:   "SELECT id, name FROM users WHERE status = $1",
			tables:  []string{"users"},
			columns: []string{"users.id", "users.name", "users.status"},
		},
		{
			name:   "join with aliases",
			query:  "SELECT u.id, o.total_usd FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = $1",
			tables: []string{"users", "orders"},
			columns: []string{
				"users.id",
				"orders.total_usd",
				"users.id",
				"orders.user_id",
				"orders.status",
			},
		},
		{
			name:    "group by",
			query:   "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
			tables:  []string{"orders"},
			columns: []string{"orders.user_id"},
		},
		{
			name:    "with clause - skipped",
			query:   "WITH cte AS (SELECT id FROM users) SELECT id FROM cte",
			tables:  []string{},
			columns: []string{},
		},
		{
			name:    "schema qualified",
			query:   "SELECT id FROM public.users WHERE active = $1",
			tables:  []string{"users"},
			columns: []string{"users.id", "users.active"},
		},
		{
			name:    "star - no column refs",
			query:   "SELECT * FROM products",
			tables:  []string{"products"},
			columns: []string{},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tables, columns, err := ExtractColumnUsage(tc.query)
			if err != nil {
				t.Fatalf("ExtractColumnUsage returned error: %v", err)
			}

			if !elementsMatch(tables, tc.tables) {
				t.Fatalf("tables mismatch: got=%v want=%v", tables, tc.tables)
			}

			if !elementsMatch(columns, tc.columns) {
				t.Fatalf("columns mismatch: got=%v want=%v", columns, tc.columns)
			}
		})
	}
}

func TestMineAvailabilityCheck(t *testing.T) {
	pool := integrationPool(t)

	err := checkAvailability(context.Background(), pool)
	if err == nil {
		t.Skip("pg_stat_statements is available in test database")
	}

	if !errors.Is(err, ErrLogMiningUnavailable) {
		t.Fatalf("expected ErrLogMiningUnavailable, got %v", err)
	}
}

func TestMineReturnsProfiles(t *testing.T) {
	pool := integrationPool(t)
	_ = loadTestSchema(t, pool)

	profiles, err := Mine(context.Background(), pool)
	if errors.Is(err, ErrLogMiningUnavailable) {
		t.Skip("pg_stat_statements not available in test database")
	}
	if err != nil {
		t.Fatalf("Mine returned error: %v", err)
	}
	if profiles == nil {
		t.Fatalf("expected non-nil []UsageProfile")
	}
}

func elementsMatch(got, want []string) bool {
	gotSet := make(map[string]struct{}, len(got))
	for _, v := range got {
		gotSet[v] = struct{}{}
	}

	wantSet := make(map[string]struct{}, len(want))
	for _, v := range want {
		wantSet[v] = struct{}{}
	}

	if len(gotSet) != len(wantSet) {
		return false
	}
	for v := range wantSet {
		if _, ok := gotSet[v]; !ok {
			return false
		}
	}
	return true
}
