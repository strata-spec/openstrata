package postgres

import (
	"context"
	"os"
	"strings"
	"testing"
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

func TestProfileEcommerce(t *testing.T) {
	pool := integrationPool(t)
	schema := loadTestSchema(t, pool)

	tables, _, err := Introspect(context.Background(), pool, schema)
	if err != nil {
		t.Fatalf("Introspect returned error: %v", err)
	}

	profiles, err := Profile(context.Background(), pool, tables)
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

	profiles, err := Profile(context.Background(), pool, tables)
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
