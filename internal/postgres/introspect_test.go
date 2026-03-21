package postgres

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFingerprintFormat(t *testing.T) {
	t.Parallel()

	fp, err := Fingerprint("postgres://user:secret@localhost:5432/appdb?sslmode=disable")
	if err != nil {
		t.Fatalf("Fingerprint returned error: %v", err)
	}

	if len(fp) != 16 {
		t.Fatalf("expected fingerprint length 16, got %d (%q)", len(fp), fp)
	}

	if !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(fp) {
		t.Fatalf("fingerprint has invalid format: %q", fp)
	}
}

func TestFingerprintStability(t *testing.T) {
	t.Parallel()

	dsn := "postgres://user:secret@localhost:5432/appdb?sslmode=disable"
	fp1, err := Fingerprint(dsn)
	if err != nil {
		t.Fatalf("first Fingerprint call returned error: %v", err)
	}

	fp2, err := Fingerprint(dsn)
	if err != nil {
		t.Fatalf("second Fingerprint call returned error: %v", err)
	}

	if fp1 != fp2 {
		t.Fatalf("expected stable fingerprint, got %q and %q", fp1, fp2)
	}
}

func TestFingerprintCredentialIsolation(t *testing.T) {
	t.Parallel()

	dsnA := "postgres://user:passwordA@db.example.com:5432/appdb?sslmode=disable"
	dsnB := "postgres://user:passwordB@db.example.com:5432/appdb?sslmode=disable"

	fpA, err := Fingerprint(dsnA)
	if err != nil {
		t.Fatalf("Fingerprint(dsnA) returned error: %v", err)
	}

	fpB, err := Fingerprint(dsnB)
	if err != nil {
		t.Fatalf("Fingerprint(dsnB) returned error: %v", err)
	}

	if fpA != fpB {
		t.Fatalf("expected same fingerprint for DSNs differing only by password, got %q and %q", fpA, fpB)
	}
}

func TestFingerprintHostSensitivity(t *testing.T) {
	t.Parallel()

	dsnA := "postgres://user:secret@db-a.example.com:5432/appdb?sslmode=disable"
	dsnB := "postgres://user:secret@db-b.example.com:5432/appdb?sslmode=disable"

	fpA, err := Fingerprint(dsnA)
	if err != nil {
		t.Fatalf("Fingerprint(dsnA) returned error: %v", err)
	}

	fpB, err := Fingerprint(dsnB)
	if err != nil {
		t.Fatalf("Fingerprint(dsnB) returned error: %v", err)
	}

	if fpA == fpB {
		t.Fatalf("expected different fingerprints for different hosts, both were %q", fpA)
	}
}

func integrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("STRATA_TEST_DSN")
	if dsn == "" {
		t.Skip("STRATA_TEST_DSN not set")
	}

	pool, err := Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	t.Cleanup(func() { pool.Close() })
	return pool
}

func loadTestSchema(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()

	schemaName := fmt.Sprintf("strata_test_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:12])

	ddl, err := os.ReadFile("../../testdata/schemas/ecommerce.sql")
	if err != nil {
		t.Fatalf("read ecommerce.sql: %v", err)
	}

	ctx := context.Background()

	_, err = pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", schemaName))
	if err != nil {
		t.Fatalf("create test schema: %v", err)
	}

	t.Cleanup(func() {
		_, dropErr := pool.Exec(context.Background(),
			fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
		if dropErr != nil {
			t.Logf("warning: failed to drop test schema %s: %v", schemaName, dropErr)
		}
	})

	patchedDDL := fmt.Sprintf("SET search_path TO %s;\n%s", schemaName, string(ddl))
	_, err = pool.Exec(ctx, patchedDDL)
	if err != nil {
		t.Fatalf("load ecommerce DDL into test schema: %v", err)
	}

	return schemaName
}

func TestIntrospectEcommerce(t *testing.T) {
	pool := integrationPool(t)
	schema := loadTestSchema(t, pool)

	tables, err := Introspect(context.Background(), pool, schema)
	if err != nil {
		t.Fatalf("Introspect returned error: %v", err)
	}

	if len(tables) != 4 {
		t.Fatalf("expected 4 tables, got %d", len(tables))
	}

	byName := make(map[string]TableInfo, len(tables))
	for _, table := range tables {
		byName[table.Name] = table
	}

	for _, expected := range []string{"users", "products", "orders", "order_items"} {
		if _, ok := byName[expected]; !ok {
			t.Fatalf("expected table %q to be present", expected)
		}
	}

	orders := byName["orders"]
	ordersToUsersFound := false
	for _, fk := range orders.ForeignKeys {
		if fk.ToTable == "users" {
			ordersToUsersFound = true
			if len(fk.FromColumns) != 1 || fk.FromColumns[0] != "user_id" {
				t.Fatalf("orders->users FK from columns mismatch, got %#v", fk.FromColumns)
			}
			if len(fk.ToColumns) != 1 || fk.ToColumns[0] != "id" {
				t.Fatalf("orders->users FK to columns mismatch, got %#v", fk.ToColumns)
			}
		}
	}
	if !ordersToUsersFound {
		t.Fatalf("expected orders FK to users")
	}

	orderItems := byName["order_items"]
	hasOrdersFK := false
	hasProductsFK := false
	for _, fk := range orderItems.ForeignKeys {
		if fk.ToTable == "orders" {
			hasOrdersFK = true
		}
		if fk.ToTable == "products" {
			hasProductsFK = true
		}
	}
	if !hasOrdersFK || !hasProductsFK {
		t.Fatalf("expected order_items to have FKs to orders and products")
	}

	users := byName["users"]
	usersEmailFound := false
	for _, col := range users.Columns {
		if col.Name == "email" {
			usersEmailFound = true
			if col.IsNullable {
				t.Fatalf("expected users.email to be not nullable")
			}
		}
	}
	if !usersEmailFound {
		t.Fatalf("expected users.email column to be present")
	}

	products := byName["products"]
	priceFound := false
	for _, col := range products.Columns {
		if col.Name == "price_usd" {
			priceFound = true
			dt := strings.ToLower(col.DataType)
			if !strings.Contains(dt, "numeric") && !strings.Contains(dt, "decimal") {
				t.Fatalf("expected products.price_usd type to contain numeric or decimal, got %q", col.DataType)
			}
		}
	}
	if !priceFound {
		t.Fatalf("expected products.price_usd column to be present")
	}

	if len(users.PrimaryKey) != 1 || users.PrimaryKey[0] != "id" {
		t.Fatalf("expected users primary key [id], got %#v", users.PrimaryKey)
	}

	ddl, err := os.ReadFile("../../testdata/schemas/ecommerce.sql")
	if err != nil {
		t.Fatalf("read ecommerce.sql for comment assertion: %v", err)
	}
	if strings.Contains(string(ddl), "COMMENT ON TABLE public.orders IS") {
		expectedComment := "Customer orders placed in the storefront."
		if orders.Comment != expectedComment {
			t.Fatalf("expected orders comment %q, got %q", expectedComment, orders.Comment)
		}
	}
}

func TestIntrospectEmptySchema(t *testing.T) {
	pool := integrationPool(t)

	schemaName := fmt.Sprintf("strata_test_empty_%s", strings.ReplaceAll(uuid.New().String(), "-", "")[:12])

	_, err := pool.Exec(context.Background(), fmt.Sprintf("CREATE SCHEMA %s", schemaName))
	if err != nil {
		t.Fatalf("create empty schema: %v", err)
	}

	t.Cleanup(func() {
		_, dropErr := pool.Exec(context.Background(),
			fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
		if dropErr != nil {
			t.Logf("warning: failed to drop test schema %s: %v", schemaName, dropErr)
		}
	})

	tables, err := Introspect(context.Background(), pool, schemaName)
	if err != nil {
		t.Fatalf("Introspect returned error for empty schema: %v", err)
	}

	if len(tables) != 0 {
		t.Fatalf("expected 0 tables for empty schema, got %d", len(tables))
	}
}

func TestIntrospectNonexistentSchema(t *testing.T) {
	pool := integrationPool(t)

	_, err := Introspect(context.Background(), pool, "strata_definitely_does_not_exist")
	if err == nil {
		t.Fatalf("expected error for nonexistent schema")
	}

	if !strings.Contains(strings.ToLower(err.Error()), "not found") {
		t.Fatalf("expected error to contain %q, got %q", "not found", err.Error())
	}
}
