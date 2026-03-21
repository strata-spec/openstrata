//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	mcp "github.com/mark3labs/mcp-go/mcp"
	"github.com/strata-spec/openstrata/internal/mcp/tools"
	"github.com/strata-spec/openstrata/internal/overlay"
	"github.com/strata-spec/openstrata/internal/smif"
)

// repoRoot returns the absolute path to the module root.
// The test binary runs from the test package directory, so we walk up.
func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	// file is .../test/integration/e2e_test.go — two levels up
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// --------------------------------------------------------------------------
// Binary management — build once per test run.
// --------------------------------------------------------------------------

var (
	binaryOnce sync.Once
	binaryPath string
	binaryErr  error
)

// strataPath returns the path to the compiled strata binary, building it on
// the first call and caching the result for all subsequent calls.
func strataPath(t *testing.T) string {
	t.Helper()
	binaryOnce.Do(func() {
		dir, err := os.MkdirTemp("", "strata-e2e-bin-*")
		if err != nil {
			binaryErr = fmt.Errorf("create binary temp dir: %w", err)
			return
		}
		out := filepath.Join(dir, "strata")
		cmd := exec.Command("go", "build", "-o", out, ".")
		cmd.Dir = repoRoot()
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			binaryErr = fmt.Errorf("build strata binary: %w", err)
			return
		}
		binaryPath = out
	})
	if binaryErr != nil {
		t.Fatalf("strata binary build failed: %v", binaryErr)
	}
	return binaryPath
}

// --------------------------------------------------------------------------
// Test helpers
// --------------------------------------------------------------------------

// setupWorkspace creates a temp directory and returns it along with the DSN.
// The test is skipped if STRATA_TEST_DSN is not set.
func setupWorkspace(t *testing.T) (dir string, dsn string) {
	t.Helper()
	dsn = os.Getenv("STRATA_TEST_DSN")
	if dsn == "" {
		t.Skip("STRATA_TEST_DSN not set")
	}
	dir = t.TempDir()
	return dir, dsn
}

// loadEcommerceSchema creates a temporary Postgres schema, loads the ecommerce
// DDL into it, and registers a cleanup to drop it. Returns the schema name.
func loadEcommerceSchema(t *testing.T, dsn string) string {
	t.Helper()

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}

	schemaName := "strata_e2e_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:12]

	ddlPath := filepath.Join(repoRoot(), "testdata", "schemas", "ecommerce.sql")
	ddl, err := os.ReadFile(ddlPath)
	if err != nil {
		t.Fatalf("read ecommerce.sql: %v", err)
	}

	ctx := context.Background()
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", schemaName)); err != nil {
		t.Fatalf("create test schema: %v", err)
	}

	patchedDDL := fmt.Sprintf("SET search_path TO %s;\n%s", schemaName, string(ddl))
	if _, err := pool.Exec(ctx, patchedDDL); err != nil {
		// Drop the schema we just created before failing
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
		t.Fatalf("load ecommerce DDL: %v", err)
	}

	t.Cleanup(func() {
		_, dropErr := pool.Exec(context.Background(),
			fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
		if dropErr != nil {
			t.Logf("warning: failed to drop test schema %s: %v", schemaName, dropErr)
		}
		pool.Close()
	})

	return schemaName
}

// runCLI runs the strata binary with the given args in the given working dir.
// Returns stdout, stderr, and the exit code.
func runCLI(t *testing.T, dir string, args ...string) (stdout, stderr string, exitCode int) {
	t.Helper()
	bin := strataPath(t)
	cmd := exec.Command(bin, args...)
	cmd.Dir = dir

	var outBuf, errBuf strings.Builder
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	err := cmd.Run()
	stdout = outBuf.String()
	stderr = errBuf.String()

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = 1
		}
	}
	return stdout, stderr, exitCode
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

// TestInitProducesValidSMIF runs the full init pipeline against a real
// Postgres database (using the ecommerce schema) and verifies the output files
// are present and structurally valid.
//
// Requires: STRATA_TEST_DSN, ANTHROPIC_API_KEY
func TestInitProducesValidSMIF(t *testing.T) {
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		t.Skip("ANTHROPIC_API_KEY not set")
	}

	dir, dsn := setupWorkspace(t)
	schema := loadEcommerceSchema(t, dsn)

	// Build the search_path DSN so init targets our temp schema.
	schemaDSN := dsn
	if strings.Contains(dsn, "?") {
		schemaDSN = dsn + "&search_path=" + schema
	} else {
		schemaDSN = dsn + "?search_path=" + schema
	}

	stdout, stderr, code := runCLI(t, dir, "init",
		"--db", schemaDSN,
		"--schema", schema,
		"--llm", "anthropic",
	)
	if code != 0 {
		t.Fatalf("strata init exited %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}

	semanticPath := filepath.Join(dir, "semantic.yaml")
	jsonPath := filepath.Join(dir, "semantic.json")
	correctionsPath := filepath.Join(dir, "corrections.yaml")

	for _, p := range []string{semanticPath, jsonPath, correctionsPath} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("expected output file %s to exist: %v", p, err)
		}
	}

	model, err := smif.ReadYAML(semanticPath)
	if err != nil {
		t.Fatalf("parse semantic.yaml: %v", err)
	}

	if model.SMIFVersion != "0.1.0" {
		t.Errorf("expected smif_version 0.1.0, got %q", model.SMIFVersion)
	}

	if len(model.Models) < 4 {
		t.Errorf("expected at least 4 models, got %d", len(model.Models))
	}

	// corrections.yaml must start empty
	correctionsData, err := os.ReadFile(correctionsPath)
	if err != nil {
		t.Fatalf("read corrections.yaml: %v", err)
	}
	if !strings.Contains(string(correctionsData), "corrections: []") {
		t.Errorf("expected corrections.yaml to contain 'corrections: []', got:\n%s", string(correctionsData))
	}
}

// TestValidatePassesOnInitOutput verifies that the output of strata init
// passes strata validate without any MUST violations.
//
// Requires: STRATA_TEST_DSN, ANTHROPIC_API_KEY
func TestValidatePassesOnInitOutput(t *testing.T) {
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		t.Skip("ANTHROPIC_API_KEY not set")
	}

	dir, dsn := setupWorkspace(t)
	schema := loadEcommerceSchema(t, dsn)

	schemaDSN := dsn
	if strings.Contains(dsn, "?") {
		schemaDSN = dsn + "&search_path=" + schema
	} else {
		schemaDSN = dsn + "?search_path=" + schema
	}

	stdout, stderr, code := runCLI(t, dir, "init",
		"--db", schemaDSN,
		"--schema", schema,
		"--llm", "anthropic",
	)
	if code != 0 {
		t.Fatalf("strata init exited %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}

	semanticPath := filepath.Join(dir, "semantic.yaml")

	stdout, stderr, code = runCLI(t, dir, "validate", "--semantic", semanticPath)
	if code != 0 {
		t.Errorf("strata validate exited %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "✓ semantic.yaml is valid SMIF") {
		t.Errorf("expected success message in stdout, got: %q", stdout)
	}
}

// TestCorrectAndReload applies a correction via strata correct and verifies
// that the entry is correctly written to corrections.yaml.
//
// Requires: STRATA_TEST_DSN, ANTHROPIC_API_KEY
func TestCorrectAndReload(t *testing.T) {
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		t.Skip("ANTHROPIC_API_KEY not set")
	}

	dir, dsn := setupWorkspace(t)
	schema := loadEcommerceSchema(t, dsn)

	schemaDSN := dsn
	if strings.Contains(dsn, "?") {
		schemaDSN = dsn + "&search_path=" + schema
	} else {
		schemaDSN = dsn + "?search_path=" + schema
	}

	stdout, stderr, code := runCLI(t, dir, "init",
		"--db", schemaDSN,
		"--schema", schema,
		"--llm", "anthropic",
	)
	if code != 0 {
		t.Fatalf("strata init exited %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}

	semanticPath := filepath.Join(dir, "semantic.yaml")
	correctionsPath := filepath.Join(dir, "corrections.yaml")

	correctionJSON := `{"target_type":"column","target_id":"orders.status","correction_type":"description_override","new_value":"Current fulfillment status of the order."}`

	stdout, stderr, code = runCLI(t, dir, "correct",
		"--semantic", semanticPath,
		"--corrections", correctionsPath,
		"--json", correctionJSON,
	)
	if code != 0 {
		t.Fatalf("strata correct exited %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "✓ Correction recorded") {
		t.Errorf("expected '✓ Correction recorded' in stdout, got: %q", stdout)
	}

	corrFile, err := overlay.LoadCorrections(correctionsPath)
	if err != nil {
		t.Fatalf("parse corrections.yaml: %v", err)
	}
	if len(corrFile.Corrections) != 1 {
		t.Fatalf("expected 1 correction entry, got %d", len(corrFile.Corrections))
	}

	c := corrFile.Corrections[0]
	if c.TargetID != "orders.status" {
		t.Errorf("expected target_id 'orders.status', got %q", c.TargetID)
	}
	if c.Source != "user_defined" {
		t.Errorf("expected source 'user_defined', got %q", c.Source)
	}
	if c.Status != "approved" {
		t.Errorf("expected status 'approved', got %q", c.Status)
	}
}

// TestRefreshPreservesCorrections verifies that strata init --refresh does not
// overwrite or delete existing corrections in corrections.yaml.
//
// Requires: STRATA_TEST_DSN, ANTHROPIC_API_KEY
func TestRefreshPreservesCorrections(t *testing.T) {
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		t.Skip("ANTHROPIC_API_KEY not set")
	}

	dir, dsn := setupWorkspace(t)
	schema := loadEcommerceSchema(t, dsn)

	schemaDSN := dsn
	if strings.Contains(dsn, "?") {
		schemaDSN = dsn + "&search_path=" + schema
	} else {
		schemaDSN = dsn + "?search_path=" + schema
	}

	initArgs := []string{
		"init",
		"--db", schemaDSN,
		"--schema", schema,
		"--llm", "anthropic",
	}

	// First init
	stdout, stderr, code := runCLI(t, dir, initArgs...)
	if code != 0 {
		t.Fatalf("strata init (first) exited %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}

	semanticPath := filepath.Join(dir, "semantic.yaml")
	correctionsPath := filepath.Join(dir, "corrections.yaml")

	// Apply a correction
	correctionJSON := `{"target_type":"column","target_id":"orders.status","correction_type":"description_override","new_value":"Current fulfillment status of the order."}`
	stdout, stderr, code = runCLI(t, dir, "correct",
		"--semantic", semanticPath,
		"--corrections", correctionsPath,
		"--json", correctionJSON,
	)
	if code != 0 {
		t.Fatalf("strata correct exited %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}

	// Refresh run
	refreshArgs := append(initArgs, "--refresh")
	stdout, stderr, code = runCLI(t, dir, refreshArgs...)
	if code != 0 {
		t.Fatalf("strata init --refresh exited %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}

	// The correction must still be present in corrections.yaml
	corrFile, err := overlay.LoadCorrections(correctionsPath)
	if err != nil {
		t.Fatalf("parse corrections.yaml after refresh: %v", err)
	}

	found := false
	for _, c := range corrFile.Corrections {
		if c.TargetID == "orders.status" && c.Source == "user_defined" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected correction for orders.status to be preserved after refresh; corrections.yaml has %d entries", len(corrFile.Corrections))
	}
}

// TestServeListModels verifies the list_models MCP tool handler returns the
// correct models when loaded from the ecommerce_semantic.yaml fixture.
//
// This test calls the tool handler function directly rather than spawning a
// full MCP subprocess, consistent with the unit test pattern in tools_test.go.
// A full subprocess test over stdio is omitted because wiring an MCP stdio
// transport inside a Go test requires significant scaffolding (exec.Command
// pipe management, protocol framing) without additional correctness value
// beyond what the handler-level test already provides.
func TestServeListModels(t *testing.T) {
	fixturePath := filepath.Join(repoRoot(), "testdata", "fixtures", "ecommerce_semantic.yaml")
	model, err := smif.ReadYAML(fixturePath)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	_, handler := tools.ListModels(func() *smif.SemanticModel { return model })

	var req mcp.CallToolRequest
	req.Params.Name = "list_models"

	result, err := handler(context.Background(), req)
	if err != nil {
		t.Fatalf("list_models handler error: %v", err)
	}

	if len(result.Content) == 0 {
		t.Fatalf("expected non-empty content in list_models response")
	}

	text := extractText(t, result)

	var rows []map[string]any
	if err := json.Unmarshal([]byte(text), &rows); err != nil {
		t.Fatalf("unmarshal list_models response: %v", err)
	}

	if len(rows) == 0 {
		t.Fatalf("expected non-empty model list")
	}

	// All four ecommerce models should be present
	wantModels := map[string]bool{
		"users": false, "products": false, "orders": false, "order_items": false,
	}
	for _, row := range rows {
		id, _ := row["model_id"].(string)
		wantModels[id] = true

		// Each entry must have the required fields
		for _, field := range []string{"model_id", "label", "description"} {
			if _, ok := row[field]; !ok {
				t.Errorf("model row missing field %q: %v", field, row)
			}
		}
	}
	for id, found := range wantModels {
		if !found {
			t.Errorf("expected model %q in list_models response", id)
		}
	}
}

// --------------------------------------------------------------------------
// Internal helpers
// --------------------------------------------------------------------------

// extractText pulls the first text content item from an MCP result.
func extractText(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	if res == nil || len(res.Content) == 0 {
		t.Fatalf("missing tool content")
	}
	text, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected content type %T", res.Content[0])
	}
	return text.Text
}
