package localconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	want := Config{DB: "postgres://user:pass@localhost:5432/db", Schema: "analytics"}

	if err := Write(dir, want); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	got, err := Read(dir)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got == nil {
		t.Fatal("Read() got nil config")
	}
	if got.DB != want.DB {
		t.Fatalf("DB = %q, want %q", got.DB, want.DB)
	}
	if got.Schema != want.Schema {
		t.Fatalf("Schema = %q, want %q", got.Schema, want.Schema)
	}
}

func TestReadMissingFile(t *testing.T) {
	dir := t.TempDir()

	got, err := Read(dir)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got != nil {
		t.Fatalf("Read() = %#v, want nil", got)
	}
}

func TestEnsureGitignored_Creates(t *testing.T) {
	dir := t.TempDir()

	if err := EnsureGitignored(dir); err != nil {
		t.Fatalf("EnsureGitignored() error = %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, ".gitignore"))
	if err != nil {
		t.Fatalf("ReadFile(.gitignore) error = %v", err)
	}

	if string(data) != ".strata\n" {
		t.Fatalf(".gitignore = %q, want %q", string(data), ".strata\\n")
	}
}

func TestEnsureGitignored_Appends(t *testing.T) {
	dir := t.TempDir()
	gitignorePath := filepath.Join(dir, ".gitignore")
	if err := os.WriteFile(gitignorePath, []byte("node_modules\n.DS_Store\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(.gitignore) error = %v", err)
	}

	if err := EnsureGitignored(dir); err != nil {
		t.Fatalf("EnsureGitignored() error = %v", err)
	}

	data, err := os.ReadFile(gitignorePath)
	if err != nil {
		t.Fatalf("ReadFile(.gitignore) error = %v", err)
	}

	got := string(data)
	if !strings.Contains(got, "node_modules\n") || !strings.Contains(got, ".DS_Store\n") {
		t.Fatalf(".gitignore unexpectedly changed existing entries: %q", got)
	}
	if !strings.HasSuffix(got, ".strata\n") {
		t.Fatalf(".gitignore missing trailing .strata entry: %q", got)
	}
}

func TestEnsureGitignored_Idempotent(t *testing.T) {
	dir := t.TempDir()

	if err := EnsureGitignored(dir); err != nil {
		t.Fatalf("EnsureGitignored() first call error = %v", err)
	}
	if err := EnsureGitignored(dir); err != nil {
		t.Fatalf("EnsureGitignored() second call error = %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, ".gitignore"))
	if err != nil {
		t.Fatalf("ReadFile(.gitignore) error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	count := 0
	for _, line := range lines {
		if line == ".strata" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf(".strata entries = %d, want 1 in %q", count, string(data))
	}
}
