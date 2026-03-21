package inference

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestLoadStrataMD(t *testing.T) {
	t.Parallel()

	t.Run("file does not exist", func(t *testing.T) {
		content, found, err := Load(filepath.Join(t.TempDir(), "missing-strata.md"))
		if err != nil {
			t.Fatalf("Load returned error: %v", err)
		}
		if found {
			t.Fatalf("expected found=false")
		}
		if content != "" {
			t.Fatalf("expected empty content, got %q", content)
		}
	})

	t.Run("file exists under limit", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "strata.md")
		want := "domain context for ecommerce"
		if err := os.WriteFile(path, []byte(want), 0o644); err != nil {
			t.Fatalf("write file: %v", err)
		}

		content, found, err := Load(path)
		if err != nil {
			t.Fatalf("Load returned error: %v", err)
		}
		if !found {
			t.Fatalf("expected found=true")
		}
		if content != want {
			t.Fatalf("content mismatch: got %q want %q", content, want)
		}
	})

	t.Run("file exists over limit", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "strata.md")
		data := strings.Repeat("a", maxStrataMDBytes+50)
		if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
			t.Fatalf("write file: %v", err)
		}

		content, found, err := Load(path)
		if err != nil {
			t.Fatalf("Load returned error: %v", err)
		}
		if !found {
			t.Fatalf("expected found=true")
		}
		if !strings.HasSuffix(content, strataMDTruncationWarning) {
			t.Fatalf("expected truncation warning suffix")
		}
		expectedLen := maxStrataMDBytes + len(strataMDTruncationWarning)
		if len(content) != expectedLen {
			t.Fatalf("expected truncated length %d, got %d", expectedLen, len(content))
		}
	})

	t.Run("file unreadable", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("chmod-based unreadable test is not portable on windows")
		}

		path := filepath.Join(t.TempDir(), "strata.md")
		if err := os.WriteFile(path, []byte("secret"), 0o600); err != nil {
			t.Fatalf("write file: %v", err)
		}
		if err := os.Chmod(path, 0); err != nil {
			t.Fatalf("chmod 0: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(path, 0o600) })

		content, found, err := Load(path)
		if err == nil {
			t.Fatalf("expected error for unreadable file")
		}
		if found {
			t.Fatalf("expected found=false on error")
		}
		if content != "" {
			t.Fatalf("expected empty content on error")
		}
	})
}
