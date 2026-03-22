package inference

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestNoOpProgressCompiles(t *testing.T) {
	t.Parallel()

	p := NoOpProgress{}
	done := p.Stage("test stage")
	p.Item("some item")
	p.Info("some info")
	done(nil)
	done(errors.New("boom"))
}

func TestStderrProgressStageSuccess(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	p := NewStderrProgress(&buf)

	done := p.Stage("test stage")
	done(nil)

	out := buf.String()
	if !strings.Contains(out, "► test stage") {
		t.Fatalf("expected stage start in output, got %q", out)
	}
	if !strings.Contains(out, "✓ test stage") {
		t.Fatalf("expected stage success in output, got %q", out)
	}
}

func TestStderrProgressStageFailure(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	p := NewStderrProgress(&buf)

	done := p.Stage("test stage")
	done(errors.New("boom"))

	out := buf.String()
	if !strings.Contains(out, "✗ test stage") {
		t.Fatalf("expected stage failure in output, got %q", out)
	}
}

func TestStderrProgressItem(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	p := NewStderrProgress(&buf)

	p.Item("some item")

	out := buf.String()
	if !strings.Contains(out, "some item") {
		t.Fatalf("expected item output, got %q", out)
	}
}

func TestNilProgressSafeInInit(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{Schema: "public", Progress: nil})

	if cfg.Progress == nil {
		t.Fatal("expected nil progress to be replaced")
	}
	if _, ok := cfg.Progress.(NoOpProgress); !ok {
		t.Fatalf("expected NoOpProgress substitution, got %T", cfg.Progress)
	}
}
