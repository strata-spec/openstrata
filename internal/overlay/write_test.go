package overlay

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAppendCorrectionNewFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrections.yaml")

	c := Correction{
		CorrectionID:   "corr_new",
		TargetType:     "model",
		TargetID:       "orders",
		CorrectionType: "description_override",
		NewValue:       "new description",
		Source:         "user_defined",
		Status:         "approved",
		Timestamp:      "2026-03-20T00:00:00Z",
	}

	if err := AppendCorrection(path, "0.1.0", c); err != nil {
		t.Fatalf("AppendCorrection() error = %v", err)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	content := string(b)
	if !strings.Contains(content, "smif_version: \"0.1.0\"") {
		t.Fatalf("expected smif_version header in file, got:\n%s", content)
	}
	if !strings.Contains(content, "correction_id: corr_new") {
		t.Fatalf("expected correction content in file, got:\n%s", content)
	}
}

func TestAppendCorrectionExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrections.yaml")

	first := Correction{
		CorrectionID:   "corr_first",
		TargetType:     "model",
		TargetID:       "orders",
		CorrectionType: "description_override",
		NewValue:       "first",
		Source:         "user_defined",
		Status:         "approved",
		Timestamp:      "2026-03-20T00:00:00Z",
	}
	second := Correction{
		CorrectionID:   "corr_second",
		TargetType:     "column",
		TargetID:       "orders.user_id",
		CorrectionType: "label_override",
		NewValue:       "User",
		Source:         "user_defined",
		Status:         "approved",
		Timestamp:      "2026-03-20T00:00:01Z",
	}

	if err := AppendCorrection(path, "0.1.0", first); err != nil {
		t.Fatalf("AppendCorrection(first) error = %v", err)
	}
	if err := AppendCorrection(path, "0.1.0", second); err != nil {
		t.Fatalf("AppendCorrection(second) error = %v", err)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	var got CorrectionsFile
	if err := yaml.Unmarshal(b, &got); err != nil {
		t.Fatalf("yaml.Unmarshal() error = %v", err)
	}
	if len(got.Corrections) != 2 {
		t.Fatalf("expected 2 corrections, got %d", len(got.Corrections))
	}
	if got.Corrections[0].CorrectionID != "corr_first" {
		t.Fatalf("first correction changed, got %q", got.Corrections[0].CorrectionID)
	}
}

func TestAppendCorrectionIsAppendOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrections.yaml")

	first := Correction{
		CorrectionID:   "corr_first",
		TargetType:     "model",
		TargetID:       "orders",
		CorrectionType: "description_override",
		NewValue:       "first",
		Source:         "user_defined",
		Status:         "approved",
		Timestamp:      "2026-03-20T00:00:00Z",
	}
	second := Correction{
		CorrectionID:   "corr_second",
		TargetType:     "metric",
		TargetID:       "total_orders",
		CorrectionType: "required_filter_add",
		NewValue:       "tenant_id = 1",
		Source:         "user_defined",
		Status:         "approved",
		Timestamp:      "2026-03-20T00:00:01Z",
	}

	if err := AppendCorrection(path, "0.1.0", first); err != nil {
		t.Fatalf("AppendCorrection(first) error = %v", err)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	if _, err := f.WriteString("garbage: stays\n"); err != nil {
		f.Close()
		t.Fatalf("write garbage error = %v", err)
	}
	f.Close()

	if err := AppendCorrection(path, "0.1.0", second); err != nil {
		t.Fatalf("AppendCorrection(second) error = %v", err)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	content := string(b)
	if !strings.Contains(content, "garbage: stays") {
		t.Fatalf("expected garbage text to remain in file")
	}
	if !strings.Contains(content, "correction_id: corr_second") {
		t.Fatalf("expected second correction to be appended")
	}
}
