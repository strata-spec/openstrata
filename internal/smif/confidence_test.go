package smif

import "testing"

func TestConfidenceDefaultStrataMD(t *testing.T) {
	t.Parallel()

	got := Default("strata_md", "")
	if got != 0.95 {
		t.Fatalf("expected strata_md default confidence 0.95, got %v", got)
	}
}
