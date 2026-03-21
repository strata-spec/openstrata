package mcp

import (
	"fmt"

	"github.com/strata-spec/openstrata/internal/overlay"
	"github.com/strata-spec/openstrata/internal/smif"
)

// Load reads semantic.yaml and corrections.yaml, applies the overlay,
// and returns the merged in-memory model ready for serving.
// If corrections.yaml does not exist, returns the semantic model unchanged.
func Load(semanticPath, correctionsPath string) (*smif.SemanticModel, error) {
	model, err := smif.ReadYAML(semanticPath)
	if err != nil {
		return nil, fmt.Errorf("load semantic model: %w", err)
	}

	corrections, err := overlay.LoadCorrections(correctionsPath)
	if err != nil {
		return nil, fmt.Errorf("load corrections: %w", err)
	}

	merged, err := overlay.ApplyOverlay(model, corrections)
	if err != nil {
		return nil, fmt.Errorf("apply overlay: %w", err)
	}

	return merged, nil
}
