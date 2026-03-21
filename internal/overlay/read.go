package overlay

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadCorrections loads corrections.yaml from disk.
func LoadCorrections(path string) (*CorrectionsFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &CorrectionsFile{}, nil
		}
		return nil, fmt.Errorf("read corrections file: %w", err)
	}

	var corrections CorrectionsFile
	if err := yaml.Unmarshal(b, &corrections); err != nil {
		return nil, fmt.Errorf("parse corrections yaml: %w", err)
	}

	return &corrections, nil
}
