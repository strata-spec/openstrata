package smif

import (
	"encoding/json"
	"os"

	"gopkg.in/yaml.v3"
)

// ReadYAML reads a SMIF semantic.yaml file from disk.
func ReadYAML(path string) (*SemanticModel, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var model SemanticModel
	if err := yaml.Unmarshal(b, &model); err != nil {
		return nil, err
	}
	return &model, nil
}

// WriteYAML writes a SMIF semantic model to YAML.
func WriteYAML(path string, model *SemanticModel) error {
	b, err := yaml.Marshal(model)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// WriteJSON writes a SMIF semantic model to JSON.
func WriteJSON(path string, model *SemanticModel) error {
	b, err := json.MarshalIndent(model, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
