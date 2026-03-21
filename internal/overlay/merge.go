package overlay

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/strata-spec/openstrata/internal/smif"
)

// ApplyOverlay merges corrections into model, applying the trust hierarchy.
// This function operates on an in-memory copy of the model only.
// It MUST NOT write to any file.
//
// Trust hierarchy (highest to lowest):
//
//	user_defined > catalog_import > log_inferred > llm_inferred > schema_constraint
//
// NOTE: This hierarchy applies to in-memory model construction ONLY.
// It MUST NOT influence any write to semantic.yaml.
//
// Corrections with source: llm_suggested and status: pending are
// loaded but NEVER applied to the returned model.
func ApplyOverlay(model *smif.SemanticModel, corrections *CorrectionsFile) (*smif.SemanticModel, error) {
	if model == nil {
		return nil, fmt.Errorf("model is nil")
	}

	b, err := json.Marshal(model)
	if err != nil {
		return nil, fmt.Errorf("deep copy marshal: %w", err)
	}

	var out smif.SemanticModel
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("deep copy unmarshal: %w", err)
	}

	if corrections == nil {
		return &out, nil
	}

	for _, c := range corrections.Corrections {
		if c.Status != "approved" || c.Source == "llm_suggested" {
			continue
		}

		applyCorrection(&out, c)
	}

	return &out, nil
}

func applyCorrection(model *smif.SemanticModel, c Correction) {
	newValue := strings.TrimSpace(fmt.Sprint(c.NewValue))

	switch c.CorrectionType {
	case "description_override":
		applyDescriptionOverride(model, c, newValue)
	case "label_override":
		applyLabelOverride(model, c, newValue)
	case "role_override":
		if m, col := findColumnByTarget(model, c.TargetType, c.TargetID); col != nil {
			col.Role = newValue
			col.Provenance.SourceType = "user_defined"
			_ = m
		}
	case "join_override":
		if rel := findRelationship(model, c.TargetType, c.TargetID); rel != nil {
			rel.JoinCondition = newValue
			rel.Provenance.SourceType = "user_defined"
		}
	case "suppress":
		applySuppress(model, c)
	case "grain_override":
		if m := findModel(model, c.TargetType, c.TargetID); m != nil {
			m.Grain = newValue
			m.Provenance.SourceType = "user_defined"
		}
	case "example_values_override":
		if _, col := findColumnByTarget(model, c.TargetType, c.TargetID); col != nil {
			if newValue == "" {
				col.ExampleValues = nil
			} else {
				col.ExampleValues = strings.Split(newValue, ", ")
			}
			col.Provenance.SourceType = "user_defined"
		}
	case "confidence_override":
		if _, col := findColumnByTarget(model, c.TargetType, c.TargetID); col != nil {
			f, err := strconv.ParseFloat(newValue, 64)
			if err == nil {
				col.Provenance.Confidence = f
				col.Provenance.SourceType = "user_defined"
			}
		}
	case "required_filter_add":
		if metric := findMetric(model, c.TargetType, c.TargetID); metric != nil {
			metric.RequiredFilters = append(metric.RequiredFilters, newValue)
			metric.Provenance.SourceType = "user_defined"
		}
	}
}

func applyDescriptionOverride(model *smif.SemanticModel, c Correction, value string) {
	switch c.TargetType {
	case "domain":
		model.Domain.Description = value
		model.Domain.Provenance.SourceType = "user_defined"
	case "model":
		if m := findModel(model, c.TargetType, c.TargetID); m != nil {
			m.Description = value
			m.Provenance.SourceType = "user_defined"
		}
	case "column":
		if _, col := findColumnByTarget(model, c.TargetType, c.TargetID); col != nil {
			col.Description = value
			col.Provenance.SourceType = "user_defined"
		}
	case "metric":
		if metric := findMetric(model, c.TargetType, c.TargetID); metric != nil {
			metric.Description = value
			metric.Provenance.SourceType = "user_defined"
		}
	}
}

func applyLabelOverride(model *smif.SemanticModel, c Correction, value string) {
	switch c.TargetType {
	case "model":
		if m := findModel(model, c.TargetType, c.TargetID); m != nil {
			m.Label = value
			m.Provenance.SourceType = "user_defined"
		}
	case "column":
		if _, col := findColumnByTarget(model, c.TargetType, c.TargetID); col != nil {
			col.Label = value
			col.Provenance.SourceType = "user_defined"
		}
	}
}

func applySuppress(model *smif.SemanticModel, c Correction) {
	switch c.TargetType {
	case "model":
		if m := findModel(model, c.TargetType, c.TargetID); m != nil {
			m.Suppressed = true
			m.Provenance.SourceType = "user_defined"
		}
	case "column":
		m, col := findColumnByTarget(model, c.TargetType, c.TargetID)
		if col == nil || m == nil {
			return
		}
		col.Suppressed = true
		col.Provenance.SourceType = "user_defined"
		for i := range model.Relationships {
			r := &model.Relationships[i]
			if (r.FromModel == m.ModelID && r.FromColumn == col.Name) || (r.ToModel == m.ModelID && r.ToColumn == col.Name) {
				r.Suppressed = true
				r.Provenance.SourceType = "user_defined"
			}
		}
	case "relationship":
		if rel := findRelationship(model, c.TargetType, c.TargetID); rel != nil {
			rel.Suppressed = true
			rel.Provenance.SourceType = "user_defined"
		}
	case "metric":
		if metric := findMetric(model, c.TargetType, c.TargetID); metric != nil {
			metric.Suppressed = true
			metric.Provenance.SourceType = "user_defined"
		}
	}
}

func findModel(model *smif.SemanticModel, targetType string, targetID string) *smif.Model {
	if targetType != "model" {
		return nil
	}
	for i := range model.Models {
		if model.Models[i].ModelID == targetID {
			return &model.Models[i]
		}
	}
	return nil
}

func findColumnByTarget(model *smif.SemanticModel, targetType string, targetID string) (*smif.Model, *smif.Column) {
	if targetType != "column" {
		return nil, nil
	}
	parts := strings.SplitN(targetID, ".", 2)
	if len(parts) != 2 {
		return nil, nil
	}
	modelID := strings.TrimSpace(parts[0])
	columnName := strings.TrimSpace(parts[1])
	if modelID == "" || columnName == "" {
		return nil, nil
	}

	for i := range model.Models {
		m := &model.Models[i]
		if m.ModelID != modelID {
			continue
		}
		for j := range m.Columns {
			if m.Columns[j].Name == columnName {
				return m, &m.Columns[j]
			}
		}
	}
	return nil, nil
}

func findRelationship(model *smif.SemanticModel, targetType string, targetID string) *smif.Relationship {
	if targetType != "relationship" {
		return nil
	}
	for i := range model.Relationships {
		if model.Relationships[i].RelationshipID == targetID {
			return &model.Relationships[i]
		}
	}
	return nil
}

func findMetric(model *smif.SemanticModel, targetType string, targetID string) *smif.Metric {
	if targetType != "metric" {
		return nil
	}
	for i := range model.Metrics {
		if model.Metrics[i].Name == targetID {
			return &model.Metrics[i]
		}
	}
	return nil
}
