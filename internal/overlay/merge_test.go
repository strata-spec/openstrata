package overlay

import (
	"testing"

	"github.com/strata-spec/openstrata/internal/smif"
)

func TestApplyOverlayDescriptionOverride(t *testing.T) {
	original := overlayBaseModel()
	corrections := &CorrectionsFile{
		SMIFVersion: "0.1.0",
		Corrections: []Correction{
			{
				CorrectionID:   "corr_desc",
				TargetType:     "column",
				TargetID:       "orders.user_id",
				CorrectionType: "description_override",
				NewValue:       "corrected",
				Source:         "user_defined",
				Status:         "approved",
			},
		},
	}

	merged, err := ApplyOverlay(original, corrections)
	if err != nil {
		t.Fatalf("ApplyOverlay() error = %v", err)
	}

	if merged.Models[0].Columns[1].Description != "corrected" {
		t.Fatalf("expected merged description corrected, got %q", merged.Models[0].Columns[1].Description)
	}
	if original.Models[0].Columns[1].Description != "original" {
		t.Fatalf("expected original model unchanged, got %q", original.Models[0].Columns[1].Description)
	}
}

func TestApplyOverlaySuppress(t *testing.T) {
	model := overlayBaseModel()
	corrections := &CorrectionsFile{
		SMIFVersion: "0.1.0",
		Corrections: []Correction{
			{
				CorrectionID:   "corr_suppress",
				TargetType:     "column",
				TargetID:       "orders.user_id",
				CorrectionType: "suppress",
				NewValue:       true,
				Source:         "user_defined",
				Status:         "approved",
			},
		},
	}

	merged, err := ApplyOverlay(model, corrections)
	if err != nil {
		t.Fatalf("ApplyOverlay() error = %v", err)
	}

	if !merged.Models[0].Columns[1].Suppressed {
		t.Fatalf("expected orders.user_id to be suppressed")
	}
	if !merged.Relationships[0].Suppressed {
		t.Fatalf("expected relationship referencing orders.user_id to be suppressed")
	}
}

func TestApplyOverlayPendingNotApplied(t *testing.T) {
	model := overlayBaseModel()
	corrections := &CorrectionsFile{
		SMIFVersion: "0.1.0",
		Corrections: []Correction{
			{
				CorrectionID:   "corr_pending",
				TargetType:     "column",
				TargetID:       "orders.user_id",
				CorrectionType: "description_override",
				NewValue:       "pending change",
				Source:         "llm_suggested",
				Status:         "pending",
			},
		},
	}

	merged, err := ApplyOverlay(model, corrections)
	if err != nil {
		t.Fatalf("ApplyOverlay() error = %v", err)
	}

	if merged.Models[0].Columns[1].Description != "original" {
		t.Fatalf("expected pending llm suggestion to be ignored")
	}
}

func TestApplyOverlayRoleOverride(t *testing.T) {
	model := overlayBaseModel()
	corrections := &CorrectionsFile{
		SMIFVersion: "0.1.0",
		Corrections: []Correction{
			{
				CorrectionID:   "corr_role",
				TargetType:     "column",
				TargetID:       "orders.user_id",
				CorrectionType: "role_override",
				NewValue:       "measure",
				Source:         "user_defined",
				Status:         "approved",
			},
		},
	}

	merged, err := ApplyOverlay(model, corrections)
	if err != nil {
		t.Fatalf("ApplyOverlay() error = %v", err)
	}

	if merged.Models[0].Columns[1].Role != "measure" {
		t.Fatalf("expected role override to apply")
	}
	if merged.Models[0].Columns[1].Provenance.SourceType != "user_defined" {
		t.Fatalf("expected provenance source_type user_defined, got %q", merged.Models[0].Columns[1].Provenance.SourceType)
	}
}

func overlayBaseModel() *smif.SemanticModel {
	return &smif.SemanticModel{
		SMIFVersion: "0.1.0",
		Domain: smif.Domain{
			Name:       "commerce",
			Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9, HumanReviewed: true},
		},
		Models: []smif.Model{
			{
				ModelID:        "orders",
				Name:           "orders",
				PhysicalSource: smif.PhysicalSource{Schema: "public", Table: "orders"},
				DDLFingerprint: "fp_orders",
				Columns: []smif.Column{
					{Name: "id", DataType: "uuid", Role: "identifier", Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9, HumanReviewed: true}},
					{Name: "user_id", DataType: "uuid", Role: "dimension", Description: "original", Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9, HumanReviewed: true}},
				},
				Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9, HumanReviewed: true},
			},
			{
				ModelID:        "users",
				Name:           "users",
				PhysicalSource: smif.PhysicalSource{Schema: "public", Table: "users"},
				DDLFingerprint: "fp_users",
				Columns: []smif.Column{
					{Name: "id", DataType: "uuid", Role: "identifier", Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9, HumanReviewed: true}},
				},
				Provenance: smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9, HumanReviewed: true},
			},
		},
		Relationships: []smif.Relationship{
			{
				RelationshipID:   "rel_orders_users",
				FromModel:        "orders",
				FromColumn:       "user_id",
				ToModel:          "users",
				ToColumn:         "id",
				RelationshipType: "many_to_one",
				JoinCondition:    "orders.user_id = users.id",
				Provenance:       smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9, HumanReviewed: true},
			},
		},
	}
}
