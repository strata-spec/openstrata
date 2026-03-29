package overlay

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/strata-spec/openstrata/internal/smif"
)

func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	// file is .../internal/overlay/merge_test.go — two levels up
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// TestOmdbCastsDescriptionCorrectionFixture verifies that the OMDB fixture
// files parse correctly and that the user-defined description override for the
// casts model is applied by the overlay, eliminating the hallucination-inducing
// phrase "cast and crew" from the active description.
func TestOmdbCastsDescriptionCorrectionFixture(t *testing.T) {
	root := repoRoot()
	semanticPath := filepath.Join(root, "testdata", "fixtures", "omdb_semantic.yaml")
	correctionsPath := filepath.Join(root, "testdata", "fixtures", "omdb_corrections.yaml")

	model, err := smif.ReadYAML(semanticPath)
	if err != nil {
		t.Fatalf("read omdb_semantic.yaml: %v", err)
	}

	corrections, err := LoadCorrections(correctionsPath)
	if err != nil {
		t.Fatalf("read omdb_corrections.yaml: %v", err)
	}
	if len(corrections.Corrections) == 0 {
		t.Fatalf("expected at least one correction in omdb_corrections.yaml")
	}

	corr := corrections.Corrections[0]
	if corr.Source != "user_defined" {
		t.Errorf("correction source = %q, want user_defined", corr.Source)
	}
	if corr.TargetType != "model" || corr.TargetID != "casts" {
		t.Errorf("correction target = %s/%s, want model/casts", corr.TargetType, corr.TargetID)
	}
	if corr.CorrectionType != "description_override" {
		t.Errorf("correction_type = %q, want description_override", corr.CorrectionType)
	}

	merged, err := ApplyOverlay(model, corrections)
	if err != nil {
		t.Fatalf("ApplyOverlay() error = %v", err)
	}

	var castsModel *smif.Model
	for i := range merged.Models {
		if merged.Models[i].ModelID == "casts" {
			castsModel = &merged.Models[i]
			break
		}
	}
	if castsModel == nil {
		t.Fatalf("casts model not found in merged output")
	}

	// The correction must have been applied — provenance must now be user_defined.
	if castsModel.Provenance.SourceType != "user_defined" {
		t.Errorf("casts provenance source_type = %q, want user_defined", castsModel.Provenance.SourceType)
	}

	// The new description must not contain the hallucination-inducing phrase.
	if strings.Contains(castsModel.Description, "cast and crew") {
		t.Errorf("casts description still contains ambiguous phrase 'cast and crew': %q", castsModel.Description)
	}

	// The new description must reference the physical table name to avoid CAST() confusion.
	if !strings.Contains(strings.ToLower(castsModel.Description), "casts") {
		t.Errorf("casts description should reference the physical table name 'casts': %q", castsModel.Description)
	}
}

func TestOmdbMovieRatingsCorrectionFixture(t *testing.T) {
	root := repoRoot()
	semanticPath := filepath.Join(root, "testdata", "fixtures", "omdb_semantic.yaml")
	correctionsPath := filepath.Join(root, "testdata", "fixtures", "omdb_corrections.yaml")

	model, err := smif.ReadYAML(semanticPath)
	if err != nil {
		t.Fatalf("read omdb_semantic.yaml: %v", err)
	}

	corrections, err := LoadCorrections(correctionsPath)
	if err != nil {
		t.Fatalf("read omdb_corrections.yaml: %v", err)
	}

	merged, err := ApplyOverlay(model, corrections)
	if err != nil {
		t.Fatalf("ApplyOverlay() error = %v", err)
	}

	want := map[string]string{
		"vote_average": "always include WHERE kind = 'movie' when aggregating or ranking by this column",
		"votes_count":  "always include WHERE kind = 'movie' when filtering or ranking by votes",
	}

	var moviesModel *smif.Model
	for i := range merged.Models {
		if merged.Models[i].ModelID == "movies" {
			moviesModel = &merged.Models[i]
			break
		}
	}
	if moviesModel == nil {
		t.Fatalf("movies model not found in merged output")
	}

	for colName, snippet := range want {
		var col *smif.Column
		for i := range moviesModel.Columns {
			if moviesModel.Columns[i].Name == colName {
				col = &moviesModel.Columns[i]
				break
			}
		}
		if col == nil {
			t.Fatalf("column %s not found in movies model", colName)
		}
		if !strings.Contains(col.Description, snippet) {
			t.Fatalf("column %s description missing required guidance; got: %q", colName, col.Description)
		}
		if col.Provenance.SourceType != "user_defined" {
			t.Fatalf("column %s provenance source_type = %q, want user_defined", colName, col.Provenance.SourceType)
		}
	}
}

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
