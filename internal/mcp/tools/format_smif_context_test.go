package tools

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/strata-spec/openstrata/internal/smif"
	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// TestComputeHubThreshold
// ---------------------------------------------------------------------------

// TestComputeHubThreshold verifies the dynamic hub-threshold calculation for
// three representative graph shapes.
func TestComputeHubThreshold(t *testing.T) {
	prov := smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9}

	t.Run("3-model graph yields floor 2", func(t *testing.T) {
		// orders(2) – categories(1) – jobs(1); hidden suppressed.
		// mean≈1.33, stddev≈0.47 → round(1.80)=2 → max(2,2)=2
		models := []smif.Model{
			{ModelID: "orders", Provenance: prov},
			{ModelID: "categories", Provenance: prov},
			{ModelID: "jobs", Provenance: prov},
			{ModelID: "hidden", Suppressed: true, Provenance: prov},
		}
		rels := []smif.Relationship{
			{RelationshipID: "r1", FromModel: "orders", ToModel: "categories", Provenance: prov},
			{RelationshipID: "r2", FromModel: "orders", ToModel: "jobs", Provenance: prov},
			{RelationshipID: "r3", FromModel: "hidden", ToModel: "orders", Suppressed: true, Provenance: prov},
		}
		got := computeHubThreshold(models, rels)
		if got != 2 {
			t.Errorf("3-model graph: want hub_threshold=2, got %d", got)
		}
	})

	t.Run("star graph with hub degree 4 yields threshold 3", func(t *testing.T) {
		// hub(4) → leaf1(1), leaf2(1), leaf3(1), leaf4(1)
		// mean=1.6, stddev=1.2 → round(2.8)=3 → max(2,3)=3
		models := []smif.Model{
			{ModelID: "hub", Provenance: prov},
			{ModelID: "leaf1", Provenance: prov},
			{ModelID: "leaf2", Provenance: prov},
			{ModelID: "leaf3", Provenance: prov},
			{ModelID: "leaf4", Provenance: prov},
		}
		rels := []smif.Relationship{
			{RelationshipID: "r1", FromModel: "hub", ToModel: "leaf1", Provenance: prov},
			{RelationshipID: "r2", FromModel: "hub", ToModel: "leaf2", Provenance: prov},
			{RelationshipID: "r3", FromModel: "hub", ToModel: "leaf3", Provenance: prov},
			{RelationshipID: "r4", FromModel: "hub", ToModel: "leaf4", Provenance: prov},
		}
		got := computeHubThreshold(models, rels)
		if got != 3 {
			t.Errorf("star graph: want hub_threshold=3, got %d", got)
		}
	})

	t.Run("single node no relationships yields floor 2", func(t *testing.T) {
		models := []smif.Model{
			{ModelID: "solo", Provenance: prov},
		}
		got := computeHubThreshold(models, nil)
		if got != 2 {
			t.Errorf("single node: want hub_threshold=2, got %d", got)
		}
	})
}

// ---------------------------------------------------------------------------
// TestFilterTokens
// ---------------------------------------------------------------------------

// TestFilterTokens verifies that stopwords are stripped, single-character
// tokens are removed, and the result is lowercased.
func TestFilterTokens(t *testing.T) {
	cases := []struct {
		question string
		wantAny  []string // tokens that MUST appear
		wantNone []string // tokens that must NOT appear
	}{
		{
			question: "how many movies are in each genre",
			wantAny:  []string{"movies", "genre"},
			wantNone: []string{"how", "many", "are", "in"},
		},
		{
			question: "show me all the directors",
			wantAny:  []string{"directors", "me"},
			wantNone: []string{"show", "all", "the"},
		},
		{
			question: "find films with revenue over 1 million",
			wantAny:  []string{"films", "revenue", "over", "million"},
			wantNone: []string{"find", "with"},
		},
		{
			question: "count records where status is active",
			wantAny:  []string{"records", "status", "active"},
			wantNone: []string{"count", "where", "is"},
		},
		{
			// Single-character tokens ('a') must be dropped.
			question: "get a list of orders",
			wantAny:  []string{"orders"},
			wantNone: []string{"get", "list", "of", "a"},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.question, func(t *testing.T) {
			got := filterTokens(tc.question)
			tokenSet := make(map[string]bool, len(got))
			for _, tok := range got {
				tokenSet[tok] = true
			}
			for _, want := range tc.wantAny {
				if !tokenSet[want] {
					t.Errorf("filterTokens(%q): expected token %q, got %v", tc.question, want, got)
				}
			}
			for _, banned := range tc.wantNone {
				if tokenSet[banned] {
					t.Errorf("filterTokens(%q): stopword/single-char %q must not appear, got %v", tc.question, banned, got)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestFormatSMIFContextHubAwareGating
// ---------------------------------------------------------------------------

// hubFixture returns a semantic model where "movies" is a hub (degree 4,
// above the computed threshold of 3) connected to four neighbours:
//
//   - cast_members — relevant: model_id contains "members" which matches the
//     question token "members"
//   - genres       — irrelevant
//   - access_log   — irrelevant
//   - metrics      — irrelevant
//
// Without hub-aware gating all four neighbours would be returned.
// With hub-aware gating only cast_members (score > 0) is returned.
func hubFixture() *smif.SemanticModel {
	prov := smif.Provenance{SourceType: "llm_inferred", Confidence: 0.9}
	col := func(name string) smif.Column {
		return smif.Column{Name: name, DataType: "text", Role: "dimension", Provenance: prov}
	}
	rel := func(id, from, to string) smif.Relationship {
		return smif.Relationship{
			RelationshipID:   id,
			FromModel:        from,
			ToModel:          to,
			RelationshipType: "many_to_one",
			Provenance:       prov,
		}
	}
	return &smif.SemanticModel{
		SMIFVersion: "0.1.0",
		Domain:      smif.Domain{Provenance: prov},
		Models: []smif.Model{
			{
				ModelID:        "movies",
				Name:           "movies",
				Label:          "Movies",
				Description:    "Film catalog.",
				PhysicalSource: smif.PhysicalSource{Schema: "public", Table: "movies"},
				Columns:        []smif.Column{col("id"), col("title")},
				Provenance:     prov,
			},
			{
				ModelID:        "cast_members",
				Name:           "cast_members",
				Label:          "Cast Members",
				Description:    "Production cast assignments.",
				PhysicalSource: smif.PhysicalSource{Schema: "public", Table: "cast_members"},
				Columns:        []smif.Column{col("id"), col("name")},
				Provenance:     prov,
			},
			{
				ModelID:        "genres",
				Name:           "genres",
				Label:          "Genres",
				Description:    "Film genre taxonomy.",
				PhysicalSource: smif.PhysicalSource{Schema: "public", Table: "genres"},
				Columns:        []smif.Column{col("id"), col("name")},
				Provenance:     prov,
			},
			{
				ModelID:        "access_log",
				Name:           "access_log",
				Label:          "Access Log",
				Description:    "HTTP access log for analytics.",
				PhysicalSource: smif.PhysicalSource{Schema: "public", Table: "access_log"},
				Columns:        []smif.Column{col("id"), col("path")},
				Provenance:     prov,
			},
			{
				ModelID:        "metrics",
				Name:           "metrics",
				Label:          "Metrics",
				Description:    "Operational metrics table.",
				PhysicalSource: smif.PhysicalSource{Schema: "public", Table: "metrics"},
				Columns:        []smif.Column{col("id"), col("value")},
				Provenance:     prov,
			},
		},
		Relationships: []smif.Relationship{
			rel("r_movies_cast", "movies", "cast_members"),
			rel("r_movies_genres", "movies", "genres"),
			rel("r_movies_access", "movies", "access_log"),
			rel("r_movies_metrics", "movies", "metrics"),
		},
	}
}

// TestFormatSMIFContextHubAwareGating asserts that when the seed model is a
// hub (degree > hub_threshold), only neighbours with a relevance score > 0
// are added to the result, and irrelevant neighbours are absent.
func TestFormatSMIFContextHubAwareGating(t *testing.T) {
	// Verify the fixture produces a hub at movies.
	// degrees: movies=4, cast_members=1, genres=1, access_log=1, metrics=1
	// mean=1.6, stddev=1.2, hub_threshold=3; movies.degree(4)>3 → hub.
	fix := hubFixture()
	threshold := computeHubThreshold(fix.Models, fix.Relationships)
	t.Logf("hub fixture hub_threshold=%d", threshold)
	if threshold != 3 {
		t.Fatalf("expected hub_threshold=3 for hub fixture, got %d", threshold)
	}

	_, handler := FormatSMIFContext(func() *smif.SemanticModel { return fix })

	// Question that matches "movies" (hub in seed) but NOT genres/access_log/metrics.
	// "members" matches cast_members model_id → cast_members is a relevant neighbour.
	// "which" and "most" are not stopwords but also don't match irrelevant neighbours.
	res, err := handler(context.Background(), callRequest("format_smif_context", map[string]any{
		"question": "which movies have the most cast members",
	}))
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}

	ids := modelIDsInContext(t, extractText(t, res))
	t.Logf("hub gating context models: %v", ids)

	// movies must be in context (it matched the question directly).
	if !ids["movies"] {
		t.Errorf("expected movies in context, got %v", ids)
	}
	// cast_members must be in context (relevant neighbour: "members" matches).
	if !ids["cast_members"] {
		t.Errorf("expected cast_members in context as relevant hub neighbour, got %v", ids)
	}
	// Irrelevant hub neighbours must be absent.
	for _, absent := range []string{"genres", "access_log", "metrics"} {
		if ids[absent] {
			t.Errorf("irrelevant hub neighbour %q must not appear in context (hub_threshold=%d), got %v",
				absent, threshold, ids)
		}
	}
}

// ---------------------------------------------------------------------------
// TestFormatSMIFContextQ013Regression
// ---------------------------------------------------------------------------

// loadOMDBFixture reads testdata/fixtures/omdb_semantic.yaml relative to this
// test file and returns the parsed SemanticModel.
func loadOMDBFixture(t *testing.T) *smif.SemanticModel {
	t.Helper()
	// Walk up from this file's directory to find the repo root, then locate
	// the testdata directory.
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "testdata")
	path := filepath.Join(root, "fixtures", "omdb_semantic.yaml")

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read omdb fixture: %v", err)
	}
	var doc smif.SemanticModel
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse omdb fixture: %v", err)
	}
	return &doc
}

// TestFormatSMIFContextQ013Regression loads the OMDB fixture and exercises
// q013: a question about movies revenue that should produce a focused context
// (≤5 models). The test also logs the hub_threshold computed for OMDB.
func TestFormatSMIFContextQ013Regression(t *testing.T) {
	doc := loadOMDBFixture(t)

	threshold := computeHubThreshold(doc.Models, doc.Relationships)
	t.Logf("OMDB hub_threshold=%d", threshold)

	_, handler := FormatSMIFContext(func() *smif.SemanticModel { return doc })

	// q013: pure movies question — revenue vs budget comparison.
	// Only the movies model (and its immediate join target casts) should appear.
	const q013 = "which movies had revenue greater than budget"
	res, err := handler(context.Background(), callRequest("format_smif_context", map[string]any{
		"question": q013,
	}))
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}

	ids := modelIDsInContext(t, extractText(t, res))
	t.Logf("q013 context model count=%d models=%v", len(ids), ids)

	if len(ids) > 5 {
		t.Errorf("q013 context must contain ≤5 models, got %d: %v", len(ids), ids)
	}
}
