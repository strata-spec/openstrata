package smif

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// RuleTier is MUST or SHOULD.
type RuleTier string

const (
	// TierMust is a MUST-level validation rule.
	TierMust RuleTier = "MUST"
	// TierShould is a SHOULD-level validation rule.
	TierShould RuleTier = "SHOULD"
)

// RuleScope defines where a rule is evaluated.
type RuleScope string

const (
	// ScopeDocument validates a semantic document in isolation.
	ScopeDocument RuleScope = "document"
	// ScopeCorrections validates corrections.yaml in isolation.
	ScopeCorrections RuleScope = "corrections"
	// ScopeWorkspace validates across semantic.yaml and corrections.yaml.
	ScopeWorkspace RuleScope = "workspace"
)

// Rule represents a single validation rule.
type Rule struct {
	ID          string
	Tier        RuleTier
	Scope       RuleScope
	Description string
	Check       func(doc *ValidationDoc) []Violation
}

// ValidationDoc holds all documents needed for validation.
type ValidationDoc struct {
	Semantic    *SemanticModel
	Corrections *CorrectionsFile
}

// Violation is a single validation failure or warning.
type Violation struct {
	RuleID  string
	Tier    RuleTier
	Path    string
	Message string
}

// AllRules returns all V- and W- rules defined for this spec version.
// Each stub Check function returns nil (no violations) until implemented.
func AllRules() []Rule {
	must := []Rule{
		{ID: "V-001", Tier: TierMust, Scope: ScopeDocument, Description: "smif_version present and semver", Check: checkV001},
		{ID: "V-002", Tier: TierMust, Scope: ScopeDocument, Description: "generated_at is valid ISO 8601 UTC", Check: checkV002},
		{ID: "V-003", Tier: TierMust, Scope: ScopeDocument, Description: "tool_version is non-empty", Check: checkV003},
		{ID: "V-004", Tier: TierMust, Scope: ScopeDocument, Description: "source.type equals postgres", Check: checkV004},
		{ID: "V-005", Tier: TierMust, Scope: ScopeDocument, Description: "source.host_fingerprint is non-empty", Check: checkV005},
		{ID: "V-006", Tier: TierMust, Scope: ScopeDocument, Description: "source.schema_names has at least one entry", Check: checkV006},
		{ID: "V-007", Tier: TierMust, Scope: ScopeDocument, Description: "models array is non-empty", Check: checkV007},
		{ID: "V-008", Tier: TierMust, Scope: ScopeDocument, Description: "each model has non-empty model_id", Check: checkV008},
		{ID: "V-009", Tier: TierMust, Scope: ScopeDocument, Description: "model_id values are unique", Check: checkV009},
		{ID: "V-010", Tier: TierMust, Scope: ScopeDocument, Description: "physical_source.table is non-empty", Check: checkV010},
		{ID: "V-011", Tier: TierMust, Scope: ScopeDocument, Description: "each model has at least one column", Check: checkV011},
		{ID: "V-012", Tier: TierMust, Scope: ScopeDocument, Description: "column names unique within model", Check: checkV012},
		{ID: "V-013", Tier: TierMust, Scope: ScopeDocument, Description: "each column has non-empty name", Check: checkV013},
		{ID: "V-014", Tier: TierMust, Scope: ScopeDocument, Description: "column role is defined", Check: checkV014},
		{ID: "V-015", Tier: TierMust, Scope: ScopeDocument, Description: "column data_type is non-empty", Check: checkV015},
		{ID: "V-016", Tier: TierMust, Scope: ScopeDocument, Description: "relationship endpoints are non-empty", Check: checkV016},
		{ID: "V-017", Tier: TierMust, Scope: ScopeDocument, Description: "relationship from_model resolves", Check: checkV017},
		{ID: "V-018", Tier: TierMust, Scope: ScopeDocument, Description: "relationship to_model resolves", Check: checkV018},
		{ID: "V-019", Tier: TierMust, Scope: ScopeDocument, Description: "relationship from_column resolves", Check: checkV019},
		{ID: "V-020", Tier: TierMust, Scope: ScopeDocument, Description: "relationship to_column resolves", Check: checkV020},
		{ID: "V-021", Tier: TierMust, Scope: ScopeDocument, Description: "relationship_type is defined", Check: checkV021},
		{ID: "V-022", Tier: TierMust, Scope: ScopeDocument, Description: "at most one preferred relationship per model pair", Check: checkV022},
		{ID: "V-023", Tier: TierMust, Scope: ScopeDocument, Description: "metric name is non-empty", Check: checkV023},
		{ID: "V-024", Tier: TierMust, Scope: ScopeDocument, Description: "metric names are unique", Check: checkV024},
		{ID: "V-025", Tier: TierMust, Scope: ScopeDocument, Description: "metric expression is non-empty", Check: checkV025},
		{ID: "V-026", Tier: TierMust, Scope: ScopeDocument, Description: "metric aggregation is defined", Check: checkV026},
		{ID: "V-027", Tier: TierMust, Scope: ScopeDocument, Description: "metrics[].status forbidden in semantic.yaml", Check: checkV027},
		{ID: "V-028", Tier: TierMust, Scope: ScopeDocument, Description: "metrics[].degraded_reason forbidden in semantic.yaml", Check: checkV028},
		{ID: "V-029", Tier: TierMust, Scope: ScopeDocument, Description: "provenance.source_type non-empty", Check: checkV029},
		{ID: "V-030", Tier: TierMust, Scope: ScopeDocument, Description: "provenance.source_type is defined", Check: checkV030},
		{ID: "V-031", Tier: TierMust, Scope: ScopeDocument, Description: "provenance.confidence in range [0,1]", Check: checkV031},
		{ID: "V-032", Tier: TierMust, Scope: ScopeDocument, Description: "user_defined forbidden in semantic.yaml provenance", Check: checkV032},
		{ID: "V-033", Tier: TierMust, Scope: ScopeCorrections, Description: "corrections smif_version is non-empty", Check: checkV033},
		{ID: "V-034", Tier: TierMust, Scope: ScopeCorrections, Description: "corrections.source is defined", Check: checkV034},
		{ID: "V-035", Tier: TierMust, Scope: ScopeCorrections, Description: "corrections.status is defined", Check: checkV035},
		{ID: "V-036", Tier: TierMust, Scope: ScopeCorrections, Description: "auto_applied only valid with source system", Check: checkV036},
		{ID: "V-037", Tier: TierMust, Scope: ScopeCorrections, Description: "user_defined corrections must be approved", Check: checkV037},
		{ID: "V-038", Tier: TierMust, Scope: ScopeCorrections, Description: "llm_suggested corrections must not be auto_applied", Check: checkV038},
		{ID: "V-039", Tier: TierMust, Scope: ScopeCorrections, Description: "target_type is defined", Check: checkV039},
		{ID: "V-040", Tier: TierMust, Scope: ScopeCorrections, Description: "correction_type is defined", Check: checkV040},
		{ID: "V-041", Tier: TierMust, Scope: ScopeWorkspace, Description: "corrections smif_version matches semantic", Check: checkV041},
		{ID: "V-042", Tier: TierMust, Scope: ScopeWorkspace, Description: "correction target_id resolves", Check: checkV042},
		{ID: "V-043", Tier: TierMust, Scope: ScopeDocument, Description: "required_filters entries must include non-empty expression and reason", Check: checkV043},
	}

	should := []Rule{
		{ID: "W-001", Tier: TierShould, Scope: ScopeDocument, Description: "Model description is substantive", Check: checkW001},
		{ID: "W-002", Tier: TierShould, Scope: ScopeDocument, Description: "Non-identifier columns have substantive descriptions", Check: checkW002},
		{ID: "W-003", Tier: TierShould, Scope: ScopeDocument, Description: "Ambiguous/domain-dependent columns are human-reviewed", Check: checkW003},
		{ID: "W-004", Tier: TierShould, Scope: ScopeDocument, Description: "Low-confidence unreviewed fields flagged", Check: checkW004},
		{ID: "W-005", Tier: TierShould, Scope: ScopeDocument, Description: "Low-cardinality dimensions have example_values", Check: checkW005},
		{ID: "W-006", Tier: TierShould, Scope: ScopeDocument, Description: "Ratio metrics declare additivity: non_additive", Check: checkW006},
		{ID: "W-007", Tier: TierShould, Scope: ScopeDocument, Description: "All models have ddl_fingerprint", Check: checkW007},
		{ID: "W-008", Tier: TierShould, Scope: ScopeDocument, Description: "Parameterised templates declare their parameters", Check: checkW008},
		{ID: "W-009", Tier: TierShould, Scope: ScopeWorkspace, Description: "Pending corrections surfaced for review", Check: checkW009},
		{ID: "W-010", Tier: TierShould, Scope: ScopeDocument, Description: "default_time_dimension should reference timestamp column", Check: checkW010},
		{ID: "W-011", Tier: TierShould, Scope: ScopeDocument, Description: "columns with valid_values should declare case_sensitive", Check: checkW011},
	}

	rules := make([]Rule, 0, len(must)+len(should))
	rules = append(rules, must...)
	rules = append(rules, should...)
	return rules
}

// Validate runs all rules against doc and returns all violations found.
// Violations with Tier=MUST cause a non-zero exit code.
// Violations with Tier=SHOULD are warnings, exit zero.
func Validate(doc ValidationDoc) (musts []Violation, shoulds []Violation) {
	for _, rule := range AllRules() {
		violations := rule.Check(&doc)
		for _, v := range violations {
			if v.Tier == TierMust {
				musts = append(musts, v)
				continue
			}
			shoulds = append(shoulds, v)
		}
	}
	return musts, shoulds
}

var semverPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$`)

func addViolation(out []Violation, id string, tier RuleTier, path, message string) []Violation {
	return append(out, Violation{RuleID: id, Tier: tier, Path: path, Message: message})
}

func findModel(doc *ValidationDoc, modelID string) *Model {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	for i := range doc.Semantic.Models {
		if doc.Semantic.Models[i].ModelID == modelID {
			return &doc.Semantic.Models[i]
		}
	}
	return nil
}

func findColumn(model *Model, columnName string) *Column {
	if model == nil {
		return nil
	}
	for i := range model.Columns {
		if model.Columns[i].Name == columnName {
			return &model.Columns[i]
		}
	}
	return nil
}

func findRelationship(doc *ValidationDoc, relationshipID string) *Relationship {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	for i := range doc.Semantic.Relationships {
		if doc.Semantic.Relationships[i].RelationshipID == relationshipID {
			return &doc.Semantic.Relationships[i]
		}
	}
	return nil
}

func findMetric(doc *ValidationDoc, metricName string) *Metric {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	for i := range doc.Semantic.Metrics {
		if doc.Semantic.Metrics[i].Name == metricName {
			return &doc.Semantic.Metrics[i]
		}
	}
	return nil
}

func parseModelColumnID(targetID string) (modelID string, columnName string, ok bool) {
	parts := strings.SplitN(strings.TrimSpace(targetID), ".", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func modelPath(modelID string, field string) string {
	if field == "" {
		return fmt.Sprintf("models[%s]", modelID)
	}
	return fmt.Sprintf("models[%s].%s", modelID, field)
}

func columnPath(modelID, columnName, field string) string {
	if field == "" {
		return fmt.Sprintf("models[%s].columns[%s]", modelID, columnName)
	}
	return fmt.Sprintf("models[%s].columns[%s].%s", modelID, columnName, field)
}

func relationshipPath(relID, field string) string {
	if relID == "" {
		relID = "?"
	}
	if field == "" {
		return fmt.Sprintf("relationships[%s]", relID)
	}
	return fmt.Sprintf("relationships[%s].%s", relID, field)
}

func metricPath(metricName, field string) string {
	if metricName == "" {
		metricName = "?"
	}
	if field == "" {
		return fmt.Sprintf("metrics[%s]", metricName)
	}
	return fmt.Sprintf("metrics[%s].%s", metricName, field)
}

func listProvenance(doc *ValidationDoc) []struct {
	path string
	p    *Provenance
} {
	items := make([]struct {
		path string
		p    *Provenance
	}, 0)
	if doc == nil || doc.Semantic == nil {
		return items
	}

	items = append(items, struct {
		path string
		p    *Provenance
	}{path: "domain.provenance", p: &doc.Semantic.Domain.Provenance})

	for i := range doc.Semantic.Models {
		m := &doc.Semantic.Models[i]
		items = append(items, struct {
			path string
			p    *Provenance
		}{path: modelPath(m.ModelID, "provenance"), p: &m.Provenance})
		for j := range m.Columns {
			c := &m.Columns[j]
			items = append(items, struct {
				path string
				p    *Provenance
			}{path: columnPath(m.ModelID, c.Name, "provenance"), p: &c.Provenance})
		}
	}

	for i := range doc.Semantic.Relationships {
		r := &doc.Semantic.Relationships[i]
		items = append(items, struct {
			path string
			p    *Provenance
		}{path: relationshipPath(r.RelationshipID, "provenance"), p: &r.Provenance})
	}

	for i := range doc.Semantic.Concepts {
		c := &doc.Semantic.Concepts[i]
		items = append(items, struct {
			path string
			p    *Provenance
		}{path: fmt.Sprintf("concepts[%s].provenance", c.ConceptID), p: &c.Provenance})
	}

	for i := range doc.Semantic.Metrics {
		m := &doc.Semantic.Metrics[i]
		items = append(items, struct {
			path string
			p    *Provenance
		}{path: metricPath(m.Name, "provenance"), p: &m.Provenance})
	}

	for i := range doc.Semantic.QueryTemplates {
		q := &doc.Semantic.QueryTemplates[i]
		items = append(items, struct {
			path string
			p    *Provenance
		}{path: fmt.Sprintf("query_templates[%s].provenance", q.TemplateID), p: &q.Provenance})
	}

	return items
}

func checkV001(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	v := strings.TrimSpace(doc.Semantic.SMIFVersion)
	if v == "" || !semverPattern.MatchString(v) {
		return []Violation{{RuleID: "V-001", Tier: TierMust, Path: "smif_version", Message: "must be present and match semver"}}
	}
	return nil
}

func checkV002(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	if _, err := time.Parse(time.RFC3339, strings.TrimSpace(doc.Semantic.GeneratedAt)); err != nil {
		return []Violation{{RuleID: "V-002", Tier: TierMust, Path: "generated_at", Message: "must be valid RFC3339/ISO-8601 UTC timestamp"}}
	}
	return nil
}

func checkV003(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	if strings.TrimSpace(doc.Semantic.ToolVersion) == "" {
		return []Violation{{RuleID: "V-003", Tier: TierMust, Path: "tool_version", Message: "must be non-empty"}}
	}
	return nil
}

func checkV004(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	if doc.Semantic.Source.Type != "postgres" {
		return []Violation{{RuleID: "V-004", Tier: TierMust, Path: "source.type", Message: "must equal postgres"}}
	}
	return nil
}

func checkV005(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	if strings.TrimSpace(doc.Semantic.Source.HostFingerprint) == "" {
		return []Violation{{RuleID: "V-005", Tier: TierMust, Path: "source.host_fingerprint", Message: "must be non-empty"}}
	}
	return nil
}

func checkV006(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	if len(doc.Semantic.Source.SchemaNames) == 0 {
		return []Violation{{RuleID: "V-006", Tier: TierMust, Path: "source.schema_names", Message: "must contain at least one schema"}}
	}
	return nil
}

func checkV007(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	if len(doc.Semantic.Models) == 0 {
		return []Violation{{RuleID: "V-007", Tier: TierMust, Path: "models", Message: "must contain at least one model"}}
	}
	return nil
}

func checkV008(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Models {
		if strings.TrimSpace(m.ModelID) == "" {
			out = addViolation(out, "V-008", TierMust, modelPath("?", "model_id"), "model_id must be non-empty")
		}
	}
	return out
}

func checkV009(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	seen := map[string]bool{}
	var out []Violation
	for _, m := range doc.Semantic.Models {
		if m.ModelID == "" {
			continue
		}
		if seen[m.ModelID] {
			out = addViolation(out, "V-009", TierMust, modelPath(m.ModelID, "model_id"), "model_id must be unique")
			continue
		}
		seen[m.ModelID] = true
	}
	return out
}

func checkV010(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Models {
		if strings.TrimSpace(m.PhysicalSource.Table) == "" {
			out = addViolation(out, "V-010", TierMust, modelPath(m.ModelID, "physical_source.table"), "physical_source.table must be non-empty")
		}
	}
	return out
}

func checkV011(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Models {
		if len(m.Columns) == 0 {
			out = addViolation(out, "V-011", TierMust, modelPath(m.ModelID, "columns"), "model must contain at least one column")
		}
	}
	return out
}

func checkV012(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Models {
		seen := map[string]bool{}
		for _, c := range m.Columns {
			if c.Name == "" {
				continue
			}
			if seen[c.Name] {
				out = addViolation(out, "V-012", TierMust, columnPath(m.ModelID, c.Name, "name"), "column name must be unique within model")
				continue
			}
			seen[c.Name] = true
		}
	}
	return out
}

func checkV013(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Models {
		for _, c := range m.Columns {
			if strings.TrimSpace(c.Name) == "" {
				out = addViolation(out, "V-013", TierMust, columnPath(m.ModelID, "?", "name"), "column name must be non-empty")
			}
		}
	}
	return out
}

func checkV014(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	allowed := map[string]struct{}{
		"identifier": {}, "dimension": {}, "measure": {}, "timestamp": {}, "flag": {},
	}
	var out []Violation
	for _, m := range doc.Semantic.Models {
		for _, c := range m.Columns {
			if _, ok := allowed[c.Role]; !ok {
				out = addViolation(out, "V-014", TierMust, columnPath(m.ModelID, c.Name, "role"), "role must be one of identifier|dimension|measure|timestamp|flag")
			}
		}
	}
	return out
}

func checkV015(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Models {
		for _, c := range m.Columns {
			if strings.TrimSpace(c.DataType) == "" {
				out = addViolation(out, "V-015", TierMust, columnPath(m.ModelID, c.Name, "data_type"), "data_type must be non-empty")
			}
		}
	}
	return out
}

func checkV016(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, r := range doc.Semantic.Relationships {
		if strings.TrimSpace(r.FromModel) == "" {
			out = addViolation(out, "V-016", TierMust, relationshipPath(r.RelationshipID, "from_model"), "from_model must be non-empty")
		}
		if strings.TrimSpace(r.FromColumn) == "" {
			out = addViolation(out, "V-016", TierMust, relationshipPath(r.RelationshipID, "from_column"), "from_column must be non-empty")
		}
		if strings.TrimSpace(r.ToModel) == "" {
			out = addViolation(out, "V-016", TierMust, relationshipPath(r.RelationshipID, "to_model"), "to_model must be non-empty")
		}
		if strings.TrimSpace(r.ToColumn) == "" {
			out = addViolation(out, "V-016", TierMust, relationshipPath(r.RelationshipID, "to_column"), "to_column must be non-empty")
		}
	}
	return out
}

func checkV017(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, r := range doc.Semantic.Relationships {
		if strings.TrimSpace(r.FromModel) == "" {
			continue
		}
		if findModel(doc, r.FromModel) == nil {
			out = addViolation(out, "V-017", TierMust, relationshipPath(r.RelationshipID, "from_model"), "from_model must reference a defined model")
		}
	}
	return out
}

func checkV018(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, r := range doc.Semantic.Relationships {
		if strings.TrimSpace(r.ToModel) == "" {
			continue
		}
		if findModel(doc, r.ToModel) == nil {
			out = addViolation(out, "V-018", TierMust, relationshipPath(r.RelationshipID, "to_model"), "to_model must reference a defined model")
		}
	}
	return out
}

func checkV019(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, r := range doc.Semantic.Relationships {
		m := findModel(doc, r.FromModel)
		if m == nil || strings.TrimSpace(r.FromColumn) == "" {
			continue
		}
		if findColumn(m, r.FromColumn) == nil {
			out = addViolation(out, "V-019", TierMust, relationshipPath(r.RelationshipID, "from_column"), "from_column must exist in from_model")
		}
	}
	return out
}

func checkV020(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, r := range doc.Semantic.Relationships {
		m := findModel(doc, r.ToModel)
		if m == nil || strings.TrimSpace(r.ToColumn) == "" {
			continue
		}
		if findColumn(m, r.ToColumn) == nil {
			out = addViolation(out, "V-020", TierMust, relationshipPath(r.RelationshipID, "to_column"), "to_column must exist in to_model")
		}
	}
	return out
}

func checkV021(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	allowed := map[string]struct{}{
		"many_to_one": {}, "one_to_many": {}, "one_to_one": {}, "many_to_many": {},
	}
	var out []Violation
	for _, r := range doc.Semantic.Relationships {
		if _, ok := allowed[r.RelationshipType]; !ok {
			out = addViolation(out, "V-021", TierMust, relationshipPath(r.RelationshipID, "relationship_type"), "relationship_type must be one of many_to_one|one_to_many|one_to_one|many_to_many")
		}
	}
	return out
}

func checkV022(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	pairCount := map[string]int{}
	relByPair := map[string]string{}
	for _, r := range doc.Semantic.Relationships {
		if !r.Preferred {
			continue
		}
		pair := []string{r.FromModel, r.ToModel}
		sort.Strings(pair)
		key := pair[0] + "|" + pair[1]
		pairCount[key]++
		if relByPair[key] == "" {
			relByPair[key] = r.RelationshipID
		}
	}

	var out []Violation
	for pair, count := range pairCount {
		if count > 1 {
			out = addViolation(out, "V-022", TierMust, relationshipPath(relByPair[pair], "preferred"), "at most one relationship per model pair may set preferred=true")
		}
	}
	return out
}

func checkV023(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Metrics {
		if strings.TrimSpace(m.Name) == "" {
			out = addViolation(out, "V-023", TierMust, metricPath("?", "name"), "metric name must be non-empty")
		}
	}
	return out
}

func checkV024(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	seen := map[string]bool{}
	var out []Violation
	for _, m := range doc.Semantic.Metrics {
		if m.Name == "" {
			continue
		}
		if seen[m.Name] {
			out = addViolation(out, "V-024", TierMust, metricPath(m.Name, "name"), "metric names must be unique")
			continue
		}
		seen[m.Name] = true
	}
	return out
}

func checkV025(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Metrics {
		if strings.TrimSpace(m.Expression) == "" {
			out = addViolation(out, "V-025", TierMust, metricPath(m.Name, "expression"), "metric expression must be non-empty")
		}
	}
	return out
}

func checkV026(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	allowed := map[string]struct{}{
		"sum": {}, "avg": {}, "count": {}, "count_distinct": {}, "min": {}, "max": {}, "median": {},
	}
	var out []Violation
	for _, m := range doc.Semantic.Metrics {
		if _, ok := allowed[m.Aggregation]; !ok {
			out = addViolation(out, "V-026", TierMust, metricPath(m.Name, "aggregation"), "aggregation must be one of sum|avg|count|count_distinct|min|max|median")
		}
	}
	return out
}

func checkV027(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Metrics {
		if strings.TrimSpace(m.Status) != "" {
			out = addViolation(out, "V-027", TierMust, metricPath(m.Name, "status"), "status must not appear in semantic.yaml")
		}
	}
	return out
}

func checkV028(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Metrics {
		if strings.TrimSpace(m.DegradedReason) != "" {
			out = addViolation(out, "V-028", TierMust, metricPath(m.Name, "degraded_reason"), "degraded_reason must not appear in semantic.yaml")
		}
	}
	return out
}

func checkV029(doc *ValidationDoc) []Violation {
	var out []Violation
	for _, item := range listProvenance(doc) {
		if strings.TrimSpace(item.p.SourceType) == "" {
			out = addViolation(out, "V-029", TierMust, item.path+".source_type", "source_type must be non-empty")
		}
	}
	return out
}

func checkV030(doc *ValidationDoc) []Violation {
	allowed := map[string]struct{}{
		"schema_constraint": {}, "ddl_comment": {}, "log_inferred": {}, "llm_inferred": {},
		"user_defined": {}, "catalog_import": {}, "code_extracted": {}, "strata_md": {},
	}
	var out []Violation
	for _, item := range listProvenance(doc) {
		if _, ok := allowed[item.p.SourceType]; !ok {
			out = addViolation(out, "V-030", TierMust, item.path+".source_type", "source_type must be one of schema_constraint|ddl_comment|log_inferred|llm_inferred|user_defined|catalog_import|code_extracted|strata_md")
		}
	}
	return out
}

func checkV031(doc *ValidationDoc) []Violation {
	var out []Violation
	for _, item := range listProvenance(doc) {
		if item.p.Confidence < 0.0 || item.p.Confidence > 1.0 {
			out = addViolation(out, "V-031", TierMust, item.path+".confidence", "confidence must be within [0.0, 1.0]")
		}
	}
	return out
}

func checkV032(doc *ValidationDoc) []Violation {
	var out []Violation
	for _, item := range listProvenance(doc) {
		if item.p.SourceType == "user_defined" {
			out = addViolation(out, "V-032", TierMust, item.path+".source_type", "source_type user_defined must not appear in semantic.yaml provenance")
		}
	}
	return out
}

func checkV033(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Corrections == nil {
		return nil
	}
	if strings.TrimSpace(doc.Corrections.SMIFVersion) == "" {
		return []Violation{{RuleID: "V-033", Tier: TierMust, Path: "smif_version", Message: "corrections smif_version must be non-empty"}}
	}
	return nil
}

func checkV034(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Corrections == nil {
		return nil
	}
	allowed := map[string]struct{}{"user_defined": {}, "system": {}, "llm_suggested": {}}
	var out []Violation
	for i, c := range doc.Corrections.Corrections {
		if _, ok := allowed[c.Source]; !ok {
			out = addViolation(out, "V-034", TierMust, fmt.Sprintf("corrections[%d].source", i), "source must be one of user_defined|system|llm_suggested")
		}
	}
	return out
}

func checkV035(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Corrections == nil {
		return nil
	}
	allowed := map[string]struct{}{"approved": {}, "pending": {}, "rejected": {}, "auto_applied": {}}
	var out []Violation
	for i, c := range doc.Corrections.Corrections {
		if _, ok := allowed[c.Status]; !ok {
			out = addViolation(out, "V-035", TierMust, fmt.Sprintf("corrections[%d].status", i), "status must be one of approved|pending|rejected|auto_applied")
		}
	}
	return out
}

func checkV036(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Corrections == nil {
		return nil
	}
	var out []Violation
	for i, c := range doc.Corrections.Corrections {
		if c.Status == "auto_applied" && c.Source != "system" {
			out = addViolation(out, "V-036", TierMust, fmt.Sprintf("corrections[%d].status", i), "status auto_applied is only valid with source system")
		}
	}
	return out
}

func checkV037(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Corrections == nil {
		return nil
	}
	var out []Violation
	for i, c := range doc.Corrections.Corrections {
		if c.Source == "user_defined" && c.Status != "approved" {
			out = addViolation(out, "V-037", TierMust, fmt.Sprintf("corrections[%d].status", i), "source user_defined requires status approved")
		}
	}
	return out
}

func checkV038(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Corrections == nil {
		return nil
	}
	var out []Violation
	for i, c := range doc.Corrections.Corrections {
		if c.Source == "llm_suggested" && c.Status == "auto_applied" {
			out = addViolation(out, "V-038", TierMust, fmt.Sprintf("corrections[%d].status", i), "source llm_suggested must not use status auto_applied")
		}
	}
	return out
}

func checkV039(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Corrections == nil {
		return nil
	}
	allowed := map[string]struct{}{
		"domain": {}, "model": {}, "column": {}, "relationship": {},
		"concept": {}, "metric": {}, "query_template": {},
	}
	var out []Violation
	for i, c := range doc.Corrections.Corrections {
		if _, ok := allowed[c.TargetType]; !ok {
			out = addViolation(out, "V-039", TierMust, fmt.Sprintf("corrections[%d].target_type", i), "target_type must be one of domain|model|column|relationship|concept|metric|query_template")
		}
	}
	return out
}

func checkV040(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Corrections == nil {
		return nil
	}
	allowed := map[string]struct{}{
		"description_override": {}, "label_override": {}, "role_override": {}, "join_override": {},
		"suppress": {}, "required_filter_add": {}, "grain_override": {}, "example_values_override": {}, "confidence_override": {},
	}
	var out []Violation
	for i, c := range doc.Corrections.Corrections {
		if _, ok := allowed[c.CorrectionType]; !ok {
			out = addViolation(out, "V-040", TierMust, fmt.Sprintf("corrections[%d].correction_type", i), "correction_type must be one of description_override|label_override|role_override|join_override|suppress|required_filter_add|grain_override|example_values_override|confidence_override")
		}
	}
	return out
}

func checkV041(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil || doc.Corrections == nil {
		return nil
	}
	if strings.TrimSpace(doc.Corrections.SMIFVersion) != strings.TrimSpace(doc.Semantic.SMIFVersion) {
		return []Violation{{RuleID: "V-041", Tier: TierMust, Path: "smif_version", Message: "corrections smif_version must match semantic smif_version"}}
	}
	return nil
}

func checkV042(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Corrections == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for i, c := range doc.Corrections.Corrections {
		resolved := false
		switch c.TargetType {
		case "domain":
			resolved = true
		case "model":
			resolved = findModel(doc, c.TargetID) != nil
		case "column":
			modelID, columnName, ok := parseModelColumnID(c.TargetID)
			if ok {
				m := findModel(doc, modelID)
				resolved = m != nil && findColumn(m, columnName) != nil
			}
		case "relationship":
			resolved = findRelationship(doc, c.TargetID) != nil
		case "metric":
			resolved = findMetric(doc, c.TargetID) != nil
		default:
			// V-042 scope currently defined for domain/model/column/relationship/metric.
			resolved = true
		}

		if !resolved {
			out = addViolation(out, "V-042", TierMust, fmt.Sprintf("corrections[%d].target_id", i), "target_id does not resolve to a defined object")
		}
	}
	return out
}

func checkV043(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}

	var out []Violation
	for _, model := range doc.Semantic.Models {
		for i, requiredFilter := range model.RequiredFilters {
			if strings.TrimSpace(requiredFilter.Expression) == "" {
				out = addViolation(
					out,
					"V-043",
					TierMust,
					fmt.Sprintf("models[%s].required_filters[%d].expression", model.ModelID, i),
					"required_filters entries must include non-empty expression",
				)
			}
			if strings.TrimSpace(requiredFilter.Reason) == "" {
				out = addViolation(
					out,
					"V-043",
					TierMust,
					fmt.Sprintf("models[%s].required_filters[%d].reason", model.ModelID, i),
					"required_filters entries must include non-empty reason",
				)
			}
		}
	}

	for _, metric := range doc.Semantic.Metrics {
		metricID := strings.TrimSpace(metric.MetricID)
		if metricID == "" {
			metricID = "?"
		}

		for i, requiredFilter := range metric.RequiredFilters {
			if strings.TrimSpace(requiredFilter.Expression) == "" {
				out = addViolation(
					out,
					"V-043",
					TierMust,
					fmt.Sprintf("metrics[%s].required_filters[%d].expression", metricID, i),
					"required_filters entries must include non-empty expression",
				)
			}
			if strings.TrimSpace(requiredFilter.Reason) == "" {
				out = addViolation(
					out,
					"V-043",
					TierMust,
					fmt.Sprintf("metrics[%s].required_filters[%d].reason", metricID, i),
					"required_filters entries must include non-empty reason",
				)
			}
		}
	}

	return out
}

func checkW001(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Models {
		if len(strings.TrimSpace(m.Description)) <= 20 {
			out = addViolation(out, "W-001", TierShould, modelPath(m.ModelID, "description"), "description should be substantive (>20 chars)")
		}
	}
	return out
}

func checkW002(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Models {
		for _, c := range m.Columns {
			if c.Role == "identifier" {
				continue
			}
			if len(strings.TrimSpace(c.Description)) <= 20 {
				out = addViolation(out, "W-002", TierShould, columnPath(m.ModelID, c.Name, "description"), "non-identifier column descriptions should be substantive (>20 chars)")
			}
		}
	}
	return out
}

func checkW003(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Models {
		for _, c := range m.Columns {
			if c.Difficulty != "ambiguous" && c.Difficulty != "domain_dependent" {
				continue
			}
			if !c.HumanReviewed && !c.NeedsReview {
				out = addViolation(out, "W-003", TierShould, columnPath(m.ModelID, c.Name, "human_reviewed"), "ambiguous/domain-dependent columns should be human_reviewed=true")
			}
		}
	}
	return out
}

func checkW004(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Models {
		for _, c := range m.Columns {
			if c.Provenance.Confidence <= 0.4 && !c.HumanReviewed && !c.NeedsReview {
				out = addViolation(out, "W-004", TierShould, columnPath(m.ModelID, c.Name, "provenance"), "low-confidence unreviewed columns should be flagged for review")
			}
		}
	}
	return out
}

func checkW005(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Models {
		for _, c := range m.Columns {
			if c.CardinalityCategory == "low" && len(c.ExampleValues) == 0 {
				out = addViolation(out, "W-005", TierShould, columnPath(m.ModelID, c.Name, "example_values"), "low-cardinality columns should include at least one example_value")
			}
		}
	}
	return out
}

func checkW006(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Metrics {
		if m.Aggregation == "avg" || strings.Contains(m.Expression, "/") {
			if strings.TrimSpace(m.Additivity) != "non_additive" {
				out = addViolation(out, "W-006", TierShould, metricPath(m.Name, "additivity"), "avg/ratio metrics should declare additivity=non_additive")
			}
		}
	}
	return out
}

func checkW007(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, m := range doc.Semantic.Models {
		if strings.TrimSpace(m.DDLFingerprint) == "" {
			out = addViolation(out, "W-007", TierShould, modelPath(m.ModelID, "ddl_fingerprint"), "models should include ddl_fingerprint")
		}
	}
	return out
}

func checkW008(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for i, q := range doc.Semantic.QueryTemplates {
		if strings.Contains(q.SQLTemplate, "$") && len(q.Parameters) == 0 {
			out = addViolation(out, "W-008", TierShould, fmt.Sprintf("query_templates[%d].parameters", i), "parameterized templates should define at least one parameter")
		}
	}
	return out
}

func checkW009(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Corrections == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for i, c := range doc.Corrections.Corrections {
		if c.Status == "pending" {
			out = addViolation(out, "W-009", TierShould, fmt.Sprintf("corrections[%d].status", i), "pending corrections should be reviewed and resolved")
		}
	}
	return out
}

func checkW010(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}
	var out []Violation
	for _, metric := range doc.Semantic.Metrics {
		if metric.DefaultTimeDimension == nil {
			continue
		}
		m := findModel(doc, metric.DefaultTimeDimension.Model)
		if m == nil {
			continue
		}
		c := findColumn(m, metric.DefaultTimeDimension.Column)
		if c == nil {
			continue
		}
		if c.Role != "timestamp" {
			out = addViolation(out, "W-010", TierShould, metricPath(metric.Name, "default_time_dimension"), "default_time_dimension should reference a timestamp column")
		}
	}
	return out
}

func checkW011(doc *ValidationDoc) []Violation {
	if doc == nil || doc.Semantic == nil {
		return nil
	}

	var out []Violation
	for _, model := range doc.Semantic.Models {
		for _, column := range model.Columns {
			if len(column.ValidValues) > 0 && column.CaseSensitive == nil {
				out = addViolation(
					out,
					"W-011",
					TierShould,
					columnPath(model.ModelID, column.Name, "case_sensitive"),
					"columns with valid_values should explicitly set case_sensitive",
				)
			}
		}
	}

	return out
}
