package smif

// SemanticModel is the top-level SMIF semantic.yaml document.
type SemanticModel struct {
	SMIFVersion    string          `json:"smif_version" yaml:"smif_version"`
	GeneratedAt    string          `json:"generated_at" yaml:"generated_at"`
	ToolVersion    string          `json:"tool_version" yaml:"tool_version"`
	Source         Source          `json:"source" yaml:"source"`
	Domain         Domain          `json:"domain" yaml:"domain"`
	Models         []Model         `json:"models" yaml:"models"`
	Relationships  []Relationship  `json:"relationships,omitempty" yaml:"relationships,omitempty"`
	Concepts       []Concept       `json:"concepts,omitempty" yaml:"concepts,omitempty"`
	Metrics        []Metric        `json:"metrics,omitempty" yaml:"metrics,omitempty"`
	QueryTemplates []QueryTemplate `json:"query_templates,omitempty" yaml:"query_templates,omitempty"`
}

// Source identifies the database source metadata.
type Source struct {
	Type            string   `json:"type" yaml:"type"`
	HostFingerprint string   `json:"host_fingerprint" yaml:"host_fingerprint"`
	SchemaNames     []string `json:"schema_names" yaml:"schema_names"`
}

// Domain is the database-level semantic context.
type Domain struct {
	Name         string         `json:"name" yaml:"name"`
	Description  string         `json:"description" yaml:"description"`
	KeyConcepts  []string       `json:"key_concepts,omitempty" yaml:"key_concepts,omitempty"`
	Temporal     *TemporalModel `json:"temporal_model,omitempty" yaml:"temporal_model,omitempty"`
	KnownGotchas []string       `json:"known_gotchas,omitempty" yaml:"known_gotchas,omitempty"`
	Provenance   Provenance     `json:"provenance" yaml:"provenance"`
}

// TemporalModel captures global time conventions.
type TemporalModel struct {
	BusinessDateColumn string `json:"business_date_column,omitempty" yaml:"business_date_column,omitempty"`
	Timezone           string `json:"timezone,omitempty" yaml:"timezone,omitempty"`
	Note               string `json:"note,omitempty" yaml:"note,omitempty"`
}

// RequiredFilter is a mandatory SQL filter condition with a human-readable reason.
type RequiredFilter struct {
	Expression string `json:"expression" yaml:"expression"`
	Reason     string `json:"reason"     yaml:"reason"`
}

// Model is a queryable semantic model.
type Model struct {
	ModelID         string           `json:"model_id" yaml:"model_id"`
	Name            string           `json:"name" yaml:"name"`
	Label           string           `json:"label" yaml:"label"`
	Grain           string           `json:"grain,omitempty" yaml:"grain,omitempty"`
	Description     string           `json:"description" yaml:"description"`
	Suppressed      bool             `json:"suppressed,omitempty" yaml:"suppressed,omitempty"`
	PhysicalSource  PhysicalSource   `json:"physical_source" yaml:"physical_source"`
	PrimaryKey      any              `json:"primary_key,omitempty" yaml:"primary_key,omitempty"`
	DDLFingerprint  string           `json:"ddl_fingerprint" yaml:"ddl_fingerprint"`
	RowCountEst     int64            `json:"row_count_estimate,omitempty" yaml:"row_count_estimate,omitempty"`
	RequiredFilters []RequiredFilter `json:"required_filters,omitempty" yaml:"required_filters,omitempty"`
	Columns         []Column         `json:"columns" yaml:"columns"`
	Provenance      Provenance       `json:"provenance" yaml:"provenance"`
	XProperties     map[string]any   `json:"x_properties,omitempty" yaml:"x_properties,omitempty"`
}

// PhysicalSource identifies the backing table/view/sql for a model.
type PhysicalSource struct {
	Schema string `json:"schema" yaml:"schema"`
	Table  string `json:"table,omitempty" yaml:"table,omitempty"`
	SQL    string `json:"sql,omitempty" yaml:"sql,omitempty"`
}

// Column is a semantic column definition.
type Column struct {
	Name                string         `json:"name" yaml:"name"`
	DataType            string         `json:"data_type" yaml:"data_type"`
	Role                string         `json:"role" yaml:"role"`
	Label               string         `json:"label" yaml:"label"`
	Description         string         `json:"description" yaml:"description"`
	Suppressed          bool           `json:"suppressed,omitempty" yaml:"suppressed,omitempty"`
	Nullable            bool           `json:"nullable" yaml:"nullable"`
	CardinalityCategory string         `json:"cardinality_category" yaml:"cardinality_category"`
	ExampleValues       []string       `json:"example_values,omitempty" yaml:"example_values,omitempty"`
	ValidValues         []string       `json:"valid_values,omitempty"   yaml:"valid_values,omitempty"`
	CaseSensitive       *bool          `json:"case_sensitive,omitempty" yaml:"case_sensitive,omitempty"`
	UsageProfile        *UsageProfile  `json:"usage_profile,omitempty" yaml:"usage_profile,omitempty"`
	Difficulty          string         `json:"difficulty,omitempty" yaml:"difficulty,omitempty"`
	NeedsReview         bool           `json:"needs_review,omitempty" yaml:"needs_review,omitempty"`
	HumanReviewed       bool           `json:"human_reviewed,omitempty" yaml:"human_reviewed,omitempty"`
	Provenance          Provenance     `json:"provenance" yaml:"provenance"`
	XProperties         map[string]any `json:"x_properties,omitempty" yaml:"x_properties,omitempty"`
}

// UsageProfile holds column usage frequencies mined from query logs.
type UsageProfile struct {
	SelectFrequency  float64  `json:"select_frequency,omitempty" yaml:"select_frequency,omitempty"`
	WhereFrequency   float64  `json:"where_frequency,omitempty" yaml:"where_frequency,omitempty"`
	GroupByFrequency float64  `json:"group_by_frequency,omitempty" yaml:"group_by_frequency,omitempty"`
	JoinFrequency    float64  `json:"join_frequency,omitempty" yaml:"join_frequency,omitempty"`
	CommonClauses    []string `json:"common_clauses,omitempty" yaml:"common_clauses,omitempty"`
}

// Relationship defines a join path between two models.
type Relationship struct {
	RelationshipID   string     `json:"relationship_id" yaml:"relationship_id"`
	FromModel        string     `json:"from_model" yaml:"from_model"`
	FromColumn       string     `json:"from_column" yaml:"from_column"`
	ToModel          string     `json:"to_model" yaml:"to_model"`
	ToColumn         string     `json:"to_column" yaml:"to_column"`
	RelationshipType string     `json:"relationship_type" yaml:"relationship_type"`
	JoinCondition    string     `json:"join_condition" yaml:"join_condition"`
	SemanticRole     string     `json:"semantic_role,omitempty" yaml:"semantic_role,omitempty"`
	AlwaysValid      bool       `json:"always_valid" yaml:"always_valid"`
	Preferred        bool       `json:"preferred,omitempty" yaml:"preferred,omitempty"`
	Suppressed       bool       `json:"suppressed,omitempty" yaml:"suppressed,omitempty"`
	Provenance       Provenance `json:"provenance" yaml:"provenance"`
}

// Metric defines an aggregated business measure.
type Metric struct {
	MetricID             string                `json:"metric_id" yaml:"metric_id"`
	Name                 string                `json:"name" yaml:"name"`
	Label                string                `json:"label" yaml:"label"`
	Description          string                `json:"description" yaml:"description"`
	Suppressed           bool                  `json:"suppressed,omitempty" yaml:"suppressed,omitempty"`
	Expression           string                `json:"expression" yaml:"expression"`
	Aggregation          string                `json:"aggregation" yaml:"aggregation"`
	DefaultTimeDimension *ModelColumnRef       `json:"default_time_dimension,omitempty" yaml:"default_time_dimension,omitempty"`
	ValidDimensions      []ModelColumnRef      `json:"valid_dimensions,omitempty" yaml:"valid_dimensions,omitempty"`
	InvalidDimensions    []InvalidDimensionRef `json:"invalid_dimensions,omitempty" yaml:"invalid_dimensions,omitempty"`
	RequiredFilters      []RequiredFilter      `json:"required_filters,omitempty" yaml:"required_filters,omitempty"`
	Additivity           string                `json:"additivity,omitempty" yaml:"additivity,omitempty"`
	Provenance           Provenance            `json:"provenance" yaml:"provenance"`
	Status               string                `json:"status,omitempty" yaml:"status,omitempty"`
	DegradedReason       string                `json:"degraded_reason,omitempty" yaml:"degraded_reason,omitempty"`
}

// Concept represents a named business concept.
type Concept struct {
	ConceptID     string           `json:"concept_id" yaml:"concept_id"`
	Label         string           `json:"label" yaml:"label"`
	Description   string           `json:"description" yaml:"description"`
	DefinitionSQL string           `json:"definition_sql,omitempty" yaml:"definition_sql,omitempty"`
	Broader       *string          `json:"broader,omitempty" yaml:"broader,omitempty"`
	MapsToModels  []string         `json:"maps_to_models,omitempty" yaml:"maps_to_models,omitempty"`
	MapsToColumns []ModelColumnRef `json:"maps_to_columns,omitempty" yaml:"maps_to_columns,omitempty"`
	Provenance    Provenance       `json:"provenance" yaml:"provenance"`
}

// QueryTemplate is a parameterized SQL template.
type QueryTemplate struct {
	TemplateID  string               `json:"template_id" yaml:"template_id"`
	Description string               `json:"description" yaml:"description"`
	SQLTemplate string               `json:"sql_template" yaml:"sql_template"`
	Parameters  []QueryTemplateParam `json:"parameters,omitempty" yaml:"parameters,omitempty"`
	Provenance  Provenance           `json:"provenance" yaml:"provenance"`
}

// QueryTemplateParam describes one query template parameter.
type QueryTemplateParam struct {
	Name        string `json:"name" yaml:"name"`
	DataType    string `json:"data_type" yaml:"data_type"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
}

// ModelColumnRef references a model and column pair.
type ModelColumnRef struct {
	Model  string `json:"model" yaml:"model"`
	Column string `json:"column" yaml:"column"`
}

// InvalidDimensionRef identifies a forbidden dimension for a metric.
type InvalidDimensionRef struct {
	Model  string `json:"model" yaml:"model"`
	Column string `json:"column" yaml:"column"`
	Reason string `json:"reason" yaml:"reason"`
}

// Provenance records source metadata and confidence.
type Provenance struct {
	SourceType    string  `json:"source_type" yaml:"source_type"`
	SourceRef     string  `json:"source_ref,omitempty" yaml:"source_ref,omitempty"`
	Agent         string  `json:"agent,omitempty" yaml:"agent,omitempty"`
	Timestamp     string  `json:"timestamp,omitempty" yaml:"timestamp,omitempty"`
	Confidence    float64 `json:"confidence" yaml:"confidence"`
	HumanReviewed bool    `json:"human_reviewed" yaml:"human_reviewed"`
}

// ColumnProfile stores profile metadata when serializing profile outputs.
type ColumnProfile struct {
	CardinalityCategory string `json:"cardinality_category" yaml:"cardinality_category"`
}

// CorrectionsFile is the top-level corrections.yaml structure.
type CorrectionsFile struct {
	SMIFVersion string       `json:"smif_version" yaml:"smif_version"`
	Corrections []Correction `json:"corrections" yaml:"corrections"`
}

// Correction is a correction record from corrections.yaml.
type Correction struct {
	CorrectionID   string `json:"correction_id" yaml:"correction_id"`
	TargetType     string `json:"target_type" yaml:"target_type"`
	TargetID       string `json:"target_id" yaml:"target_id"`
	CorrectionType string `json:"correction_type" yaml:"correction_type"`
	NewValue       any    `json:"new_value" yaml:"new_value"`
	Reason         string `json:"reason,omitempty" yaml:"reason,omitempty"`
	Source         string `json:"source" yaml:"source"`
	Status         string `json:"status" yaml:"status"`
	Author         string `json:"author,omitempty" yaml:"author,omitempty"`
	SessionID      string `json:"session_id,omitempty" yaml:"session_id,omitempty"`
	Timestamp      string `json:"timestamp" yaml:"timestamp"`
}
