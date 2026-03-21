package coarse

// DomainResult contains database-level coarse-pass output used by later stages.
type DomainResult struct {
	Name          string   `json:"name"`
	Description   string   `json:"description"`
	KeyConcepts   []string `json:"key_concepts"`
	TemporalModel string   `json:"temporal_model"`
	KnownGotchas  []string `json:"known_gotchas"`
}

// TableResult contains table-level coarse-pass output used by later pipeline stages.
type TableResult struct {
	TableName   string `json:"table_name"`
	Description string `json:"description"`
	Grain       string `json:"grain"`
}
