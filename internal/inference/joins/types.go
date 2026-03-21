package joins

// InferredRelationship is a relationship ready for SMIF serialisation.
type InferredRelationship struct {
	RelationshipID   string  `json:"relationship_id"`
	FromModel        string  `json:"from_model"`
	FromColumn       string  `json:"from_column"`
	ToModel          string  `json:"to_model"`
	ToColumn         string  `json:"to_column"`
	RelationshipType string  `json:"relationship_type"`
	JoinCondition    string  `json:"join_condition"`
	SourceType       string  `json:"source_type"`
	Confidence       float64 `json:"confidence"`
	Preferred        bool    `json:"preferred"`
}

// GrainConfirmation cross-checks inferred grain against PK structure.
type GrainConfirmation struct {
	TableName      string   `json:"table_name"`
	GrainStatement string   `json:"grain_statement"`
	PKColumns      []string `json:"pk_columns"`
	Confirmed      bool     `json:"confirmed"`
	Note           string   `json:"note"`
}
