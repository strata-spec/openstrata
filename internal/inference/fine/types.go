package fine

// ColumnResult is the fine-pass output for a single column.
type ColumnResult struct {
	TableName   string `json:"table_name"`
	ColumnName  string `json:"column_name"`
	Role        string `json:"role"`
	Label       string `json:"label"`
	Description string `json:"description"`
	Difficulty  string `json:"difficulty"`
	NeedsReview bool   `json:"needs_review"`
}

// FinePassResult is the fine-pass output for one table.
type FinePassResult struct {
	TableName string         `json:"table_name"`
	Columns   []ColumnResult `json:"columns"`
}
