package postgres

// TableInfo represents a Postgres table as extracted by schema introspection.
type TableInfo struct {
	Schema      string
	Name        string
	OID         uint32
	Comment     string
	Columns     []ColumnInfo
	PrimaryKey  []string
	ForeignKeys []FKConstraint
}

// ColumnInfo represents a single column from schema introspection.
type ColumnInfo struct {
	Name       string
	DataType   string
	IsNullable bool
	Default    string
	Comment    string
	Position   int
}

// FKConstraint represents a foreign key relationship.
type FKConstraint struct {
	ConstraintName string
	FromTable      string
	FromColumns    []string
	ToTable        string
	ToColumns      []string
}

// UsageProfile represents aggregated pg_stat_statements data for a column.
// Populated only when --enable-log-mining is set.
type UsageProfile struct {
	TableName         string
	ColumnName        string
	SelectCount       int64
	WhereCount        int64
	GroupByCount      int64
	JoinCount         int64
	SupportingQueries int64
}
