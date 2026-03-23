package inference

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	coarse "github.com/strata-spec/openstrata/internal/inference/coarse"
	"github.com/strata-spec/openstrata/internal/postgres"
)

const minJoinCountForInference = 3
const highConfidenceJoinCount = 10

const (
	sourceUserDefined      = "user_defined"
	sourceStrataMD         = "strata_md"
	sourceSchemaConstraint = "schema_constraint"
	sourceLogInferred      = "log_inferred"
)

var canonicalJoinsHeaderPattern = regexp.MustCompile(`(?i)^\s*#{2,3}\s*(canonical joins|known joins|joins|relationships)\s*$`)
var joinLinePattern = regexp.MustCompile(`(?i)^\s*([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\s*$`)

// InferredRelationship is a relationship ready for SMIF serialisation.
type InferredRelationship struct {
	RelationshipID   string  `json:"relationship_id"` // "{from_model}_{from_col}_to_{to_model}"
	FromModel        string  `json:"from_model"`
	FromColumn       string  `json:"from_column"`
	ToModel          string  `json:"to_model"`
	ToColumn         string  `json:"to_column"`
	RelationshipType string  `json:"relationship_type"` // "many_to_one"|"one_to_many"|"one_to_one"|"many_to_many"
	JoinCondition    string  `json:"join_condition"`    // SQL snippet e.g. "orders.user_id = users.id"
	SourceType       string  `json:"source_type"`       // provenance source_type value
	Confidence       float64 `json:"confidence"`
	Preferred        bool    `json:"preferred"` // true if this is the best join for this model pair
}

// GrainConfirmation cross-checks inferred grain against PK structure.
type GrainConfirmation struct {
	TableName      string   `json:"table_name"`
	GrainStatement string   `json:"grain_statement"` // from Stage 5 TableResult.Grain
	PKColumns      []string `json:"pk_columns"`      // from Stage 2 TableInfo.PrimaryKey
	Confirmed      bool     `json:"confirmed"`       // true if PK is consistent with grain
	Note           string   `json:"note"`            // human-readable explanation if not confirmed
}

// InferJoins produces relationships from all available sources.
// usageProfiles is optional (may be nil/empty).
// selectedTables is optional; when set, relationships to out-of-scope to_models are dropped.
func InferJoins(
	tables []postgres.TableInfo,
	usageProfiles []postgres.UsageProfile,
	strataMD string,
	selectedTables []string,
) ([]InferredRelationship, int, error) {
	tableByName := make(map[string]postgres.TableInfo, len(tables))
	columnsByTable := make(map[string]map[string]struct{}, len(tables))
	for _, t := range tables {
		tableName := normalizePart(t.Name)
		tableByName[tableName] = t

		colSet := make(map[string]struct{}, len(t.Columns))
		for _, c := range t.Columns {
			colSet[normalizePart(c.Name)] = struct{}{}
		}
		columnsByTable[tableName] = colSet
	}

	rels := make([]InferredRelationship, 0)
	schemaCovered := make(map[string]struct{})

	for _, t := range tables {
		fromModel := normalizePart(t.Name)
		for _, fk := range t.ForeignKeys {
			n := len(fk.FromColumns)
			if len(fk.ToColumns) < n {
				n = len(fk.ToColumns)
			}
			if n == 0 {
				continue
			}

			toModel := normalizePart(fk.ToTable)
			fromCols := make([]string, 0, n)
			toCols := make([]string, 0, n)
			conditions := make([]string, 0, n)
			for i := 0; i < n; i++ {
				fromCol := normalizePart(fk.FromColumns[i])
				toCol := normalizePart(fk.ToColumns[i])
				fromCols = append(fromCols, fromCol)
				toCols = append(toCols, toCol)
				conditions = append(conditions, fmt.Sprintf("%s.%s = %s.%s", fromModel, fromCol, toModel, toCol))
				schemaCovered[pairKey(fromModel, fromCol, toModel, toCol)] = struct{}{}
			}

			fromColumn := strings.Join(fromCols, "_")
			toColumn := strings.Join(toCols, "_")
			rel := InferredRelationship{
				RelationshipID:   relationshipID(fromModel, fromColumn, toModel),
				FromModel:        fromModel,
				FromColumn:       fromColumn,
				ToModel:          toModel,
				ToColumn:         toColumn,
				RelationshipType: "many_to_one",
				JoinCondition:    strings.Join(conditions, " AND "),
				SourceType:       sourceSchemaConstraint,
				Confidence:       1.0,
			}
			rels = append(rels, rel)
		}
	}

	for _, up := range usageProfiles {
		if up.JoinCount < minJoinCountForInference {
			continue
		}

		fromModel := normalizePart(up.TableName)
		fromColumn := normalizePart(up.ColumnName)
		if fromModel == "" || fromColumn == "" {
			continue
		}
		if _, ok := tableByName[fromModel]; !ok {
			continue
		}
		if cols, ok := columnsByTable[fromModel]; ok {
			if _, exists := cols[fromColumn]; !exists {
				continue
			}
		}

		toModel, toColumn, ok := resolveLogJoinTarget(fromModel, fromColumn, tables, columnsByTable)
		if !ok {
			continue
		}

		if _, covered := schemaCovered[pairKey(fromModel, fromColumn, toModel, toColumn)]; covered {
			continue
		}

		conf := 0.55
		if up.JoinCount >= highConfidenceJoinCount {
			conf = 0.75
		}

		rel := InferredRelationship{
			RelationshipID:   relationshipID(fromModel, fromColumn, toModel),
			FromModel:        fromModel,
			FromColumn:       fromColumn,
			ToModel:          toModel,
			ToColumn:         toColumn,
			RelationshipType: "many_to_one",
			JoinCondition:    fmt.Sprintf("%s.%s = %s.%s", fromModel, fromColumn, toModel, toColumn),
			SourceType:       sourceLogInferred,
			Confidence:       conf,
		}
		rels = append(rels, rel)
	}

	for _, parsed := range parseCanonicalJoins(strataMD) {
		fromModel := normalizePart(parsed.fromModel)
		fromColumn := normalizePart(parsed.fromColumn)
		toModel := normalizePart(parsed.toModel)
		toColumn := normalizePart(parsed.toColumn)
		if fromModel == "" || fromColumn == "" || toModel == "" || toColumn == "" {
			continue
		}
		if !hasColumn(columnsByTable, fromModel, fromColumn) || !hasColumn(columnsByTable, toModel, toColumn) {
			continue
		}

		rel := InferredRelationship{
			RelationshipID:   relationshipID(fromModel, fromColumn, toModel),
			FromModel:        fromModel,
			FromColumn:       fromColumn,
			ToModel:          toModel,
			ToColumn:         toColumn,
			RelationshipType: "many_to_one",
			JoinCondition:    fmt.Sprintf("%s.%s = %s.%s", fromModel, fromColumn, toModel, toColumn),
			SourceType:       sourceStrataMD,
			Confidence:       0.95,
		}
		rels = append(rels, rel)
	}

	filtered := filterOutOfScopeRelationships(rels, selectedTables)
	droppedCount := len(rels) - len(filtered)
	relationships := deduplicateRelationships(filtered)

	sort.Slice(relationships, func(i, j int) bool {
		if relationships[i].FromModel != relationships[j].FromModel {
			return relationships[i].FromModel < relationships[j].FromModel
		}
		if relationships[i].ToModel != relationships[j].ToModel {
			return relationships[i].ToModel < relationships[j].ToModel
		}
		if relationships[i].FromColumn != relationships[j].FromColumn {
			return relationships[i].FromColumn < relationships[j].FromColumn
		}
		return relationships[i].ToColumn < relationships[j].ToColumn
	})

	markPreferred(relationships)

	return relationships, droppedCount, nil
}

// ConfirmGrains cross-checks Stage 5 grain statements against PK structure.
func ConfirmGrains(
	tables []postgres.TableInfo,
	tableResults []coarse.TableResult,
) []GrainConfirmation {
	byTable := make(map[string]coarse.TableResult, len(tableResults))
	for _, tr := range tableResults {
		byTable[normalizePart(tr.TableName)] = tr
	}

	out := make([]GrainConfirmation, 0, len(tables))
	for _, t := range tables {
		tableName := normalizePart(t.Name)
		tr, ok := byTable[tableName]
		pkColumns := append([]string(nil), t.PrimaryKey...)

		gc := GrainConfirmation{
			TableName:      tableName,
			GrainStatement: tr.Grain,
			PKColumns:      pkColumns,
		}

		if !ok {
			gc.Confirmed = false
			gc.Note = "no coarse pass result"
			out = append(out, gc)
			continue
		}

		if len(t.PrimaryKey) == 0 {
			gc.Confirmed = false
			gc.Note = "no primary key - grain cannot be confirmed structurally"
			out = append(out, gc)
			continue
		}

		if len(t.PrimaryKey) == 1 {
			gc.Confirmed = true
			gc.Note = "single-column PK consistent with grain statement"
			out = append(out, gc)
			continue
		}

		grainLower := strings.ToLower(tr.Grain)
		missing := make([]string, 0)
		for _, pk := range t.PrimaryKey {
			if !strings.Contains(grainLower, strings.ToLower(pk)) {
				missing = append(missing, pk)
			}
		}
		if len(missing) == 0 {
			gc.Confirmed = true
			gc.Note = "composite PK columns reflected in grain statement"
		} else {
			gc.Confirmed = false
			gc.Note = fmt.Sprintf("composite PK columns not all reflected in grain statement: %s", strings.Join(missing, ", "))
		}

		out = append(out, gc)
	}

	return out
}

type parsedJoin struct {
	fromModel  string
	fromColumn string
	toModel    string
	toColumn   string
}

func parseCanonicalJoins(strataMD string) []parsedJoin {
	if strings.TrimSpace(strataMD) == "" {
		return nil
	}

	lines := strings.Split(strataMD, "\n")
	joins := make([]parsedJoin, 0)
	inCanonicalSection := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		if strings.HasPrefix(trimmed, "#") {
			if canonicalJoinsHeaderPattern.MatchString(trimmed) {
				inCanonicalSection = true
				continue
			}
			if inCanonicalSection {
				inCanonicalSection = false
			}
			continue
		}

		if !inCanonicalSection {
			continue
		}

		m := joinLinePattern.FindStringSubmatch(trimmed)
		if len(m) != 5 {
			continue
		}
		joins = append(joins, parsedJoin{fromModel: m[1], fromColumn: m[2], toModel: m[3], toColumn: m[4]})
	}

	return joins
}

func resolveLogJoinTarget(
	fromModel string,
	fromColumn string,
	tables []postgres.TableInfo,
	columnsByTable map[string]map[string]struct{},
) (string, string, bool) {
	if strings.HasSuffix(fromColumn, "_id") {
		base := strings.TrimSuffix(fromColumn, "_id")
		candidates := []string{base, base + "s"}
		for _, c := range candidates {
			c = normalizePart(c)
			if _, ok := columnsByTable[c]; !ok {
				continue
			}
			if hasColumn(columnsByTable, c, "id") {
				return c, "id", true
			}
			if len(columnsByTable[c]) > 0 {
				return c, firstColumn(columnsByTable[c]), true
			}
		}
	}

	for _, t := range tables {
		for _, fk := range t.ForeignKeys {
			n := len(fk.FromColumns)
			if len(fk.ToColumns) < n {
				n = len(fk.ToColumns)
			}
			for i := 0; i < n; i++ {
				if normalizePart(fk.FromColumns[i]) != fromColumn {
					continue
				}
				toModel := normalizePart(fk.ToTable)
				toColumn := normalizePart(fk.ToColumns[i])
				if toModel == "" || toColumn == "" || toModel == fromModel {
					continue
				}
				if hasColumn(columnsByTable, toModel, toColumn) {
					return toModel, toColumn, true
				}
			}
		}
	}

	return "", "", false
}

// deduplicateRelationships is Step 4a: keep the best relationship per
// four-part column key (from_model, from_column, to_model, to_column).
// Selection uses confidence as the primary criterion and source trust order
// as a tiebreaker. All output relationships have Preferred reset to false —
// markPreferred (Step 4b) assigns Preferred=true to winners.
func deduplicateRelationships(rels []InferredRelationship) []InferredRelationship {
	type relKey struct {
		fromModel, fromColumn, toModel, toColumn string
	}
	trustOrder := map[string]int{
		sourceUserDefined:      4,
		sourceStrataMD:         3,
		sourceSchemaConstraint: 2,
		sourceLogInferred:      1,
	}

	seen := make(map[relKey]InferredRelationship, len(rels))
	for _, r := range rels {
		k := relKey{
			fromModel:  strings.ToLower(r.FromModel),
			fromColumn: strings.ToLower(r.FromColumn),
			toModel:    strings.ToLower(r.ToModel),
			toColumn:   strings.ToLower(r.ToColumn),
		}
		existing, ok := seen[k]
		if !ok {
			seen[k] = r
		} else if r.Confidence > existing.Confidence ||
			(r.Confidence == existing.Confidence &&
				trustOrder[r.SourceType] > trustOrder[existing.SourceType]) {
			seen[k] = r
		}
	}

	// Reset Preferred=false on every output relationship.
	// markPreferred (Step 4b) assigns Preferred=true on winners.
	deduped := make([]InferredRelationship, 0, len(seen))
	for _, r := range seen {
		r.Preferred = false
		deduped = append(deduped, r)
	}
	return deduped
}

// markPreferred is Step 4b: assign Preferred=true to exactly one relationship
// per (from_model, to_model) model pair — the one with the highest confidence
// (source trust order as tiebreaker). Step 4a (deduplicateRelationships) has
// already reset all Preferred flags to false before this is called.
func markPreferred(relationships []InferredRelationship) {
	type modelPair struct {
		fromModel, toModel string
	}
	trustOrder := map[string]int{
		sourceUserDefined:      4,
		sourceStrataMD:         3,
		sourceSchemaConstraint: 2,
		sourceLogInferred:      1,
	}

	// First pass: find the best relationship index per model pair.
	// The key is sorted so A→B and B→A use the same key, matching checkV022.
	bestIdx := make(map[modelPair]int)
	for i, r := range relationships {
		pairSlice := []string{strings.ToLower(r.FromModel), strings.ToLower(r.ToModel)}
		sort.Strings(pairSlice)
		pair := modelPair{fromModel: pairSlice[0], toModel: pairSlice[1]}
		existingIdx, ok := bestIdx[pair]
		if !ok {
			bestIdx[pair] = i
		} else {
			existing := relationships[existingIdx]
			if r.Confidence > existing.Confidence ||
				(r.Confidence == existing.Confidence &&
					trustOrder[r.SourceType] > trustOrder[existing.SourceType]) {
				bestIdx[pair] = i
			}
		}
	}

	// Second pass: set preferred=true only on winners.
	for _, idx := range bestIdx {
		relationships[idx].Preferred = true
	}
}

func filterOutOfScopeRelationships(
	rels []InferredRelationship,
	selectedTables []string,
) []InferredRelationship {
	if len(selectedTables) == 0 {
		return rels
	}

	selected := make(map[string]bool, len(selectedTables))
	for _, t := range selectedTables {
		selected[strings.ToLower(t)] = true
	}

	result := make([]InferredRelationship, 0, len(rels))
	for _, r := range rels {
		if selected[strings.ToLower(r.ToModel)] {
			result = append(result, r)
		}
	}
	return result
}

func pairKey(fromModel, fromColumn, toModel, toColumn string) string {
	return strings.Join([]string{normalizePart(fromModel), normalizePart(fromColumn), normalizePart(toModel), normalizePart(toColumn)}, "|")
}

func relationshipID(fromModel, fromColumn, toModel string) string {
	from := normalizePart(fromModel)
	col := normalizePart(strings.ReplaceAll(fromColumn, " ", "_"))
	to := normalizePart(toModel)
	return from + "_" + col + "_to_" + to
}

func normalizePart(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, " ", "_")
	return s
}

func hasColumn(columnsByTable map[string]map[string]struct{}, tableName, columnName string) bool {
	cols, ok := columnsByTable[normalizePart(tableName)]
	if !ok {
		return false
	}
	_, exists := cols[normalizePart(columnName)]
	return exists
}

func firstColumn(colSet map[string]struct{}) string {
	cols := make([]string, 0, len(colSet))
	for c := range colSet {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	if len(cols) == 0 {
		return ""
	}
	return cols[0]
}
