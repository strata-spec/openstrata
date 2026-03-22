package inference

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/strata-spec/openstrata/internal/inference/llm"
	"github.com/strata-spec/openstrata/internal/postgres"
	"golang.org/x/sync/errgroup"
)

var finePassSchema = []byte(`{
  "type": "object",
  "required": ["table_name", "columns"],
  "additionalProperties": false,
  "properties": {
    "table_name": {"type": "string"},
    "columns": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["table_name", "column_name", "role", "label", "description", "difficulty"],
        "additionalProperties": false,
        "properties": {
          "table_name":   {"type": "string"},
          "column_name":  {"type": "string"},
          "role":         {"type": "string", "enum": ["identifier","dimension","measure","timestamp","flag"]},
          "label":        {"type": "string"},
          "description":  {"type": "string"},
          "difficulty":   {"type": "string", "enum": ["self_evident","context_dependent","ambiguous","domain_dependent"]}
        }
      }
    }
  }
}`)

type ColumnResult struct {
	TableName   string `json:"table_name"`
	ColumnName  string `json:"column_name"`
	Role        string `json:"role"`
	Label       string `json:"label"`
	Description string `json:"description"`
	Difficulty  string `json:"difficulty"`
	NeedsReview bool   `json:"needs_review"`
}

type FinePassResult struct {
	TableName string         `json:"table_name"`
	Columns   []ColumnResult `json:"columns"`
	TokensIn  int            `json:"-"`
	TokensOut int            `json:"-"`
}

// RunFinePass executes one LLM call per table to annotate all columns.
// Tables are processed concurrently with a cap of 3 simultaneous LLM calls.
// Results are returned in the same order as the input tableResults slice.
// A single-table failure is logged and that table is skipped (partial results returned).
// If all tables fail, an error is returned.
func RunFinePass(
	ctx context.Context,
	llmClient llm.LLMClient,
	tables []postgres.TableInfo,
	profiles map[string]postgres.ColumnProfile,
	tableResults []TableResult,
	domain *DomainResult,
	strataMD string,
) ([]FinePassResult, error) {
	results := make([]FinePassResult, len(tables))
	success := make([]bool, len(tables))

	tableResultByName := make(map[string]TableResult, len(tableResults))
	for _, tr := range tableResults {
		tableResultByName[strings.ToLower(tr.TableName)] = tr
	}

	sem := make(chan struct{}, 3)
	g, gctx := errgroup.WithContext(ctx)
	var mu sync.Mutex

	for i := range tables {
		idx := i
		t := tables[i]
		g.Go(func() error {
			select {
			case sem <- struct{}{}:
			case <-gctx.Done():
				return gctx.Err()
			}
			defer func() { <-sem }()

			tr, ok := tableResultByName[strings.ToLower(t.Name)]
			if !ok {
				tr = TableResult{TableName: t.Name, Description: "No coarse-pass description available.", Grain: "one row per record"}
			}

			tableProfiles := make(map[string]postgres.ColumnProfile)
			for _, col := range t.Columns {
				fullKey := strings.ToLower(t.Name + "." + col.Name)
				if p, found := profiles[fullKey]; found {
					tableProfiles[strings.ToLower(col.Name)] = p
				}
			}

			prompt := buildFinePassPrompt(t, tableProfiles, tr, domain, strataMD)
			var out FinePassResult
			gen, err := llmClient.GenerateStructured(gctx, prompt, finePassSchema, &out)
			if err != nil {
				log.Printf("fine pass: table %s: %v - skipping", t.Name, err)
				return nil
			}

			applyPostProcessing(&out, t)
			out.TokensIn = gen.TokensIn
			out.TokensOut = gen.TokensOut

			mu.Lock()
			results[idx] = out
			success[idx] = true
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	final := make([]FinePassResult, 0, len(tables))
	for i, ok := range success {
		if ok {
			final = append(final, results[i])
		}
	}

	if len(final) == 0 {
		return nil, fmt.Errorf("fine pass: all table calls failed")
	}

	return final, nil
}

func buildFinePassPrompt(
	table postgres.TableInfo,
	profile map[string]postgres.ColumnProfile,
	tableResult TableResult,
	domain *DomainResult,
	strataMD string,
) string {
	var b strings.Builder
	b.WriteString("You are a senior data engineer annotating columns for a semantic layer.\n\n")

	domainName := "unknown"
	domainDescription := ""
	if domain != nil {
		domainName = domain.Name
		domainDescription = domain.Description
	}
	b.WriteString("Database: " + domainName + "\n")
	b.WriteString(domainDescription + "\n\n")

	b.WriteString("Table: " + table.Name + "\n")
	b.WriteString("Description: " + tableResult.Description + "\n")
	b.WriteString("Grain: " + tableResult.Grain + "\n\n")

	if strings.TrimSpace(strataMD) != "" {
		b.WriteString("<strata_md>\n")
		b.WriteString(strataMD)
		b.WriteString("\n</strata_md>\n\n")
	}

	b.WriteString("Annotate every column listed below. For each column return:\n")
	b.WriteString("- role: one of identifier, dimension, measure, timestamp, flag\n")
	b.WriteString("- label: a title-cased human-readable name (e.g. \"Order Status\", \"Net Exposure USD\")\n")
	b.WriteString("- description: 1-3 sentences. What does this column represent? Any caveats\n")
	b.WriteString("  on interpretation? If the value is ambiguous or domain-specific, say so.\n")
	b.WriteString("- difficulty: how hard was it to determine this column's meaning?\n")
	b.WriteString("  self_evident     - name + type made it obvious\n")
	b.WriteString("  context_dependent - needed the table context to be sure\n")
	b.WriteString("  ambiguous        - uncertain even with context; a human should review\n")
	b.WriteString("  domain_dependent  - requires domain knowledge not present in the schema\n\n")

	b.WriteString("Role guidance:\n")
	b.WriteString("  identifier  - primary or foreign key, surrogate ID, natural key\n")
	b.WriteString("  dimension   - categorical attribute used for grouping or filtering\n")
	b.WriteString("  measure     - numeric value intended for aggregation\n")
	b.WriteString("  timestamp   - date, time, or datetime column\n")
	b.WriteString("  flag        - boolean or binary indicator (is_active, has_shipped, etc.)\n\n")

	pkSet := make(map[string]struct{}, len(table.PrimaryKey))
	for _, pk := range table.PrimaryKey {
		pkSet[pk] = struct{}{}
	}

	fkMap := make(map[string]string)
	for _, fk := range table.ForeignKeys {
		for i := range fk.FromColumns {
			if i < len(fk.ToColumns) {
				fkMap[fk.FromColumns[i]] = fk.ToTable + "." + fk.ToColumns[i]
			}
		}
	}

	b.WriteString("Columns to annotate:\n")
	for _, col := range table.Columns {
		nullableMarker := ""
		if col.IsNullable {
			nullableMarker = " NULLABLE"
		}

		pkMarker := ""
		if _, ok := pkSet[col.Name]; ok {
			pkMarker = " [PK]"
		}

		fkMarker := ""
		if fkTo, ok := fkMap[col.Name]; ok {
			fkMarker = " [FK→" + fkTo + "]"
		}

		commentMarker := ""
		if strings.TrimSpace(col.Comment) != "" {
			commentMarker = " // " + col.Comment
		}

		prof, ok := profile[strings.ToLower(col.Name)]
		exampleValues := "none available"
		distinctCount := "unknown"
		nullRate := "unknown"
		if ok {
			if len(prof.ExampleValues) > 0 {
				exampleValues = strings.Join(prof.ExampleValues, ", ")
			}
			distinctCount = fmt.Sprintf("%d", prof.DistinctCount)
			den := prof.DistinctCount
			if den < 1 {
				den = 1
			}
			rate := (float64(prof.NullCount) / float64(den)) * 100.0
			nullRate = fmt.Sprintf("%.1f", rate)
		}

		b.WriteString(fmt.Sprintf("- %s (%s)%s%s%s%s\n", col.Name, col.DataType, nullableMarker, pkMarker, fkMarker, commentMarker))
		b.WriteString(fmt.Sprintf("   Example values: %s\n", exampleValues))
		b.WriteString(fmt.Sprintf("   Distinct count: %s, Null rate: %s%%\n", distinctCount, nullRate))
	}

	b.WriteString("\nReturn exactly one entry per column listed. Do not add or remove columns.\n")
	b.WriteString(fmt.Sprintf("Return table_name as exactly \"%s\".\n", table.Name))

	return b.String()
}

func applyPostProcessing(result *FinePassResult, table postgres.TableInfo) {
	expected := len(table.Columns)
	if len(result.Columns) != expected {
		log.Printf("fine pass: table %s: expected %d columns, got %d - padding/truncating", table.Name, expected, len(result.Columns))
	}

	validByLower := make(map[string]string, len(table.Columns))
	for _, col := range table.Columns {
		validByLower[strings.ToLower(col.Name)] = col.Name
	}

	normalized := make(map[string]ColumnResult, len(table.Columns))
	for _, col := range result.Columns {
		lower := strings.ToLower(col.ColumnName)
		canonical, ok := validByLower[lower]
		if !ok {
			log.Printf("fine pass: table %s: unknown column %s - discarding", table.Name, col.ColumnName)
			continue
		}

		col.TableName = table.Name
		col.ColumnName = canonical
		col.NeedsReview = col.Difficulty == "ambiguous" || col.Difficulty == "domain_dependent"

		if _, exists := normalized[lower]; !exists {
			normalized[lower] = col
		}
	}

	ordered := make([]ColumnResult, 0, expected)
	for _, tc := range table.Columns {
		lower := strings.ToLower(tc.Name)
		if col, ok := normalized[lower]; ok {
			ordered = append(ordered, col)
			continue
		}
		ordered = append(ordered, defaultColumnResult(table.Name, tc.Name))
	}

	if len(ordered) > expected {
		ordered = ordered[:expected]
	}

	result.TableName = table.Name
	result.Columns = ordered
}

func defaultColumnResult(tableName, columnName string) ColumnResult {
	return ColumnResult{
		TableName:   tableName,
		ColumnName:  columnName,
		Role:        "dimension",
		Label:       titleFromColumn(columnName),
		Description: "Could not be inferred.",
		Difficulty:  "ambiguous",
		NeedsReview: true,
	}
}

func titleFromColumn(columnName string) string {
	parts := strings.Fields(strings.ReplaceAll(columnName, "_", " "))
	for i := range parts {
		if len(parts[i]) == 0 {
			continue
		}
		parts[i] = strings.ToUpper(parts[i][:1]) + strings.ToLower(parts[i][1:])
	}
	return strings.Join(parts, " ")
}
