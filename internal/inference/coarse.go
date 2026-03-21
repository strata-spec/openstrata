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

var domainSchema = []byte(`{
  "type": "object",
  "required": ["name", "description", "key_concepts", "temporal_model", "known_gotchas"],
  "additionalProperties": false,
  "properties": {
    "name":           {"type": "string"},
    "description":    {"type": "string"},
    "key_concepts":   {"type": "array",  "items": {"type": "string"}},
    "temporal_model": {"type": "string"},
    "known_gotchas":  {"type": "array",  "items": {"type": "string"}}
  }
}`)

var tableSchema = []byte(`{
  "type": "object",
  "required": ["table_name", "description", "grain"],
  "additionalProperties": false,
  "properties": {
    "table_name":  {"type": "string"},
    "description": {"type": "string"},
    "grain":       {"type": "string"}
  }
}`)

type DomainResult struct {
	Name          string   `json:"name"`
	Description   string   `json:"description"`
	KeyConcepts   []string `json:"key_concepts"`
	TemporalModel string   `json:"temporal_model"`
	KnownGotchas  []string `json:"known_gotchas"`
}

type TableResult struct {
	TableName   string `json:"table_name"`
	Description string `json:"description"`
	Grain       string `json:"grain"`
}

// RunDomainPass executes a single LLM call to produce a domain-level
// description of the database.
func RunDomainPass(ctx context.Context, llmClient llm.LLMClient, tables []postgres.TableInfo, strataMD string) (*DomainResult, error) {
	prompt := buildDomainPrompt(tables, strataMD)

	var out DomainResult
	if err := llmClient.GenerateStructured(ctx, prompt, domainSchema, &out); err != nil {
		return nil, fmt.Errorf("coarse pass: domain call failed: %w", err)
	}
	return &out, nil
}

// RunTablePass executes one LLM call per table to produce table-level
// descriptions and grain statements.
// Tables are processed concurrently with a cap of 3 simultaneous LLM calls.
func RunTablePass(ctx context.Context, llmClient llm.LLMClient, tables []postgres.TableInfo, domain *DomainResult, strataMD string) ([]TableResult, error) {
	results := make([]TableResult, len(tables))
	success := make([]bool, len(tables))
	var mu sync.Mutex

	sem := make(chan struct{}, 3)
	g, gctx := errgroup.WithContext(ctx)

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

			prompt := buildTablePrompt(t, domain, strataMD)
			var tr TableResult
			if err := llmClient.GenerateStructured(gctx, prompt, tableSchema, &tr); err != nil {
				log.Printf("coarse pass: table %s: %v - skipping", t.Name, err)
				return nil
			}

			mu.Lock()
			results[idx] = tr
			success[idx] = true
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	final := make([]TableResult, 0, len(tables))
	for i, ok := range success {
		if ok {
			final = append(final, results[i])
		}
	}
	if len(final) == 0 {
		return nil, fmt.Errorf("coarse pass: all table calls failed")
	}

	return final, nil
}

func buildDomainPrompt(tables []postgres.TableInfo, strataMD string) string {
	var b strings.Builder
	b.WriteString("You are a senior data engineer analyzing a PostgreSQL database.\n\n")
	b.WriteString("Your task: produce a concise domain description of this database based\n")
	b.WriteString("on the table inventory below. Focus on what the database is FOR - its\n")
	b.WriteString("business purpose, key entities, and any temporal characteristics\n")
	b.WriteString("(is it event-based, snapshot-based, or slowly-changing?).\n\n")

	if strings.TrimSpace(strataMD) != "" {
		b.WriteString("<strata_md>\n")
		b.WriteString(strataMD)
		b.WriteString("\n</strata_md>\n\n")
	}

	b.WriteString("<table_inventory>\n")
	for _, t := range tables {
		pk := "none"
		if len(t.PrimaryKey) > 0 {
			pk = strings.Join(t.PrimaryKey, ", ")
		}
		b.WriteString(fmt.Sprintf("- %s: %d columns, PKs: %s\n", t.Name, len(t.Columns), pk))
	}
	b.WriteString("</table_inventory>\n\n")

	b.WriteString("Rules:\n")
	b.WriteString("- description: 2-4 sentences. Business-focused, not technical.\n")
	b.WriteString("- key_concepts: 3-7 strings naming the central entities or processes.\n")
	b.WriteString("- temporal_model: one of \"event-based\", \"snapshot\", \"slowly-changing-dimension\",\n")
	b.WriteString("  \"reference-data\", \"mixed\", or \"unknown\".\n")
	b.WriteString("- known_gotchas: any naming inconsistencies, suspected denormalisation,\n")
	b.WriteString("  or ambiguous table purposes visible from the table names alone.\n")
	b.WriteString("  Empty array if none apparent.\n")
	b.WriteString("- If strata_md is empty, base your answer entirely on the table names.\n")

	return b.String()
}

func buildTablePrompt(table postgres.TableInfo, domain *DomainResult, strataMD string) string {
	var b strings.Builder
	b.WriteString("You are a senior data engineer analyzing a PostgreSQL database.\n\n")
	if domain != nil {
		b.WriteString("Domain context:\n")
		b.WriteString(fmt.Sprintf("%s: %s\n\n", domain.Name, domain.Description))
	}

	if strings.TrimSpace(strataMD) != "" {
		b.WriteString("<strata_md>\n")
		b.WriteString(strataMD)
		b.WriteString("\n</strata_md>\n\n")
	}

	b.WriteString(fmt.Sprintf("Your task: describe the table \"%s\" based on the information below.\n\n", table.Name))
	b.WriteString(fmt.Sprintf("Columns (%d):\n", len(table.Columns)))

	pkSet := make(map[string]struct{}, len(table.PrimaryKey))
	for _, pk := range table.PrimaryKey {
		pkSet[pk] = struct{}{}
	}

	fkMap := make(map[string]string)
	for _, fk := range table.ForeignKeys {
		for i := range fk.FromColumns {
			if i < len(fk.ToColumns) {
				fkMap[fk.FromColumns[i]] = fmt.Sprintf("%s.%s", fk.ToTable, fk.ToColumns[i])
			}
		}
	}

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
		if fkDest, ok := fkMap[col.Name]; ok {
			fkMarker = " [FK->" + fkDest + "]"
		}
		commentMarker := ""
		if strings.TrimSpace(col.Comment) != "" {
			commentMarker = " // " + col.Comment
		}

		b.WriteString(fmt.Sprintf("  - %s (%s)%s%s%s%s\n", col.Name, col.DataType, nullableMarker, pkMarker, fkMarker, commentMarker))
	}
	b.WriteString("\nForeign keys:\n")
	if len(table.ForeignKeys) == 0 {
		b.WriteString("  - none\n")
	} else {
		for _, fk := range table.ForeignKeys {
			b.WriteString(fmt.Sprintf("  - %s: %s -> %s.%s\n", fk.ConstraintName, strings.Join(fk.FromColumns, ", "), fk.ToTable, strings.Join(fk.ToColumns, ", ")))
		}
	}

	b.WriteString("\nRules:\n")
	b.WriteString("- description: 2-4 sentences. What does this table represent? Who/what\n")
	b.WriteString("  is the subject of each row? What is it used for?\n")
	b.WriteString("- grain: complete the sentence \"One row per ___.\" Be specific.\n")
	b.WriteString("  Example: \"one row per order line item\" not \"one row per record\".\n")
	b.WriteString("- table_name: return exactly the table name you were given.\n")
	b.WriteString("- If strata_md contains relevant context for this table, use it.\n")

	return b.String()
}
