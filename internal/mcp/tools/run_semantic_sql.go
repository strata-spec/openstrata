package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	mcp "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/strata-spec/openstrata/internal/smif"
)

var mutatingSQLPattern = regexp.MustCompile(`(?i)\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE)\b`)

// RunSemanticSQL returns the run_semantic_sql MCP tool definition and handler.
func RunSemanticSQL(getModel func() *smif.SemanticModel, pool *pgxpool.Pool) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool(
		"run_semantic_sql",
		mcp.WithDescription("Execute a semantic SQL query against the database"),
		mcp.WithString("sql", mcp.Description("Semantic SQL query"), mcp.Required()),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		semanticSQL, _ := request.Params.Arguments["sql"].(string)
		semanticSQL = strings.TrimSpace(semanticSQL)
		if semanticSQL == "" {
			return nil, fmt.Errorf("sql is required")
		}

		if pool == nil {
			payload := map[string]any{
				"error": "run_semantic_sql requires a live database connection",
				"hint":  "Restart serve with --db <connection_string>",
			}
			b, err := json.Marshal(payload)
			if err != nil {
				return nil, err
			}
			return mcp.NewToolResultText(string(b)), nil
		}

		if mutatingSQLPattern.MatchString(semanticSQL) {
			return mcp.NewToolResultError("run_semantic_sql is read-only; DDL and DML are not permitted"), nil
		}

		resolvedSQL := resolveModelNames(getModel(), semanticSQL)
		execCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		rows, err := pool.Query(execCtx, resolvedSQL)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		defer rows.Close()

		fieldDescs := rows.FieldDescriptions()
		colNames := make([]string, len(fieldDescs))
		for i, fd := range fieldDescs {
			colNames[i] = string(fd.Name)
		}

		resultRows := make([]map[string]any, 0)
		truncated := false

		for rows.Next() {
			if len(resultRows) >= 1000 {
				truncated = true
				break
			}

			values := make([]any, len(colNames))
			scanTargets := make([]any, len(values))
			for i := range values {
				scanTargets[i] = &values[i]
			}

			if err := rows.Scan(scanTargets...); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}

			row := make(map[string]any, len(colNames))
			for i, name := range colNames {
				if b, ok := values[i].([]byte); ok {
					row[name] = string(b)
					continue
				}
				row[name] = values[i]
			}
			resultRows = append(resultRows, row)
		}

		if err := rows.Err(); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		payload := map[string]any{
			"rows":         resultRows,
			"row_count":    len(resultRows),
			"resolved_sql": resolvedSQL,
			"semantic_sql": semanticSQL,
		}
		if truncated {
			payload["truncated"] = true
			payload["total_row_count_estimate"] = 1000
		}

		// TODO(loop4): log query outcome for Loop 4 implicit feedback

		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		return mcp.NewToolResultText(string(b)), nil
	}

	return tool, handler
}

func resolveModelNames(model *smif.SemanticModel, sql string) string {
	if model == nil {
		return sql
	}

	type replacement struct {
		ModelID string
		Table   string
	}
	replacements := make([]replacement, 0, len(model.Models)*2)
	for _, m := range model.Models {
		table := strings.TrimSpace(m.PhysicalSource.Table)
		if table == "" {
			table = m.Name
		}
		if strings.TrimSpace(table) == "" {
			continue
		}
		replacements = append(replacements, replacement{ModelID: m.ModelID, Table: table})
		if m.Name != "" && m.Name != m.ModelID {
			replacements = append(replacements, replacement{ModelID: m.Name, Table: table})
		}
	}

	sort.Slice(replacements, func(i, j int) bool {
		return len(replacements[i].ModelID) > len(replacements[j].ModelID)
	})

	out := sql
	for _, r := range replacements {
		pattern := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(r.ModelID) + `\b`)
		out = pattern.ReplaceAllString(out, r.Table)
	}
	return out
}
