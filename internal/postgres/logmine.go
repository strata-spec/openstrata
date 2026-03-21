package postgres

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrLogMiningUnavailable is returned when pg_stat_statements cannot be queried.
var ErrLogMiningUnavailable = errors.New("pg_stat_statements is not available or not accessible")

const maxStatements = 5000

const availabilityQuery = `SELECT COUNT(*) FROM pg_stat_statements LIMIT 1`

const statementsQuery = `
SELECT
    query,
    calls
FROM pg_stat_statements
WHERE query NOT LIKE '/*strata*/%'
ORDER BY calls DESC
LIMIT 5000`

var tokenPattern = regexp.MustCompile(`'[^']*'|\$\d+|<=|>=|<>|!=|[(),=*<>]|[A-Za-z_][A-Za-z0-9_$.]*|\d+(?:\.\d+)?`)

type columnRef struct {
	table  string
	column string
	clause string
}

type pendingColumnRef struct {
	token  string
	clause string
}

// Mine extracts usage profiles from pg_stat_statements.
// Only called when --enable-log-mining is set.
// Returns ErrLogMiningUnavailable if pg_stat_statements is not accessible.
func Mine(ctx context.Context, pool *pgxpool.Pool) ([]UsageProfile, error) {
	if err := checkAvailability(ctx, pool); err != nil {
		return nil, err
	}

	rows, err := pool.Query(ctx, statementsQuery)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, ctx.Err()
		}
		return nil, ErrLogMiningUnavailable
	}
	defer rows.Close()

	profiles := make(map[string]UsageProfile)

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		var queryText string
		var calls int64
		if scanErr := rows.Scan(&queryText, &calls); scanErr != nil {
			if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, ctx.Err()
			}
			continue
		}

		if _, _, extractErr := ExtractColumnUsage(queryText); extractErr != nil {
			continue
		}

		_, refs, parseErr := parseQueryUsage(queryText)
		if parseErr != nil {
			continue
		}

		seen := make(map[string]struct{})
		for _, ref := range refs {
			key := strings.ToLower(ref.table + "." + ref.column)
			clauseKey := key + ":" + ref.clause
			if _, ok := seen[clauseKey]; ok {
				continue
			}
			seen[clauseKey] = struct{}{}

			p := profiles[key]
			p.TableName = strings.ToLower(ref.table)
			p.ColumnName = strings.ToLower(ref.column)

			switch ref.clause {
			case "SELECT":
				p.SelectCount += calls
			case "WHERE":
				p.WhereCount += calls
			case "GROUP_BY":
				p.GroupByCount += calls
			case "JOIN":
				p.JoinCount += calls
			}
			p.SupportingQueries += calls
			profiles[key] = p
		}
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, ctx.Err()
		}
		return nil, nil
	}

	out := make([]UsageProfile, 0, len(profiles))
	for _, p := range profiles {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TableName == out[j].TableName {
			return out[i].ColumnName < out[j].ColumnName
		}
		return out[i].TableName < out[j].TableName
	})

	return out, nil
}

// checkAvailability executes the sentinel query and returns
// ErrLogMiningUnavailable if it fails.
func checkAvailability(ctx context.Context, pool *pgxpool.Pool) error {
	var count int64
	if err := pool.QueryRow(ctx, availabilityQuery).Scan(&count); err != nil {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ctx.Err()
		}
		return ErrLogMiningUnavailable
	}
	return nil
}

// ExtractColumnUsage is stubbed for Loop 3 (query log enrichment).
// TODO(loop3): Implement when strata enrich command is added.
func ExtractColumnUsage(queryText string) (tables []string, columns []string, err error) {
	tbls, refs, parseErr := parseQueryUsage(queryText)
	if parseErr != nil {
		return nil, nil, parseErr
	}

	colSet := make(map[string]struct{})
	for _, ref := range refs {
		key := strings.ToLower(ref.table + "." + ref.column)
		colSet[key] = struct{}{}
	}

	tables = make([]string, 0, len(tbls))
	for table := range tbls {
		tables = append(tables, table)
	}
	columns = make([]string, 0, len(colSet))
	for col := range colSet {
		columns = append(columns, col)
	}

	sort.Strings(tables)
	sort.Strings(columns)

	return tables, columns, nil
}

func parseQueryUsage(queryText string) (map[string]struct{}, []columnRef, error) {
	tokens := tokenizeQuery(queryText)
	if len(tokens) == 0 {
		return map[string]struct{}{}, nil, nil
	}

	if strings.EqualFold(tokens[0], "WITH") {
		return map[string]struct{}{}, nil, nil
	}

	tableSet := make(map[string]struct{})
	aliasToTable := make(map[string]string)
	refs := make([]columnRef, 0)
	pending := make([]pendingColumnRef, 0)

	clause := ""
	depth := 0
	primaryTable := ""

	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		upper := strings.ToUpper(tok)

		if tok == "(" {
			depth++
			continue
		}
		if tok == ")" {
			if depth > 0 {
				depth--
			}
			continue
		}

		if depth > 0 {
			continue
		}

		switch upper {
		case "UNION", "INTERSECT", "EXCEPT":
			return tableSet, refs, nil
		case "SELECT":
			clause = "SELECT"
			continue
		case "FROM":
			clause = "FROM"
			continue
		case "WHERE":
			clause = "WHERE"
			continue
		case "JOIN":
			clause = "JOIN"
			continue
		case "ON":
			clause = "JOIN_ON"
			continue
		case "GROUP":
			if i+1 < len(tokens) && strings.EqualFold(tokens[i+1], "BY") {
				clause = "GROUP_BY"
				i++
				continue
			}
		case "ORDER":
			if i+1 < len(tokens) && strings.EqualFold(tokens[i+1], "BY") {
				clause = "ORDER_BY"
				i++
				continue
			}
		case "HAVING":
			clause = "HAVING"
			continue
		case "SET":
			clause = "SET"
			continue
		case "INSERT", "UPDATE", "DELETE":
			clause = "DML_VERB"
			continue
		case "OVER":
			clause = "ORDER_BY"
			continue
		}

		if clause == "FROM" || clause == "JOIN" {
			if isIgnorableTableToken(tok) {
				continue
			}

			table := normalizeTableName(tok)
			if table == "" {
				continue
			}
			tableSet[table] = struct{}{}
			if primaryTable == "" {
				primaryTable = table
			}

			j := i + 1
			if j < len(tokens) && strings.EqualFold(tokens[j], "AS") {
				j++
			}
			if j < len(tokens) {
				aliasTok := tokens[j]
				if isIdentifier(aliasTok) && !isSQLKeyword(aliasTok) {
					aliasToTable[strings.ToLower(aliasTok)] = table
					i = j
				}
			}
			continue
		}

		switch clause {
		case "SELECT":
			if i+1 < len(tokens) && tokens[i+1] == "(" {
				continue
			}
			if len(tableSet) == 0 {
				if isIdentifier(tok) || strings.Contains(tok, ".") {
					pending = append(pending, pendingColumnRef{token: tok, clause: "SELECT"})
				}
				continue
			}
			if ref, ok := extractColumnRef(tok, aliasToTable, primaryTable, len(tableSet)); ok {
				ref.clause = "SELECT"
				refs = append(refs, ref)
			}
		case "WHERE":
			if i+1 < len(tokens) && tokens[i+1] == "(" {
				continue
			}
			if !isIdentifier(tok) && !strings.Contains(tok, ".") {
				continue
			}
			next := nextMeaningfulToken(tokens, i+1)
			if !isComparisonToken(next) {
				continue
			}
			if len(tableSet) == 0 {
				pending = append(pending, pendingColumnRef{token: tok, clause: "WHERE"})
				continue
			}
			if ref, ok := extractColumnRef(tok, aliasToTable, primaryTable, len(tableSet)); ok {
				ref.clause = "WHERE"
				refs = append(refs, ref)
			}
		case "JOIN_ON":
			if len(tableSet) == 0 {
				if isIdentifier(tok) || strings.Contains(tok, ".") {
					pending = append(pending, pendingColumnRef{token: tok, clause: "JOIN"})
				}
				continue
			}
			if ref, ok := extractColumnRef(tok, aliasToTable, primaryTable, len(tableSet)); ok {
				ref.clause = "JOIN"
				refs = append(refs, ref)
			}
		case "GROUP_BY":
			if i+1 < len(tokens) && tokens[i+1] == "(" {
				continue
			}
			if len(tableSet) == 0 {
				if isIdentifier(tok) || strings.Contains(tok, ".") {
					pending = append(pending, pendingColumnRef{token: tok, clause: "GROUP_BY"})
				}
				continue
			}
			if ref, ok := extractColumnRef(tok, aliasToTable, primaryTable, len(tableSet)); ok {
				ref.clause = "GROUP_BY"
				refs = append(refs, ref)
			}
		}
	}

	for _, pend := range pending {
		if ref, ok := extractColumnRef(pend.token, aliasToTable, primaryTable, len(tableSet)); ok {
			ref.clause = pend.clause
			refs = append(refs, ref)
		}
	}

	return tableSet, refs, nil
}

func tokenizeQuery(queryText string) []string {
	trimmed := strings.TrimSpace(queryText)
	if trimmed == "" {
		return nil
	}
	return tokenPattern.FindAllString(trimmed, -1)
}

func normalizeTableName(token string) string {
	trimmed := strings.Trim(token, "\"`")
	if trimmed == "" {
		return ""
	}
	parts := strings.Split(trimmed, ".")
	name := parts[len(parts)-1]
	if name == "" || !isIdentifier(name) {
		return ""
	}
	return strings.ToLower(name)
}

func extractColumnRef(token string, aliasToTable map[string]string, defaultTable string, tableCount int) (columnRef, bool) {
	trimmed := strings.Trim(token, "\"`")
	if trimmed == "" || trimmed == "*" || strings.HasPrefix(trimmed, "$") || isNumericLiteral(trimmed) || isStringLiteral(trimmed) {
		return columnRef{}, false
	}

	if !strings.Contains(trimmed, ".") {
		if !isIdentifier(trimmed) {
			return columnRef{}, false
		}
		if tableCount == 1 && defaultTable != "" {
			return columnRef{table: defaultTable, column: strings.ToLower(trimmed)}, true
		}
		return columnRef{}, false
	}

	parts := strings.Split(trimmed, ".")
	if len(parts) != 2 {
		return columnRef{}, false
	}

	left := strings.ToLower(strings.Trim(parts[0], "\"`"))
	right := strings.ToLower(strings.Trim(parts[1], "\"`"))
	if !isIdentifier(left) || !isIdentifier(right) {
		return columnRef{}, false
	}

	if table, ok := aliasToTable[left]; ok {
		return columnRef{table: table, column: right}, true
	}

	return columnRef{table: left, column: right}, true
}

func nextMeaningfulToken(tokens []string, start int) string {
	for i := start; i < len(tokens); i++ {
		tok := tokens[i]
		if tok == "," || tok == "(" || tok == ")" {
			continue
		}
		return tok
	}
	return ""
}

func isIgnorableTableToken(token string) bool {
	upper := strings.ToUpper(token)
	return upper == "LATERAL" || upper == "ONLY"
}

func isComparisonToken(token string) bool {
	upper := strings.ToUpper(token)
	switch upper {
	case "=", "!=", "<>", "<", ">", "<=", ">=", "LIKE", "IN", "IS", "BETWEEN", "ANY", "ALL":
		return true
	default:
		return false
	}
}

func isSQLKeyword(token string) bool {
	upper := strings.ToUpper(token)
	switch upper {
	case "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "HAVING", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "ON", "AS", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "SET", "INSERT", "UPDATE", "DELETE", "VALUES", "RETURNING", "AND", "OR", "NOT":
		return true
	default:
		return false
	}
}

func isIdentifier(token string) bool {
	if token == "" {
		return false
	}
	for i, r := range token {
		if i == 0 {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_') {
				return false
			}
			continue
		}
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return true
}

func isNumericLiteral(token string) bool {
	if token == "" {
		return false
	}
	for _, r := range token {
		if (r < '0' || r > '9') && r != '.' {
			return false
		}
	}
	return true
}

func isStringLiteral(token string) bool {
	return strings.HasPrefix(token, "'") && strings.HasSuffix(token, "'") && len(token) >= 2
}
