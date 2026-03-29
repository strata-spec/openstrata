package postgres

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

var (
	piiPatterns = []struct {
		label   string
		pattern *regexp.Regexp
	}{
		{"email", regexp.MustCompile(`(?i)[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`)},
		{"phone", regexp.MustCompile(`(\+1)?[\s.\-]?\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}`)},
		{"ssn", regexp.MustCompile(`\d{3}-\d{2}-\d{4}`)},
		{"cc", regexp.MustCompile(`\d{4}[\s\-]\d{4}[\s\-]\d{4}[\s\-]\d{4}`)},
	}
)

// ColumnProfile holds statistics derived from sample profiling.
type ColumnProfile struct {
	TableName           string
	ColumnName          string
	DistinctCount       int64
	NullCount           int64
	ExampleValues       []string
	ValidValues         []string
	CardinalityCategory string
	DurationMS          int64
}

// validValuesEnumLimit is the maximum number of distinct values fetched for
// low-cardinality text columns when populating ValidValues.
const validValuesEnumLimit = 50

type profileContextKey string

const profileTimeoutKey profileContextKey = "profile_timeout_secs"

// ProfileProgress receives per-column and per-table events during profiling.
// Implementations must be safe to call from multiple goroutines.
type ProfileProgress interface {
	// ColumnProfiled is called after each column is profiled.
	ColumnProfiled(tableName, columnName string, done, total int)

	// TableSkipped is called when profiling is skipped for a table.
	TableSkipped(tableName string, reason string)
}

// ProfileProgressWithStats is an optional extension used by callers that need
// the full column profile payload in callbacks.
type ProfileProgressWithStats interface {
	ColumnProfiledWithStats(tableName, columnName string, profile ColumnProfile, done, total int)
}

// NoOpProfileProgress discards all profiling progress events.
type NoOpProfileProgress struct{}

func (NoOpProfileProgress) ColumnProfiled(_, _ string, _, _ int) {}
func (NoOpProfileProgress) TableSkipped(_, _ string)             {}

// WithProfileTimeout stores per-table profiling timeout (seconds) in context.
// A value <= 0 means no timeout.
func WithProfileTimeout(ctx context.Context, secs int) context.Context {
	return context.WithValue(ctx, profileTimeoutKey, secs)
}

// Profile collects sample data statistics for all columns in the given tables.
// PII patterns are redacted from example values before they are stored.
func Profile(ctx context.Context, pool *pgxpool.Pool, tables []TableInfo, progress ProfileProgress) (map[string]ColumnProfile, error) {
	if progress == nil {
		progress = NoOpProfileProgress{}
	}

	totalColumns := 0
	for _, t := range tables {
		totalColumns += len(t.Columns)
	}

	doneColumns := 0
	var doneMu sync.Mutex
	nextDone := func() int {
		doneMu.Lock()
		defer doneMu.Unlock()
		doneColumns++
		return doneColumns
	}

	profileTimeoutSecs := profileTimeoutFromContext(ctx)
	useTableSample := poolSupportsTableSample(pool)

	profiles := make(map[string]ColumnProfile)
	var mu sync.Mutex

	tableLimiter := make(chan struct{}, 4)
	g, gctx := errgroup.WithContext(ctx)

	for _, table := range tables {
		t := table
		g.Go(func() (err error) {
			select {
			case tableLimiter <- struct{}{}:
			case <-gctx.Done():
				return gctx.Err()
			}
			defer func() {
				<-tableLimiter
			}()

			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("profile: panic while profiling table %s: %v", t.Name, r)
				}
			}()

			tableCtx := gctx
			if profileTimeoutSecs > 0 {
				var cancel context.CancelFunc
				tableCtx, cancel = context.WithTimeout(gctx, time.Duration(profileTimeoutSecs)*time.Second)
				defer cancel()
			}

			rowsByColumn, fetchErr := fetchTableSampleRows(tableCtx, pool, t, useTableSample)
			if fetchErr != nil {
				if errors.Is(fetchErr, context.Canceled) || errors.Is(fetchErr, context.DeadlineExceeded) {
					if errors.Is(fetchErr, context.DeadlineExceeded) {
						progress.TableSkipped(t.Name, "profiling timeout")
						for _, col := range t.Columns {
							profile := ColumnProfile{
								TableName:           t.Name,
								ColumnName:          col.Name,
								ExampleValues:       []string{"[profiling timeout]"},
								CardinalityCategory: "unknown",
							}

							mu.Lock()
							profiles[t.Name+"."+col.Name] = profile
							mu.Unlock()
						}
						return nil
					}
					return gctx.Err()
				}

				progress.TableSkipped(t.Name, "profiling query failed")
				for _, col := range t.Columns {
					profile := ColumnProfile{
						TableName:           t.Name,
						ColumnName:          col.Name,
						ExampleValues:       []string{"[profiling error]"},
						CardinalityCategory: "unknown",
					}
					mu.Lock()
					profiles[t.Name+"."+col.Name] = profile
					mu.Unlock()
				}
				return nil
			}

			for _, col := range t.Columns {
				start := time.Now()
				profile := profileColumnFromSample(t.Name, col.Name, rowsByColumn[col.Name])
				profile.DurationMS = time.Since(start).Milliseconds()

				mu.Lock()
				profiles[t.Name+"."+col.Name] = profile
				mu.Unlock()

				done := nextDone()
				progress.ColumnProfiled(t.Name, col.Name, done, totalColumns)
				if p, ok := progress.(ProfileProgressWithStats); ok {
					p.ColumnProfiledWithStats(t.Name, col.Name, profile, done, totalColumns)
				}
			}

			// Enumerate all distinct values for low-cardinality text columns so
			// that the semantic layer can surface valid_values to LLM query agents.
			// Only runs when the sample shows fewer than validValuesEnumLimit
			// distinct values to avoid enumerating high-cardinality free-text columns.
			for _, col := range t.Columns {
				if !isTextLikeType(col.DataType) {
					continue
				}
				profileKey := t.Name + "." + col.Name
				mu.Lock()
				prof := profiles[profileKey]
				mu.Unlock()
				if prof.DistinctCount <= 0 || prof.DistinctCount >= validValuesEnumLimit {
					continue
				}
				vals, enumErr := enumerateDistinctValues(tableCtx, pool, t, col.Name)
				if enumErr != nil {
					log.Printf("profile: table %s.%s column %s: enumerate distinct values: %v", t.Schema, t.Name, col.Name, enumErr)
					continue
				}
				prof.ValidValues = vals
				mu.Lock()
				profiles[profileKey] = prof
				mu.Unlock()
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, err
	}

	return profiles, nil
}

// RedactPII replaces values matching PII patterns with [REDACTED:<type>].
// Exported for testing.
func RedactPII(value string) string {
	for _, pii := range piiPatterns {
		loc := pii.pattern.FindStringIndex(value)
		if loc != nil {
			return value[:loc[0]] + "[REDACTED:" + pii.label + "]" + value[loc[1]:]
		}
	}

	return value
}

func cardinalityCategory(distinctCount int64) string {
	if distinctCount < 100 {
		return "low"
	}
	if distinctCount < 10000 {
		return "medium"
	}
	return "high"
}

// isTextLikeType returns true for column data types that store human-readable
// categorical text and are candidates for valid_values enumeration.
func isTextLikeType(dataType string) bool {
	dt := strings.ToLower(strings.TrimSpace(dataType))
	return dt == "text" ||
		dt == "name" ||
		strings.HasPrefix(dt, "character varying") ||
		strings.HasPrefix(dt, "varchar") ||
		strings.HasPrefix(dt, "character(") ||
		strings.HasPrefix(dt, "char(")
}

// enumerateDistinctValues fetches up to validValuesEnumLimit distinct non-null
// values for a single column, ordered lexicographically.
func enumerateDistinctValues(ctx context.Context, pool *pgxpool.Pool, table TableInfo, columnName string) ([]string, error) {
	tableIdent := pgx.Identifier{table.Schema, table.Name}.Sanitize()
	colIdent := pgx.Identifier{columnName}.Sanitize()
	query := fmt.Sprintf(
		"SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL ORDER BY 1 LIMIT %d",
		colIdent, tableIdent, colIdent, validValuesEnumLimit,
	)

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var vals []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}

	return vals, rows.Err()
}

func profileTimeoutFromContext(ctx context.Context) int {
	if ctx == nil {
		return 0
	}
	v := ctx.Value(profileTimeoutKey)
	if secs, ok := v.(int); ok {
		return secs
	}
	return 0
}

func fetchTableSampleRows(ctx context.Context, pool *pgxpool.Pool, table TableInfo, useTableSample bool) (map[string][]any, error) {
	tableIdent := pgx.Identifier{table.Schema, table.Name}.Sanitize()
	query := "SELECT * FROM " + tableIdent + " LIMIT $1"
	args := []any{10000}
	if useTableSample {
		query = "SELECT * FROM " + tableIdent + " TABLESAMPLE BERNOULLI($1) LIMIT $2"
		args = []any{1.0, 10000}
	}

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rowValues := make(map[string][]any, len(table.Columns))
	for _, col := range table.Columns {
		rowValues[col.Name] = make([]any, 0, 1024)
	}

	fieldOrder := make([]string, 0, len(rows.FieldDescriptions()))
	for _, fd := range rows.FieldDescriptions() {
		fieldOrder = append(fieldOrder, string(fd.Name))
	}

	for rows.Next() {
		vals, valuesErr := rows.Values()
		if valuesErr != nil {
			return nil, valuesErr
		}

		for i := range vals {
			if i >= len(fieldOrder) {
				continue
			}
			colName := fieldOrder[i]
			if _, ok := rowValues[colName]; ok {
				rowValues[colName] = append(rowValues[colName], vals[i])
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return rowValues, nil
}

func profileColumnFromSample(tableName, columnName string, values []any) ColumnProfile {
	profile := ColumnProfile{
		TableName:           tableName,
		ColumnName:          columnName,
		ExampleValues:       []string{},
		CardinalityCategory: "low",
	}

	if len(values) == 0 {
		profile.NullCount = 0
		profile.DistinctCount = 0
		profile.CardinalityCategory = "low"
		return profile
	}

	distinct := make(map[string]struct{})
	examples := make(map[string]struct{})
	unsupported := false

	for _, raw := range values {
		if raw == nil {
			profile.NullCount++
			continue
		}

		textVal, ok := valueToString(raw)
		if !ok {
			unsupported = true
			continue
		}

		distinct[textVal] = struct{}{}
		if len(examples) < 10 {
			examples[RedactPII(textVal)] = struct{}{}
		}
	}

	profile.DistinctCount = int64(len(distinct))
	profile.CardinalityCategory = cardinalityCategory(profile.DistinctCount)

	if unsupported {
		profile.ExampleValues = []string{"[unsupported type]"}
		return profile
	}

	if len(examples) == 0 {
		profile.ExampleValues = []string{}
		return profile
	}

	vals := make([]string, 0, len(examples))
	for v := range examples {
		vals = append(vals, v)
	}
	sort.Strings(vals)
	if len(vals) > 10 {
		vals = vals[:10]
	}
	profile.ExampleValues = vals

	return profile
}

func valueToString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case []byte:
		return string(t), true
	case fmt.Stringer:
		return t.String(), true
	case int:
		return fmt.Sprintf("%d", t), true
	case int8:
		return fmt.Sprintf("%d", t), true
	case int16:
		return fmt.Sprintf("%d", t), true
	case int32:
		return fmt.Sprintf("%d", t), true
	case int64:
		return fmt.Sprintf("%d", t), true
	case uint:
		return fmt.Sprintf("%d", t), true
	case uint8:
		return fmt.Sprintf("%d", t), true
	case uint16:
		return fmt.Sprintf("%d", t), true
	case uint32:
		return fmt.Sprintf("%d", t), true
	case uint64:
		return fmt.Sprintf("%d", t), true
	case float32:
		return fmt.Sprintf("%g", t), true
	case float64:
		return fmt.Sprintf("%g", t), true
	case bool:
		if t {
			return "true", true
		}
		return "false", true
	case time.Time:
		return t.Format(time.RFC3339Nano), true
	default:
		return "", false
	}
}
