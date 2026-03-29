package postgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
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
	CardinalityCategory string
	DurationMS          int64
}

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

// profileBatchSize is the number of tables sent to the database in a single
// pgx.Batch round-trip. Keeping it moderate bounds peak memory and avoids
// overwhelming a connection with too many simultaneous result sets.
const profileBatchSize = 8

// sampleRowLimit is the maximum number of rows fetched per table for profiling.
const sampleRowLimit = 10000

// Profile collects sample data statistics for all columns in the given tables.
// PII patterns are redacted from example values before they are stored.
// Tables are processed in batches of profileBatchSize using pgx.Batch to
// reduce round-trips; up to 4 batches run concurrently.
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

	batchLimiter := make(chan struct{}, 4)
	g, gctx := errgroup.WithContext(ctx)

	for batchStart := 0; batchStart < len(tables); batchStart += profileBatchSize {
		batchEnd := batchStart + profileBatchSize
		if batchEnd > len(tables) {
			batchEnd = len(tables)
		}
		batchTables := tables[batchStart:batchEnd]

		g.Go(func() (err error) {
			select {
			case batchLimiter <- struct{}{}:
			case <-gctx.Done():
				return gctx.Err()
			}
			defer func() { <-batchLimiter }()

			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("profile: panic while profiling batch starting at %s: %v", batchTables[0].Name, r)
				}
			}()

			batchCtx := gctx
			if profileTimeoutSecs > 0 {
				var cancel context.CancelFunc
				// Allow profileTimeoutSecs per table in the batch so that the
				// per-table budget is preserved regardless of batch size.
				batchCtx, cancel = context.WithTimeout(gctx, time.Duration(profileTimeoutSecs)*time.Duration(len(batchTables))*time.Second)
				defer cancel()
			}

			// rowsByTable[i] holds column→values for batchTables[i].
			// A nil entry means the query for that table failed.
			rowsByTable, fetchErr := fetchTableSampleRowsBatch(batchCtx, pool, batchTables, useTableSample)
			if fetchErr != nil {
				if errors.Is(fetchErr, context.Canceled) || errors.Is(fetchErr, context.DeadlineExceeded) {
					if errors.Is(fetchErr, context.DeadlineExceeded) {
						for _, t := range batchTables {
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
						}
						return nil
					}
					return gctx.Err()
				}

				for _, t := range batchTables {
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
				}
				return nil
			}

			// rowsByTable is parallel to batchTables: entry i covers batchTables[i].
			for i, t := range batchTables {
				rowsByColumn := rowsByTable[i]
				if rowsByColumn == nil {
					// Individual table query failed inside the batch.
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
					continue
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

// fetchTableSampleRowsBatch sends one query per table in a single pgx.Batch
// round-trip and collects per-column sample values for each table.
//
// The returned slice is parallel to tables: result[i] is the column→values map
// for tables[i]. A nil entry means the individual query for that table failed
// (e.g. permission denied or table not found); the caller should treat it as a
// profiling error for that table only.
//
// The function returns a non-nil error only for fatal conditions that affect the
// whole batch (e.g. context cancellation or SendBatch failure).
//
// Result-mapping correctness: pgx.BatchResults returns result sets in the same
// order as the queued queries, so the i-th br.Query() call corresponds exactly
// to tables[i]. Both the queue loop and the read loop iterate over the same
// tables slice in forward order, guaranteeing alignment.
func fetchTableSampleRowsBatch(ctx context.Context, pool *pgxpool.Pool, tables []TableInfo, useTableSample bool) ([]map[string][]any, error) {
	if len(tables) == 0 {
		return nil, nil
	}

	batch := &pgx.Batch{}
	for _, t := range tables {
		tableIdent := pgx.Identifier{t.Schema, t.Name}.Sanitize()
		if useTableSample {
			batch.Queue("SELECT * FROM "+tableIdent+" TABLESAMPLE BERNOULLI($1) LIMIT $2", 1.0, sampleRowLimit)
		} else {
			batch.Queue("SELECT * FROM "+tableIdent+" LIMIT $1", sampleRowLimit)
		}
	}

	br := pool.SendBatch(ctx, batch)
	defer br.Close()

	results := make([]map[string][]any, len(tables))

	// Iterate in the same order as the queue loop above so that result set i
	// is always paired with tables[i]. Using the loop index explicitly (i, t)
	// rather than a separate counter avoids any off-by-one mismatch.
	for i, t := range tables {
		rowValues := make(map[string][]any, len(t.Columns))
		for _, col := range t.Columns {
			rowValues[col.Name] = make([]any, 0, sampleRowLimit)
		}

		rows, err := br.Query()
		if err != nil {
			// A query-level error (e.g. table not found, permission denied)
			// affects only this table; leave results[i] nil and continue so
			// that subsequent result sets are still consumed in order.
			results[i] = nil
			continue
		}

		fieldOrder := make([]string, 0, len(rows.FieldDescriptions()))
		for _, fd := range rows.FieldDescriptions() {
			fieldOrder = append(fieldOrder, string(fd.Name))
		}

		for rows.Next() {
			vals, valErr := rows.Values()
			if valErr != nil {
				break
			}
			for j, val := range vals {
				if j < len(fieldOrder) {
					colName := fieldOrder[j]
					if _, ok := rowValues[colName]; ok {
						rowValues[colName] = append(rowValues[colName], val)
					}
				}
			}
		}
		rows.Close()

		if rows.Err() != nil {
			results[i] = nil
			continue
		}

		results[i] = rowValues
	}

	return results, nil
}

func fetchTableSampleRows(ctx context.Context, pool *pgxpool.Pool, table TableInfo, useTableSample bool) (map[string][]any, error) {
	tableIdent := pgx.Identifier{table.Schema, table.Name}.Sanitize()
	query := "SELECT * FROM " + tableIdent + " LIMIT $1"
	args := []any{sampleRowLimit}
	if useTableSample {
		query = "SELECT * FROM " + tableIdent + " TABLESAMPLE BERNOULLI($1) LIMIT $2"
		args = []any{1.0, sampleRowLimit}
	}

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rowValues := make(map[string][]any, len(table.Columns))
	for _, col := range table.Columns {
		rowValues[col.Name] = make([]any, 0, sampleRowLimit)
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
