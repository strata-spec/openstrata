package postgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
}

// Profile collects sample data statistics for all columns in the given tables.
// PII patterns are redacted from example values before they are stored.
func Profile(ctx context.Context, pool *pgxpool.Pool, tables []TableInfo) (map[string]ColumnProfile, error) {
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

			tableIdent := pgx.Identifier{t.Schema, t.Name}.Sanitize()

			for _, col := range t.Columns {
				columnKey := t.Name + "." + col.Name
				columnIdent := pgx.Identifier{col.Name}.Sanitize()

				profile := ColumnProfile{
					TableName:  t.Name,
					ColumnName: col.Name,
				}

				distinctSQL := "SELECT COUNT(*) FROM (SELECT 1 FROM " + tableIdent + " WHERE " + columnIdent + " IS NOT NULL GROUP BY " + columnIdent + " LIMIT 10001) sub"
				if qErr := pool.QueryRow(gctx, distinctSQL).Scan(&profile.DistinctCount); qErr != nil {
					if errors.Is(qErr, context.Canceled) || errors.Is(qErr, context.DeadlineExceeded) || errors.Is(gctx.Err(), context.Canceled) || errors.Is(gctx.Err(), context.DeadlineExceeded) {
						return gctx.Err()
					}

					profile.ExampleValues = []string{"[profiling error]"}
					profile.CardinalityCategory = "unknown"
					mu.Lock()
					profiles[columnKey] = profile
					mu.Unlock()
					continue
				}

				nullSQL := "SELECT COUNT(*) FROM " + tableIdent + " WHERE " + columnIdent + " IS NULL"
				if qErr := pool.QueryRow(gctx, nullSQL).Scan(&profile.NullCount); qErr != nil {
					if errors.Is(qErr, context.Canceled) || errors.Is(qErr, context.DeadlineExceeded) || errors.Is(gctx.Err(), context.Canceled) || errors.Is(gctx.Err(), context.DeadlineExceeded) {
						return gctx.Err()
					}

					profile.ExampleValues = []string{"[profiling error]"}
					profile.CardinalityCategory = "unknown"
					mu.Lock()
					profiles[columnKey] = profile
					mu.Unlock()
					continue
				}

				exampleSQL := "SELECT DISTINCT " + columnIdent + "::text FROM " + tableIdent + " WHERE " + columnIdent + " IS NOT NULL LIMIT 10"
				rows, qErr := pool.Query(gctx, exampleSQL)
				if qErr != nil {
					if errors.Is(qErr, context.Canceled) || errors.Is(qErr, context.DeadlineExceeded) || errors.Is(gctx.Err(), context.Canceled) || errors.Is(gctx.Err(), context.DeadlineExceeded) {
						return gctx.Err()
					}

					if isUnsupportedTypeError(qErr) {
						profile.ExampleValues = []string{"[unsupported type]"}
						profile.CardinalityCategory = cardinalityCategory(profile.DistinctCount)
					} else {
						profile.ExampleValues = []string{"[profiling error]"}
						profile.CardinalityCategory = "unknown"
					}
					mu.Lock()
					profiles[columnKey] = profile
					mu.Unlock()
					continue
				}

				exampleValues := make([]string, 0, 10)
				for rows.Next() {
					var raw string
					if scanErr := rows.Scan(&raw); scanErr != nil {
						rows.Close()
						if errors.Is(scanErr, context.Canceled) || errors.Is(scanErr, context.DeadlineExceeded) || errors.Is(gctx.Err(), context.Canceled) || errors.Is(gctx.Err(), context.DeadlineExceeded) {
							return gctx.Err()
						}

						profile.ExampleValues = []string{"[profiling error]"}
						profile.CardinalityCategory = "unknown"
						mu.Lock()
						profiles[columnKey] = profile
						mu.Unlock()
						goto nextColumn
					}
					exampleValues = append(exampleValues, RedactPII(raw))
				}

				if rowsErr := rows.Err(); rowsErr != nil {
					rows.Close()
					if errors.Is(rowsErr, context.Canceled) || errors.Is(rowsErr, context.DeadlineExceeded) || errors.Is(gctx.Err(), context.Canceled) || errors.Is(gctx.Err(), context.DeadlineExceeded) {
						return gctx.Err()
					}

					profile.ExampleValues = []string{"[profiling error]"}
					profile.CardinalityCategory = "unknown"
					mu.Lock()
					profiles[columnKey] = profile
					mu.Unlock()
					goto nextColumn
				}
				rows.Close()

				profile.ExampleValues = exampleValues
				profile.CardinalityCategory = cardinalityCategory(profile.DistinctCount)

				mu.Lock()
				profiles[columnKey] = profile
				mu.Unlock()

			nextColumn:
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

func isUnsupportedTypeError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "42846" || pgErr.Code == "0A000" {
			return true
		}
	}

	errText := strings.ToLower(err.Error())
	return strings.Contains(errText, "cannot cast") || strings.Contains(errText, "unsupported")
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
