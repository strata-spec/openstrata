package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Bug fix: Removed AND has_schema_privilege(n.oid, 'USAGE')
// Some schemas (like rnacen_django_test) exist but the role might
// not have explicit USAGE, yet we still want to introspect the schema
// and continue the pipeline (profiling will securely degrade if tables
// are unreadable).
const schemaAccessibleQuery = `
SELECT EXISTS (
    SELECT 1
    FROM pg_namespace n
    WHERE n.nspname = $1
)`

const listTablesQuery = `
SELECT c.relname, c.oid
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1
  AND c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY c.relname`

const listColumnsQuery = `
SELECT
    a.attname                                           AS name,
    pg_catalog.format_type(a.atttypid, a.atttypmod)    AS data_type,
    NOT a.attnotnull                                    AS is_nullable,
    COALESCE(pg_get_expr(ad.adbin, ad.adrelid), '')     AS column_default,
    a.attnum                                            AS position
FROM pg_attribute a
LEFT JOIN pg_attrdef ad
    ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
WHERE a.attrelid = $1
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum`

const tableCommentQuery = `
SELECT COALESCE(obj_description($1::oid, 'pg_class'), '')`

const columnCommentQuery = `
SELECT COALESCE(col_description($1::oid, $2::int), '')`

const primaryKeyQuery = `
SELECT a.attname
FROM pg_constraint c
JOIN pg_attribute a
    ON a.attrelid = c.conrelid
    AND a.attnum = ANY(c.conkey)
WHERE c.conrelid = $1
  AND c.contype = 'p'
ORDER BY array_position(c.conkey, a.attnum)`

const foreignKeysQuery = `
SELECT
    c.conname                                               AS constraint_name,
    (SELECT array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum))
     FROM pg_attribute a
     WHERE a.attrelid = c.conrelid
       AND a.attnum = ANY(c.conkey))                        AS from_columns,
    c2.relname                                              AS to_table,
    (SELECT array_agg(a.attname ORDER BY array_position(c.confkey, a.attnum))
     FROM pg_attribute a
     WHERE a.attrelid = c.confrelid
       AND a.attnum = ANY(c.confkey))                       AS to_columns
FROM pg_constraint c
JOIN pg_class c2 ON c2.oid = c.confrelid
WHERE c.conrelid = $1
  AND c.contype = 'f'
ORDER BY c.conname`

// Introspect extracts schema metadata for all tables in the given schema.
// It returns an optional warning string when the schema contains tables
// but no visible foreign keys.
func Introspect(ctx context.Context, pool *pgxpool.Pool, schema string) ([]TableInfo, string, error) {
	var schemaAccessible bool
	if err := pool.QueryRow(ctx, schemaAccessibleQuery, schema).Scan(&schemaAccessible); err != nil {
		return nil, "", fmt.Errorf("introspect: check schema accessibility: %w", err)
	}
	if !schemaAccessible {
		return nil, "", fmt.Errorf("schema %q not found or not accessible", schema)
	}

	tableRows, err := pool.Query(ctx, listTablesQuery, schema)
	if err != nil {
		return nil, "", fmt.Errorf("introspect: list tables: %w", err)
	}
	defer tableRows.Close()

	tables := make([]TableInfo, 0)
	for tableRows.Next() {
		var table TableInfo
		table.Schema = schema
		if err := tableRows.Scan(&table.Name, &table.OID); err != nil {
			return nil, "", fmt.Errorf("introspect: scan table row: %w", err)
		}

		columnRows, err := pool.Query(ctx, listColumnsQuery, table.OID)
		if err != nil {
			return nil, "", fmt.Errorf("introspect: list columns for table %q: %w", table.Name, err)
		}

		columns := make([]ColumnInfo, 0)
		for columnRows.Next() {
			var column ColumnInfo
			if err := columnRows.Scan(&column.Name, &column.DataType, &column.IsNullable, &column.Default, &column.Position); err != nil {
				columnRows.Close()
				return nil, "", fmt.Errorf("introspect: scan column row for table %q: %w", table.Name, err)
			}

			if err := pool.QueryRow(ctx, columnCommentQuery, table.OID, column.Position).Scan(&column.Comment); err != nil {
				columnRows.Close()
				return nil, "", fmt.Errorf("introspect: get comment for column %q.%q: %w", table.Name, column.Name, err)
			}

			columns = append(columns, column)
		}
		if err := columnRows.Err(); err != nil {
			columnRows.Close()
			return nil, "", fmt.Errorf("introspect: iterate columns for table %q: %w", table.Name, err)
		}
		columnRows.Close()
		table.Columns = columns

		if err := pool.QueryRow(ctx, tableCommentQuery, table.OID).Scan(&table.Comment); err != nil {
			return nil, "", fmt.Errorf("introspect: get table comment for table %q: %w", table.Name, err)
		}

		pkRows, err := pool.Query(ctx, primaryKeyQuery, table.OID)
		if err != nil {
			return nil, "", fmt.Errorf("introspect: list primary key for table %q: %w", table.Name, err)
		}

		pkCols := make([]string, 0)
		for pkRows.Next() {
			var col string
			if err := pkRows.Scan(&col); err != nil {
				pkRows.Close()
				return nil, "", fmt.Errorf("introspect: scan primary key row for table %q: %w", table.Name, err)
			}
			pkCols = append(pkCols, col)
		}
		if err := pkRows.Err(); err != nil {
			pkRows.Close()
			return nil, "", fmt.Errorf("introspect: iterate primary key for table %q: %w", table.Name, err)
		}
		pkRows.Close()
		table.PrimaryKey = pkCols

		fkRows, err := pool.Query(ctx, foreignKeysQuery, table.OID)
		if err != nil {
			return nil, "", fmt.Errorf("introspect: list foreign keys for table %q: %w", table.Name, err)
		}

		fks := make([]FKConstraint, 0)
		for fkRows.Next() {
			var fk FKConstraint
			fk.FromTable = table.Name
			if err := fkRows.Scan(&fk.ConstraintName, &fk.FromColumns, &fk.ToTable, &fk.ToColumns); err != nil {
				fkRows.Close()
				return nil, "", fmt.Errorf("introspect: scan foreign key row for table %q: %w", table.Name, err)
			}
			fks = append(fks, fk)
		}
		if err := fkRows.Err(); err != nil {
			fkRows.Close()
			return nil, "", fmt.Errorf("introspect: iterate foreign keys for table %q: %w", table.Name, err)
		}
		fkRows.Close()
		table.ForeignKeys = fks

		tables = append(tables, table)
	}

	if err := tableRows.Err(); err != nil {
		return nil, "", fmt.Errorf("introspect: iterate tables: %w", err)
	}

	totalFKs := 0
	for _, t := range tables {
		totalFKs += len(t.ForeignKeys)
	}

	if len(tables) > 0 && totalFKs == 0 {
		warning := "⚠ 0 foreign keys found — the connected role may have restricted\n" +
			"  pg_constraint visibility. Join inference will use name heuristics\n" +
			"  and strata.md canonical joins only. To verify, run:\n" +
			"  SELECT count(*) FROM information_schema.referential_constraints;"
		return tables, warning, nil
	}

	return tables, "", nil
}
