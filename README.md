# Strata

Strata is an open-source CLI tool that connects to a PostgreSQL database, automatically infers a semantic layer, and outputs SMIF (Semantic Model Interchange Format) documents. It serves the semantic model via an MCP server so LLM agents can query your database schema intelligently. The SMIF spec is at [github.com/strata-spec/spec](https://github.com/strata-spec/spec).

Strata is the reference implementation of SMIF. The spec is the product. Strata is the proof that it works.

---

## Install

```bash
go install github.com/strata-spec/openstrata@latest
```

Requires Go 1.23+. Single binary, no runtime dependencies. `CGO_ENABLED=0` throughout — cross-compiles cleanly.

---

## Quick start

```bash
# Set your LLM provider key
export ANTHROPIC_API_KEY=sk-...

# Point at your database — that's it
strata init --db postgres://user:pass@localhost:5432/mydb

**Connection string tips:**
- AWS RDS / Aurora: append `?sslmode=require`
- Azure Database for PostgreSQL: append `?sslmode=require`
- Hosts without a verifiable certificate: append `?sslmode=disable`
- Large schemas: use `--max-tables 20` to avoid accidental large LLM runs

# Validate the output
strata validate

# Serve to MCP clients
strata serve --db postgres://user:pass@localhost:5432/mydb

# Apply a correction
strata correct --json '{
  "target_type": "column",
  "target_id": "orders.status",
  "correction_type": "description_override",
  "new_value": "Current fulfillment status. Values: pending, processing, shipped, delivered, cancelled."
}'
```

---

## The three-file system

| File | Written by | Purpose |
|------|-----------|---------|
| `strata.md` | You (before init) | Domain context — tells Strata about your business, key concepts, and quirks |
| `semantic.yaml` | Strata | The inferred semantic model in SMIF format |
| `corrections.yaml` | You (after init) | Overrides for anything Strata got wrong |

**Workflow:** Write `strata.md` once, run `strata init`, review `semantic.yaml`, use `strata correct` to fix any mistakes. Corrections are applied at serve time and survive re-runs with `--refresh`. The separation means automated inference never overwrites your manual corrections.

---

## strata.md (optional but recommended)

Place `strata.md` in your working directory before running `strata init`. Strata reads it and injects the content into every LLM call, dramatically improving the quality of inferred descriptions, labels, and join semantics.

```markdown
## About this database
{what this database is for}

## Key concepts
{most important entities and what they mean}

## Known gotchas
{naming quirks, columns that mean something non-obvious, filters always needed}

## Canonical joins
{table.column = table.column}
```

The more context you provide, the more accurate the output. A one-paragraph `strata.md` is better than none.

---

## Commands

| Command | What it does | Key flags |
|---------|-------------|-----------|
| `strata init` | Run the full inference pipeline; write `semantic.yaml`, `semantic.json`, `corrections.yaml` | `--db`, `--schema`, `--llm`, `--strata-md`, `--refresh`, `--enable-log-mining`, `--max-tables` |
| `strata validate` | Lint `semantic.yaml` against the SMIF spec | `--semantic`, `--corrections` |
| `strata serve` | Start the MCP server | `--semantic`, `--db`, `--port` |
| `strata correct` | Write a correction record to `corrections.yaml` | `--semantic`, `--corrections`, `--json` |

`strata init --refresh` re-runs inference and merges with your existing corrections. It never overwrites `source: user_defined` fields.

---

## How inference works

Strata connects to Postgres and introspects the schema (tables, columns, constraints, DDL comments). It then profiles each column — computing distinct value counts, null rates, and sampled example values with PII redacted. If `--enable-log-mining` is set, it also mines `pg_stat_statements` for query patterns to derive richer usage profiles and join candidates. The results feed into a two-pass LLM pipeline: a coarse pass infers domain context and table-level grain; a fine pass classifies each column's semantic role, label, and description. Join relationships are resolved from FK constraints, optional log patterns, and canonical joins declared in `strata.md`. The output is validated against the full SMIF rule set before being written to disk.

On schemas with more than 20 tables, Strata prints an estimated LLM
call count before proceeding. Use `--max-tables N` to abort if the
schema exceeds N tables.

`pg_stat_statements` is never accessed without `--enable-log-mining`. It is always opt-in.

---

## MCP tools

The MCP server exposes the following tools to connected LLM agents. Start it with `strata serve`.

| Tool | What it does | Requires `--db` |
|------|-------------|-----------------|
| `list_models` | Returns all non-suppressed models with id, label, description, grain | No |
| `get_model` | Returns full model including columns, relationships, metrics | No |
| `search_semantic` | Full-text search across labels and descriptions | No |
| `run_semantic_sql` | Resolves semantic SQL against physical tables and executes it | Yes |
| `record_correction` | Appends a correction to `corrections.yaml` and reloads the model | No |

**v0 scope: single-model queries + one-hop joins only.** Multi-hop joins and complex metric aggregations return a clear error message explaining the limitation rather than silently failing.

`run_semantic_sql` is strictly read-only. It rejects any query containing DDL or DML (`INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`, `ALTER`, `TRUNCATE`, `COPY`). Postgres credentials are never exposed to agents through MCP responses.

---

## Privacy

- **No telemetry.** No analytics. No version pings. No usage tracking of any kind.
- **Data sent to LLM:** table names, column names, DDL comments, sample values (PII-redacted), and your `strata.md` content. No query results, no row data beyond samples.
- Schema and column names are sent to the configured LLM provider. On large schemas, use `--max-tables` to control scope.
- **PII redaction:** email addresses, phone numbers, and SSN-like patterns are replaced with `[REDACTED]` before any LLM call.
- `pg_stat_statements` is never accessed without `--enable-log-mining`. Opting in sends normalised query templates (no literal values) to the LLM to improve join and usage inference.
- Raw Postgres connection strings never appear in `semantic.yaml` or in any MCP response. The host is represented only as a non-reversible fingerprint.

---

## The SMIF spec

Strata is the reference implementation of SMIF (Semantic Model Interchange Format). Every file Strata generates validates against the published spec. SMIF is designed to be tool-agnostic — any tool can generate or consume it independently of Strata.

Spec: [github.com/strata-spec/spec](https://github.com/strata-spec/spec)  
JSON Schema: [strata-spec.github.io/spec/schema/0.1.0/semantic.schema.json](https://strata-spec.github.io/spec/schema/0.1.0/semantic.schema.json)

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

---

## License

Apache 2.0. See [LICENSE](LICENSE).

