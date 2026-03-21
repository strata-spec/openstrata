# Contributing to Strata

## Setup

- Go 1.23+
- Docker (for integration tests) or a Postgres instance
- `go test ./...` for unit tests (no DSN required)
- `STRATA_TEST_DSN=postgres://... go test ./...` for unit tests that hit Postgres
- `STRATA_TEST_DSN=... ANTHROPIC_API_KEY=... go test -tags integration ./test/integration/...` for full end-to-end integration tests

## Never-break rules

See `CLAUDE.md`. These are invariants, not guidelines.

The short version:

1. Corrections in `corrections.yaml` are never overwritten by automated inference.
2. `ddl_fingerprint` changes on existing models always produce a visible warning before proceeding.
3. PII patterns are always redacted from example values before any LLM call.
4. `pg_stat_statements` is never accessed without `--enable-log-mining`.
5. `run_semantic_sql` is read-only. It must never execute DDL or DML.
6. The MCP server must never expose raw Postgres credentials to agents.

## Before submitting a PR

1. `go build ./...` passes
2. `go vet ./...` passes
3. `go test ./...` passes (unit tests, no DSN required)
4. New production code has tests
5. New inference pipeline behaviour updates `DESIGN.md`

## Spec changes

Changes to SMIF output format require a corresponding change to [github.com/strata-spec/spec](https://github.com/strata-spec/spec). Open the spec PR first, reference it in your Strata PR.

## CGO

CGO is prohibited. `CGO_ENABLED=0` in all build targets. Any dependency requiring CGO breaks cross-compilation and the single-binary guarantee. Do not introduce CGO dependencies.
