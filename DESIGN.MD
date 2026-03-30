# Strata — Architecture and Design Decisions

**Version:** 0.1.0-draft  
**Repository:** https://github.com/strata-spec/openstrata  
**Spec:** https://github.com/strata-spec/spec  
**Last revised:** 2026-03

This document records *why* decisions were made, not just what they are.
Any contributor challenging a decision should start here.

---

## 1. What Strata is (and is not)

Strata is the reference implementation of SMIF (Semantic Model Interchange
Format). It is a CLI tool that connects to PostgreSQL, infers a semantic
layer from schema introspection, sample data profiling, and optionally query
log mining, and outputs a SMIF-conformant `semantic.yaml`.

**Strata is not:**
- A query engine. It does not parse, plan, or cache SQL.
- A data catalog. It does not crawl object storage, manage lineage, or
  maintain a web UI.
- A managed service. No telemetry. No backend. No data leaves the user's
  environment except to the LLM provider they configure.
- Tied to dbt, Spark, Cube, or any transformation framework. Those are
  optional future connectors, not dependencies.

The strategic intent is that SMIF — not Strata — becomes the industry
standard. Strata is the proof that SMIF is implementable and useful. The
spec is the product. Strata is the reference.

---

## 2. Language: Go

**Decision:** Go. Non-negotiable for v0.

**Rationale:**
- Single-binary distribution is the primary deployment requirement. Go
  compiles to a self-contained binary with no runtime dependency. Python
  requires a venv or container. Rust would work but the ecosystem for
  Postgres drivers and MCP is thinner.
- `go install github.com/strata-spec/openstrata@latest` is the target
  install experience. It must work on macOS, Linux, and Windows without
  Docker, pip, or cargo.
- Go's standard library handles JSON, SHA256, and HTTP without third-party
  dependencies. The external dependency surface is intentionally small.
- CGO is **prohibited** in Strata. Any dependency that requires CGO
  breaks cross-compilation and the single-binary guarantee. This constraint
  is actively enforced: `CGO_ENABLED=0` in all build targets.

**What this forecloses:**
- `github.com/pganalyze/pg_query_go` — the gold-standard Postgres SQL
  parser — requires CGO. It is not used in v0. The SQL parsing surface in
  Stage 4 (log mining) uses a pure-Go approach scoped to the extraction
  patterns needed (table refs, column refs, join conditions). Upgrading to
  pg_query_go via a CGO build mode is a v1 decision.

---

## 3. Postgres driver: pgx/v5

**Decision:** `github.com/jackc/pgx/v5`. Not `database/sql`.

**Rationale:**
- pgx exposes native Postgres types (OID, arrays, composite types) that
  `database/sql` requires manual scanning for. Schema introspection (Stage 2)
  queries `pg_description`, `information_schema`, and `pg_constraint` —
  all of which return Postgres-native types.
- pgx's `pgxpool` is used for the MCP server's live query path
  (`run_semantic_sql`). `database/sql`'s connection pool does not expose
  the fine-grained connection lifecycle controls needed for a safe
  read-only query executor.
- pgx supports `QueryRow` with named struct scanning via `pgx/v5/pgxscan`,
  reducing boilerplate in introspection code.
- `database/sql` is not used anywhere. Mixing drivers is prohibited.

---

## 4. MCP serving: mark3labs/mcp-go

**Decision:** `github.com/mark3labs/mcp-go`.

**Rationale:**
- The official Anthropic MCP Go SDK (`modelcontextprotocol/go-sdk`) reached
  v1 stability in early 2026 but has a larger API surface designed for
  multi-transport, multi-capability servers. mark3labs/mcp-go has a simpler
  API for the CLI-embedded server use case and has wider adoption in the
  OSS ecosystem as of the v0 build date.
- **Decision to revisit:** If the official SDK matures and adoption catches
  up before Strata v0.1.0 ships, migrate to it. The MCP server is isolated
  in `internal/mcp/` — the migration surface is bounded.

---

## 5. Competitive positioning: Wren Engine

Wren Engine (https://github.com/canner/wren-engine) is the closest
architectural predecessor. Key comparison:

| Dimension         | Wren Engine                        | Strata                              |
|-------------------|------------------------------------|-------------------------------------|
| Serving           | MCP-native, Apache DataFusion      | MCP-native, direct Postgres         |
| Schema definition | Hand-authored MDL                  | Automatically inferred, human-correctable |
| Query execution   | DataFusion (in-process columnar)   | Postgres passthrough (read-only)    |
| Format            | MDL (proprietary to Wren)          | SMIF (open, tool-agnostic)          |
| Zero-config       | No                                 | Yes                                 |

**Where we borrow from Wren:** MCP-first serving architecture. The idea
of a semantic layer as the agent interface, not just a BI convenience.

**Where we diverge:** Wren requires an engineer to write MDL by hand before
it is useful. Strata's inference pipeline means the first `strata init`
produces a useful (if imperfect) semantic model with no prior work. The
corrections overlay is how you improve it, not the starting point.

**Why we dropped DataFusion:** DataFusion is the right call if you need
portable execution across multiple data sources. For a Postgres-only v0
that executes read-only SQL, adding DataFusion adds ~50MB to the binary,
a significant compilation dependency surface, and semantic SQL rewriting
complexity we haven't validated in practice. The thin SQL translation layer
(model name → physical table, concept name → definition_sql expansion) is
implementable without it. DataFusion is a v1 option if multi-source
execution becomes a requirement.

---

## 6. The three-file system
```
strata.md          (human, before)
      │
      ▼
INFERENCE PIPELINE ──► semantic.yaml   (tool-generated, overwritable)
                              │
                    corrections.yaml   (human, after, append-only)
                              │
                    SERVE LAYER ◄──── merged in-memory model
```

**`strata.md`** is human prose read as a prior by the LLM pipeline. No
special syntax. It is injected into every Stage 5 and Stage 6 LLM call.
It is not a config file. It is not parsed programmatically. It is what
you would write to onboard a new engineer onto this database.

**`semantic.yaml`** is owned by the tool. Re-running `strata init`
overwrites it (modulo the --refresh merge rules in Section 9). It is
human-readable and human-editable, but edits made directly to it are
fragile — they will be lost on the next `init`. For persistent human
intent, use `corrections.yaml` or `strata.md`.

**`corrections.yaml`** is owned by humans. The tool never overwrites it.
The tool only appends to it (via `record_correction`). It is the durable
layer of human intent.

---

## 7. The corrections overlay: invariants

These invariants are enforced in code, not just documented. Any code path
that would violate them is a bug.

**INV-1: Append-only.**
`corrections.yaml` records are never deleted or modified by automated
processes. `write.go` in `internal/overlay/` opens the file in append
mode. It has no delete or update function.

**INV-2: User intent survives re-inference.**
`strata init --refresh` MUST NOT modify any field in `semantic.yaml` for
a column, model, or relationship that has a corresponding entry in
`corrections.yaml` with `source: user_defined` and `status: approved`.
The matching key is `target_id`. See Section 9 for the exact algorithm.

**INV-3: Pending LLM suggestions are never served.**
Corrections with `source: llm_suggested` and `status: pending` are loaded
into memory by the serve layer but are NEVER merged into the model served
to agents. They are surfaced as a separate pending-review list.

**INV-4: Trust hierarchy is in-memory only.**
The trust hierarchy (`user_defined > catalog_import > log_inferred >
llm_inferred > schema_constraint`) applies exclusively to the in-memory
model constructed at serve time. It MUST NOT cause any write to
`semantic.yaml`. This is enforced by the invariant that `semantic.yaml`
writes only happen from the inference pipeline, and the overlay merge
only happens in `internal/overlay/merge.go` on the in-memory struct.

**INV-5: `source_type: user_defined` never appears in semantic.yaml.**
The provenance value `user_defined` is applied by the serve layer at
merge time. The inference pipeline MUST NOT write it into `semantic.yaml`.

---

## 8. The four learning loops

The architecture is designed for four feedback loops at different
timescales. v0 implements loops 1 and 2. Loops 3 and 4 are stubbed with
clear interfaces.

### Loop 1 — Correction capture (real-time)
**Timescale:** Seconds.  
**Mechanism:** `record_correction` MCP tool → appends to `corrections.yaml`
→ MCP server reloads in-memory model immediately.  
**v0 status:** Implemented.

### Loop 2 — Schema drift detection (on --refresh)
**Timescale:** Minutes to hours.  
**Mechanism:** On `strata init --refresh`, compute `ddl_fingerprint` for
each model in the live database. Compare against fingerprints in
`semantic.yaml`. For changed models, re-run inference (respecting
correction overlay). For unchanged models, carry forward existing
inference. Emit a human-visible warning for every changed model before
proceeding.  
**v0 status:** Implemented.

### Loop 3 — Query log enrichment (future)
**Timescale:** Hours to days.  
**Mechanism:** Periodic re-mining of `pg_stat_statements` to accumulate
richer `usage_profile` data → feeds back into column descriptions and
join confidence.  
**v0 status:** Interface stubbed in `internal/postgres/logmine.go`. Not
called by any pipeline stage. Activated by a future `strata enrich`
command.

### Loop 4 — Implicit query feedback (future)
**Timescale:** Days to weeks.  
**Mechanism:** The `run_semantic_sql` MCP tool logs query outcomes
(success/error, rows returned, agent session). Outcome data nudges
confidence scores on the columns and joins referenced.  
**v0 status:** Interface stubbed in `internal/mcp/tools/run_semantic_sql.go`
as `// TODO(loop4): log query outcome`. Not implemented.

---

## 9. The --refresh merge algorithm

This algorithm is the canonical definition. The implementation in
`internal/inference/pipeline.go` (`Refresh()`) must match it exactly.
Tests in `internal/inference/pipeline_test.go` must cover each case.
```
Input:
  - live_schema:    result of Stage 2 on the live database
  - existing_smif:  current semantic.yaml parsed into memory
  - corrections:    current corrections.yaml parsed into memory

Algorithm:

1. For each model M in live_schema:
   a. Compute ddl_fingerprint(M) from live schema.
   b. If M exists in existing_smif AND fingerprint matches:
      → Skip inference for M. Carry forward existing_smif[M] unchanged.
   c. If M exists in existing_smif AND fingerprint differs:
      → Emit warning: "Model <M> schema has changed. Re-inferring."
      → Proceed to step 2 for M.
   d. If M is new (not in existing_smif):
      → Emit: "New model <M> detected. Inferring."
      → Proceed to step 2 for M.

2. For model M requiring re-inference:
   a. Build corrected_columns = set of column target_ids in corrections.yaml
      for this model with source: user_defined, status: approved.
      Format: "<model_id>.<column_name>"
   b. Run Stages 5 and 6 (LLM inference) for M.
   c. For each inferred column C in the result:
      - If "<M.model_id>.<C.name>" is in corrected_columns:
        → Discard inferred values for ALL fields covered by the correction.
        → Log: "Skipping inference for corrected column <C.name> in <M>"
      - Else: accept inferred values.
   d. For each new column in live_schema[M] not in existing_smif[M]:
      → Always infer. No correction can reference a column that didn't exist.
   e. Run Stage 7 (join inference) for M.
   f. Update existing_smif[M] with the merged result.

3. For each model in existing_smif NOT in live_schema:
   → Emit warning: "Model <M> no longer exists in database."
   → Set model.provenance.stale = true. Do not remove.

4. Write updated existing_smif to semantic.yaml.
   Note: corrections.yaml is NEVER written by this algorithm.
```

**Edge case — model-level corrections:**
If a correction targets `target_type: model` (e.g. description_override
on the model itself), the model description is preserved but all column
inference still runs for changed columns. Model-level corrections do not
suppress column re-inference.

---

## 10. LLM abstraction

**Decision:** Define a `LLMClient` interface with `GenerateStructured`
before writing any inference code. Provider differences (Anthropic tool-use
forcing vs OpenAI `response_format: json_schema`) are fully encapsulated
behind this interface.
```go
// LLMClient is the interface all LLM providers must implement.
// The inference pipeline depends only on this interface.
type LLMClient interface {
    // GenerateStructured sends prompt to the LLM and returns the response
    // body unmarshalled into result. schema is a JSON Schema describing
    // the expected response shape. The implementation is responsible for
    // forcing structured output using provider-appropriate mechanisms
    // (tool-use forcing for Anthropic, response_format for OpenAI).
    // Retries once on malformed output before returning an error.
    GenerateStructured(ctx context.Context, prompt string, schema []byte, result any) (GenerateResult, error)

    // Provider returns a string identifying the provider (for logging).
    Provider() string

    // Model returns the model name used for inference calls.
    Model() string
}
```

The pipeline only imports `inference/llm`. It never imports
`inference/llm/anthropic` or `inference/llm/openai` directly. Provider
selection happens in `cmd/init.go` based on the `--llm` flag, but
environment variables take precedence (see below).

### Environment variable contract

All LLM calls in the inference pipeline (`strata init`, coarse pass, fine
pass) are configured by the following environment variables:

| Variable | Default | Description |
|---|---|---|
| `STRATA_LLM_MODEL` | `claude-sonnet-4-20250514` | Model name passed to the provider API |
| `STRATA_LLM_BASE_URL` | *(empty — use provider default)* | OpenAI-compatible base URL override |
| `STRATA_LLM_API_KEY` | *(see fallback chain below)* | API key |

**`STRATA_LLM_API_KEY` fallback chain:** `STRATA_LLM_API_KEY` →
`ANTHROPIC_API_KEY` → `OPENAI_API_KEY`.

**`STRATA_LLM_BASE_URL`** enables any OpenAI-compatible endpoint
(DeepSeek, Gemini, local Ollama, etc.) with no code changes. When set,
an OpenAI-compatible client is used regardless of the `--llm` flag:

```bash
STRATA_LLM_MODEL=deepseek-chat \
STRATA_LLM_BASE_URL=https://api.deepseek.com \
STRATA_LLM_API_KEY=<key> \
strata init --db $DSN
```

**Default behaviour is unchanged** — if no env vars are set, the existing
Anthropic/Sonnet behaviour is preserved.

**Note:** inference quality is model-dependent. Results from smaller or
quantised models may have lower confidence scores.

Every `strata init` run writes a `run_start` entry to `strata.log` that
includes the resolved model and base URL, ensuring `semantic.yaml` outputs
are reproducible and traceable:

```json
{"time":"...","event":"run_start","llm_model":"deepseek-chat","base_url":"https://api.deepseek.com"}
```

---

## 11. Validation: Go rules, not JSON Schema

**Decision:** V-001 through V-042 and W-001 through W-010 are implemented
directly as Go functions in `internal/smif/validate.go`. The JSON Schema
at `schema/0.1.0/semantic.schema.json` is derived output, not the
validator.

**Rationale:** The JSON Schema is acknowledged as a structural skeleton in
the spec (SPEC.md Appendix A). Implementing the validator against it would
mean validating against an incomplete spec. More importantly, several rules
require cross-object lookups (e.g. V-042: correction target_id must resolve
to a defined object) that JSON Schema cannot express. The Go implementation
is authoritative. The JSON Schema, when completed, is useful for external
tooling (IDE plugins, linters) and will be generated from the Go rule
definitions, not maintained in parallel.

---

## 12. `strata serve --db` (optional)

**Decision:** `--db` is optional on `strata serve`. If absent, the MCP server
starts and serves all metadata tools (`list_models`, `get_model`,
`search_semantic`, `record_correction`). `run_semantic_sql` returns a
structured error:
```json
{
  "error": "run_semantic_sql requires a live database connection",
  "hint": "Restart serve with --db <connection_string>"
}
```

**Rationale:** The metadata tools are useful without a live database — for
agents that only need to understand the schema, or for environments where
the semantic model is checked into git and served without a database
connection. Failing loudly on `serve` startup for a missing `--db` breaks
this use case.

---

## 13. PII redaction

PII redaction happens in Stage 3 (sample profiling) before any data is
passed to an LLM call. It is not optional. The patterns redacted in v0:

- Email addresses: `[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`
- US phone numbers: `(\+1)?[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}`
- SSN-like patterns: `\d{3}-\d{2}-\d{4}`
- Credit card-like patterns: `\d{4}[\s-]\d{4}[\s-]\d{4}[\s-]\d{4}`

Redacted values are replaced with `[REDACTED:<type>]` in `example_values`.
The redaction function is in `internal/postgres/profile.go` and is tested
with explicit cases. Any expansion of this list requires a test case.

---

## 14. No telemetry

Strata collects no usage data. No analytics. No crash reporting. No
version-check pings. The binary makes exactly two categories of outbound
network call:
1. Connections to the Postgres database specified by `--db`.
2. Calls to the LLM provider specified by `--llm` (Anthropic or OpenAI).

Both are explicit user actions. This is documented in the README and
enforced by code review.
```