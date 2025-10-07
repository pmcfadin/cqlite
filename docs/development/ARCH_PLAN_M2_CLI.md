## Architecture Plan: M2 – CLI (REPL + One‑Shot)

This plan defines the CLI architecture and M2 scope alignment with the acceptance criteria in `docs/development/M2_CLI_SPEC.md`, and maps them to concrete interfaces and a work breakdown. The focus is to deliver a cqlsh‑compatible developer experience for querying Cassandra 5 SSTables from local disk using provided schemas.

- Epic: `docs/development/EPIC_M2_CLI.md`
- Spec: `docs/development/M2_CLI_SPEC.md`
- PRD (context): `docs/development/PRD.md`
- Usage examples: `cqlite-cli/CLI_USAGE_EXAMPLES.md`
- Test data: `test-data/datasets/`, schemas in `test-data/schemas/`

---

### 1) Integration Decisions

#### 1.1 CLI ↔ core integration surface

Data flow (both one‑shot and REPL):

1. CLI parses flags and loads config (precedence: flags > env > config file > defaults).
2. Schema loader parses CQL/JSON sources into a catalog and publishes to `cqlite-core` `SchemaManager`.
3. Discovery service scans `--data-dir` for keyspaces/tables and exposes a stable summary API.
4. Query execution uses `cqlite-core::query::QueryEngine` and returns `cqlite-core::query::QueryResult`.
5. Result mapper converts `QueryResult` to CLI writers: table (cqlsh‑compatible), JSON, CSV.

Key modules/interfaces (CLI side):

- `cqlite-cli/src/config.rs` – load/merge/serialize config; implement `:config save`.
- `cqlite-cli/src/repl/` – command routing via `command_parser.rs`, execution via `engine.rs`, session via `session.rs`.
- `cqlite-cli/src/services/schema_loader.rs` – new: parse CQL and JSON, merge, and apply to `SchemaManager`.
- `cqlite-cli/src/services/discovery.rs` – new: scan `--data-dir`, summarize, compute coverage.
- `cqlite-cli/src/output/` – new: `json_writer.rs`, `csv_writer.rs`; reuse `formatter.rs` for table output.

Core surface (reuse):

- `cqlite-core::query::{QueryEngine, QueryResult, QueryRow, ColumnInfo}`.
- `cqlite-core::schema::SchemaManager` ingestion hooks.
- `cqlite-core` discovery utilities if available; otherwise CLI discovery will compute summaries and only pass table access to core at query time.

Decision: For M2, prefer using `cqlite-core::query::QueryEngine` as the execution path. Consolidate any ad‑hoc CLI query executors by adapting them to emit `cqlite-core::query::QueryResult` so writers are uniform.

#### 1.2 SELECT strategy

- Supported in M2: `SELECT [columns|*] FROM <[keyspace.]table> WHERE <partition/primary-key equality> [AND clustering equality/prefix] [LIMIT N]`.
- Not supported in M2: `ORDER BY`, `ALLOW FILTERING`, joins, aggregates, secondary indexes, range filters on non‑key columns.
- Feature flag: keep `state_machine` feature OFF for M2 to reduce risk; use the existing `QueryEngine` path.
- Fallback error semantics: on unsupported SELECT, return exit code 5 with a message like: “Unsupported query form in M2. Supported: SELECT with primary/partition key equality and optional LIMIT. Try narrowing WHERE clause.”

#### 1.3 Schema ingestion

- Accepted sources: `.cql` (CREATE TABLE/TYPE) and JSON descriptors. `--schema` is repeatable; directory inputs are recursively scanned. See `docs/development/SCHEMA_JSON_FORMAT.md` for the canonical JSON format (minimal and full variants).
- Precedence and merging:
  - Multiple `--schema` paths are applied in the order provided; last writer wins for conflicts (per fully‑qualified object name: keyspace.table, keyspace.type).
  - Within a directory, process files in lexical order for determinism.
  - UDT/type dependencies: two passes (types then tables); unresolved references produce schema error (exit 3) with actionable hints.
- Contract: schema loader produces a consolidated catalog published to `SchemaManager`; the REPL `:schema` commands operate on this catalog (list/show/load/refresh/unload in memory; optional persistence via config save).

---

### 2) Discovery and Sync Model

#### 2.1 Data‑dir scanning algorithm

- Root is `--data-dir` (e.g., `test-data/datasets/sstables`).
- For each keyspace directory (excluding hidden and `system`), collect candidate table directories matching `tablename-<uuid>`.
- For each table directory, consider it “has data” if it contains valid SSTable files as recognized by the core I/O layer.
- Output: Keyspaces list, tables per keyspace, SSTable counts, discovery timestamp, and optional version hints.

Implementation notes:

- Provide a `DiscoverySummary` struct consumable by `:status` and `:tables`.
- Prefer core discovery utilities when available; otherwise, implement a robust CLI fallback (pattern detection + minimal validation) with the same output shape.

#### 2.2 Schema coverage computation

- Define sets:
  - D = discovered tables from data‑dir
  - S = tables with loaded schema
- Coverage: `covered = |D ∩ S| / |D|`. Badge thresholds: Green ≥ 0.95; Yellow in [0.50, 0.95); Red < 0.50 or critical errors.
- Report: tables missing schema (sample), schemas without data (sample), totals, and discovery timestamp.

#### 2.3 Version hints/detection

- Precedence chain (final for M2):
  1. User flag: `--cassandra-version=<VER>` (explicit override)
  2. SSTable metadata via core API (if available)
  3. Dataset metadata: parse `metadata.yml` near the data directory
  4. Fallback: display "unknown" (no guessing/heuristics)
- `:status` displays both “detected” and “configured” values and warns on mismatch.

---

### 3) REPL Architecture

#### 3.1 Command routing and handlers

- Parser: `cqlite-cli/src/repl/command_parser.rs` produces typed `CommandType` variants.
- Engine: `cqlite-cli/src/repl/engine.rs` routes to handlers.
- Handlers (new modules under `cqlite-cli/src/repl/handlers/`):
  - `config.rs`: show/set values; `:config save` writes file.
  - `schema.rs`: `list|load|show|refresh|unload` and parse results summary.
  - `discovery.rs`: `:discover` and `:status` using Discovery service.
  - `health.rs`: run checks and print actionable tips.
  - `introspection.rs`: `:keyspaces`, `:tables`, `DESCRIBE`/`DESC` using schema catalog.

#### 3.2 Session model

Session stores: `data_dir`, `default_keyspace`, `page_size`, `timing`, `color`, `history_path`, and effective schema catalog. `:use` updates `default_keyspace`. History is persisted (platform‑appropriate path).

#### 3.3 Config precedence and save

Precedence: flags > env > config file > defaults. `:config save [FILE]` writes the effective config (TOML by default) including `repl.*` keys. On REPL start with flags, session is pre‑seeded.

#### 3.4 Scripting

`:source <FILE>` executes lines from a file. Errors abort the script with exit code 2 (invalid args) or 5 (execution), surfaced with the same messages as interactive mode.

---

### 4) Output & Formatting

#### 4.1 Mapping `QueryResult` → writers

- Table: use `CqlshTableFormatter` (`cqlite-cli/src/formatter.rs`) and align with documented cqlsh rules (headers, separators, right‑alignment where applicable, row count footer). Enforce stable column order from `QueryResult.metadata.columns`. See `docs/development/QUERY_RESULT_CONTRACT.md` for writer guarantees and mapping rules.
- JSON: array of row objects in column order; ensure deterministic ordering by materializing objects by column sequence rather than map iteration.
- CSV: header row from `metadata.columns`, then rows with stringified values.

#### 4.2 Value formatting expectations

- UUID/TimeUUID: lowercase hyphenated.
- Timestamps: cqlsh‑like `YYYY-MM-DD HH:MM:SS[.fff][+0000]` (timezone per dataset or as UTC default for M2).
- Collections: `list [a, b]`, `set {a, b}`, `map {k: v}` consistent with cqlsh representation.
- Blobs: `0x`‑prefixed lowercase hex.

Adapters: Where core `Value` formatting differs from cqlsh, add thin adapters in the CLI writer layer without mutating core types.

---

### 5) Diagnostics & Errors

#### 5.1 `:health` checklist

- Data‑dir readability and expected layout
- Schema parse success/failed file counts
- Schema↔data sync summary (from `:status`)
- Compression codec availability (LZ4/Snappy/Deflate)
- Config coherence: page size, timing, color, effective config file path
- Actionable next steps (e.g., “Missing schema for ks.tbl → :schema load <file>”).

#### 5.2 Exit codes

- `0` success
- `2` invalid args (CLI or meta‑commands)
- `3` schema errors
- `4` data‑dir/discovery errors
- `5` query execution errors

Errors should include succinct hints (cqlsh‑style wording where reasonable) and, when possible, suggest the corrective meta‑command or flag.

---

### 6) Testing Strategy

- Integration tests (one‑shot and REPL) under `tests/` using `test-data/datasets` and `test-data/schemas`.
- Golden snapshots for table formatting (`insta`) to lock headers/separators/row counts.
- Minimum fixture set: at least 1 table per category present in `test-data` (e.g., narrow rows, wide rows, collections, timeseries) to exercise formatting and pagination.
- CLI help text snapshot to guard parity with `M2_CLI_SPEC.md`.
- Use environment vars in tests for default paths: `CQLITE_DATA_DIR`, `CQLITE_SCHEMA`.

Snapshot policy: Any intentional changes to table formatting require updating spec examples and snapshots in the same PR.

---

### 7) Risk Log + Mitigations

- SELECT coverage gaps: Constrain to documented subset; add explicit error messages and tests for unsupported forms.
- Discovery variance across datasets: Validate multiple directory patterns; document assumptions; fall back to simple pattern matching with explicit skips.
- Formatting drift: Maintain golden snapshots; centralize all table rendering in `CqlshTableFormatter`.
- Scope control: Defer Parquet, TUI, and advanced `state_machine` processing to M3+.

---

### 8) Interfaces to Introduce (illustrative)

Rust‑level sketches (CLI crate) to clarify boundaries:

```rust
// Schema loading service
pub trait SchemaLoader {
    fn load_paths(&mut self, inputs: &[PathBuf]) -> anyhow::Result<SchemaCatalog>;
    fn list_objects(&self) -> Vec<String>; // keyspaces/tables/types
    fn show_table(&self, fqtn: &str) -> Option<TableSchema>;
}

// Discovery service output powering :status/:tables
pub struct DiscoverySummary {
    pub discovered_at: std::time::Instant,
    pub keyspaces: Vec<String>,
    pub tables_by_keyspace: BTreeMap<String, Vec<String>>,
    pub total_sstables: usize,
}

pub struct CoverageReport {
    pub tables_with_schema: usize,
    pub tables_missing_schema: Vec<String>,
    pub schemas_without_data: Vec<String>,
    pub coverage_ratio: f64,
    pub badge: CoverageBadge, // Green/Yellow/Red
}

pub enum CoverageBadge { Green, Yellow, Red }

// Result mapping entrypoint
pub enum OutputFormat { Table, Json, Csv }
pub fn write_output(result: &cqlite_core::query::QueryResult, fmt: OutputFormat) -> anyhow::Result<()> { /* ... */ }
```

---

### 9) Proposed Work Breakdown, Sequencing, and Owners

Phases align with `M2_CLI_SPEC.md` and the epic milestones; owners are suggestions.

#### Phase 1: One‑shot plumbing and global flags (Lead + CLI Engineer)

- Implement top‑level flags: `--schema`, `--data-dir`, `-e/--file`, `--out`, `--limit`, `--page-size`, `--no-color`.
- Introduce `SchemaLoader` and `DiscoveryService` modules; wire one‑shot execution to `cqlite-core::query::QueryEngine`.
- Implement `write_output` mapping to table/JSON/CSV; reuse `CqlshTableFormatter` for table.
- Exit code mapping across one‑shot.
- Tests: arg parsing; happy‑path JSON/CSV/table against `test-data`.

Deliverable: one‑shot works end‑to‑end; JSON/CSV/table emit correct rows; exit codes enforced.

#### Phase 2: REPL core and status (CLI Engineer)

- Enable `repl` command; session defaults seeded from flags/env/config.
- Implement `:config` (show/set/save) and persistence.
- Wire `:discover` and `:status` using Discovery service and Coverage computation.
- Add `:schema list|load|show|refresh` commands backed by `SchemaLoader`.
- Tests: REPL parsing; config mutation; `:status` rendering for synthetic layouts.

Deliverable: REPL baseline with configuration and sync visibility.

#### Phase 3: Introspection and formatting parity (CLI Engineer + Core Engineer)

- Implement `:keyspaces`, `:tables`, `DESCRIBE`/`DESC` with schema catalog; `USE` keyspace.
- Tighten `CqlshTableFormatter` parity; ensure stable column/value formatting.
- Golden snapshots for representative schemas and queries.

Deliverable: cqlsh‑like introspection and stable table snapshots.

#### Phase 4: Health diagnostics and polish (Lead + CLI Engineer + SDET)

- Implement `:health` checks and actionable tips.
- Honor `--no-color`; finalize help text; error polish.
- Final integration tests and snapshot updates.

Deliverable: Acceptance suite in `M2_CLI_SPEC.md` satisfied and green.

Suggested owners:

- Lead/Architect: integration decisions, interfaces, SELECT subset ADR.
- CLI Engineer: flags wiring, REPL handlers, writers, `:status`/`:health`.
- Core Engineer: confirm `QueryResult` shape and SELECT subset reliability; schema/discovery hooks.
- SDET: integration/snapshot tests and fixtures.

---

### 10) Open Questions

- Schema JSON format: finalize accepted shape(s) and alignment with core `SchemaManager` import.
- Timestamp display: confirm cqlsh exact formatting and timezone default for M2.
- Version detection: expose SSTable version in core discovery API, or rely on hints + dataset metadata for M2.

---

### 11) Validation Plan

- Run examples in `cqlite-cli/CLI_USAGE_EXAMPLES.md` against `test-data` on macOS/Linux.
- Execute `just test` across the workspace; ensure golden snapshot stability under `tests/`.
- Confirm `--help` parity with `M2_CLI_SPEC.md` and examples reflect repo paths.


