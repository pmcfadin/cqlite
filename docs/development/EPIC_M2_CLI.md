# Epic: M2 – CLI (REPL + One‑Shot) Delivery

## Objective
Deliver a cqlsh‑compatible CLI (REPL + one‑shot) that can read Cassandra 5 SSTables using provided schemas, with JSON/CSV/table outputs, status/health diagnostics, and developer‑friendly docs/examples.

## Scope (linked to M2_CLI_SPEC)
- One‑shot flags and execution wired to core query engine
- REPL with `:config`, `:schema`, `:status`, `:health`, `:use`, `:keyspaces`, `:tables`, `DESCRIBE`/`DESC`, `SELECT` subset
- Output formatting: cqlsh‑compatible table, JSON, CSV
- Data discovery and schema coverage reporting
- Docs and tests using `test-data`

## Owners & Roles
- PM (you): acceptance criteria, scope, tracking, docs/examples
- Lead/Architect: CLI↔core integration decisions; discovery/schema interfaces
- CLI Engineer: flags wiring, REPL handlers, output writers, status/health
- Core Engineer: ensure `SELECT` subset works; schema ingest & result surfaces
- SDET: integration + snapshot tests; golden outputs; fixtures hygiene

## Timeline
- Week 1: One‑shot + output; REPL boot + `:config`; initial tests
- Week 2: `:schema`, `:status`, `:tables`, `DESCRIBE`; formatting snapshots
- Week 3: `:health`, polish, docs, final test pass

## Milestones
- M2‑P1: One‑shot plumbing merged; JSON/CSV/table outputs; tests green
- M2‑P2: REPL core + `:config` + `:schema` + discovery API
- M2‑P3: Introspection (`:keyspaces`, `:tables`, `DESCRIBE`), table snapshots
- M2‑P4: `:health`, docs complete, acceptance suite green

## Tracking Checklist
- One‑shot
  - [ ] `--schema`, `--data-dir`, `-e/--file`, `--out`, `--limit`, `--page-size`
  - [ ] Execution path to core `QueryEngine`
  - [ ] JSON/CSV writers and table formatting
  - [ ] Error handling and exit codes
- REPL
  - [ ] Enable `repl` command
  - [ ] `:config` (show/set/save), history, `--no-color`
  - [ ] `:schema list|load|show|refresh`
  - [ ] `:status` (discovery + coverage), `:health`
  - [ ] `:use`, `:keyspaces`, `:tables`, `DESCRIBE`/`DESC`, `SELECT ... LIMIT`
- Tests & Docs
  - [ ] Integration tests against `test-data`
  - [ ] Golden snapshots for table output
  - [ ] Update `CLI_USAGE_EXAMPLES.md` with `test-data` paths
  - [ ] Help text parity with spec

## Risks & Mitigations
- SELECT coverage gaps → constrain to documented subset; add fixtures; fallback errors
- Discovery accuracy across datasets → validate directory patterns; document assumptions
- Formatting parity drift → maintain golden snapshots; spec‑driven formatter tests
- Time overrun → prioritize must‑haves; defer TUI/Parquet/advanced SELECT

## Test Data
- `--data-dir` = `/Users/patrick/local_projects/cqlite/test-data/datasets`
- `--schema` = `/Users/patrick/local_projects/cqlite/test-data/schemas`
- Env: `CQLITE_DATA_DIR`, `CQLITE_SCHEMA`
- Scripts: `test-data/scripts/start-clean.sh`, `test-data/scripts/export.sh`

## Definition of Done (M2)
Acceptance criteria in `M2_CLI_SPEC.md` satisfied; tests green across one‑shot and REPL; docs updated; developers can run examples successfully on macOS/Linux.


