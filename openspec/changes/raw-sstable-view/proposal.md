# Proposal: a raw SSTable view — every physical row of a key, across generations, in SQL

**Issue**: #4222 (raw SSTable view). Child of epic #4192 (SSTable tool program) — this is **F2** of
`docs/architecture/forensics-surface-2026-09.md` ("`raw.*` tables — the SQLite spin"). Priority P1,
board `Ready` → `In Progress`, `resume-dont-ask` sealed (Seam-1 pre-approved).

**Routing**: **design-driven.** There is no single external oracle for "what should the SQL surface
look like" — the physical facts (timestamps, TTLs, tombstone kinds) are oracle-driven and validated
against `sstabledump`, but the catalog shape, column contract, and naming are a genuine design
choice with real latitude, which is exactly OpenSpec's job per `openspec/config.yaml`.

## Problem

An operator debugging "why does this row have this value" or "why didn't my delete work" has to run
`sstabledump` by hand, once per SSTable generation, and reconcile timestamps/tombstones/TTLs in
their head — CQLite's own `SELECT` already discards that information at the reconciliation boundary
(`docs/architecture/forensics-surface-2026-09.md:16-17`). There is no queryable, joinable surface for
the raw, per-generation, per-cell facts a diagnosis needs.

Two structural facts from the codebase bound what this change can do:

1. **CQLite's query engine has no catalog at all.** Table resolution runs directly from a `TableId`
   string to on-disk SSTables — `SelectExecutor::extract_table_id`
   (`cqlite-core/src/query/select_executor/mod.rs:1275`) → `StorageEngine::scan`
   (`cqlite-core/src/storage/mod.rs:364`) → `SSTableManager::scan_with_meter`
   (`cqlite-core/src/storage/sstable/mod.rs:747`). There is no `Catalog`/`TableProvider` trait, no
   registry of "known tables", and **no precedent anywhere for a virtual, derived, or synthetic
   table** — the closest thing, `FromClause::TableAlias` (`query/select_ast.rs:187`), is a per-query
   `AS` alias over a real table, not a catalog-registered name. Exposing a new table therefore means
   intercepting table/schema resolution before it reaches disk, not registering into an existing
   registry.
2. **The query engine's SQL parser has no `JOIN` executor.** `m2_select_validator.rs` documents `JOIN`
   as an explicit M2-unsupported feature and rejects `JOIN`/`INNER JOIN`/`LEFT JOIN`/etc. keywords with
   a validation error (`cqlite-cli/tests/unsupported_query_tests.rs:11-23` asserts exit code 5). The
   issue's illustrative `SELECT ... FROM ks.t JOIN ks.t_raw_sstable_data USING (...)` cannot execute
   against today's engine, independent of anything this change adds. See Design D4 for the resulting
   scope decision.

## Scope (this change)

A **new queryable per-table view**, `<keyspace>.<table>_raw_sstable_data`, exposed through the
existing `cqlite query` SQL surface (same parser, same output writers), backed directly by the
per-generation, pre-reconciliation row/cell data the point-read and compaction machinery already
produce internally (`SSTableReader::read_single_partition_for_compaction`,
`stream_all_partitions_for_compaction`) — **no new engine**, a new egress over data CQLite already
decodes. One row per physical row per SSTable generation; every per-cell timestamp/TTL/local-deletion-
time/tombstone-kind fact; row- and partition-level deletion; range tombstones as their own rows;
source SSTable identity. Full column contract, naming, and the three owner-delegated design calls are
in `design.md`; the verifiable requirements are in `specs/raw-sstable-view/spec.md`.

## Non-goals

- **Reconciliation / the "winner" verdict.** That is #4193 (`explain`), which is designed (D3) to
  consume this view's rows as its input so the two surfaces can never disagree.
- **CommitLog/memtable-resident data.** Stays with #4205 (`find`) — except the "which SSTable
  generations hold this key" enumeration, which folds into this view per D2 below; #4205 keeps the
  CommitLog half and the finer bloom-negative/index-miss probe classification for **misses**, which
  this view (a positive-hits-only projection) cannot produce.
- **Literal SQL `JOIN` syntax.** Blocked on a pre-existing, unrelated gap (see Problem #2 and D4) —
  not something this issue can or should fix. "Joinable" is satisfied here by correlated queries
  against the shared key columns, not by `JOIN ... USING (...)` executing.
- **Writing anything.** Read-only, like every `raw.*`/forensics-surface slice
  (`forensics-surface-2026-09.md:158`).
- **Python/Node bindings.** Core API + CLI only in this slice, per the issue's own scope note; bindings
  follow once the column contract is pinned by the snapshot test this change adds.
- **A general table-valued-function or schema-namespacing mechanism.** Rejected as the naming vehicle
  in D1 — out of proportion to what's needed here.

## Doctrine impact

- **No-heuristics (#28):** every emitted column comes from the schema, the on-disk cell/row/partition
  header structures the existing decoders already parse authoritatively (`row_decoder/`,
  `compaction_row.rs`), or `Statistics.db`-derived generation metadata (`format`, `generation`) — never
  byte-pattern inference.
- **Public-surface contract:** a new catalog-visible table name and a new column set are a public
  surface; `specs/raw-sstable-view/spec.md` requires a pinned column-contract snapshot test (per the
  issue's own doctrine note and `pub-surface`/wiring-evidence conventions).
- **Memory (<128MB, oom-audit):** the row producer must be a true per-generation stream, not a
  materializing `collect::<Vec<_>>()` — `xtask/src/oom_audit/rule.rs`'s `STREAM_RETURNS_VEC` rule
  flags exactly that shape inside any scan-shaped function; see D6.
- **Fail-closed / typed errors:** unlike the base SELECT path (which today silently returns zero rows
  for an unknown table — `SSTableManager::scan_with_meter`, `storage/sstable/mod.rs:747-753` — and
  silently falls back to row-derived columns for a missing schema — `SchemaRegistry::find_schema_by_table`,
  `schema/registry.rs:624-625`), the raw view is new surface and is specified to fail closed with a
  typed error instead (D7), per the no-heuristics/fail-closed mandate this program is built under.

## Oracle

Physical-dump parity (#1742) against `sstabledump` JSONL goldens, on named fixtures spanning ≥2
generations, tombstone/TTL variety, BTI, and compression — enumerated per-requirement in
`specs/raw-sstable-view/spec.md`.
