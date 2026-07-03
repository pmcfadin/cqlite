# Design: add-iceberg-materializer

## Context

Epic #696 gives us `DeltaRecord` streaming (`scan_delta`) and a
`DeltaParquetWriter` with an envelope schema (`__op`, `__ts`, configurable
prefix). The byte-parity compaction work (#842/#921/#938) gives us a
machine-verified reconcile rule set. This change composes the two: reconcile
semantics applied to delta envelopes, committed as Iceberg v2 snapshots.

## Decisions

### D1. Consume envelopes, not raw SSTables

The materializer's input is the delta-envelope stream/files, not Data.db.
Rationale: one authoritative extraction path (delta-scan) with existing
tombstone-fidelity tests; the materializer stays format-agnostic and its
oracle (DuckDB reference merge) consumes the identical input.

### D2. Deletes: equality deletes on identifier fields, resolved per shape

- **Row tombstone** → one equality-delete row on (partition key +
  clustering key) identifier fields. Direct mapping.
- **Partition tombstone** → one equality-delete row on partition-key fields
  only (Iceberg equality deletes match on the declared subset).
- **Range tombstone** → no direct Iceberg analogue. Resolved at fold time:
  the materializer scans the affected partition in current table state,
  enumerates shadowed rows, and emits equality deletes for each. This is a
  read-before-delete, bounded by one partition per range tombstone. The
  behavioral requirement (spec) is shape-independent; this is the mechanism.
- Timestamps: `__ts` ordering decides fold outcome *before* delete-file
  emission, so Iceberg sequence numbers are not asked to encode Cassandra
  LWW — the fold layer owns reconcile, Iceberg owns visibility.

### D3. Commit protocol and exactly-once

One materialize invocation = at most one Iceberg snapshot commit. Snapshot
summary properties record:

- `cqlite.generations` — sorted list of consumed generation identities
  (keyspace/table UUID + generation + Data.db content digest).
- `cqlite.delta-horizon-micros` — authoritative watermark (spec req).
- `cqlite.lineage` — for compaction outputs, the input generation set.

Idempotency check = set membership against the union of `cqlite.generations`
across current-branch snapshot history. Crash safety comes from Iceberg's
atomic metadata swap: orphaned data files from a pre-commit crash are
invisible and reclaimable by standard Iceberg maintenance.

### D4. Lineage source

Compaction runs performed by CQLite record input→output lineage in a
sidecar manifest written atomically with the output TOC (same
publication-barrier pattern as #591's TOC-first delete). Generations without
a lineage record are treated as flush-origin; `--require-lineage` makes
unknown lineage fail closed (spec req) for directories where Cassandra — not
CQLite — performed compaction and lineage is genuinely unknowable.

### D5. Catalog: embedded SQL catalog (SQLite) — REVISED 2026-07-03 per OQ1

OQ1 research (`iceberg-oq1-build-vs-adopt.md`) found apache/iceberg-rust
ships **no filesystem/Hadoop catalog** (pattern rejected upstream;
`StaticTable` is read-only). Child 1 uses the ASF-official
`iceberg-catalog-sql` crate on an embedded SQLite backend (`catalog.db`
alongside the warehouse dir): commit-capable, persistent, readable by
DuckDB/PyIceberg, works offline. Self-emitted filesystem metadata
(metadata JSON + `version-hint.text`, atomic swap) is the documented
no-dependency fallback. REST catalog stays its own change (child 5);
owner note: evaluate a **Cassandra-backed catalog** (LWT compare-and-swap
behind a REST front) as a candidate design for the shared cluster catalog
in child 5 — decided out of scope for child 1 (offline/archived data dirs
must materialize without a live cluster).

### D5a. OQ1 verdict: HYBRID — adopt writers, build the commit layer

iceberg-rust 0.9.1 ships `EqualityDeleteWriter`/`PositionDeleteFileWriter`
and public manifest/metadata building blocks, but **no released or main
transaction action can commit delete files into a snapshot** (upstream PRs
#1882/#1987 both closed stale). Child 1 adopts the crate for types,
writers, FileIO, and manifest encoding, and builds the delete-aware
snapshot+commit layer in `commit.rs` (~scope of PR #1987; the same shape
RisingWave runs in production). Delete when upstream lands `row_delta`.

### D5b. Arrow isolation (SD-arrow, decided 2026-07-03)

iceberg-rust 0.9.1 requires arrow 57; the workspace pins arrow 53. The
`iceberg` feature pulls arrow 57 **isolated inside `export/iceberg/`** —
batches built directly from CQLite `Value`s, no type-sharing with arrow-53
code (two arrow majors already coexist in the lock via duckdb). No
workspace upgrade in this epic.

### D6. Feature isolation

`iceberg` feature on `cqlite-core`, additive only, mirroring the `parquet`
feature precedent (Epic #682): default dependency surface unchanged, CLI
subcommand compiled out without the flag. Campsite rule: new module tree
`export/iceberg/{mod.rs, fold.rs, deletes.rs, commit.rs, lineage.rs,
schema.rs}` — no file grows past target.

## Open questions — ALL RESOLVED 2026-07-03 (owner decisions)

- **OQ1** (RESOLVED): **HYBRID** — adopt iceberg-rust 0.9.1, build the
  delete-aware commit layer (see D5a). Spike task 1.1 is dead; replaced by
  a named commit-layer build task. Evidence:
  `iceberg-oq1-build-vs-adopt.md` +
  `cassandra-index/research-iceberg-oq1.md`.
- **OQ2** (RESOLVED): **fail closed with a named error** (table, column,
  type) when a primary-key column's type is disallowed as an Iceberg
  equality field (float/double). Position-delete degradation is a possible
  follow-up child if real corpora demand it.
- **OQ3** (RESOLVED): **denormalize static columns per row** — one table,
  zero consumer join knowledge. Static-only partitions (no clustering
  rows) are skipped with a counted warning in child 1. Companion-table
  shape rejected (reintroduces the consumer join this epic removes).
- Catalog naming (lead default, reversible): `catalog.<keyspace>.<table>`
  with a `--namespace` override flag; no configurable mapping in child 1.

## Non-goals restated

Continuous watching, RF/primary-range dedup, repaired-status gating, REST
catalogs, and delete-compaction maintenance are follow-up changes listed in
the proposal; nothing in this design forecloses them.
