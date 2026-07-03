# Change: add-iceberg-materializer

## Why

Epic #696 (delta-scan / delta-export) projects an SSTable generation into a
delta-envelope Parquet file with full tombstone fidelity. That output is a
**CDC log, not a table**: today the consumer must fold upserts and tombstones
into current state themselves — the shipped DuckDB reference-merge
reconciliation guide (#878) is the proof that this burden currently sits on
the user.

This change moves the fold to our side of the boundary. A materializer
consumes delta envelopes and maintains an **Apache Iceberg v2 table**, so any
Iceberg-aware engine (Spark, Trino, DuckDB, Snowflake external tables) can
`SELECT * FROM catalog.ks.table` with zero merge knowledge. It is the missing
link between "delta-export exists" and "every Cassandra table is a lakehouse
table", and it is the OLAP half of the parallel OLTP/OLAP path strategy.

The DuckDB reference-merge guide is promoted from user documentation to the
**parity oracle** for the materializer: for the same envelope set, materialized
Iceberg state must equal the reference merge.

## What Changes

- New capability spec `iceberg-materializer`.
- New cargo feature `iceberg` on `cqlite-core` (NOT in defaults), gating an
  `export/iceberg/` module: `IcebergMaterializer` public API that consumes
  delta envelopes (or a `scan_delta` stream) and commits Iceberg v2 snapshots.
- New CLI subcommand `cqlite materialize` behind `--features iceberg`
  (single invocation, single table, one-or-more input generations →
  one Iceberg snapshot commit).
- Catalog support: embedded SQL catalog (`iceberg-catalog-sql`, SQLite
  backend) only in this change; self-emitted filesystem metadata as
  documented fallback. (Revised 2026-07-03: apache/iceberg-rust ships no
  filesystem catalog — see design D5/D5a and
  `iceberg-oq1-build-vs-adopt.md`. HYBRID verdict: we build the
  delete-aware snapshot commit layer; arrow 57 isolated behind the
  `iceberg` feature.)
- Snapshot commits carry consumed-generation identities and an authoritative
  delta-horizon watermark in snapshot properties.

## Impact

- Affected specs: `iceberg-materializer` (new).
- Affected code: `cqlite-core/src/export/iceberg/` (new, feature-gated),
  `cqlite-cli` `materialize` subcommand, `Cargo.toml` (iceberg-rust + arrow
  deps behind the `iceberg` feature only — default dependency surface
  unchanged, matching the #558 precedent).
- Parity manifest: adds `claim.safe.iceberg_materialize_embedded_catalog`;
  records `claim.blocked.iceberg_rest_catalog` and
  `claim.blocked.continuous_materialization` until follow-ups land.
- Note on #1406: this change neither depends on nor alters the
  uncompressed-SSTable-writes claim boundary. Parquet data files written by
  the materializer carry Parquet-level compression; SSTable write claims are
  untouched.

## Dependencies

- **Hard (LANDED 2026-07-02)**: authoritative `maxTimestamp` decoding in
  Statistics.db (#1729, PR #1730) and live-cell `maxLocalDeletionTime`
  fidelity (#1728, PR #1732) — both closed. The watermark requirement's
  fail-closed behavior on placeholder stats stays as defense against
  legacy/malformed inputs.
- **Hard**: Epic #696 delta-envelope schema (`__op`/`__ts`, `--envelope-prefix`).
- **Soft**: Epic #673 Arrow type mapping (reused for Iceberg schema derivation).

## Out of scope (follow-up changes under the same epic)

0. **Unflushed memtable tail exports** (CEP-11 plugin design,
   `memtable-plugin-design.md`): the materializer consumes ONLY real
   flushed/compacted SSTable generations from the data directory. Tail
   state must reach the lakehouse via a normal flush, never via tail
   exports. (Orthogonality confirmed 2026-07-03.)
1. `add-materializer-daemon` — continuous sidecar-style watcher with
   generation-lineage tracking against the live data directory.
2. `add-materializer-primary-range-dedup` — cluster mode: each node
   materializes only its primary token ranges (RF dedup), reusing the Flight
   connector's token-range pruning.
3. `add-materializer-repaired-gating` — consistency watermark = incremental
   repair horizon; only repaired SSTables feed the lakehouse.
4. `add-iceberg-rest-catalog` — REST catalog backend + credential handling.
5. `add-iceberg-maintenance` — equality-delete compaction / snapshot rewrite
   and expiry.
