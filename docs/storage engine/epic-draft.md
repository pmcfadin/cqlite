# [EPIC] Lakehouse materialization: Cassandra tables as Iceberg tables

*Draft GitHub epic body — label `epic`, children follow 1:1:1:1
(one issue ↔ one OpenSpec change ↔ one PR).*

## Goal

Complete the OLAP path opened by delta-export (Epic #696): fold delta
envelopes into queryable Apache Iceberg v2 tables so every Cassandra table
is readable by Spark/Trino/DuckDB/Snowflake with zero ETL and zero consumer
merge logic. Complements — does not replace — the Flight/Trino hot path
(Epics #874/#918): Flight serves fresh per-node reads, Iceberg serves
complete history at lakehouse scale.

## Why now

- Byte-parity compaction (#842/#921/#938) makes our reconcile semantics
  machine-verified — the trust prerequisite for owning the fold.
- Delta-export already emits tombstone-faithful envelopes; the DuckDB
  reference-merge guide (#878) proves the remaining consumer burden and
  doubles as the parity oracle.
- Statistics.db authoritativeness work LANDED 2026-07-02 (#1728 via PR
  #1732, #1729 via PR #1730; #1388 also closed) — the watermark foundation
  this epic requires is in place.

## Child issues / changes (in order)

1. **add-iceberg-materializer** — single-invocation materializer, embedded
   SQL catalog (SQLite), exactly-once commits, lineage-safe,
   reference-merge parity. OQ1 verdict (2026-07-03): **HYBRID** — adopt
   iceberg-rust 0.9.1 writers/types, build the delete-aware snapshot
   commit layer ourselves (upstream has no delete-commit action; see
   `iceberg-oq1-build-vs-adopt.md`). Effort M–L, not glue. (OpenSpec
   change drafted; ready for flow-groom.)
2. **add-materializer-daemon** — continuous data-directory watcher;
   generation lifecycle; backoff/retry; sidecar-deployable.
3. **add-materializer-primary-range-dedup** — cluster mode: per-node
   materialization restricted to primary token ranges (reuse Flight
   token-range pruning); RF-safe lakehouse writes.
4. **add-materializer-repaired-gating** — consistency contract: only
   repaired SSTables materialize; snapshot watermark = repair horizon.
5. **add-iceberg-rest-catalog** — REST catalog backend, auth, commit-conflict
   handling. Candidate design (owner suggestion 2026-07-03): a
   **Cassandra-backed catalog** — LWT compare-and-swap as the commit lock
   behind a REST front; the cluster is already present in sidecar/cluster
   mode and solves cross-node commit coordination that SQLite cannot.
6. **add-iceberg-maintenance** — equality-delete compaction, snapshot
   rewrite/expiry, orphan-file cleanup.

## Dependencies

- #1729 / #1728 (authoritative Statistics.db) — LANDED 2026-07-02 (PRs
  #1730 / #1732); fail-closed on placeholder stats stays as defense.
- Epic #696 envelope schema — stable input contract.
- OQ1 (iceberg-rust write maturity) — ANSWERED 2026-07-03: HYBRID (adopt
  0.9.1 + build the delete-commit layer); spike task replaced by a named
  commit-layer build task in child 1. Evidence:
  `iceberg-oq1-build-vs-adopt.md`.

## Exit criteria (epic)

- A three-node cluster continuously materializes a keyspace to one Iceberg
  catalog with no duplicate rows across replicas.
- Trino query over the Iceberg table matches a quorum CQL read as of the
  published repair-horizon watermark, verified by an automated harness.
- Demo: `SELECT` in DuckDB against the catalog, sub-second, zero pipeline
  code.

## Product decisions — ALL DECIDED 2026-07-03 (owner)

- OQ2: **fail closed with a named error** for PK columns whose types
  Iceberg disallows as equality fields (float/double); position-delete
  degradation is a possible follow-up child.
- OQ3: **denormalize statics per row**; static-only partitions skipped
  with a counted warning in child 1.
- SD-arrow: **feature-isolated arrow 57 inside `export/iceberg/`** — no
  workspace upgrade in this epic.
- SD-catalog: **`iceberg-catalog-sql` on SQLite** for child 1;
  Cassandra-backed catalog recorded as child-5 candidate; self-emitted
  filesystem metadata as fallback.
- Catalog naming (lead default): `catalog.<keyspace>.<table>` +
  `--namespace` override flag; no configurable mapping in child 1.
