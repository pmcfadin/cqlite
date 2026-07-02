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
- Statistics.db authoritativeness work (#1728/#1729 → #1388) is landing the
  watermark foundation this epic requires.

## Child issues / changes (in order)

1. **add-iceberg-materializer** — single-invocation materializer, filesystem
   catalog, exactly-once commits, lineage-safe, reference-merge parity.
   (OpenSpec change drafted; ready for flow-groom.)
2. **add-materializer-daemon** — continuous data-directory watcher;
   generation lifecycle; backoff/retry; sidecar-deployable.
3. **add-materializer-primary-range-dedup** — cluster mode: per-node
   materialization restricted to primary token ranges (reuse Flight
   token-range pruning); RF-safe lakehouse writes.
4. **add-materializer-repaired-gating** — consistency contract: only
   repaired SSTables materialize; snapshot watermark = repair horizon.
5. **add-iceberg-rest-catalog** — REST catalog backend, auth, commit-conflict
   handling.
6. **add-iceberg-maintenance** — equality-delete compaction, snapshot
   rewrite/expiry, orphan-file cleanup.

## Dependencies

- #1729 / #1728 (authoritative Statistics.db) — blocks child 1's watermark
  requirement (fail-closed until landed).
- Epic #696 envelope schema — stable input contract.
- OQ1 (iceberg-rust write maturity) — spike task inside child 1.

## Exit criteria (epic)

- A three-node cluster continuously materializes a keyspace to one Iceberg
  catalog with no duplicate rows across replicas.
- Trino query over the Iceberg table matches a quorum CQL read as of the
  published repair-horizon watermark, verified by an automated harness.
- Demo: `SELECT` in DuckDB against the catalog, sub-second, zero pipeline
  code.

## NEEDS YOU (product decisions before child 1 activates)

- OQ2: tables whose clustering columns can't be Iceberg identifier fields —
  degrade to position deletes or fail closed?
- OQ3: static-column materialization shape (denormalize vs companion table).
- Catalog naming convention: `catalog.<keyspace>.<table>` vs configurable
  namespace mapping.
