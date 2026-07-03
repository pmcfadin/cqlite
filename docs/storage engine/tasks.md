# Tasks: add-iceberg-materializer

Each task lands with the public-surface test that becomes C-audit evidence
for its requirement. Gate (`scripts/agent-gate.sh`) must pass per task; the
change archives only after C reports every requirement `satisfied`.

## 1. Scaffolding & commit-layer build (OQ1 resolved 2026-07-03: HYBRID)

- [x] 1.1 ~~Spike~~ DONE as research 2026-07-03: HYBRID verdict recorded in
      design.md D5a + `iceberg-oq1-build-vs-adopt.md` (adopt iceberg-rust
      0.9.1 writers/types; upstream has no delete-commit action)
- [ ] 1.2 Add `iceberg` feature (core + CLI) pulling iceberg-rust 0.9.1 +
      feature-isolated arrow 57 (design D5b), empty `export/iceberg/`
      module tree, feature-gated `materialize` subcommand stub; reverse
      the "Iceberg committing out of scope" boundary comment at
      `cqlite-core/src/export/mod.rs:29-36`
      — evidence: default-build CLI test (subcommand absent) +
      feature-build CLI test (help prints) → **Requirement:
      Feature-flag claim boundary**
- [ ] 1.3 **Build the delete-aware snapshot commit layer** (`commit.rs`):
      manifest entries for equality-delete files (content=2), sequence
      numbers, snapshot summary properties, atomic commit via
      `iceberg-catalog-sql` (SQLite) — scope ≈ upstream PRs #1882/#1987
      (both closed stale), built on iceberg-rust's public `spec::`
      building blocks
      — evidence: delete-file-in-committed-snapshot readable by an
      independent engine (DuckDB/PyIceberg) → feeds **Requirements:
      Delta folding + Exactly-once**

## 2. Schema derivation

- [ ] 2.1 CQL schema → Iceberg schema via the #673 Arrow mapping;
      PK columns → identifier fields; unsupported types fail closed
      naming the column
      — evidence: collections round-trip test + unsupported-type
      exit-code test → **Requirement: Schema and type mapping fidelity**
- [ ] 2.2 OQ2 DECIDED 2026-07-03 (fail closed, named error for
      float/double PK columns); encode the decision as a test. OQ3
      DECIDED (denormalize statics per row; static-only partitions
      skipped with counted warning) — encode both

## 3. Fold engine

- [ ] 3.1 Envelope reader (delta Parquet + `scan_delta` stream) honoring
      `--envelope-prefix`
- [ ] 3.2 LWW fold keyed on identifier fields with byte-parity reconcile
      rules (equal-timestamp tombstone-wins per #498)
      — evidence: upsert scenario test + equal-timestamp scenario test
      → **Requirement: Delta folding** (partial)
- [ ] 3.3 Delete emission: row/partition tombstones → equality-delete
      files; range tombstones → partition-scoped resolution (design D2)
      — evidence: row-tombstone and range-tombstone scenario tests
      → **Requirement: Delta folding** (complete)

## 4. Commit protocol

- [ ] 4.1 Snapshot commit with `cqlite.generations` +
      `cqlite.delta-horizon-micros` properties; idempotent re-submission
      — evidence: no-op re-run test + interrupted-run/re-run test
      → **Requirement: Exactly-once generation consumption**
- [ ] 4.2 Watermark sourced from authoritative Statistics.db
      `maxTimestamp`; placeholder stats fail closed (dep #1729)
      — evidence: watermark-equality test + placeholder fail-closed test
      → **Requirement: Authoritative delta-horizon watermark**

## 5. Lineage

- [ ] 5.1 Compaction lineage sidecar manifest (written under the TOC-first
      publication barrier); reader in materializer
- [ ] 5.2 Supersession skip for consumed-input compaction outputs;
      `--require-lineage` fail-closed path
      — evidence: no-duplicate-rows test + unknown-lineage exit-code test
      → **Requirement: Compaction supersession safety**

## 6. Parity oracle & wiring

- [ ] 6.1 DuckDB reference-merge parity harness over the pinned
      mixed-tombstone corpus fixtures; comparator reused from the #881
      delta parity harness
      — evidence: corpus parity test → **Requirement: Reference-merge
      parity**
- [ ] 6.2 Wire an `iceberg` component into `agent-gate.sh` (SKIP-aware,
      like `delivery-telemetry`) and the parity CI tier that fits
      (`canonical-semantic`)
- [ ] 6.3 Parity manifest claims: add
      `claim.safe.iceberg_materialize_embedded_catalog` (SQLite-backed
      SQL catalog per SD-catalog); record blocked
      claims (REST catalog, continuous materialization); regenerate
      `docs/reports/cassandra-test-parity.md`

## 7. Docs & finalize

- [ ] 7.1 User doc page (site) + CLI examples in CLAUDE.md command block
- [ ] 7.2 CHANGELOG entry; file follow-up issues for the five out-of-scope
      changes named in proposal.md; `openspec archive`
