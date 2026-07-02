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

### D5. Catalog: filesystem first

`iceberg-rust` filesystem catalog only. REST catalog (credentials, retries,
multi-writer commit conflicts) is its own change. **Open question OQ1**
covers iceberg-rust v2-delete write maturity; fallback is emitting the
Iceberg metadata/manifests directly (we already own a Parquet writer), which
is more code but zero new semantics.

### D6. Feature isolation

`iceberg` feature on `cqlite-core`, additive only, mirroring the `parquet`
feature precedent (Epic #682): default dependency surface unchanged, CLI
subcommand compiled out without the flag. Campsite rule: new module tree
`export/iceberg/{mod.rs, fold.rs, deletes.rs, commit.rs, lineage.rs,
schema.rs}` — no file grows past target.

## Open questions (NEEDS YOU)

- **OQ1**: iceberg-rust write-path maturity for v2 equality deletes at our
  pin date — spike task 1.1 answers build-vs-adopt before implementation.
- **OQ2**: identifier fields for tables where clustering columns include
  types Iceberg disallows in identifier fields — degrade to
  position deletes for those tables, or block with a named error?
- **OQ3**: static columns — materialize as denormalized per-row values
  (simple, redundant) or as a companion table (normalized, two commits)?

## Non-goals restated

Continuous watching, RF/primary-range dedup, repaired-status gating, REST
catalogs, and delete-compaction maintenance are follow-up changes listed in
the proposal; nothing in this design forecloses them.
