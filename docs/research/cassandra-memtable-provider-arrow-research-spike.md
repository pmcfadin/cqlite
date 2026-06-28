# Cassandra Memtable Provider and Arrow Export Research Spike

**Date:** 2026-06-27
**Status:** Research output
**Audience:** CQLite maintainers, Apache Cassandra contributors, analytics integrators
**Related local docs:**
- [`cqlite-flight/README.md`](../../cqlite-flight/README.md)
- [`docs/plans/2026-06-17-cassandra-fast-analytics-arrow-flight-design.md`](../plans/2026-06-17-cassandra-fast-analytics-arrow-flight-design.md)
- [`docs/architecture/cassandra-sidecar-parquet-projections.md`](../architecture/cassandra-sidecar-parquet-projections.md)
- [`docs/compaction/byte-parity-rules.md`](../compaction/byte-parity-rules.md)
- [`docs/garbage-free-compaction-improvements/cqlite-findings-and-applicability.md`](../garbage-free-compaction-improvements/cqlite-findings-and-applicability.md)
- [`docs/garbage-free-compaction-improvements/ffm-memtable-investigation.md`](../garbage-free-compaction-improvements/ffm-memtable-investigation.md)
- [`docs/garbage-free-compaction-improvements/ffm-memtable-offheap-plan.md`](../garbage-free-compaction-improvements/ffm-memtable-offheap-plan.md)
- [`docs/development/parity-ci-tiers.md`](../development/parity-ci-tiers.md)

## Executive Summary

The research spike evaluated whether CQLite should pursue a Cassandra memtable
provider, a Cassandra-native gRPC-to-Arrow feature, or a staged combination of
both.

The recommendation is to **prioritize a Cassandra-native cursor-to-Arrow export
path and keep CQLite focused on SSTable/Arrow correctness, benchmarks, and
integration proof**. Treat a Rust/CQLite-backed Cassandra memtable provider as a
later, high-risk research lane only after the Arrow path, cursor reconciliation,
and operational gates are strong.

The short version:

1. Cassandra already has a deep pluggable memtable contract. It is not a simple
   "store rows here" extension point.
2. A memtable provider would put Rust/CQLite inside Cassandra's write durability,
   flush, repair, streaming, indexing, and operational failure boundary.
3. It would still not solve full-table analytics by itself, because memtables
   only contain unflushed writes.
4. The cleaner strategic path is to expose Cassandra's own reconciled read
   state through a cursor-backed gRPC/Arrow service, eventually including both
   memtables and SSTables.
5. CQLite remains highly valuable as the proving ground for SSTable decoding,
   reconciliation, Arrow type mapping, Flight/Trino integration, and parity
   testing.

```text
Recommended path

Near term:
  Cassandra snapshots/SSTables -> CQLite scanner -> Arrow Flight -> Trino/clients

Mid term:
  Cassandra cursor read path -> Cassandra-native gRPC/Arrow endpoint
    - can see memtables and SSTables
    - can reuse Cassandra's own reconciliation semantics

Later research:
  Rust/FFM memtable provider or storage-engine component
    - only after strong proof that Rust materially improves the live storage path
```

## Research Lanes

This spike split the question into five independent lanes:

| Lane | Question |
|---|---|
| Cassandra internals | How pluggable memtables, flush, compaction, repair, and cursor work constrain the idea. |
| Rust and Java post-JDK 21 | Whether JNI, JNA/JNR, FFM/Panama, or out-of-process Flight is the right Rust/JVM boundary. |
| Operations | Failure modes, rollout, rollback, security, metrics, resource isolation, and production trust. |
| CQLite strategy | Whether a memtable provider aligns with CQLite's goals and shipped surface. |
| Testing | Correctness, performance, memory, chaos, and acceptance criteria. |

## What Cassandra Requires From a Memtable

Cassandra's storage engine is an LSM design: writes are recorded in the commitlog,
applied to memtables, flushed to immutable SSTables, and later compacted. The
official storage-engine docs describe the core sequence as commitlog write,
memtable write, memtable flush, and SSTable persistence, with commitlog replay
restoring acknowledged writes after failure.

Primary reference:
- [Apache Cassandra storage engine documentation](https://cassandra.apache.org/doc/latest/cassandra/architecture/storage-engine.html)

Cassandra's pluggable memtable work is real and relevant. CEP-11 and the current
memtable API support named memtable configurations and table-level selection of
implementations such as skip-list and trie memtables.

Primary references:
- [CEP-11: Pluggable memtable implementations](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-11%3A%2BPluggable%2Bmemtable%2Bimplementations)
- [Cassandra `Memtable_API.md`](https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/memtable/Memtable_API.md)
- [Cassandra `Memtable.java`](https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/memtable/Memtable.java)

The implication is important: a provider must participate in a broad storage
contract:

- Commitlog durability, replay bounds, CDC/PITR behavior, and truncation safety.
- Write ordering and lifecycle barriers such as active/inactive memtable
  switching.
- Memory accounting, flush pressure, blocked writes, and metrics.
- Flush iteration through Cassandra's SSTable writers and metadata collectors.
- Read iteration that composes with SSTables and tombstone reconciliation.
- Schema changes, dropped columns, static columns, and serialization headers.
- Secondary indexes, SAI, and materialized-view update hooks or explicit
  rejection.
- Repair, bootstrap, streaming, snapshots, backups, and owned-range changes.

This is why "CQLite as a memtable provider" should be treated as a Cassandra
storage-engine project, not a CQLite connector feature.

## Alignment With CQLite

CQLite's current public direction is local Apache Cassandra SSTable access,
manipulation, write support, compaction parity, Arrow Flight, Trino integration,
BTI read/write support, and CDC-style export. That is naturally aligned with:

- Reading immutable SSTables.
- Proving Cassandra-compatible reconciliation.
- Producing typed Arrow/Parquet output.
- Providing a fast independent implementation for analytics and validation.
- Feeding fixtures, benchmarks, and compatibility findings back to Cassandra.

It is less naturally aligned with owning Cassandra's live in-memory storage path.
CQLite has its own WAL/memtable/write engine, but that implementation is an
embedded library design. It is not a drop-in replacement for Cassandra's
clustered commitlog/memtable/flush lifecycle.

The existing `cqlite-flight` crate already demonstrates the best near-term
boundary: a co-located Arrow Flight server that reads flushed SSTables, performs
a read-only compaction merge, applies token/predicate/projection filters, and
emits Arrow batches. Its explicit limitation is that memtable rows are invisible
until Cassandra flushes.

That limitation is real, but it is better solved by Cassandra exposing a
memtable-aware read cursor to Arrow than by CQLite impersonating Cassandra's
memtable implementation.

## Rust and Java Integration Post-JDK 21

There are four viable integration styles, but they fit different risk profiles.

| Option | Production internals fit | Prototype fit | Strengths | Main risks |
|---|---:|---:|---|---|
| Out-of-process gRPC/Arrow Flight | High for analytics | High | Crash isolation, container packaging, language-neutral clients, clean rollback | Does not see memtables unless Cassandra exposes them; IPC and Arrow IPC are not same-process zero-copy |
| JNI Rust `cdylib` | Narrow only | Medium | Mature, works on JDK 21, precise ABI control | JVM crash risk, JNI references, thread attach/detach, classloader/native-library constraints, packaging |
| JNA/JNR | Low for hot paths | Medium for probes/tools | Low Java glue | Slower/less explicit, JNI underneath, poor fit for Cassandra storage hot paths |
| FFM/Panama | Future production candidate | High for in-process research | Standard post-JDK-21 foreign memory/function API, `MemorySegment`/`Arena` lifetimes, C ABI calls | Requires JDK 22+ for finalized API, native-access warnings/restrictions, no crash isolation |

JDK 22 finalized the Foreign Function and Memory API in JEP 454. That makes FFM
the most coherent in-process research path for Rust/Java memory work after JDK
21. JEP 472 also makes native-access restrictions a forward-looking operational
concern.

Primary references:
- [JEP 454: Foreign Function & Memory API](https://openjdk.org/jeps/454)
- [JEP 472: Prepare to Restrict the Use of JNI](https://openjdk.org/jeps/472)
- [JNI Invocation API specification](https://docs.oracle.com/en/java/javase/25/docs/specs/jni/invocation.html)

Arrow has a separate boundary consideration. The Arrow C Data interface is the
same-process zero-copy sharing mechanism. Arrow Flight is a cross-process RPC
protocol built on gRPC and Arrow IPC. Flight is still the right production
analytics boundary, but it should not be described as same-process zero-copy.

Primary references:
- [Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html)
- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)

## Operational Assessment

The operational risk ranking is:

1. **Highest risk: in-process Rust memtable provider in Cassandra's live storage
   path.** A Rust panic, FFI defect, allocator bug, flush mismatch, or commitlog
   replay mismatch can affect node writes and data correctness. Rollback may not
   be enough if bad SSTables were emitted.
2. **Medium-high risk: Cassandra-native Java gRPC/Arrow service.** If kept
   read-only it is safer than a memtable provider, but in-JVM analytic scans can
   still compete with coordinator reads/writes, compaction, repair, streaming,
   GC, disk, and page cache.
3. **Lowest risk: out-of-process co-located Flight service over read-only
   snapshots.** Failures usually affect analytics queries, not Cassandra writes.
   Resource controls and rollback are much cleaner.

Operators would need the following before trusting any production path:

- A consistency and freshness contract: snapshot epoch, schema digest, TTL
  reference time, selected replica per token range, and explicit non-goals.
- Snapshot/provider lease management with unique names, cleanup, disk-pressure
  admission, and full-ring coverage checks.
- Hard limits for concurrency, CPU, memory, open files, disk bandwidth, batch
  size, deadlines, result bytes, and cancellation.
- TLS/mTLS, table-level authorization, signed short-lived tickets, audit logs,
  and no client-provided filesystem paths.
- Metrics and alerts for scan duration, bytes read, bytes returned, in-flight
  RPCs, backpressure time, memory, pinned snapshot bytes, disk pressure, and
  Cassandra foreground latency.
- Compatibility matrix for Cassandra version, SSTable format, Sidecar version,
  partitioner, compression, and excluded table features.
- Runbooks for disable, drain, rollback, leaked snapshot cleanup, bad schema,
  unsupported SSTable format, and range-planning failures.

## Recommended Architecture Tracks

### Track A: CQLite-Owned Near-Term Work

This is the practical path that can produce value while Cassandra-native work
goes through design and upstream review.

```text
Sidecar snapshot/ring/schema
        |
        v
CQLite SnapshotScanPlan
        |
        v
read-only SSTable reconciliation
        |
        v
projection/predicate/token filtering
        |
        v
Arrow RecordBatch stream
        |
        v
Flight/Trino/DataFusion clients
```

CQLite-owned deliverables:

- `SnapshotScanPlan` / `SnapshotManifest` API with schema digest, token ranges,
  selected SSTable manifest, projection, predicates, reference time, and limits.
- Bounded streaming reconciler for multi-SSTable reads.
- Public decorated-key token scan API with `(start, end]` and wraparound
  semantics.
- Predicate/projection pushdown with explicit residual predicate handling.
- Public Arrow `RecordBatch` stream builder in `cqlite-core`.
- `cqlite-flight` bounded streaming/backpressure and terminal error propagation.
- Benchmark suite for time-to-first-batch, rows/sec, decoded bytes/sec, peak
  memory, bytes read vs returned, fan-in, and Cassandra foreground impact.

### Track B: Cassandra-Upstream Cursor-to-Arrow

This is the strategic path Jon described: put the gRPC-to-Arrow capability in
Cassandra proper, based on cursor compaction/read-path work.

Desired properties:

- Read-only service.
- Memtable and SSTable visibility through Cassandra's own read/reconciliation
  model.
- Cursor-native to avoid materializing rows/cells unnecessarily.
- Shared semantics with compaction and storage-engine correctness rules.
- Cassandra-owned security, authz, metrics, tracing, resource limits, and
  upgrade lifecycle.
- CEP-shaped design after Cassandra 6.0 timing permits.

CQLite's role:

- Prove Arrow schema and type mapping.
- Provide independent SSTable/Arrow implementation for comparison.
- Supply benchmark methodology and query-engine integration learnings.
- Feed edge cases from compaction parity back into Cassandra's test matrix.

### Track C: Rust/FFM Memtable Provider Research

This is a later research lane, not the primary improvement.

It should proceed only if there is evidence that Rust/CQLite can materially
improve Cassandra's live storage path and if Cassandra's JDK baseline makes FFM
practical.

Prerequisites:

- A narrow table-feature subset, explicitly declared.
- FFM/Panama prototype with safe ownership boundaries and no Rust panic crossing
  the Java boundary.
- Commitlog replay and memtable switching proof.
- Flush output validated by Cassandra load/read/repair/streaming paths.
- Memory accounting parity with Cassandra memtable limits.
- Failure-injection tests covering crash, replay, rollback, and cleanup.

## Testing and Acceptance Criteria

The testing rule should be evidence-driven: no P0 data-loss or correctness claim
can be backed only by smoke tests.

Use the existing CQLite parity taxonomy:

- `smoke`: parses/loads without error.
- `canonical_semantic`: decoded values match Cassandra after documented
  normalization.
- `byte_for_byte`: output bytes/components match Cassandra exactly.

### Correctness Gates

| Area | Required gate | Primary home |
|---|---|---|
| Memtable-visible reads | CQL write is visible through Arrow before flush; unflushed and flushed generations reconcile like Cassandra `SELECT`; concurrent write contract is documented. | Cassandra upstream and integration repo |
| Cursor-compaction equivalence | Same input SSTables, deterministic timestamps, explicit `gcBefore`/`now`, logical parity first, byte parity where claimed. | CQLite plus `compaction-parity/` |
| Arrow output equivalence | Arrow schema and values match canonical Cassandra rows; batch boundaries are not semantic; nulls, decimals, UUIDs, time, lists, maps, UDTs, and residual predicates are covered. | CQLite, Cassandra upstream, integration repo |
| Live cluster behavior | Exactly-once token coverage, one schema digest, one TTL reference time, selected healthy replica per interval, no silent range gaps/overlaps. | Integration repo |

### Performance Gates

Track:

- Time to first Arrow batch.
- Rows/sec and decoded bytes/sec.
- SSTable/memtable bytes read vs returned.
- CPU per decoded row and per returned row.
- Peak memory and allocation rate.
- Network bytes and backpressure time.
- Reconciliation fan-in.
- Cassandra foreground read/write latency during scans.
- Snapshot or provider acquisition time.

Vary:

- SSTable count.
- Memtable size.
- Predicate selectivity.
- Projection width.
- Row width.
- Compression.
- Tombstone/TTL density.
- Collections and UDTs.
- Concurrency.
- BIG and BTI formats.

### Failure Gates

Cover:

- Sidecar unavailable.
- Provider unavailable.
- Partial snapshot/provider acquisition.
- Node death mid-stream.
- Schema mismatch.
- Unsupported type or SSTable format.
- Topology change.
- Rolling restart.
- Flush/compaction while scanning.
- Disk pressure.
- Bad predicate or type mapping.
- Network partition.
- Cancellation.
- Slow client.

Scanner failures must return terminal errors. They must not be converted into a
clean EOF.

## Prototype Acceptance Bar

A research/prototype phase is successful when:

- The consistency/freshness/unsupported-feature contract is written.
- A transport-only Arrow spike passes on one complete BIG/OA SSTable with scalar
  columns and no deletes, TTL, or schema changes.
- A memtable-visible prototype, if attempted, proves unflushed rows are Arrow
  visible and equal to Cassandra `SELECT` for a small deterministic matrix.
- A 3-node RF=3 integration test proves exactly-once token coverage and zero
  canonical row diffs for the declared matrix.
- CQLite compaction logical parity remains green for covered scenarios.
- Gaps are recorded explicitly instead of hidden behind smoke tests.

Production readiness requires:

- No P0 claim backed only by smoke evidence.
- Required parity CI against real Cassandra-backed fixtures.
- Nightly/live regeneration or validation against pinned Cassandra versions.
- Complete declared matrix with zero canonical semantic diffs.
- Byte parity where byte identity is claimed.
- Memory bounded under 10x data growth and slow-client backpressure.
- Passing security and operations gates: mTLS/RBAC, signed tickets, quotas,
  cancellation, metrics, audit logs, upgrade/rollback matrix, cleanup, and
  runbooks.

## Proposed GitHub Improvement Scope

Create one improvement issue for this research output with the following scope:

1. Adopt this recommendation as the working strategy:
   - CQLite-owned snapshot/Arrow/Flight correctness first.
   - Cassandra-native cursor-to-Arrow as the upstream strategic target.
   - Rust/FFM memtable provider as later research.
2. Add CQLite issues or sub-issues for:
   - `SnapshotScanPlan` / `SnapshotManifest`.
   - Bounded streaming reconciler.
   - Decorated-key token scans.
   - Predicate/projection pushdown contract.
   - Public Arrow `RecordBatch` stream.
   - `cqlite-flight` streaming/backpressure/error propagation.
   - Benchmark and parity matrix.
3. Link future Cassandra CEP/prototype discussions back to this document.

## Final Recommendation

Do not make "CQLite as Cassandra memtable provider" the next primary direction.
It is too tightly coupled to Cassandra's live storage engine and has the wrong
blast radius for an analytics/export problem.

The strongest path is:

1. Harden CQLite's snapshot-backed SSTable-to-Arrow path.
2. Use that as the proof and benchmark bed for a Cassandra-native cursor-to-Arrow
   design.
3. Revisit Rust/FFM memtable or storage-engine pluggability only after the
   upstream Arrow interface and CQLite parity gates show a clear performance or
   operational reason to do so.
