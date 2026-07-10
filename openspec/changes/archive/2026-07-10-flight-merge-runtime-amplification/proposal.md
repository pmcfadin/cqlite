# Flight/merge runtime amplification — bound the producer-thread cost per merge

## Milestone
0.14 (read-path performance). **Design-driven** — this is a merge-architecture change to the
k-way merge that both compaction and the Flight `do_get` read path share; it warrants an OpenSpec
change and a `spec-auditor` (C) intent audit at closer time. Promoted from epic #2313 WS3 (owner
decision 2026-07-09) as the top-ranked failure mode in
`docs/architecture/issue-throughput-saturation-research.md`; directly relevant to the round-6
loaded-cluster field runs.

## Why
The k-way merge in `cqlite-core/src/storage/write_engine/merge/mod.rs` — invoked by the Flight
`do_get` streaming egress via `spawn_blocking` (`cqlite-flight/src/streaming.rs`) and by the
write-engine compaction/maintenance paths — fans out **one OS producer thread per input SSTable**:

- `KwayMerge::new_with_gc_and_registry_cancellable` (~line 2190) loops over `input_paths` and calls
  `SSTableRowIteratorAdapter::open()` once per path.
- `open()` (~line 442) does `std::thread::spawn(producer_thread)` — one OS thread per SSTable.
- `producer_thread` (~line 485) constructs **its own full multi-threaded `tokio::runtime::Runtime`
  via `Runtime::new()`** (~line 519). A default multi-threaded runtime spins up `num_cpus` worker
  threads (plus an on-demand blocking pool).

Net thread cost of ONE merge over M SSTables ≈ **M producer threads + M·num_cpus runtime workers**.
A 10-SSTable table on a 32-core node ≈ ~320 threads for a single query; N concurrent Flight queries
multiply that into the thousands. Kernel scheduler / futex contention arrives well before disk
bandwidth saturates — the defect caps throughput under concurrency.

The amplification is currently **invisible**: no thread-count / blocking-pool gauge exists in the
observability surface, so a loaded node shows the symptom (context-switch storm) with no metric that
names the cause.

Critically, each `producer_thread` runs a **single sequential async scan**
(`reader.stream_all_partitions_for_compaction(...).await` inside one `rt.block_on(...)`), with no
`tokio::spawn` of parallel work inside it. The multi-threaded worker pool each producer builds is
therefore never used for concurrency — it is pure per-producer overhead.

## What changes
Bound the per-merge thread cost to **O(M)** by removing the per-producer multi-core runtime. The
recommended mechanism (see `design.md`) is to drive each producer's sequential async scan on a
**`current_thread` Tokio runtime** owned by that producer thread — which adds **zero** extra worker
threads — instead of a multi-threaded `Runtime::new()`. This is a localized change to
`producer_thread`; the merge's channel/backpressure, ownership/lifecycle, k-way heap, and
cancellation (#2264) semantics are untouched, so merge output stays byte-identical.

Alongside it, land a **thread / blocking-pool gauge** in the observability surface so the bound is
observable in production and assertable in a pinned regression test (coordinating the metric naming
with epic #2313 WS2).

## Non-goals
- **A full task-based rearchitecture to O(1) threads** (candidate c — producers as tokio tasks on
  the server's shared runtime). It could push below O(M) but requires reworking the blocking
  `SyncSender` backpressure into an async channel and re-plumbing #2264 cancellation across the
  task boundary — a large blast radius that endangers byte-parity for no gain over O(M). Out of
  scope for 0.14; may be revisited if O(M) proves insufficient under field load.
- **Changing merge output, reconciliation, tombstone shadowing, or GC semantics.** Byte-parity vs
  Apache Cassandra is the invariant, not a target.
- **Tuning the streaming channel capacity or the k-way heap.**
- **Bindings / CLI surface changes.** This is internal to the storage + Flight read path.

## Doctrine impact
- No change to the no-heuristics mandate (#28), the version floor, or the write-surface claim
  boundary.
- Adds one gauge to the observability catalog (`cqlite-core/src/observability/catalog.rs`) and its
  doc; the metric name is coordinated with epic #2313 WS2 so the two workstreams do not collide on
  naming. CLAUDE.md / website doctrine need no change (no workflow or user-facing contract shifts).

## Cross-links
Epic #2313 (WS3; WS2 = the metrics surface). #2230 (materialization — same file). #1668 (the
write-engine compaction path — **same `write_engine/merge/mod.rs` file**, so a real if mechanical
merge-conflict risk if #1668 is in flight concurrently; flagged to the lead, not a code dependency).
