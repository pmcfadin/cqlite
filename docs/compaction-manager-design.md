# Design: Standalone Compaction Manager for CQLite

**Issue:** [#905](https://github.com/pmcfadin/cqlite/issues/905)
**Status:** Draft for discussion
**Depends on:** M5 (SSTable write support) for the executor; the planner and simulator have no write dependency.

## 1. Summary

This document proposes a compaction subsystem for CQLite built as three layers: a **planner** (a pure-function implementation of compaction strategies, starting with UCS), an **executor** (a k-way merge engine that reads N sstables and writes their merged replacement via the M5 writer), and a **manager** (a long-running process that schedules and throttles compaction work over one or more table directories). Each layer is independently useful and ships in that order.

The design deliberately inverts a Cassandra assumption: compaction here is an *offline, standalone* operation over files on disk, with no live node, no memtables, and no cluster membership. That constraint is what makes the tool simple, but it also changes the correctness rules — particularly around tombstone garbage collection — and those changes are treated as first-class design decisions rather than caveats.

## 2. Motivation and use cases

CQLite's current design philosophy is "single sstable per table, no compaction complexity." Two forces make that untenable to hold forever. First, M5 introduces write support: once CQLite can create sstables, any workload with ongoing writes produces many of them, and read performance degrades linearly with sstable count until something merges them. Second — and independently — there is a standing operational gap in the Cassandra ecosystem that a standalone compactor fills:

**Snapshot and backup hygiene.** A node's snapshot contains hundreds of sstables accumulated across compaction generations, full of shadowed data and expired tombstones. Compacting a snapshot offline before archiving or restoring it can shrink it dramatically and speeds up every downstream consumer.

**Analytics pre-merge.** CQLite's export path (Parquet, CSV, CQL) currently reads sstables as-is. Merging to a canonical view first — one pass that resolves timestamps and tombstones — makes exports both smaller and semantically correct without every export format reimplementing reconciliation.

**Compaction offload (future).** Compaction inside a Cassandra node competes with reads for CPU, page cache, and disk bandwidth. A standalone compactor with its own resource envelope (its own cgroup, its own machine, its own throttle) is the building block for offloading that work entirely — ship sstables out, compact on cheap hardware, ship results back. This design does not implement offload, but takes care not to foreclose it.

**Strategy research and tuning.** Because the planner is a pure function over sstable metadata, it doubles as a simulator: replay a strategy against a real directory or a synthetic workload and measure write amplification, space amplification, and sstable count over time — without touching any data. No standalone tool like this exists today.

**CQLite-native tables.** Post-M5, CQLite's own write path needs background compaction. The manager is that component.

## 3. Goals and non-goals

Goals:

- Implement UCS (Unified Compaction Strategy) as the first and default strategy, behind a strategy trait that third parties can implement.
- Merge semantics that are bit-for-bit faithful to Cassandra 5 reconciliation: last-write-wins by timestamp, row and range tombstone shadowing, TTL expiry, static rows.
- Output sstables in valid Cassandra 5 'oa' format, so compacted data can be handed back to a cluster (`nodetool import` / sstableloader workflows).
- Crash safety: an interrupted compaction never leaves a table directory in an ambiguous or data-losing state.
- Explicit, user-controlled tombstone GC with safe defaults.
- Deterministic, offline operation: given the same files and config, a run is reproducible; no network access in the compaction path.

Non-goals (v1):

- Compacting files under a **live Cassandra node's data directory**. Cassandra owns those files through its own lifecycle transaction machinery; external modification corrupts node state. The tool operates on snapshots, backups, offline data directories, and CQLite-native tables only, and should refuse or loudly warn when it detects a live node (e.g. presence of `*.log` lifecycle transaction files with recent mtimes).
- Distributed coordination between multiple compactor instances.
- Strategies other than UCS (the trait allows them; we do not commit to shipping them).
- Anti-compaction, repair awareness, or pending-repair sstable handling.

## 4. Background (short version)

Readers familiar with LSM compaction and UCS can skip to §5. A fuller UCS primer is in Appendix A.

Cassandra tables are log-structured: writes accumulate in immutable sstables, and a row's current value is the timestamp-wise merge of its fragments across all sstables that contain it. Deletes are writes too — tombstones — which must persist long enough to propagate to all replicas before they can be purged. Compaction is the background process that merges sstables, discarding shadowed data and expired tombstones, trading **write amplification** (rewriting data repeatedly) against **read amplification** (consulting many sstables per read) and **space amplification** (retaining dead data).

Classic Cassandra strategies pick different points on that triangle. STCS (size-tiered) merges similarly-sized sstables — cheap writes, poor reads and space. LCS (leveled) maintains non-overlapping runs per level — great reads, expensive writes. TWCS specializes for time-series. **UCS**, introduced in Cassandra 5, unifies tiered and leveled behavior under one algorithm with a per-level scaling parameter, so a single implementation can express the whole spectrum (T4 ≈ STCS, L10 ≈ LCS) and even mix regimes across levels. Implementing UCS once gives us the practical coverage of implementing all three classic strategies, which is why it goes first.

The one Cassandra concept UCS adds that matters to our data model: sstables are bucketed by **density** (data size divided by the fraction of the token space the sstable spans), not raw size, so the planner needs each sstable's token range — which CQLite already parses from `Statistics.db`.

## 5. Architecture overview

```
┌───────────────────────────────────────────────────────┐
│ Layer 3 · cqlite-compactd (manager)                   │
│ discovery · scheduler · throttle · control API        │
└──────────────────────────┬────────────────────────────┘
┌──────────────────────────┴────────────────────────────┐
│ Layer 2 · cqlite-compaction (library)                 │
│ planner (strategy trait, UCS) · executor · simulator  │
└───────────┬──────────────────────────────┬────────────┘
┌───────────┴───────────┐      ┌───────────┴────────────┐
│ Layer 1 · reader      │      │ Layer 1 · writer (M5)  │
│ cqlite-core, shipped  │      │ hard dependency        │
└───────────────────────┘      └────────────────────────┘
```

The library layer is the product; the CLI (`cqlite compact`, `cqlite simulate`) and the daemon are thin shells over it. The planner never performs I/O. The executor performs all I/O but makes no decisions about *what* to compact. This separation is the "more modular" answer to Cassandra, where strategy logic is entangled with `ColumnFamilyStore`, disk boundaries, and the live tracker, and cannot be exercised without most of a node running.

## 6. The planner

### 6.1 Strategy as a pure function

A strategy consumes an immutable snapshot of table state and returns proposed jobs. It performs no I/O, holds no locks, and has no side effects. This is the load-bearing decision in the whole design: it makes strategies unit-testable with fabricated metadata, makes third-party strategies possible without exposing internals, and makes the simulator (§12) nearly free.

```rust
pub trait CompactionStrategy: Send + Sync {
    /// Propose zero or more compaction jobs for the current state.
    /// Must be deterministic for a given (state, config) pair.
    fn plan(&self, state: &TableState) -> Vec<CompactionJob>;

    /// Human-readable explanation of why each job was (or wasn't) proposed.
    /// Powers `--dry-run --explain` and the simulator's reporting.
    fn explain(&self, state: &TableState) -> PlanReport;
}

pub struct TableState {
    pub sstables: Vec<SSTableMeta>,
    pub schema: TableSchema,          // parsed from schema.cql
    pub now: Timestamp,               // injected, never read from the clock
}

pub struct SSTableMeta {
    pub id: SSTableId,
    pub data_size: u64,
    pub first_token: Token,
    pub last_token: Token,            // density = size / token span covered
    pub min_timestamp: Timestamp,
    pub max_timestamp: Timestamp,
    pub estimated_droppable_tombstones: f64,
    pub level_hint: Option<u32>,      // from Statistics.db when present
}

pub struct CompactionJob {
    pub inputs: Vec<SSTableId>,
    pub kind: JobKind,                // Merge | SplitShard | TombstoneOnly
    pub priority: Priority,
    pub estimated_output_size: u64,
    pub output_shards: u32,           // UCS sharding, 1 = single output
}
```

`now` being injected rather than read from the clock is deliberate: it keeps runs reproducible and lets the simulator fast-forward time.

### 6.2 UCS specifics

The UCS implementation follows Cassandra 5's model: sstables are grouped into levels by density, each level has a scaling parameter (`T*` tiered-leaning, `L*` leveled-leaning, `N` neutral), and a level triggers a merge when its bucket exceeds the parameter's threshold. Configuration mirrors Cassandra's so that settings can be copied verbatim from `system_schema.tables`:

```toml
[table."ks.events".compaction]
strategy            = "ucs"
scaling_parameters  = ["T4", "T4", "L8"]  # per level; last value repeats
target_sstable_size = "1GiB"
base_shard_count    = 4
```

One scenario Cassandra never faces but we hit on day one: the input directory was usually produced by **some other strategy** — years of STCS output in a snapshot, say. Convergence from an arbitrary legacy layout to the target UCS shape is therefore a first-class planner mode, not an edge case. A one-shot `cqlite compact` run plans the full convergence (possibly as a sequence of jobs to bound peak disk usage); the long-running manager then does incremental maintenance. The simulator can cost a convergence before you commit to it.

### 6.3 Major compaction

`JobKind::Merge` over all live sstables, sharded per UCS output rules. This is also the degenerate strategy used by the phase-1 CLI before the UCS planner lands: "merge everything into one (or N sharded) sstables" requires the executor only.

## 7. The executor

### 7.1 Merge

The executor opens the job's input sstables and drives a k-way merge iterator in (partition key, clustering key) order — the same shape as CQLite's existing multi-sstable read path, if one exists post-M3 tombstone work, and shared with it where possible. Reconciliation follows Cassandra semantics exactly: cell-level last-write-wins by timestamp (ties broken by value, matching Cassandra), row tombstones and range tombstones shadow older data, TTL'd cells convert to tombstones at expiry, static rows merge per-partition. Output rows stream directly into the M5 writer; memory stays bounded by the merge heap plus writer buffers regardless of input size, consistent with the project's <128MB target.

### 7.2 Tombstone purge and the overlap invariant

Whether a tombstone can be *dropped* (rather than carried forward) is governed by two independent checks, and conflating them is the classic way to resurrect deleted data:

1. **Age:** the tombstone's local deletion time is older than `gc_grace_seconds` (see §8 for where that value comes from).
2. **Overlap:** no sstable *outside the job's input set* contains data for the same partition older than the tombstone. Dropping a tombstone while an excluded sstable still holds the data it shadows resurrects that data on the next read.

Check 2 is a hard invariant in **every** GC mode, including `authoritative`. The modes in §8 control check 1's policy; nothing controls check 2 — it is always enforced, using min/max token ranges and, where necessary, partition-level bloom/index probes against the excluded set. Standalone usage makes this more important than in Cassandra, not less, because users will naturally run partial compactions ("just merge these three files").

### 7.3 Atomicity and crash safety

A compaction transaction proceeds as: write outputs to a `tmp-<uuid>/` directory inside the table dir → fsync all components → append a commit record to a small transaction log (`compaction-<uuid>.txn`) naming inputs and outputs → atomically rename outputs into place → delete inputs → delete the log. On startup (or `cqlite compact --repair-log`), an incomplete log is rolled back (delete temps, keep inputs) if no commit record exists, or rolled forward (finish renames/deletions) if one does. This is a simplified version of Cassandra's lifecycle transaction log, and it is the reason the tool must never run against a live node's directory — two writers of this protocol cannot coexist.

### 7.4 Resource isolation

"Isolated" in the issue is read as *resource* isolation. The executor exposes a bytes-per-second throttle and a concurrency limit as library-level knobs; everything stronger comes free from being a separate OS process — cgroups, `nice`/`ionice`, or simply running on a different machine against a snapshot. The design assumes nothing about locality beyond "inputs are readable, output dir is writable," which is what keeps the future offload scenario open.

## 8. Tombstone GC modes and gc_grace resolution

Standalone compaction cannot know whether a tombstone has propagated to all replicas, so the user must choose a policy. Three modes:

- **`off`** (default): never drop tombstones; pure merge. Always safe, still removes shadowed data, which is usually most of the win.
- **`grace`**: drop tombstones older than `gc_grace_seconds`, subject to the overlap invariant. Matches a node's local behavior and inherits the same repair-related risks — the doc for this flag says so.
- **`authoritative`**: the user asserts this dataset is the complete truth (a full-cluster export, or a CQLite-native table with no replicas). Grace is ignored; the overlap invariant is not.

`gc_grace_seconds` resolves with the following precedence (highest wins):

1. CLI flag (`--gc-grace 864000`)
2. Config file (`[table."ks.events".gc] gc_grace_seconds = ...`)
3. `schema.cql` table property (`WITH gc_grace_seconds = N`) — the free win, since CQLite already parses this file
4. Cassandra's default (864000, 10 days) with a warning that no explicit value was found

To keep the compaction path offline and deterministic, fetching values from a live cluster is a **separate command**, not part of compaction:

```
cqlite schema pull --contact-points node1:9042 --keyspace ks --table events
```

This connects over the native protocol, reads `system_schema.tables` (which carries `gc_grace_seconds` *and* the live compaction params), and writes/updates the local `schema.cql` and config — meaning "match what the cluster does" is one command away, after which every run is reproducible from files on disk. The CQL driver dependency lives behind a `cluster-sync` feature flag so the core library stays dependency-lean.

## 9. Configuration

One TOML file, per-table sections, everything overridable per-invocation. Complete example:

```toml
# cqlite-compaction.toml

[defaults]
throughput_limit = "64MiB/s"
concurrent_jobs  = 2

[table."ks.events"]
schema   = "schemas/events.cql"
data_dir = "data/ks/events"

[table."ks.events".compaction]
strategy            = "ucs"
scaling_parameters  = ["T4", "T4", "L8"]
target_sstable_size = "1GiB"
base_shard_count    = 4

[table."ks.events".gc]
mode             = "grace"        # off | grace | authoritative
gc_grace_seconds = 864000         # optional; overrides schema.cql
```

## 10. CLI surface

```
cqlite compact <table-dir> [--config f] [--strategy ucs|major]
               [--gc-mode off|grace|authoritative] [--gc-grace SECS]
               [--dry-run [--explain]] [--throughput 64MiB/s] [--jobs N]

cqlite simulate <table-dir | --synthetic workload.toml>
               [--strategy-config f] [--steps N] [--report json|table]

cqlite schema pull --contact-points h:9042 --keyspace ks --table t [--out dir]

cqlite compact --repair-log <table-dir>     # recover interrupted transaction
```

`--dry-run` runs the planner only and prints the jobs it would execute; `--explain` adds the strategy's reasoning per bucket (from `PlanReport`). These two flags are cheap because of the pure-planner design and are expected to be the most-used flags in practice.

## 11. The manager (`cqlite-compactd`)

A long-running process wrapping the library: it discovers table directories (explicit config list first; filesystem watching later), runs a plan/execute loop per table, and enforces global limits — max concurrent jobs, aggregate throughput, and a disk-headroom guard that refuses jobs whose estimated output would drop free space below a threshold. A small HTTP endpoint serves status (per-table pending/running jobs, progress, bytes compacted), Prometheus-format metrics, and pause/resume. Deliberately boring: all interesting logic lives in the library, and the daemon must stay simple enough to reason about in a crash loop.

## 12. The simulator

`plan()` being pure over `TableState` means simulation is a loop, not a subsystem: synthesize or ingest a starting state, apply the planner, apply each job's *predicted* effect to the metadata (no data touched), inject new "flushed" sstables per a workload model, advance `now`, repeat. Reported per run: cumulative write amplification, space amplification over time, sstable count over time, and largest transient disk requirement. Primary uses: costing a legacy-layout convergence before running it (§6.2), and comparing scaling parameters against a real directory's shape. This is the piece with no existing standalone equivalent in the ecosystem and is a strong candidate for the first public demo.

## 13. Safety invariants (normative)

1. Never modify a directory that shows signs of a live Cassandra node; refuse by default, proceed only with `--force-unsafe`.
2. Inputs are never deleted before outputs are durably committed (fsync + txn log commit record).
3. The tombstone overlap check (§7.2) is enforced in every GC mode without exception.
4. In `off` mode, the merged output is read-equivalent to the input set: any query returns identical results before and after.
5. A killed process leaves the directory recoverable by `--repair-log` to a state equivalent to either "job never ran" or "job completed" — never in between.
6. The compaction path performs no network I/O.

Invariant 4 is testable property-based: generate random sstable sets with the existing test-data tooling, compact, and diff full-table reads. This should be the backbone of the executor's test suite.

## 14. Phasing

**Phase A — executor + major compaction** (needs M5 writer): `cqlite compact --strategy major`, GC modes, txn log, throttle. Immediately useful for snapshot hygiene and pre-export merge. Forces the tombstone-semantics work early, where it belongs.

**Phase B — UCS planner + simulator** (no writer dependency; can proceed in parallel with M5): strategy trait, UCS with density bucketing and legacy-layout convergence, `--dry-run/--explain`, `cqlite simulate`.

**Phase C — manager daemon**: scheduling loop, global limits, status/metrics API, pause/resume.

**Phase D (exploratory) — cluster sync & offload groundwork**: `schema pull`, and validation that round-tripped sstables import cleanly into a live Cassandra 5 cluster.

## 15. Open questions

- Should `schema pull` seed the UCS config from the cluster's live compaction params by default (making "match the cluster" the zero-config behavior), or stay copy-only?
- Sharding fidelity: do we replicate UCS's shard-boundary computation exactly (bit-compatible output layout with what a node would produce), or is "valid oa format, sensible shards" sufficient for v1?
- How much of the tombstone-merge machinery currently behind the `tombstones` feature flag is reusable as the executor's reconciliation core, versus needing a rewrite for streaming writes?
- Does `estimated_droppable_tombstones` from Statistics.db carry enough signal to justify `JobKind::TombstoneOnly` (single-sstable rewrite) in v1, or defer?
- Minimum viable overlap check: token-range intersection only (coarse, may retain some droppable tombstones) vs. per-partition index probes (precise, slower). Propose coarse for v1 with the precise path behind a flag.

## Appendix A — UCS primer

*Background for readers who have not worked with Cassandra 5's Unified Compaction Strategy. Nothing here is normative.*

**The problem UCS solves.** Cassandra historically shipped separate strategies occupying fixed points on the write/read/space-amplification triangle, and switching between them was disruptive (a full rewrite of the table's layout). UCS's insight is that tiered and leveled compaction are the same algorithm with one parameter flipped — how many sstables (or "runs") you tolerate per size class before merging.

**Density, not size.** UCS classifies sstables into levels by *density*: data size divided by the fraction of the token space the sstable covers. A 1 GiB sstable spanning the whole ring and a 256 MiB sstable spanning a quarter of it have the same density and belong to the same level. This matters because UCS also *shards* output: big compactions split their output at token boundaries into multiple sstables, which caps individual file size and increases parallelism. Raw size would misclassify those shards; density classifies them correctly. Consequence for any implementation: the planner must know each sstable's first/last token, not just its byte size.

**Scaling parameters.** Each level gets a parameter from an integer `w`, written in operator-friendly form:

| Notation | Meaning | Behaves like |
|---|---|---|
| `T4`, `T10`, … | tiered with fan-out F = value; merge when F sstables accumulate in a level | STCS (T4 ≈ classic STCS) |
| `N` | neutral, F = 2 | middle ground |
| `L4`, `L10`, … | leveled with fan-out F = value; merge as soon as 2 runs exist in a level | LCS (L10 ≈ classic LCS) |

Higher `T` values favor cheap writes and tolerate more sstables per read (bounded read amplification per level ≈ F−1 for tiered, 1 for leveled). A common production pattern is tiered at low levels (absorb flush churn cheaply) and leveled at high levels (where data is cold and read amplification hurts most) — e.g. `["T4","T4","L8"]`. The parameter list applies per level, with the last value repeating for all deeper levels.

**Why UCS first for CQLite.** One implementation covers the practical behavior space of STCS, LCS, and much of what people use TWCS for; its configuration is directly copyable from a Cassandra 5 cluster's schema; and it is the strategy the Cassandra project itself is converging on, which matters for a project intended for eventual donation. The costs are the density/sharding machinery (modest, since CQLite already parses the needed metadata) and slightly harder mental model for contributors, which this appendix exists to offset.

**Further reading.** CEP-26 (Unified Compaction Strategy), the Cassandra 5 documentation on UCS, and `UnifiedCompactionStrategy.java` in the Cassandra source tree are the authoritative references for the bucketing and shard-boundary math; this design intentionally defers bit-level fidelity questions to §15.
