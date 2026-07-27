## Compaction Strategies

Compaction rewrites SSTables to control read amplification, reclaim space from tombstones/expired data, and maintain healthy on-disk layouts. This chapter compares Size-Tiered (STCS), Leveled (LCS), and Time-Window (TWCS) strategies in Cassandra 5.0, calls out tombstone purging behavior and overlap implications, and includes a sidebar on Unified Compaction Strategy (UCS).

### In this chapter you will learn
- The goals and mechanics of STCS, LCS, and TWCS
- Trade-offs across size-, level-, and time-oriented compaction
- How tombstone purging works and why overlap matters
- Practical defaults and when to use each strategy

## Strategy Overviews

### Size-Tiered Compaction Strategy (STCS)
STCS groups SSTables of similar size into tiers and periodically merges a handful into larger SSTables. It minimizes write amplification on write-heavy, append-mostly workloads but allows overlapping SSTables across wide key ranges, increasing read amplification for point and range queries.

Key parameters include `min_threshold`/`max_threshold` (number of SSTables to compact) and bucket sizing (`bucket_low`/`bucket_high`).

### Leveled Compaction Strategy (LCS)
LCS organizes SSTables into size-constrained levels where each SSTable at L1+ contains non-overlapping token ranges. This sharply reduces read amplification at the cost of higher write amplification and compaction work. It is a strong default for read-heavy, low-latency workloads with random point and short-range queries.

Key parameters include `sstable_size_in_mb` and `fanout_size` (how many SSTables per next level target size).

### Time-Window Compaction Strategy (TWCS)
TWCS places SSTables into time windows (defaults to 1 day; configurable to hours or other units) and compacts only within each window. This isolates older immutable data from newer hot data and is well-suited to time-series and TTL-heavy workloads. It eases tombstone purging when windows close, while accepting overlap across windows for large time-range scans.

Key parameters include `compaction_window_unit` and `compaction_window_size` (and optional split during flush).

## Comparison

Small comparison of strategy behaviors (indicative, not absolute):

| Strategy | Organizing principle | Read amplification | Write amplification | Space amplification | Best for |
| --- | --- | --- | --- | --- | --- |
| STCS | Merge similar-size tiers | Higher (overlap across many SSTables) | Low | Low–Moderate | High-ingest, append-mostly, larger partitions |
| LCS | Non-overlapping leveled ranges | Low (except L0 during backlog) | Higher | Low | Read-heavy, low-latency point/slice reads |
| TWCS | Time windows | Moderate (overlap across windows) | Low–Moderate | Low | Time-series, TTL-heavy, time-bucketed access |

![Compaction strategy comparison](diagrams/compaction-strategy-comparison)
- Alt text: Visual summary of STCS/LCS/TWCS organizing principles and typical trade-offs.
- Caption: STCS groups by size, LCS levels to remove overlap, TWCS isolates data in time windows.

## Memory and IO Patterns (Operational Shape)

- STCS
  - IO: Predominantly sequential writes for the new SSTable, mixed random reads across N similarly sized inputs.
  - Memory: Iteration buffers per input SSTable; minimal in-memory state compared to LCS. Bloom/Index/Summary may be mmapped rather than fully loaded.
  - Space overhead: Temporary disk usage roughly equal to the size of the compaction output until old files are removed.
- LCS
  - IO: Many small random reads across levels; sequential writes of level-target-sized SSTables. L0 can temporarily increase read amp until compacted.
  - Memory: Additional manifest/accounting overhead and higher iterator concurrency; more frequent compaction cycles due to small targets.
  - Space overhead: Usually lower than STCS for steady-state but can spike during level reshaping.
- TWCS
  - IO: Bounded to active time window(s); compactions are localized; cross-window scans still incur overlap.
  - Memory: Similar to STCS within a window; benefits from window isolation for cache locality.
  - Space overhead: Localized to windows being compacted; TTL expiry tends to reclaim space efficiently as windows age.

Concurrency and throttling:
- Compaction is typically multi-threaded and rate-limited; per-strategy concurrency interacts with disk bandwidth and page cache. LCS often benefits from stricter throttling to avoid foreground read jitter; STCS benefits from batching/merging larger tiers.

Implementation touchpoints (Cassandra 5.0.8): `CompactionManager`, `CompactionController`, strategy classes listed below.

### Sidebar: Unified Compaction Strategy (UCS)

UCS ([`UnifiedCompactionStrategy`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/compaction/UnifiedCompactionStrategy.java))
unifies tiered and leveled compaction. It groups SSTables into exponential density levels, compacts when a
configurable number of SSTables accumulate on a level, and splits output across token-range shards for concurrent
compaction without cross-node coordination.

#### Scaling Parameter W

The `scaling_parameters` option is a comma-separated list of integers W, one per level
(the last value extends to all higher levels). The W value encodes both fanout and threshold
([`UnifiedCompactionStrategy.java:106–113`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/compaction/UnifiedCompactionStrategy.java#L106-L113)):

- **W > 0 → tiered (T-style)**: fanout `f = 2 + W`, threshold `t = f`. Written as `T(f)` — e.g. `T4` for W=2 gives
  f=4, t=4. Compaction fires once four SSTables accumulate; low write amplification, higher read amplification.
- **W < 0 → leveled (L-style)**: fanout `f = 2 − W`, threshold `t = 2`. Written as `L(f)` — e.g. `L10` for W=−8
  gives f=10, t=2. Compact aggressively at every two SSTables; low read amplification, higher write amplification.
- **W = 0 → N**: `f = t = 2`. Midpoint; equivalent to T2 or L2.

**Default `scaling_parameters`**: `T4`, matching STCS default threshold=4. To emulate LCS with fanout 10, use `L10`.

#### Key Options and Defaults

Loaded via `Controller.fromOptions()`
([`Controller.java:408–461`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/compaction/unified/Controller.java#L408-L461));
documented in [`UnifiedCompactionStrategy.md`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/compaction/UnifiedCompactionStrategy.md):

| Option | Default | Notes |
|--------|---------|-------|
| `scaling_parameters` | `T4` | Per-level W list; last value repeats |
| `target_sstable_size` | 1 GiB | Minimum enforced at 1 MiB (`Controller.java:83`) |
| `base_shard_count` | 4 | Min shards for lowest density levels; 1 for system tables |
| `sstable_growth` (λ) | 0.333 | 0=fixed target size; 1=fixed shard count; 0.333=sstable size grows as cube-root of density |
| `min_sstable_size` | 100 MiB | Below this, shards drop below `base_shard_count` |
| `max_sstables_to_compact` | no limit | Option value ≤ 0 means `Integer.MAX_VALUE` (`Controller.java:202–203`) |
| `expired_sstable_check_frequency_seconds` | 600 | Same as TWCS default |

Maximum shard splitting is bounded at `base_shard_count × 2^20`
(`MAX_SHARD_SHIFT = 20`, `Controller.java:139`).

### Key Takeaways
- STCS favors write throughput; expect higher read amplification due to overlap.
- LCS minimizes read amplification by enforcing non-overlap above L0; compaction work increases.
- TWCS isolates data by time; works well with TTLs and time-bucketed queries.
- Tombstone purging depends on compaction merging overlapped data and `gc_grace_seconds`.
- Choose based on access patterns: point/slices → LCS, time-series/TTL → TWCS, bulk ingest → STCS.

## Tombstone Purging and Overlap

Tombstones are dropped when a compaction can prove they no longer shadow live data in the overlap set and are past `gc_grace_seconds` (or restricted by repair policy like `only_purge_repaired_tombstones`).

- STCS: Purging may be delayed if overlapping SSTables haven’t been merged recently.
- LCS: Non-overlap in L1+ improves reliability of purging; L0 backlogs can defer purges.
- TWCS: Purging is effective as windows age/close; cross-window scans still see overlap.

Overlap increases read IO and defers purging; reducing overlap (e.g., with LCS) helps both latency and space reclamation predictability.

### Dropping fully-expired SSTables whole

When an entire input SSTable is past `gc_grace_seconds`, a compaction can reclaim it without reading it. Cassandra's `CompactionController.getFullyExpiredSSTables` classifies an input as *fully expired* when its `StatsMetadata.maxLocalDeletionTime < gcBefore` (every cell/tombstone's local deletion time is below the cutoff), and drops it whole — excluded from the K-way merge and deleted after the output publishes — rather than reading, merging, and re-serializing its dead cells.

CQLite mirrors this (issue #1388):

- **Detection is metadata-only — the candidate's `Data.db` is never opened.** The decision reads a single authoritative field, `Statistics.db` `TimestampStatistics.max_deletion_time` (the `maxLocalDeletionTime`), and compares it against `gcBefore` (`is_fully_expired`, `cqlite-core/src/storage/write_engine/merge/fully_expired.rs:41`). That is the whole point of the optimization *and* what keeps it no-heuristics-compliant: a 10 GB fully-expired input costs one small metadata read instead of a full decode-merge-reserialize pass. The LIVE / `NO_DELETION_TIME` sentinel (surfaced by the parser as `i64::MAX`) is never `< gcBefore`, so an SSTable holding any live non-TTL cell — even alongside ancient tombstones — is never classified expired.
- **Overlap safety is a TWO-part bound.** A fully-expired SSTable is dropped only when its authoritative `max_timestamp` is **strictly less than** the minimum write timestamp across (a) every overlapping SSTable **outside** the compaction set and (b) every **non-expired input of this same compaction**. Part (b) is required for parity and for correctness: dropping an expired SSTable whose tombstone shadows an *older* live cell in a co-compacting input would resurrect that cell — the tombstone is gone, and the co-input's cell is merged into the output. A fully-expired co-input is *not* folded into the bound (its own data is past `gcBefore` and would be purged anyway). Cassandra's `CompactionController.getFullyExpiredSSTables` folds the same two sets (plus memtables) into a local `minTimestamp` bound (`CompactionController.java:183-204` at 5.0.8) and then drops only candidates with `getMaxTimestamp() < minTimestamp`. CQLite computes part (a) with `compute_max_purgeable_timestamp` (`merge/mod.rs:1808`, the coarse global-`min_timestamp` gate — key-range precision is deliberately deferred) and part (b) in `classify_drop_set` (`fully_expired.rs:200`).
- **Every unknown fails closed.** No `gcBefore` (absent/invalid `gc_grace_seconds`) ⇒ empty drop-set. A non-empty outside set whose bound cannot be computed (an unreadable outside `Statistics.db`) ⇒ empty drop-set. A candidate whose own `Statistics.db` is absent or unparseable ⇒ treated as non-expired, and because its `min_timestamp` is then unknown and cannot be folded into the bound, the whole drop-set is disabled. A candidate whose `max_timestamp` is the UNAVAILABLE sentinel cannot be proven to predate the bound and is retained. An empty outside set *and* no non-expired inputs (a major compaction of only-expired inputs) gives a `+inf` bound, so every candidate is droppable — with a degenerate guard that keeps one input so the merger still has a source.
- **Where the two compaction surfaces differ.** The engine compaction path passes a real outside set (`cqlite-core/src/storage/write_engine/compaction.rs:120`). The CLI one-shot has no knowledge of SSTables beyond its explicit input list, so it attempts the drop **only** when the operator asserts `--major`/`--purge-tombstones` (`purge_safe`), which is the assertion that the input set spans every overlapping SSTable (`merge/mod.rs:2295`). Without that flag, no drop occurs.
- The dropped SSTables are excluded from the merger's input list before the merge (so they are never read — the perf win) and reclaimed only after the merged output is atomically published, via the same component-delete path as the merged inputs. The compaction report records the dropped-whole set distinctly from the merged inputs.

For implementation details, see Appendix C.

### References

- Cassandra 5.0.8 (code):
  - `SizeTieredCompactionStrategy` — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/compaction/SizeTieredCompactionStrategy.java`
  - `LeveledCompactionStrategy` — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/compaction/LeveledCompactionStrategy.java`
  - `TimeWindowCompactionStrategy` — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/compaction/TimeWindowCompactionStrategy.java`
  - `UnifiedCompactionStrategy` (sidebar) — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/compaction/UnifiedCompactionStrategy.java`
  - `CompactionController` (tombstone purging) — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/compaction/CompactionController.java`
  
For implementation details, see Appendix C.


