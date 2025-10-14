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
TWCS places SSTables into time windows (e.g., 1 hour or 1 day) and compacts only within each window. This isolates older immutable data from newer hot data and is well-suited to time-series and TTL-heavy workloads. It eases tombstone purging when windows close, while accepting overlap across windows for large time-range scans.

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

Implementation touchpoints (Cassandra 5.0.0): `CompactionManager`, `CompactionController`, strategy classes listed below.

### Sidebar: UCS (Unified Compaction Strategy)

UCS generalizes compaction with scaling presets to emulate tiered (Tn) or leveled (Ln) behavior while adding shard-based concurrency and configurable target sizes. For migration, you can set scaling parameters to approximate STCS (`T4`) or LCS (`L10`) while maintaining a single strategy across tables. See also the Cassandra 5.0 code for `UnifiedCompactionStrategy` and the operating guide for typical presets.

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

For implementation details, see Appendix C.

### References

- Cassandra 5.0.0 (code):
  - `SizeTieredCompactionStrategy` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/compaction/SizeTieredCompactionStrategy.java`
  - `LeveledCompactionStrategy` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/compaction/LeveledCompactionStrategy.java`
  - `TimeWindowCompactionStrategy` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/compaction/TimeWindowCompactionStrategy.java`
  - `UnifiedCompactionStrategy` (sidebar) — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/compaction/UnifiedCompactionStrategy.java`
  - `CompactionController` (tombstone purging) — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/compaction/CompactionController.java`
  
For implementation details, see Appendix C.


