# Design — sstable-diagnose (issue #4204)

## D0. Source read for this design

Read directly at `cassandra-5.0.8` (never a CQLite `file:line`, #3041):
- `src/java/org/apache/cassandra/tools/SSTableMetadataViewer.java` — the `sstablemetadata` tool:
  `printSStableMetadata` (cheap tier, no `-s`) and `printScannedOverview` (the `-s` scan tier: widest/
  largest/most-tombstoned partitions via `MinMaxPriorityQueue.maximumSize(5)`).
- `src/java/org/apache/cassandra/io/sstable/metadata/StatsMetadata.java` —
  `getEstimatedDroppableTombstoneRatio`/`getDroppableTombstonesBefore`.
- `src/java/org/apache/cassandra/utils/EstimatedHistogram.java` — bucket offsets, `mean()`,
  `count()`, `sum()`, `percentile()`.
- `src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java` — default histogram
  shapes (already ported into CQLite, `estimated_histogram.rs`, issue #1327).
- CQLite's own already-Cassandra-parity code, read as evidence of what exists to REUSE, not as
  format authority: `cqlite-core/src/storage/sstable/writer/stats_writer/estimated_histogram.rs`
  (histogram parity), `cqlite-core/src/storage/sstable/statistics_reader.rs` +
  `parser/statistics.rs::{SSTableStatistics, TimestampStatistics}` (the read side), and
  `cqlite-core/src/storage/write_engine/merge/fully_expired.rs::is_fully_expired` (the
  fully-expired-drop predicate, `MAX_deletion_time < gc_before`, already Cassandra-parity).

## D1. Two cost tiers, exactly `sstablemetadata`'s own split

```
cqlite diagnose <table-dir>
        │
        ▼
CHEAP (default) — per generation, Statistics.db + Index/Summary + CompressionInfo + TOC only:
   StatisticsReader::open  ──►  SSTableStatistics { timestamp_stats, row_stats.partition_count,
                                                     compression_stats, tombstone_drop_times, .. }
        │
        ├─ min/max timestamp, min/max LDT           (TimestampStatistics, direct fields)
        ├─ estimated partition count                (row_stats.partition_count — the existing
        │                                             read_table_counts authoritative decode)
        ├─ estimated droppable tombstones            (design D2 formula, ported from
        │                                             StatsMetadata.getEstimatedDroppableTombstoneRatio)
        ├─ repaired/pending-repair, compression ratio (direct fields)
        ├─ fully-expired-drop prediction              (write_engine::merge::fully_expired::is_fully_expired,
        │                                              reused as a READ-ONLY predicate — diagnose does not
        │                                              compact, it only asks "would this generation be
        │                                              dropped whole at gcBefore")
        └─ generation overlap per token range          (design D4)
        ▼
--deep — ONE additional full streaming scan per table (existing point-compaction/full-scan iterator,
          one partition resident at a time, same primitive `compact_sstables`'s producer thread uses):
        ├─ partition-size histogram, clustering-width histogram (independently bucketed by diagnose,
        │                                                        NOT read from Statistics.db —
        │                                                        `sstablemetadata -s` does not
        │                                                        recompute the STATS histogram either;
        │                                                        both tools' -s tier reports what the
        │                                                        scan itself found)
        ├─ top-N largest / widest partitions            (mirrors `largestPartitions`/`widestPartitions`)
        ├─ top-N tombstone-heaviest partitions           (mirrors `mostTombstones` — RAW per-generation
        │                                                 marker count, see D2; NOT #4200's reconciled
        │                                                 report)
        └─ full-compaction-at-`--now` reclaim prediction  (reuses the existing `MergeStats`/purge tally
                                                            CODE PATH in a dry, no-write invocation —
                                                            same numbers `compact --major` would produce,
                                                            computed without writing anything)
```

`--deep` NEVER writes. There is no `--out <dir>` write destination anywhere in this verb — `--out`
in the CLI names the RENDERING format (`text|json`), matching `verify`'s existing `--out` convention
(`Verify { path, mode, out: VerifyOutputArg }`), not `salvage`/`scrub`/`compact`'s `--out <dir>`
write-destination convention. This is a deliberate, stated divergence from the epic's other verbs
that this design calls out explicitly so it is never misread as "diagnose can write."

## D2. The tombstone-signal boundary with #4200, in exact source terms

`sstablemetadata -s`'s `mostTombstones` queue is filled by `printScannedOverview`'s scan loop:

```java
if (!partition.partitionLevelDeletion().isLive())  { tombstoneCount++; ptombcount++; }
...
case RANGE_TOMBSTONE_MARKER: tombstoneCount++; ptombcount++; break;
...
if (cell.isTombstone()) { tombstoneCount++; ptombcount++; }
...
mostTombstones.add(new ValuedByteBuffer(partition.partitionKey().getKey(), ptombcount));
```

This is a **flat count of tombstone markers seen in ONE generation's own physical scan** — no
`gcBefore` comparison, no cross-generation reconciliation, no kind label. `diagnose --deep`'s
tombstone-heaviest ranking reproduces exactly this shape, per generation, using the existing raw
per-partition scan already needed for the size/width histograms (D1) — it is the SAME scan pass,
one more counter per partition, not a second traversal.

#4200 `cqlite tombstones` (design already drafted on its branch) is a categorically different
computation: it drives `merge::trace::RecordingSink` through `build_single_partition_merger` across
EVERY generation with `purge_safe(true)`, producing a kind-classified
(`partition|range|row|cell|collection`), droppable-at-`now`-flagged, shadow-counted report. That
requires the full reconciliation machinery `diagnose` deliberately does not invoke. **The two numbers
for the same partition can legitimately differ**: `diagnose`'s count is "how many tombstone markers
does generation N physically contain," `tombstones`' count is "how many of this partition's
tombstones, across every generation, are live/droppable-at-now, after reconciliation." `diagnose`'s
report text says so plainly next to the field, and names `cqlite tombstones` as the authoritative
follow-up — it does not attempt to approximate #4200's number.

## D3. `getEstimatedDroppableTombstoneRatio`, ported exactly

```java
// StatsMetadata.java, cassandra-5.0.8
public double getEstimatedDroppableTombstoneRatio(long gcBefore) {
    long estimatedColumnCount = estimatedCellPerPartitionCount.mean() * estimatedCellPerPartitionCount.count();
    if (estimatedColumnCount > 0) {
        double droppable = estimatedTombstoneDropTime.sum(gcBefore);   // sum of bucket counts at/under gcBefore
        return droppable / estimatedColumnCount;
    }
    return 0.0f;
}
```

`estimatedCellPerPartitionCount` is CQLite's existing `estimated_histogram.rs`-ported
`estimatedCellPerPartitionCount` histogram (already Cassandra-parity, issue #1327 — 118 offsets/119
buckets, the exact geometric series). `estimatedTombstoneDropTime` is
`SSTableStatistics.tombstone_drop_times` (issue #1073's best-effort decode) — `sum(gcBefore)` is the
running total of bucket counts whose point is `<= gcBefore`. Ported 1:1 as a `pub(crate)` helper in
the new `diagnose` module (not duplicated inline), taking `gc_before_secs` from the SAME
`compute_gc_before(schema, now)` formula `compact`/`scrub`/`fully_expired` already use, so a
`diagnose` run and a `scrub --purge` run at the same `--now` agree on what "droppable" means.

**This is an ESTIMATE, by Cassandra's own admission** (the field is literally named "estimated" in
both the Java field and CQLite's port) — `sstablemetadata`'s own printed number is this same
estimate, not a scan-verified count. `diagnose` therefore reports it labeled `estimated`, and R2's
oracle is "matches `sstablemetadata`'s printed value for the same fixture and `gcBefore`," never
"matches an exact scan count" (that assertion belongs to #4200's `tombstones` report, or to R1's own
`--deep` raw count for a DIFFERENT, non-estimate number).

## D4. Generation overlap per token range

No existing CQLite primitive computes this (confirmed: `write_engine`'s existing "overlap" hits are
all about the #921/#935 overlapping-SSTable PURGE-SAFETY bound, a different question — "does an
outside SSTable's token range intersect this compaction's inputs," not "how many generations cover
each of K token buckets"). Built fresh, cheaply, from data every generation's reader already exposes
(`reader::types` `first_key`/`last_key`, i.e. each generation's own min/max partition token — no new
component read):

1. Take the UNION of every generation's `[first_token, last_token]` span for the table.
2. Partition that union into `K` equal-width token buckets (`K` is a fixed, documented default —
   e.g. 16 — not operator-configurable in this slice; a fixed `K` keeps the report shape stable for
   the JSON contract, R9-style, without a variable-length column set).
3. For each bucket, count how many generations' `[first_token, last_token]` spans intersect it.

This is metadata-only (no Data.db read), matches the cheap tier's cost budget, and answers the
operator question the epic's issue text asks for ("how many generations cover each of K token
buckets") directly — a high bucket count is exactly what un-compacted overlapping generations look
like, the same signal `nodetool tablestats`' "SSTables in each level"/compaction-strategy views give
online, reconstructed here from files alone.

## D5. Report schema (stable; text is a rendering of it)

```json
{ "table_dir": "<path>", "now": "<RFC3339>", "deep": false, "top_n": 5,
  "generations": [
    { "generation": 12, "format": "nb", "source": "statistics",
      "min_timestamp": 1700000000000000, "max_timestamp": {"value": 1700003600000000, "source": "statistics"},
      "min_local_deletion_time": 0, "max_local_deletion_time": 2147483647,
      "estimated_partition_count": {"value": 40231, "source": "index"},
      "estimated_droppable_tombstone_ratio": {"value": 0.0412, "source": "statistics", "gc_before": 1699913600},
      "repaired_at": 0, "pending_repair": null, "compression_ratio": {"value": 0.31, "source": "statistics"},
      "fully_expired_at_now": false,
      "deep": null },
  ],
  "token_overlap": { "buckets": 16, "counts": [1,1,2,2,3,3,2,2,1,1,1,1,1,1,1,1] },
  "now_used_for_gc_before": "<RFC3339>" }
```

When `--deep` runs, each generation's `deep` field is populated:

```json
"deep": {
  "partition_size_histogram": [[0,1024,412],[1024,4096,88], "..."],
  "clustering_width_histogram": [["..."]],
  "top_largest_partitions": [{"key_hex":"…","bytes":88192}, "..."],
  "top_tombstone_heaviest_partitions": [{"key_hex":"…","tombstones":412}, "..."],
  "reclaim_at_now": {"rows_in": 40231, "rows_out": 39998, "tombstones_purged": 233}
}
```

Every leaf value that can be absent (an older layout, a `None` from issue #1653's honest-`Option`
fields) is `{"value": null, "source": "unmeasured", "cause": "<why>"}` — never a bare `0` or `null`
with no `source`/`cause` (the #4159 class this doctrine names explicitly). Field order matches
`DiagnoseReport`'s serialization order (`cqlite-core/src/storage/sstable/diagnose/mod.rs`), verified
against the compiled binary's real output, not read off the struct in isolation (the #4196 C-audit
discipline, applied from the first PR here too).

## D6. Oracles (all Cassandra-written; never CQLite round-trip alone, #3042)

| Case | Fixture | Expected value derived by |
|---|---|---|
| cheap-tier fields (min/max timestamp, LDT, compression ratio, estimated droppable tombstones, repaired-at, pending-repair) | any committed `test_basic`/`test_tomb`/`test_compaction_tombstone_ttl` generation | `sstablemetadata` run against the SAME file at the pinned `cassandra-5.0.8` build, captured as a committed text oracle with the tool version recorded (issue AC 3) |
| `--deep` partition-size / clustering-width histograms, top-N rankings | `test_wide_rows`, `test_tomb`, `test_compaction_tombstone_ttl` | the test independently computes bucket membership / ranking directly from the Cassandra-written `*.jsonl` sstabledump goldens — never from `diagnose`'s own scan compared to itself |
| top-N tombstone-heaviest (raw count) | `test_tomb/*` | independently counted tombstone markers per partition from the JSONL golden, per generation — matching `sstablemetadata -s`'s definition (D2), not #4200's reconciled count |
| fully-expired-at-now prediction | a generation whose `max_deletion_time` is provably below a chosen `gcBefore` | re-derives `is_fully_expired`'s own formula independently in the test (same as `fully_expired.rs`'s existing test suite already does) |
| token overlap bucket counts | any multi-generation table with known, distinct token spans | the test computes bucket intersection independently from each generation's committed first/last key |

## D7. CLI shape and campsite

`cqlite-cli/src/commands/diagnose.rs` (new file; read-only, no destructive-path-argument surface at
all — no `write_guard` reuse needed, since this verb never opens anything for writing). `Commands::
Diagnose(DiagnoseArgs)` in `cli_types.rs`, `--out` reusing the `VerifyOutputArg`-shaped `text|json`
enum (NOT `SalvageOutFormatArg`'s naming, to avoid implying a write-destination meaning it does not
have — see D1). Exit codes: `0` always, unless usage error (`1`) — there is no failure/refusal mode
for a read-only report (`--deep` on a corrupted generation reports what it can and names what it
couldn't with `source: unmeasured`, rather than refusing the whole run; a genuinely unreadable
generation is itself one more field in the report, not a process exit code).
