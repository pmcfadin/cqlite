## Statistics.db

`Statistics.db` captures table-level metadata such as histograms, min/max timestamps, repair/level flags, compression ratios, and counts that inform compaction and read heuristics.

Because it is the fallback source of truth for types and encoding baselines when no schema
is supplied (no-heuristics mandate, issue #28), `Statistics.db` is **authoritative
metadata, not a hint** — which is why a present-but-unparseable file is a hard error rather
than a degraded read (see *Corruption handling*).

### In this chapter you will learn
- What `StatsMetadata` contains and how it is used
- How min/max aggregates encode "absent" and the live-cell deletion-time sentinel
- The real serialized layout (TOC + four checksummed components) and why `0x26291b05` is
  not a magic number
- How statistics are collected during flush, and the fail-closed corruption posture
- How stats influence compaction and read behavior

## Stats Overview

Trimmed excerpt from `test_basic/simple_table`:

```text
Bloom Filter FP chance: 0.01
Minimum timestamp: 2025-09-16 22:14:23
Maximum timestamp: 2025-09-16 22:14:24
Compressor: org.apache.cassandra.io.compress.SnappyCompressor
Compression ratio: 0.976...
SSTable Level: 0
totalRows: 1000
```

### File Structure and Key Fields

`Statistics.db` serializes `StatsMetadata` alongside related metadata blocks. Important fields (names align with Cassandra classes where applicable):

- Timestamps and Deletions:
  - **min_timestamp / max_timestamp**: microsecond epoch range of writes. Both are `long`
    fields ([`StatsMetadata.java:65-66`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/StatsMetadata.java))
    written **adjacently as two big-endian int64s** in the STATS component, after
    `estimatedCellPerPartitionCount` and the partition-size histogram
    ([`StatsMetadata.java:407-408`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/StatsMetadata.java)
    -- `out.writeLong(minTimestamp); out.writeLong(maxTimestamp);`). There is no
    "absent" flag: see *Absent min/max values* below for how Cassandra encodes
    "nothing was recorded".
  - **min/max local deletion time**: lower/upper bounds for tombstone local deletion time,
    also `long` fields ([`StatsMetadata.java:67-68`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/StatsMetadata.java)),
    but with a **version-dependent wire width** -- see *Local deletion time and the live-cell
    sentinel* below.
- Bloom and Compression:
  - **bloom filter fp chance**: build-time target false-positive rate used when constructing `Filter.db`. Runtime observed FPR may diverge as the filter saturates or key distribution shifts; validate empirically and rebuild if drift is unacceptable.
  - **compressor / compression ratio**: algorithm and computed ratio for `Data.db`
- Cardinality and Sizes:
  - **estimated cardinality**: approximate partition count
  - **estimated partition size histogram**: size distribution (bytes) with percentiles
  - **estimated column count histogram**: columns per partition distribution with percentiles
- Topology and Repair:
  - **level**: LCS level (0 for STCS/TWCS)
  - **repaired at / pending repair / originating host id**: repair metadata
  - **covered commit log positions**: replay coverage

These fields drive compaction policies (e.g., tombstone purging thresholds), read heuristics (e.g., read-ahead sizing), and operational insights (e.g., skew from partition histograms).

### Absent min/max values (no "unset" flag on the wire)

`min_timestamp` / `max_timestamp` occupy a fixed 8 bytes each, so an SSTable in which
*nothing* was tracked still writes two int64s. Cassandra's `MetadataCollector` resolves this
with per-tracker defaults rather than a presence flag:

- `timestampTracker = new MinMaxLongTracker()`
  ([`MetadataCollector.java:111`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java)),
  whose no-arg constructor is `this(Long.MIN_VALUE, Long.MAX_VALUE)`
  ([`MetadataCollector.java:411-413`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java)).
- While `isSet == false`, `min()` returns the *defaultMin* and `max()` returns the
  *defaultMax*
  ([`MetadataCollector.java:438-446`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java)).

So for an SSTable with no recorded timestamps the serialized pair is
**`minTimestamp = Long.MIN_VALUE`, `maxTimestamp = Long.MAX_VALUE`** — the *widest possible*
interval, which is the safe (never-exclude) answer for a min/max pruning filter. The same
values are what `MetadataCollector.defaultStatsMetadata()` hands out
([`MetadataCollector.java:80-84`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java)).
Note the asymmetry: `Long.MIN_VALUE` is the unset **min** default and `Long.MAX_VALUE` is
the unset **max** default; neither is a general-purpose "absent" marker, and a real
recorded value can legitimately be negative (Cassandra permits negative write timestamps).

CQLite models "absent" explicitly rather than propagating a sentinel into query
predicates: the parsed `max_timestamp` is an `Option<i64>` and is left `None` when it
cannot be read (`cqlite-core/src/parser/enhanced_statistics_parser/mod.rs:249-276`,
issues #1729/#1653), and `decode_max_timestamp` maps the reserved value it treats as
"unset" to `None` (`cqlite-core/src/parser/repair_metadata.rs:1239-1261`). Consumers must
therefore handle `None` as "unknown — do not prune", not as "zero".

On the write side CQLite's collector seeds `min_timestamp = i64::MAX` /
`max_timestamp = i64::MIN` (`cqlite-core/src/storage/sstable/writer/stats_writer/metadata.rs:199-204`)
and normalizes any still-unset sentinel to `0` in `finalize()`
(`.../metadata.rs:397-418`). That is a **CQLite implementation choice and a deliberate
divergence from Cassandra**, which emits the widening defaults above; it is only reachable
for an SSTable in which no cell timestamp was ever folded.

### Local deletion time and the live-cell sentinel

`min/max_local_deletion_time` are `long` in memory but their **serialized width and
sentinel differ by SSTable version**:

| Version | Wire form | "no deletion / live" sentinel on disk |
|---|---|---|
| `oa`/`da` and later (`hasUIntDeletionTime()`) | int32 holding an **unsigned** seconds value | `0xFFFFFFFF` (`Cell.NO_DELETION_TIME_UNSIGNED_INTEGER`) |
| `na`/`nb` (legacy path) | signed int32, clamped to `Integer.MAX_VALUE - 1` | `Integer.MAX_VALUE` |

Authority: `hasUIntDeletionTime()` is `version >= "oa"`
([`BigFormat.java:409`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java));
the two branches are
[`StatsMetadata.java:409-420`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/StatsMetadata.java)
(`Cell.deletionTimeLongToUnsignedInteger` for `oa`+, else the
`== Long.MAX_VALUE ? Integer.MAX_VALUE : min(v, Integer.MAX_VALUE - 1)` clamp); the read
side reverses it and remaps a legacy `Integer.MAX_VALUE` back to `Cell.NO_DELETION_TIME`
([`StatsMetadata.java:551-568`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/StatsMetadata.java)).

**The in-memory sentinel is not `Integer.MAX_VALUE`.** `Cell.NO_DELETION_TIME` is
`Long.MAX_VALUE`; the related-but-distinct constants are all defined together at
[`Cell.java:48-57`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/rows/Cell.java):

- `NO_TTL = 0`
- `NO_DELETION_TIME = Long.MAX_VALUE` — "this cell is not deleted"
- `NO_DELETION_TIME_UNSIGNED_INTEGER = CassandraUInt.MAX_VALUE_UINT` (`0xFFFFFFFF`) — the
  `oa`+ on-disk spelling of the same fact
- `MAX_DELETION_TIME = CassandraUInt.MAX_VALUE_LONG - 2` — largest *real* deletion time
- `INVALID_DELETION_TIME = CassandraUInt.MAX_VALUE_LONG - 1`
- `MAX_DELETION_TIME_2038_LEGACY_CAP = Integer.MAX_VALUE - 1` — the pre-`oa` clamp ceiling

Conflating these is a real hazard: `Long.MAX_VALUE` (memory), `0xFFFFFFFF` (`oa`+ disk) and
`Integer.MAX_VALUE` (`nb` disk) are three spellings of "live", while
`Integer.MAX_VALUE - 1` is a *legitimate* deletion time under the legacy clamp.
Independently, `DeletionTime.LIVE` is `(Long.MIN_VALUE, Long.MAX_VALUE)`
([`DeletionTime.java:46`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/DeletionTime.java))
— a live *range/partition* deletion, not a cell.

**Why `max_local_deletion_time` reads as "live" in mixed SSTables.** The tracker is seeded
with the sentinel on *both* ends —
`localDeletionTimeTracker = new MinMaxLongTracker(Cell.NO_DELETION_TIME, Cell.NO_DELETION_TIME)`
([`MetadataCollector.java:112`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java))
— and **every cell** is folded in, live ones included, because `update(Cell)` passes
`cell.localDeletionTime()` unconditionally
([`MetadataCollector.java:229-236`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java)).
A live, non-expiring cell contributes `NO_DELETION_TIME`, i.e. `Long.MAX_VALUE`, so an
SSTable containing *any* live non-TTL cell finalizes with `max_local_deletion_time` at the
sentinel regardless of how many tombstones it also holds. The tombstone-drop histogram is
kept clean by the explicit exclusion at
[`MetadataCollector.java:267-271`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java)
(`if (newLocalDeletionTime != Cell.NO_DELETION_TIME) estimatedTombstoneDropTime.update(...)`).
Consequence for readers: `max_local_deletion_time` is **not** a usable "latest tombstone"
bound — treat the sentinel as "contains live data", and use
`estimatedTombstoneDropTime` for tombstone scheduling.

CQLite matches this behavior (issue #1728): a live write folds
`note_live_local_deletion_time()`, which raises `max_local_deletion_time` to the sentinel
(`cqlite-core/src/storage/sstable/writer/stats_writer/metadata.rs:286-292`, called from
`cqlite-core/src/storage/sstable/writer/stats_fold.rs:95-157` for live simple cells and
live complex elements). TTL'd rows deliberately do **not** fold the live sentinel, since an
expiring cell has a real local deletion time. Both behaviors are pinned by
`test_live_write_plus_tombstone_max_ldt_is_live_sentinel` and
`test_ttl_row_write_does_not_fold_live_sentinel`
(`cqlite-core/src/storage/sstable/writer/mod.rs:2115-2219`).

## Collection and Serialization

Statistics are collected during flush and serialized alongside component files. Readers parse `Statistics.db` to provide summaries and drive decisions (e.g., compaction tuning, bloom FPR reporting).

Pinpoints in Cassandra 5.0.8:
- `MetadataCollector` gathers live stats during flush (row counts, histograms)
- `MetadataSerializer` writes and reads the metadata blocks
- `StatsMetadata` exposes typed accessors for the above

For an implementation walkthrough of parsing and reporting helpers, see Appendix C.

### Serialized layout of the file (real TOC, all four components)

Every Cassandra 5.0 `Statistics.db` is written by `MetadataSerializer.serialize`
([`MetadataSerializer.java:67-112`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataSerializer.java)):

```text
[i32 BE] component count                       -- 4 in 5.0
[i32 BE] CRC32(count bytes)                    -- only when version.hasMetadataChecksum()
4 × ( [i32 BE] MetadataType ordinal, [i32 BE] absolute offset of that component )
[i32 BE] CRC32(all TOC entry bytes)            -- cumulative, same gate
per component, in ordinal order:
  [ component body ]
  [i32 BE] CRC32(body)                         -- same gate
```

`MetadataType` ordinals are `VALIDATION 0, COMPACTION 1, STATS 2, HEADER 3`
([`MetadataType.java:28-34`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataType.java)).
`hasMetadataChecksum()` is `version >= "na"`
([`BigFormat.java:404`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java)),
so **every version in this guide's scope (`na`, `nb`, `oa`, `da`) carries the checksums**.
`CHECKSUM_LENGTH` is 4 ([`MetadataSerializer.java:65`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataSerializer.java));
each CRC is fed big-endian via `FBUtilities.updateChecksumInt`
([`FBUtilities.java:1117-1123`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/utils/FBUtilities.java)).

Because the count is always 4 and the checksum immediately follows it, the second word of
a 5.0 `Statistics.db` is *always* the constant `CRC32(0x00000004) = 0x26291b05`. It is a
derived checksum, **not** a magic number — see *The `0x26291b05` "magic number"* below.

Cassandra validates each of those CRCs on read and throws
`CorruptSSTableException("Checksums do not match for " + file)` on mismatch
([`MetadataSerializer.java:214-226`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataSerializer.java)).

### Corruption handling: fail-closed, with one stated gap (issue #1626)

`Statistics.db` is **authoritative metadata**, not a hint: under the no-heuristics mandate
(issue #28) it is the fallback source of truth for types and encoding baselines when no
schema is supplied. A silently-degraded parse would therefore corrupt decoding downstream,
so CQLite's posture is:

| Situation | Behavior |
|---|---|
| `Statistics.db` **absent** | `Ok(None)` — the reader continues without it |
| `Statistics.db` **present but unparseable** | **hard error out of `SSTableReader::open()`** |
| Filename version below the 5.0 floor | `Error::UnsupportedVersion` **before any file read** |

`load_statistics_reader` returns `Result<Option<StatisticsReader>>` and maps *only*
`Error::NotFound` to `Ok(None)`; a corruption error is re-wrapped with the offending
component path and every other error is propagated unchanged
(`cqlite-core/src/storage/sstable/reader/component_loading.rs:249-300`). The call site
uses `?`, so the failure surfaces from `open()` rather than yielding a reader with
zero-valued stats (`cqlite-core/src/storage/sstable/reader/mod.rs:792-794`). Version gating
runs from the filename before any filesystem access (issue #1249), so a below-floor SSTable
is rejected without being read
(`cqlite-core/src/storage/sstable/statistics_reader.rs:46-127`).

**Stated precondition — what "unparseable" covers.** This guarantee is about *structural*
parse failure: truncated or nonsensical bytes that the parser cannot decode. CQLite does
**not yet verify the per-component CRC32s described above; that validation is explicitly
deferred** (`cqlite-core/src/storage/sstable/statistics_reader.rs:105-120`), so a
`Statistics.db` whose bytes are corrupt *but still structurally decodable* can be accepted
where Cassandra would raise `CorruptSSTableException`. Do not read the table above as
"any corrupt `Statistics.db` is detected".

Where a specific field is unreadable, CQLite fails closed on that field rather than
inventing a value: the parsed `max_timestamp` stays `None` and `max_deletion_time` defaults
to `i64::MAX` (the widest, never-prune answer) in
`cqlite-core/src/parser/enhanced_statistics_parser/mod.rs:249-276`.

### The `0x26291b05` "magic number" and the single TOC walk (issue #2148)

`0x26291b05` is frequently described as a `Statistics.db` magic number or a
`statistics_kind` tag. It is neither: it is exactly `CRC32` over the four big-endian bytes
of the component count `4`, emitted by the `maybeWriteChecksum` call that follows
`out.writeInt(components.size())`
([`MetadataSerializer.java:76-78`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataSerializer.java)).
It is stable only because 5.0 always writes exactly four components; a file with a
different component count would carry a different word there, so **it must not be used as a
format-identification magic value**.

Confirmed on a real fixture — the first 40 bytes of
`test_basic/composite_key_table.../nb-1-big-Statistics.db`:

```text
00000000  00 00 00 04   26 29 1b 05        count = 4, CRC32(count) = 0x26291b05
00000008  00 00 00 00  00 00 00 2c        VALIDATION  (ordinal 0) @ 0x2c
00000010  00 00 00 01  00 00 00 65        COMPACTION  (ordinal 1) @ 0x65
00000018  00 00 00 02  00 00 01 a1        STATS       (ordinal 2) @ 0x1a1
00000020  00 00 00 03  00 00 13 99        HEADER      (ordinal 3) @ 0x1399
00000028  1d a8 06 ed                     CRC32 over the TOC entries
```

CQLite parses this TOC **once per file** and threads the result to every consumer. The walk
is `repair_metadata::parse_statistics_toc` /`walk_statistics_toc`
(`cqlite-core/src/parser/repair_metadata.rs:203-400`); the single call site is
`cqlite-core/src/parser/enhanced_statistics_parser/mod.rs:335`, whose `toc` is passed to
both the header/STATS consumers and the STATS-extras post-pass
(`.../mod.rs:324-427`). Before issue #2148 the same TOC was re-walked once per consumer
(three walks per file); the header reader now documents that the walk has moved out
(`cqlite-core/src/parser/enhanced_statistics_parser/header.rs:64-69`). Bounds rules the
walk enforces: offsets must lie inside the file, the last STATS entry wins, the first
HEADER entry wins, and a component's end is the next offset minus the 4-byte trailing CRC
(`cqlite-core/src/parser/repair_metadata.rs:290-400`).

## Operational Implications

- Compaction strategies consider levels, droppable tombstones, and partition histograms.
- Read path can report expected Bloom FPR and compression effectiveness.

#### Performance and Capacity Planning
- Use the partition-size histogram percentiles (P50/P95/P99) to set read-ahead and block cache sizing.
- Droppable tombstone estimates help target compaction to reclaim space.
- Compression ratio trends indicate if chunk sizes or algorithms need tuning (see Ch. 9).

#### Troubleshooting Pointers
- Unexpectedly high `bloom fp chance` often indicates a mis-sized Bloom at write time; verify `bloom_filter_fp_chance` and key cardinality.
- Large gap between min/max timestamp suggests hot + cold data mixing; check compaction strategy alignment (Ch. 15).
- Level > 0 with STCS may indicate previous LCS usage or tooling inconsistencies; confirm table options.

### Key Takeaways
- `Statistics.db` exposes key health and distribution signals for the table.
- Min/max timestamps and histograms drive maintenance and expectations on reads.
- Compression info and bloom FPR here help explain IO and false positives.

### Example Walkthrough (trimmed → interpretation)
- The sample shows P50 partition size ≈ 770 B and totalRows=1000, implying light rows and low IO per partition, favoring read-ahead windows at or near one chunk (see Ch. 9).
- Compression ratio ≈ 0.98 suggests low compressibility (random-looking bytes in `description`), so prioritize CPU over disk savings.

## SerializationHeader Component

Statistics.db in Cassandra 5.0 (nb-format) also contains an embedded **SerializationHeader** component that defines the table schema used when writing the SSTable. This is critical for correctly deserializing Data.db content.

### Binary Format

The SerializationHeader follows this structure (from `SerializationHeader.java`):

```
[UnsignedVInt minTimestamp_delta]              -- 64-bit delta from TIMESTAMP_EPOCH (µs)
[UnsignedVInt32 minLocalDeletionTime_delta]    -- 32-bit delta from DELETION_TIME_EPOCH (s)
[UnsignedVInt32 minTTL_delta]                  -- 32-bit delta from TTL_EPOCH (0)
                                               -- (EncodingStats block: 3–14 bytes total)
[VInt pk_type_len] [pk_type_string]            -- partition key type
[UnsignedVInt32 ck_count]                      -- clustering key count
  for each clustering key:
    [VInt ck_len] [ck_type_string]             -- clustering key type
[UnsignedVInt32 static_count]                  -- static column count (0 if none)
  for each static column:
    [VInt name_len] [name] [VInt type_len] [type]
[UnsignedVInt32 reg_count]                     -- regular column count
  for each regular column:
    [VInt name_len] [name] [VInt type_len] [type]
```

**Key insight**: When `static_count = 0`, the VInt encodes as `0x00`. This can appear to be a separator, but it is actually the static column count. Tables with static columns will have `static_count > 0` and include the static column definitions between clustering keys and regular columns.

### Example: Table with Static Columns

For `static_columns_table` with schema:
- Partition key: `id` (uuid)
- Clustering key: `event_time` (timestamp)
- Static column: `static_data` (text)
- Regular columns: `row_data` (text), `row_value` (int)

The SerializationHeader contains:
```
pk_type: org.apache.cassandra.db.marshal.UUIDType
ck_count: 1
ck_types: [org.apache.cassandra.db.marshal.TimestampType]
static_count: 1
static_columns: [{name: "static_data", type: "UTF8Type"}]
reg_count: 2
regular_columns: [{name: "row_data", type: "UTF8Type"}, {name: "row_value", type: "Int32Type"}]
```

### References
- Cassandra 5.0.8:
  - [`StatsMetadata`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/StatsMetadata.java)
  - [`MetadataCollector`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java)
  - [`MetadataSerializer`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataSerializer.java)
  - [`SerializationHeader`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/SerializationHeader.java)
  - [`EncodingStats`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/rows/EncodingStats.java)

For implementation details, see Appendix C.

## Writing Statistics.db

CQLite's writer emits the **real Cassandra TOC layout described above** — four components
(VALIDATION, COMPACTION, STATS, SERIALIZATION_HEADER) with the count checksum, the
cumulative TOC checksum, and a trailing CRC32 per component
(`cqlite-core/src/storage/sstable/writer/stats_writer/mod.rs:142-249`;
`NUM_COMPONENTS = 4` and the `METADATA_TYPE_*` ordinals 0..3 are at `.../mod.rs:78-84`).
The CRC feed mirrors `FBUtilities.updateChecksumInt` (big-endian words, `.../mod.rs:252-274`).

> **Correction notice:** earlier revisions of this chapter described a CQLite "hybrid
> format" — a 32-byte header doubling as a *fake* TOC, with `0x26291b05` as a
> "Statistics magic number" and only an EncodingStats block after byte 32. That
> description no longer matches the writer (and never matched Cassandra): the TOC is real,
> the four components are all present, and `0x26291b05` is `CRC32` of the component count
> (see above). The related "Full Cassandra TOC Structure (Not Implemented)" claim has been
> removed for the same reason.

The STATS body has two builders in the writer:
`build_stats_component` (the legacy / `hasUIntDeletionTime() == false` layout) and
`build_stats_component_da` (the BtiFormat layout, which adds the covered-clustering `Slice`,
**unsigned** deletion times, key range and token-space coverage) —
`cqlite-core/src/storage/sstable/writer/stats_writer/components.rs:99-320`.

**Which builder runs, precisely.** The dispatch is on the writer's `bti: bool` flag, not on a
version string: `if self.bti { build_stats_component_da } else { build_stats_component }`
(`.../stats_writer/mod.rs:150-157`). That flag is `false` for `StatisticsWriter::new` and
`true` for `new_bti` (`.../mod.rs:107-122`). The two sentinels each builder emits are:

| Builder | Unset local-deletion-time written | Site |
|---|---|---|
| `build_stats_component` (non-BTI) | `Integer.MAX_VALUE` (`i32::MAX`) | `.../components.rs:120-134` |
| `build_stats_component_da` (BTI) | `0xFFFFFFFF` | `.../components.rs:251-262` |

**Reconciliation with the version rule (no `oa` case exists on CQLite's write path).** Per
*Local deletion time and the live-cell sentinel*, `Integer.MAX_VALUE` is the **`na`/`nb`**
disk sentinel and `0xFFFFFFFF` is the **`oa`/`da`** one. CQLite's writer emits exactly two
descriptors — `SSTableFormat::Big → "nb"` and `SSTableFormat::Bti → "da"`
(`cqlite-core/src/storage/sstable/writer/finish.rs:381-384`) — so the mapping above is
correct *for everything CQLite actually writes*: `nb` gets `Integer.MAX_VALUE`, `da` gets
`0xFFFFFFFF`. **`oa` is never produced by the writer at all**, so there is no "`oa` through
the legacy builder" behavior to describe.

> **Correction notice:** an earlier revision of this section described the legacy builder as
> covering "BIG (`nb`/`oa`)" while simultaneously claiming it "follows the version rule".
> Those two statements contradict each other — `oa` has `hasUIntDeletionTime() == true`
> ([`BigFormat.java:409`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java)),
> so an `oa` STATS body routed through the legacy `Integer.MAX_VALUE` builder would be
> **wrong**, not version-following. The resolution is that CQLite's writer has no `oa` output
> at all; the `nb`/`oa` grouping was simply inaccurate.

> **CQLite implementation note (a latent hazard, not current behavior).** Because the
> builder is selected by a format *boolean* rather than by a version gate
> (`BigVersionGates::has_uint_deletion_time`), the correct sentinel is only a side effect of
> `SSTableFormat::Big` hard-coding `"nb"`. If a BIG `oa` write path were ever added without
> re-gating this dispatch, `oa` would silently receive the `na`/`nb` sentinel. Two stale
> comments still assert exactly that incorrect outcome — `.../stats_writer/mod.rs:151-153`
> ("BIG (`nb`/`oa`) emits the legacy layout") and the `StatisticsWriter::new` doc at
> `.../stats_writer/mod.rs:111` ("the legacy `nb`/`oa` BIG layout").

**Read side:** `oa` here is a **BIG** version; it is not a BTI version. The only BTI version
CQLite reads is `da` — `BtiVersionGates::from_version` returns
`Error::UnsupportedVersion` for anything else
(`cqlite-core/src/storage/sstable/version_gate/bti.rs:55-61`), so nothing in this section
should be read as implying an `oa` BTI SSTable is readable. On the BIG side the supported
allowlist is exactly `{na, nb, oa}`
(`cqlite-core/src/storage/sstable/version_gate/big.rs:99-138`), and `oa` reads use the
unsigned-deletion-time branch via `has_uint_deletion_time`.

### Delta Encoding Baselines

The primary purpose of Statistics.db is to provide **baseline values for delta encoding** in Data.db. Three critical fields establish these baselines:

#### min_timestamp
- **Purpose**: Baseline for timestamp delta encoding
- **Unit**: Microseconds since Unix epoch
- **Encoding**: `writeUnsignedVInt` (64-bit unsigned VInt, up to 8 bytes; delta from
  `TIMESTAMP_EPOCH` = Sept 22 2015 in µs). **Not** ZigZag/signed.
- **Usage**: Data.db encodes cell timestamps as deltas from this value

#### min_local_deletion_time
- **Purpose**: Baseline for tombstone deletion time encoding
- **Unit**: Seconds since Unix epoch
- **Encoding**: `writeUnsignedVInt32` (32-bit unsigned VInt, up to 5 bytes; delta from
  `DELETION_TIME_EPOCH` = Sept 22 2015 in seconds). **Not** ZigZag/signed.
- **Usage**: Tombstone deletion times in Data.db are encoded as deltas from this value

#### min_ttl
- **Purpose**: Baseline for TTL delta encoding
- **Unit**: Seconds
- **Encoding**: `writeUnsignedVInt32` (32-bit unsigned VInt, up to 5 bytes; delta from
  `TTL_EPOCH` = 0). **Not** ZigZag/signed.
- **Usage**: Cell TTL values in Data.db are encoded as deltas from this value
- **Special case**: If no TTL is used in the SSTable, this value is set to 0

**Critical ordering**: Statistics.db MUST be written BEFORE Data.db, as the Data.db writer requires these baseline values to encode timestamps, deletion times, and TTL values correctly.

### EncodingStats Serialization

The three baselines live at the **very start of the HEADER component** (`MetadataType`
ordinal 3), not at a fixed file offset: `SerializationHeader.Serializer.serialize` writes
`EncodingStats` first, then the key type, the clustering-type list, the static columns and
the regular columns
([`SerializationHeader.java:451-460`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/SerializationHeader.java)).
Locate it through the TOC, never by counting bytes from the file start.

The EncodingStats block itself is exactly three unsigned VInts, in this order:

1. `min_timestamp` delta: `writeUnsignedVInt` (64-bit, up to 8 bytes)
2. `min_local_deletion_time` delta: `writeUnsignedVInt32` (32-bit, up to 5 bytes)
3. `min_ttl` delta: `writeUnsignedVInt32` (32-bit, up to 5 bytes)

> **Correction notice:** an earlier revision of this section described the EncodingStats
> block as beginning at byte 32 with a `u32 BE = 3` "metadata type marker", a length
> placeholder, a `VUInt`-prefixed partitioner string and two `VUInt` placeholders "observed
> in real files". Those bytes belong to other structures: the `3` is the HEADER **TOC
> entry's** type ordinal (in the TOC, not in the component body), and the 43-byte
> `org.apache.cassandra.dht.Murmur3Partitioner` string is the **VALIDATION** component,
> written with Java `writeUTF` (2-byte big-endian length `0x002b`, then the bytes) and
> followed by an f64 `bloomFilterFPChance`
> ([`ValidationMetadata.java:79-83`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/ValidationMetadata.java)).
> The COMPACTION component in between is a length-prefixed HyperLogLogPlus cardinality
> estimator
> ([`CompactionMetadata.java:83-86`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/CompactionMetadata.java)).
> No field in this file is a "placeholder of unclear purpose".

**VInt encoding**: All baseline values use **unsigned** VInt encoding. Deltas are always
non-negative (subtracted from their respective epochs). ZigZag (signed) encoding is NOT
used. Using a 32-bit VInt for `minTimestamp` will corrupt timestamps after 2037. See
[`EncodingStats.java:272–276`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/rows/EncodingStats.java#L272) and Appendix B for VInt encoding details.

### Statistics Metadata Collection

During memtable flush, the writer tracks the following metadata (subset shown):

```rust
pub struct StatisticsMetadata {
    pub min_timestamp: i64,           // Microseconds
    pub max_timestamp: i64,           // Microseconds
    pub min_local_deletion_time: i32, // Seconds
    pub max_local_deletion_time: i32, // Seconds
    pub min_ttl: i32,                 // Seconds
    pub max_ttl: i32,                 // Seconds
    pub partition_count: u64,
    pub row_count: u64,
    pub column_count: u64,
    pub total_rows_size: u64,
    pub tombstone_histogram: TombstoneHistogram, // estimatedTombstoneDropTime
}
```

(Note the CQLite-side width divergence: Cassandra's `minLocalDeletionTime` /
`maxLocalDeletionTime` are `long` in memory, while CQLite tracks them as `i32` and widens at
serialization time.)

**Update methods** (`cqlite-core/src/storage/sstable/writer/stats_writer/metadata.rs`):
- `update_timestamp(i64)` (`:240-246`): tracks the min/max timestamp range, skipping the
  live/`NO_DELETION` markers `i64::MIN` and `i64::MAX` so they cannot poison the range
- `update_local_deletion_time(i32)` (`:257-264`): tracks the **tombstone** deletion-time
  range **and** accumulates into the `estimatedTombstoneDropTime` histogram (see below);
  a live marker (`i32::MAX`) is rejected here (issue #851)
- `note_live_local_deletion_time()` (`:286-292`): raises **`max_local_deletion_time` alone**
  to the live sentinel for a live non-TTL cell — the mixed-SSTable behavior Cassandra gets
  from folding every cell's `localDeletionTime` (issue #1728). It deliberately does not
  touch the min or the histogram.
- `update_ttl(i32)` (`:310-315`): tracks the TTL range, ignoring 0 (`NO_TTL`)
- `increment_partition_count()` / `increment_row_count()`: counts (rows = live + tombstones)

**Finalization** (`.../metadata.rs:397-418`): the *unset* sentinels are normalized to `0` —
`min_timestamp == i64::MAX`, `max_timestamp == i64::MIN`,
`min_local_deletion_time == i32::MAX`, `max_local_deletion_time == i32::MIN`, and
`min_ttl == i32::MAX` each become `0`. This affects only aggregates that were never
updated, so a real recorded value is never rewritten. Two consequences worth stating:

- A `0` local-deletion-time aggregate reaching the STATS builder is re-encoded as the
  *no-deletions* sentinel for the target version — `i32::MAX` for `nb`
  (`cqlite-core/src/storage/sstable/writer/stats_writer/components.rs:120-134`) and
  `0xFFFFFFFF` for `da` (`.../components.rs:197-320`) — so the round-trip stays
  Cassandra-compatible.
- `max_local_deletion_time == i32::MAX` from `note_live_local_deletion_time()` is **not**
  the `i32::MIN` unset case, so it survives `finalize()` and serializes as the live
  sentinel — which is the point of issue #1728.

Timestamps are the divergence noted earlier: Cassandra would emit the widening
`Long.MIN_VALUE` / `Long.MAX_VALUE` defaults for a fully-unrecorded SSTable, CQLite emits
`0`/`0`.

### Implementation Reference

For the complete write-side implementation, see:
- `cqlite-core/src/storage/sstable/writer/stats_writer/` (module; `mod.rs` writes the TOC +
  components, `metadata.rs` holds the collector, `components.rs` builds the per-component
  bodies): Statistics.db writer
- `cqlite-core/src/storage/sstable/writer/data_writer/`: Data.db writer that uses these baselines
- `cqlite-core/src/storage/sstable/writer/stats_fold.rs`: the per-cell/per-mutation folds
  that drive the collector

### estimatedTombstoneDropTime histogram (STATS component)

The STATS component carries `estimatedTombstoneDropTime`, the histogram of tombstone
local-deletion-times that Cassandra uses to compute `estimatedDroppableTombstoneRatio`
and schedule tombstone compaction. CQLite emits this histogram from its own writer
(`cqlite-core/src/storage/sstable/writer/stats_writer/components.rs:145-149`, with the
builder in `.../stats_writer/metadata.rs`).

Structure and algorithm:

- **Bins.** A streaming histogram keyed by `local_deletion_time` (seconds since epoch),
  each bin holding a tombstone count. CQLite mirrors Cassandra's
  `StreamingTombstoneHistogramBuilder` with a fixed cap of **100 bins**
  (`TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE`). When the cap is exceeded, the two **nearest**
  bins are merged: the merged key is the count-weighted average of the two points and
  the merged value is the sum of the counts (`merge_closest_bins`, matching Cassandra's
  `mergeNearestBins`).
- **Legacy (nb) serialization** — `write_tombstone_histogram`:

  ```text
  i32 BE  maxBinSize   (100 when non-empty, 0 when empty)
  i32 BE  size         (number of bins)
  for each bin:
    f64 BE  point       (local-deletion-time as a double)
    i64 BE  value       (tombstone count in this bin)
  ```

  An empty histogram serializes as 8 bytes (`maxBinSize = 0, size = 0`).

- **Determinism / byte-stability (scope-limited).** Bins are stored in a
  `BTreeMap<i64, i64>` and serialized in ascending key order, so for a **given final
  bin set** the serialized bytes are order-stable. This is NOT a full
  insertion-order-independence guarantee: the histogram is built by **streaming
  insertion** (`update`), and once the input exceeds the 100-bin cap the builder
  merges the two **nearest** bins on the fly (`merge_closest_bins`). Because the
  merged key is the count-weighted average of whichever two bins happen to be
  adjacent at that moment, the resulting merged bins — and thus the final byte
  output — can differ depending on the **order** in which the same multiset of
  deletion times was inserted. So byte-identity across different insertion orders of
  the same deletion-time multiset is **not guaranteed**, and the `stats_writer` module does
  **not** assert it. What the tests in `stats_writer/` DO assert (issue #730) is
  the framing: a non-empty histogram reports `maxBinSize = 100`, an empty one reports
  `maxBinSize = 0, size = 0`, and below the cap there is one bin per distinct
  deletion time (no byte-stability / insertion-order-independence assertion above the
  cap).

Authority: `org.apache.cassandra.utils.streamhist.StreamingTombstoneHistogramBuilder`
and `org.apache.cassandra.io.sstable.metadata.StatsMetadata` (`estimatedTombstoneDropTime`).

(The TOC structure that used to be described here as "Full Cassandra TOC Structure (Not
Implemented)" is documented — and is what CQLite actually writes — under *Serialized layout
of the file* and *Writing Statistics.db* above. The 44-byte TOC prelude is
`4 + 4 + 4×8 + 4`, not 32 bytes.)


