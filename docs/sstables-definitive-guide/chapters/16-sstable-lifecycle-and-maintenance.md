## SSTable Lifecycle and Maintenance

SSTable files are immutable but not static: they are validated, scrubbed, compacted, upgraded, and shipped across nodes. This chapter outlines common lifecycle operations and the invariants that keep multi-file components consistent (with a focus on `TOC.txt`). It closes with an anti-corruption checklist and a brief linkage to repair and streaming.

### In this chapter you will learn
- Common lifecycle operations and when they apply
- How TOC and component invariants are validated
- How to spot orphaned or mismatched components
- Where repair and streaming fit (overview)

## Lifecycle Operations

Key offline tools and operations that act on SSTables:

- `sstablescrub`: Scans SSTables to detect and attempt recovery from certain corruptions; rewrites a safe copy when possible (offline).
- `sstablemetadata`: Prints `Statistics.db` contents and derived summaries; useful for verifying timestamp ranges, row counts, compression, and droppable tombstones.
- `sstabledump`: Dumps partition/row content for inspection and triage (read-only).
- Compaction: Background process that rewrites files to control amplification and reclaim space; see Chapter 15.

Tiny, trimmed example (illustrative) from running metadata on a single SSTable:

```text
$ sstablemetadata nb-1-big-Data.db   # trimmed output
SSTable: nb-1-big-Data.db
min_timestamp: ...
max_timestamp: ...
total_rows: ...
estimated_droppable_tombstones: ...
compression: algorithm=LZ4, ratio=...
```

Tip: Prefer verifying a few SSTables per table (newest, oldest, largest) to establish envelope ranges and spot anomalies.

## TOC and Component Invariants

`TOC.txt` enumerates the components present for a given generation and is authoritative for lifecycle checks. Tools validate it against the directory listing and component headers.

> Don’t mix component families in a single generation: BIG components must not be combined with BTI components under the same `{generation}`. During upgrades you may have both families in the directory, but each generation is internally consistent.

Core invariants (Cassandra 5.0 multi-file BIG/BTI formats):

- Presence: All components listed in `TOC.txt` exist on disk; no unexpected files beyond the set and `TOC.txt` itself.
- Cross-listing: All component files present (except `TOC.txt`) are listed in `TOC.txt`.
- Header consistency: Generation and table identity are consistent across `Data.db`, `Index.db`/`Partitions.db`, `Rows.db`, `Statistics.db`, `CompressionInfo.db`.
- Summary/Index alignment: `Summary.db` samples are sorted and correspond to valid `Index.db` positions.
- Compression alignment: `CompressionInfo.db` chunk count/offsets are plausible for the `Data.db` size.
- Digest/integrity: Optional per-chunk CRCs and `Digest.crc32` (when present) validate payloads.

For implementation examples of directory validation, statistics parsing, and compression metadata checks, see Appendix C.

## SSTable-Set Freshness and Refresh

A reader that holds a table's SSTable set has a second consistency question beyond
per-generation component integrity: **when does its view of *which* generations exist
change?** A flush or compaction can add and remove generations at any moment.

In Cassandra the answer is a live in-memory `View` guarded by
[`Tracker`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/lifecycle/Tracker.java)
(`org.apache.cassandra.db.lifecycle.Tracker`): every SSTable add/remove replaces the
`View` under a lock, so a read never observes a half-published compaction result. An
offline reader has no such coordinator — it only sees the directory.

CQLite makes the answer **explicit per surface** (issue #1749):

| Surface | Freshness | Behavior when a generation disappears mid-query |
|---------|-----------|--------------------------------------------------|
| Long-lived library handle (Python / Node / CLI REPL) | Snapshot taken at `open`; changes apply **only** on an explicit `refresh()` | In-flight scans hold their readers and complete on the pre-refresh set — never torn |
| CLI one-shot | Fresh per process — the directory is re-discovered on every invocation | Not applicable (a single short-lived scan) |
| Arrow Flight | Fresh per request — `*-Data.db` is re-listed when the request starts | Classified from the typed error, not guessed: a vanished single split is a benign `superseded_split`; a retired snapshot/table directory is `snapshot_retired` |

A long-lived handle is deliberately *not* auto-refreshing. Discovery and open cost
O(SSTable count) — every `Index.db` / `Statistics.db` parsed and bloom filters built —
which is the entire reason a handle exists; re-checking the directory on each query would
defeat it. CQLite does not watch the filesystem, poll directory mtimes, or stat on the
query path.

`refresh()` (`cqlite-core/src/lib.rs:425` on `Database`, implemented by
`SSTableManager::refresh_tables` in `cqlite-core/src/storage/sstable/refresh.rs`) re-runs
the **same** TOC/filename-component discovery `open` used — no content sniffing — and
applies the diff:

- **added** generations are opened and become queryable;
- **removed** generations stop being queried; the reader closes once the last in-flight
  scan releases its reference;
- **unchanged** generations keep their existing reader, with the parsed index,
  statistics, and bloom filter untouched — refreshing an unchanged directory is
  effectively free.

Two properties matter operationally. It is **atomic and fail-closed**: every added
generation is opened *before* the held set is mutated, so a corrupt `Statistics.db` in a
new generation makes `refresh()` return a typed error and change nothing — there is no
partial view. And it provides **in-flight isolation**: a query resolves its reader list
once at start and holds references, so a scan running across a concurrent `refresh()`
drains to completion on the set it started with. This is consistency by held readers, not
by filesystem snapshots — CQLite does not hardlink or copy files the way a Cassandra
snapshot does (Chapter 19).

`refresh()` reports what it applied: `tables_scanned`, `readers_added`, `readers_removed`
(`RefreshReport`, `cqlite-core/src/storage/sstable/refresh.rs:55`).

## Concurrency During Active Use

Validation often runs while flush, compaction, or streaming may be writing new generations:

- Treat `TOC.txt` as the publication barrier. If `TOC.txt` is missing for a set of components, consider that generation in-flight and skip or re-scan later.
- Prefer snapshot-based scans (filesystem snapshots or stable directory listings) to avoid racing with file creation/deletion.
- Use read-only handles; avoid file locks. If a component disappears mid-validate, record a transient warning and retry.
- For mmapped components, open after existence checks and avoid long-held descriptors across rescans.

Implementation guidance mirrors this: directory scans should be resilient to transient access failures and record TOC/header inconsistencies as warnings unless clear corruption is detected. See Appendix C for a concrete walkthrough.

## Complexity and Performance Notes

- Directory scan: O(F) over files in the table directory.
- Per-generation validation: O(C) over components; header checks are O(1) per file.
- TOC reconciliation: O(C) to compare sets; includes string parsing and dedup checks.
- Summary/Index alignment: O(E) over sampled entries (linear) with sortedness checks.
- Compression map plausibility: O(K) over chunk offsets.

Practical guidance:
- Bound work per cycle (e.g., N generations) and backoff when compaction is busy.
- Cache prior results (mtime/size) to skip unchanged components.
- Emit compact reports for CI/ops; reserve deep dump only for failures.

## Repair and Streaming Linkage

Repair/streaming move SSTables between nodes and reconcile divergent histories; they depend on the same file/component invariants described above. See Chapter 18 (`18-repair-streaming-bootstrap.md`) for the process overview and when these occur.

### Integration Details
- Publication: send complete component sets; receivers validate `TOC.txt` and headers before marking SSTables available.
- Tombstone policy: `only_purge_repaired_tombstones` defers purging until repaired, impacting compaction outcomes post-repair.
- Level/metadata: level/repaired markers in `Statistics.db` inform LCS placement and post-repair compaction.
- Streaming integrity: digests are verified per stream; on failure, SSTables are discarded and retried.

Related Cassandra 5.0.0 code (pinned) for further study:
- Streaming session — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/streaming/StreamSession.java`
- Active repair service — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/service/ActiveRepairService.java`

### Key Takeaways
- `TOC.txt` is authoritative; validate both presence and cross-listing of components.
- SSTable-set freshness is per-surface and explicit: a long-lived handle is a snapshot at
  open until `refresh()`; a CLI one-shot is always fresh; Flight re-lists per request.
- Use `sstablemetadata` and `sstabledump` for quick health and content checks.
- Header, index/summary, and compression metadata must agree across files.
- Purging and compaction improve integrity over time but do not replace validation.
- Repair/streaming rely on the same invariants; broken invariants propagate.

### References
**Cassandra 5.0.8 (pinned):**
- `SSTableMetadataViewer` (tool) — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/tools/SSTableMetadataViewer.java`
- `SSTableExport` (tool, CLI alias: `sstabledump`) — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/tools/SSTableExport.java`
- `Descriptor` (component paths/TOC context) — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/Descriptor.java`
- `SSTableWriter` (emits `TOC.txt`) — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/SSTableWriter.java`
- `Tracker` (live SSTable-set `View` publication) — `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/lifecycle/Tracker.java`

For implementation details, see Appendix C.


