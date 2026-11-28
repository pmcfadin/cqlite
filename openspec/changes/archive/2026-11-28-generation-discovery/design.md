# generation-discovery — design (issue #4626)

All decisions below are the owner's, Q32 to Q42, settled 2026-11-24/25. "Beat" names the alternative
each decision won against, so a reviewer can see what was already weighed. Nothing here adds an option.

## Terms

Defined in the repo-root `GLOSSARY.md`; short form here.

- **Generation.** One SSTable: the set of component files sharing a descriptor
  (`<version>-<id>-<format>-<Component>.<ext>`) in one directory.
- **Generation id.** The `<id>` token. A sequence id (digits, at most 10 characters, within the Java
  `int` range) or a UUID id (28 characters, base-36 TimeUUID, e.g. `3gb2_0rbd_2lhua2pw7a2hlyvbsp`).
- **Live set.** The generations Cassandra would load from a directory: after applying transaction
  logs, with the conditions below reported, not hidden.
- **Scope.** Which one directory is listed: Live, Backups, Live+Backups, or Snapshot(tag).
- **Condition.** A closed, named fact about a generation or its metadata.
- **Trusted metadata.** A generation's `Statistics.db` content, obtained only through the module's one
  accessor (floor, bounded read, integrity check, single parse, cached).

## The module

`cqlite-core/src/storage/sstable/generation/`, declared in `sstable/mod.rs` with its cfg on the
declaration (#1712). It owns four things and nothing else.

1. **Listing.** Given a table directory and a Scope, return the generations with their component sets
   and conditions, sorted by Generation id.
2. **Id handling.** Parse, validate, order and allocate ids.
3. **Trusted metadata.** The one accessor per generation.
4. **Transaction-log liveness.** Reading `*_txn_*.log` and classifying files.

Callers make roughly four calls: list a table directory with a Scope and a TocMissing policy; ask a
generation for its id, descriptor, components and conditions; ask a generation for its trusted metadata;
ask a directory for the next id. Raw lenient `Statistics.db` parsing becomes private to the module (Q36),
so a caller cannot bypass the accessor. The module returns data; it does no rendering.

### Listing, following Cassandra

`Directories.SSTableLister` (`db/Directories.java:980`) lists via `LifecycleTransaction.getFiles`, which
is `LogAwareFileLister`. The module mirrors the parts that decide liveness:

- **One directory per Scope, no recursion.** `LogAwareFileLister.list` drops every directory entry
  (`LogAwareFileLister.java:110`), so a secondary-index subdirectory, `snapshots/` and `backups/` are never
  entered while listing the table directory. `Directories.SSTableLister.filter` lists `backups/` and
  `snapshots/<tag>` as separate, explicit calls (`Directories.java:1094-1108`); the names are
  `BACKUPS_SUBDIR` and `SNAPSHOT_SUBDIR` (`:119-120`).
- **Non-log files first, then logs.** Files are listed before transaction logs because logs are
  deleted last (`LogAwareFileLister.java:88-95`). The module does the same so a racing cleanup cannot
  show a log without its files.
- **Descriptor filtering.** A file whose name does not parse as an SSTable component is ignored
  (`Directories.java:1128-1130`, `Descriptor.java:364-410`). Transaction logs themselves are never
  returned as components (`:1121-1122`).
- **Transaction-log liveness.** A log matching `LogFile.FILE_REGEX` (`LogFile.java:80`), e.g.
  `nb_txn_compaction_<uuid>.log`, is read. If it is complete (a last record is present), a committed
  transaction hides its REMOVE files and an uncommitted one hides its ADD files, "regardless of disk
  status" (`LogAwareFileLister.java:144-148`, `setTemporary` `:197-203`). If it is not complete but every
  old file is present, the transaction is in progress and is treated as aborted (`:150-154`). If old
  files are missing and the log is not complete after one re-read, the directory is inconsistent
  (`:172-186`), and the listing fails with the named error `TxnInconsistent`.
- **Relocated table directories.** Settled by owner ruling Q47 (2026-11-27): a txn log record applies
  when its keyspace directory, table directory and file name match the listed directory's, whatever its
  absolute prefix, and the listing carries a note naming the original prefix. A deliberate deviation from
  Cassandra's `isInFolder` (`LogFile.getFilesOfType`), which would hide nothing in a copied directory.
- **Corrupt log records.** Cassandra's `OnTxnErr` chooses between throwing and logging
  (`LogAwareFileLister.java:133-137`). The module follows the setting Cassandra uses to load a table,
  `IGNORE` (`ColumnFamilyStore.java:519`): it classifies from the readable records and names the corrupt
  log in a directory-level note of the listing result, which a verb may refuse on (owner ruling Q44).
  Beat: always `THROW`, which would refuse a directory Cassandra opens and serves.

### Conditions (Q38)

The set is closed. A condition is attached to a generation, or (for `Absent`) returned for a
requested one that is not there.

| Condition | Meaning | Live? |
|---|---|---|
| `TocMissing` | `Data.db` present, `TOC.txt` absent | live |
| `Orphan` | `Data.db` missing or zero-length, other components present | not live, reported |
| `UnsupportedVersion` | version below the floor, or not a version the format defines | live-listed, refused on use |
| `Unreadable` | a component could not be read (I/O error) | live |
| `Corrupt` | `Statistics.db` failed the integrity check (#4612) | live |
| `Absent` | the requested generation or component does not exist | n/a |

`TocMissing`: Cassandra includes such a generation (`TOCComponent.loadOrCreate` falls back to
`descriptor.discoverComponents()` and recreates the TOC, `TOCComponent.java:123-145`), so excluding it
would be wrong. `Orphan`: Cassandra's scrub treats a missing or zero-length `Data.db` as "all components
are orphaned" (`SortedTableScrubber.java:165-180`). Listing is not scrub, so the module reports orphans
and never deletes them.

### Two caller policies (Q41)

Callers choose only:

1. **Scope.** The default is Live.
2. **Whether to accept TocMissing.** The read-only verbs (`explain`, `find`, `diagnose`, `diff`,
   `verify`, `salvage`) and `rebuild` accept it and show the condition. `compact` and the 0.20 write
   verbs refuse it by name, because their output depends on CQLite's TOC-last publication barrier
   (owner ruling Q43, 2026-11-25, amending Q41).

Anything a verb cannot handle (a BTI input to a BIG-only path, a UUID id to a path that has not been
migrated) is a **named refusal in that verb**, raised from the module's typed list. The module never
filters on a verb's behalf, and a verb never drops a generation without saying so (spec requirement
"No silent filtering").

### Trusted metadata, lazily (Q36)

Per generation, on first request:

1. **Floor, from the descriptor.** If the version is below the floor, return `UnsupportedVersion`
   without opening any file.
2. **Bounded read.** Read `Statistics.db` under `MAX_STATISTICS_BYTES`.
3. **Integrity check.** The strict layout and CRC check already in
   `statistics_reader/integrity.rs` (`read_checked_statistics`), a port of
   `MetadataSerializer.deserialize`. This change reuses it; it does not rewrite it.
4. **Single parse.** Parse once into the typed metadata.
5. **Cache.** Store the value, or the typed failure (`Unreadable`, `Corrupt`, `UnsupportedVersion`).
   A second request returns the cached outcome without touching the disk.

Each failure is a typed value, so a verb maps it to its own wording (`explain` exit 2, `diagnose`
`unreadable_generations`, `diff` `side=b`). Step order matters: floor first means a pre-`na` file never
reaches a parser that would mis-read it (`sstable-version-floor` requirement 2).

### Generation ids and allocation (Q34)

- **Parse.** A token is a sequence id if it is 1 to 10 digits (`SequenceBasedSSTableId.java:112`) and
  fits Java `int` (`Integer.parseInt`, `:124`), and a UUID id if it is exactly 28 characters matching the
  base-36 pattern (`UUIDBasedSSTableId.java:41,114,130`). UUID is tried first
  (`SSTableIdFactory.java:38-45`). A 10-digit token above 2147483647 does not parse: the descriptor is
  invalid, as in Cassandra.
- **Order.** `SSTableIdFactory.COMPARATOR` (`:81-96`): sequence ids compare as `int`, UUID ids by
  TimeUUID, and every sequence id sorts before every UUID id. `sortedList` uses it
  (`Directories.java:1058-1063`).
- **One id space.** The id is the second token and the format is independent
  (`Descriptor.java:402`). BIG (`na`, `nb`, `oa`) and BTI (`da` only) share one id space, so `nb-1-big`
  and `da-1-bti` collide in id even though the names differ. `getUIDGenerator` scans every descriptor
  in the directory with `includeBackups(true)` (`Directories.java:1394-1405`).
- **Next id.** The maximum sequence id over the Live set and `backups/`, plus one
  (`SequenceBasedSSTableId.java:97-107`; starts from 0, so the first id is 1). UUID ids are ignored for the
  maximum (`:99`).
- **Refusal.** A writer asks the module for the next id. If the maximum plus one exceeds 2147483647,
  the module returns a named error and the writer writes nothing. This is stricter than allocating a
  `u64` (today's `scan_generations`) and is what Cassandra's `int` constructor would require.
- **No create-over.** A writer additionally never publishes onto an existing name (the #4623 overwrite
  hazard); the next id from this module is the mechanism, and the existing `StagedPublish` check
  (`staged_publish.rs:174-183`) stays as a second line.

## Decisions and what each beat

- **Q33, txn logs decide liveness; TocMissing is live; an inconsistent state fails the listing.** Beat:
  (a) the status quo, a bare directory scan, which counts files a committed compaction obsoleted; (b)
  requiring `TOC.txt` as the liveness test, which hides the very generations `rebuild` and `salvage`
  exist for and which Cassandra itself keeps; (c) tolerating an inconsistent state with a warning, which
  returns a set Cassandra would refuse to load.
- **Q34, both id forms, Cassandra's comparator, one id space, max+1, int-range refusal.** Beat:
  sequence-only (UUID tables are real, since `uuid_sstable_identifiers_enabled` is a documented
  setting); a string compare of ids (wrong order across widths); per-format or per-version id spaces
  (Cassandra has one); saturating or wrapping at the limit.
- **Q35, four Scopes, one directory each, no recursion.** Beat: a recursive walk with path filters (it
  is how #4622 happened); one Scope per verb (the same four values cover all current callers).
- **Q36, one lazy accessor, cached value or failure, lenient parse private.** Beat: eager metadata on
  every listing (a directory of 10,000 generations should not read 10,000 files to print names); a
  free function each caller invokes (the 16+ sites); caching only success (a damaged file would be
  re-read and re-judged per call).
- **Q37, floor first, from the descriptor.** Beat: checking after the open (reads a file the floor
  forbids); a per-verb floor (today's disagreement). Consequence: `rebuild` now refuses a below-floor
  generation, where it reported the unreadable Statistics fields as `lost`.
- **Q38, closed condition set, per-verb vocabulary kept.** Beat: an open or free-text set (callers
  cannot match exhaustively); one shared output vocabulary (it would change every verb's JSON shape
  and break their specs).
- **Q41, scope and TocMissing are the only caller policy; named refusals, no silent filtering.** Beat:
  per-verb filter closures (silent filtering is what today's `nb-*-big` check does).
- **Q42, six terms and a repo-root `GLOSSARY.md`.** Beat: leaving the terms in this design only (they
  would drift).

## Delivery phases (Q39, Q40)

One OpenSpec change, approved by the owner at Seam 1 before any code. PRs, in order. Each migration PR
deletes the copy it replaces.

1. **The module plus Cassandra-oracle tests.** BIG, BTI and UUID-id fixtures generated on the local
   Cassandra 5.0.8 VM with `uuid_sstable_identifiers_enabled: true`, plus transaction logs, snapshots
   and backups. No verb changes.
2. **Write engine and compact.** Replaces `scan_generations` and `discover_input_sstables`. Closes
   #4622 and #4623.
3. **explain, find, diagnose, diff.** Replaces `explain_resolve::discover_live_generations`, `find.rs`
   and the `diagnose` listing; `diff` reuses `explain`'s resolver and moves with it.
4. **rebuild and salvage.** Replaces the `rebuild` and `salvage` listings. Rebuild's below-floor
   behaviour changes here.
5. **`merge/mod.rs` Statistics reads, and its file split.** The raw Statistics reads there move to the
   accessor; the file is over the #1116/#1135 size threshold, so the split lands with the change
   that touches it.
6. **verify.** Last, because `verify` classifies damage itself (`read_bounded_statistics` without the
   integrity check, "for callers that apply their own classification") and needs the accessor's typed
   failures to feed that classification unchanged.

Each phase ends with the full gate on Linux, the C intent audit, roborev through
`scripts/flow/roborev-review.sh`, and `premerge-assert`.

## Seam 1 notes (for the owner)

These are not new options; they are places where the approved design leaves a per-verb value to fill in
and this spec filled it with the lowest-change choice. The owner may overrule at Seam 1.

- **Which verbs accept TocMissing.** Settled by owner ruling Q43 (2026-11-25): the read-only verbs and
  `rebuild` accept; `compact` and the write verbs refuse.
- **Corrupt log records.** Settled by owner ruling Q44 (2026-11-25): Cassandra's startup `IGNORE`
  behaviour plus a named directory-level note; not `TxnInconsistent`.
- **`UnsupportedVersion` is listed, not dropped.** A below-floor generation appears in the listing with
  its condition so `rebuild` and `explain` can refuse it by name.
