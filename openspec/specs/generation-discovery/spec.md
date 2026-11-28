# generation-discovery Specification

## Purpose
One module owns SSTable generation discovery for a table directory: which generations are Live, their ids and next-id allocation, the version floor, and cached trusted Statistics.db metadata. Verbs list through it instead of scanning directories themselves.

## Requirements

### Requirement: G1 — A committed transaction hides its REMOVE files, an uncommitted one hides its ADD files

The Live set SHALL be computed from the directory's transaction logs: a committed transaction hides its REMOVE files and an uncommitted one hides its ADD files, as `LogAwareFileLister` does (`db/lifecycle/LogAwareFileLister.java:144-148`, `setTemporary` `:197-203`). A log such as `nb_txn_compaction_<uuid>.log` matches `LogFile.FILE_REGEX` (`db/lifecycle/LogFile.java:80`).

#### Scenario: G1.1 committed compaction hides the inputs
- **Rule detail and Cassandra reference**: When the log is complete, the files of its REMOVE records are not live if the transaction committed, and the files of its ADD records are not live if it did not, whatever is on disk (`db/lifecycle/LogAwareFileLister.java:144-148`, `setTemporary` `:197-203`). When the log is incomplete but every file its REMOVE records name is present, the transaction is in progress and is treated as aborted: its ADD files are not live (`:150-154`). The module SHALL list non-log files before reading logs (`:88-95`). A transaction log SHALL never appear as a component (`db/Directories.java:1121-1122`). A record names its file by absolute path; the module SHALL apply a record to the listed directory when the record's path is inside it, or when the record's keyspace directory name, then its `<table>-<tableid>` directory name, then its file name match the listed directory's (the absolute prefix MAY differ), and a record naming a different keyspace or table directory SHALL NOT apply. When any applied record's absolute prefix differs from the listed directory, the listing result SHALL carry a directory-level note saying the log came from a relocated path and naming the original prefix. This is a deliberate, documented deviation from Cassandra, whose `LogFile.getFilesOfType` filters with `isInFolder`, a canonical-path compare that hides nothing in a relocated or copied directory (owner ruling Q47, 2026-11-27); the table id in the directory name is authoritative metadata, not a heuristic. A relocated record is verified (checksum aside, its REMOVE files' update time and count) against the listed directory, never against the path it names; the relocation rule applies to Scope `Live` only, and a record applies only to a file that is a direct child of the directory.
- **Given** a Cassandra-written directory holding `nb-1-big-*`, `nb-2-big-*`, `nb-3-big-*` and a complete
  committed `nb_txn_compaction_<uuid>.log` whose REMOVE records name generations 1 and 2 and whose ADD
  record names generation 3
- **When** the Live set is listed
- **Then** it contains generation 3 and not 1 or 2, even though `nb-1-big-Data.db` and `nb-2-big-Data.db`
  are still on disk.

#### Scenario: G1.2 uncommitted transaction hides the outputs
- **Given** a directory with `nb-1-big-*` and `nb-2-big-*` and an incomplete
  `nb_txn_compaction_<uuid>.log` whose REMOVE record names generation 1 (present) and whose ADD record
  names generation 2
- **When** the Live set is listed
- **Then** it contains generation 1 and not 2.

#### Scenario: G1.3 no log, no hiding
- **Given** a Cassandra-written `test_basic` table directory with no transaction log
- **When** the Live set is listed
- **Then** it contains every generation that has a `Data.db`, and no log file is returned as a
  component.

#### Scenario: G1.4 relocated table directory
- **Given** a Cassandra-written table directory copied away from the path its committed
  `nb_txn_compaction_<uuid>.log` records name, with the keyspace and `<table>-<tableid>` directory names
  unchanged
- **When** the Live set is listed
- **Then** the log applies exactly as in G1.1, and the listing carries a note naming the log and the
  original absolute prefix; the same log in a directory whose table id differs hides nothing and
  produces no note.

### Requirement: G2 — An inconsistent transaction state fails the whole listing with TxnInconsistent

When a transaction log is not complete, some file its REMOVE records name is missing, and after one
re-read the log is still not complete, the listing SHALL fail with the named error `TxnInconsistent`
carrying the directory and the log file name, and SHALL return no partial set
(`db/lifecycle/LogAwareFileLister.java:156-186`, the "inconsistent disk state" throw at `:184`).

#### Scenario: G2.1 old file missing, log incomplete
- **Rule detail and Cassandra reference**: If the log file is gone on the re-read, the transaction finished concurrently and the listing continues without it (`:159-161`). Records that fail verification are not this error; see G2.2.
- **Given** a directory with `nb-3-big-*` and an incomplete `nb_txn_compaction_<uuid>.log` whose REMOVE
  record names `nb-1-big` and `nb-2-big`, both absent from disk
- **When** the Live set is listed
- **Then** the result is `Err(TxnInconsistent)` naming the directory and the log, and no generation is
  returned.

#### Scenario: G2.2 corrupt log record follows Cassandra's startup lister
- **Rule detail and Cassandra reference**: Cassandra throws on records that fail verification only under `OnTxnErr.THROW` (`db/lifecycle/LogAwareFileLister.java:133-137`); the lister it uses to load a table at startup is `OnTxnErr.IGNORE` (`db/ColumnFamilyStore.java:519`), as is next-id allocation (`db/Directories.java:1397`). The module SHALL classify from the readable records as IGNORE does, and the listing result SHALL carry a directory-level note naming the corrupt log path. The note is not a generation Condition; a verb MAY refuse on it (owner ruling Q44, 2026-11-25). The inconsistent-state check of G2.1 still applies after classification.
- **Given** a completed transaction log one of whose records fails checksum verification, beside `nb-3-big-*`
- **When** the Live set is listed
- **Then** the result is `Ok`: the Live set is computed from the readable records exactly as Cassandra's
  IGNORE lister computes it, and the result's directory notes name the corrupt log path.

### Requirement: G3 — A generation without TOC.txt is live, carrying the condition TocMissing

A generation whose `Data.db` is present and non-empty and whose `TOC.txt` is absent SHALL be in the Live
set with the condition `TocMissing`. The module SHALL NOT exclude it. Cassandra keeps it: a missing TOC
is recovered by discovering the components on disk (`io/sstable/format/TOCComponent.java:123-145`).
Whether a verb accepts a TocMissing generation is the caller's policy (G10); a verb that does not accept
it SHALL refuse with a named error, not skip it.

#### Scenario: G3.1 TOC deleted
- **Given** a copy of a `test_basic` table with `nb-1-big-TOC.txt` deleted
- **When** the Live set is listed
- **Then** `nb-1-big` is present, its conditions are `{TocMissing}`, and its component set is the
  files discovered on disk.

#### Scenario: G3.2 a caller that does not accept TocMissing refuses
- **Given** the same directory and a caller that does not accept TocMissing
- **When** the caller requests its input set
- **Then** it receives a refusal naming `nb-1-big` and the condition, and no generation is dropped
  without being named.

### Requirement: G4 — A generation with a missing or zero-length Data.db is an Orphan: reported, not live

A generation whose component set does not contain `Data.db`, or whose `Data.db` has length zero, SHALL
be reported with the condition `Orphan` and SHALL NOT be in the Live set. This is Cassandra's own
definition: "missing the DATA file! all components are orphaned"
(`io/sstable/format/SortedTableScrubber.java:165-180`). The module never deletes an orphan's files.

#### Scenario: G4.1 Data.db missing
- **Given** a directory holding `nb-4-big-Index.db`, `nb-4-big-Statistics.db` and `nb-4-big-TOC.txt` and
  no `nb-4-big-Data.db`
- **When** listed
- **Then** `nb-4-big` is reported with `{Orphan}`, is not in the Live set, and its files are untouched.

#### Scenario: G4.2 Data.db zero-length
- **Given** `da-5-bti-Data.db` of length 0 and its other components
- **When** listed
- **Then** `da-5-bti` is reported `{Orphan}` and is not live.

### Requirement: G5 — Both id forms parse, with Cassandra's limits, and order by Cassandra's comparator

A Generation id token SHALL be parsed as a UUID id if it is exactly 28 characters matching
`([0-9a-z]{4})_([0-9a-z]{4})_([0-9a-z]{5})([0-9a-z]{13})` case-insensitively
(`io/sstable/UUIDBasedSSTableId.java:41,114,130`), otherwise as a sequence id if it is 1 to 10 ASCII
digits (`io/sstable/SequenceBasedSSTableId.java:90,112`) whose value fits in `0..=2147483647`
(`Integer.parseInt`, `:124`; the constructor asserts non-negative, `:40`).

#### Scenario: G5.1 UUID id accepted
- **Rule detail and Cassandra reference**: UUID is tried first (`io/sstable/SSTableIdFactory.java:38-45`). A token matching neither makes the file not an SSTable component of this table (`io/sstable/Descriptor.java:392-400`) and it is ignored. Ids SHALL order by `SSTableIdFactory.COMPARATOR` (`:81-96`): sequence ids by integer value, UUID ids by TimeUUID, and every sequence id before every UUID id. The listing SHALL be in that order (`db/Directories.java:1058-1063`).
- **Given** a Cassandra 5.0.8 table written with `uuid_sstable_identifiers_enabled: true`, with
  `nb-3gb2_0rbd_2lhua2pw7a2hlyvbsp-big-Data.db`
- **When** listed
- **Then** the generation is present with a UUID id whose text is `3gb2_0rbd_2lhua2pw7a2hlyvbsp`, and
  the component set is complete.

#### Scenario: G5.2 sequence ids sort numerically, not as text
- **Given** `nb-9-big-Data.db`, `nb-10-big-Data.db` and `nb-100-big-Data.db`
- **When** listed
- **Then** the order is 9, 10, 100.

#### Scenario: G5.3 sequence before UUID
- **Given** `nb-7-big-Data.db` and `nb-3gb2_0rbd_2lhua2pw7a2hlyvbsp-big-Data.db` in one directory
- **When** listed
- **Then** generation 7 precedes the UUID generation.

#### Scenario: G5.4 id outside the parse limits is not an SSTable
- **Given** `nb-2147483648-big-Data.db` (10 digits, above `Integer.MAX_VALUE`) and
  `nb-12345678901-big-Data.db` (11 digits)
- **When** listed
- **Then** neither appears as a generation, and no error is raised for either.

### Requirement: G6 — BIG and BTI, across every version letter, share one id space; the next id is max(live+backups)+1

The module SHALL provide the next sequence id for a table as the maximum sequence id found across the
Live set and `backups/`, plus Orphans, below-floor generations and files a transaction log hides (Cassandra's allocator lists without `skipTemporary`, so none of them is skipped), over every descriptor whatever its format (`big`, `bti`) and version letter
(BIG `na`, `nb`, `oa`; BTI `da` only), plus one, and 1 when there is none.

#### Scenario: G6.1 format and version do not separate ids
- **Rule detail and Cassandra reference**: UUID ids do not contribute to the maximum (`io/sstable/SequenceBasedSSTableId.java:97-107`, filter at `:99`; `db/Directories.java:1394-1405`, `includeBackups(true)` at `:1398`). A listing used for allocation SHALL ignore transaction-log errors as Cassandra's generator does (`OnTxnErr.IGNORE`, `:1397`), so a log with a corrupt record never blocks allocation (it is a note) and never lowers the maximum. An inconsistent transaction state (G2.1) still fails the listing and therefore the allocation, as it fails Cassandra's lister under either setting.
- **Given** `nb-1-big-*`, `oa-4-big-*` and `da-6-bti-*` in the Live set
- **When** the next id is requested
- **Then** it is 7.

#### Scenario: G6.2 backups count
- **Given** Live set max 3 and `backups/nb-9-big-*`
- **When** the next id is requested
- **Then** it is 10.

#### Scenario: G6.3 snapshots do not count
- **Given** Live set max 3 and `snapshots/before-upgrade/nb-50-big-*`
- **When** the next id is requested
- **Then** it is 4.

#### Scenario: G6.4 UUID ids do not count
- **Given** only `nb-3gb2_0rbd_2lhua2pw7a2hlyvbsp-big-*`
- **When** the next id is requested
- **Then** it is 1.

### Requirement: G7 — Writers refuse sequence ids outside the Java int range

If the next sequence id would exceed 2147483647, the module SHALL return a named error and the writer
SHALL write nothing. Cassandra's sequence id is a Java `int` (`SequenceBasedSSTableId.java:46-53`,
`:124`); an id above it is not a valid descriptor (G5.4). A writer SHALL never publish onto a name that
exists.

#### Scenario: G7.1 allocation at the limit
- **Given** a Live set whose maximum sequence id is 2147483647
- **When** a writer asks for the next id
- **Then** it receives the named refusal, and the directory is byte-identical afterward.

#### Scenario: G7.2 next to the limit
- **Given** a Live set whose maximum sequence id is 2147483646
- **When** the next id is requested
- **Then** it is 2147483647.

### Requirement: G8 — Each Scope lists exactly one directory and never recurses

Scope SHALL be one of `Live` (the default), `Backups`, `LiveAndBackups`, or `Snapshot(tag)`. `Live` lists the table directory itself; `Backups` lists `<table-dir>/backups`; `LiveAndBackups` lists
both; `Snapshot(tag)` lists `<table-dir>/snapshots/<tag>` and nothing else
(`db/Directories.java:1094-1108`; names `:119-120`).

#### Scenario: G8.1 Live does not read snapshots or backups
- **Rule detail and Cassandra reference**: No Scope SHALL descend into a subdirectory it was not named for: directory entries are skipped when listing (`db/lifecycle/LogAwareFileLister.java:110`), so a secondary-index subdirectory such as `.idx_name` and the `snapshots/` and `backups/` directories are never entered by `Live`. A missing table directory under Scope `Live` is an error; a missing `backups/` or `snapshots/<tag>` directory under Scope `Backups` or `Snapshot(tag)` yields an empty listing (`getSnapshotDirectoryIfExists` / `getBackupsDirectoryIfExists`, `:1096,1106`). Transaction logs apply within the one directory only.
- **Given** a table directory with `nb-1-big-*`, `snapshots/pre/nb-1-big-*` and `backups/nb-2-big-*`
- **When** listed with Scope Live
- **Then** only `nb-1-big` in the table directory itself is returned.

#### Scenario: G8.2 Snapshot(tag) lists the tag directory
- **Given** the same directory
- **When** listed with Scope `Snapshot("pre")`
- **Then** exactly the generations in `snapshots/pre` are returned and `snapshots/` is not enumerated for
  other tags.

#### Scenario: G8.3 secondary-index subdirectory is not entered
- **Given** `nb-1-big-*` and a `.by_email/nb-1-big-*` index directory
- **When** listed with Scope Live
- **Then** one generation is returned.

#### Scenario: G8.4 absent scope directory
- **Given** a table directory with no `backups/`
- **When** listed with Scope Backups
- **Then** the result is empty and is not an error.

### Requirement: G9 — The version floor is checked first, from the descriptor, with no file access

The module SHALL decide the version-floor condition from the filename descriptor alone, before opening
any component of the generation. A generation whose version is below the floor, or whose version string
the format does not define, SHALL carry `UnsupportedVersion` and every metadata request for it SHALL
return that condition without reading any file.

#### Scenario: G9.1 pre-na version, no I/O
- **Rule detail and Cassandra reference**: The floor is `na`/`nb` and `oa` for BIG and `da` for BTI (`sstable-version-floor`); a version `Version.validate` or `isCompatible` rejects is invalid in Cassandra (`io/sstable/Descriptor.java:388-407`). Each verb maps this condition to a refusal in its own wording, not to a degraded or empty result.
- **Given** `ma-1-big-Data.db` with no `Statistics.db` beside it, and a test that fails on any
  `open` of a component of that generation
- **When** its trusted metadata is requested
- **Then** the result is `UnsupportedVersion` naming `ma` and the floor, and no file was opened.

#### Scenario: G9.2 supported versions pass the floor
- **Given** `nb-3-big-*`, `oa-4-big-*` and `da-5-bti-*`
- **When** the floor condition is evaluated
- **Then** none carries `UnsupportedVersion`.

### Requirement: G10 — Callers choose only a Scope and whether to accept TocMissing; nothing is filtered silently

A caller SHALL supply at most (a) a Scope and (b) whether a TocMissing generation is acceptable. Any
generation the caller cannot handle (a format, version or id form) SHALL produce a named refusal in that
verb, never be dropped. The module SHALL NOT take a predicate, a format allow-list or a version
allow-list. The listing returned to a verb SHALL be complete for its Scope: the Live set plus the
reported Orphans.

#### Scenario: G10.1 refused, not skipped
- **Given** a Live set with `nb-1-big-*` and `da-2-bti-*` and a verb that handles BIG only
- **When** the verb runs
- **Then** it exits with a refusal naming `da-2-bti` and the unsupported format, and does not proceed
  with generation 1 alone.

#### Scenario: G10.2 no predicate parameter
- **Given** the module's public surface
- **When** the `pub-surface` listing is inspected
- **Then** no listing function accepts a closure, a format filter, or a version filter.

### Requirement: G11 — Trusted metadata is read once, in a fixed order, and failures are cached

A generation SHALL expose one accessor for its `Statistics.db` content. On first use it SHALL: (1) check
the floor (G9); (2) read the file under `MAX_STATISTICS_BYTES`; (3) apply the strict layout and checksum
check of `statistics_reader/integrity.rs` (the port of `MetadataSerializer.deserialize`,
`io/sstable/metadata/MetadataSerializer.java:146-227`); (4) parse once; (5) cache the value or the typed
failure.

#### Scenario: G11.1 second request does not touch the disk
- **Rule detail and Cassandra reference**: A later request SHALL return the cached outcome and SHALL NOT read the file again. The lenient parse SHALL be private to the module.
- **Given** a generation whose trusted metadata was requested once
- **When** it is requested again after `nb-1-big-Statistics.db` is deleted
- **Then** the same value is returned.

#### Scenario: G11.2 a failure is cached
- **Given** a generation with a truncated `Statistics.db`
- **When** the metadata is requested twice
- **Then** both return the same `Corrupt` failure naming the path, and the file is read once.

#### Scenario: G11.3 order of checks
- **Given** `ma-1-big-Statistics.db` truncated
- **When** the metadata is requested
- **Then** the outcome is `UnsupportedVersion`, not `Corrupt`.

#### Scenario: G11.4 no raw parse outside the module
- **Given** the repository after the final migration PR
- **When** the final grep task runs
- **Then** no `Statistics.db` read or lenient parse exists outside `generation/`.

### Requirement: G12 — The condition set is closed

The conditions SHALL be exactly `TocMissing`, `Orphan`, `UnsupportedVersion`, `Unreadable`, `Corrupt` and
`Absent`. A verb MAY render each in its own vocabulary but SHALL NOT add a condition to the module's
type, and a `match` on the type SHALL be exhaustive.

#### Scenario: G12.1 exhaustive match
- **Given** the condition type
- **When** a test matches it with no wildcard arm
- **Then** it compiles, and adding a seventh variant fails that test to compile.

#### Scenario: G12.2 conditions combine
- **Given** `nb-1-big` with `TOC.txt` absent and `Statistics.db` truncated
- **When** listed, then its metadata requested
- **Then** it carries `TocMissing`, and the metadata request fails `Corrupt`.

### Requirement: G13 — No silent filtering anywhere

Every file in a Scope directory SHALL be accounted for as exactly one of: a component of a listed
generation, a component of an Orphan, a component a transaction log hides (G1, exposed with the log that
hides it), a transaction log, or a file whose name is not an SSTable component (ignored as Cassandra
ignores it, `db/Directories.java:1128-1130`). The listing SHALL expose the hidden and ignored sets so a
verb can report them.

#### Scenario: G13.1 unrelated file is counted, not hidden
- **Rule detail and Cassandra reference**: A file that parses as an SSTable component but belongs to a different table than the requested one SHALL be ignored on the same rule (`db/Directories.java:1135-1138`).
- **Given** a table directory with `nb-1-big-*`, `.gitignore` and `notes.txt`
- **When** listed
- **Then** one generation is returned and the ignored set is `{.gitignore, notes.txt}`.

#### Scenario: G13.2 every Data.db is accounted for
- **Given** the committed corpus tables
- **When** each is listed
- **Then** every `*-Data.db` file on disk is a Live generation, an Orphan, or hidden by a named
  transaction log; none is in none of them.
