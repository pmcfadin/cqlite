# Design — atomic-publish (#4529)

**D1 Staging directory, not per-file temp names.** One directory `<out>/.cqlite-publish-<tag>.tmp/`
holds the generation in its final layout (`<keyspace>/<table>/` for salvage, flat for rebuild).
Writers are unchanged apart from their root. A dot-prefixed `.tmp` directory is recognisable, never a
keyspace/table directory, and Cassandra lists one directory without recursing.

**Owner ruling (2026-11-16).** AC1 reads: "After a crash at any boundary, Cassandra never loads a partial set: any final-named partial set lacks `Data.db`." Citation: Cassandra 5.0.8
`src/java/org/apache/cassandra/io/sstable/format/SSTableReaderLoadingBuilder.java:74`
(`checkArgument(components.contains(Components.DATA), "Data component is missing for sstable %s", descriptor)`
in `build`). This supersedes any wording that no component-named file appears before the set is
complete. The `Data.db`-last rename order is the accepted design. `--out` keeps accepting an existing
empty directory.

**D2 Publish order: everything else, then `TOC.txt`, then `Data.db`.** Renames cannot be collectively
atomic. At 5.0.8 a final-named set lacking `Data.db` is refused (`SSTableReaderLoadingBuilder.build`
`checkArgument`), whereas a set with `Data.db` could be opened from whichever components exist. So the
rename window never exposes an openable partial set; it exposes at worst a Data.db-less set. Final-named
files MAY exist during the window; `Data.db` never does until the set equals an uninterrupted run.

**D3 Leftovers refuse, they are not auto-cleaned.** An existing `.cqlite-publish-*.tmp` makes the verb
exit `1` naming it: interleaving with an interrupted run's files, or deleting files the operator may
want to inspect, are both worse than asking. The verbs already refuse a non-empty `--out`; the message
now names the cause. A refused or failed run removes its own staging directory.

**D4 Test seam.** In debug builds `CQLITE_TEST_PUBLISH_ABORT=<n>` aborts before the n-th rename.
Release builds compile it out. The kill-at-every-boundary proof is #4498's `issue_4529_*` harness.

## Durability barrier and pre-flight collision check

**Pre-flight collision check.** `publish` first checks EVERY destination path (`symlink_metadata`)
before the first rename. A collision refuses naming the path with nothing renamed; the dropped
stage is removed, so `--out` (and the existing file) is left exactly as found. A per-rename check
would already have moved earlier files.

**Directory fsync barrier.** Rename order alone is not durability order: under power loss a
filesystem may persist the `Data.db` rename before the others. `publish` therefore: (0) makes the
stage marker durable BEFORE any final name can be: fsyncs the stage directory, then its parent (the
stage's own entry), then every ancestor `begin` newly created up to the nearest pre-existing anchor
(a single-component relative `--out` such as `out` has the empty path as parent, normalised to `.`,
so `.` is the anchor and is fsynced) — otherwise a power loss could persist destination renames
while the `.cqlite-publish-*.tmp` entry is lost, leaving an unidentifiable final-named partial set
(AC2), pinned by an operation-order test (stage, parent and chain `SyncDir` precede the first
`Rename`) and a pure-helper test of the anchor; (1) creates the
destination directories; (2) renames every component except `Data.db` (`TOC.txt` last of those);
(3) fsyncs each destination directory, each intermediate directory up to the sync floor (e.g.
`out/ks`), and, when this run created `--out`, the chain up to its nearest pre-existing ancestor, so
the new directory entries are durable; (4) renames `Data.db`; (5) fsyncs those directories again;
(6) removes the stage. Any final-named partial set after power loss therefore lacks `Data.db`,
which preserves AC1. The order is pinned by a unit test recording the operation sequence
(dir-fsync, Data.db rename, dir-fsync). After step (6) `Drop` performs no filesystem operation
(the `published` flag), also pinned by a test.

## Publication started: the stage is retained

Once `publish` enters the rename window (the `publication_started` flag, set before the first
rename), a failed rename or directory fsync must not make the leftover anonymous: `Drop` then leaves
the stage directory (even when empty) and `--out`'s directory in place, so the final-named partial
set (never containing `Data.db`) sits next to a recognisable `.cqlite-publish-*.tmp` stage, and a
re-run refuses naming it. A failure BEFORE the first rename (collision, staged-file fsync, directory
creation) still removes the stage and leaves `--out` as found. After a fully successful publish
`published` keeps `Drop` inert. Pinned by unit tests that inject a failure on the 3rd rename and on the
pre-`Data.db` directory fsync, and by the #4498 harness, which injects ENOSPC and short writes at
every boundary from the first rename on and requires the re-run diagnostic to name the stage.

**Deferred (#4592).** The non-Unix directory fsync (a no-op today) concurrent publishes into one
`--out` (no-replace rename plus a root lock), and the fail-open leftover scan (`find_leftover` treats
`read_dir`/`file_type` errors as "no leftover"; roborev job 353) are tracked in #4592, not this change.
