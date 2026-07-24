# CQLite v0.16.1

Patch release on v0.16.0. One headline feature: CQLite now reads a **second
Cassandra on-disk format** — CommitLog segment files — alongside SSTables.

## New: Cassandra 5.0 CommitLog reader (#2389, PR #2797)

A `CommitLogReader` that parses Cassandra 5.0 CommitLog segment files (the raw
files Cassandra writes via `CommitLogSegment`/`CommitLogDescriptor`) into a
stream of decoded mutations. Contributed by @rustyrazorblade.

- **Library API**: `cqlite_core::storage::commitlog` — `CommitLogReader::open`
  / `open_with_schemas`, lazy `MutationIter` (streaming decode, one record at a
  time; 128 MB segment cap).
- **CLI**: new `read-commitlog` subcommand (JSON and text output) alongside the
  existing SSTable commands.
- **What it decodes**: descriptor header (version-gated, Cassandra 5.0-era
  commitlog version 7), CRC-framed sync sections with torn-tail tolerance
  (mirrors `tolerateTruncation`), and schema-aware mutation/cell decode for the
  common insert path.
- **Honest bail, never a guess**: unmodeled constructs (clustering columns,
  static rows, collection/complex columns, deletions, range tombstones) are
  reported structurally rather than misdecoded; compressed and encrypted
  segments fail closed with a typed error. No-heuristics throughout — format
  facts come from the descriptor header and supplied schema only.
- **Verified against Cassandra source and real fixtures**: field-for-field
  adjudication against `cassandra-5.0.2` (descriptor layout, sync-marker CRC
  feed, mutation/PartitionUpdate serialization, `VIntCoding`,
  `CounterContext`), plus parity fixtures produced by a real Cassandra 5.0.2
  node with an insert-set-vs-decoded-set oracle. Format documentation shipped
  as Appendix H of the SSTable definitive guide.
- **Scope line**: reader only. The CommitLog **writer** is #2388 (explicit
  follow-on); CDC tailing, encryption support, and query/Flight-surface
  integration are out of scope for this pass. Segments written under
  `storage_compatibility_mode: NONE/UPGRADING` (commitlog version 8) are
  rejected with a typed error — version-8 support is tracked as a follow-up.
- Known hardening follow-ups (hostile-file edge cases, none reachable from
  authentic Cassandra output): #2838.

## Also in this release

- Delivery/CI housekeeping riding main since v0.16.0 (0.16.0 GA field-validation
  report, telemetry records).

## Upgrade notes

No breaking changes. All 0.16.0 APIs are unchanged; the commitlog module and
CLI subcommand are purely additive.
