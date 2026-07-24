## Why

CQLite reads Cassandra's SSTable formats but has zero ability to read Cassandra's other durable
on-disk format: the **CommitLog** segment (`CommitLog-<version>-<id>.log`). CQLite already parses a
*reference* to commit-log position (`CommitLogPosition` — segmentId + byte offset) as metadata inside
`Statistics.db`, but never the segment file itself. This blocks two things: (1) standalone
crash-recovery/forensic inspection of a node's commit log, independent of any running Cassandra
process, and (2) the freshness primitives explored in #2037 (ArrowMemtable/OLAP epic), which anticipate
a "commit-log watermark" as a bound for unflushed data — that bound is meaningless without a reader
that can actually decode the log. Now is the right time: it's a bounded, self-contained parser (like
each SSTable component was), it doesn't touch the write/compaction path, and it can be validated the
same way SSTable parsing was — real Cassandra-produced fixtures, not hand-crafted bytes.

This is the **reader** half of a two-part ask (read + write commit logs). The writer is tracked
separately as issue #2388 and is an explicit non-goal here, per owner decision to sequence reader
first — the reader needs the writer's *output* to validate against (real segments), but the reader
implementation itself does not need CQLite to *produce* segments; real Cassandra nodes already do,
and are how the test fixtures get generated (mirrors how CQLite's SSTable readers were built and
validated against Cassandra-written SSTables before CQLite could write its own).

Target milestone: none of M6 (WASM)/M7 (perf/v1.0) — this is scope expansion beyond the current 0.15
theme (cqlite-trino latency/throughput). Tracked as its own line, not folded into an active milestone.

**Oracle vs design: design-driven.** Unlike SSTable parsing, there is no `sstabledump`-equivalent tool
that dumps a CommitLog segment to a comparable JSONL reference, and CQLite has never generated
CommitLog test fixtures before — the test/fixture strategy itself has to be designed, not just the
parser. This proposal defines both.

## What Changes

- Add a new `CommitLogReader` in `cqlite-core` that opens a Cassandra 5.0 CommitLog segment file and
  yields decoded mutations (partition key, table reference, cell/row data per Cassandra's mutation
  wire format), matching the semantics of Cassandra's own `CommitLogReader`/`CommitLogReplayer`
  (CRC-validated per-record framing, sync-marker-delimited sections, `tolerateTruncation` behavior on
  a torn tail).
- Add a `CommitLogDescriptor` parser: segment id, version, compression parameters — version-gated the
  same way `BigVersionGates`/`BtiVersionGates` gate SSTable versions, scoped to the Cassandra 5.0-era
  commitlog version only.
- Add a public surface to exercise the reader end-to-end: a library API plus a CLI subcommand
  alongside the existing SSTable dump commands (exact command name decided in design.md).
- Add a new test-fixture generation path for CommitLog segments (Docker-based, mirroring
  `test-data/scripts/fetch-datasets.sh`'s SSTable fixture story) since none exists today, plus a
  parity oracle: "mutations we inserted" vs. "mutations the reader decoded."
- Add a fuzz target for the CommitLog frame parser, matching the existing `fuzz/` safety bar for
  SSTable parsers (never panic/hang/OOM on arbitrary bytes).

Not changing: no SSTable reader/writer code, no compaction, no query engine, no write-path
(`write_engine`/WAL) behavior. This proposal introduces a second, independent binary-format parser;
it does not touch the first.

## Capabilities

### New Capabilities
- `commitlog-reader`: parsing Cassandra 5.0 CommitLog segment files (descriptor header + mutation
  stream) into decoded mutations, with CRC validation, truncation tolerance, and a public surface
  (library + CLI) proven by an end-to-end test against real Cassandra-produced fixtures.

### Modified Capabilities
(none — this is purely additive; no existing spec's requirements change)

## Impact

- **New code surface**: a new module in `cqlite-core` (name decided in design.md, e.g.
  `storage::commitlog`) plus a new CLI subcommand in `cqlite-cli`. No changes to existing public APIs.
- **No-heuristics impact**: none introduced — segment version/format is read from the
  `CommitLogDescriptor` header authoritatively, same posture as SSTable version detection. No byte-
  pattern sniffing.
- **Binding surfaces (Python/Node)**: out of scope for v1 — the reader ships as a Rust library +ClI
  surface first; binding exposure is a future follow-up, not part of this change's acceptance criteria.
- **Memory budget (<128MB)**: in scope for the design to address explicitly — Cassandra caps a segment
  at 32MB by default, so a whole-segment materialization would still be under budget for a single
  segment, but the design must state the streaming-vs-whole-load choice and its budget implications
  rather than leaving it implicit.
- **Test infrastructure**: adds a second Docker-based fixture-generation path (alongside the existing
  SSTable one) and a new oracle style (insert-set vs. decoded-set comparison) since no
  `sstabledump`-equivalent exists for CommitLog segments.
- **Dependencies**: none new expected — CRC32 and basic binary framing are already available in the
  workspace (used by SSTable digest/CRC handling).

## Non-goals

- **Commit log writer** (#2388) — a separate, explicitly deferred issue/PR. This change does not add
  any CQLite-side CommitLog *writing* capability.
- **CDC tailing/live-streaming**: `cdc_raw` hard-link semantics, `.cdc-index` polling, the `CDCState`
  state machine — this is a live-sidecar/tailing feature, not segment parsing, and is out of scope.
  Static, offline parsing of a segment file (CDC or standard) is the only goal.
- **Encryption support**: encrypted CommitLog segments are not handled by this change.
- **Compression support**: whether LZ4/Snappy-compressed segments are in or out is decided explicitly
  in design.md (not left ambiguous) — uncompressed segments are the guaranteed baseline regardless.
- **Integration into the query/Flight surface**: this change proves the parser is correct in isolation;
  wiring it as a freshness source for #2037 or any other consumer is separate, future work.
- **Any change to CQLite's own internal WAL** (`storage/write_engine/wal.rs`) — that is a distinct,
  CQLite-native format for CQLite's own write-support feature and is untouched by this proposal.
