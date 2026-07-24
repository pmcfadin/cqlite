## Context

CQLite has one binary-format parsing precedent to follow: the SSTable readers under
`cqlite-core/src/storage/sstable/reader/` (`header.rs`, `crc.rs`, `compression.rs`, `parsing/`,
`component_loading.rs`, ...), each format component gets its own module, version detection is gated
through an explicit `*VersionGates` type (`BigVersionGates`, `BtiVersionGates`), and every parser is
proven against real Cassandra-produced binaries fetched via `test-data/scripts/fetch-datasets.sh`,
never hand-crafted bytes. The CLI exposes reader functionality via subcommands like `read-sstable`,
`write-stats` (`cqlite-cli/src/cli_types.rs`).

CQLite also already has an internal `write_engine::wal` module (`cqlite-core/src/storage/write_engine/
wal.rs`) — this is CQLite's **own** write-ahead log for its own write-support feature, an entirely
different format from Cassandra's CommitLog. The two must not be confused in naming, module placement,
or (later) CLI verbs.

In-repo research (`docs/storage engine/cassandra-index/commitlog-cdc.md`) maps the Cassandra-side
class structure at a responsibility level: `CommitLogDescriptor` (segment id, version, compression/
encryption params, inferred from the filename `CommitLog-<version>-<id>.log`), `CommitLogSegment`
(the writer side — sync markers, CDC state, 32MB default segment size), `CommitLogReader` /
`CommitLogReplayer` (the reader side — CRC-validated per-mutation deserialization, `minPosition` seek
for resuming from a checkpoint, `tolerateTruncation` for a torn tail). That doc is class-level, not
byte-level — this design commits to the module boundary and test strategy; exact field ordering/
offsets inside the descriptor header and per-record framing are pinned during implementation against
real fixture bytes (the same way SSTable header parsing was originally reverse-engineered and then
locked down with golden tests), not asserted here as unverified byte layouts.

## Goals / Non-Goals

**Goals:**
- Land a `CommitLogReader` capability with a clear, non-colliding module home.
- Establish how CQLite will get and validate against real CommitLog fixtures, since none exist today
  (this is as much a test-infrastructure decision as an implementation one).
- Make an explicit, stated call on compression support and on streaming-vs-whole-load, instead of
  leaving either implicit.

**Non-Goals:** (mirrors proposal.md's Non-goals — not restated in full here)
- No writer (#2388), no CDC tailing, no encryption, no query/Flight wiring.

## Decisions

### D1 — Module home: `cqlite-core/src/storage/commitlog/`, a sibling of `sstable/` and `write_engine/`
**Chosen.** Not a submodule of `sstable/` (it's a different file format entirely, not an SSTable
component) and not placed anywhere near `write_engine/wal.rs` (different format, different purpose —
Cassandra's CommitLog vs. CQLite's own WAL). A top-level `storage::commitlog` sibling makes the
distinction unmistakable in the source map and in imports (`storage::commitlog::CommitLogReader` reads
unambiguously; nothing shares the `wal` name).
Layout, following the SSTable reader's per-concern-module convention:
- `storage/commitlog/descriptor.rs` — `CommitLogDescriptor` parsing (segment id, version, compression
  params) + the version gate (`CommitLogVersionGates`, same pattern as `BigVersionGates`).
- `storage/commitlog/reader.rs` — `CommitLogReader`: opens a segment, walks sync-marker-delimited
  sections, yields decoded mutations.
- `storage/commitlog/frame.rs` — per-record framing + CRC validation (length, length-CRC, payload,
  payload-CRC) and the truncation-tolerant end-of-segment handling.
- `storage/commitlog/mutation.rs` — the decoded mutation representation returned to callers (kept
  distinct from `write_engine::mutation` — read-side output type, not tied to CQLite's own write path).
**Alternatives considered:** nesting under `sstable/` — rejected, CommitLog is not an SSTable
component and Statistics.db's `CommitLogPosition` field is a reference, not this format. Nesting under
`write_engine/` — rejected specifically to avoid the WAL-naming collision the issue calls out.

### D2 — Fixture + oracle strategy: real Cassandra node, insert-set vs. decoded-set comparison
**Chosen.** Extend the Docker-based test-data generation story
(`test-data/scripts/`) with a CommitLog-specific script: spin up a real Cassandra 5.0 node, run a known
set of CQL inserts (tracked as ground truth), capture the still-open (or just-rolled) CommitLog
segment file before it's discarded, and ship it as a new gitignored fixture class (mirroring how
SSTable `Data.db` binaries are gitignored and fetched via `fetch-datasets.sh`). The parity oracle is
then: does `CommitLogReader` decode exactly the mutations we know we inserted, with matching
table/partition/cell values? This is the CommitLog analog of CQLite's existing **query-semantics
oracle** pattern (compare against a known result set at a pinned point), not the **physical-dump
oracle** pattern (there is no `sstabledump`-equivalent tool to diff against for CommitLog).
**Alternatives considered:** hand-crafting segment bytes from the Java source's serialization format —
rejected as exactly the kind of behavior-guessing the no-heuristics mandate and CQLite's whole parity
philosophy exist to avoid; a hand-built fixture can only prove the parser agrees with our own
understanding of the format, not with Cassandra's actual output. Reusing existing SSTable fixture
generation containers — rejected, CommitLog segments come from live mutation traffic, not a table
snapshot; needs its own generation script even if it shares the Docker Cassandra image.

### D3 — Compression: uncompressed only for v1, compressed explicitly deferred
**Chosen.** Ship uncompressed-segment support only. State this as a hard limitation (analogous to the
existing uncompressed-write claim boundary, issue #1406) rather than attempting partial/unproven
compressed-segment support. Compressed CommitLog segments use per-segment compressor class + params
recorded in the descriptor — decoding them correctly needs the same LZ4/Snappy chunked-decompression
work CQLite already has for SSTables (`storage/sstable/reader/compression.rs`), but wiring that up for
a second, independently-framed format is real additional scope this change does not need to carry.
**Alternatives considered:** supporting compression from the start — rejected as scope creep against
the issue's own "state explicitly, don't leave ambiguous" instruction; deferred to a follow-up once
v1's uncompressed path is proven against real fixtures.

### D4 — Streaming, not whole-segment materialization
**Chosen.** `CommitLogReader` exposes an iterator/streaming API over decoded mutations (reads and
decodes one sync-section at a time), not a `Vec<Mutation>` return. Segments are capped at 32MB by
Cassandra default, so whole-loading a single segment would still fit the <128MB memory budget in
isolation — but a caller inspecting multiple segments (the realistic forensic/recovery use case) must
not have that cost multiply per segment, and the `oom-audit` gate component's structural
no-unbounded-materialization expectation applies to new parsers the same as existing ones. Matches the
precedent set by CQLite's own `WriteAheadLog::replay()` being flagged for exactly the opposite
anti-pattern (Epic P / #1609, #1661 "streaming WAL replay") — this change should not introduce the
issue CQLite is actively removing from its own WAL.
**Alternatives considered:** whole-segment `Vec` return for simplicity — rejected per the above and
per the explicit ask in the issue's acceptance criteria #6.

### D5 — Public surface: library API + CLI subcommand `read-commitlog`
**Chosen.** `CommitLogReader::open(path) -> impl Iterator<Item = Result<Mutation>>` (or equivalent) as
the library surface, plus a `cqlite read-commitlog <path>` CLI subcommand alongside the existing
`read-sstable`/`write-stats` subcommands (`cqlite-cli/src/cli_types.rs`), satisfying the wiring-evidence
rule with a named, exercised entry point rather than an internal-only API. Output format (human-
readable summary vs. JSON) follows the CLI's existing output-writer conventions; exact flag surface is
an implementation-time detail, not a design-time one.

### D6 — Error handling: typed `Error` variants, no panics, matches the fuzz safety bar
**Chosen.** Add `Error::CorruptCommitLogFrame`, `Error::UnsupportedCommitLogVersion` (or similarly
named) variants to the existing `Error` enum (`cqlite-core/src/error.rs`) rather than introducing a
separate error type — keeps CQLite's error surface uniform across format parsers. A new fuzz target
(`fuzz/fuzz_targets/fuzz_commitlog_frame.rs`) exercises the frame parser on arbitrary bytes, matching
the existing five-target safety bar (never panic/hang/OOM).

## Risks / Trade-offs

- **[Risk] No local Cassandra 5.0 source was available to verify exact byte-level field ordering
  during design.** → **Mitigation:** design commits to structure and module boundaries, not asserted
  byte offsets; `sstable-developer` verifies field-for-field against actual Cassandra 5.0 source
  during implementation (the same TDD-against-real-fixtures process used for every SSTable component),
  and D2's insert-set/decoded-set oracle catches a wrong byte-level read as a correctness failure
  immediately, not silently.
- **[Risk] New fixture-generation Docker path is test infrastructure, not just test data** — a second
  thing that can flake/rot alongside the existing SSTable dataset pipeline. → **Mitigation:** reuse the
  same Cassandra Docker image and dataset-pin discipline (`test-data/scripts/`) rather than inventing a
  parallel toolchain; document the new fixture class in `docs/development/test-data-management`-adjacent
  docs as part of `tasks.md`.
- **[Risk] Segment format may have undocumented edge cases (mid-write torn segments, sync-marker
  chaining across a crash) beyond what `commitlog-cdc.md`'s class-level notes cover.** → **Mitigation:**
  acceptance criteria #3/#4 (truncation tolerance, fuzz safety) are non-negotiable gate items, not
  nice-to-haves; if a real edge case can't be reproduced via the Docker fixture path, it's filed as a
  follow-up issue rather than blocking this change on an unbounded investigation.
- **[Trade-off] Deferring compression (D3) means the reader cannot open every real-world CommitLog
  segment out of the box** — production Cassandra clusters commonly enable commitlog compression.
  Accepted for v1 scope control; the descriptor parser (D1) still detects and reports compression
  params so a compressed segment fails with a clear `Error::UnsupportedFormat`-style message instead
  of silently misparsing, keeping the failure mode honest (no-heuristics-adjacent: never guess past an
  unsupported format).
