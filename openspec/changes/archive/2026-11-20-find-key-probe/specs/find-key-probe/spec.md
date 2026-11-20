# find-key-probe — new capability (issue #4205)

`cqlite find` SHALL report, for one partition key, exactly which generations of a table hold it
(and how each generation was probed — bloom-negative, index-miss, scanned, or hit, with location and
timing detail on a hit), and, when asked, which CommitLog mutations touch the key and whether any is
unflushed. All requirements are ADDED.

## ADDED Requirements

### Requirement: F1 — Per-generation probe outcome is the real read-path signal, un-collapsed

Each generation's probe result SHALL be one of `Hit`, `BloomNegative`, `IndexMiss`, or `Scanned(Hit|Miss)`, derived from the SAME authoritative primitives the production point-read path uses (bloom / Index.db for BIG, the trie descent for BTI), never a byte-pattern guess.

#### Scenario: F1.1 corpus-wide hit/absent parity with the sstabledump goldens
- **Given** the committed corpus's `*-Data.db.jsonl` goldens for a BIG table and a BTI table
- **When** `probe_generation` runs for every key present in ANY golden, against EVERY generation of
  that table
- **Then** the result is `Hit` in exactly the generations whose golden contains the key, and
  `BloomNegative`/`IndexMiss` in every other generation — never `Hit` where the golden disagrees
  (`cqlite-core/tests/issue_4205_find_corpus.rs`; corpus gating per #1094 —
  `CQLITE_REQUIRE_FIXTURES=1` hard-requires).

#### Scenario: F1.2 an absent key on a bloom-present BIG table never reports Hit
- **Given** a committed BIG fixture with `Filter.db` present, and a key present in NO generation
- **When** `probe_generation` runs on every generation
- **Then** every result is `BloomNegative` or `IndexMiss`, never `Hit`.

#### Scenario: F1.3 an absent bloom filter still yields index-miss, never unmeasured
- **Given** a temp copy of a committed BIG fixture with `Filter.db` deleted, and an absent key
- **When** `probe_generation` runs
- **Then** the result is `IndexMiss` — the missing optional component never degrades this into an
  unmeasurable/unknown outcome.

#### Scenario: F1.4 a BTI prefix collision resolves as IndexMiss without a scan
- **Given** a BTI fixture and a key whose trie prefix collides with a present partition's, but whose
  decoded key differs (design.md §D1.1)
- **When** `probe_generation` runs
- **Then** the result is `IndexMiss`, and no fail-safe scan is attempted (the trie's own resolved
  decode is authoritative).

### Requirement: F2 — A hit carries location and timing detail, decoded once, no extra I/O

`GenerationProbe::Hit` SHALL carry the partition's `Data.db` offset, its byte length (or `ToEof` for the last partition), the maximum writetime, whether it carries a partition deletion, and the `CellCoverage` the decoded rows prove (F4) — all derived from the SAME decode the probe already performs.

#### Scenario: F2.1 hit detail matches independently-computed expectations
- **Given** a committed fixture and a key present in one generation
- **When** `probe_generation` returns `Hit(detail)`
- **Then** `detail.data_offset` equals the offset independently resolved from `Index.db`/the BTI
  trie for that key, `detail.max_writetime` equals the max `liveness_info.tstamp`/cell timestamp
  found in that partition's JSONL golden, and `detail.has_partition_deletion` matches whether the
  golden's partition carries a deletion.

#### Scenario: F2.2 the last partition in a generation reports ToEof
- **Given** a committed fixture's LAST partition by on-disk order
- **When** `probe_generation` returns `Hit(detail)` for its key
- **Then** `detail.byte_length == ByteLength::ToEof`, never a guessed numeric length.

#### Scenario: F2.3 a fail-safe-scan hit reports unmeasured length, never a guess
- **Given** a generation with no usable partition index, holding the key
- **When** `probe_generation` falls back to the fail-safe scan and finds it
- **Then** the result is `Scanned(Hit(detail))` with `detail.byte_length == ByteLength::Unmeasured(_)`
  (and `detail.data_offset` is `None` when the scan reported no boundary) — never `0` or a derived length.

#### Scenario: F2.4 offset and length are honestly optional, never guessed
- **Given** the `HitDetail` shape
- **Then** `data_offset` is `Option<u64>` and `byte_length` is `Known(u64) | ToEof | Unmeasured(cause)`
  (#4159: a value that cannot be measured is named, never `0`). Text renders `unmeasured(<cause>)`; JSON renders
  `{"unmeasured": <cause>}` for the length and `null` for an absent offset.

### Requirement: F3 — CommitLog mutations touching the key are reported with segment, position, and writetime

With `--commitlog <dir>`, every mutation across every segment whose partition key matches SHALL be reported with its segment, byte position, and writetime — or `unmeasured(<cause>)` when the mutation's wire shape (a partition deletion, a clustered table, or a complex column) is not modeled by the decoder, never a fabricated value.

#### Scenario: F3.1 an unflushed insert is reported with a real writetime
- **Given** the committed `commitlog_test` CommitLog segment(s) + its companion SSTable set (this
  change's fixture, tasks.md §3) where one insert was never flushed
- **When** `find <table-dir> <key> --commitlog <dir>` runs for that key
- **Then** the unflushed mutation is reported with its segment id, byte position, and a writetime
  equal to the ground-truth value recorded in the fixture's manifest (computed independently by the
  test from the manifest, never from `find`'s own prior output).

#### Scenario: F3.2 a clustered-table mutation reports unmeasured writetime, never a guess
- **Given** a CommitLog segment carrying a mutation for a table with a non-empty clustering key
- **When** `find --commitlog` runs for a key that mutation touches
- **Then** the mutation IS reported (segment + position — partition-key matching is unaffected by
  clustering), with `writetime: unmeasured("clustered table not decoded")`.

#### Scenario: F3.3 an unreadable segment is its own named line, never a silent skip
- **Given** `--commitlog <dir>` pointing at a directory containing one healthy segment and one
  corrupt/unsupported-version segment
- **When** `find --commitlog` runs
- **Then** the healthy segment's matching mutations are reported normally, and the corrupt segment
  produces its own line naming the failure — never a silently shorter report (#4159 class).

#### Scenario: F3.4 a body-decode error does not end the segment scan
- **Given** a segment in which one CRC-valid record's mutation body cannot be decoded (a documented byte edit
  of the Cassandra-written segment), followed by a valid matching mutation
- **When** `find --commitlog` runs
- **Then** the bad record is its own `SegmentFailure` ("record at position N could not be read: …"), AND the
  later matching mutation is still reported (`cqlite-cli/tests/find_cli_tests.rs`).

#### Scenario: F3.5 the decoder API exposes record positions without changing `mutations()`
- **Given** `CommitLogReader`
- **Then** `mutations()` keeps its item type `Result<Mutation>`; the new `mutations_with_positions()` yields
  `(Option<u64>, Result<Mutation>)`. `Some(offset)` is the record's byte offset within the segment file; `None`
  means a framing error (no record offset exists and the iterator ends). A body-decode error carries
  `Some(offset)` and iteration CONTINUES past it (the frame walker has already advanced past the CRC-valid
  record).

### Requirement: F4 — `unflushed` is a fail-closed PROOF, not a timestamp comparison

AC change approved by the owner on 2026-11-16: stricter flushed proof

`unflushed` SHALL be `yes` when the mutation's writetime exceeds every generation's max for the key, `no` ONLY when every cell, liveness marker and deletion of the mutation is PROVEN present in the SSTables, and `unmeasured(<cause>)` otherwise. "At or below the generation max" alone SHALL NOT yield `no`.

#### Scenario: F4.1 mutations whose every cell is proven present report unflushed=no
- **Given** the fixture from F3.1
- **When** `find --commitlog` runs
- **Then** a mutation reports `unflushed: no` ONLY IF every one of its cells, its liveness marker and its
  deletions is proven present by a Hit generation (strictly newer, covered by a deletion, or an exact-state
  equal-timestamp match). A writetime at or below the generation max is NOT by itself sufficient.

#### Scenario: F4.2 a key absent from every generation is unconditionally unflushed
- **Given** a key present in the CommitLog but in NO generation (never flushed at all)
- **When** `find --commitlog` runs
- **Then** every mutation touching that key reports `unflushed: yes` — there is nothing to compare
  against, and doctrine (#4159) forbids a fabricated `no`.

#### Scenario: F4.3 an unmeasured writetime propagates to an unmeasured unflushed verdict
- **Given** the F3.2 clustered-table mutation
- **When** `find --commitlog` runs
- **Then** `unflushed: unmeasured("clustered table not decoded")` — the comparison never silently
  substitutes a default.

#### Scenario: F4.4 a mutation at or below the generation max with an unproven cell is unmeasured
- **Given** a mutation of two columns `a` and `b` where a Hit generation holds a newer cell of `a` (so the
  mutation's writetime is at or below the generation's max) but no newer, covering or exactly-equal cell of `b`
- **When** `find --commitlog` evaluates `unflushed`
- **Then** the verdict is `unmeasured("mutation coverage not proven by SSTable cells: column b @ <ts> (no
  newer or exactly equal SSTable cell)")` — NOT `no`.

#### Scenario: F4.5 an equal timestamp proves flushed only on an exact state match
- **Given** a mutation cell at timestamp T and a Hit generation holding that column at the SAME timestamp T
- **When** the SSTable cell is live, non-expiring and has identical value bytes
- **Then** the cell is proven (`unflushed: no` if every other need is proven too); but when the SSTable cell is
  a tombstone, is expiring, differs in value bytes, or its value cannot be serialized (also: `write-support`
  not compiled in), the cell is NOT proven and the verdict is `unmeasured` naming that cell.

#### Scenario: F4.6 what proves one cell, marker or deletion present
- **Given** one need of a mutation (a cell, a liveness marker or a deletion)
- **Then** a single Hit generation proves it by holding: (a) a STRICTLY newer cell of the same column (or a
  strictly newer marker); or (b) a COVERING deletion (partition deletion, row tombstone, row deletion) at or
  above the need's timestamp; or (c) an EQUAL timestamp ONLY on an exact state match — both cells live,
  non-expiring, identical value bytes (an equal non-expiring marker likewise). At an equal timestamp Cassandra
  breaks ties by tombstone/expiring state, local deletion time, TTL, then value bytes (`Cells.resolveRegular`,
  `src/java/org/apache/cassandra/db/rows/Cells.java`; `LivenessInfo.supersedes`,
  `src/java/org/apache/cassandra/db/LivenessInfo.java`; at `cassandra-5.0.8`), so an equal timestamp alone
  proves nothing. The comparison is per cell, never the partition's max timestamp, and is modeled only for a
  table without clustering columns; a clustered or undecoded mutation is `unmeasured`.

### Requirement: F5 — Exit codes and the zero-generations case

`cqlite find` SHALL exit `0` when the report completed (including "0 generations hold this key"), `1` on a usage error, and `2` when anything could not be read in full — never a shorter report standing in for a read failure.

#### Scenario: F5.1 a key absent everywhere still exits 0 with a full report
- **Given** a key absent from every generation of a table with no `--commitlog`
- **When** `cqlite find <table-dir> <key>` runs
- **Then** exit `0`, and every generation is listed with its `BloomNegative`/`IndexMiss`/`Scanned(Miss)`
  result — `cqlite-cli/tests/find_cli_tests.rs`, named in the gate's `cli-tests` list per #3522.

#### Scenario: F5.2 an unreadable generation is a named exit-2 failure
- **Given** a table directory with one generation whose `Data.db` cannot be opened
- **When** `cqlite find` runs
- **Then** exit `2`, and the failing generation is named in the output — never omitted.

#### Scenario: F5.3 a torn-tail segment is a named SegmentTruncated line and exits 2
- **Given** the Cassandra-written segment truncated mid-record (documented byte edit)
- **When** `find --commitlog` runs
- **Then** a `SegmentTruncated` line names the segment, mutations before the tear are still reported, and the
  exit code is `2`.

#### Scenario: F5.4 an unlistable directory is exit 2 naming the cause
- **Given** a table directory (or `--commitlog` directory) whose `read_dir` fails
- **When** `cqlite find` runs
- **Then** exit `2` and the underlying error is named (`cannot list …`) — never a usage error (exit 1) and
  never an empty report.

#### Scenario: F5.5 what exit 2 covers and how it renders
- **Given** any of: a generation that could not be opened or probed; a segment that could not be opened; a
  CommitLog record whose body failed to decode (scanning CONTINUES past it); a `SegmentTruncated` torn tail; a
  mutation touching the key that declares updates the decoder cannot walk past; a table directory or
  `--commitlog` directory whose `read_dir` fails
- **Then** the exit code is `2` and the cause is named. Text lines of the CommitLog section:
  `  <segment>@<position>: writetime=<ts|unmeasured(<cause>)>  unflushed=<yes|no|unmeasured(<cause>)>` (a
  mutation); `  <segment>: UNREADABLE  <cause>` (`SegmentFailure`; for a body-decode error the cause is
  `record at position N could not be read: <error>`, or `record at framing error could not be read: <error>`
  when no position exists); `  <segment>: torn tail -- mutations in the torn bytes cannot be seen`
  (`SegmentTruncated`). JSON: `{"segment","unreadable": <cause>}` and `{"segment","truncated": true}`.

### Requirement: F6 — No header hunting

Every probe and every CommitLog field SHALL be derived from a decoded, authoritative structure — `Filter.db`, `Index.db`/the BTI trie, `Data.db`'s parsed partition, or the CommitLog's own `EncodingStats`/delta-encoded fields — and the fail-safe scan path SHALL be a full schema-driven decode, never a byte-pattern resync search.

#### Scenario: F6.1 static no-resync-scan guard
- **Given** a script mirroring #4194/#4195's `test_*_no_resync_scan.sh` pattern (`tooling-tests`)
- **When** it greps the new `find_probe` module for `memchr`, `windows(`, `find(|b|`, `position(|b|`
  outside `#[cfg(test)]`
- **Then** none is present; any hit FAILs naming the line.

### Requirement: F7 — Partition-key literals: nested values split at depth zero

`<partition-key>` SHALL be one CQL literal per partition-key column, comma-separated, split by `split_key_literals` at commas at nesting depth zero and outside quoted text, then parsed by the SAME literal parser `query` uses.

#### Scenario: F7.1 nested tuple, collection, map and UDT literals stay whole
- **Given** the argument `{street: 'a,b', zip: 1}, [(1,2),(3,4)]`
- **When** `split_key_literals` runs
- **Then** it returns exactly two literals, `{street: 'a,b', zip: 1}` and `[(1,2),(3,4)]` (likewise `(1, 'a'), 7`,
  `[1, 2, 3], 'x'`, `{1: 'a', 2: 'b'}, 3`, `'it''s',2`, `"a,b", 2`) — a comma inside a nested value or a quoted
  string never splits (unit test `cqlite-cli/src/commands/find_tests.rs`).

#### Scenario: F7.2 a composite partition key is a hit end to end
- **Given** the Cassandra-written `test_basic.multi_partition_table` (partition key `(tenant_id, user_id)`)
  and a key present in its sstabledump golden
- **When** the real `cqlite find <table-dir> '<tenant>, <user>'` binary runs
- **Then** the generation is a `hit` (`cqlite-cli/tests/find_cli_tests.rs`). No committed fixture has a
  tuple/collection/UDT partition key, so nested literals are covered at unit level only.

#### Scenario: F7.3 nesting and quoting rules
- **Given** a `<partition-key>` argument
- **Then** `(`/`[`/`{` nest (tuple, list/set, map, UDT literals); `'...'` (with `''` escaped) and `"..."` text is
  opaque; each part is encoded with the partition-key codec against the schema column by position; a part count
  different from the schema's partition-key column count is a usage error (exit 1).
