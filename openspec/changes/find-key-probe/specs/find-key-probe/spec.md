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

`GenerationProbe::Hit` SHALL carry the partition's `Data.db` offset, its byte length (or `ToEof` for the last partition), the maximum writetime among its decoded rows and any partition-level deletion, and whether it carries a partition deletion — all derived from the SAME decode the probe already performs.

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

### Requirement: F4 — `unflushed` is a fail-closed comparison

`unflushed` SHALL be `yes` when a mutation's writetime exceeds the maximum writetime any generation holds for the key, `no` otherwise, and `unmeasured(<cause>)` whenever either side of the comparison is itself unmeasured — never a guessed yes/no.

#### Scenario: F4.1 flushed mutations report unflushed=no
- **Given** the fixture from F3.1
- **When** `find --commitlog` runs
- **Then** every mutation whose writetime is at or below the max generation writetime for the key
  reports `unflushed: no`.

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

### Requirement: F5 — Exit codes and the zero-generations case

`cqlite find` SHALL exit `0` when the report completed (including "0 generations hold this key"), `1` on a usage error, and `2` when any generation or CommitLog segment could not be read at all — never a shorter report standing in for a read failure.

#### Scenario: F5.1 a key absent everywhere still exits 0 with a full report
- **Given** a key absent from every generation of a table with no `--commitlog`
- **When** `cqlite find <table-dir> <key>` runs
- **Then** exit `0`, and every generation is listed with its `BloomNegative`/`IndexMiss`/`Scanned(Miss)`
  result — `cqlite-cli/tests/find_cli_tests.rs`, named in the gate's `cli-tests` list per #3522.

#### Scenario: F5.2 an unreadable generation is a named exit-2 failure
- **Given** a table directory with one generation whose `Data.db` cannot be opened
- **When** `cqlite find` runs
- **Then** exit `2`, and the failing generation is named in the output — never omitted.

### Requirement: F6 — No header hunting

Every probe and every CommitLog field SHALL be derived from a decoded, authoritative structure — `Filter.db`, `Index.db`/the BTI trie, `Data.db`'s parsed partition, or the CommitLog's own `EncodingStats`/delta-encoded fields — and the fail-safe scan path SHALL be a full schema-driven decode, never a byte-pattern resync search.

#### Scenario: F6.1 static no-resync-scan guard
- **Given** a script mirroring #4194/#4195's `test_*_no_resync_scan.sh` pattern (`tooling-tests`)
- **When** it greps the new `find_probe` module for `memchr`, `windows(`, `find(|b|`, `position(|b|`
  outside `#[cfg(test)]`
- **Then** none is present; any hit FAILs naming the line.
