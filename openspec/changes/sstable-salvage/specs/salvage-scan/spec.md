# salvage-scan — new capability (sstable-salvage, issue #4196)

`cqlite-core` SHALL recover every completely-decodable partition of a damaged SSTable into a fresh
uncompressed generation and account for every partition it could not. All requirements are ADDED.

> **DEFERRED SCENARIOS (issue #4196, roborev finding — not implemented in the #4196 PR, tracked as
> follow-up work, NOT satisfied-by-reference despite being named below):** R2.2 (uncompressed chunk
> CRC flip), R2.3 (truncated Data.db), R4.2 (key-at-offset-disagrees-with-index staging), R5.1
> (input-sha256-unchanged listing), R6.1 (memory-budget lane entry). R1.1/R1.2, R2.1, R2.4, R3.1,
> R4.1, R4.3 and R5.2 are implemented and pass against real fixtures
> (`cqlite-core/tests/issue_4196_salvage_healthy_parity.rs`,
> `issue_4196_salvage_corruption_corpus.rs`, `issue_4196_salvage_partition_atomicity.rs`,
> `scripts/tests/test_salvage_no_resync_scan.sh`) — R3.1's fixture demonstrates the zero-output-rows
> safety property unconditionally. Round 20 (roborev) proved the scenario text's original
> `rows_decoded_before_failure >= 2` clause UNREACHABLE by construction (an exhaustive scan of a
> real multi-row partition), and round 21 removed the manifest field it referred to rather than
> ship one that could only ever read `0` — see R2.4/R3.1 below and
> `issue_4196_salvage_partition_atomicity.rs`'s module doc for the full derivation, and issue #4218
> for reinstating a real count if that ever becomes possible.
>
> **R1.1's SECOND half (dump parity with the `*-Data.db.jsonl` golden) is implemented for the BTI
> `da` case ONLY**, in `salvage_of_healthy_bti_sstable_preserves_every_row` (C-audit on this issue:
> the BTI case previously compared CQLite's decode of the input against CQLite's decode of CQLite's
> own output and nothing else — a symmetric round trip, invariant to a uniform framing error per
> CLAUDE.md's #3042 blind spot, so it could not validate an on-disk property at all). The BIG cases
> in that file assert the FIRST half (byte parity against `compact_sstables`, itself byte-parity
> -proven against Cassandra under #1017) and do NOT compare against their goldens; R1.1's literal
> "every committed table under `test_basic`, `test_collections`, `test_tomb` and `test_da`" sweep
> remains a per-case selection of four fixtures, not a corpus-wide one. Both are declared here
> rather than left to be inferred from a green suite.

## ADDED Requirements

### Requirement: R1 — Salvage of a healthy SSTable equals a no-purge compaction of it

`salvage_sstable` SHALL produce, for an input that verifies clean, output byte-identical to
`compact_sstables` over that single input with purging disabled, and a report whose loss list is
empty in the affirmative form.

#### Scenario: R1.1 byte parity with compaction and dump parity with the golden
- **Given** every committed table under `test_basic`, `test_collections`, `test_tomb` and `test_da`
  (BIG compressed, BIG uncompressed where present, BTI), roots resolved per table
- **When** `salvage_sstable` and `compact_sstables(purge_safe=false, gc_before=None, now=None)`
  each run into a temp dir
- **Then** every output component is byte-equal between the two, and the salvage output's
  compaction-row dump equals the fixture's `*-Data.db.jsonl` golden
  (`cqlite-core/tests/issue_4196_salvage_healthy_parity.rs`, per-case assertion, fail-closed on a
  missing committed fixture).

#### Scenario: R1.2 affirmative empty loss list
- **When** R1.1's salvage runs
- **Then** `report.losses` is empty AND `report.partitions.total == recovered > 0`, and the text
  rendering reads `losses: 0 RECOGNISED` — a report with `total == 0` on a table with rows FAILs.

### Requirement: R2 — Losses are exactly the partitions the format says are untrustworthy

For a damaged input, the set of lost partitions SHALL equal the set derived from the healthy
source's boundary positions and chunk table, and every other partition SHALL be recovered
intact.

#### Scenario: R2.1 compressed chunk CRC flip
- **Given** `test_comp_corrupt/data_db_bit_flip` and its clean source
- **When** the test computes, from the clean source's `Index.db` positions and `CompressionInfo.db`
  chunk table, the partitions whose byte range intersects the flipped chunk, and salvage runs on
  the corrupt copy
- **Then** `report.losses` keys == that set with class `chunk-crc`, the output dump == golden minus
  that set, and `report.component_findings` contains `ChunkDecompressionError`
  (`cqlite-core/tests/issue_4196_salvage_corruption_corpus.rs`; skip-clean when the corpus is
  absent, FAIL present-but-wrong, hard-required under `CQLITE_REQUIRE_FIXTURES=1`).

#### Scenario: R2.2 uncompressed chunk CRC flip
- **Given** `test_comp_corrupt/uncompressed_data_bit_flip`
- **When** salvage runs
- **Then** as R2.1 using `CRC.db`'s chunk size, finding class `UncompressedChunkCrcMismatch`.

#### Scenario: R2.3 truncated Data.db
- **Given** `test_comp_corrupt/data_db_truncation`
- **When** salvage runs
- **Then** every partition whose range extends past EOF is a loss with class `truncated`, all
  earlier partitions are recovered, and the dump equals golden minus the truncated set.

#### Scenario: R2.4 decodable-but-corrupt row, BIG and BTI
- **Given** `corrupt_byte_fixture::stage_control_and_mutated` for `BIG_COMPOSITE` and
  `BTI_MULTICLUSTERING` (one byte flipped inside a compressed chunk, CRC recomputed)
- **When** salvage runs on `mutated`
- **Then** exactly the partition holding the needle is lost with class `decode`, every other
  partition's dump equals `control`'s
  (`cqlite-core/tests/issue_4196_salvage_partition_atomicity.rs`).

### Requirement: R3 — A partition is recovered whole or not at all

Salvage SHALL NOT write any row of a partition whose decode fails at any row.

#### Scenario: R3.1 no prefix of a lost partition in the output
- **Given** R2.4's mutated BIG fixture
- **When** salvage runs
- **Then** the output holds zero rows for that partition key (asserted by key seek on the output),
  and the manifest names the loss with class `decode`. This holds BY CONSTRUCTION (design D2, round
  20-21): the decode-at-offset path buffers a partition's rows locally and forwards them to the
  writer only on structural completion, so a mid-partition failure is reached with nothing
  externally visible to accidentally write — never asserted via a row-count field (issue #4218
  tracks reinstating one if the underlying buffering ever becomes incremental).

### Requirement: R4 — Boundaries come only from an authoritative source; otherwise refuse

Salvage SHALL enumerate partitions from `Index.db` (BIG) or the `Partitions.db` trie (BTI) and
SHALL refuse, writing no `Data.db`, when that source is unreadable; it SHALL NOT locate partitions
by scanning bytes for a plausible header.

#### Scenario: R4.1 damaged boundary source refuses with the rebuild remedy
- **Given** `test_comp_corrupt/index_db_bit_flip_big`, `bti_partitions_footer_flip`,
  `bti_rows_truncation`
- **When** salvage runs on each
- **Then** `report.refused.reason == boundary-source-unreadable`, `remedy` names `rebuild`
  (#4197), and `--out` contains no `Data.db`.

#### Scenario: R4.2 key at offset disagrees with the index
- **Given** a temp copy of a healthy BIG fixture with one `Index.db` entry's position pointed at
  a different partition's header
- **When** salvage runs
- **Then** that slot is a loss with class `key-mismatch`; the partition the offset actually
  holds is still recovered from its OWN index entry exactly once.

#### Scenario: R4.3 no header hunting
- **Given** `scripts/tests/test_salvage_no_resync_scan.sh` (`tooling-tests`)
- **When** it greps `cqlite-core/src/storage/write_engine/salvage/` for any byte-pattern search
  primitive (`memchr`, `windows(`, `find(|b|`, `position(|b|`) outside tests
- **Then** none is present; any hit FAILs naming the line.

### Requirement: R5 — The input is never modified and nothing is recovered from nothing

Salvage SHALL open its input read-only and MUST refuse, writing no `Data.db`, when no partition decodes.

#### Scenario: R5.1 input sha256 listing unchanged
- **Given** a recursive sha256 listing of the input dir before every scenario above
- **Then** the listing is identical afterwards.

#### Scenario: R5.2 nothing decodable refuses
- **Given** a temp copy of `data_db_bit_flip` with every chunk's CRC trailer zeroed
- **When** salvage runs
- **Then** `refused.reason == nothing-decodable`, every partition is in `losses`, no `Data.db`.

### Requirement: R6 — Bounded memory

Salvage SHALL hold at most one partition resident on the read side and one on the write side.

#### Scenario: R6.1 wide partitions under the budget lane
- **Given** `test_wide_rows` (every table) under the gate's `memory-budget` component (dhat)
- **When** salvage runs
- **Then** peak heap stays within the existing lane threshold for a single-input compaction of
  the same table.
