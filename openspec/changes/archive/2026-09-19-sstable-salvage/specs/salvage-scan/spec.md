# salvage-scan — new capability (sstable-salvage, issue #4196)

`cqlite-core` SHALL recover every completely-decodable partition of a damaged SSTable into a fresh
uncompressed generation and account for every partition it could not. All requirements are ADDED.

> **DEFERRED AND SCOPED SCENARIOS (issue #4196) — corrected in BOTH directions by the C intent audit
> on this issue, which found this block simultaneously under-claiming two scenarios and over-claiming
> one. A note that misreports coverage is worse than no note, because it is what stops the next
> reader looking.**
>
> **IMPLEMENTED, with a declared construction deviation:**
>
> * **R2.2** (uncompressed chunk CRC flip) is **implemented** —
>   `issue_4196_salvage_corruption_corpus.rs::uncompressed_chunk_crc_flip_loses_exactly_the_intersecting_partitions`.
>   It does NOT use the scenario's named `test_comp_corrupt/uncompressed_data_bit_flip`: it flips one
>   byte inside chunk 0 of a temp copy of clean `test_comp.uncompressed_table`, deriving the expected
>   loss set from that source's own `Index.db` positions and the real `CRC.db`-declared chunk size,
>   and asserts the `UncompressedChunkCrcMismatch` finding. Because it needs that table's `CRC.db`,
>   which is NOT git-tracked, the case is skippable (hard under `CQLITE_REQUIRE_FIXTURES=1`, which the
>   gate's dataset lanes set). The named corpus fixture IS fully committed, so switching to it would
>   make this case fail-closed; that swap is NOT a fixture substitution alone — the committed fixture
>   carries its flip pre-applied at a byte the test does not know (chunk **1**, per
>   `corruption-manifest.yml`'s `byte_offset: 70000`) and holds a single partition spanning every
>   chunk, so the expected-loss derivation, the skip block and the partial-vs-total assertions all
>   change with it. Carried as a follow-up line on **#4229** rather than done here.
> * **R4.2** (key at offset disagrees with the index) is **implemented** —
>   `issue_4196_salvage_corruption_corpus.rs::swapped_index_entry_keys_classify_key_mismatch`. It does
>   NOT use the scenario's literal offset-repoint construction, which is unreachable:
>   `enumerate_boundaries`' `check_strictly_ascending` guard refuses the whole generation before any
>   slot is classified. The equivalent construction swaps two adjacent entries' `[key_len][key]` spans
>   and leaves every `data_offset` untouched, so both slots decode their true partition against a
>   wrong declared key and both classify `key-mismatch`; a third, entirely untouched entry is the
>   control for the scenario's "the partition is still recovered from its OWN index entry exactly
>   once".
> * **R2.4** is implemented for **BOTH** format families the scenario names. The BIG (`nb`) leg is
>   `issue_4196_salvage_partition_atomicity.rs::corrupt_row_loses_the_needle_partition_whole_never_a_prefix`
>   (`BIG_COMPOSITE` / `test_basic.composite_key_table`, fetched corpus, skippable); the BTI (`da`)
>   leg is `::corrupt_row_in_a_bti_partition_loses_it_whole_never_a_prefix` (`BTI_MULTICLUSTERING` /
>   `test_da.multiclustering_table`, FULLY git-tracked, so fail-closed unconditionally). The C audit
>   found the BTI leg absent while this block listed R2.4 as passing — the `da`
>   decode-at-offset path had no partition-atomicity coverage at all, which matters because
>   `bti_scan_with_metadata_cancellable` reaches the row parse by a different route than BIG's
>   `sequential_scan` (issue #3782). Declared deviation: BTI has no `Index.db`, so that leg derives
>   the needle partition's identity from the committed `*-Data.db.jsonl` `sstabledump` golden's own
>   per-partition `position` — Cassandra's own record of the uncompressed data-file offset — rather
>   than from a re-implementation of the `Partitions.db` trie.
>
> **NOT IMPLEMENTED:**
>
> * **R2.3** (truncated `Data.db`) is **NOT implemented** and is **NOT satisfied by reference**. The
>   corpus's only truncation fixture, `test_comp_corrupt/data_db_truncation`, is COMPRESSED, and
>   `compressed_chunk_preflight` walks every declared chunk before the per-partition loop runs — a
>   chunk whose bytes no longer exist fails to READ and lands in `bad_chunks`, so the loop's
>   `chunk-crc` short-circuit fires and `LossClass::Truncated`'s own path is unreachable through it. A
>   naive uncompressed byte-truncation has the same problem one layer down (a mid-row cut is a hard
>   decode `Err` → `LossClass::Decode`). `Truncated` is reached ONLY via
>   `decode_partition_at_offset_for_salvage`'s early `offset_usize >= end` check. The class IS pinned,
>   by that route, in
>   `issue_4196_salvage_oom_bounds.rs::index_entry_offset_past_eof_classifies_truncated`; the
>   scenario's own fixture and its "dump equals golden minus the truncated set" assertion are not.
>   The C audit's N5 recorded that this deferral previously named NO tracking issue anywhere, which is
>   indistinguishable from an oversight six weeks on — it is carried as a follow-up line on **#4229**.
> * **R6.1** (memory-budget dhat lane entry) — owner ruling 2026-09-13, tracked as **#4229**.
>
> **R1.1, R1.2, R2.1, R3.1, R4.1 (scoped — see below), R4.3 and R5.2 are implemented and pass against
> real fixtures** (`cqlite-core/tests/issue_4196_salvage_healthy_parity.rs`,
> `issue_4196_salvage_corruption_corpus.rs`, `issue_4196_salvage_partition_atomicity.rs`,
> `issue_4196_salvage_output_input_contracts.rs`, `issue_4196_salvage_round15_bounds.rs`,
> `scripts/tests/test_salvage_no_resync_scan.sh`) — R3.1's fixture demonstrates the zero-output-rows
> safety property unconditionally, on both format families. Round 20 (roborev) proved the scenario
> text's original `rows_decoded_before_failure >= 2` clause UNREACHABLE by construction (an exhaustive
> scan of a real multi-row partition), and round 21 removed the manifest field it referred to rather
> than ship one that could only ever read `0` — see R2.4/R3.1 below and
> `issue_4196_salvage_partition_atomicity.rs`'s module doc for the full derivation, and issue #4218
> for reinstating a real count if that ever becomes possible.
>
> **R4.1's fixture set is SCOPED to one of the three it names (C audit).** `index_db_bit_flip_big` is
> driven at BOTH layers — `issue_4196_salvage_corruption_corpus.rs::damaged_index_db_refuses_with_the_rebuild_remedy`
> and `salvage_cli_tests.rs::refusal_exit_2_no_data_db_written`. `bti_partitions_footer_flip` and
> `bti_rows_truncation` are exercised by NO #4196 test; the BTI refusal arm is covered instead by a
> SUBSTITUTE, `::damaged_bti_rows_db_missing_refuses_with_the_rebuild_remedy`, which removes `Rows.db`
> from the committed `test_da.multiclustering_table` while `Partitions.db` still references a
> `RowsOffset` leaf. The substitution is deliberate and is the stronger choice: neither named BTI
> corpus fixture has its `*.db` binaries git-tracked (only `Digest.crc32` and `TOC.txt` are), so a
> test on them would skip-clean on any unfetched checkout, whereas the substitute fails closed on
> committed bytes. Declared here rather than left to be inferred from a green suite.
>
> **R5.2's three clauses are pinned TOGETHER and unconditionally (C audit N6).**
> `issue_4196_salvage_round15_bounds.rs::losses_beyond_the_cap_are_counted_not_resident` asserts
> `RefusalReason::NothingDecodable`, no `Data.db` anywhere under `--out`, and a D5 manifest still
> naming the refusal in its kebab-case vocabulary with a non-empty remedy — on a git-tracked fixture,
> outside any data-dependent branch. Previously those clauses were covered only in aggregate, with
> `reason == NothingDecodable` asserted solely inside an `if expected_lost.len() ==
> clean_positions.len()` in two corpus-gated tests. The scenario's stated fixture (a `data_db_bit_flip`
> copy with every chunk's CRC trailer zeroed) is still not built.
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
>
> **R5.1 is implemented, with its SWEEP scoped (C-audit on this issue).**
> `cqlite-core/tests/issue_4196_salvage_output_input_contracts.rs::salvage_never_modifies_a_byte_of_its_input`
> takes a per-file SHA-256 census of the input generation directory before and after a real salvage
> run and asserts the set AND every digest unchanged, reporting a violation by component name. Its
> scope is the two COMMITTED compressed fixtures (`test_comp.lz4_table` BIG/`nb`,
> `test_da.multiclustering_table` BTI/`da`), both fail-closed per case — NOT the scenario's literal
> "before every scenario above": the corruption lanes do not each carry their own digest census.
> Salvage's production sources contain no write primitive aimed at the input, so this pins a property
> that already holds rather than fixing a defect. The same target also pins the issue **#1406**
> write-surface boundary (design D4), which had no test at all:
> `salvaged_output_never_contains_a_compression_info_db` asserts a compressed input's `--out` tree
> holds no `*-CompressionInfo.db` anywhere, with the compressed-input premise and the
> a-generation-was-actually-written premise both asserted so it cannot pass vacuously.

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

#### Scenario: R1.3 a stale caller schema is normalized from the input's OWN header

Roborev, issue #4196, round-23 High finding, confirmed by an independent Cassandra/SSTable-format
expert review with a working reproduction. `compact_sstables` normalizes the caller's schema against
the input SSTable headers BEFORE decode — `effective_compaction_schema` then
`apply_udt_marshals_from_inputs` — and `salvage_sstable` did NEITHER, so a `--schema` disagreeing
with the on-disk serialization header decoded against the wrong layout and re-encoded with a
divergent header while the manifest reported every partition `recovered`. For a recovery tool a
confidently-clean manifest over silently wrong bytes is the worst failure shape, and salvage is
one-shot: there is no second chance once the input is gone.

The reproduced instance needed **no UDT column at all** — a hand-written schema simply OMITTING
`static_data TEXT STATIC` produced an output whose header declared that column NOWHERE (8981 bytes
against compaction's 10432), at exit `0`, reporting `recovered=100 lost=0`. So the two skipped calls
are INDEPENDENT hazards: static columns present in the input header but absent from the caller's
schema, and UDT marshal shape. Neither may be closed alone.

- **Given** the committed Cassandra 5.0-written `test_basic.static_columns_table` and a caller
  `TableSchema` from which its static column has been REMOVED
- **When** `salvage_sstable` and `compact_sstables` each run over that input with THAT schema
- **Then** the two agree on the output serialization header's column set, the omitted static column
  is present in both — the expectation read out of the INPUT's own `Statistics.db` header, never
  from CQLite's prior salvage behavior (#3042: a CQLite-written + CQLite-read round-trip is
  INVARIANT to this defect class and cannot serve as its oracle) — and every decoded row is equal
  (`salvage_with_a_stale_schema_matches_compaction_on_the_effective_column_set` in
  `cqlite-core/tests/issue_4196_salvage_effective_schema.rs`; per-case, hard-fails under
  `CQLITE_REQUIRE_FIXTURES=1`).
- **And** the normalized schema reaches BOTH the decoder and the output writer, so a normalized
  decode feeding an unnormalized writer is impossible by construction rather than by discipline.
- **And** a normalization that CANNOT be completed is a classified REFUSAL that still carries the
  manifest — `report.refused` names the component and the cause — never a silent success and never a
  bare error that discards the losses and findings already gathered.
- **And** a self-healed run stays exit `0`, exactly as `compact` does: the
  `SchemaNormalizedFromHeader` finding it records is operator-visible in the manifest but is NOT a
  verification gap.

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
