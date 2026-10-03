# verify-location Specification

## Purpose
TBD - created by archiving change corruption-locator. Update Purpose after archive.

## Requirements

### Requirement: L1 — Located findings name their intersecting partitions

A `VerifyFinding` anchored to a `Data.db` byte range (`ChunkDecompressionError`, `UncompressedChunkCrcMismatch`, or a truncation-classified `ChunkOffsetOutOfBounds`) SHALL carry a `location.partitions` set equal to the partitions computed independently from the healthy source's boundary positions and chunk table, never from CQLite's own behaviour on the corrupt copy.

Deviation from this requirement's original draft (roborev round-3 MEDIUM finding): the truncation-anchored class is `ChunkOffsetOutOfBounds` (from `check_compression_info`'s declared-offset-vs-`Data.db`-length bounds check), not `DigestMismatch`/`UnexpectedEof` — verified directly against the real `test_comp_corrupt/data_db_truncation` fixture. `DigestMismatch` is a whole-file CRC with no per-chunk anchor and is never located; `cqlite-cli/tests/verify_location_cli_tests.rs` asserts its `location` is `null`.

#### Scenario: L1.1 compressed chunk CRC flip names the intersecting partitions
- **Given** `test_comp_corrupt/data_db_bit_flip` and its clean source `lz4_table`
- **When** the test computes, from the clean source's `Index.db` positions
  (`IndexReader::get_partition_entries`) and `CompressionInfo.db`'s `chunk_for_offset` chunk table,
  the partitions whose `Data.db` range intersects the flipped chunk, and `verify_sstable(dir,
  VerifyMode::Full, ..)` runs on the corrupt copy
- **Then** the `ChunkDecompressionError` finding's `location.chunk_index` equals the flipped chunk's
  index, `location.partitions` is `Resolved` and deep-equals the independently-computed set, and
  `location.component == "Data.db"`
  (`cqlite-core/tests/issue_4194_verify_location.rs`; corpus gating per #1094 —
  `CQLITE_REQUIRE_FIXTURES=1` hard-requires).

#### Scenario: L1.2 uncompressed chunk CRC flip uses the CRC.db grid
- **Given** `test_comp_corrupt/uncompressed_data_bit_flip` and its clean source `uncompressed_table`
- **When** the test computes the intersecting partition(s) from the clean source's `Index.db`
  positions and the chunk size recorded in the clean source's `CRC.db` header (64 KiB in this
  fixture, the CQLite-writer default — not a universal on-disk constant), and `verify_sstable` runs
  on the corrupt copy
- **Then** the `UncompressedChunkCrcMismatch` finding's `location` matches, with `component ==
  "Data.db"` and the chunk index derived from the `CRC.db` grid (not `CompressionInfo.db`, which this
  fixture has none of).

#### Scenario: L1.3 truncated Data.db names every partition past the new EOF
- **Given** `test_comp_corrupt/data_db_truncation`
- **When** the test computes, from the clean source's `Index.db` positions, every partition whose
  range extends past the corrupted file's actual size, and `verify_sstable` runs on the corrupt copy
- **Then** the `ChunkOffsetOutOfBounds` finding's `location.partitions` deep-equals that set (only the
  FIRST out-of-bounds chunk is located — the most inclusive range — per §L1's OOM-bound follow-up
  requirement below; every other `ChunkOffsetOutOfBounds` finding in the same report carries
  `location: None`).

#### DECLARED GAP — Scenario L1.4 (the decodable-but-corrupt needle partition, BIG and BTI) is NOT implemented
- **Given** `corrupt_byte_fixture::stage_control_and_mutated` for `BIG_COMPOSITE` (a text
  clustering-value byte flipped inside a compressed chunk, CRC recomputed, #3782)
- **Measured directly** (a throwaway probe against the real `BIG_COMPOSITE` fixture, recorded in
  `cqlite-core/tests/issue_4194_verify_location.rs`'s module doc): that mutation produces exactly one
  finding, `VerifyErrorClass::RowScanFailed` (an invalid-UTF-8 clustering-value decode failure
  surfacing through `classify_scan_error_class`'s generic fallback), carrying **no byte offset
  anywhere in its message/error chain**. `RowScanFailed` is not one of the three chunk/offset-anchored
  classes §L1 locates, and giving it a location would mean plumbing a byte offset out of the
  row-decode path — a new boundary-source-adjacent primitive this change's own non-goals rule out
  ("no new boundary-source primitive for BTI … the existing full trie walk is reused as-is").
  Tracked as a follow-up rather than implemented in this change.

#### DECLARED GAP — L1's `Resolved` outcome is NOT reachable for BTI (owner ruling, option (a))
- **Ruling** (issue #4194, 2026-10-02): where a BTI boundary source cannot be corroborated, the tool
  fails closed rather than presenting an uncorroborated set as `Resolved` — option (a) of three,
  chosen over narrowing this requirement's wording (option (c)). The cost was explicit and disclosed
  when the option was chosen: it removes the only BTI-`Resolved` case entirely.
- **Measured directly** — the loss is UNIVERSAL, not an occasional corner case. Every real BTI
  `PendingLocation`-producing corruption also fails the read path's own CRC check, so
  `bti_partition_identity_mismatch` (the sole corroborating cross-check) can never succeed for a
  genuinely corrupted BTI location; an attempt to construct a counterexample produced none.
  `location.partitions` is therefore always `Unresolved` for BTI.
- **Consequence for this requirement**: L1's SHALL is worded format-agnostically, but its `Resolved`
  outcome is satisfied on BIG only — scenarios L1.1–L1.3 are BIG fixtures throughout (`lz4_table`,
  `uncompressed_table`, `data_db_truncation`, all via `Index.db`). No scenario asserts a
  BTI-`Resolved` outcome, so the requirement's acceptance criteria remain satisfiable as written;
  what is NOT reachable is the BTI half of its prose promise. §L2's `Unresolved` path carries BTI
  instead, consistent with §D2's preference for a refused answer over a confident wrong one — but
  under its OWN cause, `BTI_IDENTITY_UNCORROBORATED` ("BTI partition-index leaves were not
  corroborated against Data.db..."), NOT the generic `boundary-source-unreadable`. The distinction
  is deliberate and test-pinned, because the two name different operator actions ("repair this
  component" vs "the trie was never cross-checked"): see
  `bti_compressed_chunk_crc_flip_refuses_uncorroborated_rows_offset_leaves`,
  `path3_chunk_offset_out_of_bounds_disables_its_own_guard_and_refuses`, and
  `path1_direct_boundary_finding_keeps_its_own_more_specific_cause`, which asserts that a DIRECT
  boundary finding keeps `boundary-source-unreadable` rather than being flattened into the
  corroboration cause.
- Surfacing an UNCORROBORATED BTI leaf set as a distinct, clearly-labelled outcome (a
  `ResolvedUncorroborated` corroboration state — option (b)) instead of refusing it is tracked as
  follow-up **#4337** (milestone 0.19), rather than implemented in this change.

### Requirement: L2 — A damaged boundary source poisons every location, never a guess

Every OTHER finding's `location.partitions` SHALL be `Unresolved` **under a NAMED cause** — never omitted, left empty, populated from a plausible-looking scan, or refused anonymously — whenever the report casts doubt on the format's boundary source. Trust is withdrawn on the UNION of the signals below: each is SUFFICIENT on its own, and none may be dropped in favour of another. Signals (1) and (2) are format-agnostic and both report `boundary-source-unreadable`:

1. **The finding's COMPONENT** is a boundary component — `Index.db` for BIG, `Partitions.db` or `Rows.db` for BTI — for ANY finding class. This covers the boundary component being ABSENT (`MissingComponent`, incl. the TOC critical-component check) or TRUNCATED (`UnexpectedEof` on a `Partitions.db` shorter than the mandatory 8-byte trie root footer), not merely structurally corrupt. It also covers the one genuine wrong-ANSWER case: with `Rows.db` absent, the BTI structural check still returns the `DataOffset` leaves it could read — a PARTIAL boundary list — which a class-only predicate would hand on and present as `Resolved`.
2. **The finding's CLASS** says the index structure is corrupt — `IndexEntryCorrupt` (BIG), `BtiRootPointerCorrupt`/`BtiTrieCorrupt` (BTI). This signal cannot be dropped in favour of (1): `BtiTrieCorrupt` is raised on component `Rows.db`, which this requirement did not originally name a boundary source at all.

3. **BTI only — the leaves were never CORROBORATED** against `Data.db`. A BTI leaf's identity is not one declaration (the trie emits a byte-comparable prefix; the raw key comes from the leaf's PAYLOAD), so a corruption that keeps a leaf's prefix while rewriting its payload resolves confidently to the WRONG key, and the FULL-mode identity cross-check is the only thing that can see it. That cross-check does not run in QUICK mode, when `compression_metadata_corrupt` is set, or when the scan fails — and `ChunkOffsetOutOfBounds` is pushed ALONGSIDE the very `PendingLocation` that needs resolving, so one corruption event both creates the location and disables its only guard. Cause: `BTI_IDENTITY_UNCORROBORATED`, deliberately DISTINCT from (1)/(2) because the operator action differs. Signal (1) is checked FIRST, so a direct boundary finding keeps its more specific cause.

Additionally, BIG withdraws trust when `IndexReader::is_fully_parsed()` reports a partial parse, since that reader silently truncates its partition list on the first malformed entry (#2302) and a partial prefix presented as `Resolved` is a confident wrong answer (cause: `boundary-source-unreadable`). Three further BIG refusals each carry their own cause rather than a generic one, so the operator learns WHAT disagreed: `BOUNDARY_SOURCE_OPEN_FAILED` (the real `io::Error` from opening `Index.db`, e.g. `Permission denied` — previously discarded), `BOUNDARY_ENTRY_ORDER_VIOLATION` (declared positions not strictly ascending in on-disk parse order), and `BOUNDARY_ENTRY_OFFSET_OUT_OF_BOUNDS` (a declared position at or past the declared logical length). The last two exist because a bit flip in a non-leading byte of a `position` vint leaves every entry PARSING cleanly — so no signal above fires — while silently dropping one partition and mis-attributing its bytes to a neighbour.

This requirement is satisfied by refusing in MORE cases than (1) and (2) alone, never fewer: every signal above withdraws trust, and none of them grants it.

The predicate is deliberately OVER-conservative where (1) and (2) overlap: a boundary component present on disk but unlisted in `TOC.txt` withdraws trust even though the component itself reads fine. §D2 prefers a refused answer to a confident wrong one.

#### Scenario: L2.1 corrupt BIG Index.db unresolves every location
- **Given** `test_comp_corrupt/index_db_bit_flip_big`
- **When** `verify_sstable(dir, VerifyMode::Full, ..)` runs
- **Then** the report's `IndexEntryCorrupt` finding is present (unchanged from today), and every
  OTHER finding's `location.partitions == Unresolved("boundary-source-unreadable")`
  (`cqlite-core/tests/issue_4194_verify_location.rs`).

#### Scenario: L2.2 corrupt BTI boundary source unresolves every location
- **Given** `test_comp_corrupt/bti_rows_truncation`, staged as a copy whose own `Data.db` is then
  bit-flipped so a second, `Data.db`-anchored finding exists to assert `Unresolved` on
- **When** `verify_sstable` runs
- **Then** as L2.1, for the BTI `BtiTrieCorrupt` findings.
- **Note** `bti_partitions_footer_flip` is NOT usable here, as an earlier draft's Given assumed:
  that fixture is only DETECTED via the FULL-mode `Data.db`-scan identity cross-check
  (`bti_partition_identity_mismatch`), which never runs once `Data.db` is ALSO corrupted (the scan
  itself errors first) — measured directly, staging that combination yields no BTI boundary-source
  finding at all. `bti_rows_truncation`'s findings are raised structurally in
  `check_bti_structure` BEFORE the chunk-CRC check and the scan, so they survive pairing with an
  independent `Data.db` corruption.
- **Note** boundary-source distrust is a UNION of the finding CLASS and the finding COMPONENT, so
  a boundary component that is ABSENT (`MissingComponent`) or truncated (`UnexpectedEof`) — not
  merely structurally corrupt — also unresolves every location; see §L2's own requirement text.

#### Scenario: L2.3 a healthy boundary source with no other finding never fabricates an unresolved marker
- **Given** any committed clean `test_basic`/`test_comp` fixture
- **When** `verify_sstable` runs
- **Then** `report.findings` is empty (as today) — `Unresolved` never appears on a report with zero
  findings, since there is no finding to attach a location to.

### Requirement: L3 — No header hunting

Partition resolution SHALL read only the boundary source's own decoded entries and the
`CompressionInfo.db`/`CRC.db` chunk tables; it SHALL NOT locate a partition by scanning `Data.db`
bytes for a plausible header.

#### Scenario: L3.1 static no-resync-scan guard
- **Given** `scripts/tests/test_verify_location_no_resync_scan.sh` (the gate's UNSCOPED
  `roborev-lints` component, so a `cqlite-core`-only diff cannot skip it — #4266)
- **When** it greps every PRODUCTION file of the `verify_location` module for `memchr`,
  `windows(`, `find(|b|`, `position(|b|` outside `#[cfg(test)]`. The file set is DISCOVERED, not
  hard-pinned to one name (roborev Medium #2): flat `verify_location*.rs` siblings AND, if it
  exists, every `*.rs` under a `verify_location/` module directory — so a future campsite-rule
  split cannot move code out from under the guard while it still reports "0 hits RECOGNISED".
  `*_tests.rs` siblings and `*/tests/*` paths are excluded as test code.
- **Then** none is present; any hit FAILs naming the line. A file set that resolves EMPTY (the
  module renamed or moved away) is a REFUSAL, not a pass, and the `ok` line states how many
  production files were scanned and names them — an affirmative count, so a guard that swept
  nothing cannot read identically to one that swept everything and found nothing.

### Requirement: L4 — Additive report shape; the existing parity guard is unaffected

`VerifyFinding`/`VerifyReport`'s pre-existing fields, `Display` output, and CLI text/JSON rendering SHALL be unchanged by this addition. `location` SHALL be absent from the text rendering unless present; in JSON the `location` key is always present on every finding object (matching the existing `rows_scanned`/`toc_components` convention of an always-present, `null`-when-absent field) and its VALUE SHALL be `null` unless a location was resolved for that finding (roborev round-1 clarification: "only when present" in an earlier draft of this requirement was ambiguous between "key omitted" and "value null" — the implementation, matching every other optional field this report already emits, is the latter).

#### Scenario: L4.1 the existing corruption parity suite is unaffected
- **Given** `cqlite-core/tests/sstable_parity_corruption_verify.rs` (issue #1236, unmodified by this
  change)
- **When** the full gate runs it against the committed corpus
- **Then** every existing class/verdict assertion still passes — this change adds a field, it never
  alters `VerifyErrorClass` classification or the corrupt/clean verdict.

#### Scenario: L4.2 CLI renders location in text and json
- **Given** the built binary and `test_comp_corrupt/data_db_bit_flip`
- **When** `cqlite verify <dir> --mode full --out text` and `--out json` both run
- **Then** the text output names the chunk index and at least one partition key, and the JSON output
  carries a `location` object on the corresponding finding with `chunk_index`, `byte_offset` and a
  `partitions` array — while a finding with no `location` (e.g. `MissingComponent`) renders exactly
  as it did before this change (`cqlite-cli/tests/verify_location_cli_tests.rs`, named in the gate's
  `cli-tests` list per #3522 — no manual edit needed since `cli-tests` enumerates the
  `cqlite-cli/tests/*.rs` glob).

### Requirement: L6 — A physical range is never presented as damage it is not (roborev job 92 MEDIUM)

`Location.byte_offset`/`byte_len` SHALL NOT be presented as a damaged byte extent when the range is
merely one the SSTable's own metadata DECLARES and the file does not satisfy. Every `Location` SHALL
carry a `PhysicalAnchor` stating which of the two it is, and BOTH the text renderer and the JSON
output SHALL disclose it. For `ChunkOffsetOutOfBounds` the declared chunk offset lies past EOF, so
its anchor is `DeclaredRecord` and the damaged extent is the LOGICAL range `partitions` enumerates;
for a chunk CRC/decompression failure the bytes are present and damaged, so its anchor is
`DamagedExtent`.

#### Scenario: L6.1 a truncation's location is labelled declared, not damaged
- **Given** `test_comp_corrupt/data_db_truncation`
- **When** `verify_sstable(dir, VerifyMode::Full, ..)` runs
- **Then** the `ChunkOffsetOutOfBounds` finding's `location.anchor` is `DeclaredRecord`, and
  `format_location` renders `declared offset 0x…` together with an explicit statement that the range
  is `the record does not fit within the file` — so an operator is never told that 4 bytes are damaged at an offset
  where `dd`/`xxd` returns nothing
  (`cqlite-core/tests/issue_4194_verify_location.rs`'s `l1_3_…`, and `l5_1_…` for the capped case).

#### Scenario: L6.2 a real damaged extent keeps the damaged reading (positive control)
- **Given** `test_comp_corrupt/data_db_bit_flip` (a compressed chunk CRC flip)
- **When** the same verify runs
- **Then** that finding's `location.anchor` is `DamagedExtent` and the rendered line does NOT read
  `declared offset` — without this control, classifying EVERY location as declared would satisfy
  L6.1 (`cqlite-core/tests/issue_4194_verify_location.rs`'s `l1_1_…`).

#### Scenario: L6.3 the JSON channel discloses the anchor too
- **Given** the built binary on a truncated fixture
- **When** `cqlite verify <dir> --mode full --out json` runs
- **Then** each `location` object carries `"anchor": "damaged_extent" | "declared_record"` —
  disclosing in the machine channel exactly what the text channel discloses, since a JSON consumer
  reads the same two physical fields (`cqlite-cli/src/commands/verify.rs`'s location writer).

### Requirement: L5 — Resolved partition sets are bounded (roborev round-2 MEDIUM finding)

`location.partitions`'s `Resolved` set SHALL NOT grow unbounded: a truncation's damaged range can
intersect essentially every partition in a large table, so `PartitionResolution::Resolved` SHALL cap
the materialized key list at a fixed limit (`MAX_RESOLVED_KEYS = 100`) and SHALL carry an explicit
count of how many additional intersecting partitions were not materialized (`0` when nothing was
omitted), bounded DURING accumulation, not only in the final output.

#### Scenario: L5.1 a badly truncated table's resolved set is capped, not unbounded
- **Given** a boundary source with more than `MAX_RESOLVED_KEYS` partitions all intersecting one
  finding's damaged range
- **When** `resolve_partitions` resolves that finding's location
- **Then** `location.partitions` is `Resolved` with exactly `MAX_RESOLVED_KEYS` keys and a `truncated`
  count naming how many more intersected
  (`cqlite-core/src/storage/sstable/verify_location.rs`'s
  `resolved_set_is_capped_and_names_the_truncated_count`).
- **And** the same cap is observed END TO END through the PUBLIC `verify_sstable` surface, not only
  through the crate-internal resolver: `cqlite-core/tests/issue_4194_verify_location.rs`'s
  `l5_1_resolved_set_is_capped_and_names_the_omitted_count_end_to_end` copies the real
  Cassandra-written `test_basic.simple_table` generation (**1000 partitions** — 10x the cap) to a
  tempdir, truncates the COPY's `Data.db` to its first compressed chunk, and asserts the resulting
  `ChunkOffsetOutOfBounds` finding's location against this test file's own independent
  `Index.db`/`CompressionInfo.db` oracle. MEASURED: **977** intersecting partitions, **100**
  materialized, **truncated = 877** — the two halves sum to the oracle's total, so a silently
  dropped entry would fail the case rather than pass it. No synthetic fixture and no mutation of
  the shared corpus is involved (the clean generation is copied first, as L2.1/L2.2 do).
