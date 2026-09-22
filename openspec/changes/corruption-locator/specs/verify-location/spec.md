# verify-location — new capability (corruption-locator, issue #4194)

`cqlite-core` SHALL extend every `VerifyFinding` that has a natural byte range with the partitions
that range intersects, derived only from the format's own authoritative boundary sources, and SHALL
name the reason rather than guess when those sources cannot be trusted. All requirements are ADDED.

## ADDED Requirements

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
  positions and the fixed 64 KiB `CRC.db` chunk grid, and `verify_sstable` runs on the corrupt copy
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

### Requirement: L2 — A damaged boundary source poisons every location, never a guess

Every OTHER finding's `location.partitions` SHALL be `Unresolved("boundary-source-unreadable")` — never omitted, left empty, or populated from a plausible-looking scan — whenever `Index.db` (BIG) or `Partitions.db` (BTI) is itself the corrupt component named by a finding in that report.

#### Scenario: L2.1 corrupt BIG Index.db unresolves every location
- **Given** `test_comp_corrupt/index_db_bit_flip_big`
- **When** `verify_sstable(dir, VerifyMode::Full, ..)` runs
- **Then** the report's `IndexEntryCorrupt` finding is present (unchanged from today), and every
  OTHER finding's `location.partitions == Unresolved("boundary-source-unreadable")`
  (`cqlite-core/tests/issue_4194_verify_location.rs`).

#### Scenario: L2.2 corrupt BTI boundary source unresolves every location
- **Given** `test_comp_corrupt/bti_partitions_footer_flip` and `bti_rows_truncation`
- **When** `verify_sstable` runs on each
- **Then** as L2.1, for the BTI `BtiRootPointerCorrupt`/`BtiTrieCorrupt` findings.

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
- **Given** `scripts/tests/test_verify_location_no_resync_scan.sh` (`tooling-tests`)
- **When** it greps `cqlite-core/src/storage/sstable/verify_location.rs` for `memchr`, `windows(`,
  `find(|b|`, `position(|b|` outside `#[cfg(test)]`
- **Then** none is present; any hit FAILs naming the line.

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
