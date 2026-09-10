# verify-location — new capability (corruption-locator, issue #4194)

`cqlite-core` SHALL extend every `VerifyFinding` that has a natural byte range with the partitions
that range intersects, derived only from the format's own authoritative boundary sources, and SHALL
name the reason rather than guess when those sources cannot be trusted. All requirements are ADDED.

## ADDED Requirements

### Requirement: L1 — Located findings name their intersecting partitions

A `VerifyFinding` anchored to a `Data.db` byte range (`ChunkDecompressionError`, `UncompressedChunkCrcMismatch`, or a truncation-classified `DigestMismatch`/`UnexpectedEof`) SHALL carry a `location.partitions` set equal to the partitions computed independently from the healthy source's boundary positions and chunk table, never from CQLite's own behaviour on the corrupt copy.

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
- **Then** the truncation-classified finding's `location.partitions` deep-equals that set.

#### Scenario: L1.4 the decodable-but-corrupt needle partition, BIG and BTI
- **Given** `corrupt_byte_fixture::stage_control_and_mutated` for `BIG_COMPOSITE` and
  `BTI_MULTICLUSTERING` (one byte flipped inside a compressed chunk, CRC recomputed, #3782)
- **When** `verify_sstable` runs on `mutated`
- **Then** the resulting finding's `location.partitions` is `Resolved` with exactly the needle
  partition (the one holding the flipped clustering-key byte), verified against
  `index_partition_positions(control)` (BIG) / the BTI trie walk over `control` (BTI).

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

`VerifyFinding`/`VerifyReport`'s pre-existing fields, `Display` output, and CLI text/JSON rendering SHALL be unchanged by this addition, and `location` SHALL render in the CLI's text and JSON output only when present.

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
