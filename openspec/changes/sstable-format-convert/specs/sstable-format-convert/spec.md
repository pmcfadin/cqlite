# sstable-format-convert — new capability (issue #4202)

`cqlite-core` SHALL expose a per-generation BIG↔BTI format conversion that preserves every cell,
tombstone and Statistics field the target format can carry, and writes nothing when it cannot
preserve something silently. All requirements are ADDED.

## ADDED Requirements

### Requirement: R1 — Conversion preserves reconciliation output exactly (no-purge, 1:1)

`convert_sstable_generation` SHALL decode ONE input generation completely through the same
single-input, no-purge reconciliation path `salvage`'s R1 oracle uses (`purge_safe(false)`,
`gc_before_secs: None`, `now_secs: None`), and write the result through
`SSTableWriter::with_format` at the requested target format. It SHALL NOT merge across generations
and SHALL NOT purge or drop any tombstone.

#### Scenario: R1.1 round trip is content-lossless, both directions
- **Given** `cqlite-core/tests/issue_4202_convert_roundtrip.rs`, every committed `test_basic`
  and `test_da` table fetched under `CQLITE_DATASETS_ROOT`
- **When** a BIG source converts to BTI, then that BTI output converts back to BIG
- **Then** every intermediate output dump-equals the ORIGINAL source's Cassandra-written JSONL
  golden (partition-, row- and cell-level, including tombstones/static rows/complex
  deletions/range tombstones), and the final BIG generation is byte-equal to a direct no-purge
  compaction of the original source (never to the untouched original file itself — see design.md D3
  for why those two are not the same claim).

#### Scenario: R1.2 the known zero-clustering-column re-encoding divergence is asserted, not hidden
- **Given** `test_basic.uncompressed_table` (zero clustering columns, the exact fixture salvage's
  round-4 finding, #4196, diverged on)
- **When** the R1.1 round trip runs against it
- **Then** the test asserts CONTENT parity (decode-and-compare) explicitly, naming the same
  divergence class salvage documented, rather than asserting raw byte equality and silently loosening
  it if the same divergence reappears.

### Requirement: R2 — BTI output is structurally correct against Cassandra-written bytes, not a CQLite round trip

Every BTI output `convert` produces SHALL pass the SAME class of structural assertion
`issue_3002_bti_rows_root_base.rs` makes against Cassandra-written `da` fixtures — never validated
only by CQLite reading its own writer's output (#3042's round-trip-invariance lesson).

#### Scenario: R2.1 BTI structural parity lane coverage
- **Given** the `bti-multiclustering` gate component and `test_da/multiclustering_table`
- **When** `convert` produces a BTI output from a BIG source
- **Then** the output passes the SAME assertions `bti-multiclustering` runs for a natively-written
  BTI file (row-index root base, `Rows.db` `NEXT_COMPONENT` framing, trie leaf structure), each
  expectation derived from `cassandra-5.0.8` writer source or a Cassandra-written `da` fixture, not
  from CQLite's own prior BTI output.

#### Scenario: R2.2 a corrupted-framing defect that cancels on CQLite's own read path is still caught
- **Given** a converted BTI output
- **When** `issue_3002_bti_rows_root_base.rs`'s exact assertion style runs against it (source-derived
  expected byte offsets, not CQLite-decoded ones)
- **Then** the test fails if the SAME class of defect #3002 was (a masked, compensating pair of
  encoder bugs), proving R2's oracle actually distinguishes real framing correctness from
  self-consistent-but-wrong output.

### Requirement: R3 — Every Statistics field is accounted for: preserved-equal or named not-representable

For every field in `StatisticsMetadata`, `convert` SHALL either preserve it with a passing
equality test, or explicitly document (in code comment AND in the spec's own table, design.md §D2)
that the target format cannot carry it, citing the `cassandra-5.0.8` source line establishing that.

#### Scenario: R3.1 every representable field preserved, both directions
- **Given** `issue_4202_convert_roundtrip.rs`
- **When** conversion runs in EITHER direction
- **Then** `min/max_timestamp`, `min/max_local_deletion_time`, `min/max_ttl`, `partition_count`,
  `row_count`, `column_count`, `total_rows_size`, `tombstone_histogram`, `first_key`, `last_key`,
  `repaired_at`, `pending_repair`, `is_transient`, `estimated_partition_size` and
  `estimated_cell_count` in the output's Statistics.db equal the source's, field by field.

#### Scenario: R3.2 `has_partition_level_deletions` is named not-representable in BIG output
- **Given** a BTI source with at least one partition-level deletion
- **When** it converts to BIG
- **Then** the BIG output's Statistics.db carries no such field (the legacy `nb`/`oa` STATS body
  never serialises it — cited in design.md §D2), and `ConvertReport.statistics_not_representable`
  names the field and the reason, rather than the conversion silently succeeding with no record of
  what was dropped.

#### Scenario: R3.3 no undocumented field — "compaction ancestry" resolves to "neither format has it"
- **When** the spec's own accounting (design.md §D2) is checked against `StatisticsMetadata`'s actual
  field list
- **Then** every field in the struct appears in exactly one of R3.1's preserved set or R3.2's
  not-representable set, and the issue's "compaction ancestry where the format has it" phrase is
  resolved explicitly as "no ancestors field exists in either `na`+`nb` or `da` Statistics.db" —
  never silently unaddressed.

### Requirement: R4 — A compressed target refuses

`convert` SHALL refuse `Error::UnsupportedFormat` for any `--compression` value other than `none`
(#1406), and never silently ignore the request by writing uncompressed anyway without saying so.

#### Scenario: R4.1 compressed target refuses closed
- **When** `convert_sstable_generation` is called with a compressed target option
- **Then** it returns `Error::UnsupportedFormat` naming #1406, and writes nothing to `output_dir`.
