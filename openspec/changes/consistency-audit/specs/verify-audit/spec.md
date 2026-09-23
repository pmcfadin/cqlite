# verify-audit — new capability (consistency-audit, issue #4195)

`cqlite verify --mode audit` SHALL check that an SSTable generation's components agree with EACH
OTHER — not merely that each is internally well-formed — reporting a closed set of 8 named
invariants as `Pass | Fail | Skip(<cause>)`, and SHALL name the reason rather than guess when an
invariant cannot be measured. All requirements are ADDED.

## ADDED Requirements

### Requirement: A0 — Audit mode reports a fixed, always-8-row checklist

`VerifyReport.audit` SHALL be `Some(AuditSummary)` only when `mode == VerifyMode::Audit`, and `AuditSummary.rows` SHALL contain exactly 8 rows, one per named invariant, always present regardless of whether that invariant applies to the SSTable's format or state — a non-applicable or unmeasurable invariant is `Skip(<cause>)`, never an omitted row.

#### Scenario: A0.1 audit on a healthy BIG SSTable reports all 8 rows
- **Given** a committed healthy `test_comp`/`test_basic` BIG fixture
- **When** `verify_sstable(dir, VerifyMode::Audit, ..)` runs (no `--deep`)
- **Then** `report.audit.rows.len() == 8`, every BIG-applicable row (per the design's applicability
  table) is `Pass` or a named `Skip` (e.g. `compression-coverage` is `Skip("no CompressionInfo:
  uncompressed")` on an uncompressed fixture), every BTI-only row (`bti-trie-bounds`) is
  `Skip("not applicable: BIG format")`, and `statistics-vs-scan` is `Skip("requires --deep")`
  (`cqlite-core/tests/issue_4195_component_audit.rs`; corpus gating per #1094 —
  `CQLITE_REQUIRE_FIXTURES=1` hard-requires).

#### Scenario: A0.2 audit on a healthy BTI SSTable reports all 8 rows
- **Given** a committed healthy `test_da` BTI fixture
- **When** `verify_sstable` runs with `VerifyMode::Audit`
- **Then** as A0.1, with `index-data-identity`/`summary-index-correlation` (BIG-only) each
  `Skip("not applicable: BTI format")` and `bti-trie-bounds` `Pass`.

### Requirement: A1 — Index.db entry positions land on the matching Data.db partition header (BIG)

Every BIG `Index.db` entry's declared `(key, data_offset)` SHALL be cross-checked against the partition header actually present at `data_offset` in `Data.db`, reading ONLY that header — never a full partition decode.

#### Scenario: A1.1 healthy BIG SSTable — every index entry matches its Data.db header
- **Given** a committed healthy BIG fixture
- **When** `index-data-identity` runs
- **Then** `Pass`, and no `IndexPositionKeyMismatch` finding is present.

#### Scenario: A1.2 swapped index offsets are caught without a full scan
- **Given** `test_comp_corrupt/index_entry_offset_swap` (two adjacent `Index.db` entries' declared
  `data_offset`s swapped; each remains a structurally valid VInt entry, so `check_big_index` alone
  stays green)
- **When** `index-data-identity` runs
- **Then** `Fail`, an `IndexPositionKeyMismatch` finding names both offsets and both keys, and the
  check reads only the two affected partition headers — never a full `Data.db` row decode (asserted
  by the test via the same read-bound mechanism the #4194 sweep memory bound uses, or an explicit
  read-count/byte-range assertion if the header-read primitive exposes one).

#### Scenario: A1.3 a corrupt Index.db Skips rather than guesses
- **Given** `test_comp_corrupt/index_db_bit_flip_big`
- **When** `index-data-identity` runs
- **Then** `Skip("boundary source unreadable")`, never a `Fail` built on an untrustworthy `Index.db`.

### Requirement: A2 — Summary.db samples resolve to the matching Index.db position and key (BIG)

Every `Summary.db` sample's `(partition_key, index_position)` SHALL resolve, at `index_position` (a byte offset INTO `Index.db`), to an `Index.db` entry carrying the SAME key.

#### Scenario: A2.1 healthy BIG SSTable — every sample resolves correctly
- **Given** a committed healthy BIG fixture with a non-trivial `Summary.db` (more than one sample)
- **When** `summary-index-correlation` runs
- **Then** `Pass`.

#### Scenario: A2.2 a corrupted sample position is caught
- **Given** `test_comp_corrupt/summary_sample_position_corrupt` (one `Summary.db` sample's
  `index_position` rewritten to a DIFFERENT, still in-bounds `Index.db` entry)
- **When** `summary-index-correlation` runs
- **Then** `Fail`, a `SummarySampleMismatch` finding names the sample index, the claimed key, and the
  key actually found at the corrupted position.

#### Scenario: A2.3 no Summary.db Skips
- **Given** a temp copy of a healthy fixture with `Summary.db` removed
- **When** `summary-index-correlation` runs
- **Then** `Skip("no Summary.db")`.

### Requirement: A3 — Bloom filter has no false negative (BIG and BTI)

`Filter.db` SHALL report `might_contain == true` for every key actually present in `Data.db`, checked on BOTH formats against each format's own authoritative present-key set (`Index.db` for BIG, resolved BTI trie leaves for BTI).

#### Scenario: A3.1 healthy fixture — no false negatives (both formats)
- **Given** committed healthy BIG and BTI fixtures
- **When** `bloom-no-false-negative` runs on each
- **Then** `Pass` on both.

#### Scenario: A3.2 BIG false negative (existing coverage, reused as-is)
- **Given** `test_comp_corrupt/filter_db_bit_flip`
- **When** `bloom-no-false-negative` runs
- **Then** `Fail`, reusing the EXISTING `FilterFalseNegative` finding (issue #1398) — no new finding
  class for this half.

#### Scenario: A3.3 BTI false negative (new coverage)
- **Given** `test_comp_corrupt/filter_db_bit_flip_bti` (BTI analogue: one bit 1→0 in a BTI
  `Filter.db`)
- **When** `bloom-no-false-negative` runs
- **Then** `Fail`, a `FilterFalseNegative` finding names the affected key, sourced from the resolved
  BTI trie leaves rather than `Index.db`.

### Requirement: A4 — BTI trie leaves are reachable, in-bounds, and identity-correct

Every `Partitions.db` leaf reachable from the trie root SHALL resolve to a raw partition key matching the corresponding `Data.db`/`Rows.db` content (issue #1103, already implemented), and every `DataOffset` leaf's payload position SHALL be within `Data.db`'s bounds BEFORE any scan is attempted.

#### Scenario: A4.1 healthy BTI SSTable — reachability, identity and bounds all hold
- **Given** a committed healthy BTI fixture
- **When** `bti-trie-bounds` runs
- **Then** `Pass` (exercising the existing `bti_partition_identity_mismatch` path plus the new bounds
  check together).

#### Scenario: A4.2 existing identity-mismatch coverage is reused as-is
- **Given** `test_comp_corrupt/bti_partitions_footer_flip`
- **When** `bti-trie-bounds` runs
- **Then** `Fail`, reusing the EXISTING `BtiRootPointerCorrupt` finding — no behavior change.

#### Scenario: A4.3 an out-of-bounds DataOffset leaf is caught before any scan
- **Given** `test_comp_corrupt/bti_data_offset_out_of_bounds` (a `DataOffset` leaf's payload
  rewritten to a position past `Data.db`'s actual length)
- **When** `bti-trie-bounds` runs in `--mode audit` WITHOUT `--deep` (no scan attempted)
- **Then** `Fail`, a `BtiDataOffsetOutOfBounds` finding names the leaf's prefix and the out-of-bounds
  position — caught by the bounds check alone, never requiring a scan to surface it.

### Requirement: A5 — TOC.txt exact listing (reused, unchanged)

The `toc-exact-listing` audit row SHALL reflect the EXISTING `check_toc_and_presence` result (both directions: listed-but-absent, present-but-unlisted) with no new checking logic.

#### Scenario: A5.1 existing TOC coverage maps onto the audit row unchanged
- **Given** `test_comp_corrupt/toc_missing_component`
- **When** `toc-exact-listing` runs
- **Then** `Fail`, reusing the EXISTING `MissingComponent` finding(s) — no new detection logic added
  by this change for this invariant.

### Requirement: A6 — Digest.crc32 matches Data.db (reused, unchanged)

The `digest-matches-data` audit row SHALL reflect the EXISTING `check_digest` result with no new checking logic.

#### Scenario: A6.1 existing digest coverage maps onto the audit row unchanged
- **Given** `test_comp_corrupt/digest_crc32_mismatch`
- **When** `digest-matches-data` runs
- **Then** `Fail`, reusing the EXISTING `DigestMismatch` finding — no new detection logic added by
  this change for this invariant.

### Requirement: A7 — CompressionInfo chunk table covers exactly Data.db's length

For a compressed SSTable, `CompressionInfo.db`'s declared chunk count SHALL equal `ceil(data_length / chunk_length)`, and the last chunk's physical range SHALL end exactly at Data.db's real file size.

#### Scenario: A7.1 healthy compressed fixture — exact coverage
- **Given** a committed healthy compressed BIG fixture (e.g. `lz4_table`)
- **When** `compression-coverage` runs
- **Then** `Pass`.

#### Scenario: A7.2 a short chunk table is caught
- **Given** `test_comp_corrupt/compression_info_short_coverage` (the LAST `chunk_offsets` entry
  dropped)
- **When** `compression-coverage` runs
- **Then** `Fail`, a `CompressionCoverageGap` finding names the declared vs. expected chunk count.

#### Scenario: A7.3 uncompressed SSTable Skips
- **Given** a committed healthy uncompressed fixture (no `CompressionInfo.db`)
- **When** `compression-coverage` runs
- **Then** `Skip("no CompressionInfo: uncompressed")`.

### Requirement: A8 — Statistics.db's declared values agree with a real scan (`--deep` only)

`Statistics.db`'s declared `partition_count` (and, unless declared a gap per §DECLARED GAP below, `min_timestamp`/`max_timestamp`/`min_local_deletion_time`/`max_local_deletion_time`) SHALL agree with values observed by an actual `--deep` scan, and this invariant SHALL NOT run at all without `--deep`.

#### Scenario: A8.1 without --deep, the row Skips naming the requirement
- **Given** any fixture
- **When** `verify_sstable` runs with `VerifyMode::Audit` and `deep == false`
- **Then** `statistics-vs-scan` is `Skip("requires --deep")` — no scan is attempted for this row.

#### Scenario: A8.2 healthy fixture, --deep — declared values agree with the scan
- **Given** a committed healthy fixture
- **When** `verify_sstable` runs with `VerifyMode::Audit` and `deep == true`
- **Then** `statistics-vs-scan` is `Pass`.

#### Scenario: A8.3 a corrupted min_timestamp is caught under --deep
- **Given** `test_comp_corrupt/statistics_timestamp_mismatch` (`Statistics.db`'s `min_timestamp`
  rewritten to a plausible-but-wrong value; the file still parses)
- **When** `verify_sstable` runs with `deep == true`
- **Then** `Fail`, a `StatisticsScanMismatch` finding names the declared vs. observed value.

#### DECLARED GAP — the timestamp/min-LDT/max-LDT half of A8 may ship as partition-count-only
- Per design.md §D7/tasks.md 0.5: whether the write-path running-stats accumulator is cheaply
  reusable read-only is a task-0 premise. If it is not, within this change's size budget A8 checks
  `partition_count` only, and Scenario A8.3 (or an equivalent `min_timestamp`-only case) is NOT
  implemented — recorded here rather than silently narrowed, matching #4194's L1.4 convention.
  Extending to the full four-value comparison is then a follow-up issue, not fabricated in this
  change.

### Requirement: A9 — A damaged boundary source Skips its dependent rows, never a guess

Whenever an invariant's own authoritative source is itself the corrupt component named by another finding in the same report, that invariant's row SHALL be `Skip("boundary source unreadable")` rather than `Fail` on unreliable input or a fabricated `Pass`.

#### Scenario: A9.1 corrupt BIG Index.db Skips both dependent rows
- **Given** `test_comp_corrupt/index_db_bit_flip_big`
- **When** `verify_sstable` runs with `VerifyMode::Audit`
- **Then** `index-data-identity` and `summary-index-correlation` are both `Skip("boundary source
  unreadable")`, and the report's `IndexEntryCorrupt` finding is present unchanged.

#### Scenario: A9.2 corrupt BTI boundary source Skips its dependent rows
- **Given** `test_comp_corrupt/bti_rows_truncation`
- **When** `verify_sstable` runs with `VerifyMode::Audit`
- **Then** `bti-trie-bounds` and `bloom-no-false-negative` (BTI) are `Skip("boundary source
  unreadable")`.

### Requirement: A10 — Exit code is a closed function of the row verdicts

`cqlite verify --mode audit` SHALL exit `2` when any row is `Fail`, SHALL exit `2` when EVERY row is `Skip` (naming `audit: 0 invariants MEASURED`), and SHALL exit `0` otherwise (a mix of `Pass` and `Skip` with zero `Fail` is a genuine pass).

#### Scenario: A10.1 all-Skip is its own failing exit, not a false clean bill of health
- **Given** a temp dir holding only a bare `Data.db` with `TOC.txt` present but every OTHER
  component absent (so every invariant Skips: no `Index.db`, no `Summary.db`, no `Filter.db`, no
  `CompressionInfo.db`, wrong-format `Partitions.db`/`Rows.db` absent, no `--deep`)
- **When** `cqlite verify <dir> --mode audit` runs
- **Then** exit `2`, and the text/JSON output names `audit: 0 invariants MEASURED` —
  affirmative-zero doctrine (`cqlite-cli/tests/`, named in the gate's `cli-tests` list per #3522).

#### Scenario: A10.2 Pass + Skip with zero Fail is a real pass
- **Given** a committed healthy uncompressed BIG fixture without `--deep`
- **When** `cqlite verify <dir> --mode audit` runs
- **Then** exit `0` — `compression-coverage` and `statistics-vs-scan` Skip, every other applicable
  row Passes, and the mix does not trip the failing exit code.

### Requirement: A11 — No header hunting

Every invariant check SHALL read only decoded, authoritative structures — `Index.db`/`Summary.db` entries, the BTI trie's resolved leaves, `CompressionInfo.db`'s chunk table, and (invariant 1 only) a header-only decode at an index-named offset — and SHALL NOT locate a partition or a boundary by scanning `Data.db` bytes for a plausible pattern.

#### Scenario: A11.1 static no-resync-scan guard
- **Given** `scripts/tests/test_verify_audit_no_resync_scan.sh` (`tooling-tests`)
- **When** it greps `cqlite-core/src/storage/sstable/verify_audit.rs` (or the `verify_audit/`
  directory) for `memchr`, `windows(`, `find(|b|`, `position(|b|` outside `#[cfg(test)]`
- **Then** none is present; any hit FAILs naming the line.

### Requirement: A12 — Additive report shape; existing Quick/Full behavior is unaffected

`VerifyReport`'s pre-existing fields, `VerifyMode::Quick`/`Full` behavior, and the existing text/JSON rendering SHALL be completely unchanged by this addition — `audit` SHALL be `None` for `Quick`/`Full` reports.

#### Scenario: A12.1 the existing corruption parity suite is unaffected
- **Given** `cqlite-core/tests/sstable_parity_corruption_verify.rs` (unmodified by this change)
- **When** the full gate runs it against the committed corpus
- **Then** every existing class/verdict assertion still passes — this change adds a new mode and
  finding classes, it never alters `Quick`/`Full` classification or verdicts.

#### Scenario: A12.2 CLI renders the checklist only under --mode audit
- **Given** the built binary and a committed healthy fixture
- **When** `cqlite verify <dir> --mode full --out json` runs
- **Then** the JSON output's `audit` key is `null` (or absent, matching the existing always-present-
  key-when-optional convention established by #4194's `location` field) and every other field renders
  exactly as it did before this change.
