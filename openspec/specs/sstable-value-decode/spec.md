# sstable-value-decode Specification

## Purpose
TBD - created by archiving change per-column-resolved-dispatch. Update Purpose after archive.
## Requirements
### Requirement: Cell decode dispatch is resolved once per column, not per cell

The V5CompressedLegacy cell decode path SHALL resolve each column's decode dispatch
decision exactly ONCE per SSTable block, from authoritative column metadata, and reuse it
for every cell of that column. `ColumnToParse` SHALL carry a precomputed dispatch tag
(`CellKind`) derived from the supplied-schema type (or, for a dropped column, the on-disk
header marshal type), and a precomputed `is_complex` flag derived from the authoritative
complex-ness type (on-disk header marshal type preferred, supplied-schema type on the
header-empty fallback). `parse_cell_value_schema_order` SHALL dispatch on the precomputed
`CellKind` and the row body SHALL branch on the precomputed `is_complex`; neither SHALL
call `data_type.to_lowercase()` per cell.

#### Scenario: A full scalar-column scan performs zero per-cell type normalizations
- **WHEN** a full `SELECT *` scan runs over a present real fixture whose columns are scalar CQL types (with the `work-counters` gauge enabled)
- **THEN** the scan returns a non-zero number of rows AND the `TYPE_NORMALIZE_CALLS` work-counter reads exactly `0` (on `main` the same scan reads at least one normalization per returned row)

#### Scenario: The empty-value cell path also performs zero per-cell normalizations
- **WHEN** a full scan runs over a present fixture containing empty (`HAS_EMPTY_VALUE`) `text` and `blob` cells
- **THEN** the scan returns the expected rows AND `TYPE_NORMALIZE_CALLS` reads exactly `0` (on `main` the empty-value early-return itself normalizes per cell)

### Requirement: Decoded values are byte-for-byte unchanged (parity preserved)

Resolving dispatch per column SHALL NOT change any decoded value for any CQL type. The
per-cell dispatch tag SHALL be derived ONLY from authoritative column metadata and never
inferred from value byte patterns (no-heuristics mandate, issue #28). The empty-value
early-return SHALL keep its exact type-specific behavior (`text`/`varchar`/`ascii` decode
to `Text("")`, `blob` decodes to `Blob([])`, every other declared type decodes to `Null`).
The complex / frozen / tuple / collection / marshal-UDT decode SHALL remain byte-identical.

#### Scenario: Scalar, collection, UDT, and frozen columns all decode identically
- **WHEN** the value-decode lockstep suite and the 33-table JSONL parity suite run against real fixtures with the change applied
- **THEN** every decoded value matches the pre-change / `sstabledump` golden output, and the string-ladder decoder and `ComparatorType` decoder still agree for the same bytes across all types

#### Scenario: A dropped column present on disk but absent from schema stays byte-aligned
- **WHEN** a row is decoded whose on-disk serialization header contains a column absent from the supplied schema
- **THEN** the dropped column's bytes are consumed (keeping trailing columns aligned) using the on-disk header marshal type as the dispatch source, no cell is emitted for it, and the emitted columns are identical to the pre-change output

### Requirement: Scalar byte-payload values are extracted without a per-cell copy

Byte-carrying scalar values SHALL be extracted as refcounted subslices of the decompressed chunk
substrate rather than copied out per cell. This covers the `Text`, `Blob`, `Varint`, and
`Decimal`-unscaled payloads: the decoder SHALL slice the chunk (`bytes::Bytes` / `Bytes::slice_ref`)
instead of calling `.to_vec()` or `String::from_utf8(bytes.to_vec())`. This applies to the decode paths
in `reader/parsing/v5_compressed_legacy/` (`cell_value`, `raw_value`, `raw_type_value`, `udt`,
`complex_column`) and `parsing/comparator_value_parsing.rs`. UTF-8 validation for `Text` SHALL
be performed in place on the borrowed slice (`str::from_utf8`) — validation without a copy — and the
validated `Bytes` stored. The extracted value SHALL never be inferred from byte patterns; the decode
tag continues to come from authoritative column metadata (no-heuristics, issue #28).

#### Scenario: A text-heavy scan copies ≈ zero payload bytes into values

- **WHEN** the H2 dhat text-heavy lane runs a full scan over a present text-heavy fixture after the
  change
- **THEN** the bytes-copied-into-`Value`-payloads metric is ≈ 0 (borrowed / refcounted), versus ~1×
  the scanned payload on `main`
- **AND** the scan returns a non-zero number of rows with byte-identical values.

#### Scenario: Interim UTF-8 win removes the throwaway pre-validation Vec

- **WHEN** a decode path that still copies is exercised (any path not yet on the borrow substrate)
- **THEN** it uses `str::from_utf8(bytes)?.to_owned()` (validate-then-own), not
  `String::from_utf8(bytes.to_vec())`
- **AND** an allocation-count test on the UTF-8 decode path confirms the throwaway pre-validation `Vec`
  is gone (this interim win is independent of the window substrate and may land first).

### Requirement: Chunk retention is bounded (no tiny value pins a whole chunk)

Borrowing a `Bytes` subslice keeps its parent chunk alive by refcount. The extraction path SHALL apply
a documented copy-out policy so a small and/or long-lived value does not pin its entire decompressed
chunk: values at or below a documented byte threshold, and any value that outlives the scan window
(e.g. a materialized/collected result), SHALL be copied into their own allocation rather than borrowing
the chunk. The threshold and the long-lived-copy rule SHALL be documented at the extraction site.

#### Scenario: A tiny long-lived value does not retain its chunk

- **GIVEN** a decoded value at or below the copy-out threshold that is retained beyond the scan window
  (e.g. collected into a materialized result), decoded from a large (e.g. 64 KB) chunk
- **WHEN** the scan advances past that chunk
- **THEN** the chunk buffer is released (the retained tiny value does not hold a strong reference to the
  whole chunk) — asserted by a retention test.

### Requirement: Decoded values remain byte-for-byte unchanged (parity preserved)

Zero-copy extraction SHALL NOT change any decoded value for any CQL type across any compression
algorithm. Scalar, collection, UDT, frozen, and tuple decode SHALL remain byte-identical, and the
binding conversion layers SHALL produce identical converted values.

#### Scenario: 33-table parity and binding suites are unchanged after zero-copy decode

- **WHEN** the 33-table JSONL/sstabledump parity suite (all four compression algorithms) and the Python
  and Node binding suites run after the change
- **THEN** every decoded value matches the pre-change / sstabledump golden output
- **AND** both binding suites pass with byte-identical converted values.

