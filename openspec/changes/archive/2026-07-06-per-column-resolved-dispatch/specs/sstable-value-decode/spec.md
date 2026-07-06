## ADDED Requirements

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
