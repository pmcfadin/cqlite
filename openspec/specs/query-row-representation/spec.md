# query-row-representation Specification

## Purpose
TBD - created by archiving change intern-cell-names. Update Purpose after archive.
## Requirements
### Requirement: Column names in a decoded row are shared, not cloned per cell
The row decoder SHALL NOT allocate a new heap `String` for a column name on a per-cell, per-row basis.
Column names SHALL be interned once (schema-owned) and shared into the returned cells representation via
a reference-counted handle (`Arc<str>`), so that populating a cell with its name is a refcount bump, not
a `String` allocation. The shared handle SHALL be carried end-to-end into the public row
(`QueryRow.values`) without an intermediate step that re-allocates the name (the prior
`Value::Text(String)` map-key round-trip SHALL be removed).

#### Scenario: A full table scan does not allocate a String per cell for the column name
- **WHEN** the read path decodes rows during a scan (the path through `row_data.rs`)
- **THEN** no new heap `String` is allocated to carry a column name into a cell (the name is a shared `Arc<str>` refcount bump)
- **AND** the per-cell column-name `String` allocation no longer appears in the dhat heap-profile top allocation ranks for the scan harness

#### Scenario: The interned name reaches the public row without a re-allocation
- **WHEN** a decoded cell's name flows from the decoder through the emit pipeline into `QueryRow.values`
- **THEN** the name is carried as the shared `Arc<str>` handle
- **AND** there is no `.to_string()` / `String`-reallocation of the name in the emit→build pipeline

### Requirement: Observed CQL values, output, and ordering are unchanged
Interning column names SHALL be representation-internal only. Observed CQL values, the public column
names, the JSON/CSV/table output bytes, the JSONL parity goldens, and row/column ordering SHALL be
byte-identical before and after the change. Cells SHALL remain addressable by name.

#### Scenario: Parity goldens are unchanged across all tables
- **WHEN** the read path is exercised against the test datasets after the change
- **THEN** all existing sstabledump/JSONL parity tests and the Python parity suite (all 33 tables) pass with unchanged values
- **AND** cells remain retrievable by column name (`QueryRow::get(&str)` and iteration over names behave identically)

#### Scenario: CLI output is byte-identical
- **WHEN** the CLI renders a query result as JSON, CSV, or table after the change
- **THEN** the output bytes are identical to before the change (output-determinism regression tests pass)
- **AND** column ordering (emit-time alphabetical) is preserved

#### Scenario: QueryResult still serializes and deserializes
- **WHEN** a `QueryResult`/`QueryRow` is serialized to JSON/YAML and deserialized back
- **THEN** the round-trip succeeds (serde support for the reference-counted key is enabled)
- **AND** the deserialized row has the same column names and values

### Requirement: Bindings expose unchanged row content
The Python and Node bindings SHALL expose the same column names, values, and iteration behavior as
before the change; the interning is invisible across the FFI boundary.

#### Scenario: Python and Node binding row access is unchanged
- **WHEN** a row is read via the Python bindings (`Row.__getitem__`/`keys`/`items`) or the Node bindings (`executeNative`/streaming)
- **THEN** the returned column names and values are identical to before the change
- **AND** the existing Python and Node binding test suites pass

