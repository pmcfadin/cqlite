# value-representation

## ADDED Requirements

### Requirement: The `Value` enum fits within a bounded inline size

The public `Value` enum SHALL NOT inline its rare, large variants. The fat cold variants
(`Tombstone`, `Udt`, `Json`, and any other variant whose inline payload is the layout maximum) SHALL be
boxed so that `size_of::<Value>() <= 40` bytes. The compile-time size pin in `cqlite-core/src/types.rs`
SHALL be tightened from `<= 88` to `<= 40`. The crate SHALL enable `clippy::large_enum_variant` at deny
level so a future fat inline variant fails the build rather than silently regressing the layout.

#### Scenario: The layout pin enforces the 40-byte ceiling

- **GIVEN** the `Value` enum after boxing the fat cold variants
- **WHEN** the crate is compiled
- **THEN** the compile-time assertion `size_of::<Value>() <= 40` holds (on `main` this assertion fails
  at 88 bytes)
- **AND** `clippy::large_enum_variant` reports no violation for `Value` under `-D warnings` (on `main`
  the lint would fire).

#### Scenario: A `size_of` regression fails closed

- **WHEN** a change re-inlines a fat variant (or adds a new one) that pushes `size_of::<Value>()` above
  40 bytes
- **THEN** the build fails at the compile-time pin (the regression cannot merge silently).

### Requirement: Boxing the rare variants preserves all observable value behavior

Boxing SHALL be representation-internal only. Decoded values, `Display` output, serde
(serialize/deserialize round-trip), and comparison/ordering behavior SHALL be byte-identical before and
after the change. Cassandra float/NaN ordering and signed-zero comparison semantics SHALL be unchanged.
The public `QueryRow.values` map contract and every existing public API SHALL be unchanged (no new
public variant, no removed variant).

#### Scenario: 33-table parity and binding suites are unchanged

- **WHEN** the read path is exercised against the test datasets after boxing, and the Python and Node
  binding test suites run
- **THEN** all 33-table sstabledump/JSONL parity tests pass with byte-identical values
- **AND** both binding suites pass (the PyO3 and napi conversion layers that match on `Tombstone`/`Udt`/
  `Json` are updated and produce identical converted values).

#### Scenario: Ordering and serde are byte-identical

- **WHEN** a set of `Value`s spanning the boxed and unboxed variants is sorted with the production
  comparator and round-tripped through serde
- **THEN** the sort order and the serialized bytes are identical to `main` (including NaN-last and
  `-0.0 < +0.0` semantics).
