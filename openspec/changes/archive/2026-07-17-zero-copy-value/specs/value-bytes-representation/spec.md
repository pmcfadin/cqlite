# value-bytes-representation

## ADDED Requirements

### Requirement: Byte-carrying `Value` variants are `Bytes`-backed within the size budget

The byte-carrying scalar variants of the public `Value` enum SHALL be backed by `bytes::Bytes` instead
of an owned `String`/`Vec<u8>`, specifically `Text`, `Blob`, `Varint`, and `Inet`. The change SHALL keep
`size_of::<Value>() <= 40` (the existing compile-time pin in `cqlite-core/src/types.rs`). `Decimal`'s
`unscaled` field SHALL remain an owned `Vec<u8>` because a `Bytes` field would push the `Decimal`
variant past the 40-byte ceiling; no variant SHALL be added or removed. The `Bytes` payload MAY be a
zero-copy `slice_ref` view of a decoded chunk or a standalone owned buffer; both are the same public
type.

#### Scenario: The layout pin still holds after Bytes-ification

- **GIVEN** `Value` with `Text`/`Blob`/`Varint`/`Inet` changed to `Bytes`-backed payloads
- **WHEN** `cqlite-core` is compiled
- **THEN** the compile-time assertion `size_of::<Value>() <= 40` holds
- **AND** no `Value` variant is added or removed relative to `main`.

#### Scenario: A payload that would exceed the budget fails closed

- **WHEN** a change makes a byte-carrying variant hold a payload that pushes `size_of::<Value>()` above
  40 bytes (for example Bytes-ifying `Decimal.unscaled` without boxing `Decimal`)
- **THEN** the build fails at the compile-time pin rather than silently regressing the layout.

### Requirement: Public accessors and observable value behavior are unchanged

The public accessors SHALL keep their existing signatures and semantics: `as_str` returns
`Option<&str>`, `as_bytes` / `as_inet_bytes` return `Option<&[u8]>`, and `len` / `is_empty` behave as
before. `Text`'s backing `Bytes` SHALL be UTF-8-validated at construction (decode time) so `as_str`
remains a cheap borrowed view with no re-copy. `Display`, ordering/comparison, and hashing behavior
SHALL be byte-identical to `main`, including Cassandra float/NaN ordering and `-0.0 < +0.0`. The public
`QueryRow.values` map contract SHALL be unchanged.

#### Scenario: Accessors return the same views after the representation change

- **GIVEN** a `Value::Text` and a `Value::Blob` constructed after the change
- **WHEN** `as_str`, `as_bytes`, `len`, and `is_empty` are called
- **THEN** they return the same `&str`/`&[u8]`/length/emptiness a pre-change `Value` returned for the
  same logical content.

#### Scenario: Ordering, Display, and hashing are byte-identical

- **WHEN** a set of `Value`s spanning the Bytes-backed and other variants is sorted with the production
  comparator, formatted via `Display`, and hashed
- **THEN** the sort order, formatted output, and hash-based grouping are identical to `main` (including
  NaN-last and `-0.0 < +0.0`).

### Requirement: serde wire format is byte-identical

The serde serialization of the `Bytes`-backed variants SHALL be byte-identical to the pre-change
`String`/`Vec<u8>` serialization so that every JSONL golden and every serde round-trip is unchanged. The
variants SHALL use a custom serde representation (serialize `Text` as a string and `Blob`/`Varint`/
`Inet` as their byte representation) rather than `Bytes`'s default derived form.

#### Scenario: serde round-trip and JSONL goldens are unchanged

- **GIVEN** `Value`s covering `Text`, `Blob`, `Varint`, and `Inet`
- **WHEN** each is serialized and deserialized through serde
- **THEN** the serialized bytes and the round-tripped value are identical to `main`
- **AND** the 33-table JSONL/sstabledump parity goldens are unchanged.

### Requirement: Ergonomic constructors preserve source compatibility

The enumerated breaking change (inner type `String`/`Vec<u8>` → `Bytes`) SHALL be softened with
ergonomic constructors and `From` conversions so idiomatic call sites remain source-compatible:
`Value` SHALL provide construction from `&str`, `String`, `Vec<u8>`, `&[u8]`, and `Bytes` for the
Bytes-backed variants. The set of breaking changes SHALL be enumerated in the change (proposal/design)
so no consumer is surprised.

#### Scenario: Constructing a text/blob value from common source types

- **WHEN** a caller constructs a `Value::Text` from a `&str` or `String`, or a `Value::Blob` from a
  `Vec<u8>` or `&[u8]`
- **THEN** the construction compiles via the provided constructor / `From` impl and yields the expected
  value with byte-identical content.
