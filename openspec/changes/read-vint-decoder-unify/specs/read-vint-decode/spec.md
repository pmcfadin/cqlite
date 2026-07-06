# read-vint-decode Specification

## Purpose

Define the read-side Variable-length-integer (VInt) decoder for the Cassandra
SSTable format: a single canonical unsigned/signed decode pair mirroring the
write-side module, onto which every read-side VInt call site is repointed.

## ADDED Requirements

### Requirement: One canonical read-side VInt decode implementation

The read side SHALL provide exactly one VInt bit-assembly implementation, a pair
of functions in `parser/vint.rs` mirroring the write-side `serialization/vint.rs`
style — a single `leading_ones()` length computation, continuation bytes
assembled via one `u64::from_be_bytes` load on a copied array (no per-byte index
loop and no `#[allow(clippy::needless_range_loop)]` suppression), slice framing
(no `nom::take` for framing), uniform `#[inline]`, no hardcoded single-byte match
table, and no `fixed → zigzag` double-decode fallback:

- `decode_unsigned(input: &[u8]) -> Result<(u64, usize)>` decodes an unsigned
  Cassandra `writeUnsignedVInt`, returning the value and the number of bytes
  consumed.
- `decode_signed(input: &[u8]) -> Result<(i64, usize)>` decodes `decode_unsigned`
  then applies the ZigZag unmap.

The obsolete `parser/vint_fixed.rs` module SHALL be removed, and no second live
VInt bit-assembly SHALL remain on the read path.

#### Scenario: Unsigned decode returns value and consumed length

- **WHEN** `decode_unsigned` is called on a well-formed unsigned VInt of width 1..=9 bytes
- **THEN** it returns the decoded `u64` value and the exact byte count consumed (equal to the lead byte's leading-ones count plus one)

#### Scenario: Signed decode is unsigned-then-ZigZag

- **WHEN** `decode_signed` is called on a well-formed signed (ZigZag) VInt
- **THEN** it returns the ZigZag-unmapped `i64` and the same consumed length `decode_unsigned` would report for those bytes

#### Scenario: vint_fixed module is gone and there is a single implementation

- **WHEN** the crate is built
- **THEN** `parser/vint_fixed.rs` no longer exists, `parse_zigzag_vint` no longer exists, and the public `parse_vuint` / `parse_vint` / `parse_unsigned_vint32` wrappers contain no VInt bit-assembly of their own but delegate to `decode_unsigned` / `decode_signed`

### Requirement: Truncated and empty input are rejected without fabrication

The decoder SHALL return an error (never a fabricated or framing-dependent value)
when the input is empty or shorter than the width declared by the lead byte's
leading-ones count. It SHALL consume exactly the declared width for a complete
VInt regardless of any trailing bytes, preserving the I2 (#1624) framing
guarantees.

#### Scenario: Empty input errors

- **WHEN** `decode_unsigned` / `decode_signed` is called on an empty slice
- **THEN** it returns `Err` and consumes nothing

#### Scenario: Truncated multi-byte lead errors

- **WHEN** a lead byte declares an N-byte VInt but fewer than N bytes are present (e.g. a bare `0x80`, `0xC0`, or a `0xFF` with only 7 trailing bytes)
- **THEN** the decoder returns `Err` rather than a partial or buffer-length-dependent value

#### Scenario: Complete value is framing-independent

- **WHEN** the same complete VInt is decoded both alone and followed by trailing junk bytes
- **THEN** the decoded value is identical in both cases and exactly the encoded width is consumed, leaving any trailing bytes unconsumed

### Requirement: Decode semantics are identical to the pre-refactor decoder

The consolidation SHALL NOT change any decode result or the signed/unsigned
verdict of any call site. Equivalence with the pre-refactor `parse_vuint` /
`parse_vint` SHALL be proven by test, not asserted.

#### Scenario: Encode round-trip identity

- **WHEN** any `u64` (via `serialization::vint::encode_unsigned`) or `i64` (via `encode_signed`) is encoded and then decoded by `decode_unsigned` / `decode_signed`
- **THEN** the decoded value equals the original and the consumed length equals the encoded byte length (verified as a proptest over the full value range)

#### Scenario: Corpus differential against the old decoder

- **WHEN** the new `decode_unsigned` and a snapshot of the pre-refactor unsigned bit-assembly are each run over a generated space of lead bytes, continuation patterns, and truncation boundaries
- **THEN** they agree on both the decoded value and the consumed length (or both error) at every position

#### Scenario: 33-table parity and corpus audit unchanged

- **WHEN** the existing `vint_length_corpus_audit_tests` and the 33-table `sstabledump` parity suite run against the refactored decoder
- **THEN** they pass byte-for-byte unchanged

### Requirement: The VInt decode primitive is benched and regression-gated

The measurement suite SHALL provide a `decode/vint_decode` criterion bench that
exercises `decode_unsigned` / `decode_signed` over a fixed set of representative
VInt widths, running without the `cli-helpers` / `bench-internals` features (no
fixture required), and `cqlite-core/benches/perf-gate.json` SHALL carry a
`decode/vint_decode` entry so the primitive's cost is tracked.

#### Scenario: vint_decode bench runs feature-independently

- **WHEN** `cargo bench -p cqlite-core --bench decode` runs without `cli-helpers` / `bench-internals`
- **THEN** the `decode/vint_decode` group still measures the decode primitive (it does not compile to the empty no-op main)

#### Scenario: perf-gate.json tracks vint_decode

- **WHEN** `cqlite-core/benches/perf-gate.json` is read
- **THEN** it contains a `decode/vint_decode` entry with a defined `threshold_pct`
