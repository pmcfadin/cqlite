# read-vint-decode Specification

## MODIFIED Requirements

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

The `parse_unsigned_vint32` public wrapper — mandated by the archived
`read-vint-decoder-unify` change — is a dead surface with zero non-test,
non-benchmark callers and SHALL NOT be required to exist; it was removed by the
`delete-dead-parser-generations` change (parser-audit finding J3). The remaining
public wrappers `parse_vuint` / `parse_vint` SHALL contain no VInt bit-assembly of
their own but delegate to `decode_unsigned` / `decode_signed`.

#### Scenario: Unsigned decode returns value and consumed length

- **WHEN** `decode_unsigned` is called on a well-formed unsigned VInt of width 1..=9 bytes
- **THEN** it returns the decoded `u64` value and the exact byte count consumed (equal to the lead byte's leading-ones count plus one)

#### Scenario: Signed decode is unsigned-then-ZigZag

- **WHEN** `decode_signed` is called on a well-formed signed (ZigZag) VInt
- **THEN** it returns the ZigZag-unmapped `i64` and the same consumed length `decode_unsigned` would report for those bytes

#### Scenario: vint_fixed module is gone and there is a single implementation

- **WHEN** the crate is built
- **THEN** `parser/vint_fixed.rs` no longer exists, `parse_zigzag_vint` no longer exists, and the public `parse_vuint` / `parse_vint` wrappers contain no VInt bit-assembly of their own but delegate to `decode_unsigned` / `decode_signed`

#### Scenario: The removed parse_unsigned_vint32 wrapper is no longer required

- **WHEN** the workspace is searched for `parse_unsigned_vint32`
- **THEN** no compiled (non-comment) Rust source references it, and the read-vint-decode capability does not require the wrapper to exist
