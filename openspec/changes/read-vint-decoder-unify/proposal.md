## Why

The July 2026 parser-performance audit
(`docs/reports/parser-performance-audit-2026-07-01.md`, §Epic J finding **J4**,
audit block 2) found that the **read side** carries **four VInt decode
implementations** — `parser/vint.rs::parse_zigzag_vint`,
`parser/vint_fixed.rs::parse_vint_fixed`, and the near-verbatim in-place
assemblies inside `parse_vuint` / `parse_unsigned_vint32` — with a grab-bag of
defects the audit calls out:

- `parse_vint` tries `parse_vint_fixed` first and **double-decodes** on failure
  via a `fixed → zigzag` fallback that, for any complete buffer, is dead (the
  leading-ones framing always wins) yet still ships.
- Per-byte index loops (`for i in 1..total { value = (value << 8) | input[i] }`)
  carrying `#[allow(clippy::needless_range_loop)]`, instead of a single
  `u64::from_be_bytes` load.
- `nom::take` used purely for framing where a slice suffices.
- Inconsistent `#[inline]`.

I1 (#1623, unsigned-not-ZigZag length verdict) and I2 (#1624, framing/truncation
correctness) already fixed the **correctness** defects and pinned them with the
`vint_length_corpus_audit_tests` and the `_1624` regression tests. Their tests
are this refactor's spec. The **duplication remains** — a pure-consolidation,
constant-factor cleanup on the hottest decode primitive in the reader.

The write side (`storage/serialization/vint.rs`) is the audit's **exemplary**
model: one `leading_ones` length computation, `from_be_bytes` assembly, uniform
`#[inline]`, no match table, no fallback. This change makes the read side mirror
it: **one** canonical decode pair, all read-side callers repointed onto it,
`vint_fixed.rs` deleted.

**Routing: design-driven, owner-pre-decided.** The audit is the source of truth
and this decision is locked by the read-path performance audit's standing owner
Seam-1 approval (2026-07-06 drain directive); this change encodes that locked
decision and does not re-litigate the J4 verdict.

Milestone: **v0.14 perf wave (Epic J / one-decoder)**. This is a subtractive
consolidation with **identical decode semantics**, proven by a corpus
differential and an encode→decode round-trip — not a format change.

## What Changes

- **Add one canonical read-side decode pair** in `parser/vint.rs`, mirroring
  `serialization/vint.rs`'s style:
  - `decode_unsigned(input: &[u8]) -> Result<(u64, usize), VIntError>` — the
    single Cassandra `writeUnsignedVInt` bit-assembly: one `leading_ones()`
    length, continuation bytes loaded via `u64::from_be_bytes` on a copied
    8-byte array (no per-byte index loop), `#[inline]`.
  - `decode_signed(input: &[u8]) -> Result<(i64, usize), VIntError>` =
    `decode_unsigned` then ZigZag unmap.
- **Repoint every read-side decoder onto the one implementation.** The public
  nom wrappers `parse_vuint`, `parse_vint`, `parse_unsigned_vint32` become thin
  adapters over `decode_unsigned` / `decode_signed` (preserving their existing
  `IResult` signatures and every caller). `parse_vint` loses the
  `fixed → zigzag` double-decode fallback.
- **Delete the duplicate/dead decoders**: `parser/vint_fixed.rs` (whole file),
  `parse_zigzag_vint`, the dead `#[allow(dead_code)]` `parse_cassandra_vint_format`
  / `parse_custom_vint_format` / `detect_ascii_corruption`, and the
  `vint_fixed`-dependent `parse_vint_cassandra` / `encode_vint_cassandra`
  (zero callers).
- **Add a `decode/vint_decode` criterion micro-bench** (in `benches/decode_bench.rs`,
  runs feature-independently) and a `perf-gate.json` entry, so the primitive's
  ns/op is measured and guarded.

## Non-goals

- **NOT changing the signed/unsigned verdict of any call site** — I1 (#1623)
  owns that; this is a pure consolidation with identical semantics.
- **NOT changing any decode RESULT.** 33-table `sstabledump` parity and the
  `vint_length` corpus audit stay byte-for-byte green.
- **NOT the encoder cleanup.** The read-side `encode_*` cluster (`encode_vint`,
  `encode_vuint`, `vint_size`, …) is J3's dead-code scope; only the
  `vint_fixed`-coupled encoder (`encode_vint_cassandra`, 0 callers) is removed
  here because its dependency is deleted.
- **NOT the campsite split of `vint.rs`** — that is M2's amortized scope; this
  change is a net line reduction.

## Impact

- **Public surface:** `parse_vuint` / `parse_vint` / `parse_unsigned_vint32` /
  `parse_vint_length` / `parse_vint_length_signed` keep identical signatures and
  behavior. New `pub` items `decode_unsigned` / `decode_signed` / `VIntError`
  are additive. Removed items (`parse_vint_cassandra`, `encode_vint_cassandra`,
  the `vint_fixed` module) have zero in-crate or cross-crate callers.
- **No-heuristics mandate:** unaffected — the removed `detect_ascii_corruption`
  was already dead (`#[allow(dead_code)]`); no byte-content inference is added.
- **Correctness:** decode semantics are provably identical (corpus differential
  vs the pre-refactor `parse_vuint`; encode→decode round-trip vs
  `serialization::vint`). Truncation/framing behavior stays as I2 (#1624) fixed
  it.
