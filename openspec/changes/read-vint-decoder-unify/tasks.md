# Tasks — read-vint-decoder-unify (issue #1638, Epic J / J4)

> Sequenced AFTER I1 (#1623) / I2 (#1624) — their `vint_length_corpus_audit_tests`
> and `_1624` regression tests are this refactor's spec and stay green unchanged.
> Pure consolidation with identical decode semantics; write the equivalence proofs
> first (RED where they can be), then delete the duplicates.

## 1. TDD equivalence proofs first (surface: `parser::vint::{decode_unsigned, decode_signed}`)
- [ ] 1.1 Round-trip proptest (`parser/vint.rs` tests): for all `u64` /`i64`,
      `serialization::vint::{encode_unsigned, encode_signed}` → `decode_unsigned` /
      `decode_signed` → identity of both value and consumed length.
- [ ] 1.2 Corpus differential unit test: snapshot the pre-refactor unsigned
      bit-assembly and assert the new `decode_unsigned` agrees on value AND consumed
      length (or both error) across generated lead bytes × continuation patterns ×
      truncation boundaries.
- [ ] 1.3 Truncation/framing tests on the new pair: empty → `Err`; bare `0x80`/`0xC0`
      → `Err`; complete value alone vs with trailing junk → identical value + exact
      width consumed (mirror the I2 `_1624` guarantees at the decoder level).

## 2. Add the one canonical decoder (surface: `parser::vint`)
- [ ] 2.1 Add `VIntError`, `decode_unsigned`, `decode_signed` to `parser/vint.rs`
      mirroring `serialization/vint.rs`: single `leading_ones()`, `u64::from_be_bytes`
      assembly (no index loop / no `needless_range_loop` `#[allow]`), `split_at`
      framing, uniform `#[inline]`. Guard the width math (no shift/`<<` overflow;
      `extra ∈ 0..=8`).

## 3. Repoint the read-side callers onto the one implementation (surface: `parse_vuint` / `parse_vint` / `parse_unsigned_vint32`)
- [ ] 3.1 Rewrite `parse_vuint` as a thin nom adapter over `decode_unsigned`.
- [ ] 3.2 Rewrite `parse_vint` as a thin nom adapter over `decode_signed` — removing
      the `fixed → zigzag` double-decode fallback.
- [ ] 3.3 Rewrite `parse_unsigned_vint32` to keep its `leading_ones > 4` width cap,
      then delegate assembly to `decode_unsigned` and `u32::try_from` the result.
- [ ] 3.4 Confirm `parse_vint_length` / `parse_vint_length_signed` are unchanged
      (they already delegate) and their `MAX_VINT_LENGTH` cap is retained.

## 4. Delete the duplicates / dead decoders
- [ ] 4.1 Delete `parser/vint_fixed.rs`; remove `pub mod vint_fixed;` and its doc
      references from `parser/mod.rs`.
- [ ] 4.2 Delete `parse_zigzag_vint`, `parse_cassandra_vint_format`,
      `parse_custom_vint_format`, `detect_ascii_corruption`, and the
      `vint_fixed`-coupled `parse_vint_cassandra` / `encode_vint_cassandra`
      (verified 0 callers).
- [ ] 4.3 Delete the tests that exercised the removed internals
      (`test_detect_ascii_corruption_patterns`,
      `test_parse_zigzag_vint_extended_consumes_exactly_nine_1624`); the framing/
      truncation intent is preserved by the `parse_vint`-level `_1624` tests.
- [ ] 4.4 `rg` sweep for every removed symbol → confirm no remaining caller in
      `cqlite-core/src`, benches, tests, or sibling crates.

## 5. Bench + gate (surface: `benches/decode_bench.rs`, `benches/perf-gate.json`)
- [ ] 5.1 Add a `decode/vint_decode` criterion group in `decode_bench.rs` that
      decodes a fixed table of 1..=9-byte signed/unsigned VInt buffers through
      `decode_unsigned` / `decode_signed`, running without `cli-helpers` /
      `bench-internals`.
- [ ] 5.2 Add a `decode/vint_decode` entry to `perf-gate.json` with a `threshold_pct`
      and a `_note` (no base baseline on first landing — handled as "new").

## 6. Validation
- [ ] 6.1 Fast iteration gate on each fix round:
      `CQLITE_ALLOW_FILE_GROWTH=1 CQLITE_DATASETS_ROOT=<main-checkout>/test-data/datasets
      scripts/agent-gate.sh --lite` → RESULT: PASS.
- [ ] 6.2 33-table parity + smoke green (`CQLITE_DATASETS_ROOT` set) — decode results
      byte-for-byte unchanged.
- [ ] 6.3 `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` clean —
      no `manual_range_contains`, no re-introduced `needless_range_loop` `#[allow]`,
      no dead-code warnings for the removed symbols.
- [ ] 6.4 Full `scripts/agent-gate.sh` PASS once before merge (run by the lead) —
      paste the AGENT-GATE SUMMARY block.
- [ ] 6.5 `spec-auditor` (C) PASS against
      `openspec/changes/read-vint-decoder-unify/specs/**` (every requirement
      satisfied with a public-surface / parity test as evidence).
- [ ] 6.6 `roborev review --branch --base origin/main` clean before merge.
