# Tasks — Zero-copy value extraction (K5, issue #1644)

Staged; each stage is parity-green before the next. Anchors are `main`-relative (post-E1, post-#1940)
and will drift — re-grep before editing. Write the measurement/red tests FIRST.

## Stage 0 — measurement + guards (must fail / be unmeasurable on `main`)
- [ ] 0.1 Wire/extend the #2075 dhat **allocs-per-row** lane for a text-heavy decode+row-assembly path;
  record the `main` baseline (the win target). (value-zero-copy-decode)
- [ ] 0.2 Add the window-borrow **work-probe** (bytes-copied-into-values) and a **chunk-retention**
  test harness; both fail / are unmeasurable on `main`. (scan-window-borrow, value-zero-copy-decode)
- [ ] 0.3 Confirm the `size_of::<Value>() <= 40` pin and the serde-round-trip / comparator-ordering
  golden tests are in place as the parity net. (value-bytes-representation)

## Stage 1 — representation: Bytes-back Text/Blob/Varint/Inet
- [ ] 1.1 Change `Value::{Text,Blob,Varint,Inet}` to `Bytes`-backed in `cqlite-core/src/types.rs:30`;
  keep `Decimal.unscaled: Vec<u8>`. Keep the ≤40 pin (`types.rs:98`). (value-bytes-representation)
- [ ] 1.2 Preserve accessors `as_str`/`as_bytes`/`as_inet_bytes`/`len`/`is_empty`
  (`types.rs:825-867`); `Text` `Bytes` validated UTF-8 at construction so `as_str` is a cheap view.
  (value-bytes-representation)
- [ ] 1.3 Custom serde for the four variants (serialize `Text` as string, `Blob`/`Varint`/`Inet` as
  bytes) — byte-identical wire format; add the round-trip test. (value-bytes-representation)
- [ ] 1.4 Add ergonomic constructors / `From<&str|String|Vec<u8>|&[u8]|Bytes>` and enumerate the
  breaking changes in the change docs. (value-bytes-representation)
- [ ] 1.5 Fix construction/match sites across core (write/serialization/parser/export), the Python
  (`bindings/python/src/value.rs:34-35`) and Node (`bindings/node/src/value.rs:188-191`) conversion
  arms, and CLI arms — mechanical. Both binding suites + CLI tests green. (value-bytes-representation)
- [ ] 1.6 33-table parity + query-semantics oracle + serde + comparator ordering green (still copying at
  decode — representation only). (value-bytes-representation)

## Stage 2 — window borrow API (D1, the #1940-deferred consumer)
- [ ] 2.1 Make `WindowCursor` (`reader/window_cursor.rs:28`) backing an enum `Borrowed(Bytes)` /
  `Stitched(Vec<u8>)`; on refill of a fully-consumed window, REPLACE the backing with the incoming
  chunk `Bytes` (move, no `extend_from_slice`); straddle → stitch as today. Keep `as_slice()` for the
  parser scanning path unchanged. (scan-window-borrow)
- [ ] 2.2 Add `borrow(range) -> ValueBytes` (Borrowed → `Bytes::slice`; Stitched →
  `Bytes::copy_from_slice`); preserve the #1589 byte-movement contract and window-size bound.
  (scan-window-borrow)
- [ ] 2.3 Switch the streaming value-materialization decode sites to `borrow`:
  `row_decoder/raw_value.rs`, `row_decoder/cell_value.rs`, `parsing/raw_type_value.rs`,
  `parsing/complex_column.rs`, `.../udt.rs`. UTF-8 validate `Text` in place. (value-zero-copy-decode)
- [ ] 2.4 Prove: allocs-per-row lane drops vs `main`; window-borrow probe shows ≈0 bytes copied on the
  non-straddling path; predicate-rejected value copies nothing; straddle fixture decodes byte-identically;
  33-table parity green. (value-zero-copy-decode, scan-window-borrow)

## Stage 3 — comparators + byte-comparable
- [ ] 3.1 Borrow in `parsing/comparator_value_parsing.rs:168,172,216,224`, `parsing/byte_comparable.rs:245`,
  `parsing/custom_scalar.rs:48,64`. (value-zero-copy-decode)
- [ ] 3.2 Comparator-ordering scenario byte-identical (NaN-last, `-0.0 < +0.0`); parity green.
  (value-zero-copy-decode)

## Stage 4 — export + FFI boundary
- [ ] 4.1 Verify Arrow append (`export/arrow_convert.rs:654,674,1545,1581`) borrows-through into the
  columnar builder with no double copy; Arrow arrays own their materialized memory. (value-zero-copy-decode)
- [ ] 4.2 Confirm bindings copy at the FFI boundary (they already do) and no chunk `Bytes` escapes FFI;
  binding-lifetime test (value valid after scan/chunks dropped). (value-zero-copy-decode)

## Stage 5 — retention boundary (D2)
- [ ] 5.1 Implement + document `Value::into_owned()` compaction
  (`backing.len() > payload.len() + RETENTION_SLACK`, `RETENTION_SLACK` documented/tunable) and apply it
  at core-internal retention/buffer points (materialized result sets, LIMIT/sort/dedup, caches).
  (value-zero-copy-decode)
- [ ] 5.2 Retention test: a tiny long-lived value releases its 64 KB chunk; a large streaming value
  borrows without compaction. (value-zero-copy-decode)

## Stage 6 — gate + audit + review (definition of done)
- [ ] 6.1 Full `scripts/agent-gate.sh` ONCE → PASS (SUMMARY pasted). `RUSTFLAGS="-D warnings"` clean; no
  `unwrap()`/`expect()` in library code.
- [ ] 6.2 spec-auditor **C** anchored to `openspec/changes/zero-copy-value/specs/**` → PASS (every
  requirement satisfied by a public-surface test).
- [ ] 6.3 roborev clean (test/docs-only rounds re-certify with `--delta`).
- [ ] 6.4 #2075 dhat allocs/row win posted; predicate-eval + sort throughput micro-bench posted.
- [ ] 6.5 Close #1644 on merge; reconcile/archive the ancestor `value-representation-v2` change (its E1
  + #1940 stages already shipped) per the owner's decision (see proposal open forks). Archive
  `zero-copy-value`.
