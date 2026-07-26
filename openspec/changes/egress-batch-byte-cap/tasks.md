# Tasks — egress-batch-byte-cap (issue #2825, T4/M11)

Placement note (campsite rule, epic #1116): `cqlite-flight/src/producer.rs` (3,330),
`cqlite-flight/src/service.rs` (2,036) and `cqlite-core/src/export/arrow_convert.rs`
(2,596) are **already over** the ~800-line source threshold, so the `file-size`
ratchet will trip on even a one-field addition. Keep edits to those files to the
absolute minimum (a field, a parameter, an accumulator); put all logic in the new
modules below; re-run the gate with `CQLITE_ALLOW_FILE_GROWTH=1` and leave a note
linking #1116. `cqlite-flight/src/streaming.rs` (768/800) has ~35 lines of
headroom — put nothing there. `cqlite-flight/src/streaming_tests.rs` is 1,336/1,500
— new tests go in the new sibling test file.

## 1. TDD guards (must FAIL on main first)

- [ ] 1.1 Wide-row byte-cut test: with the new synthetic wide-row fixture and a cap
      smaller than a full row-capped batch, assert every non-final batch has
      **strictly fewer than `batch_size` rows** and that more than one batch is
      emitted. Surface: `MergeProducer` full-scan (`producer.rs`). FAILS on main.
- [ ] 1.2 Narrow no-regression test: with a narrow fixture and the default cap,
      assert every non-final batch has **exactly `batch_size` rows** and that the
      boundaries equal those at an effectively unbounded cap.
- [ ] 1.3 Both-paths test: repeat 1.1 through the `producer_stream.rs` `flush` path,
      asserting the same boundary rule. FAILS on main.
- [ ] 1.4 One-row floor test: a single row wider than the whole cap is emitted as a
      one-row batch; N consecutive over-cap rows yield exactly N one-row batches;
      caps of `0` and `1` yield one row per batch. No drop, no hang. FAILS on main.
- [ ] 1.5 Capacity-tolerance test: every emitted batch satisfies
      `get_array_memory_size() <= CAPACITY_FACTOR * cap + slack`, and every
      multi-row batch's **payload** bytes (buffer lengths, recursive) are `<= cap`.
      FAILS on main.
- [ ] 1.6 Estimator conservatism property test: over a shape corpus (fixed-width,
      `text`, `blob`, `list`/`set`, `map`, `tuple`/UDT, all-null, empty string,
      empty collection), assert
      `Σ estimate_arrow_row_bytes(..) >= payload_bytes(rows_to_record_batch(..))`
      for every shape. Surface: `cqlite_core::export`.
- [ ] 1.7 Knob test: two distinct `--max-batch-bytes` values produce
      correspondingly different batch counts/boundaries (proves non-decorative).
      FAILS on main.
- [ ] 1.8 End-to-end wiring test: `CQLITE_MAX_BATCH_BYTES` governs a **real
      streamed `do_get`** through the service surface — wiring evidence, not a
      helper-only unit test. FAILS on main.
- [ ] 1.9 Result-invariance test: capped vs effectively-unbounded runs concatenate
      to identical rows, order, values and Arrow schema; total row count is
      invariant across a descending series of caps.

## 2. Estimator (`cqlite-core`, NEW file)

- [ ] 2.1 Add `cqlite-core/src/export/arrow_size.rs` with
      `#[cfg(feature = "arrow")] pub fn estimate_arrow_row_bytes(columns: &[ColumnInfo], row: &QueryRow) -> usize`,
      walking `columns` (not the whole `values` map) the way
      `export/arrow_columnar.rs:59` `transpose_columns` resolves cells.
- [ ] 2.2 Content-bytes walk: hardened per the `memtable.rs:227` precedent —
      iterative worklist, node budget, `saturating_add` throughout, fails closed to
      a saturated width. No `unwrap()`/`expect()`. No panic on nesting depth.
- [ ] 2.3 Structural addends: a named `ARROW_CELL_OVERHEAD_BYTES` per cell (offsets
      entry + validity bit rounded up) for variable-width Arrow outputs, and a
      per-**element** addend for `List`/`Set`/`Map`/`Tuple`/UDT child arrays. This
      is the term every existing estimator omits and is what makes the estimate
      conservative rather than an under-count.
- [ ] 2.4 Re-export from `cqlite-core/src/export/mod.rs` beside
      `rows_to_record_batch` (mod.rs:57). Do **not** change
      `cqlite-core/src/query/result_budget.rs` visibility — see design §(d).
- [ ] 2.5 Add a test-only payload-bytes oracle (sum of Arrow buffer **lengths**,
      recursive over child data) used by 1.5/1.6; keep it test-scoped so it is not
      new public surface.

## 3. Cap mechanism (`cqlite-flight`, NEW file)

- [ ] 3.1 Add `cqlite-flight/src/batch_bytes.rs` with
      `pub const DEFAULT_MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;`,
      `pub const ENV_MAX_BATCH_BYTES: &str = "CQLITE_MAX_BATCH_BYTES";`, the
      published `BATCH_BYTES_CAPACITY_FACTOR` + per-column slack constants, and a
      small `BatchByteCap` accumulator (`push(width) -> ShouldFlush`, `reset()`),
      documented with the §(b)/§(c) rationale.
- [ ] 3.2 Add `cqlite-flight/src/batch_bytes_tests.rs` wired via `#[path]` (the
      `admission.rs`/`admission_tests.rs` precedent), holding the accumulator unit
      tests and the wide-row fixture tests.
- [ ] 3.3 Push-then-test ordering so a batch is cut only when the buffer is
      non-empty — the one-row floor of design §(e). Clamp semantics for `0`/`1`
      documented next to the `batch_size.max(1)` precedent.

## 4. Producer wiring (both paths — minimal edits)

- [ ] 4.1 `cqlite-flight/src/producer.rs`: add the cap field to `MergeProducer`
      (beside `batch_size`, `:328`), set it in the single `with_spec` funnel
      (`:409-422`), accumulate at the push site (`:949`) and extend the trip at
      `:951` to the dual condition. Reset the accumulator in `flush_buffer` (`:1222`).
- [ ] 4.2 `cqlite-flight/src/producer_stream.rs`: the same at `:118` / `:204` /
      `:206` / `:87`. Both paths share the `batch_bytes` accumulator — no duplicated
      boundary logic.
- [ ] 4.3 Confirm the aggregate and point-read routes reach the same accumulator (or
      document why a route emits no multi-row batch).

## 5. Configuration (CLI → service → producers)

- [ ] 5.1 `cqlite-flight/src/main.rs`: add
      `#[arg(long, env = ENV_MAX_BATCH_BYTES, default_value_t = DEFAULT_MAX_BATCH_BYTES)] max_batch_bytes: usize`
      to `struct Args` (`:25-51`) with a doc comment carrying the sizing rationale,
      mirroring `--max-concurrent-scans` (`:38-44`).
- [ ] 5.2 Thread it into `CqliteFlightService` (`service.rs:286` field,
      `:314`/`:320` constructors) and on into `MergeProducer::with_spec`
      (`service.rs:427`). Per design §(f) the cap is **on by default on every
      construction path**, unlike `Admission::unconstrained()` — record that
      divergence in the constructor doc comment.
- [ ] 5.3 Add `max_batch_bytes` to the startup log line (`main.rs:109-116`).

## 6. Fixtures

- [ ] 6.1 Add a deterministic, self-contained synthetic wide-row fixture (wide blob
      and/or many-column shape) beside `cqlite-flight/src/test_fixtures.rs`'s
      existing shapes. It MUST NOT depend on the fetched `test_wide_rows` dataset.
- [ ] 6.2 Give every wide-row test a non-vacuity assertion (rows > 0 **and**
      batches > 1) that runs before any byte assertion.

## 7. Performance evidence (outside the correctness path)

- [ ] 7.1 Add the ~1.0–1.1× throughput comparison as an `#[ignore]`d test annotated
      `perf-gate-allow`. **No** wall-clock threshold anywhere in the correctness
      path (#2642 / `roborev-lints`).
- [ ] 7.2 Confirm `cqlite-flight/tests/issue_1494_producer_mem_budget.rs` still
      passes under `--features dhat-heap` (its `BATCH_SIZE = 8192` / narrow ~20 B/row
      fixture is a single sub-cap batch, so the cap must be a no-op there).

## 8. Docs

- [ ] 8.1 Correct the **single** M11 line at
      `docs/architecture/throughput-program-2026-07.md:385` — `57,344-row` →
      the production `~49,152-row` residency. Do **not** touch the five dated
      `docs/research/` snapshots.
- [ ] 8.2 Document `--max-batch-bytes` / `CQLITE_MAX_BATCH_BYTES` in the flight/ops
      knob docs, including the payload-vs-capacity currency and the published
      worst-case per-batch capacity bound that #2821 composes on.
- [ ] 8.3 Post the revised B4 arithmetic (design §(b)) as a comment on **issue
      #2821**, which is parked on this change: its ceiling must be budgeted in
      capacity currency, not payload.

## 9. Endgame

- [ ] 9.1 `--lite` green each fix round (summary-file redirect).
- [ ] 9.2 `rust-reviewer` + roborev on the lite-green diff, **before** the full gate.
- [ ] 9.3 ONE full `scripts/agent-gate.sh` inside `flow-closer`; record the
      `AGENT-GATE SUMMARY`. Note the `CQLITE_ALLOW_FILE_GROWTH=1` justification
      (#1116) if the ratchet trips on `producer.rs`/`service.rs`.
- [ ] 9.4 `spec-auditor` (C) against `openspec/changes/egress-batch-byte-cap/specs/**`.
- [ ] 9.5 Final roborev clean → arm `gh pr merge --auto --squash --delete-branch`
      after `scripts/flow/premerge-assert.sh`.
- [ ] 9.6 `openspec archive egress-batch-byte-cap` at finalize.
