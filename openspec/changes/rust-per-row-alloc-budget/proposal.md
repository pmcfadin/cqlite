# Proposal: Rust-side per-row allocation budget (counting-allocator ratchet) + L5 FxHash row map

## Milestone / theme
0.17 (milestone #14) — scan-path throughput program, epic #2817 manifest item **M4**. Closes issue #1883.

## Routing
**Design-driven** (new test lane + measured baseline + hot-path structural change). The alloc-count
_verdict_ itself is oracle-ish once the harness is shaped (an exact allocation count is a hard number),
but the harness design, the baseline choice, and the L4/L5 scope are design decisions — hence OpenSpec.

## Problem

#1449 added binding-layer per-row budget tests (Python `tracemalloc`, Node V8 `heapUsed` delta, a Rust
`#[cfg(test)]` Set/Map ctor-lookup counter) and **empirically proved** those tests **cannot observe** two
of the W-fixes they were meant to pin:

- **#1447 (clone→move)**: the reverted clone is a transient *Rust-heap* allocation freed inside
  `executeNative()`/`execute()` before any post-execute V8/Python heap sample. Reverting #1447 moved the
  Node median 1335.0 → 1333.8 B/row — noise.
- **#1445/#1446 (key interning)**: emitting fresh key strings per cell didn't move the number either — V8
  internally dedups property-name strings; Python `tracemalloc` was similarly blind.

The binding-layer budgets legitimately pin the **gross per-row value graph** (a value-graph-doubling
regression goes RED) and the **Set/Map ctor-lookup count** — but the **transient native-allocation**
dimension is physically unobservable from the JS/Python heap. A regression that re-introduces a per-row
clone or drops key interning would ship green.

## Goal

Add a **Rust-side per-row allocation budget** that CAN observe transient native allocations, using the
**existing in-crate `test_alloc_probe::measure`** counting allocator (`cqlite-core/src/lib.rs:82`), so the
row-conversion hot path (`build_row_from_scan_cached`, `row_build.rs:227`) gets an **allocation-count
ratchet**: reverting #1447 (into_iter→iter().clone()) OR dropping the key interning makes the test go RED;
restoring it green. Baseline is **measured and asserted as an exact/≤ count**, not a fuzzy byte budget.

Fold in the one cheap, profile-supportable row-hot-path win from epic #2817 M4:

- **L5 — FxHash `row_values` map**: swap the per-row `HashMap<Arc<str>, Value>` (SipHash,
  `row_build.rs:246`) to `FxHashMap` using the **already-vendored** `rustc-hash = "1.1"`
  (`cqlite-core/Cargo.toml:76`, already used at `aggregation.rs:20`). SipHash disappears from the row hot
  path. Expected ~1.04× narrow & wide; the alloc-budget test does not regress (hasher swap is
  alloc-neutral — the ratchet's job here is to prove L5 introduces no new per-row allocation).

## L4 — surfaced, not committed (a design question for the owner)

The manager order (2026-07-22) bundled **L4 — RowKey Arc hoist** ("hoist the `RowKey(Arc<[u8]>)` build
outside `for entry in rows`"). Investigation of the current tree shows the L4 premise is **largely already
satisfied**:

- The partition-constant work (decoding pk columns from `key.0`) is **already hoisted** across a
  partition's rows by `PartitionKeyCache` (#1817), created once per row loop and threaded through
  (`execute.rs:638`, `streaming.rs:138`).
- The remaining per-row `RowKey(Arc<[u8]>)` is **genuinely per-row** — each row carries its own key from
  the scan window (`scan_stream_windowed.rs`), and inside `build_row_from_scan_cached` the key is only
  **read** (`pk_cache.columns_for(&key.0, …)`) then **moved** into `QueryRow.key` (`row_build.rs:279`), not
  re-allocated.

So there is **no obvious remaining per-row `Arc` allocation to hoist** in the conversion function itself.
The honest position: **the new alloc-budget ratchet is the correct instrument to adjudicate L4** — it will
report the exact per-row allocation count, and *if* that count reveals a hoistable per-row `Arc` clone, L4
becomes a concrete follow-up; if the count is already minimal, L4 is credited **1.0× / no-op** (which the
manager order already anticipated: "1.0× credited on single-row-partition — do not claim a field win the
profile can't support"). **This proposal delivers the ratchet + L5, measures the per-row count, and reports
the L4 verdict from that measurement** rather than pre-committing a hoist the code may not need.

## Approach (chosen — see design.md for alternatives)

1. Add a `#[cfg(test)]` per-row allocation-budget test in `cqlite-core` that drives the **real public**
   `build_row_from_scan_cached` conversion (mod.rs:99) inside `test_alloc_probe::measure(...)` over a wide
   result, asserting `allocations / row <= MEASURED_BASELINE`. Mirrors the existing precedent
   `lookup.rs:822 cartesian_product_builds_each_combo_in_one_allocation`.
2. Add a **negative-control** assertion structure: the test is shaped so that re-introducing a per-row
   clone (the #1447 revert) or a per-cell fresh-key allocation (the #1445/#1446 revert) pushes the count
   over budget — documented in-test with the exact deltas.
3. Apply **L5** (FxHashMap for `row_values`) and confirm the alloc budget is unchanged (hasher swap adds
   no per-row allocation) while SipHash leaves the row hot-path profile.
4. Document the **measured baseline** (allocs/row for narrow + wide fixtures) in the test and in the M4
   section of `docs/architecture/throughput-program-2026-07.md`.

## Non-goals

- **NOT** a dhat-heap lane. `test_alloc_probe::measure` (counting allocator) and `dhat::Alloc` are
  **mutually-exclusive** `#[global_allocator]`s (one per binary); the counting allocator is the right tool
  for a deterministic *count* ratchet and is already gated into the default test build. dhat stays reserved
  for the byte-level streaming heap-stats lane (`streaming_dhat_test.rs`).
- **NOT** a new alloc harness. Reuse `test_alloc_probe::measure` — "coordinate, don't build a second
  harness" (issue note).
- **NOT** the binding-layer budgets — those stay as the gross-value-graph + ctor ratchets (#1449); this
  closes the transient-native-alloc gap they physically can't cover.
- **NOT** an unconditional L4 hoist — L4 is adjudicated by the new ratchet's measurement (see above).
- **NOT** a change to the no-heuristics decode path, write path, or any public API signature.

## Doctrine impact

- **Wiring-evidence**: the ratchet drives the real public `build_row_from_scan_cached` conversion surface,
  not a helper — a value-graph or clone regression on the row hot path fails a test.
- **Gate**: adds a `cqlite-core` test target; no new gate component required (runs under the existing
  core-tests component, `state_machine` feature already default).
- Updates `docs/architecture/throughput-program-2026-07.md` §7 M4 with the measured baseline + L4 verdict.
