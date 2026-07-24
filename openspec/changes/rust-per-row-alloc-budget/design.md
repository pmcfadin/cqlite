# Design: Rust per-row allocation-budget ratchet + L5 FxHash row map

## Context

`build_row_from_scan_cached` (`cqlite-core/src/query/select_executor/row_build.rs:227`) is the single
per-row conversion the query engine runs for every scanned row before it reaches the CLI / bindings. It:

- builds a per-row `HashMap<Arc<str>, Value>` (`row_build.rs:246`, capacity-hinted), currently **std
  SipHash**;
- inserts projected cell values (`.into_owned()`, `row_build.rs:259`) — this is where #1447 reverted a
  clone into a move;
- inserts partition-key columns from the **already-memoized** `PartitionKeyCache` (`row_build.rs:270`,
  #1817) — the partition-constant decode is hoisted here, not per row.

Two independent alloc-observability facts drive the design:

1. **The counting allocator already exists.** `cqlite-core/src/lib.rs:82` defines `test_alloc_probe` with
   a thread-local `CountingAllocator` set as `#[global_allocator]` under
   `#[cfg(all(test, feature = "state_machine", not(feature = "dhat-heap")))]`, exposing
   `measure<R>(f) -> (u64 allocations, R)`. `state_machine` is a **default** feature, so this is live in
   the ordinary `cargo test -p cqlite-core` build. Precedent test:
   `lookup.rs:822 cartesian_product_builds_each_combo_in_one_allocation` already asserts allocation counts
   with it.
2. **dhat is the wrong tool here and is mutually exclusive.** `dhat::Alloc` (lib.rs:144) is a second
   `#[global_allocator]`, only under `feature = "dhat-heap"` (non-default), and a binary may have exactly
   one global allocator. It measures *bytes* (`HeapStats`) for the streaming lane, not a deterministic
   per-call *count*. A count ratchet wants the counting allocator.

## Decision 1 — Instrument with `test_alloc_probe::measure`, not dhat

**Chosen.** Reuse the in-crate counting allocator. Rationale:

- It yields a **deterministic integer** (`allocations`), so the ratchet is an exact `<=` / `==` assert, not
  a noisy byte budget — this is exactly why the binding-layer byte budgets failed to observe #1447.
- It is **already in the default test build** (`state_machine` default) — no new feature, no new gate lane,
  no second harness (honoring the issue's "coordinate, don't build a second harness").
- It has a **working precedent** (`lookup.rs:822`) to copy structure from.

**Rejected — dhat-heap lane** (the "parser epic H H2" pattern). Would require: a non-default feature build
(`--features write-support,dhat-heap`), a separate CI lane, byte-granular (noisier) assertions, and it
**conflicts** with the counting allocator (can't have both global allocators). The M4 issue floated dhat
"or a custom counting global allocator" — the custom counting allocator already exists and is strictly
better for a count ratchet. dhat stays reserved for `streaming_dhat_test.rs` (byte-level streaming).

## Decision 2 — Drive the real public conversion surface (wiring-evidence)

**Chosen.** The test calls the public `build_row_from_scan_cached` (re-exported at
`select_executor/mod.rs:99`) inside `measure(...)`, over a synthesized wide cell set + a real
`PartitionKeyCache`, asserting allocs/row. This is the actual per-row surface the engine runs — not a
private helper — so a regression on the hot path (a re-introduced clone, a lost intern) is observed.

Two fixtures:
- **narrow** (few columns) — pins the fixed per-row cost;
- **wide** (many columns) — pins the per-cell scaling, where a per-cell key allocation (the #1445/#1446
  revert) shows up as `allocs growing with column count`.

**Negative controls, documented in-test** (not committed as code, described as comments with measured
deltas): reverting #1447 (`.iter().map(clone)` instead of `into_iter`) adds N per-row allocations;
dropping key interning adds one alloc per projected cell. The baseline is set just below those so the
revert trips it.

## Decision 3 — Baseline: measured `<=`, asserted per-row, tolerance-free where deterministic

**Chosen.** Run the harness, record the observed `allocations` for narrow + wide, and assert
`allocs <= observed` (with the count divided per row where the fixture is multi-row). The counting
allocator is deterministic for a fixed input, so no statistical tolerance is needed (unlike the V8/tracemalloc
byte budgets). Document the exact numbers in-test and in the throughput-program doc. If a platform-dependent
allocation (e.g. capacity rounding) proves non-deterministic across targets, fall back to a small explicit
slack **documented with the reason** — not a silent fudge.

## Decision 4 — L5 (FxHashMap) folded in; L4 adjudicated by measurement, not pre-committed

**L5 (chosen, committed):** change `row_build.rs:246` `HashMap<Arc<str>, Value>` →
`rustc_hash::FxHashMap<Arc<str>, Value>` (dep already present, already used at `aggregation.rs:20`).
`FxHashMap::default()` + `.reserve(cap)` or `FxHashMap::with_capacity_and_hasher(cap, Default::default())`
preserves the capacity hint. The alloc-budget test asserts this swap adds **no** per-row allocation (hasher
is inline, no heap state) — so L5's win is a hashing-cost win the profile shows, with the ratchet proving
alloc-neutrality. Same-signature return type at the call boundary is preserved (callers iterate the map;
`FxHashMap` is a drop-in `HashMap<_,_,FxBuildHasher>`).

**L4 (surfaced, not committed):** per the Problem section — the partition-constant decode is already
hoisted (#1817) and the residual `RowKey(Arc<[u8]>)` is genuinely per-row and moved. The new ratchet
**measures** whether any hoistable per-row `Arc` clone remains. Outcome recorded in the throughput doc:
- if the measured count reveals a hoistable clone → file a concrete L4 follow-up issue with the exact
  site;
- if the count is already minimal → L4 credited **1.0× / no-op**, matching the manager's own
  "1.0× credited … do not claim a field win the profile can't support."

This is the design question for the owner at Seam 1: **approve delivering the ratchet + L5 now and letting
the ratchet adjudicate L4**, vs. requiring a speculative L4 hoist up front.

## Risks / mitigations

- **Counting-allocator determinism across platforms** — mitigate with the documented-slack fallback
  (Decision 3); the precedent test (`lookup.rs:822`) already runs cross-platform in CI, so the mechanism is
  proven.
- **FxHashMap iteration-order change** — `row_values` is consumed as a map (keyed lookups / full
  iteration into the binding object); no code depends on `HashMap` iteration order (already
  non-deterministic). Confirmed no ordering assertion downstream.
- **Feature gating** — the test must be gated `#[cfg(all(test, feature = "state_machine", not(feature = "dhat-heap")))]`
  to match the allocator's gate, else it fails to find `measure` under a dhat build.
