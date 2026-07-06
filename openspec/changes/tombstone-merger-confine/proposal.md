## Why

The July 2026 read-path audit (`docs/reports/read-path-performance-audit-2026-07-01.md`
§Epic G, child **G4** — "Confine the legacy") flagged two legacy hazards sitting off the
default read path:

1. **`TombstoneMerger`** (`cqlite-core/src/storage/sstable/tombstone_merger.rs`) — the
   module carries an `apply_range_tombstones` / `range_tombstone_applies` pair whose inner
   loop is **O(entries × range-tombstones)** nested iteration, plus a spread of legacy
   public helpers. The module is already `#[cfg(feature = "tombstones")]`-gated, but its
   `get()` use means the `tombstones` build falls off the C1/C4 point-read fast path, and
   the quadratic method is the specific complexity the audit named.
2. **Legacy duplicate-work parallel table scan** (`executor.rs`, "N workers each scan the
   full table and keep 1/Nth"). **Already retired** by issue #1691: `execute_table_scan`
   now issues a SINGLE bounded `scan_stream` pass and ships a work-counter test
   (`table_scan_parallel_branch_issues_one_whole_table_pass`, asserting exactly 1
   whole-table pass). No further work is required for hazard 2 — this change records that
   and leaves the proof in place.

**Routing: design-driven, owner-pre-decided.** The audit is the source of truth. This
change encodes G4's locked posture (standing owner Seam-1 approval, 2026-07-06 drain
directive) with **no new design latitude**. The audit offered two mechanics for hazard 1:
(a) replace the `get()` use with the KWay single-partition point path *if that path
exists*, else (b) confine the merger behind `tombstones` with doc scoping.

**We take (b), confinement.** Reason (stated per the issue's "state the choice + reason"
requirement): **no single-key multi-candidate KWay point path exists to reuse.** The
KWay merger (`scan_merge::kway_merge_token_order`) is scan-oriented — it merges
token-ordered *row streams* into a scan result — and the multi-candidate fast path
(`scan_partition*`) is itself `#[cfg(not(feature = "tombstones"))]`-gated. The `tombstones`
build's `get()` performs cross-generation LWW/tombstone resolution for a *single key*,
which the non-tombstones `get()` does NOT do (it returns first-found). Per the issue's
"reuse, don't rewrite" guidance and the guardrail that the `tombstones` feature's
**SEMANTICS are parity-pinned — only mechanics may change**, rewriting a point-KWay path
is out of scope; confinement + honest complexity docs is the minimal correct move.

Milestone: **v0.14 perf wave** (Epic G capstone, Wave 4) / maintenance. No change to any
read result.

## What Changes

- **Delete the named quadratic hazard**: remove `TombstoneMerger::apply_range_tombstones`
  and its helper `TombstoneMerger::range_tombstone_applies` — the O(entries × tombstones)
  nested loops — plus their unit test. Both have **zero production call sites** (deadness
  proven by `rg`; the verification is pasted in the PR per the issue's TDD requirement).
  Removing them eliminates the specific complexity the audit named and shrinks the file.
- **Confine the retained surface behind `tombstones` with doc scoping**: add a
  module-level doc block that states (i) the module is legacy, `tombstones`-feature-only,
  and OFF the default C1/C4 point-read fast path; (ii) the complexity of each retained
  live method (all ≤ O(n log n) after the deletion); (iii) why the retained complexity is
  acceptable there (parity-pinned semantics; per-key generation sets are low-cardinality;
  not on the hot default path); and (iv) the future consolidation direction (fold the
  `get()` use into a single-key multi-candidate KWay point path once one exists —
  reuse, don't rewrite).
- **Fix the `unwrap()` hard-rule violation** in `TombstoneMerger::new()`
  (`SystemTime::now().duration_since(UNIX_EPOCH).unwrap()`): replace with a graceful
  fallback (`unwrap_or_default()` → epoch), matching the existing no-unwrap fallback
  already used in `integrity.rs::filter_tombstone`. This is a mechanic, not a semantic
  change: a clock before 1970 (the only failure) yields `current_time = 0`, under which no
  TTL/tombstone is treated as expired — a safe, documented degradation.
- **Record hazard 2 as already-done**: the legacy parallel scan was retired by #1691; its
  work-counter proof (`scans issued per plan == 1`) is retained, not re-implemented.

## Non-goals

- **NOT replacing `get()` with a KWay point path (option a).** No reusable single-key
  multi-candidate point path exists; rewriting one is out of G4's "reuse, don't rewrite"
  scope and would risk the parity-pinned semantics guardrail.
- **NOT changing any `tombstones`-build read RESULT.** The reconciliation semantics of the
  retained live methods (`merge_generations`, `fast_tombstone_check`,
  `batch_merge_with_tombstones`) are unchanged; the `test_deltas`/clustered full-scan
  parity stays green.
- **NOT a general dead-code sweep of the module.** Other proven-dead public helpers
  (`merge_row_entries`, `resolve_conflict`, `merge_cell_tombstones`,
  `identify_garbage_collectible_tombstones`, `merge_collection_with_tombstones`, and the
  dead reader method `filter_with_multi_generation_merge`) are noted for a separate G1
  "delete the dead" sweep, NOT deleted here. This change removes only the audit-named
  quadratic path.
- **NOT touching the default (`not(tombstones)`) read path**, the parallel-scan retirement
  (#1691, done), or any other Epic G child.

## Impact

- **Public surface (semver):** `apply_range_tombstones` / `range_tombstone_applies` are
  `pub` on a `#[cfg(feature = "tombstones")]` module that is NOT in `cqlite-core` defaults
  and NOT enabled by any binding or the CLI. Their removal is invisible to every shipped
  build; deadness is `rg`-proven. No default-build surface changes.
- **No-heuristics mandate:** unaffected — this removes dead machinery, documents, and
  fixes an unwrap; no byte-content inference is introduced or removed.
- **Code quality:** removes a `unwrap()` from library code (hard rule) and reduces
  `tombstone_merger.rs` size (campsite rule).
- **Feature-matrix hygiene:** the crate must build+test both WITH and WITHOUT `tombstones`
  after the change (the retained live surface is exercised in both the unit tests and the
  `--all-features` full-scan parity test).
