# Design — Confine the legacy `TombstoneMerger`

## Context

`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic G / G4. Two hazards:

- **Hazard 1 — `TombstoneMerger` complexity.** Module already gated behind the
  `tombstones` feature. The audit named "O(entries × tombstones) nested loops."
- **Hazard 2 — legacy duplicate-work parallel scan.** Already retired by #1691.

## Live vs dead surface (verified by `rg` over `cqlite-core/src`, `cqlite-cli/src`, `bindings`)

The `tombstones` build's live callers of `TombstoneMerger`:

| Method | Live caller | Complexity |
|---|---|---|
| `merge_generations` | `SSTableManager::get` (mod.rs) | O(n log n) sort + 2 O(n) passes |
| `fast_tombstone_check` | `SSTableReader::filter_tombstone` (integrity.rs) | O(1) |
| `batch_merge_with_tombstones` | `filter_with_multi_generation_merge` (integrity.rs) | O(Σ per-key generations · log) |
| `new` / `Default` / `tombstone_info` / `is_tombstone_expired` / `is_value_expired` | support for the above | O(1) |

Proven-dead public methods (0 production call sites):
`apply_range_tombstones` (**the quadratic hazard**), `range_tombstone_applies` (its
helper), `merge_row_entries`, `resolve_conflict`, `merge_cell_tombstones`,
`identify_garbage_collectible_tombstones`, `merge_collection_with_tombstones`.

## Decision: confinement (audit option b), not KWay-point-path (option a)

The audit permits (a) *if the KWay point path exists* — it does not. `kway_merge_token_order`
merges token-ordered row *streams* for scans; the single-partition fast path
(`scan_partition*`) is `not(tombstones)`-gated; and the `tombstones` `get()` does single-key
cross-generation resolution the default `get()` omits. Building a point-KWay path is a
rewrite, forbidden by G4's "reuse, don't rewrite" and the parity-pinned semantics
guardrail. So: **confine.**

Confinement is three concrete mechanics, none of which change any reconciliation result:

1. **Excise the named quadratic path.** Delete `apply_range_tombstones` +
   `range_tombstone_applies` + their one unit test. This is the only O(entries ×
   tombstones) method in the module, and it is dead. After removal every retained public
   method is ≤ O(n log n), which the module docs can now state honestly. Deadness proof is
   pasted in the PR (issue TDD requirement: "Deadness verification pasted in the PR for any
   deletion").
2. **Doc-scope the retained surface.** Module `//!` header: legacy status,
   `tombstones`-only, off the default C1/C4 fast path, per-method complexity, why the
   retained cost is acceptable (parity-pinned; low-cardinality per-key generation sets; not
   hot), and the future direction (a single-key multi-candidate KWay point path — reuse
   when it exists).
3. **Remove the `unwrap()` in `new()`.** `SystemTime::now().duration_since(UNIX_EPOCH)`
   fails only if the clock predates 1970; `unwrap_or_default()` yields `Duration::ZERO`
   → `current_time = 0`, under which nothing is deemed expired (safe). This mirrors the
   fallback already present in `integrity.rs::filter_tombstone`. No test uses the real
   clock today (all use `with_time`), so `new()`'s panic path is currently untested — the
   change adds a test that constructs via the production `new()` and merges, closing that
   gap.

## Semantics are pinned, not changed

- The retained methods keep byte-identical behavior; existing unit tests and the
  `--all-features` clustered full-scan parity test
  (`issue_1085_tombstones_full_scan_parity.rs`) are the parity pins.
- Hazard 2's proof (`table_scan_parallel_branch_issues_one_whole_table_pass`, `== 1`) is
  retained unchanged.

## Alternatives rejected

- **Option (a) KWay point path** — rejected: no reusable point path; a rewrite violates
  the guardrail. Recorded as the documented future direction instead.
- **Full dead-code sweep of the module** — deferred to a G1 "delete the dead" sweep; out
  of G4's quadratic-confinement scope. Surfaced as an out-of-scope finding.
- **Keep the quadratic method, doc-only** — rejected: deleting the dead quadratic path is
  strictly better and lets the docs assert an honest ≤ O(n log n) bound.
