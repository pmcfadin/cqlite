# xtask static audit for the no-unbounded-materialization invariant (#2012)

## Milestone
0.15 — the cqlite-trino latency/throughput/operations theme (epic #2403, Lane: memory).
**Design-driven — OpenSpec + Seam 1 required before any implementation.** This change adds *tooling
and a gate component*; it is a process/infrastructure decision (a new structural gate), hence OpenSpec
rather than an oracle-driven bug fix. No `Value` decode, comparator, ordering, or on-disk-format
behavior changes.

## Problem
"Never materialize an unbounded read" is CQLite's central memory-safety invariant, but it is enforced
only *at points*, never *structurally*:

- `cqlite-core/src/query/result_budget.rs` enforces a byte budget **at runtime**.
- The `byte-budget-guard` / `scan-offload-guard` / `work-counters-guard` / `memory-budget` gate
  components (`scripts/agent-gate.sh` `COMPONENTS=`, line 1042) exercise **specific known paths**
  with pinned regression tests.
- Past incidents live as tribal knowledge in code comments (#1591 read-guard-across-I/O, #1573
  seek-cursor mutex).

Nothing catches a **new** unbounded materialization spliced into a streaming path. The recurring
failure class is concrete and expensive:

- **#2361** — the UNCOMPRESSED scan path materialized the *whole SSTable* into a `Vec` before the
  first emit (LIMIT was consumer-only, so it never fired).
- **#2230 / #2423** — `KWayMerger::step` and the point-read/cache-warm merge materialized the *whole
  partition* before emit; `LIMIT`/`batch_size` did not bound intra-partition memory.
- **#1517 (Epic E)** findings — per-row clones/copies in hot scan closures.

Each was found *after the fact*, in production or field runs. A regex/grep guard cannot catch these:
string matching is exactly what misses a renamed helper, a refactored closure, or an owned-`Vec`
return spliced in from another module. The invariant deserves a structural, AST-level gate.

## Proposed change
Add a small **`xtask` crate** to the workspace implementing a `syn`-based AST audit invoked as
`cargo run -p xtask -- oom-audit [--enforce]`, wired as a **SKIP-aware `oom-audit` agent-gate
component** (the `delivery-telemetry` model: SKIP loud if the tool can't build; FAIL hard on a real
violation). This is deliberately a **lint, not a prover** — it recognizes committed *shapes*, not a
proof of boundedness.

**v1 rule (this change):** `STREAM_RETURNS_VEC` — a `.collect::<Vec<_>>()` or a `Vec::push`/`Vec::extend`
loop over a **row/partition/cell iterator** in a **reader/producer scan function**, with **no
intervening budget/batch bound** (`ResultBudget`, a `buffer_size`/`batch_size`/`limit` parameter, or a
`take(n)`) in scope. Detected syntactically per function, path-scoped — no interprocedural call-graph
proof.

**Scope (v1, per owner constraint):** `cqlite-core/src/storage/sstable/reader/data_access/**` +
`cqlite-core/src/query/**` + `cqlite-flight/src/producer*.rs` / `streaming.rs`. The wider surface
(`export`, `bindings/python`, `bindings/node`, `tools/`, write path) and the higher-false-positive
rules (`UNBOUNDED_RANGE_READ`, `CLONE_IN_SCAN_CLOSURE`) are **explicitly deferred** to follow-ups so
v1 lands green and precise.

**Suppression:** a single committed allowlist TOML. Every entry carries a **content fingerprint** (not
a line number — lines drift), a **mandatory `issue =` link**, and a **mandatory `justification =`**
string. An entry whose fingerprint no longer matches any source (**orphaned**) fails the audit, so the
allowlist cannot rot. Optional `expiry =` date; when present, an expired entry fails.

**Landing safely (seeding):** land **report-only first**. Stage 1 runs the audit, triages every hit,
and seeds the allowlist for the reviewed-and-sound sites (each with issue + justification). Only once
the report is clean does Stage 2 flip the gate component to `--enforce`. The component never lands red.

## Non-goals
- **Not a prover.** No interprocedural reachability / dataflow proof of boundedness; committed
  syntactic shapes only. Soundness gaps are accepted and covered by the runtime guards that already exist.
- **No whole-workspace scope in v1** — `export`/bindings/`tools`/write path are follow-ups.
- **Rules 2 & 3 deferred** (`UNBOUNDED_RANGE_READ`, `CLONE_IN_SCAN_CLOSURE`) — higher false-positive,
  separate change.
- **No runtime behavior change** — this is build-time tooling; zero effect on read/write output bytes.
- **No new library dependency in cqlite-core / cqlite-flight** — `syn`/`quote`/`walkdir`/`toml` live
  only in the `xtask` crate.
- **No format, comparator, or no-heuristics-doctrine change.**

## Doctrine impact
- Reinforces the no-heuristics + memory-budget doctrine with a structural gate; no doctrine *text*
  change to the invariant itself.
- CLAUDE.md gate table + `docs/development/gate-ops.md` (or the gate-contract website page) gain a
  one-line `oom-audit` component entry, in-change per the keep-doctrine-current rule.

## Definition of done
`scripts/agent-gate.sh` full PASS (SUMMARY recorded, `oom-audit` component PASS/SKIP as designed) +
spec-auditor **C** PASS (every requirement `satisfied` with a public-surface test as evidence: the
self-test fixtures below) + roborev clean; `RUSTFLAGS="-D warnings"` clean; no `unwrap()`/`expect()` in
the tool's own non-test paths where a real error is reachable; audit runtime under ~30s on the scoped
set. Then `openspec archive`.
