# Design — xtask static audit for no-unbounded-materialization (#2012)

A **lint, not a prover.** The goal is to catch the recognizable *shape* of the recurring OOM class
(#2361 whole-SSTable Vec, #2230/#2423 whole-partition Vec, #1517 hot-path copies) at PR time, cheaply
and with a near-zero false-positive rate, backed by a reviewable allowlist. Soundness gaps are accepted
and remain covered by the existing runtime guards (`ResultBudget`, `byte-budget-guard`,
`scan-offload-guard`, `work-counters-guard`, `memory-budget`).

## Context / in-tree anchors (this worktree; `main`-relative, will drift — re-grep)
- **Runtime budget:** `cqlite-core/src/query/result_budget.rs` (`ResultBudget`).
- **Scan entry points named by the issue:** `StorageEngine::scan_stream`
  (`cqlite-core/src/storage/mod.rs:495`), the SSTable scan surface
  (`cqlite-core/src/storage/sstable/reader/data_access/sequential.rs` — `scan` :64, `scan_stream`
  :309, `run_scan_stream` :360, `scan_for_key` :659), and the select executor
  (`cqlite-core/src/query/select_executor.rs`).
- **Flight producer path:** `cqlite-flight/src/producer.rs`, `producer_stream.rs`, `producer_point.rs`,
  `producer_warm.rs`, `streaming.rs`.
- **Gate wiring model:** `run_delivery_telemetry` (`scripts/agent-gate.sh` :1633) — the SKIP-aware
  component template; `COMPONENTS=` (:1042); the dispatch `case` (:3394). Sibling guards
  `run_scan_offload_guard_cmd` (:3107), `byte-budget-guard` (:3180).
- **Workspace:** root `Cargo.toml` `[workspace] members` lists crates explicitly; `fuzz/` is
  `exclude`d as its own workspace. No `xtask` crate exists yet. `[workspace.lints.rust]` denies
  `unused_imports`.

## A — Workspace membership of `xtask` (chosen: member)
**Chosen: `xtask` is a normal workspace member**, added to `[workspace] members`, depending only on
`syn` (with `full`/`visit` features), `quote` (for fingerprint normalization), `walkdir`, and `toml`.
Rationale: `cargo run -p xtask -- oom-audit` must resolve against the workspace; membership gives it
`fmt`/`clippy` coverage and the shared lockfile. It pulls **no** cqlite crate, so it never enters the
`cqlite-core`/`cqlite-flight` build graph — a plain `cargo build`/`test` of the core packages does not
compile it, and the SKIP-aware gate component isolates its build from the rest of the gate.
**Rejected:** the `fuzz/`-style *excluded* separate workspace — that split exists because cargo-fuzz
needs nightly + libFuzzer; `xtask` is stable and wants to be a first-class `-p` target and be linted.

## B — What the v1 rule recognizes (STREAM_RETURNS_VEC), and what it does not
**Chosen: one precise syntactic rule, function-local, path-scoped.** Within a function whose file is in
scope and whose name/signature marks it a scan/producer path (`scan*`, `run_scan*`, `produce*`,
`iterate_*partitions*`, or a fn returning/holding a row/partition/cell iterator), flag:
- `EXPR.collect::<Vec<_>>()` / `collect::<Vec<T>>()` where `EXPR` is (transitively, within the
  expression) an iterator over a row/partition/cell type, **and**
- a `let mut v = Vec::new(); ... loop/while/for { v.push(..) | v.extend(..) }` accumulation fed by such
  an iterator,

**unless** a bound is in scope for the function: a `ResultBudget` binding/param, a param named
`buffer_size`/`batch_size`/`limit`/`max_*`, or a `.take(n)` on the accumulated iterator.

**Deliberately NOT in v1** (accepted soundness gaps, documented, covered by runtime guards):
- interprocedural cases (a helper returns the `Vec`, caller streams it) — no call-graph proof;
- `Rule 2 UNBOUNDED_RANGE_READ` and `Rule 3 CLONE_IN_SCAN_CLOSURE` (issue's other rules) — higher
  false-positive; separate follow-up change.

The row/partition/cell "iterator-ness" is decided from the syntactic type where present
(`impl Iterator<Item = ...Row/Cell/Partition...>`, `RecordBatch`, known scan return aliases) — a
conservative allowlist of type-name fragments, not inference. When the type is not syntactically
visible, the rule does **not** fire (favor false-negatives over false-positives — this is the whole
point of the allowlist-backed lint posture).

## C — Allowlist format and the anchor-drift problem (chosen: content fingerprint)
**Chosen: allowlist keyed on a content fingerprint, not `file:line`.** Line numbers drift on every edit,
producing spurious "moved" churn. Each entry:
```toml
[[allow]]
file = "cqlite-core/src/storage/sstable/statistics/reader.rs"
fn = "parse_statistics"
# fingerprint = blake3 of the syn-normalized (whitespace/renamed-local-insensitive) offending expr
fingerprint = "b3:9f2c…"
issue = "#2012"
justification = "Statistics.db is bounded-small (< a few KB); whole-file parse is sound."
# expiry = "2026-12-31"   # optional; when present, a past date fails the audit
```
- **Orphaned** (fingerprint matches nothing in scope) → FAIL. Keeps the list from rotting.
- Missing `issue`/`justification` → FAIL. Every suppression is reviewable and attributable.
- `expiry` optional (see fork F).
The fingerprint is the `quote`-normalized token stream of the offending expression hashed — stable
across reformatting and local renames, changes when the code changes (so a real new materialization at
a previously-allowed site re-fires).

## D — Modes, exit codes, and gate wiring (chosen: SKIP-aware like delivery-telemetry)
- `cargo run -p xtask -- oom-audit` → report-only, exit `0` always.
- `cargo run -p xtask -- oom-audit --enforce` → exit non-zero on any unallowlisted finding / orphan /
  malformed / expired entry.
- New `run_oom_audit` gate function on the `run_delivery_telemetry` template: `command -v cargo` absent
  or `cargo build -p xtask` fails → `SKIP` (loud); build ok + audit non-zero → `FAIL`; else `PASS`.
  Added to full `COMPONENTS`, to the dispatch `case`, and **not** to `DATASET_COMPONENTS` (self-guarding,
  no datasets). Runtime target < ~30s over the v1 scope; the `xtask` build is cached across gate runs.

## E — Seeding so it lands green (chosen: report-only → triage → seed → enforce)
Two-stage delivery within the one PR/branch:
1. Build the tool + `STREAM_RETURNS_VEC` + allowlist machinery; run report-only; **triage every hit**;
   seed the allowlist for reviewed-sound sites (each with issue + justification). Self-test fixtures
   (a crate-local `tests/fixtures/` pair: one violating, one bounded) prove both directions.
2. Only once report is clean, add the `oom-audit` component in `--enforce` and update gate docs.
The component therefore never lands red. False-positive posture: the rule favors false-negatives
(fires only when the shape *and* iterator type are syntactically visible), so the seeded allowlist is
expected to be short and to consist of genuinely-bounded whole-buffer reads (e.g. small Statistics.db).

## Open forks for Seam 1 (owner decides)
1. **Scope width (F-scope).** Recommended v1 = `cqlite-core/.../data_access/**` + `cqlite-core/query/**`
   + `cqlite-flight` producers only, per the task's constraint (5). The issue text lists a wider set
   (`export`, `bindings/python`, `bindings/node`, `tools/`). Confirm v1 = narrow, wider surface as
   follow-ups — or widen now.
2. **Expiry semantics (F-expiry).** The issue mandates an **expiry date per entry**; but a mandatory
   expiry is a wall-clock time bomb — the gate can fail red on a date when nobody touched the code
   (the same hazard as the "wall-clock races in tests" self-check class). **Recommended:** make
   `issue` + `justification` **mandatory** and `expiry` **optional** (still fails when present-and-past),
   with a periodic manual allowlist-review cadence instead of forced per-entry expiry. Owner may instead
   insist on mandatory expiry with a long default horizon.
3. **Rule 2/3 timing (F-rules).** Confirm `UNBOUNDED_RANGE_READ` + `CLONE_IN_SCAN_CLOSURE` are separate
   follow-up changes (recommended) vs bundled here (raises false-positive risk and landing difficulty).
4. **Lite-tier membership (F-tier).** The AC asks for < ~30s "so it fits the lite gate tier," but
   `--lite` today is `file-size fmt clippy scoped-tests` only. Recommended: add `oom-audit` to the
   **full** `COMPONENTS` (parity with the sibling guards) and leave lite unchanged; optionally add it to
   `--lite` later if runtime proves trivial. Owner confirms tier.
