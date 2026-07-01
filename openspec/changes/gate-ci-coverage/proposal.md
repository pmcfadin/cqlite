## Why

`scripts/agent-gate.sh` is the delivery pipeline's **authoritative pre-PR gate** (16 components incl.
`fmt`, `clippy`, core/integration/write/cli tests, `python-bindings`, `node-bindings`, `smoke`), but a
read-only audit confirms **no GitHub Actions workflow invokes it**. The only `agent-gate` reference in
`.github/workflows/` is a comment (`delta-roundtrip.yml:17`). Consequences:

- A PR that edits the gate itself (`scripts/agent-gate.sh`) triggers the `scripts/**`-filtered lanes
  (`ci.yml`, `m1-ci.yml`, `smoke-tests.yml`, `sstabledump-parity-gate.yml`) — **none of which shell out
  to the gate or build the napi/maturin binding modules**. So changes to gate components are
  CI-unverified.
- Concretely, the `node-bindings` component (added in #1255) is exercised in CI only as a side effect of
  a `cqlite-core/**` change reding `node-ci.yml`/`python-ci.yml`. A gate-only change that breaks a
  component (e.g. the gate's own node-bindings step) reds **nothing** in CI.
- No scheduled lane runs the gate either (8 cron lanes exist, all parity-regeneration; none runs the gate).

- **Milestone:** maintenance / CI hardening. **Design-driven** — the issue explicitly defers *which lane
  and how strict* to design; there is no SSTable-format oracle here. Real latitude.
- Adds a new `gate-ci-coverage` capability.

Audit facts that constrain the design:
- `agent-gate.sh` supports `--only <csv>` for a subset, but that run is **stamped `PARTIAL` and
  explicitly "does NOT count as the gate"** (`agent-gate.sh:71-72,788`). A lane that is meant to *count*
  must run the **full** gate.
- A full run builds the napi (node) + maturin (python) modules, runs the full workspace test set, and
  the smoke suite over the dataset corpus — comparable weight to `node-ci`/`python-ci` (30-min caps).
- The dataset-dependent components need the dataset preflight (`fetch-datasets.sh` +
  `CQLITE_DATASETS_ROOT`) or they skip.
- No composite/reusable "setup-rust+cache" action exists; the de-facto pattern is inline
  `Swatinem/rust-cache@v2` (bindings) / `sccache` (workspace).

## Design pivot (post-#1360)

The original design recommended **Option C** (a scoped, required PR-triggered lane on gate-defining
inputs **plus** a nightly cron backstop). After Seam 1, **PR #1377 (epic #1360)** merged the tiered CI
model: the ONE required, always-running PR check is the light `.github/workflows/pr-gate.yml` (fmt +
cqlite-core clippy `-D warnings` + all-feature build + fast tests; no Docker/datasets/agent-gate), with
heavy checks moved to nightly and **path-filtered heavy lanes explicitly not required**. A heavy,
required, path-filtered PR lane would directly contradict that model. This change therefore pivots to
**nightly-backstop-only**: `gate.yml` drops its `pull_request` trigger entirely and keeps only the
`schedule:` cron + `workflow_dispatch`, complementing (not duplicating) the required light `pr-gate.yml`.

## What Changes

- **Add CI coverage for the authoritative gate** so a change that breaks a gate component is caught by a
  CI run instead of going unverified. The lane is a **nightly, path-independent full-gate deep-check
  backstop** (`schedule:` cron + `workflow_dispatch`), NOT a required per-PR check — it complements the
  light required `pr-gate.yml` from epic #1360 rather than layering a heavy required lane on top of it.
- The lane fetches the pinned datasets and sets `CQLITE_DATASETS_ROOT` so dataset-dependent components
  actually execute (never silently skip).

## Non-goals

- **No change to `scripts/agent-gate.sh` behavior or its component set.** This wires CI around the
  existing gate; it does not redefine the gate.
- **No making the full gate a required per-PR check at all.** Post-#1360 the required PR check is the
  light `pr-gate.yml`; the full gate stays a nightly/on-demand backstop. Piling a ~25-min full gate onto
  PRs (as a broad Option A, or even a path-filtered required lane per Option C) is out of scope and would
  contradict epic #1360's tiered model.
- **No new composite action / CI refactor** beyond what this lane needs (reuse the inline cache pattern).
- **No change to the 8 existing cron parity lanes.**
