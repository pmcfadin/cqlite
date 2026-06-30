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

## What Changes

- **Add CI coverage for the authoritative gate** so a change that breaks a gate component is caught by a
  CI lane instead of merging silently. The recommended shape (see `design.md`) is a **scoped
  PR-triggered lane** on the gate-defining inputs (`scripts/agent-gate.sh`, its self-tests,
  `bindings/**` build inputs) **plus a nightly full-gate cron backstop** that runs the complete gate
  path-independently. The owner approves the lane + strictness at Seam 1.
- The lane fetches the pinned datasets and sets `CQLITE_DATASETS_ROOT` so dataset-dependent components
  actually execute (never silently skip).

## Non-goals

- **No change to `scripts/agent-gate.sh` behavior or its component set.** This wires CI around the
  existing gate; it does not redefine the gate.
- **No making the full gate a required check on every `cqlite-core/**` PR.** Core regressions already red
  `ci.yml` + `node-ci`/`python-ci`; piling a ~25-min full gate onto every core PR is out of scope and
  costly. (This is the rejected Option A — see design.)
- **No new composite action / CI refactor** beyond what this lane needs (reuse the inline cache pattern).
- **No change to the 8 existing cron parity lanes.**
