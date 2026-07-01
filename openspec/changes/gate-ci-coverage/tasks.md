# Tasks — gate-ci-coverage (#1269)

> Design pivoted post-Seam-1 to **nightly-backstop-only** after epic #1360 / PR #1377 merged the light
> required `pr-gate.yml`. `gate.yml` is a nightly deep-check (schedule + workflow_dispatch), NOT a
> required per-PR lane.

## 1. Gate CI workflow
- [x] 1.1 `.github/workflows/gate.yml`: NO `pull_request` trigger. Triggers are (a) a `schedule:` cron
  (slotted off-peak alongside the existing nightly lanes — 03:37 UTC); (b) `workflow_dispatch`.
- [x] 1.2 Job: checkout, set up `@stable` Rust. NOTE: this lane RUNS the gate's clippy + fmt
  components, so it requests `components: rustfmt, clippy` (the quality-gates.yml pattern, the other
  clippy-running lane) rather than the `rustup component remove clippy` normalization (per #1217),
  which is for build-only lanes that must NOT reinstall clippy-preview. `Swatinem/rust-cache@v2`,
  Node 20 + Python 3.12 toolchains (matching node-ci.yml / python-ci.yml) for the binding builds.
- [x] 1.3 Fetch the pinned datasets (`bash test-data/scripts/fetch-datasets.sh`) and export
  `CQLITE_DATASETS_ROOT` so dataset-dependent components run (not skip).
- [x] 1.4 Run the FULL gate: `bash scripts/agent-gate.sh` (NOT `--only`); fail the job on non-PASS.
  Upload the AGENT-GATE SUMMARY (`AGENT_GATE_SUMMARY_FILE`) as an artifact.

## 2. Prove the acceptance (wiring evidence)
- [ ] 2.1 Demonstrate a scheduled/dispatch run reds on a broken component: on a throwaway branch,
  introduce a deliberate `node-bindings` break and confirm a `workflow_dispatch` run of `gate.yml`
  fails (capture the run URL); revert it. (Live-CI demonstration handled by the owner.)
- [x] 2.2 Confirm `gate.yml` has NO `pull_request` trigger, so it never runs as a per-PR check and does
  not duplicate/contradict the required light `pr-gate.yml` (epic #1360).

## 3. Doctrine / discoverability
- [x] 3.1 Note the nightly deep-check gate CI lane in the gate-contract doctrine
  (`website/src/content/docs/agents-developing/gate-contract.md`) — CI-enforced via nightly `gate.yml`
  (schedule + workflow_dispatch), complementing the light required `pr-gate.yml`, NOT a per-PR gate.

## 4. Quality gates (definition of done)
- [x] 4.1 `actionlint .github/workflows/gate.yml` clean.
- [x] 4.2 Change is workflow/docs/spec-only; full agent-gate not required for this pivot.
- [ ] 4.3 spec-auditor **C** PASS (anchored to `openspec/changes/gate-ci-coverage/specs/**`).
- [ ] 4.4 roborev clean (`--agent codex --base origin/main`).
- [x] 4.5 `openspec validate gate-ci-coverage --strict` clean.
