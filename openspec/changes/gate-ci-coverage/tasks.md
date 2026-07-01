# Tasks — gate-ci-coverage (#1269)

> Implement only after the owner approves the lane + strictness at Seam 1. Tasks below assume the
> RECOMMENDED Option C (scoped PR lane + nightly backstop); adjust if the owner picks A or B.

## 1. Gate CI workflow
- [x] 1.1 Add `.github/workflows/gate.yml` with: (a) `pull_request` trigger filtered to the direct
  gate inputs — `scripts/**` (incl. explicit `scripts/agent-gate.sh`, `scripts/tests/**`),
  `test-data/scripts/**`, `bindings/**`, and the workflow's own file (NO docs/website paths); (b) a
  `schedule:` cron (slotted off-peak alongside the existing nightly lanes — 03:37 UTC); (c)
  `workflow_dispatch`.
- [x] 1.2 Job: checkout, set up `@stable` Rust. NOTE: this lane RUNS the gate's clippy + fmt
  components, so it requests `components: rustfmt, clippy` (the quality-gates.yml pattern, the other
  clippy-running lane) rather than the `rustup component remove clippy` normalization (per #1217),
  which is for build-only lanes that must NOT reinstall clippy-preview. `Swatinem/rust-cache@v2`,
  Node 20 + Python 3.12 toolchains (matching node-ci.yml / python-ci.yml) for the binding builds.
- [x] 1.3 Fetch the pinned datasets (`bash test-data/scripts/fetch-datasets.sh`) and export
  `CQLITE_DATASETS_ROOT` so dataset-dependent components run (not skip).
- [x] 1.4 Run the FULL gate: `bash scripts/agent-gate.sh` (NOT `--only`); fail the job on non-PASS.
  Upload the AGENT-GATE SUMMARY (`.agent-gate-summary.txt` / `AGENT_GATE_SUMMARY_FILE`) as an artifact.

## 2. Prove the acceptance (wiring evidence)
- [ ] 2.1 Demonstrate the lane reds on a broken component: on a throwaway branch, introduce a deliberate
  `node-bindings` break and confirm `gate.yml` fails (capture the run URL in the PR); revert it.
  (Live-CI demonstration handled by the owner at PR time.)
- [x] 2.2 Confirm a docs-only change does NOT trigger `gate.yml` (path filter correctness — filter is
  scoped to the direct gate inputs `scripts/**`, `test-data/scripts/**`, `bindings/**`, and the
  workflow's own file; contains no `docs/**` or `website/**` path).

## 3. Doctrine / discoverability
- [x] 3.1 Note the new gate CI lane in the gate-contract doctrine
  (`website/src/content/docs/agents-developing/gate-contract.md`) so contributors know the gate is
  now CI-enforced and where.

## 4. Quality gates (definition of done)
- [x] 4.1 `actionlint .github/workflows/gate.yml` clean.
- [x] 4.2 `scripts/agent-gate.sh` PASS locally (the change is workflow-only; confirm no regression).
- [ ] 4.3 spec-auditor **C** PASS (anchored to `openspec/changes/gate-ci-coverage/specs/**`).
- [ ] 4.4 roborev clean (`--agent codex --base origin/main`).
- [x] 4.5 `openspec validate gate-ci-coverage --strict` clean.
