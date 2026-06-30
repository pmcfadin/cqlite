# Tasks — gate-ci-coverage (#1269)

> Implement only after the owner approves the lane + strictness at Seam 1. Tasks below assume the
> RECOMMENDED Option C (scoped PR lane + nightly backstop); adjust if the owner picks A or B.

## 1. Gate CI workflow
- [ ] 1.1 Add `.github/workflows/gate.yml` with: (a) `pull_request` trigger filtered to
  `scripts/agent-gate.sh`, `scripts/tests/**`, `bindings/**`, and the workflow's own file; (b) a
  `schedule:` cron (slotted off-peak alongside the existing nightly lanes); (c) `workflow_dispatch`.
- [ ] 1.2 Job: checkout, set up `@stable` Rust with the `rustup component remove clippy --toolchain
  stable` normalization (per #1217), `Swatinem/rust-cache@v2`, Node + Python toolchains for the binding
  builds.
- [ ] 1.3 Fetch the pinned datasets (`bash test-data/scripts/fetch-datasets.sh`) and export
  `CQLITE_DATASETS_ROOT` so dataset-dependent components run (not skip).
- [ ] 1.4 Run the FULL gate: `bash scripts/agent-gate.sh` (NOT `--only`); fail the job on non-PASS.
  Upload the AGENT-GATE SUMMARY (`.agent-gate-summary.txt` / `AGENT_GATE_SUMMARY_FILE`) as an artifact.

## 2. Prove the acceptance (wiring evidence)
- [ ] 2.1 Demonstrate the lane reds on a broken component: on a throwaway branch, introduce a deliberate
  `node-bindings` break and confirm `gate.yml` fails (capture the run URL in the PR); revert it.
- [ ] 2.2 Confirm a docs-only change does NOT trigger `gate.yml` (path filter correctness).

## 3. Doctrine / discoverability
- [ ] 3.1 Note the new gate CI lane in the gate-contract doctrine
  (`agents-developing/gate-contract` source) so contributors know the gate is now CI-enforced and where.

## 4. Quality gates (definition of done)
- [ ] 4.1 `actionlint .github/workflows/gate.yml` clean.
- [ ] 4.2 `scripts/agent-gate.sh` PASS locally (the change is workflow-only; confirm no regression).
- [ ] 4.3 spec-auditor **C** PASS (anchored to `openspec/changes/gate-ci-coverage/specs/**`).
- [ ] 4.4 roborev clean (`--agent codex --base origin/main`).
- [ ] 4.5 `openspec validate gate-ci-coverage --strict` clean.
