# CI Runtime Overhaul Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce PR CI from broad, expensive validation to a small enforceable gate backed by local pre-merge validation, nightly deep checks, and release gates.

**Architecture:** CI becomes a tiered system: local pre-merge validation is the primary engineering signal, one light always-running GitHub PR gate is the required branch-protection signal, heavy parity/matrix/performance validation moves to targeted PR labels, nightly, or release workflows. Shared composite actions and policy validation prevent the workflow sprawl from recurring.

**Tech Stack:** GitHub Actions, Rust/Cargo/Clippy, optional cargo-nextest, shell scripts, GitHub branch-protection config, CQLite dataset fetch/provenance scripts.

---

## Target Model

### Required PR Gate

One stable always-running workflow should be the only global branch-protection requirement. It should run:

- `cargo fmt --all -- --check`
- `cargo clippy --package cqlite-core --all-targets --all-features -- -D warnings`
- `cargo build --package cqlite-core --all-features`
- fast representative `cqlite-core` tests
- workflow YAML/policy validation
- a minimal smoke/parity slice only if it can run quickly and deterministically

### Local Pre-Merge Validation

Before merge approval, engineers run a maintained local script that covers the expensive checks appropriate to the change. This must be documented and executable without relying on GitHub runner state.

### Targeted PR Checks

Path-specific and label/manual checks may run on relevant PRs, but should not be global required checks:

- minimal feature matrix
- binding smoke tests
- docs build
- workflow policy validation
- focused parity smoke for storage/write changes

### Nightly Deep Checks

Nightly `main` workflows run:

- full SSTableDump parity
- CQL type, tombstone/TTL, compression/corruption, live-cell compaction regeneration parity
- Cassandra ingest paths
- full all-table smoke
- Node/Python/Flight/Trino full matrices
- coverage, performance, observability overhead

### Release Gate

Release/tag workflows run the full validation matrix and publish dry-runs before any package/image publishing.

---

## Files and Ownership Boundaries

### Governance and Documentation

- Modify `.github/branch-protection.json`: required check list.
- Modify `.github/setup-branch-protection.js`: keep required contexts in sync.
- Modify `.github/QUALITY_GATES_ENFORCEMENT.md`: document tiered policy.
- Modify `.github/workflows/workflow-config.yml`: enforce workflow policy.
- Create `docs/ci/ci-tier-policy.md`: human-readable source of truth.
- Create or modify `scripts/local/pre-merge.sh`: local validation entrypoint.

### Shared CI Primitives

- Create `.github/actions/setup-rust-ci/action.yml`: Rust setup/cache/sccache policy.
- Create `.github/actions/restore-canonical-datasets/action.yml`: dataset cache/fetch/provenance policy.
- Create `.github/actions/setup-sstabledump/action.yml`: Java/sstabledump setup.
- Modify `test-data/scripts/fetch-datasets.sh`: keep as canonical dataset fetcher.
- Modify `scripts/ci/ensure_real_dataset.sh`: update stale remediation text and consume canonical dataset metadata.

### Core PR Gate and Rust Workflows

- Create or modify `.github/workflows/pr-gate.yml`: light required PR workflow.
- Modify `.github/workflows/ci.yml`: remove duplicate required work and move deep jobs to targeted/nightly/manual tiers.
- Modify `.github/workflows/m1-ci.yml`: demote legacy heavy lanes or retire if superseded.
- Modify `.github/workflows/ci-minimal-features.yml`: consolidate feature checks.
- Modify `scripts/ci/run-core-test-group.sh`: keep or replace with nextest path.

### Heavy Validation Workflows

- Modify `.github/workflows/sstabledump-parity-gate.yml`.
- Modify `.github/workflows/cassandra-validation.yml`.
- Modify `.github/workflows/e2e-readback.yml`.
- Modify `.github/workflows/smoke-tests.yml`.
- Modify `.github/workflows/perf-regression.yml`.
- Modify `.github/workflows/observability-gate.yml`.
- Modify `.github/workflows/cql-type-parity.yml`.
- Modify `.github/workflows/tombstone-ttl-parity.yml`.
- Modify `.github/workflows/compression-corruption-parity.yml`.
- Modify `.github/workflows/live-cell-compaction-parity.yml`.
- Modify `.github/workflows/compaction-parity.yml`.

### Polyglot, Docs, Flight, and Trino

- Modify `.github/workflows/node-ci.yml`.
- Modify `.github/workflows/python-ci.yml`.
- Modify `.github/workflows/flight-ci.yml`.
- Modify `.github/workflows/flight-image.yml`.
- Modify `.github/workflows/flight-trino-e2e.yml`.
- Modify `.github/workflows/trino-connector-ci.yml`.
- Modify `.github/workflows/docs-site.yml`.
- Modify release/publish workflows only where cache/concurrency policy changes are needed.

---

## Issue Breakdown

### Task 1: Define CI Tier Policy and Required-Check Source of Truth

**Files:**
- Create: `docs/ci/ci-tier-policy.md`
- Modify: `.github/branch-protection.json`
- Modify: `.github/setup-branch-protection.js`
- Modify: `.github/QUALITY_GATES_ENFORCEMENT.md`
- Modify: `.github/workflows/quality-gates.yml`

- [ ] Document the four tiers: local pre-merge, required PR, targeted/nightly, release.
- [ ] Define which workflows are allowed to be globally required.
- [ ] Remove stale references to missing workflows such as `multi-arch.yml` and `benchmark.yml`.
- [ ] Keep required status names stable during migration or document any deliberate rename.
- [ ] Verification: `ruby -e 'require "json"; JSON.parse(File.read(".github/branch-protection.json")); puts "branch protection JSON ok"'`
- [ ] Verification: `gh workflow list` after push confirms referenced workflows exist.

### Task 2: Add Local Pre-Merge Validation Entrypoint

**Files:**
- Create: `scripts/local/pre-merge.sh`
- Modify: `scripts/local/test-m1-ci-locally.sh`
- Modify: `scripts/local/test-all-ci-locally.sh`
- Modify: `docs/ci/ci-tier-policy.md`

- [ ] Add `scripts/local/pre-merge.sh` with modes: `fast`, `core`, `storage`, `bindings`, `full`.
- [ ] `fast` must run formatting, `cqlite-core` Clippy with `-D warnings`, all-feature core build, and fast core tests.
- [ ] `storage` must add dataset fetch/provenance checks and focused SSTable parity smoke.
- [ ] `bindings` must add Linux-only Python and Node smoke where local toolchains are present.
- [ ] `full` may call existing broader local scripts but must clearly print skipped external prerequisites.
- [ ] Verification: `bash scripts/local/pre-merge.sh fast`.
- [ ] Verification: `shellcheck scripts/local/pre-merge.sh scripts/local/test-m1-ci-locally.sh scripts/local/test-all-ci-locally.sh` if `shellcheck` is installed.

### Task 3: Create Shared GitHub Actions Primitives

**Files:**
- Create: `.github/actions/setup-rust-ci/action.yml`
- Create: `.github/actions/restore-canonical-datasets/action.yml`
- Create: `.github/actions/setup-sstabledump/action.yml`
- Modify: `test-data/scripts/fetch-datasets.sh`
- Modify: `scripts/ci/ensure_real_dataset.sh`

- [ ] `setup-rust-ci` installs the pinned/stable Rust toolchain, optional components, and one cache strategy.
- [ ] `restore-canonical-datasets` restores the canonical dataset cache and always runs `test-data/scripts/fetch-datasets.sh` to guard partial caches.
- [ ] `setup-sstabledump` installs Java and `sstabledump` consistently.
- [ ] Remove direct dataset download snippets from workflows as each workflow migrates.
- [ ] Update stale v2 dataset remediation text in `ensure_real_dataset.sh`.
- [ ] Verification: migrate one non-required workflow first and run `gh workflow view <workflow>` after push.

### Task 4: Build the Light Required PR Gate

**Files:**
- Create: `.github/workflows/pr-gate.yml`
- Modify: `.github/branch-protection.json`
- Modify: `.github/setup-branch-protection.js`
- Modify: `.github/workflows/workflow-config.yml`

- [ ] Add an always-running `pull_request` workflow with no path filters.
- [ ] Include workflow config validation, Rust fmt, `cqlite-core` Clippy, core all-feature build, and fast tests.
- [ ] Use a stable aggregate job name such as `Required PR Gate / required`.
- [ ] Do not run Docker, full datasets, release builds, or platform matrices here.
- [ ] Update branch protection to require the new aggregate check once the workflow is live.
- [ ] Verification: a docs-only PR still produces the required check and completes quickly.

### Task 5: Collapse `ci.yml` and `m1-ci.yml` Overlap

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/m1-ci.yml`
- Modify: `.github/branch-protection.json`
- Modify: `scripts/ci/run-core-test-group.sh`

- [ ] Decide whether `m1-ci.yml` remains as legacy/manual or is absorbed into `pr-gate.yml`.
- [ ] Remove duplicate fmt/clippy/build/test lanes once `pr-gate.yml` owns them.
- [ ] Move publish dry-run, flow tooling, CLI deep smoke, and broad dataset tests out of the required path.
- [ ] Preserve required-check continuity until branch protection is updated.
- [ ] Verification: `gh run list --limit 20` on a test PR shows fewer workflows triggered at PR creation.

### Task 6: Introduce `cargo-nextest` Build-Once/Test-Many for Core Tests

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/pr-gate.yml`
- Modify: `scripts/ci/run-core-test-group.sh`
- Create: `.config/nextest.toml` if no existing nextest config is present and project-specific configuration is needed.

- [ ] Add `cargo-nextest` installation through `taiki-e/install-action` or an existing repo-approved tool installer.
- [ ] Replace hand-rolled integration shard compilation with `cargo nextest archive`.
- [ ] Run partitions from the archive using `nextest run --archive-file ... --partition count:N/M`.
- [ ] Keep doc tests and example build checks separate because nextest does not replace all Cargo test modes.
- [ ] Verification: nextest archive job succeeds and each partition reports tests run.

### Task 7: Split Full SSTableDump Parity into PR Smoke and Nightly Full Gate

**Files:**
- Modify: `.github/workflows/sstabledump-parity-gate.yml`
- Modify: `.github/workflows/cassandra-parity.yml`
- Modify: `scripts/ci/ensure_real_dataset.sh`

- [ ] Create a small PR smoke path covering representative Data.db, Index.db, Summary.db, Statistics.db, compression, and tombstone/TTL signals.
- [ ] Move the current full parity matrix to nightly/manual/release trigger.
- [ ] Remove informational `continue-on-error` PR work that does not affect merge.
- [ ] Add explicit `timeout-minutes`.
- [ ] Keep failure artifacts on all full parity runs.
- [ ] Verification: ordinary core PR no longer runs the full parity suite by default.

### Task 8: Tier Cassandra Ingest, Smoke, Performance, and Observability Workflows

**Files:**
- Modify: `.github/workflows/cassandra-validation.yml`
- Modify: `.github/workflows/e2e-readback.yml`
- Modify: `.github/workflows/smoke-tests.yml`
- Modify: `.github/workflows/perf-regression.yml`
- Modify: `.github/workflows/observability-gate.yml`

- [ ] Keep at most one Cassandra ingest smoke path on default writer-path PRs.
- [ ] Move the second ingest path to label/manual/nightly/release.
- [ ] Move all-table smoke to nightly/manual unless a change explicitly touches all-table smoke logic.
- [ ] Keep observability correctness on relevant PRs; move overhead benchmarks to nightly/manual.
- [ ] Move perf regression to nightly/manual or label-gated PRs.
- [ ] Verification: a storage PR does not enqueue two 30-minute Cassandra jobs by default.

### Task 9: Split Node, Python, Flight, Trino, and Docs Matrices by Tier

**Files:**
- Modify: `.github/workflows/node-ci.yml`
- Modify: `.github/workflows/python-ci.yml`
- Modify: `.github/workflows/flight-ci.yml`
- Modify: `.github/workflows/flight-image.yml`
- Modify: `.github/workflows/flight-trino-e2e.yml`
- Modify: `.github/workflows/trino-connector-ci.yml`
- Modify: `.github/workflows/docs-site.yml`
- Modify: `.github/workflows/node-release.yml`
- Modify: `.github/workflows/python-release.yml`
- Modify: `.github/workflows/release.yml`

- [ ] PR binding smoke: Linux x64 only for ordinary core changes.
- [ ] Full OS/arch native binding matrices: nightly/manual/release.
- [ ] Flight CI: validate on PR, publish images only from release/image workflow.
- [ ] Flight-Trino E2E: nightly/manual/full-label by default, with a minimal PR smoke for direct connector/Flight changes.
- [ ] Docs: build/link check on docs PRs; dataset recipe smoke nightly/manual or when examples/scripts change.
- [ ] Verification: core-only PR does not run full Node/Python native matrices.

### Task 10: Add Nightly Deep Validation Orchestrator and Release Gate Policy

**Files:**
- Modify: `.github/workflows/quality-gates.yml`
- Modify: `.github/workflows/coverage.yml`
- Modify: `.github/workflows/coverage-baseline.yml`
- Modify: `.github/workflows/exhaustive-regeneration.yml`
- Modify: release workflows as needed.

- [ ] Define nightly deep validation as the backstop for reduced PR CI.
- [ ] Standardize nightly schedule offsets to avoid top-of-hour queue bursts.
- [ ] Ensure nightly failure artifacts and summaries are retained long enough for triage.
- [ ] Define release gate checklist that runs full parity, full matrices, coverage, perf, and publish dry-runs.
- [ ] Verification: all nightly/full workflows can be triggered with `workflow_dispatch`.

### Task 11: Enforce Workflow Policy Automatically

**Files:**
- Modify: `.github/workflows/workflow-config.yml`
- Create: `scripts/ci/validate-workflows.rb` or keep inline Ruby if the repo prefers single-file workflow validation.

- [ ] Fail on PR workflows missing `concurrency`.
- [ ] Fail on jobs missing `timeout-minutes`, except documented tiny aggregate jobs.
- [ ] Fail on workflows missing least-privilege `permissions`.
- [ ] Warn or fail on direct dataset download snippets outside the shared action/script.
- [ ] Warn or fail on broad PR matrices in binding workflows.
- [ ] Verification: `ruby scripts/ci/validate-workflows.rb` or the inline workflow validation step passes locally.

### Task 12: Add CI Runtime Metrics and Migration Success Criteria

**Files:**
- Modify: `.github/workflows/pr-gate.yml`
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/sstabledump-parity-gate.yml`
- Create: `scripts/ci/ci-timing-summary.sh`
- Create: `docs/ci/ci-runtime-dashboard.md`

- [ ] Add timing summaries for dataset fetch, cargo build/test groups, parity groups, and matrix jobs.
- [ ] Document baseline and target metrics: required PR queue-to-green, workflow count per PR, cache hit rates, and p95 job runtime.
- [ ] Add a short runbook for handling nightly failures.
- [ ] Verification: a PR run emits `GITHUB_STEP_SUMMARY` timing data for the required gate.

---

## Suggested Dependency Order

1. Task 1: policy and required-check source of truth.
2. Task 2: local pre-merge script.
3. Task 3: shared setup/dataset primitives.
4. Task 4: new light required PR gate.
5. Task 5: collapse `ci.yml`/`m1-ci.yml`.
6. Task 6: nextest optimization.
7. Task 7 and Task 8: heavy validation tiering.
8. Task 9: polyglot matrix tiering.
9. Task 10: nightly/release orchestration.
10. Task 11: workflow policy enforcement.
11. Task 12: runtime metrics.

---

## Acceptance Criteria for the Epic

- A docs-only PR runs one required GitHub Actions check and does not leave required path-filtered checks pending.
- A typical Rust core PR runs the required gate plus only relevant targeted checks.
- Full SSTableDump parity, full Cassandra ingest, full binding matrices, coverage, perf, and observability overhead do not run by default on ordinary PRs.
- Nightly `main` validation runs the deep checks and produces actionable artifacts.
- `scripts/local/pre-merge.sh fast` passes locally before PRs.
- `cargo clippy --package cqlite-core --all-targets --all-features -- -D warnings` remains a hard gate.
- Branch protection references only stable aggregate check names.
- Workflow policy validation prevents reintroducing unbounded PR fan-out.

---

## Self-Review

- Spec coverage: The plan covers local-first validation, light PR CI, nightly deep checks, release gates, branch protection, Rust efficiency, parity tiering, binding matrices, shared CI primitives, and metrics.
- Placeholder scan: No task contains TBD/TODO/fill-in placeholders.
- Type consistency: Workflow names, scripts, and paths match existing repo structure or are explicitly listed as new files.
