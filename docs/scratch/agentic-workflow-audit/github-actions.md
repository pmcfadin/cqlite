# CQLite GitHub Actions CI/CD Audit

**Scope**: All 41 `.github/workflows/` files. Trigger patterns, purposes, cost signals, overlap, and self-heal jobs.

---

## Inventory & Classification

### Required PR Gate
| Workflow | Trigger | Purpose | Cost |
|----------|---------|---------|------|
| `pr-gate.yml` | `pull_request` only | **THE** required per-PR check: fmt, scoped clippy, cqlite-core build, fast tests | 30 min timeout; no Docker/datasets |

### Nightly Deep Check (Full Gate)
| Workflow | Trigger | Purpose | Cost |
|----------|---------|---------|------|
| `gate.yml` | schedule (03:37 UTC) + dispatch | Full `scripts/agent-gate.sh` (16 components: fmt, clippy, core/integration/write/cli/python/node tests, smoke) | **75 min** timeout; 13GB swapfile; OOM guard; full dataset corpus; cold-cache budget |
| `exhaustive-regeneration.yml` | schedule (08:10 UTC) + dispatch | Regenerate parity corpus (nb/oa/da formats, test_deltas, compression, corruption fixtures) + audit | **180 min** timeout; Docker; 8+ generator scripts |

### Advisory Canary
| Workflow | Trigger | Purpose | Cost |
|----------|---------|---------|------|
| `future-rust-canary.yml` | schedule (weekly) + dispatch | Build workspace + write-support CLI on **latest stable** Rust (not pinned) | Advisory only; never PR-required |

### Parity Lanes (Per-PR & Nightly)
| Workflow | Trigger | Purpose | Cost |
|----------|---------|---------|------|
| `sstabledump-parity-gate.yml` | PR (45 min) / push (240 min) / nightly (45 min) / release | Smoke parity (PR scope-scoped) vs full on nightly + release | Scope-gates PR run (~45 min); full on schedule (240 min; 4h timeout) |
| `cassandra-parity.yml` | PR/push (manifest only) + nightly (orphaned-debt check) | Linter: manifest schema, cross-field rules, report staleness, tier coverage; **self-heal** via `parity-report-heal` | 30 min (lint) + 10 min (partial check); no Docker |
| `e2e-readback.yml` | PR + schedule (02:30 UTC) + dispatch | Read back via sstableloader after write on live Cassandra | PR: Docker; nightly: full E2E |
| `cassandra-validation.yml` | PR/push + dispatch | Validate via sstableloader + compare JSON vs sstabledump | Docker required; compares output formats |
| `compaction-parity.yml` | PR + schedule + dispatch | Byte-for-byte compaction matrix (differential harness) | Docker; matrix of formats (nb/oa/da × features) |
| `compression-corruption-parity.yml` | PR + schedule (nightly Docker Epic #970) | Compression + corruption byte parity via Docker + live Cassandra | Docker; fixture corpus |
| `cql-type-parity.yml` | PR + schedule (nightly Docker Epic #971) | CQL type & schema-evolution parity | Docker |
| `tombstone-ttl-parity.yml` | PR + schedule (nightly Docker Epic #972) | Tombstone/TTL parity | Docker |
| `live-cell-compaction-parity.yml` | PR + schedule + dispatch | Live-cell compaction byte parity (issue #1017, #1020) | Docker; matrix |

### Release & Publishing
| Workflow | Trigger | Purpose | Cost |
|----------|---------|---------|------|
| `release.yml` | push to main (release tags) | Build + sign CLI binaries (macOS, Linux, Windows; cross-compile matrix) | Matrix: 3 targets × 2 variants |
| `python-release.yml` | push (main only) | Build + publish Python wheels to PyPI | 10 min timeout |
| `node-release.yml` | push (main only) | Build + publish Node.js bindings to npm | 10 min timeout |
| `api-docs.yml` | push + dispatch | Build & publish Rustdoc to GitHub Pages | 10 min |

### Binding CI (Per-PR & Nightly)
| Workflow | Trigger | Purpose | Cost |
|----------|---------|---------|------|
| `python-ci.yml` | PR/push/schedule (03:35 UTC) | Python binding tests (3.9–3.12 matrix × 2 variants) | 30 min timeout; matrix |
| `node-ci.yml` | PR/push/schedule (03:15 UTC) | Node.js binding tests (Node 18, 20 matrix) | 30 min timeout; matrix |
| `node-release.yml` | push | Release Node.js bindings | 10 min |

### General CI
| Workflow | Trigger | Purpose | Cost |
|----------|---------|---------|------|
| `ci.yml` | PR/push + dispatch | General CI (Rust, integration tests; calls setup-rust-ci) | No timeout specified; quick path |
| `ci-minimal-features.yml` | PR/push | Minimal features build (no-default-features) | No Docker/datasets |
| `m1-ci.yml` | PR + dispatch | Core library (minimal) on Ubuntu | No timeout |
| `quality-gates.yml` | schedule + dispatch | Coordination of multi-lane quality checks; no native job (orchestrator) | 0 min (orchestrator; delegates) |
| `observability-gate.yml` | PR + schedule + dispatch | Observability (metrics, tracing) validation | No timeout |
| `coverage.yml` | schedule + dispatch | Coverage gate (90% target; cargo-llvm-cov) | No PR trigger; prebuilt tools |
| `coverage-baseline.yml` | PR/schedule + dispatch | Coverage baseline (prebuilt tarpaulin + llvm-cov) | No timeout |
| `fuzz.yml` | PR/schedule + dispatch | Parser fuzz (libFuzzer + nightly Rust) | No Docker; nightly-only |
| `perf-regression.yml` | PR + schedule + dispatch | Performance regression benchmarks vs baseline | Criterion |

### Connectors & Extensions
| Workflow | Trigger | Purpose | Cost |
|----------|---------|---------|------|
| `flight-ci.yml` | PR/push/schedule + dispatch | Arrow Flight provider validation | No Docker specified |
| `flight-image.yml` | push + dispatch | Build cqlite-flight Docker image | Docker build; registry push |
| `flight-trino-e2e.yml` | PR/push/schedule + dispatch | Trino E2E via Flight | Docker; Trino container |
| `trino-connector-ci.yml` | PR/push/schedule + dispatch | Trino connector tests | No Docker specified initially |
| `trino-publish.yml` | push (main) | Publish Trino connector | 10 min |

### Delta & Smoke
| Workflow | Trigger | Purpose | Cost |
|----------|---------|---------|------|
| `delta-roundtrip.yml` | PR/push + dispatch | Delta round-trip test (CDC export Parquet, re-ingest) | Issue #707; no Docker stated |
| `smoke-tests.yml` | PR/push/schedule (multiple crons) + dispatch | Smoke-test all 33 tables against JSONL goldens | Validation matrix in `test-data/validation-matrix.md` |

### Management & Admin
| Workflow | Trigger | Purpose | Cost |
|----------|---------|---------|------|
| `cassandra-parity.yml` → `parity-report-heal` | push (main) + nightly orphaned-debt check | **Self-heal job** for semantic merge race (issue #1338): regenerates stale parity report, opens regen PR via `auto/parity-report-regen` branch | 30 min (heal); needs `PARITY_HEAL_TOKEN` secret |
| `parity-failure-issue.yml` | workflow_dispatch (manual) | File issues for parity failures (compression, CQL type, tombstone/TTL) | Automation tool; parses workflow runs |
| `parity-failure-issue-tests.yml` | PR + dispatch | Unit tests for parity failure automation | No timeout |
| `project-board-sync.yml` | schedule + dispatch | GitHub Project board sync (status field automation) | No timeout |
| `docs-site.yml` | PR/push/schedule + dispatch | Docs site build & deploy | No timeout |
| `workflow-config.yml` | PR/push + dispatch | Workflow YAML validation (policy #1990, toolchain consistency) | No timeout; Ruby script |

---

## How It Works

### Architecture & Hierarchy

**Seam 1: PR gate** (`pr-gate.yml`) → **Seam 2: Full gate + roborev + C audit** (via `flow-implement`)
- PRs must pass `pr-gate.yml` (30 min; required context)
- Before merge: full `scripts/agent-gate.sh` runs locally or on nightly `gate.yml` (75 min; backstop)
- Roborev + spec audit (`C`) follow the full gate
- Merge on green (worker autonomy; no human merge gate per #2026-07-06 decision)

**Tiered concurrency** (issue #1825)
- Full `agent-gate.sh` capped to N runs machine-wide (default `max(2, floor((ncpu-2)/4))`)
- `--lite` runs (1–5 min) are exempt from cap; used for each fix round
- Queued runs block with `waiting for gate slot` message; stale slots reaped via daemon

**Parity layers** (issue #1338 + tiers #1026–#1027)
1. **Linter** (`cassandra-parity.yml`, 30 min): fast PR gate; checks manifest + docs sync
2. **Self-heal** (`parity-report-heal`, 30 min): push-to-main job; regenerates stale derived report, opens PR
3. **Smoke** (`sstabledump-parity-gate.yml`, 45 min): PR + nightly; scope-gated on PR
4. **Full** (`sstabledump-parity-gate.yml` full, 240 min / 4h): nightly + release only
5. **Docker parity** (Compression #970, CQL Type #971, Tombstone/TTL #972): nightly + PR; Docker required
6. **Exhaustive** (`exhaustive-regeneration.yml`, 180 min): nightly only; regenerates corpus + audits

**Toolchain policy** (issue #1990; documented in `ci-toolchain-policy.md`)
- **Pinned to `rust-toolchain.toml` (1.88.0)**: Most workflows omit `toolchain:` so they auto-read the pin
- **Explicit dtolnay pin** (`dtolnay/rust-toolchain@1.88.0`): `ci.yml`, `gate.yml`, `cassandra-parity.yml`, and 14 others (dtolnay cannot read pin file)
- **Advisory canary** (`future-rust-canary.yml`): **Latest stable only** (weekly + dispatch; never PR-required)
- **Nightly exception** (`fuzz.yml`): `nightly` toolchain (legitimately needs it)
- **Release lanes**: `release.yml`, `*-release.yml`, `api-docs.yml` intentionally use `stable` (release artifacts)

**OOM & disk management** (issue #1269, #1848)
- Disk cleanup in `gate.yml`: free ~20–30 GB (dotnet, Android, CodeQL, boost)
- 13 GB swapfile provisioned on `/mnt` (larger ephemeral disk)
- `sccache` cross-worktree object cache (~25% speedup on fresh builds)
- `cargo-nextest` parallelizes core-test execution
- 2-lane component parallelism (MAIN cargo + SIDE isolated for bindings) with `AGENT_GATE_JOBS` cap

---

## Measured/Observed Costs

### Per-Run Wall-Clock
| Workflow | Hot Cache | Cold Cache | Notes |
|----------|-----------|-----------|-------|
| `pr-gate.yml` | ~10–15 min | ~20–30 min | Required; no datasets/Docker |
| `gate.yml` | ~20–30 min (measured warm) | ~45 min | Full gate; COLD budget 75 min; sccache helps |
| `sstabledump-parity-gate.yml` (smoke) | ~30–45 min | ~45 min | PR gated; full on nightly (240 min) |
| `exhaustive-regeneration.yml` | N/A | ~3h (180 min timeout) | Docker; corpus regeneration; once nightly |
| `cassandra-parity.yml` (linter) | ~5 min | ~15 min | Fast PR gate; no Docker |
| `python-ci.yml` | ~15 min (3.12) | ~30 min | Matrix: Python 3.9–3.12; maturin builds |
| `node-ci.yml` | ~15 min (Node 20) | ~30 min | Matrix: Node 18, 20; napi-rs builds |
| `coverage.yml` | N/A | ~20 min | Schedule only; prebuilt cargo-llvm-cov |
| `flight-trino-e2e.yml` | ~30 min | ~45 min | Docker Trino + Flight |
| `e2e-readback.yml` | ~20 min | ~40 min | Docker Cassandra + sstableloader |

### Matrix Sizes
| Workflow | Matrix | Count |
|----------|--------|-------|
| `release.yml` | OS × architecture (macOS, Linux, Windows) | 3 |
| `python-ci.yml` | Python 3.9–3.12 | 4 |
| `node-ci.yml` | Node 18, 20 | 2 |
| `compaction-parity.yml` | Format (nb/oa/da) × compaction feature variants | 6–9 |
| `python-release.yml` | Python 3.9–3.12 | 4 |

### Resource Usage Peaks
- **Gate OOM incidents** (issue #1269): Two observed at 23m51s and 40m; fixed by `CARGO_BUILD_JOBS=2`, `CARGO_INCREMENTAL=0`, 13 GB swapfile
- **Disk ceiling** (issue #722): ~30 GB for workspace + datasets + build artifacts; cleanup + swapfile mitigate
- **Dataset cache**: `datasets-v3` (~3 GB compressed, SHA256 pinned) cached per tag + SHA256; miss forces re-download
- **sccache** (new 2026-07-03): ~25.6% cold-build speedup; occasionally corrupted objects under high load (workaround: `CQLITE_DISABLE_SCCACHE=1`)

---

## Friction Points

### 1. **Workflow Redundancy & Overlap**
- **3 separate release workflows** (`release.yml`, `python-release.yml`, `node-release.yml`) all on `push` with separate secret/credential scopes → potential for merge race or partial publication
- **13 separate parity lanes** (compression, CQL type, tombstone, compaction, e2e-readback, etc.) all on PR + schedule; unclear which are truly **required** vs advisory; manifest lacks explicit tier-enforcement hooks
- **Coverage** split across 3 workflows (`coverage.yml`, `coverage-baseline.yml`, `quality-gates.yml`); no clear "is coverage gating PR merge?" signal
- **Docs build** lives in `docs-site.yml` but also invoked by `quality-gates.yml` orchestrator → orchestration fragmented

### 2. **Self-Heal Fragility**
- **`parity-report-heal`** (in `cassandra-parity.yml`, ~30 min) depends on `PARITY_HEAL_TOKEN` secret (PAT/App token) for regen PR to trigger CI
  - Missing secret → SKIPS with `::notice::` instead of opening check-less PR (graceful, but silent)
  - If both token AND branch get deleted → no recovery path
  - Regen PR must be manually merged; no auto-merge guard documented (could block if owner unavailable)
- **Force-push to `auto/parity-report-regen`** with TOCTOU guard (`cancel-in-progress: false`) but concurrent main pushes still serialize in lock (coarse-grained)

### 3. **Manifest-Driven Parity Tiers Not Enforced in CI**
- `docs/development/parity-ci-tiers.md` defines 5 tiers (smoke, canonical_semantic, byte_for_byte, exhaustive_regeneration, nightly_docker)
- `cassandra-parity-manifest.yml` embeds tier + blocking flags
- But **no workflow step reads the manifest to conditionally skip/require steps** → tiers are docs-only guardrails, not code enforcement
- Missing a lane → silent drift (only caught by the nightly linter's `--check`)

### 4. **Toolchain Pin Brittleness**
- 19 workflows hardcode `dtolnay/rust-toolchain@1.88.0` as a literal string
- Bumping the pin requires updating 5 files in lockstep (`rust-toolchain.toml`, `setup-rust-ci/action.yml`, 19 workflow refs, `nightly-docker-parity.yml` explicit input)
- **No CI check** enforces lockstep → a human can miss one ref and CI silently drifts
- Policy doc warns but does not automate; only future-canary catches the drift (24 h lag)

### 5. **Concurrency & Serialization**
- Full gate capped to N runs globally (good), but **PR gate is NOT capped** → 100 concurrent PRs could each run a 30-min PR gate
- `--lite` gate exempt from cap but used by workers for fast iteration; if worker spawns 2 parallel lite gates in same worktree, sccache corruption risk (2026-07-06 ops note)
- `parity-report-heal` uses `cancel-in-progress: false` to serialize per-branch but `group: parity-report-heal` is global → only 1 regen at a time machine-wide (coarse)

### 6. **Docker Dependency Spread**
- 10 workflows require Docker; tight coupling to live Cassandra image + sstableloader
- **No published base image** for Cassandra 5.0 + sstableloader; assume public Docker Hub availability
- If Docker Hub goes down or image is removed → 10 workflows silently fail (no fallback)
- Docker image version not pinned in most workflows; `cassandra-validation.yml`, `compaction-parity.yml` reference `cassandra:5.0` (latest tag; drifts)

### 7. **Dataset Pin Fragility**
- 7 workflows fetch datasets; pin via `DATASET_TAG`, `DATASET_ASSET`, `DATASET_SHA256` env vars
- Must be bumped in lockstep across files; no automation
- `.dataset-pin` file (gitignored) tracks local pin but doesn't help CI
- Missing dataset → tests silently **skip** (unless `CQLITE_REQUIRE_FIXTURES=1` or `CQLITE_PARITY_REQUIRE_DATASETS=1`)

### 8. **Workflow-as-Code Drift**
- 41 workflows + 5 reusable actions (`.github/actions/`) = ~46 files of workflow code
- No single source of truth for common patterns (e.g., "setup Rust + cache" repeated 15+ times)
- Reusable actions exist (`setup-rust-ci`, `restore-canonical-datasets`) but adoption is inconsistent
  - `release.yml` doesn't use `setup-rust-ci`
  - Some workflows set `CARGO_BUILD_JOBS` ad-hoc; others inherit from gate only
- YAML duplication makes bulk edits (e.g., updating cron times) error-prone

### 9. **Observability of CI State**
- **No unified dashboard** showing which parity lanes are currently passing/failing
- `cassandra-parity.yml` publishes a `docs/reports/cassandra-test-parity.md` report but must be manually regenerated on manifest changes
- `parity-failure-issue.yml` auto-files issues for failures but only via manual workflow_dispatch; no automation to catch a new regression
- Nightly deep-check (`gate.yml`) uploads a summary artifact but no alert on failure (silent unless owner checks Actions tab)

### 10. **Artifact Retention Fragility**
- 14 workflows publish artifacts with `retention-days` varying (14, 30, 60, …)
- `cassandra-parity.yml` enforces retention minimums via the `retention-check` step (good!) but this is only checked on manifest changes, not on every workflow edit
- If a workflow `retention-days` is lowered, the check will fail — but only next time manifest changes

### 11. **CI Merge-Blocking Discipline**
- **Required checks**: only `pr-gate.yml` (and optionally `cassandra-parity.yml` linter on manifest changes)
- **Advisory checks**: all 13 parity lanes, coverage, observability, etc.
- But branch protection rules are **not exported** → unclear which checks are actually enforced on `main` (must view GitHub Settings to audit)
- If a PR-required check is mistakenly marked optional, it silently stops gating

### 12. **Cost Attribution Opaqueness**
- No per-workflow cost tracking in logs; only wall-clock times visible in Actions UI
- `gate.yml` memory/disk snapshots logged but no aggregation tool
- Nightly lanes (180-min exhaustive, 240-min full parity) run in parallel; peak CI bill unknown

---

## Open Questions

1. **Parity tier enforcement**: Should `cassandra-parity-manifest.yml` tier flags be **read by CI** to auto-SKIP unsupported lanes (e.g., skip compression-parity if tier is `smoke`)? Current: docs-only.

2. **Dataset versioning**: Why is the dataset pin a tuple of env vars instead of a single `DATASET_RELEASE` tag? Would simplify bumping and reduce lockstep burden.

3. **Docker image versioning**: Should `cassandra:5.0` be pinned to a specific digest or tag (e.g., `cassandra:5.0.1`) to avoid silent drifts?

4. **Release publish atomicity**: Do `release.yml`, `python-release.yml`, `node-release.yml` run in strict sequence, or could they race? If parallel, a partial failure (e.g., PyPI down, npm up) leaves the repo in an inconsistent state. Is there a rollback strategy?

5. **Workflow-as-code modularity**: Should all "setup Rust + cache" sequences be extracted to a single reusable action to reduce duplication and drift?

6. **CI-driven tier enforcement**: Should a new tool (`cassandra-parity tier-gate`) pre-check a manifest change to auto-FAIL if tier is downgraded (e.g., `byte_for_byte` → `smoke`)? Currently only docs warn.

7. **Artifact cleanup**: Is there a policy for pruning old artifacts (e.g., >90 days)? GH Actions default is 30 days; some workflows override. No central audit of total storage.

8. **nightly-docker-parity.yml** explicit `toolchain: 1.88.0` input: Why is this the only workflow passing `toolchain:` to `setup-rust-ci`? Should be omitted per policy #1990.

9. **Parity-failure automation loop-back**: `parity-failure-issue.yml` files issues, but is there a workflow that **closes** them when the parity lane green? Currently: manual.

10. **`future-rust-canary` lead time**: Advisory canary runs weekly; by the time a red is spotted, Rust stable has been out for a week. Is weekly sufficient, or should it run on release day?

11. **OOM mitigation sustainability**: The 13 GB swapfile + `CARGO_BUILD_JOBS=2` are workarounds. Is there a longer-term plan (e.g., split gate into smaller jobs, or upgrade runner)? Swapfile performance degrades under load.

12. **Self-heal cycle time**: `parity-report-heal` regenerates + opens a PR; owner must manually merge. If regen PR has CI failures, the heal stalls. Should it auto-merge on green, or is owner approval intentional?
