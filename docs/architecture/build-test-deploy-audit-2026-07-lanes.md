# Build/Test/Deploy Audit 2026-07-17 — raw per-lane packets

> **PRE-REVIEW RAW EVIDENCE.** These are the six subagent audit packets as returned, before the
> owner-team review. Several claims herein were corrected or retracted — see
> `build-test-deploy-audit-2026-07.md` (rev 2) revision history. Where a packet conflicts with
> the synthesis, **the synthesis wins**. Known-superseded claims are marked ⚠ inline.

---

## Lane 1 — Local gate system (`scripts/agent-gate.sh`)

### Inventory (component → what it runs → est. relative cost)

Full gate = 24 components (`agent-gate.sh:1042`). MAIN lane runs serially in one shared `target/`;
SIDE lane runs the two bindings concurrently in isolated target dirs (`:3452-3491`).

| # | Component | What it runs | Rel. cost |
|---|-----------|--------------|-----------|
| 1 | core-tests | `cargo nextest run -p cqlite-core --features cli-helpers` + `cargo test --doc` (`:3072`) | XL — "67% execution floor" (gate-ops.md:116) |
| 2 | python-bindings (SIDE) | maturin develop + import-verify + pytest (`:1525`) | XL cold, M warm (persistent venv) |
| 3 | node-bindings (SIDE) | `npm ci && npm run build && jest write-readback-content` (`:1596`) | L |
| 4 | clippy | 4 invocations: workspace all-features (5 excludes) + cqlite-core ~40-feature list + cqlite-cli feature list + flight/py/node (`:1454`) | L |
| 5 | cli-tests | Two full passes (default + write-support) over globbed `cqlite-cli/tests/*.rs` (`:3222`) | L |
| 6 | integration-tests | `--no-run` then 6 golden-path `--test` targets (`:3208`) | L |
| 7 | memory-budget | 3 dhat-heap lanes `--test-threads=1` (core×2 + flight) (`:3188`) | M |
| 8–11 | tombstones-scan / scan-offload-guard / work-counters-guard / byte-budget-guard | each a `cargo test -p cqlite-core --test …` under a DISTINCT feature set (`:3160-3186`) | M each |
| 12 | write-tests | `--lib` + roundtrip + compaction_integration, write-support (`:3218`) | M |
| 13 | compaction-byte-parity | Rust re-compaction byte-diff subset (`:1690`) | M |
| 14 | format-compat | `cargo test -p format-compatibility-tests` (`:3217`) | M |
| 15 | arrow-parity-guard | one `--test` under `arrow` (`:3130`) | S–M |
| 16 | minimal-build | `build --no-default-features --features all-compression` + test `--no-run`, `RUSTFLAGS=-D warnings` (`:3398`) | S–M |
| 17 | smoke | build cqlite bin + `smoke-test-all-tables.sh` (`:3415`) | S–M |
| 18–24 | query-semantics-oracle, parity-report, delivery-telemetry, binding-unwind-profile, tooling-tests, file-size, fmt | small cargo/python/bash checks (`:1633-1990`) | S each |

### Findings

1. ⚠ (magnitude unmeasured — #2647 Step 0) Feature-set fragmentation forces ~8–12 redundant
   cqlite-core recompiles. Nearly every component uses a different `--features` set
   (core-tests=`cli-helpers`; tombstones-scan=`write-support,cli-helpers,tombstones` `:3161`;
   scan-offload-guard=`cli-helpers,scan-offload-probe` `:3109`; work-counters-guard=
   `write-support,cli-helpers,state_machine,work-counters` `:3165`; byte-budget-guard=
   `write-support,cli-helpers,state_machine` `:3181`; write-tests=`write-support`;
   arrow-parity=`arrow`; memory-budget=`cli-helpers,dhat-heap,arrow`). Each combo = distinct
   fingerprint = from-scratch compile; sccache cannot dedupe differing feature flags.
2. The guard cluster (#8–12) is unifiable into 1–2 compiles: work-counters-guard and
   byte-budget-guard differ only by `work-counters`; a superset invocation would compile core once
   instead of ~5×. `dhat-heap`/`arrow` must stay isolated (global allocator, `--test-threads=1`).
   ⚠ "~4–6 min saved" is an estimate, not a measurement.
3. MAIN lane is strictly serial; only bindings parallelize (`launch_components` `:3468-3489`).
   memory-budget dhat lanes, smoke, and script components are isolatable into extra lanes.
4. Heavy components duplicate non-required PR CI (`ci.yml` runs core/integration/write/CLI/smoke
   with datasets fetched `ci.yml:96-127`), while the required check is only the thin
   `pr-gate.yml`. Local-vs-CI boundary is an owner decision.
5. ⚠ (causality unresolved — #2641 characterize first) sccache corruption under extreme load: one
   recorded incident (loadavg ~150); the gate trusts sccache unconditionally.
6. ⚠ (precision: DHAT/sstableloader do set `--test-threads=1` for isolation; the missing control
   is a general per-gate CPU quota) The #1825 cap bounds gate count, not per-gate resource use;
   no `cargo -j`/nextest-thread/`nice` load management (`:2855`, gate-ops.md:137).
7. Lite blast-radius: metadata-authoritative and sound (`classify_scoped_plan` `:628`), two
   under-select edges: (a) no jq AND no python3 → silently narrows to `cqlite-core --lib` only
   (`:2118`); (b) a core-src change breaking a separate test crate is invisible to lite — main
   lite-green→full-red source.
8. Delta fail-closed rules correct and appropriately paranoid (`_delta_is_allowed_path` `:640`,
   metadata-verified `.rs` allowance `:702`, node refuses without pre-built module `:2666`).
9. Script health: 3569 lines, 51 functions, 18 arg-hooks, 21 env knobs; cli-tests is a ~170-line
   embedded heredoc program (`:3222-3389`); clippy hardcodes a ~40-item feature string (`:1473`)
   drift-guarded only by nightly `CQLITE_CLIPPY_FULL`.

---

## Lane 2 — Required/correctness CI

### Inventory

| Workflow | PR trigger | push/sched | Role | Cost driver |
|---|---|---|---|---|
| pr-gate.yml | every PR, no path filter (`:4`) | — | THE required check (`required` job `:14`) | full cqlite-core all-features clippy+build+`--lib` tests |
| ci.yml | every PR (`:24-25`) | push main; dispatch | broad lanes behind `ci:broad` label (`:52-54`); `test` job = transitional legacy required context running `pre-merge.sh fast` (`:898-903`) | duplicate fast compile every PR |
| m1-ci.yml | every PR (`:11-12`) | dispatch | two no-op legacy required contexts (`:23,47`) | ~seconds |
| ci-minimal-features.yml | PR if core+manifests (`:6-11`) | push | minimal-feature build (`:50`) | core compile |
| observability-gate.yml | every PR (`:23-25`) | nightly | cheap classifier (`:39`) → correctness if obs paths | classifier ~free |
| fuzz.yml | PR if parser paths (`:20-33`) | nightly | fuzz smoke build | nightly toolchain |
| coverage-baseline.yml | PR if core src (`:9-11`) | nightly | tarpaulin baseline, no cache | instrumented compile |
| coverage.yml | none (`:4-7`) | nightly | ≥77% read-path enforcement — never gates a PR | tarpaulin |
| gate.yml | none | nightly | full agent-gate deep-check on main | full gate |
| quality-gates.yml / future-rust-canary.yml / smoke-tests.yml / workflow-config.yml / project-board-sync.yml | various | schedule-heavy | advisory/hygiene | light |

### Findings

1. Triple redundant required-context emitters per PR: `pr-gate.yml:14` (real), `ci.yml:869`
   transitional `test`, `m1-ci.yml:23/47` no-ops — one full duplicate cqlite-core compile + two
   zombie jobs, purely for stale branch-protection context names.
   ⚠ (post-review: live protection ALREADY requires only `required`; committed
   `branch-protection.json` + `setup-branch-protection.js` are drifted and would RESTORE the
   legacy contexts — fix drift BEFORE deleting workflows; see #2648.)
2. ⚠ (do NOT fix with paths-ignore — required workflow that doesn't trigger blocks the PR; use an
   always-emitted classifier, #2645) Required gate has no path awareness — docs-only PRs compile
   cqlite-core all-features twice.
3. Broad correctness is opt-in on PRs (push main / `ci:broad` / dispatch, `ci.yml:52-54` etc.) —
   on a default PR zero integration/parity/CLI tests gate the merge; sound only because merge
   doctrine requires the local full gate.
4. Coverage ≥77% enforcement never gates a PR (`coverage.yml:4-7`); PR side is the weaker 30%
   baseline. ⚠ (consolidate, don't add a parallel job — #2659.)
5. Fragmented caching: five distinct cache mechanisms; coverage-baseline has none; 3–6 cold core
   compiles per PR from disjoint namespaces.
6. `ci.yml` `test` aggregator exits 0 when `RUN_BROAD != true` (`:913-918`) but correctly checks
   `needs.*.result` in broad mode (`:932-937`).
7. ⚠ (post-review: KEEP the build step — clippy is not a production link/codegen substitute)
   pr-gate compiles core three ways in one job; the standalone `cargo build` after clippy looked
   near-redundant.
8. scripts/ci hygiene: `validate-workflows.rb` live; `ci-timing-summary.sh` consistent; three old
   scripts (Aug 2025 mtimes) worth a drift check.

---

## Lane 3 — Parity/validation CI fleet

### Inventory (workflow → cadence → unique property → cost)

| Workflow | Cadence | Unique property | Cost driver |
|---|---|---|---|
| sstabledump-parity-gate.yml | PR (broad smoke_regex) + nightly | canonical/byte parity vs committed sstabledump JSONL goldens | dataset fetch, 45min |
| cassandra-parity.yml | PR/push (manifest paths) + nightly | manifest lint, tier/retention cross-check, report-heal | light, no Docker |
| cassandra-validation.yml | PR/push (paths) | sstableloader round-trip into real Cassandra | cassandra:5.0.2 service, 30min |
| compaction-parity.yml | PR (paths) + nightly | LOGICAL (PR) + BYTE tier (`gradle byteParity`, nightly) | JVM harness; byte tier builds Cassandra, 60min |
| live-cell-compaction-parity.yml | PR + nightly | live-cell/frozen-UDT compaction byte parity | committed fixtures, cheap |
| compression-corruption-parity.yml | PR (paths) + nightly | CompressionInfo + inline-CRC + corruption verdicts | Docker regen, 60min |
| cql-type-parity.yml | PR (paths) + nightly | CQL type + schema-evolution parity | Docker regen, 60min |
| tombstone-ttl-parity.yml | PR (paths) + nightly | tombstone/TTL/deletion/resurrection parity | Docker regen, 60min |
| delta-roundtrip.yml | PR/push (delta paths) | CDC delta-export round-trip | Docker + regen, 45min |
| e2e-readback.yml | nightly + PR behind `ci:ingest-full` | live read-back semantic parity | compose Cassandra, 30min |
| nightly-docker-parity.yml | nightly | umbrella: readback + BTI sstabledump + compaction logical+byte + Bloom | builds Cassandra from source (Ant) — ⚠ source build serves the COMPACTION legs, not BTI |
| exhaustive-regeneration.yml | cron `10 8 * * *` = DAILY (doc says weekly) | full-corpus coverage/presence audit | 180min, full Docker regen |
| perf-regression.yml | nightly + PR behind `ci:perf` | Criterion read-path 10% wall; flaky benches advisory | double bench build |
| parity-failure-issue{,-tests}.yml | workflow_run on 6 lanes | red-lane → deduped tracking issue | light |

### Redundancy map

- Compaction BYTE parity: compaction-parity nightly `byteParity` AND nightly-docker-parity re-run
  the same gradle target — twice nightly; PR proxy in agent-gate + live-cell + tombstone lanes.
- Live Cassandra read-back: e2e-readback AND nightly-docker-parity leg 1 — twice nightly.
- BTI sstabledump parity: umbrella leg 2 + sstabledump-parity-gate.
- CompressionInfo: committed golden + Docker-regen — intentional strength ladder, not pure dup.

### Findings

1. exhaustive-regeneration runs DAILY vs documented weekly (#1026). ⚠ wording: 7 runs vs 1 —
   "removes 6 of 7", not "6× reduction".
2. nightly-docker-parity duplicates two standalone nightly lanes. ⚠ remedy: reusable workflows or
   drop standalone schedules; preserve the citable aggregate (#2650).
3. Query-semantics oracle (#1742) not on the required PR path — only local gate + nightly gate.yml.
   ⚠ remedy must be fail-closed: `CQLITE_REQUIRE_FIXTURES=1` + required features (#2644).
4. No parity property on the required per-PR check; red-lane latency-to-human ~1 day via nightlies.
5. ⚠ (design: separate tracked pin; sha spans 11 workflows, not ~4 — #2646) `.dataset-pin` is
   untracked (generated by fetch); workflows hardcode `DATASET_SHA256`.
6. Failure routing solid: stable 5-field fingerprint, body-marker dedup, idempotent resolution,
   never auto-closes, self-provisions label, unit-tested. Gap: cassandra-validation, e2e-readback,
   delta-roundtrip, nightly-docker-parity, perf-regression not in the workflow_run list.
7. PARITY_HEAL_TOKEN: absent → SKIP with notice (loud, not silent); merge-race stale report blocks
   the PR queue until manual regen.
8. perf-regression is now signal: flaky benches demoted to advisory; strict gate is read-path at
   10% same-runner; opt-in label per PR.

---

## Lane 4 — Release/deploy train

### Inventory (workflow → trigger → artifact → credentials)

| Workflow | Trigger | Artifact | Credentials | Arch cost |
|---|---|---|---|---|
| release.yml | push tag v* | 6 CLI archives → GH Release; crates.io (core→cli); Homebrew tap | crates.io OIDC; HOMEBREW_TAP_TOKEN | 6 targets |
| python-release.yml | push tag v* | sdist + 5 wheels → PyPI/TestPyPI | PyPI OIDC | 5 wheels |
| node-release.yml | push tag v*, dispatch | 5 `.node` → npm (provenance) | npm OIDC | 5 targets |
| trino-publish.yml | push tag v*, dispatch | Maven Central (`automaticRelease=true`) | MAVEN_CENTRAL_* + GPG | 1 |
| flight-image.yml | push tag v*, dispatch | multi-arch GHCR (vX.Y.Z, vX.Y, latest) | GITHUB_TOKEN | 2 arches |
| flight-ci.yml | push main/tag v*, PR, nightly | flight bin; image job pushes GHCR on tag, single-arch (`:120-153`) | GITHUB_TOKEN | 1 |
| api-docs.yml | push tag v*, dispatch | rustdoc → gh-pages | GITHUB_TOKEN | 1 |
| docs-site.yml | push main, PR, nightly | Astro site → gh-pages | GITHUB_TOKEN | 1 |

### Findings

1. Dueling image builds on a v* tag: flight-image (multi-arch) and flight-ci image job
   (single-arch amd64, `type=ref,event=tag`) push the identical GHCR tag concurrently — last
   writer wins.
2. ⚠ (remedy revised: tag→GITHUB_SHA provenance assertion, not a confirmation prompt — #2639)
   `gh workflow run trino-publish.yml` is an unguarded real publish (`dry_run` defaults false,
   `:37-40,120`); flight-image/api-docs dispatches similarly armed.
3. ⚠ (scope revised: preflight prevents version-skew only; registries can't be atomic — #2652)
   No aggregate abort path: ≥7 independent publish lanes, per-lane tag==manifest checks only,
   immutable targets, no rollback.
4. Version bump fully manual across 4 files (`Cargo.toml:28`, `Cargo.toml:50`,
   `pyproject.toml:7`, `package.json:3`); Trino derives from the tag.
5. CI never exercises the shipped Python profile: release ships `--profile release-unwind`
   (`python-release.yml:96-99`); python-ci builds plain `--release`. Node is consistent.
6. Agent-plumbing lane blocks deploy on main but not the PR: `validate-agent-plumbing.mjs` inside
   docs-site build (`:79-80`); not PR-required → unlinked page merges green, fails on main,
   silently skips deploy (#2480 class).
7. flight-image manual `version` dispatch not SHA-pinned (#2456 class); digest never written to
   job summary (log-grep hunt persists).
8. Non-idempotent re-runs: crates.io skip-if-published OK, Homebrew diff-guarded OK; PyPI has no
   skip-existing; npm/Maven hard-fail on duplicate versions.
9. Redundant from-scratch core compiles across release lanes; per-workflow caches only.
10. Good, keep: command-injection guards (env-passed inputs, semver allowlist); cross glibc floor
    pin; fail-closed secret guard in trino-publish; node dispatch requires pre-existing tag;
    bootstrap carries no publish credentials (publishing is CI-OIDC only).

---

## Lane 5 — Fleet/worker orchestration

### Inventory

- `worker-supervisor.sh` (402 L): one worker process per iteration; mkdir+pid single-instance
  lock (`L179-209`); fail-closed preflight (load>ncpu / leftover cargo / disk<40G → HOLD,
  `L239-276`); crash-loop breaker (`L377`); head-blocked detector (`L356-364`); budgets
  (`L391-394`). SPOF: correctness depends on the worker LLM honoring the marker contract.
- `claim-heartbeat.sh` (213 L): liveness via `refs/heartbeats/<machine>` root-commit push; reap
  threshold documented but enforced nowhere in code — flow-board SKILL prose only.
- `finalize-cleanup.sh` (325 L): the most defensively-correct script in the set (validate-all-
  then-execute, fails closed on >1 lock / dirty / unpushed / unmerged / remote-error).
- `project-board-sync.yml`: closed-unmerged→Done + 30-min null-Status sweep; needs
  `PROJECTS_TOKEN`; no-ops silently if absent.
- gate-slot cap (#1825): real machine-wide semaphore, N flock lockfiles + detached
  `gate_slot_daemon.py`; N = `max(2,(ncpu-2)/4)` (`agent-gate.sh:2825/2855`).
- `bootstrap-agent-machine.sh`: check-only installer; sets no load/CPU policy.

### Telemetry readout (289 records)

| Metric | All 289 | Last 60 | Baseline |
|---|---|---|---|
| gate_runs mean | 1.78 (median 1) | 1.57 | 1.87 |
| first-pass (gate_runs==1) | 56% | 67% | 54% |
| rework mean / % with rework>0 | 1.82 / 66% | 1.93 | 1.92 |
| roborev blockers | ⚠ CORRECTED: 55/97 = 56.7% of CLASSIFIED (field present on 97/289 only) | 34/60 = 56.7% — FLAT | — |
| claim_collisions>0 | 2 total ever | 2 | — |

⚠ The packet's original "19% all-time vs 57% recent" divided by all 289 records (fabricated
zeros) and is retracted. Blocker rate is high but flat; "the bottleneck moved" is withdrawn.

### Findings

1. ⚠ (precision: DHAT/sstableloader set `--test-threads=1` for isolation; missing control is a
   general per-gate CPU/process quota; SIGKILL evidence ~15 concurrent gates, 2 gates ⇒ timing
   flakes) Gate cap limits count, not cores-per-gate; no `cargo -j`/`nice`/`taskpolicy` anywhere.
2. Agents distrust the cap and hand-serialize via pgrep; `CQLITE_GATE_MAX_CONCURRENCY=1` exists
   but defaults ≥2.
3. Heartbeat + reaper entirely prose-driven; worker-supervisor never calls claim-heartbeat
   (grep-verified). Beat-then-crash looks alive 4h.
4. #2499 orphan endgame is structural; the git-ref-claim design candidate is sound (supervisor
   stamps/refreshes a machine+PID-scoped claim ref; reaper checks age+PID+no-open-PR).
5. Board auto-add unreliability real; 30-min sweep is a lag, not a fix; PROJECTS_TOKEN lapse
   silently no-ops the safety net.
6. Heartbeat ref shared across lanes; `cmd_clear` (`L179`) deletes unconditionally.
7. Marker/PID-reuse edges handled well (stale-marker removal `L306`, finalized-without-issue
   rejection `L333`).

---

## Lane 6 — Test suite + test data

### Test-target inventory

370 integration `--test` binaries workspace-wide: cqlite-core/tests 318 (162K lines, avg ~510/bin,
dominated by `issue_NNNN_*` micro-bins), cqlite-cli 41, tools 11; plus lib tests,
integration/format-compat crates, python 30 files, node 35. Heaviest: `parquet_writer_tests.rs`
(4838), `sstableloader_integration.rs` (3821, Docker-gated), `issue_819_differential_compaction.rs`
(1953), `scan_delta_parity_test.rs` (1716), `compaction_integration.rs` (1616),
`write_integration.rs` (1536). No `.config/nextest.toml` exists — no test-groups, no retries, no
slow-timeout config.

### Flake ledger

| Test / class | Class | Status |
|---|---|---|
| collection_benchmarks #2369 | wall-clock in correctness gate | fixed (592a2947) |
| sstable_performance_regression_tests.rs | wall-clock asserts (50ms) in correctness path, lines 92/98/107/116/431/449/472 | LATENT — #2369 rule not applied |
| cli integration_sstable_tests.rs 695/711 | loose 30s/60s ceilings | latent, low risk |
| write_integration.rs:742 | `elapsed < 50ms` | latent, tight |
| cli enhanced_unit_tests.rs:498 | `startup < 100ms` | ⚠ file is ORPHANED from the harness (tests/unit/ referenced by no target) — coverage gap, assert currently inert (#2642) |
| sstable_header_parsing_basic_tests.rs:363 | `< 1000ms` | latent, loose |
| python `ModuleNotFoundError` #1803 | stale editable venv | fixed (determinism harness) |
| index_probes #2451 / StreamWalkScope #2428 | counter scoping | fixed |
| Docker-runtime tests | env-dependent | contained (`CQLITE_SKIP_DOCKER_TESTS=1` default) |

### Findings

1. `sstable_performance_regression_tests.rs` is a live #2369 violation in default core-tests.
2. Target sprawl: 318 core bins; clusters (`issue_953_*`×4, `issue_1577_*`×4, `issue_1578_*`×5,
   `issue_2412_*`×4, `issue_1143_*`×3) consolidatable per epic.
3. No nextest config — no scoped retries or serialization groups.
4. Timing residuals beyond #1 (see ledger).
5. Fail-closed (#2078) is gate-only: bindings suites have no equivalent anti-vacuous guard.
6. Generator suite correctly CI-owned by policy: heavy Docker generators run only on
   schedule/dispatch workflows; outputs pinned + fetched read-only; the normal local gate
   disables Docker.
7. Oracle balance lopsided: 163 physical-dump goldens vs one 5.4KB query-semantics oracle; new
   reconciliation-sensitive features not structurally forced to extend the semantic oracle.
8. Node bindings lack a determinism harness (python's #1803 fix has no node analog).
