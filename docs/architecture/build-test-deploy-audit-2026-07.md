# Build / Test / Deploy System Audit — 2026-07-17 (rev 2)

Six-lane subagent audit of the full delivery system: local agent-gate, required CI, parity CI
fleet, release/deploy train, fleet orchestration, and test suite + test data. Goal frame: fast,
reliable agentic development iterations — the right check at the right time, local vs CI split
correctly.

This revision integrates the 2026-07-17 owner-team review directly into the text; see Revision
history at the bottom. This document is the synthesis of record. Raw per-lane evidence:
`build-test-deploy-audit-2026-07-lanes.md` (committed companion; where a lane packet conflicts
with this synthesis, this synthesis wins). Remediation epic: #2636 (children #2637–#2660 carry
the authoritative acceptance criteria).

## Executive summary — four cross-cutting themes

1. **Redundant compilation is the prime cost suspect — magnitude unmeasured.** Static analysis
   shows the full gate uses a distinct `--features` combination for nearly every component, so
   Cargo feature-unification recompiles `cqlite-core` once per combo (sccache cannot dedupe
   differing feature flags); CI adds 3–6 cold core compiles per PR across disjoint cache
   namespaces plus one fully duplicated required-context compile; 318 core `--test` bins each
   re-link the core rlib. However, gate-ops records a ~67% *execution* floor for core-tests, which
   cuts against compilation dominating overall — so #2647 mandates profiling the compile/link/
   execution split before any unification work, and the "~4–6 min saved" figure is a hypothesis
   to be confirmed or retired by that measurement.

2. **Load is managed by prose, not mechanism.** There is no *general per-gate CPU/process quota*:
   no `CARGO_BUILD_JOBS` derivation, no `nice`/`taskpolicy`, no load-aware admission. (DHAT and
   sstableloader lanes do set `--test-threads=1`, but for correctness isolation, not load
   control.) Documented failure modes: ~15 concurrent gate-ish processes produced SIGKILLs; two
   concurrent gates produce timing flakes (`test_write_throughput` class). Agents compensate by
   hand-serializing gates with racy pgrep checks. One incident of sccache serving corrupted
   objects occurred under extreme load; the load→corruption causality is unresolved and must be
   characterized before any auto-disable ships (#2641). Same pattern in orchestration: heartbeats
   and claims live in skill prose; the worker-supervisor — the one mechanical loop — never calls
   the heartbeat, so #2499 orphan endgames are structural until the supervisor authors the claim
   ref itself (#2655).

3. **Right check, wrong time.** `exhaustive-regeneration.yml` — the heaviest lane (180 min, full
   Docker regen) — runs daily where the tier doc says weekly: 7 runs/week vs 1, i.e. 7× the
   documented frequency (#2637 embeds the keep-or-fix decision). The nightly umbrella re-proves
   live readback + compaction byte-parity that standalone nightlies proved hours earlier the same
   night. The query-semantics oracle (#1742) — cheap, no Docker — never gates a PR, and coverage
   enforcement is nightly-only (the PR side has only a weaker 30% baseline). Docs-only PRs pay a
   full all-features core compile twice.

4. **Telemetry: gate cost is fine; roborev blockers are high but flat.** From the ledger (289
   records): gate-runs/issue 1.57 recent (vs 1.87 baseline), first-pass 67% — the gate loop is
   already at/below baseline. Blocker classification exists on only 97/289 records; among
   classified, 55/97 = 56.7% overall and 34/60 = 56.7% in the last 60 — **high and stable, not
   rising**. (Never divide blocker counts by all 289: the unclassified records are unobserved,
   not zero.) Mechanizing blocker classes into `--lite` lints (#2656) is promising but
   taxonomy-first: class → count → estimated lint catch rate before implementation.

Also: the deploy train has two genuine safety holes (dueling image builds racing on the release
tag; unguarded real-publish dispatches) and is non-atomic across five immutable registries with a
manual 4-file version bump — non-atomicity cannot be fixed, but version-skew partials and
non-resumable re-runs can (#2639, #2652).

## What's already good (verified — keep, don't touch)

Delta-mode fail-closed rules; finalize-cleanup discipline; the generator suite (the normal local
gate disables Docker and dataset regeneration is CI-owned by policy — heavy generators run only
on scheduled/dispatch workflows with pinned, fetchable outputs); python-binding determinism
harness (#1803 fixed); parity-failure-issue dedup design; perf-regression now signal (flaky
benches advisory, opt-in label); publish credentials CI-OIDC only; tier-contract machine
enforcement (lint, retention, contract checks).

## Prioritized recommendations

Issue bodies under epic #2636 are the authoritative acceptance criteria; this table is the map.

### Quick wins (P1, board `Ready`)

| Issue | Change | Win | Effort |
|---|--------|-----|--------|
| #2637 | exhaustive-regeneration cron → weekly (or ratify daily + fix docs) | Removes 6 of 7 weekly runs of the heaviest CI lane | S |
| #2638 | Remove GHCR tag-push from `flight-ci.yml` image job (main/sha only) | Release image can't be clobbered single-arch by the race with `flight-image.yml` | S |
| #2639 | `trino-publish` `dry_run` default → true; flight-image version dispatch asserts `refs/tags/v$version` == `GITHUB_SHA` (provenance, not prompts) | No accidental publish; no arbitrary SHA wearing a release tag | S |
| #2640 | Per-gate CPU/process quota: derive `CARGO_BUILD_JOBS` + nextest threads from gate slot; `nice`/`taskpolicy` wrap; `CQLITE_GATE_MAX_CONCURRENCY=1` default; delete pgrep-serialize prose | Oversubscription flake class retired mechanically | S–M |
| #2641 | sccache corruption: characterize the incident first (load correlation vs disk/eviction artifact), then pick the mitigation from evidence | No blind auto-disable; corruption class addressed on facts | S |
| #2642 | Apply #2369 record-not-assert to `sstable_performance_regression_tests.rs` + `write_integration.rs:742`; rewire (or consciously retire) the orphaned `cqlite-cli/tests/unit/` directory | Top latent flakes retired; hidden coverage gap closed | S |
| #2643 | `.config/nextest.toml`: timing/docker test-groups, retries scoped to timing tests only | Timing transients absorbed without masking regressions | S |
| #2644 | Query-semantics oracle on `pr-gate.yml`, **fail-closed**: `CQLITE_REQUIRE_FIXTURES=1` + required features; fixtures-absent run must FAIL | The reconciliation bug class can no longer merge green — and can't skip-pass vacuously | S |
| #2645 | Always-emitted docs-only classifier in the required workflow (**no paths-ignore** — a required workflow that doesn't trigger blocks the PR forever); classifier fail-closed on unknown file classes | Near-instant required-green on docs-only PRs, no deadlock risk | S |
| #2646 | Separate **tracked** dataset pin (the fetch-generated `.dataset-pin` stays untracked), consumed by fetch + existing `bump-dataset-pin.sh`; CI check across the 11 workflows carrying the sha | Silent pin drift impossible | S |

### Structural (P2, board `Backlog`)

| Issue | Change | Win | Effort |
|---|--------|-----|--------|
| #2647 | **Step 0: profile** the full gate's compile/link/execution split; only if measured redundant-compile time justifies it, unify the guard-cluster feature sets (dhat/arrow stay isolated) | Either ~minutes/gate or a definitive measurement closing the question | M |
| #2648 | Branch-protection **drift fix first** (committed config still lists 4 legacy contexts live protection no longer requires; the setup script would restore them), then remove the transitional checkout/setup/compile steps from `ci.yml`'s `test` aggregator **retaining its broad-result aggregation**, and delete `m1-ci.yml`'s no-op contexts. `pr-gate`'s `cargo build` step stays (clippy is not a link/codegen substitute) | Duplicate required compile removed safely; config can't resurrect dead contexts | S–M |
| #2649 | Consolidate CI Rust caching (one strategy, shared key; cache for coverage-baseline) | 2–4 fewer cold core compiles per PR | M |
| #2650 | Nightly de-dup via **reusable workflows** (or drop standalone schedules) — preserve the umbrella's citable aggregate pass; note the umbrella's Cassandra source build serves the compaction legs | Each property proven once per night; tier contract intact | M |
| #2651 | Consolidate compression-corruption / cql-type / tombstone-ttl into one matrix workflow | 3 near-identical 60-min YAMLs → 1 | M–L |
| #2652 | Release train: shared preflight (prevents **version-skew** partials — five registries cannot be atomic) + `bump-version.sh` (4 hand-edited files today) + resumable re-runs (skip-existing / tolerate-duplicate) | Skew-halves prevented; failed trains resume instead of hard-failing | M |
| #2653 | python-ci builds/tests the shipped `release-unwind` profile | Release-only breakage caught in CI | S |
| #2654 | Make `validate-agent-plumbing` merge-required on docs PRs (it already runs there; it just doesn't gate) | #2480 class reds the PR, not the post-merge deploy | S |
| #2655 | Supervisor-authored git-ref claims + CI-side reaper (#2499 design); heartbeat-clear refuses refs with open PRs; PROJECTS_TOKEN absence alerts | Orphan endgames closed mechanically; management overhead off dev boxes | M |
| #2656 | Roborev blocker lints, **taxonomy-first** (baseline: 56.7% of classified records, flat) | Review rounds converted to cheap local rounds — if the taxonomy justifies it | M |
| #2657 | Parallel gate sub-lanes for isolatable non-core components | ~2–4 min/gate (to be measured) | M |
| #2658 | Widen `--lite` to compile-check dependent test crates on core diffs; no-jq/no-python3 fallback fails loudly | Fewer lite-green→full-red wasted rounds | M |
| #2659 | Consolidate coverage onto **one** PR-side mechanism (the existing 30% baseline — raise/converge it or auto-file on nightly regression; no third job) | Coverage regressions stop being merge-invisible | M |
| #2660 | Extend parity-failure-issue triggers to the uncovered nightly lanes | No silent red nights | S |

### P2 residue (deliberately not filed — groom later if wanted)

Test-bin consolidation 318→~150 (L); agent-gate heredoc extraction + generated clippy feature
list; node determinism harness; bindings fail-closed centralization; semantic-oracle checklist
lint; flight-image digest to `$GITHUB_STEP_SUMMARY`; rollback runbook (only PyPI yank is
documented today); `PARITY_HEAL_TOKEN` provisioning.

## Owner decisions (not mine to make)

1. **The local-vs-CI merge boundary.** `ci.yml`'s broad jobs substantially duplicate the heaviest
   local gate components but only run post-merge or behind `ci:broad`. Promoting them to required
   and slimming the local full gate trades local wall-clock for merge-feedback latency and
   touches #2433 doctrine. Status quo is coherent only because every merge genuinely runs the
   local full gate.
2. **#2637**: weekly (recommended) vs ratify daily for exhaustive-regeneration.
3. **Merge-latency appetite** for new required checks (#2644, #2654, #2659).
4. **#2648**: live protection has `strict=false`, committed config says `strict=true` — pick one
   deliberately.

## Suggested execution order (reviewer-concurred)

#2648 (drift + duplicate required compile) → #2638 (GHCR tag race) → #2639 (release provenance)
→ #2644 (fail-closed oracle) → #2653 (release-unwind parity), then the remaining Ready batch;
#2647/#2641/#2656 gated behind their measure/characterize/taxonomy first steps.

## Revision history

- **rev 2 (2026-07-17)**: Integrated the owner-team review directly into the summary and tables
  (previously an appended corrections log). Material changes: compilation dominance downgraded to
  a hypothesis with a profiling gate (#2647); telemetry blocker claim corrected to 56.7% of
  classified, flat (the original "57% vs 19% rising" divided by all 289 records and is
  retracted); paths-ignore replaced by an always-emitted classifier (#2645); tracked-pin design
  replaces committing the generated `.dataset-pin` (#2646, scope 11 workflows); pr-gate's
  `cargo build` step retained (#2648, plus drift-first ordering); "source-build BTI" corrected
  (the source build serves compaction legs, #2650); release preflight scoped to version-skew +
  resumability, not atomicity (#2652); flight-image guard is tag→SHA provenance, not a
  confirmation prompt (#2639); `--test-threads` claim corrected (per-gate CPU quota is the
  missing control, #2640); SIGKILL evidence precision (~15 gates; 2 gates ⇒ timing flakes);
  `enhanced_unit_tests.rs` reclassified as an orphaned-coverage gap (#2642); coverage
  consolidation instead of a parallel job (#2659); Docker phrasing corrected to policy-based.
- **rev 1 (2026-07-17)**: Initial six-lane synthesis.

## Evidence

Raw per-lane audit packets (committed): `docs/architecture/build-test-deploy-audit-2026-07-lanes.md`.
Lanes: local gate, required CI, parity fleet, release train, fleet orchestration, test suite +
data. Lane packets are pre-review raw evidence — where they conflict with this synthesis (rev 2),
this synthesis wins.
