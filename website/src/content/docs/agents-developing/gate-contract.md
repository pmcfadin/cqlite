---
title: Gate Contract
description: scripts/agent-gate.sh is THE gate. What it runs, what "passing" means, and the machine-checkable summary block format.
sidebar:
  label: Gate contract
  order: 1
---

`scripts/agent-gate.sh` is THE gate. A builder claiming "the gate passed" must have
run this script and pasted its summary block verbatim. Ad-hoc `cargo` invocations do
not count. This rule exists because epic #646 shipped three false-green reports from
ambiguity about "which commands count" — specifically, feature-gated tests silently
skipping and partial runs reported as full runs.

**CI-enforced as a nightly deep-check (issue #1269, reconciled with epic #1360).** The gate is no longer
local-only: `.github/workflows/gate.yml` runs the *full* `scripts/agent-gate.sh` (never `--only`) in CI
as a **nightly, path-independent deep-check backstop** (`schedule:` cron + `workflow_dispatch` for
on-demand runs). It is **NOT** a required per-PR check — under epic #1360's tiered model the ONE required,
always-running PR check is the light `.github/workflows/pr-gate.yml` (fmt + cqlite-core clippy
`-D warnings` + all-feature build + fast tests; no Docker/datasets/agent-gate). The nightly `gate.yml`
lane fetches the pinned datasets and sets `CQLITE_DATASETS_ROOT` so the dataset-dependent components
execute rather than skip, and uploads the SUMMARY block as an artifact. So a change that breaks a gate
component (e.g. `node-bindings`) that the light PR check cannot see is still caught within 24h and
surfaces on the Actions dashboard.

## What a green `required` covers (issue #2910)

Branch protection requires exactly ONE context, `required` (`.github/workflows/pr-gate.yml`), and a
GitHub Actions job cannot `needs:` a job in another workflow. Until #2910 that meant every tier in a
sibling workflow — the Flight e2e tier, the parity lanes, every label-gated suite — was **invisible to
`gh pr merge --auto`**: a PR could land green with its most important integration test pending, failed,
or never triggered (the real instance: PR #2906's wiring-evidence e2e test ran nowhere, because
`flight-ci.yml` was `paths:`-filtered *and* its heavy job needed a `ci:flight-full` label the PR did not
carry).

`required` now aggregates. Concretely:

- **A declared registry.** `.github/ci-gating-tiers.yml` names every gating tier by workflow file and by
  the exact check-run context it emits (a check run's name is the emitting job's `name:`), plus an
  `exempt:` block listing every other `pull_request` workflow with a reason and an issue link.
- **`required` fails closed** on any registered tier that is `failure`/`timed_out`, still
  non-terminal at the aggregation deadline (60 min default, per-tier override, always strictly below the
  job's `timeout-minutes` so expiry is a reported red rather than a cancellation), or **absent**.
  Absence is NEVER read as inapplicability: a registered tier always emits its context, and an
  inapplicable tier reports that as an explicit **success**. Unregistered check runs (perf, docs recipe
  smoke) stay advisory and cannot block.
- **…but a false RED is an outage too.** Failing closed applies at the DEADLINE; mid-poll, the states
  that are transient and self-correcting are re-polled. A `cancelled` tier is routine — marking a draft
  ready for review or adding a label cancels the tier's in-flight run under `cancel-in-progress` — so it
  is treated as superseded while a replacement is plausible (supersession is detected positively: the
  replacement mints a higher check-run id), and fails once the grace lapses or the deadline arrives. A
  5xx/rate-limit/DNS blip reading the check-runs API is retried under backoff and fails only on
  persistence. Neither weakens a genuine negative into a pass.
- **The diff mandates the tier, not the label.** A registered tier's own classifier decides
  applicability from `git diff --name-only base...head`. A mandating diff runs the tier **with or
  without** its `ci:*` label; the label survives only as an opt-in for non-mandating diffs. **No step of
  the delivery flow asks you to work out which tiers are out of band, or to apply a label.**
- **Enrolment is forced.** `pr-gate.yml` now has two jobs: `pr-gate-core` (the gate's own
  fmt/clippy/test/policy steps) and `required` (`needs: [pr-gate-core]`, `if: always()`, the unchanged
  branch-protection context) which does the aggregating. `scripts/ci/validate-workflows.rb` runs as a
  step in **`pr-gate-core`**, and `required` fails unconditionally when the core job did not conclude
  `success` — so the rule still reds `required`, one job removed. It fails when a
  `pull_request`/`pull_request_target` workflow is neither registered nor exempted, when a registered
  workflow carries a blocking trigger filter (`paths`, `branches`, or a `types:` set that is too narrow
  to fire on every new head sha or wider than the aggregator observes), when its emitting job's
  condition is not exactly `${{ !cancelled() }}`, when no step of that job both reads a dependency's
  `.result` and can exit non-zero, or when a registry entry is dangling. A new tier that forgets to enrol
  **reds `required`**. (`always()` is rejected on a tier gate job: it runs the job *while the run is being
  cancelled*, turning `needs.*.result == cancelled` into a `failure` conclusion, which makes the
  supersession grace unreachable and reds `required` on every routine supersession.)
- **A migration state reds in seconds, not after an hour.** The registry is read from the base ref while
  the emitter comes from the tree the event ran (the merge commit, for a `pull_request` event). If the base
  registers a tier that tree provably cannot emit — its workflow absent, no PR trigger, `types:` excluding
  every activity type that could put the context on this head sha (not merely the event that started this
  run: check runs accumulate on the head from whichever event minted them), `branches:` excluding this
  base, or no job with that name — `required` fails on the first poll and names the remedy:
  **rebase**, or `ci:waive:<tier-id>` if the tier is deliberately being renamed or retired (a registry
  change only takes effect once merged). Inconclusive evidence never produces that verdict, and the verdict
  is never a pass.
- **Arming `--auto` stays correct.** GitHub releases the merge on `required` going green, and `required`
  cannot go green until every registered tier has reported success — so keep arming immediately (#2667)
  and never poll a PR's own CI.
- **Residual — re-run order.** A tier re-run *after* `required` has already gone green cannot be
  retracted by a finished job: **re-run the tier, then re-run `required`**, in that order.
  `scripts/flow/premerge-assert.sh <pr> <certified-sha> <gate-of-record-summary> [<delta-summary>]`
  remains the closer's last look — and since #3465 it is where the gate of record stops being a
  convention: the third argument is REQUIRED, and the script refuses the merge unless that file holds
  one full `==== AGENT-GATE SUMMARY ====` block with `RESULT: PASS`, `tree-integrity: PASS`, no
  `nested-under:` line (#2874: a nested sub-gate runs at the SAME tree, so the sha binding provably
  cannot see it), `dirty: no` (#3648) and `commit:`/`tree-start:` covering the certified sha. The
  `dirty:` requirement is the third property the sha binding cannot see: a `dirty: yes` run certified
  the sha PLUS uncommitted TRACKED edits (the gate's capture is `--exclude-standard`, so never a
  gitignored log) and stamps the very same `commit:`/`tree-start:` it would have stamped clean. It is
  matched AFFIRMATIVELY — an absent, empty or unrecognised value REFUSES rather than being read as
  clean — and there is deliberately no env opt-out, because a dirty tree is always re-gateable. A `--lite` summary is refused
  by name anywhere, and a `--delta` summary as the THIRD argument (their headers are distinct by
  construction) — which is exactly the PR #3408 escape: 22 lite PASSes and no full gate.
  **The OPTIONAL fourth argument is how a `--delta` re-cert certifies a merge.** #1892 *mandates*
  `--delta`, "never a repeat full gate", for a test/docs-only diff on top of a full PASS at anchor
  `X`, and mandates the PR record BOTH blocks — so a 3-arg-only guard red on correct, mandated input.
  In that shape the third argument is the ANCHOR's full PASS (its sha need not be the certified sha)
  and the fourth is one `==== AGENT-GATE DELTA SUMMARY ====` block carrying `MODE: delta` (asserted
  affirmatively, the inverse of the full block's belt), `RESULT: PASS`, `tree-integrity: PASS`,
  `dirty: no` (#3648 — required of the anchor block too, since a dirty anchor hangs the whole chain
  off a tree nobody can reconstruct), a
  `delta-anchor:` naming exactly that anchor — an `(UNRESOLVED)` anchor refuses — and its own
  `commit:`/`tree-start:` at the certified sha. The chain is closed end to end; a delta block ALONE
  is still the #3408 escape and still refused.
  **What a `PREMERGE: OK` does NOT prove (#3650), printed on the success path as `PREMERGE: SCOPE`.**
  It proves the diff is unchanged since certification and that a full gate PASSed on THAT EXACT TREE.
  It does not prove the change was certified against the `main` it will join: a squash-merge composes
  the diff with main's CURRENT tip, so for any PR whose base is behind main the certified tree and the
  merged tree are **different objects** (measured on #3358/PR #3362 — a head gate FAILing only because
  a known flake's fix was on main and not in that base; the malign direction is a PASS at a stale head
  hiding an interaction with something that landed in between). A gate on the merge result is #3650
  **slice 2** and is deliberately not part of this mechanism.
  **#3650 slice 1 DID land, as a non-blocking advisory: `PREMERGE: ADVISORY` lines.** They carry
  `scripts/flow/base-staleness.sh`'s report — `N` commits behind the **merge-base** with `origin/main`
  (never the base ref's tip, #3392) and `M` of those touching the diff's blast radius, defined as
  *(paths the diff touches) + (a hard-coded, no-env-override gate-global set)*: content that can change
  ANY gate's verdict regardless of the diff — a NAMED, COMMITTED list (`GATE_GLOBAL_PATTERNS`), never
  an inline glob, whose membership asserts exactly one predicate: *content here can change a gate's
  verdict INDEPENDENTLY of the diff* (`.config/nextest.toml`, the toolchain pin, the Cargo
  manifests, `scripts/agent-gate.sh`, `scripts/ci/**`, `scripts/tests/**`,
  `cqlite-core/tests/support/**`, `test-data/**`, `.github/workflows/**`). `scripts/tests/**` is in it
  because the gate does not merely READ that roster, it EXECUTES it (`tooling-tests` runs ~16 of them),
  so one commit touching one of those files reds EVERY lane's full gate. Measured against the case that
  produced the issue: on PR #3362 the culprit commit and the diff share NO path, so path intersection
  alone would call that certification fresh exactly when it was not; intersection + gate-global fires on
  37 of 107 commits behind (35%) — measured at `origin/main` `b1e8598a2`, subject `4bc6b913a`, the sha
  quoted because `behind` is a function of where main was — and the
  run NAMES the culprit (`matched 5e08db201 gate-global .config/nextest.toml`) so the detection is
  attributable rather than coincidental on a count. The list is **declared NON-CLOSED in the output**
  (gap 2 of 2, beside the dependency-closure gap), and the two path sources are pinned
  **rename-symmetric and root-relative** — porcelain `git diff` honours `diff.renames`/`diff.relative`
  and plumbing `git diff-tree` does not, so unpinned, a PR that renames a path would lose the old path
  and report `blast-radius 0 RECOGNISED` on a genuinely stale base (a fail-open). **It is
  information, not a verdict** — it cannot change `premerge-assert.sh`'s exit code, and an absent,
  failing, timed-out or `UNMEASURED` advisory is reported and non-fatal. Its 60s bound carries a
  **SIGKILL escalation** (`--kill-after`), because plain `timeout <secs>` only SIGTERMs and then waits,
  so a child ignoring TERM would keep the merge critical path blocked indefinitely; the runner is
  resolved as `timeout` then `gtimeout` (GNU coreutils on macOS) with `--kill-after` support PROBED, and
  where none exists the advisory is **SKIPPED and reported**, never run unbounded or behind a bound a
  child can ignore. Anchoring is the other half of "information, not a verdict", so its output is
  **ANCHORED**: every line, stdout and stderr, begins with `BASE-STALENESS: `, every dynamic field is
  control-character sanitized (git permits newlines in paths, and one would otherwise emit an
  unprefixed line), the verdict appears only on a `verdict ` line carrying a closed-set token, and the
  script's own static template text carries none of `PASS`/`OK`/`RESULT:`, asserted structurally over
  the source. *The earlier, absolute claim — "its output carries no `PASS`, `OK` or `RESULT:` in any
  run" — was FALSIFIED BY REVIEW and is recorded as changed, not softened: the advisory prints
  repository-controlled paths verbatim, and the tracked path
  `test-data/scripts/CI_SMOKE_TEST_USAGE.md` contains `OK`. Declared residual: a path may contain a
  reserved substring, and the anchor is what makes that harmless.* Its no-finding verdict is
  `NO-STALENESS-RECOGNISED` (a scan result, never `FRESH`/`CLEAN`), `M = 0` prints `0 RECOGNISED` and
  never a bare `0`, and every run declares that the blast radius is **not a dependency closure**. A consumer that acts on it (slice
  2) MUST treat exit `5`/`UNMEASURED` as STALE, never as fresh. The three `PREMERGE: SCOPE` lines are
  RETAINED, because the advisory does not close the gap they disclose. **The sha half of the same check closes a second, different escape (PR
  #3616): a real gate, someone else's.** A closer located its gate run dir by recency
  (`ls -t /tmp/agent-gate.*`), read a PEER LANE's dir, saw 33 of 37 components PASS and was about to
  merge #3616 on PR #3580's verdict — everything about it was real, and only the `run-id:` line
  exposed it, read by a human. `premerge-assert.sh` cannot verify `run-id:` (it did not launch the
  gate, and #2874's reader contract belongs to whoever did), so requiring BOTH `commit:` (7 hex) and
  `tree-start:` (12 hex) to match the certified sha is what turns a cross-lane verdict into a
  mechanical refusal: a peer's summary names the OTHER PR's head.
- **Break-glass is per-tier, and it actually works.** `ci:waive:<tier-id>` (an owner action) excuses a
  tier that is **absent** or **pending at the deadline**; it can **never** excuse a failed or cancelled
  one, and there is no blanket waiver. The label takes effect without a re-run: `required` re-reads the
  PR's current labels on every poll, and `pr-gate.yml` subscribes to `labeled`/`unlabeled` so applying one
  to an already-finished gate starts a fresh run that sees it. Each honoured waiver emits a warning
  annotation and a job-summary line naming the tier and the person who **applied the label** — resolved
  from the PR's `labeled` events, not the actor of the run (who is usually whoever pushed or hit re-run);
  an unresolvable attribution says so rather than guessing. A registered tier may not cancel its in-flight
  run on a label event, so the hatch does not fight the tier it waives.
- **A waiver is bound to the head sha you applied it for.** A label persists across pushes, so "waive an
  absent tier on sight" plus a leftover label would waive every later head's tier in the seconds before it
  could report — a permanent bypass. The **immediate** waiver therefore requires the `labeled` event to be
  newer than that head sha's first CI activity (and, for a pending tier, the run must be the one the label
  event itself started). **Push a commit and your waiver stops short-circuiting**: the tier is polled for
  the full deadline, and if it reports a failure in that time it reds the gate. The waiver still applies at
  the deadline, so it delays a verdict rather than pre-empting one. To get the instant hatch back on the
  new head, **remove and re-apply the label** — the diagnostic says so.
- **The waiver's EVIDENCE state is always on the record (issue #3033).** Both behaviours above depend on
  reading this PR's `labeled` events, and a broken read used to look exactly like an ordinary PR: an empty
  feed, one vague warning, a waiver that quietly waited for the deadline with nobody named. The job summary
  now always states what happened — `Waiver evidence: n/a` (no `ci:waive:` label present); `READ OK — feed
  read (N labeled event(s)), M of them for a ci:waive: label in force`; the same `READ OK` with **`0 for a
  waiver label in force now`** (the read worked and carried nothing usable, so attribution stays
  `UNRESOLVED`); or **`UNREADABLE (broken read …)`** carrying the HTTP status, or the command's exit status
  when there is none. `401`/`403`/`404` means the token may not read this PR's events — check the workflow's
  `permissions:`; a `403` naming a rate limit or any `5xx` is transient; no HTTP status means the client
  failed (`gh` absent, bad `--jq`) and a re-run will not help. The counts are **feed total**, the
  **`ci:waive:` history** and the subset **in force** precisely because the feed carries every label's
  events and `labeled` events are immutable: an event for a label since removed is history that can bind
  nothing. A healthy read is evidence that the read works, never that a waiver bound — binding is a
  per-tier verdict against that head sha's first CI activity. An unreadable read is **never** a failure of
  `required`: it withholds the early waiver and the attribution, and the waiver is still honoured at the
  deadline.
- **The report never claims more than it observed (issue #3033 round 4).** The same defect recurred one
  layer out: the summary asserted absence (`no ci:waive: label is present`) even when the LIVE label read
  had failed and the labels in hand were the run-start payload — a confident false negative about a waiver
  applied mid-run, which is what the polling window exists for. So the label read's trustworthiness is now
  tracked and reported: a fallback to the payload reports **`UNKNOWN (label read UNTRUSTED)`** with the
  failed-read count and status, and it makes the in-force count **UNKNOWN** rather than zero. A failed or
  ambiguous read can only ever *withhold* a waiver, never grant one, and none of these states can change
  `required`'s verdict — that comes from tier evaluation alone. The same discipline was then applied to
  **every** line of the block, not only the ones a reviewer named: claims about labels are phrased against
  "the label set this run is using" plus that set's provenance, and a run that admits a read failed carries
  no denial that one did — so you never get `permissions:` advice beside an "authorization is not the
  problem" line, and the suite pins that property across the combined states rather than the wording.
- **A mistyped waiver label is named as a typo (issue #3033 round 6).** A waiver label is
  `ci:waive:<tier-id>` with a LOWER-CASE tier id (`[a-z0-9][a-z0-9-]*`), which is what the evaluator
  matches. Anything else — `ci:waive:Flight` for tier `flight` — waives nothing, so the evidence read is
  never even attempted for it and the summary reports **`INVALID WAIVER LABEL — it waives NOTHING`**, naming
  the offending label and the fix. Previously such a label was reported through the `UNREADABLE`/`UNAVAILABLE`
  diagnostics, which sent the reader to the workflow's `permissions:` block for what is a capitalisation
  typo. An off-shape label applied beside a valid one is still named once, under the valid label's state.
- **Labelling is cheap.** Subscribing to label events must not make every `ci:perf` / board-mirror /
  `needs-decision` label — or the waiver itself — restart a 30-minute gate. So a label mutation never
  cancels the in-flight run (cancellation is conditional on the event action; the shared concurrency group
  still guarantees only one run per PR can report `required`), and the label-triggered run **skips
  `pr-gate-core`** and reuses the core result already recorded for the same head sha. That reuse is
  fail-closed: absent, pending, failed, or skipped-on-a-non-label-event all red `required`, and the run's
  own skipped check run cannot stand in for the real one.
- **Only GitHub Actions can satisfy a tier.** A check-run name is global to the commit and anything
  holding `checks:write` can mint one, so `required` verifies each check run's producer (`app` + an
  Actions run URL) fail-closed. An unverifiable run neither satisfies a tier nor shadows the genuine one,
  and it is named in the red.
- **Unknown never reads as pass, inside a tier either.** A tier's gate job validates its classifier's
  applicability verdict: an empty or non-boolean verdict reds the tier instead of reporting "not
  applicable", and a verdict that claims the tier applies while its work was skipped reds it too.
- **The check is not defined by the thing it checks.** `required` evaluates the aggregator, its ruby
  modules, and `.github/ci-gating-tiers.yml` from the pull request's **base ref**, so a PR cannot gut the
  aggregator or move its own tier into `exempt:` and go green on instructions it wrote. Practical
  consequences: a registry change takes effect **after it merges**, and renaming a registered tier's
  context in the same PR needs a second PR or a `ci:waive:<id>` (the base registry still expects the old
  name). The enrolment policy still validates the HEAD tree — it is judging your change.
- **A tier's mandate covers what reaches it at runtime.** For the Flight tier that is `cqlite-flight/**`,
  `cqlite-core/**`, `test-data/**`, `Cargo.toml`/`Cargo.lock`, `rust-toolchain.toml`, the shared
  `setup-rust-ci` action and the workflow itself — one verdict for the whole tier, so a core-only diff runs
  the end-to-end tests rather than the `--lib` subset. A registered tier may have only ONE applicability
  output; two predicates behind one context is how a mandating diff silently reaches the cheap half.

Offline proofs live in `scripts/tests/test_aggregate_required_tiers.sh` (every check-run state, both
re-run directions, self-exclusion by run identity, waiver cases) and
`scripts/tests/test_gating_registry_policy.sh` (the enrolment rule and its wiring). Both run in the
gate's `tooling-tests` component and both prove non-vacuity with an always-pass stub.

## Components

The gate mirrors the enforced CI gates (`.github/workflows/ci.yml`,
`ci-minimal-features.yml`) plus the local smoke suite:

| Component | Command |
|-----------|---------|
| `fmt` | `cargo fmt --all --check` |
| `clippy` | `RUSTFLAGS="-D warnings"` clippy, **scoped per-package** (issue #1844 — see below) |
| `core-tests` | `cargo test -p cqlite-core --features cli-helpers` (one test skipped — see script) |
| `integration-tests` | seven named `--test` targets in `cqlite-integration-tests` |
| `write-tests` | `cargo test -p cqlite-core --features write-support` (lib + roundtrip + compaction) |
| `cli-tests` | `cargo test -p cqlite-cli --test unit_tests` |
| `pub-surface` | `bash scripts/ci/check-pub-surface.sh` — cqlite-core crate-root declaration-consistency guard (issue #1712): asserts an unconditional, non-`#[doc(hidden)]` top-level `pub mod NAME;` in `cqlite-core/src/lib.rs` is not gated by an inner `#![cfg(...)]` inside `NAME`'s own file. Answered entirely from source — the declaration's attributes structurally from `lib.rs`, the module file's PROLOGUE (rustc-verified to hold every inner attribute) — and it REFUSES rather than guess on any input it cannot classify (unrecognised `pub mod` shape, module file resolving to neither/both legal paths, unreadable file, block comment in a prologue, unclassifiable inner attribute). **NOT public-API drift detection**: no snapshot, no `--regenerate` — that half was removed deliberately in #1712 (five findings, one unbounded-scanner defect class); the principled route is #3366. Source-only, sub-second; SKIP-aware (loud) only on an absent cqlite-core |
| `tooling-tests` | `bash scripts/tests/test_agent_gate_summary.sh` (SUMMARY-capture regression, #1175; SKIP-aware on missing python3) |
| `minimal-build` | `cargo build -p cqlite-core --no-default-features --features all-compression` |
| `flight-tests` | `cargo test --no-fail-fast -p cqlite-flight --lib --bins` — the UNIT suite only, plus a mandatory run-time census naming the 42 integration targets it does NOT run (issue #1699; descoped on measurement, #3384) |
| `legacy-heuristics` | `cargo build -p cqlite-core --features legacy-heuristics`, then `cargo test --no-fail-fast … --lib` + `--test` targets DERIVED from **cargo metadata** — membership by a cfg site anywhere in the target's module closure, or by `required-features` (#1699) |
| `feature-iso-parquet` | `cargo test -p cqlite-core --no-default-features --features all-compression,parquet --lib --no-run` — **without** `delta-scan` (#1699) |
| `feature-iso-delta-scan` | the mirror — **without** `parquet` (#1699) |
| `all-features-check` | `cargo check` **and** `cargo clippy … -- -D warnings`, both at `-p cqlite-core --all-features --all-targets` (issue #3453). The **only** component that enables the OpenTelemetry stack: `clippy` above excludes `observability`/`observability-testing`/`metrics` by #1844 design, `core-tests` runs `--features cli-helpers`, `minimal-build` runs `--no-default-features` — so before this lane no cargo invocation in the gate ever passed `observability`. Package-scoped, **not** `--workspace` (the `duckdb` bundled-source amalgamation belongs to `cqlite-cli` alone, so this stays minutes, not the #916 cost). Declares its DERIVED feature set on every run and FAILs closed if `--all-features` no longer enables its subject. Compiles and lints only — it executes no test, so the runtime half of the class stays `pr-gate-core`'s (see below). Never SKIPs |
| `smoke` | `bash test-data/scripts/smoke-test-all-tables.sh` (against a freshly built debug binary) |

All components run even after a failure so one run reports everything.

### Feature-matrix lanes (issue #1699)

**Compiling a feature is not covering it.** The scoped clippy matrix below enables ~30 cqlite-core features
at once under `--all-targets`, so a feature can be *test-compiled* on every full gate and have **executed
nothing** — and a combined feature set is exactly what MASKS cross-feature coupling (an item gated on feature
A referencing feature B's items compiles fine while both are on). Measured when these lanes landed: turning
execution on for `legacy-heuristics` surfaced 4 tests that had never run once, two of which assert behaviour
CQLite deliberately does not support (#3372, #3374); and `flight-tests` surfaced **14 cqlite-flight targets
that execute nowhere** — not locally, not in CI — because their module-level
`#![cfg(feature = "observability-testing")]` is off in every lane that runs them (#3375). When you add a
feature flag, ask which lane **executes** it, not which lane compiles it. `experimental` is the remaining
known instance (#3373).

**Why the isolation lanes use `cargo test --lib --no-run`, not `cargo check --all-targets`.** The incident
class they exist to catch (#1978) is an ungated `#[cfg(test)]` module referencing a feature-gated item, so a
bare `cargo check` — which never compiles test targets — would be blind to it. But `--all-targets` overshoots
in the other direction: measured, it compiles cqlite-core's ~100 *integration* test files, which assume
default features, and fails on `storage::serialization`, `storage::write_engine` and `cqlite_core::query`
being configured out. That is noise, not leakage. `--lib --no-run` compiles the lib **with** its `cfg(test)`
modules and pulls in no integration target; `minimal-build` is the precedent.

**A narrowed lane DECLARES its narrowing, at run time, on every run.** `flight-tests` executes the unit suite
only, and prints a census naming how many integration targets it does not run (counted from `cargo metadata`,
never hard-coded), why (their ~50% non-determinism, #3384/#3383), and who does run them (CI's Flight tier,
which `required` fails closed on). This is not politeness: a lane that omits coverage silently is
indistinguishable from one that covers it, which is the defect this whole component set exists to remove — so
a silent narrowing would reintroduce it one level down.

**Derive, never curate.** Both executing lanes compute their subject set from committed source at run time,
so a newly added gated file is picked up and a feature joining `default` shrinks the excusal set, with no gate
edit:

- **`legacy-heuristics`** takes its candidate targets from **cargo metadata** — not a `tests/*.rs` glob, which
  cannot see a target gated only by `required-features` nor a directory-style `tests/foo/main.rs` — and
  includes one when a cfg site appears anywhere in that target's **module closure** (`mod` declarations in
  every visibility form, plus `#[path]`, resolved transitively). Its allowed-zero set is derived the same way:
  a target is excused only when NO file in its closure carries a surviving positive cfg site, and a
  manifest-gated target is never excused at all.
- **`flight-tests`** derives its unit-target set (lib + every bin) from cargo metadata, so a newly added
  binary cannot run zero tests unnoticed — a hard-coded list beside a `--bins` selector is a second registry
  that drifts silently.

A cfg shape the census cannot evaluate — `any(…)`, `not(…)`, `cfg_attr` — is reported **unclassified**, never
excused: a token list cannot tell a conjunction from a disjunction, and `any(legacy-heuristics, X)` is
*reachable* here.

The **module closure has the same class of blind spot, one level down**, and it is reported the same way. A
target can reach a child module through a cfg on the `mod` declaration itself —
`#[cfg(all(feature = "state_machine", feature = "cli-helpers"))] #[path = "support/datasets_root.rs"] mod
datasets_root;`, the shape shared test helpers actually use (3 such targets in `cqlite-core` today). The
closure followed such a child while **discarding the attribute gating it**, so a legacy-gated test inside the
subtree counted as *executable* at this lane's feature set, an ungated sibling kept the target non-zero, and
the co-required census reported **no gap** — the one thing that census exists to find. Such a subtree is now
emitted as a `DECLARED GAP` naming the target, the module and the cfg text, with a
`cfg-gated-subtree gaps: N RECOGNISED` census line that is affirmative at `0` (so a pasted census shows the
scan ran) **and that states its own non-exhaustiveness in the emitted text**. The `RECOGNISED` qualifier is
load-bearing rather than decorative: a bare `0` reads as a verified all-clear, and this scan is documented as
incomplete, so the census must say that a clean result means *nothing was recognised* — never that nothing is
there. The same applies to the co-required census, whose populated branch also carries the qualifier: the
disclaimer belongs on the surface a reader acts on, not only in the source comments.

It is **declared, not fatal**, and the distinction is load-bearing: failing the lane on it was implemented,
measured against the real tree, and reverted — those helpers are correct code, and **a lane that reds on
correct input is the lane agents learn to waive**. The `UNRESOLVED` half stays fail-closed, because the two
unknowns are not equivalent: an *incomplete* source set is permissive in membership, the polarity scan and the
census alike, whereas an *unevaluated* one is merely unattributable. Anything appearing in that stream matching
neither report is a FAIL — a closed grammar, since an unrecognised report is an unmeasured state and
inheriting the permissive branch for it is the shape this component set exists to remove. A failed derivation is a FAIL naming the
derivation, never a fallback to "nothing enabled", which would silently excuse every gated target.

**`--no-fail-fast` on both executing lanes.** `cargo test` stops after the first failing test *binary*, and a
lane whose purpose is surfacing never-executed rot must surface all of it in one run. Measured: the first run
reported only `P0_4_modern_format_rejection_tests`; the next then reported 3 further failures the first had
hidden.

**A lane in `--list` is not a lane that works.** `feature-iso-parquet` reports `PASS (0s)` warm, so presence
and a green verdict prove nothing about whether it can fail.
`scripts/tests/test_agent_gate_feature_matrix_lanes.sh` (opt-in; it performs real compiles, so it is not a
default component) plants each lane's incident-class break in a throwaway `git worktree` — never the live
checkout, since #2926 makes a mid-run tree mutation a gate FAIL — and requires the lane to red **and** to
stay green unbroken, so it cannot pass by failing everything. It also requires the red to **name the planted
symbol**: a bare red is not evidence either, because an unrelated breakage produces an identical exit code
and SUMMARY line. Recorded observation: `docs/reports/ah6-1699-feature-matrix-lanes.md`.

### Scoped clippy (issue #1844)

`--workspace --all-features` enables *every* feature on *every* package, which pulls
in two costly artifacts on **every** gate run in **every** worktree:

- the **source-built DuckDB C++ amalgamation** (cqlite-cli `duckdb-tests` feature), and
- the full **OpenTelemetry/OTLP** stack — both the tonic and reqwest transports
  (`observability`/`observability-testing` on core/cli/flight/bindings).

Neither is reusable by any other gate component (`-D warnings` gives clippy a distinct
compile fingerprint), so they were pure per-gate tax. The `clippy` component therefore
runs a **scoped per-package** lint that still covers the whole workspace with
`-D warnings` but excludes only those two feature families. **parquet/arrow are NOT
excluded** — they are reachable in normal builds (the
cli-helpers→state_machine→`cqlite-core/parquet` chain) and stay linted. Both the full
gate and `--lite` use the same scoping. See `run_clippy()` in `scripts/agent-gate.sh`.

Coverage of the excluded features is **moved, not deleted**: set `CQLITE_CLIPPY_FULL=1`
to run the historical `cargo clippy --workspace --all-targets --all-features -D warnings`
matrix. `.github/workflows/gate.yml` (the nightly deep-check) sets it, so a lint that
only fires behind `duckdb-tests` or `observability*` is still caught within 24h. The
per-package feature lists in `run_clippy()` can drift as features are added; that
nightly full pass is the drift backstop.

## Pre-condition: test data must be present

The gate aborts with exit code 1 if no `*-Data.db` files exist under
`$CQLITE_DATASETS_ROOT/sstables`. Fetch them first, and export **the
`export CQLITE_DATASETS_ROOT=<abs>` line the script prints** — on a box that already had the
variable set, the fetch populates *that* root, not the checkout's (issue #3131):

```bash
bash test-data/scripts/fetch-datasets.sh
```

This prevents the failure mode where dataset-dependent tests silently pass on an
empty dataset by returning 0 rows.

The fixture contract has two halves — the SSTable bytes and the committed CQL schemas that
decode them — and the FULL gate now fails closed on either. The two markers are deliberately
distinct text so a pasted SUMMARY separates the causes. The corpus guard runs first, so a run
missing both reports #2078, the half an operator must act on.

**Missing-fixtures fail-closed (issue #2078).** The FULL gate FAILs CLOSED when the
fetched validation corpus (`test_basic/…`) is absent, even though a fresh worktree's
committed byte-parity reference `*-Data.db` files keep the raw Data.db count > 0
(previously a false PASS via SKIP). It stamps `missing-fixtures: FAIL-CLOSED (#2078)`
with the remedy (`bash test-data/scripts/fetch-datasets.sh`);
`AGENT_GATE_ALLOW_MISSING_FIXTURES=1` restores the lenient SKIP and stamps a visible
`missing-fixtures: OPT-OUT (…)` line. `--lite`/`--only` are unchanged (lenient).

**Missing-schemas fail-closed (issue #3148).** The FULL gate also FAILs CLOSED when the
committed CQL schema fixtures are unreachable, stamping
`missing-schemas: FAIL-CLOSED (#3148)`. It checks **readability of the specific canonical
`.cql` files** the dataset-backed components consume, not directory existence, because a
partial copy is the realistic failure. Two causes, each with its own remedy line:

- a canonical `.cql` under the resolved schemas root is not a readable regular file —
  remedy: unset `CQLITE_SCHEMAS_ROOT`, or
  `git restore --source=HEAD -- test-data/schemas`;
- `CQLITE_SCHEMAS_ROOT` was set to a **relative** path and was rejected — remedy: export an
  absolute path, or unset it. A relative override cannot mean the same thing on both sides:
  the gate resolves it against the repository root, cargo resolves it against each test
  binary's *package* directory, so the SUMMARY would certify one schemas root while the tests
  read another.

On success the SUMMARY carries a positive
`schemas: N/N canonical .cql readable under <root> (<source>)` line, so a pasted block shows
the check RAN. `--lite`/`--only` stay lenient. Before #3148 the preflight validated only the
corpus: a layout whose `sstables/` was complete but whose schemas were unreachable passed with
`STATUS: OK`, built for ~8 minutes, then failed `core-tests` + `memory-budget` on opaque
missing-`.cql` panics — worse than no preflight, since the recorded "fixtures verified" pointed
triage at the diff under test.

**There is deliberately no opt-out for `missing-schemas:`**, unlike #2078's
`AGENT_GATE_ALLOW_MISSING_FIXTURES`. The fetched corpus is legitimately absent on a fresh box;
committed source in a checkout never is. An unreachable schemas root means a broken checkout or
a stale override, so an escape hatch could only buy a vacuous green.

The schemas root itself is resolved **checkout-relative**, never as a `..` sibling of
`$CQLITE_DATASETS_ROOT` — see [Test data](/cqlite/agents-developing/test-data/).

**Component-set skew fail-closed (issue #3544).** `scripts/agent-gate.sh` is read **from the
tree under test**, so a branch whose copy predates a component-set expansion on `main` runs the
OLD script, reports a true `N/N nonpass=0`, and is **silent about every component added since**.
Merge-cleanliness cannot see it (`git merge-tree` returns CLEAN — the skew is semantic), and
`required` cannot backstop it: `.github/ci-gating-tiers.yml` exempts the CI feature-matrix lane
*because the local gate owns it*, so each side's coverage is justified by the other's and the
component is exercised by neither. Measured: PR #3467's gate would have certified **31 of 35**
components.

At the mode dispatch — before the #1825 slot and before any component — every mode now compares
its component **set** against `origin/main`'s (never a line count, never a blob hash: a
2,000-line refactor that leaves the set alone is not a coverage problem). The branch side is the
running gate's own in-process `COMPONENTS` array; the baseline side is **read as DATA**, from the
committed manifest `scripts/agent-gate.components` — see *The baseline is data, never code* below.
The baseline is **fetched in the same invocation as the comparison**, because a remote-tracking
ref is a *cached observable* and a stale one returns "no skew" against a superseded `main`. Every
SUMMARY block carries a `component-set:` line:

- `component-set: PASS (36/36 vs origin/main <sha40> via the committed manifest)` — affirmative,
  and it **names the baseline sha and how the baseline was read**: a verdict that does not name
  its baseline cannot be audited;
- `component-set: FAIL-CLOSED (#3544) — this tree is BEHIND origin/main <sha40>; N …
  MISSING … : <names>` — the branch is behind (`origin/main` is **not** an ancestor of `HEAD`).
  Remedy: `git fetch origin && git rebase origin/main`;
- `component-set: DECLARED (#3544) — this branch REMOVES … <names>` — components are missing,
  `origin/main` IS an ancestor of `HEAD`, **and the components are absent at `HEAD` too**, so the
  removal is in this branch's own **committed** diff. Loud, **not fatal**: the author has nothing
  to rebase, only the information is actionable, and a guard that reds on correct input is the
  guard agents learn to waive. (Behind **and** removing fails as BEHIND first, and reaches
  DECLARED only after a rebase.)
- `component-set: FAIL-CLOSED (#3544) — … PRESENT in the gate script AT HEAD …` — the
  `UNCOMMITTED` verdict. **Ancestry alone is not provenance**: "is `origin/main` reachable from
  `HEAD`?" is not "did this branch's committed diff remove the component?", and answering the
  first while asserting the second was a reproduced false PASS — deleting one component from the
  **working copy** alone produced a non-fatal `DECLARED` in a certifying mode, so a full gate
  would have certified 35 of 36 components under a factually false line. The provenance is
  therefore measured against **`HEAD`'s own component set**, not the proxy "is the tree dirty"
  (which would red every mid-edit branch and still prove nothing on a clean-but-stale one). An
  uncommitted **addition** still PASSes — extra components are never skew. Remedy: commit the
  removal, or restore the component; never rebase.
- `component-set: FAIL-CLOSED (#3544) — baseline NOT measured (<kind>: <detail>)` — the fetch
  failed, `origin` is missing, **`origin` does not NAME the canonical upstream**, `git` is
  absent, neither the baseline's manifest nor its gate script could be read, the manifest was
  empty or ungrammatical, the gate script's `COMPONENTS=(…)` declaration could not be read as
  text, **whether the baseline carries a manifest could not be determined at all**
  (`baseline-probe-unmeasured`), `HEAD`'s own set is unmeasurable, or the probe could not be
  BOUNDED. **Never a
  SKIP and never a fallback to an empty baseline**: an empty baseline excuses every branch,
  which is the vacuous pass inverted. The bound is itself a named capability
  (`timeout`/`gtimeout`/a pure-bash watchdog/`none`) and an **unboundable host does not run
  the fetch at all** — a missing capability must not inherit the permissive branch, and an
  unbounded fetch could hang `--lite` on a network stall or an auth prompt.

- `component-set: FAIL-CLOSED (#3544) — the LOCAL component manifest is not usable
  (`manifest-missing` | `manifest-garbage` | `manifest-stale`)` — **this tree's own**
  `scripts/agent-gate.components` is absent, ungrammatical, or out of step with its `COMPONENTS`
  array (order included). Remedy: regenerate and commit it — never anything to do with `origin`.

A component present on the branch but absent from `main` is **not** skew (this branch may be the
one adding it) and is recorded as `[branch-only, NOT skew: …]` inside a PASS.

**The baseline is data, never code (`REQ-3544-01`).** The first design derived the baseline set
by extracting `origin/main:scripts/agent-gate.sh` and **running** it (`bash <fetched> --list`).
**Six of that mechanism's seven High-severity review findings traced to that one decision**, and
its three fixes each moved the hole one layer outward — a symbolic remote name, then the validated
URL, then the URL in `argv`. That is the signature of a **shared channel between data and
control**, where this project's standing ruling (issue #3312) is to *remove* the channel rather
than choose a rarer delimiter. So the baseline now comes from `git show
<sha>:scripts/agent-gate.components`, parsed under a **closed grammar**: one component name per
line, blank lines and `#` comments skipped, and anything else — including a name with leading or
trailing whitespace — a **named refusal** (a parser that trims is a parser that guesses).

State what this **converts** the findings into rather than claiming they are gone: a redirected or
hostile baseline now yields a **wrong component list, which the comparison itself detects**,
instead of arbitrary code execution with the developer's credentials. Everything built for the old
mechanism is **kept as defence in depth**: the canonical-identity and transport/host/port/path
pinning, the isolated fetch (the validated URL written into a `0600` config by a shell builtin, so
it never enters `argv`), the verified transfer hop, the mode-dependent bound, shallow-ancestry
handling and the redact-and-flatten detail path.

**A check must be inside the window it certifies — not before it, not after the harm.** The
pre-flight ran *before* the concurrency-slot wait, and the post-slot tree recapture then reset the
certification window: an edit made while the run was queued became the new starting tree under a
stale `component-set:` verdict. The recapture is deliberate, so the pre-flight is repeated inside
the window it opens. And HEAD's manifest was *trusted* while the local one is *verified* against the
local declaration every run — so provenance now reads HEAD's committed `COMPONENTS` declaration
directly and does not consult HEAD's manifest: remove the second source rather than reconcile it.

**A symlink is a blob, and a graft outlives `--no-replace-objects`.** The presence probe accepted
every `blob`, but a symlink is one — the difference is the mode — so the working-tree validation
*followed* the link and saw a full manifest while `git show <rev>:<path>` printed the link's target
text: `agent-gate.components -> fmt` validated locally and published a one-component baseline. And
`$GIT_DIR/info/grafts` rewrites parentage while `--no-replace-objects` does **not** disable it, so
on the object-reuse path (where ancestry still ran in the live repository) a graft could reclassify
missing components from a fatal `BEHIND` to a non-fatal `DECLARED`. Ancestry now runs in the
isolated repository on **both** paths. The pattern worth carrying: every live-repository read
preserved for speed has turned into a route.

**Stop rendering the value rather than sanitising it again.** The rejected-origin diagnostic was
the *fifth* finding in one family — raw URL, then unflattened, then unredacted stderr, then
scheme-only redaction, then query strings and multi-`@` authorities. Every fix improved the
sanitiser, and the set of places a secret can hide in a URL does not close, so the URL is no longer
published at all: the line names the **axis** the origin was rejected on, plus the normalised
identity only when that identity is itself grammatically clean. Note the layer distinction that
cost a regression on the way: the normalised value is a **comparison key**, not a diagnostic
string — reducing it made every local path compare equal.

**An allowlist has to reach the sites a later change adds.** The migrated object reads ran under a
bare `env`, inheriting the caller's environment — the same hole as round 13, re-opened at the new
sites. Every git call in the pre-flight now runs under `env -i` plus the one allowlist, including
the state probes (injected config could have made a real partial clone look non-partial and
re-opened the fast path). Two corrections came with it: a config file does **not** keep a URL out
of every argv — git passes the configured URL to a transport *helper*, whose command line then
carries the token — so a credential-bearing origin is **refused** (userinfo must be absent or
exactly `git`); and a specified control must be required to have *worked* — the `chmod 600` on the
isolated config is fail-closed with the resulting mode verified.

**A local read can be a network operation.** In a *partial* clone, `ls-tree`/`show`/`cat-file`
answer a missing object by fetching it from the **promisor remote** — under the live repository's
local config, so an `insteadOf` plus an enabled external protocol executes a remote helper, and the
lazy fetch also writes objects into the shared store. That was the third route of one family, so
**every baseline and HEAD object read, and the ancestry walk, now run inside the isolated scratch
repository**, with the lane's object directory supplied as an alternate — pure object storage,
carrying no config and therefore no promisor and nothing for a helper to be invoked from. Ancestry
compares against HEAD *resolved to a sha in the checkout*, because inside the scratch the ref `HEAD`
would mean the scratch's own unborn HEAD. The fast path is gated on the clone not being partial,
and `GIT_NO_LAZY_FETCH=1` is carried as a belt rather than the control (git ≥ 2.36; an unset
variable does nothing silently).

**Untrusted repository state is bigger than config.** Closing git's *config* sources and treating
"untrusted repository state" as closed with them left three holes. **Replacement refs**:
`refs/replace/<sha>` transparently substitutes another commit, so the pre-flight reported the
canonical sha while reading a forged, smaller manifest — and passed. `GIT_NO_REPLACE_OBJECTS=1`
plus `--no-replace-objects` on every lane-local object read closes it. **The transfer hop could
execute**: `git fetch` in the *live* repository reads its *local* config — only the environment is
sanitisable, a `.git/config` is a file — so a local `url.*.insteadOf` with
`protocol.ext.allow=always` rewrote the scratch path to an `ext::` remote helper and ran commands
**during** the fetch, before the sha comparison that was supposed to make the hop "untrusted but
safe". A check placed **after** a harmful effect can only *report* it, never *prevent* it — so where the
harm is execution, the control must be that the execution cannot be **reached**. The test asserts
unreachability, with a positive control proving the attack does execute in a plain repository. There is therefore
**no import**: the scratch object store is exposed through `GIT_ALTERNATE_OBJECT_DIRECTORIES` (an
object *source*, not a transport), and nothing is written into the shared `.git` — no pack, no
ref, no `FETCH_HEAD`. That is safe for the reason the transport was not: every read is by a **sha**
whose provenance is the isolated chain, and git objects are content-addressed. **A leaked scp-form
credential**, the third instance of one family, is fixed by *narrowing what is accepted* (scp
userinfo must be exactly `git`) rather than widening the scrubber a third time. And **cleanup now
runs on signals** (INT/TERM/HUP), because bash runs no EXIT trap for a signal that still has its
default disposition — the second axis of "cleanup registration precedes resource creation".

**The isolated hop's environment is an allowlist, and objects are fetched only when absent.**
Neutralising `GIT_CONFIG_GLOBAL`/`GIT_CONFIG_SYSTEM` and stopping there left the hop inheriting
`GIT_CONFIG_COUNT`/`KEY_*`/`VALUE_*`, `GIT_CONFIG_PARAMETERS` and `GIT_TEMPLATE_DIR` — all three
measured to redirect a fetch through `url.<attacker>.insteadOf` (the template by seeding the new
repository's own *local* config, which global/system neutralisation cannot touch). A redirect of
the **isolated** hop is worse than one of the transfer: both observations then come from the
attacker, so the equality assert compares two values that agree and emits a **false PASS**. Every
isolated git call therefore runs under **`env -i` plus an allowlist** — admit what git needs to
*reach and authenticate to* the remote (each entry with its reason), clear everything that can
change *what it fetches* or *what it runs* — so a new git environment variable is cleared by
default instead of having to be discovered. Lane-local reads are deliberately not wrapped: the
only value needing provenance is the **sha**, and everything addressed by it is content-addressed.

The sha itself comes from a **ref oracle** (`git ls-remote`, no objects), and objects are fetched
only when this repository does not already hold that commit. Measured on one fleet box: a plain
fetch into the fresh scratch repository cost **3.74 s and 92 MB of full history on every gate
invocation**; the oracle costs 0.29 s and 120 KB, and the whole pre-flight went 3.9 s → 0.51 s.
`--filter=blob:none` (0.73 s, 6.4 MB) was rejected **on measurement**: the manifest and gate
blobs are read in this repository at the baseline sha, so a blob-filtered transfer leaves them
absent exactly when `main` has changed them — a correct tree failing. "Fetched in this
invocation" is about the staleness of the *ref value*, which is still read live; and the oracle's
output is remote-controlled text, so it is **validated** (object id of a known length plus the
ref name that was asked for) rather than parsed — `baseline-ref-unparsable` otherwise.

**The local manifest is asserted against the running array on every run**, fail-closed, before
anything is fetched. That assert is what makes a manifest baseline trustworthy at all: without it
the file is an unverified claim, and a branch that grew `COMPONENTS` without regenerating the
manifest would — once merged — leave `main`'s manifest **short**, so every later branch would
compare against a too-small baseline and silently excuse real skew. Regenerate with:

```bash
{ sed -n -e '/^[^#]/q' -e p scripts/agent-gate.components; scripts/agent-gate.sh --list; } \
  > /tmp/agent-gate.components && mv /tmp/agent-gate.components scripts/agent-gate.components
```

**One transitional fallback, also data-only — and it is unreachable by *assertion*, not by
reasoning.** The baseline's tree is **probed first**, as its own step, with **three** outcomes:

| probe result | behaviour |
|---|---|
| `present` | the manifest and **nothing else**. Every failure of that read — unreadable, bound exceeded, ungrammatical, empty — is an **error**; the textual path is a **hard refusal** here. |
| `verified-absent` | the transitional text extraction, and the `component-set:` line **names it**. |
| `could-not-tell` | **REFUSE** (`baseline-probe-unmeasured`). Not the fallback. |

"The fallback is self-limiting — unreachable once the manifest is on `main`" was true and **not
enough**: that is a property somebody *reasoned about* and nothing measured, so a refactor, a
baseline pointed at an older commit, or an accidentally deleted manifest would silently re-enable
the brittle path — a pass derived from the **absence** of a bad signal. And **`git show` cannot
answer the question**: its non-zero exit conflates "no such path" with "bad object" with "the
repository could not be read", which is the two-valued-predicate error (a predicate that must
collapse "cannot tell" onto one of its answers always picks the permissive one). `git ls-tree
<rev> -- <path>` separates them affirmatively — rc 0 with an entry, rc 0 with **no** entry, rc ≠ 0
— and an entry that is not a `blob` is its own refusal. The payoff is **mechanical expiry instead
of trust**: once the manifest is on `main`, every later baseline measures `present`, so path 2 is
dead code that any attempt to enter *errors*, at the cost of one extra bounded probe.

When it does run, the extractor reads the gate script's single-line top-level `COMPONENTS=(…)`
declaration **as text** — never executed — and **refuses loudly on any shape it does not
recognise, naming it** ("is not a SINGLE-LINE literal"; a multi-line or computed array, more than
one declaration, a character outside the name grammar). That refusal branch is itself tested with
a reflowed array, because an untested refusal on the exact axis known to be brittle is not
coverage. It is format-brittle in a **shared** direction — a reflow on `main` refuses for every
branch at once, which is fail-closed rather than a false green, and the named diagnostic turns a
fleet-wide stop into a five-minute fix. Every baseline-bearing verdict line ends by naming its
baseline source (`— baseline read via the committed manifest` / `— baseline read via the TEXTUAL
FALLBACK: … VERIFIED ABSENT at that sha`), so use of the fallback is visible rather than inferred.

**And it ends by naming the OBJECT provenance too, because the shared object store is TRUSTED, not
verified** (`; objects: baseline REUSED from this lane's SHARED store` / `baseline FETCHED from the
canonical remote, HEAD's own from this lane's SHARED store` / `provenance NOT RECORDED` — each
followed by `store TRUSTED, not verified (#3746)`). Git does not rehash a packed object against the
id it was asked for on an ordinary read, and on this fleet **every lane on a box is a worktree of
one shared `.git`**, so a peer lane planting a forged pack/index can make a canonical sha resolve to
a shortened manifest — a false `PASS`, and a non-invoker route, hence a defect.

It is **declared rather than closed** because removal does not close it. The ancestry walk and the
provenance leg read HEAD's **committed** content, which has no source other than that store — the
working tree cannot substitute, since the `UNCOMMITTED` verdict exists precisely to compare against
what is committed — so a forged HEAD object still turns `UNCOMMITTED` (fatal) into `DECLARED`
(non-fatal) after removal, while charging every `--lite` round for a half-closure (measured
3.41 s / 93 MB full, 3.58 s / 45 MB at `--depth=1`; shallow is *not* cheaper — it still ships the
tip's whole tree). A check that claims nothing false is worth more than one claiming a closure it
does not deliver. Closing it properly — including the possibility that the real subject is the
infrastructure decision that lanes share an object store at all — is **#3746**.

**The baseline's identity is validated before the fetch.** Trusting a remote merely *named*
`origin` made `git remote set-url origin <anything>` a git-config-shaped opt-out — and it fires
**by accident** in the fork workflow, where `origin` legitimately names a contributor's fork whose
`main` may be months stale, so the guard compares against the wrong baseline and stamps a `PASS`.
`origin` must therefore name the canonical upstream **host included** —
`github.com/pmcfadin/cqlite`, one hard-coded literal, matched by **exact equality** after
normalising the spellings git accepts for that one repository (`https://`, `http://`, `git://`,
`ssh://`, scp-like `git@host:owner/repo`, optional userinfo, an `ssh://` port, an optional `www.`,
an optional `.git`, any case).

**Owner/repo alone is not enough, and "err toward accepting an ambiguous host" was wrong here.**
It accepted `https://evil.example/pmcfadin/cqlite` and — needing no hostile host at all — **any
local path** ending in those two segments. While the pre-flight still *ran* the baseline's copy
of the gate that admitted arbitrary code and not merely a wrong baseline; under `REQ-3544-01` what
it buys is a baseline of unknown **provenance**, from which no PASS may be derived, so the check
stays exactly as strict — as defence in depth. Anything unverifiable from the string (an ssh config
alias, a mirror, a bare local path, `file://`, a look-alike host such as `github.com.evil.tld`) is
a **named non-PASS** (`remote-not-canonical`), as is an `origin` with no URL
(`remote-unreadable`) — each with its own remedy, never a silent pass and never a SKIP.

**The grammar is closed axis by axis** — transport, userinfo, host, port, path — each with one
stated rule, because three successive rounds were "too permissive" in a *new* place (no host;
host but no transport; `http://`/`git://` accepted). **`https://` is now the ONLY accepted
transport.** `http://` and `git://` authenticate nothing, so an on-path impersonator of the
hostname supplies the objects this run certifies against — and when the rule was written those
objects were *executed*, which is why it was a High.

**`ssh://`, `git+ssh://`, `ssh+git://` and scp-form `git@host:path` were accepted and are now
REFUSED** (`ssh-transport:<form>`). The isolated environment must admit `HOME` so OpenSSH can find
keys and `known_hosts`, and OpenSSH then also honours **`~/.ssh/config`**, where a `Host github.com`
rule can rewrite `HostName` or run `ProxyCommand`/`Match exec`. That is a redirected baseline *and*
arbitrary execution behind a URL string that passes the identity check. It is **in model because
HOME is shared**: every lane runs as one user with a writable home, so the planter is a **peer
lane**, not the invoker. It was met by **descope, not hardening** — a bounded residual was
unavailable because `ProxyCommand` executes, and the usual mitigation (a redirected baseline
degrades to a wrong component list the comparison detects) does not reach a harm that lands during
*transport*, before any comparison. Measured cost: nil — every lane and CI already use `https://`,
so an ssh-form checkout fails closed with the remedy named. A non-default port is a
different endpoint and is rejected. **Userinfo is accepted** — GitHub Actions rewrites `origin`
to `https://x-access-token:<TOKEN>@github.com/…`, so rejecting it would red a legitimate CI
checkout — and is therefore **redacted** from every rendering, because SUMMARY blocks are routinely
pasted into PR comments.

Two related properties of the same pre-flight. The baseline is fetched into a **private per-run
`refs/worktree/…` ref**, never `FETCH_HEAD`: `--refmap=` removed the shared *tracking-ref* write
and left `FETCH_HEAD` carrying the baseline, and `FETCH_HEAD` is itself a single shared mutable
file that a concurrent fetch overwrites between the fetch and the read — the run would then
compare against a commit it never fetched. And `git merge-base --is-ancestor`'s
**rc 1 is three-valued too**: in a shallow clone it also means "the connecting history is
absent", so it is read as "not an ancestor" only in a repository *proven* complete (unmeasurable
shallowness ⇒ `INDETERMINATE`); otherwise a legitimate committed removal in a shallow checkout
reds as `BEHIND` — a false FAIL on correct input.

Hermetic self-tests use **local** origins, which are deliberately not canonical, so they
**substitute the artifact**: one shared helper
(`scripts/tests/lib/agent-gate-canonical-pin.sh`) rewrites the canonical literal in the fixture's
own scratch copy of the gate and then verifies the pin took. Never a settable seam — the first
design accepted local paths *so that the fixtures would work*, which made the test hook and the
vulnerability the same fact.

Fail-closed in the **certifying** modes only — the full gate and `--delta`, the two whose blocks
are recorded in a PR. `--lite` and `--only` stamp the same line with an `ADVISORY-*` token and
cannot fail on it: `--lite` runs every fix round and must not require the network to function.
**There is deliberately no opt-out**, for the same reason as `missing-schemas:` — a branch behind
`main` can always rebase, so an escape hatch could only buy a vacuous green.

## Running the gate

```bash
# Full gate — the only run that counts
scripts/agent-gate.sh

# Fast iteration loop — NOT the gate of record (issue #1821)
scripts/agent-gate.sh --lite

# Test/docs-only re-cert after a full PASS at X — NOT the gate of record (issue #1892)
scripts/agent-gate.sh --delta X --anchor-run-id <X's full-gate run-id>

# Debugging aid only — output marked PARTIAL, never counts
scripts/agent-gate.sh --only fmt,clippy

# List components without running (also --lite-list / --delta-list)
scripts/agent-gate.sh --list
```

Exit codes: `0` = PASS, `1` = FAIL/REFUSED (`--delta`), `2` = usage/anchor error
(`--delta`), `3` = PARTIAL (`--only` mode).

## Tiered gate: `--lite` iterate, full gate once (issue #1821)

The gate is tiered. `scripts/agent-gate.sh --lite` runs only the reduced component
set (`LITE_COMPONENTS`: file-size + fmt + scoped workspace clippy + `roborev-lints`
+ blast-radius-scoped tests). It is the **fast iteration loop, NOT the gate of
record** — it emits a DISTINCT `==== AGENT-GATE LITE SUMMARY ====` block
(`MODE: lite`) that must **never** be pasted as the full SUMMARY. Iterate on
`--lite` every fix round; run the FULL `scripts/agent-gate.sh` **exactly once**
before merge. `--lite` never replaces the full gate.

**`--lite` is NOT a flat `~1–5 min` budget — its cost is a FUNCTION of the diff
(issue #3764).** There are **two cost drivers, and only ONE of them scales with
your diff.**

1. **`clippy` is NOT diff-scoped.** `--lite` dispatches the IDENTICAL `run_clippy`
   the full gate does — `run_component clippy run_clippy` at
   `scripts/agent-gate.sh:17233` and `:18220` respectively — i.e. the issue #1844
   **per-package scoped workspace** matrix at `:9357`, and `run_clippy` never reads
   the diff. (The whole-workspace `--all-features --all-targets` pass is the
   `CQLITE_CLIPPY_FULL=1` path only; do not read the scoped matrix as that one.) So
   **every** `--lite` pays clippy IN FULL whatever the diff: measured over 188
   completed lite runs it is a no-op warm, 2–7 min part-warm, and **16–24 min
   cold**.
2. **`scoped-tests` is diff-scoped, and it has a fan-out leg.** It RUNS the touched
   package's `--lib` plus the diff's new `--test` targets (owners by longest-prefix
   path match over `cargo metadata`, from `merge-base(HEAD, <base>)...HEAD` — where
   `<base>` is the FIRST of `origin/main` → `main` → `origin/master` → `master` that
   resolves (`:16870`) — **plus `git diff HEAD`, i.e. the uncommitted diff over
   TRACKED files only, untracked excluded**; defaults to `cqlite-core --lib` when
   no rust package is in the diff) — **and when a changed path is under
   `cqlite-core/src/` it ALSO runs `cargo test -p <pkg> --all-targets --no-run` for
   every workspace member that DIRECTLY DECLARES a dependency on `cqlite-core`
   (the `--no-deps` metadata edge) and owns a `--test` target (issue #2658:
   COMPILE-CHECKED, never run).** That leg — not "touched packages",
   which consult no dependency edge at all — is why a core-src diff annotates 9–11
   package sets, and its `--all-targets` is what balloons `target/debug/deps`
   (**+18 GB in a single round** — reported by another lane in issues #3763/#3764,
   not measured here).
   **`cqlite-core/tests/**` does NOT trigger the fan-out; `cqlite-core/src/**`
   does.**

**Measured bands** (completed runs, one fleet box): a **narrow, WARM-clippy** diff
is **median 1.4 min** (n=43) — so the `~1–5 min` this page used to claim is that case
exactly, a **FLOOR and not a range**; a **`cqlite-core/src/`** diff is **median 20
min, range 3.8–43 min** (n=20). **The two bands are marginal over DIFFERENT subsets
and do not compose** — a 1.4 min run is by construction one that paid no cold clippy,
so the cold-clippy band is not additive on top of it. Beyond those, lane-3612
**reports** (not measured here) **up to ~104 min under peer load** in issue #3764.

**And `--lite` is EXEMPT from the issue #1825 gate-slot cap** (as are `--delta` and
`--only`) — it runs outside slot arbitration entirely, so on a shared box its build
competes with a peer's gate of record for disk and CPU with nothing arbitrating it.
**There is NO admission check for `--lite` today, and issue #3763 owns that gap** —
read this as a budgeting fact, not as an instruction to apply one yourself.

**Division of labor (issues #1855, #2084).** In the worker → subagent model, an
implementer subagent (`sstable-developer`) edits, commits, pushes, and verifies
with `--lite`/targeted tests **only** — it must **never** invoke the full gate. The
ONE full gate of record runs inside the disposable **`flow-closer`** subagent
(spawned per issue by `flow-implement`), which invokes it via `run_in_background`
with the summary-file pattern and **never idle-waits** — a subagent idle-waiting on
a 12–25 min gate gets killed by the stall watchdog and orphans its child gate
process (issue #1855). Review runs **before** that gate (see
[Delivery pipeline](/cqlite/agents-developing/delivery-pipeline/)).

## Test/docs-only delta re-certification: `--delta` (issue #1892)

Once the full gate has PASSed at a commit `X`, a post-review polish round whose
only changes are **tests and/or docs** does not need a whole new full gate — the
full gate at `X` already validated clippy, core-tests, bindings, parity, and smoke
against the production code, none of which the polish round touched. Re-certify the
`X..Y` diff with:

```bash
scripts/agent-gate.sh --delta X --anchor-run-id <X's full-gate run-id>
# or read the run-id from the recorded full SUMMARY (refuses a non-full block):
scripts/agent-gate.sh --delta X --anchor-summary-file <path-to-X-full-SUMMARY>
```

`--delta` verifies the diff `X..Y` (committed + working tree) touches **ONLY** what
the re-cert can **EXECUTE**: rust cargo test code (`.rs` under `tests/` dirs,
`*_test(s).rs` anywhere), python binding tests (`bindings/python/tests/` — run by
the issue-1893 python tier), Node.js binding tests (`bindings/node/__test__/*`, run
against an ALREADY-BUILT native module), shell self-tests (`scripts/tests/*.sh`),
and/or docs (`*.md` anywhere; **top-level-anchored** `docs/`, `website/` only —
issue #2081 moved node/shell from refused to executed). It is **fail-closed**:
anything else (src, scripts, workflows, `Cargo.*`, config, test-data, or an unbuilt
node module — it NEVER builds with cargo and never passes vacuously) makes it
**REFUSE** and name the offending files — a production change always requires a
fresh full gate. On pass it runs **only** file-size + fmt + the diff's changed test
targets (the same blast-radius scoper `--lite` uses) and emits a DISTINCT
`==== AGENT-GATE DELTA SUMMARY ====` block (`MODE: delta`, recovery default
`.agent-gate-delta-summary.txt`) carrying a `delta-executors:` line naming which
executors ran.

The delta block is **not the gate of record** and carries an explicit
`gate-of-record:` line naming the full PASS at `X` plus the anchor run-id, so it can
never be pasted as a full SUMMARY. **Record BOTH artifacts in the PR:** the anchor's
full SUMMARY (the gate of record) AND the `X..Y` DELTA block. Any production change
resets this — the next gate of record is a fresh full `scripts/agent-gate.sh` PASS.

**Standing backstop (owner condition, 2026-07-04).** Long-term quality is
backstopped by the nightly full run on `main`: `.github/workflows/gate.yml`
(deep-check) re-runs the FULL gate with `CQLITE_CLIPPY_FULL=1`, deeper than the local
gate. Delta re-certification leans on that nightly as the net for anything a
test/docs round scoped past. `--delta` (like `--lite` and `--only`) is EXEMPT from
the machine-wide concurrency cap.

## CITE-AND-WAIVE: waiving a red on a genuinely prose diff (#3042, narrowed by #3250)

A **genuinely prose** diff cannot change the compiled binary, so a test failure in
its full gate is by definition **pre-existing on `main` or a flake**, and the
correct response is CITE-AND-WAIVE — never a source patch to turn the gate green
(that is a real change smuggled in under a docs diff, certified by nothing, and it
masks the actual main-red).

The waiver's precondition is that the diff touches **no compiled input**: no `src`,
no `Cargo.*`, no build script, no workflow, no test-data. **That qualifier is a
path-shape test, and a path shape is not evidence. Don't judge it — run it:**

```bash
git diff --name-only origin/main...HEAD | bash scripts/ci/classify-docs-only.sh
# exit 0 => docs-only  (the waiver is available)
# exit 1 => full       (the waiver does NOT apply — the failure is presumed yours)
```

`scripts/ci/classify-docs-only.sh` is the same classifier that decides whether
`pr-gate-core` runs at all, so running it here asks the gate's own question rather
than a paraphrase of it. It answers documentation only on an **affirmative**
allowlist match — prose/images/legal text, inert report artifacts under `docs/`,
and code-bearing report formats (`.json`, `.html`) **only inside** an
artifact-bearing directory — and fails closed on every unrecognized extension and
on **every** extensionless path. `scripts/tests/test_classify_docs_only.sh` pins
that behaviour, and it runs inside the full gate's `tooling-tests` component.

**Why the old wording was unsafe, in one concrete case.** This repository ships
measurement harnesses under `docs/reports/*-artifacts/` **by convention**, so a
PR-#3222-shaped diff contains `src/main.rs` **and** `Cargo.toml` under
`docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness/`. It satisfies "no
`src`, no `Cargo.*`" **textually** while being false **materially** — measured, the
three merged `docs/`-only PRs #3222 / #3081 / #3216 carried 35 / 30 / 1 executable
or config-as-code paths and reported `required` green in 13–16 s against a
~14-minute baseline. An agent correctly following the old text would waive a red
that was genuinely its own.

Everything else about the rule is unchanged:

1. Confirm the diff really is non-compiling input — with the classifier, not by eye.
2. Identify the failure as a **known** main-red issue or a known flake; reproduce it
   on a clean `origin/main` checkout if it is not already filed, and FILE it if not.
3. Record the waiver in the PR body, naming the failing component **and** the issue
   number it belongs to. **A waiver with no cited issue is not a waiver — it is an
   unexplained red.**

Conversely, **if ANY compiled input is in the diff the waiver is void**: the failure
is presumed yours until proven otherwise.

**Same definition, two sides.** This is the gate-side spelling of the review-side
rule in [roborev findings](/cqlite/agents-developing/roborev-findings/): "docs-only"
means a **code-free census**, never a `docs/` path prefix. The two definitions are
cross-referenced deliberately so they cannot drift apart again — #3229 fixed the
reviewer's copy of this bug and #3250 fixed the gate's.

## New-machine setup

A fresh machine that will run the gate should first run
`bash scripts/bootstrap-agent-machine.sh` (see
`docs/development/agent-machine-setup.md`): it installs/verifies the accelerators
below, the datasets, `gh` auth + the `project` scope, and roborev's local config,
then prints the gate's `accelerators:` line as a health check.

## Accelerators are LOUD when missing (issue #1848)

Every optional accelerator the gate depends on is auto-detected, and every SUMMARY
block (full **and** `--lite`) carries a machine-checkable line:

```
accelerators: sccache=on nextest=on lanes=on
```

On **Linux** the line additionally carries a `mold=` token and a `perf=` token
(byte-identical / no tokens on macOS — both are Linux-only):

```
accelerators: sccache=on nextest=on lanes=on sccache-health=ok sccache-cap=32212254720 sccache-used=1375141619(4%) mold=linked perf=ok
cpu-budget: wrapper=nice ncpu=16 max-concurrency=1(pinned) cores-per-gate=16 build-jobs=16(derived) test-threads=16
```

(The `cpu-budget:` line is a sibling of `accelerators:` and carries the resolved
slot cap **and where it came from** — see the concurrency-cap section below.)

- **`sccache`** — cross-worktree compile cache (~25.6% faster fresh builds).
- **`nextest`** — parallel `core-tests` (the gate's long pole).
- **`lanes`** — parallel gate components (needs bash ≥4.3 for `wait -n`).
- **`mold`** (issue #2859, **Linux only**) — the fast linker, wired via a per-machine
  `~/.cargo/config.toml` managed block. States: `mold=linked` (wired) · `overridden`
  (a global `RUSTFLAGS` is suppressing the wired flags — don't export one on a worker)
  · `present-unconfigured` (installed but not wired — re-run bootstrap) · `absent`.
- **`perf`** (issue #3249, **Linux only**) — *can this box be profiled at all?* A free
  read of `/proc/sys/kernel/{perf_event_paranoid,kptr_restrict}` through shell builtins.
  **Free is an enforced cost, not a slogan**: the emit-time path runs **zero external
  processes and zero command substitutions** (a `$( )` forks a subshell, so a value read
  back through one would not be free — hence the token comes back through a
  caller-named variable), and the helper is sourced **once per gate run**, not per
  summary. `test_agent_gate_summary.sh` case `perf-free` kills any regression: it counts
  the substitutions statically and re-runs the extracted path with an unresolvable
  `PATH` under xtrace subshell counting. States: `perf=ok` (unprivileged per-CPU
  profiling **and** kernel symbols available) · `paranoid-<N>` (`perf_event_paranoid`
  = N ≥ 1 forbids **CPU-wide** events, so the `perf stat -C <cpu>` the measurement
  doctrine mandates is **denied** — a *permission* verdict, not a missing capability;
  agent images ship `4`, which on Debian/Ubuntu kernels denies perf entirely) ·
  `kptr-restricted` (paranoid is fine but `kptr_restrict != 0`, so kernel frames
  resolve to bare addresses — a silent attribution loss) · `absent` (the `/proc`
  controls are not present, e.g. a container — tune the host) · `unknown` (present but
  unparseable; never guessed). Anything but `ok` on a box you intend to measure means
  **re-run `bash scripts/bootstrap-agent-machine.sh --yes`**, which installs
  `/etc/sysctl.d/99-cqlite-perf.conf` and then *verifies* it by running
  `perf stat -C 0 -e cycles`. Rationale + security posture:
  `docs/development/fleet-runbook.md`.

State values: **`on`** (detected & used) · **`absent`** (missing → the gate prints a
loud `WARN:` on STDERR with the one-line install command) · **`off`** (intentionally
disabled via `CQLITE_DISABLE_SCCACHE` / `CQLITE_DISABLE_NEXTEST` / `AGENT_GATE_JOBS=1`;
**no warn**) · **`lanes=serial`** (degraded by bash <4.3). An intentional opt-out is
`off`, never `absent`. This exists because a machine silently ran ~3x slower for weeks
with sccache and nextest both un-installed and no signal. If a pasted SUMMARY shows
`absent`, install the tool — the state is visible in the block, not just scrollback.

Two further tokens carry the sccache **capacity** facts (issue #3727):
`sccache-cap=<bytes>` and `sccache-used=<bytes>(<N>%)` — **measured bytes, with no provenance
classifier** (lead ruling `req-3727-w4`: reporting stays, interpreting goes; the 7-state source
suffix, the value-grammar map and the remediation WARNs were removed, and their state-combination
knowledge lives in the follow-up issue). Each has an explicit `unmeasured(<why>)` /
`na(sccache-not-in-use)` rendering, so a byte count is always an affirmative measurement.

The cap is read from the **running server's** JSON and **attributed to a server by a differential**,
which is the one thing you must not skip when reading a `--show-stats` number: with no server
running, `--show-stats` does not start one and answers `max_cache_size` from the CLIENT's own
environment, and a null `cache_size` does **not** tell the two apart (a running server with an empty
cache reports null too). A running server's answer does not move when the client's
`SCCACHE_CACHE_SIZE` changes; a client's does. Unattributed therefore renders
`unmeasured(no-running-server)` or `unmeasured(unattributed)` — never a byte count.

sccache reads `SCCACHE_CACHE_SIZE` **once, at server startup**, so raising the value has no effect
until `sccache --stop-server`. Measured trap: `30G` is 30 GiB but **`30GiB` and `30GB` are SILENTLY
DISCARDED** to sccache's 10 GiB default, and a bare integer means BYTES — with no diagnostic
anywhere. **`sccache-health` cannot answer any of this**: it is the sum of four ERROR counters with
no capacity input, so a `warn` there can never be cleared by raising the cap and a permanently-full,
thrashing cache reads `ok`. Persist and verify the cap with
`bash scripts/bootstrap-agent-machine.sh --fix-sccache-cap`, which correlates the
`/etc/environment` line, the value a fresh non-login PAM session sees, that value in bytes (its own
isolated sccache oracle) and the bytes the running server enforces. **Declared residual:** a LOGIN
shell can see a different value (on this fleet `/etc/profile.d` runs after `pam_env`) and that
context is no longer measured, so the verdict is scoped to the non-login session in its own output.

## Machine-wide concurrency cap (issue #1825)

Running many sessions/worktrees at once used to let ~15 full gates hit the CPU
simultaneously (load 30–60), which SIGKILLed gates mid-`core-tests`. The full gate
now takes a **cross-process bounded semaphore**: at most **N** full
`agent-gate.sh` runs execute machine-wide at once. Excess invocations **queue**
(block) for a slot and print one line — `waiting for gate slot (N in use)…` — then
proceed when a slot frees. **They never fail from the cap**, and a non-interactive
caller blocks cleanly rather than spin-failing.

- **`--lite`, `--delta`, and `--only` runs are EXEMPT** — never queued. `--lite`
  and `--delta` are cheap by design; `--only` is a PARTIAL run (and is used by
  nested tooling self-tests, so capping it could self-deadlock the queue).
- **N** defaults to `max(2, floor((ncpu-2)/4))` — a conservative fraction of cores
  that still lets a couple of gates run on a small box. Override with
  `CQLITE_GATE_MAX_CONCURRENCY`.
- **Every SUMMARY says where N came from (issue #3414).** The `cpu-budget:` line
  stamps `max-concurrency=N(pinned|default|invalid|clamped)`, the same idiom as
  `build-jobs=N(derived|caller)` beside it: `pinned` = the env var held a valid
  integer ≥ 1 and was used verbatim, `default` = it was unset so N is the formula
  above, `invalid` = it was empty or non-numeric and was silently discarded for the
  formula, `clamped` = it was a valid integer < 1 and was silently raised to 1.
  `3` and `3 because nothing set it` are different operational facts — read
  `N(default)` on a fleet box as *the pin is not provisioned*.

  **The remedy DIFFERS BY TOKEN, and getting that wrong sends you in a circle.** A
  `default` box has no pin line at all, so
  `bash scripts/bootstrap-agent-machine.sh --fix-gate-pin` (or `--yes`) persists one.
  An `invalid` or `clamped` box ALREADY HAS the line, holding a bad value — and
  bootstrap deliberately never rewrites an existing value, because a box running >1
  gate on purpose must not be clobbered — so re-running it there is a **silent
  no-op**. Fix the VALUE by hand in `/etc/environment`. Bootstrap says the same thing
  at the same fork, as `gate-pin: NOT-HONOURED`.
- **SIGKILL-safe stale-slot reaping:** each slot is an `fcntl.flock` held by a
  small background daemon (`scripts/lib/gate_slot_daemon.py`) that the gate starts
  and monitors. Because the daemon opens the lock fd *after* it is forked, the
  gate's heavy children (`cargo`/`nextest`) never inherit the lock — a SIGKILLed
  gate frees its slot within one poll interval even while orphaned children run on.
  A crashed gate can never permanently leak a slot.
- Works **across worktrees** (shared slot dir, not per-checkout) and composes with
  the per-gate component parallelism (`AGENT_GATE_JOBS`) and `sccache`: the cap
  bounds the *worst case* (several sessions hitting their one full gate at once),
  the others cut average load and per-compile time.

**Environment knobs** (all optional):

```bash
CQLITE_GATE_MAX_CONCURRENCY=4 bash scripts/agent-gate.sh   # raise N on a big box
CQLITE_GATE_SLOTS_DIR=/path bash scripts/agent-gate.sh     # slot dir (default $TMPDIR/cqlite-gate-slots)
CQLITE_GATE_POLL_SECS=1 bash scripts/agent-gate.sh         # queue/liveness poll (default 2s)
CQLITE_GATE_DISABLE_CAP=1 bash scripts/agent-gate.sh       # force-disable the cap
```

The cap fails **open** (disabled, with a loud stderr note) when `python3` or the
daemon is unavailable — the gate must never be un-runnable because of the cap. A
hermetic self-test proving queueing at N, `--lite` exemption, and SIGKILL slot
release lives at `scripts/tests/test_gate_concurrency_cap.sh` and runs inside the
`tooling-tests` component.

## Capturing the gate: the summary-file redirect is the DEFAULT (issues #1175, #2079)

The SUMMARY block is the only gate text an agent retains — and the raw gate log
(thousands of lines) must **never** be read into a persistent agent context. So
the **required default invocation everywhere** — the full gate AND each `--lite`
round — sets a summary file in advance and reads it, rather than streaming stdout:

```bash
AGENT_GATE_SUMMARY_FILE=/tmp/gate-summary.txt \
  bash scripts/agent-gate.sh > gate.log 2>&1 < /dev/null
cat /tmp/gate-summary.txt   # complete SUMMARY block; gate.log is never read into context
```

This is the default because it is **also** the robust path. Under **non-foreground**
capture (a `script`/pty, a buffering wrapper, a "drain-until-EOF then write"
reader, or a backgrounded pipeline) a streamed SUMMARY block can be lost entirely:
a gate component sometimes leaks a descendant (a `cargo`/`rustc` build server, a
daemonizing test, etc.) that keeps the gate's stdout pipe open, so an until-EOF
reader never sees EOF, gets killed by a timeout, and discards its in-memory buffer
— even though the gate exited 0. (Detaching the gate's *own* stdout cannot fix
this: the leaked child still holds its inherited copy of the pipe write-end.) The
summary file does not depend on the stream at all — pick the path in advance and
read it:

- **Set `AGENT_GATE_SUMMARY_FILE=/path` before running.** The gate writes the
  complete SUMMARY to that exact path with plain redirection, so the file is
  complete no matter what happens to stdout. `cat` it afterward; it always
  contains the full block (start marker → `RESULT: PASS`/`RESULT: FAIL` → end
  marker). Prefer `run_in_background` (or a long timeout) so a subagent never
  idle-waits on the gate and gets watchdog-killed (issue #1855).

  ```bash
  AGENT_GATE_SUMMARY_FILE=/tmp/gate-summary.txt \
    bash scripts/agent-gate.sh > gate.log 2>&1 < /dev/null
  cat /tmp/gate-summary.txt   # complete SUMMARY, even if gate.log truncated
  ```

> **Completion probe — anchor on `PASS|FAIL`; `INCOMPLETE` is a liveness placeholder,
> not a verdict (#3041; mechanism follow-up #2908).** The gate writes
> `RESULT: INCOMPLETE (gate did not finish)` into `$AGENT_GATE_SUMMARY_FILE` **at
> launch** (its EXIT-trap startup sentinel, written *before* the #1825 slot is even
> acquired) and only *overwrites* it with `RESULT: PASS`/`RESULT: FAIL` on completion.
> The placeholder is deliberate — it makes a killed/orphaned gate detectable — but it
> means a bare `grep -q` on the bare `RESULT:` token is satisfied the instant the gate starts, before a
> single component has run. An agent polling that way can read a just-launched (or
> queued) gate as a finished one and advance toward merge on a verdict that does not
> exist. The only correct predicate is:
>
> ```bash
> grep -qE 'RESULT: (PASS|FAIL)' "$AGENT_GATE_SUMMARY_FILE"   # a VERDICT ⇒ gate finished
> ```
>
> A terminal `RESULT: INCOMPLETE` means "still running, or died" — never a certification.

- **If you don't set it,** the gate writes the same complete block to the
  documented default `$PWD/.agent-gate-summary.txt` (gitignored). If your streamed
  capture looks truncated (missing the `==== END AGENT-GATE SUMMARY ====`
  marker), `cat` that file — it is always complete.

> **Concurrency caveat (#1175):** the default `$PWD/.agent-gate-summary.txt` is
> per-*checkout*, not per-run. If you run multiple **top-level** gates concurrently
> **in the same checkout**, each MUST set a unique `AGENT_GATE_SUMMARY_FILE` or they
> will clobber each other's recovery artifact. Separate worktrees get distinct repo
> roots and so distinct default paths — already isolated, which is CQLite's normal
> model. The `run-id:` line lets a caller that captured the invocation's run-id
> confirm it is reading the right run; a caller with no expected run-id and a
> fully-lost stream cannot disambiguate two same-checkout runs, so it must use a
> unique path.
>
> **Nested / self-test sub-gates are auto-isolated (#2874):** a gate spawned by an
> enclosing gate inherits `AGENT_GATE_PARENT_RUN_ID`; if it does not pin its own
> `AGENT_GATE_SUMMARY_FILE` it defaults to its OWN `$LOG_DIR/summary-primary.txt` (never the
> checkout default) and stamps `nested-under: <parent-run-id>`, so a nested run can
> never clobber the parent gate of record. And a mid-run summary clobber (a foreign
> `run-id` appearing in the file) is caught at the next component boundary — and at the
> terminal emit — with a named **`summary-integrity: FAIL`** line + `RESULT: FAIL`, never
> a bare `INCOMPLETE` death. The old #2751 workaround (run the full gate *without*
> `AGENT_GATE_SUMMARY_FILE`) is therefore obsolete — the summary-file redirect is the
> default again, and running it alongside a peer lane's gate self-tests on one box is
> safe. (Two *top-level* full gates sharing one checkout default still need distinct
> paths, per the caveat above.)
>
> **No-clobber + reader contract (#2874).** When a gate finds the contended path already
> holding a FOREIGN `run-id` (a live peer owns it — the only way this arises is two
> top-level gates sharing one checkout-default path), it **does not rewrite that path**:
> it publishes its own FAIL verdict to a non-clobbering sibling
> `<summary-file>.integrity-fail.<run-id>` plus the `logs:` bundle (and stdout/stderr) and
> exits non-zero, deliberately leaving the peer's block on the pinned path rather than
> clobbering it. The reader contract that makes this safe: **the process exit code is
> primary, and any pinned-path block MUST be validated by its `run-id:` line** before you
> trust it — a block whose `run-id` is not the one you launched (even `RESULT: PASS`) is a
> peer's verdict, not yours. On a `run-id` mismatch (or a non-zero exit with a foreign
> block at the path), read the `.integrity-fail.<run-id>` sibling / `logs:` bundle for your
> run's verdict. A closer polling the summary file should glob `"$SUMMARY_FILE".integrity-fail.*`
> and reject any block whose `run-id` differs from the one it launched.

The path the gate used is also echoed on the `summary-file:` line inside the
block, and a copy is kept in the `logs:` bundle. The streamed copy is best-effort
only.

A fast regression test for this emission path lives at
`scripts/tests/test_agent_gate_summary.sh` (run it directly:
`bash scripts/tests/test_agent_gate_summary.sh`). It exercises
`scripts/agent-gate.sh --emit-summary-selftest`, which prints a representative
SUMMARY block through the real emission code without running the 5–8 minute gate.
The gate runs this test automatically as the `tooling-tests` component, so the
capture guarantee is enforced on every gate run.

## A green gate does not subsume `pr-gate-core` (#3453)

The gate and the `required` CI check **overlap; neither contains the other**. This is
structural, and worth stating because a green SUMMARY reads like a prediction that CI
will pass.

- **The gate runs lanes CI cannot.** `arrow-parity-guard` names a
  `#![cfg(feature = "arrow")]` integration target that pr-gate's `cargo test -p
  cqlite-core --lib --all-features` compiles no path to (a `--lib` run reaches no
  `tests/` target); the feature-matrix, binding and parity lanes are local-only in the
  same way.
- **CI runs a lane the gate does not.** That same `--lib --all-features` invocation
  **executes** cqlite-core's unit suite with the OTLP stack ON, at a feature set no gate
  component executes.

**The size of the gap, measured on `main` rather than cited from an incident:**

```
cargo test -p cqlite-core --features cli-helpers --lib -- --list   ->  3562 tests
cargo test -p cqlite-core --all-features         --lib -- --list   ->  3782 tests
```

**220 cqlite-core lib tests execute in `pr-gate-core` and nowhere in the gate of record.**
#3382's own fix pin — `a_stats_only_name_cannot_create_an_instrument_through_the_emit_path`
— is one of them, and the gate's feature set cannot even *list* it (0 matches vs 1). That
is how PR #3382 earned a **31/31 gate PASS without executing the test pinning its own
fix**. The issue was filed around that single instance; the standing gap is 220 tests wide.

`all-features-check` closes the **compile/lint half** of that gap — a type error, or a
`-D warnings` lint, inside a `#[cfg(feature = "observability")]` item now reds the gate of
record. It **deliberately does not close the runtime half**: it executes none of those 220, so an
order-dependent defect of #3382's shape — a process-wide `OnceLock<Instruments>` poisoned
by whichever test binds the global meter to the no-op provider first, invisible to
`#[serial_test::serial]` grouping — still fails **only** in CI. (Those tests are gated on
`observability-testing`, not `observability`.) A full all-features *test* lane was
rejected on cost: tens of minutes on every endgame to duplicate a required check.

So: a red CI check on a green-gate PR is an ordinary event, not evidence the gate
malfunctioned.

## Machine-checkable summary block

The gate emits a block between `==== AGENT-GATE SUMMARY ====` markers. The last
line is always `RESULT: PASS` or `RESULT: FAIL`. Paste this block verbatim in your
PR report — prose summaries are not accepted.

**Format (as emitted by `scripts/agent-gate.sh`; ABRIDGED — 13 of the 37 component rows are
shown, and the meta lines a run also emits are `component-set:`, `accelerators:` and
`cpu-budget:`):**

This block previously called itself *exact* and was not — it showed a subset of rows and had
drifted behind the emitted format. "Exact" is a claim this page cannot keep true across every
component addition, so it now says what it is. Every field name, and the whole `NON-EXHAUSTIVE`
clause, are verbatim from a real run; only counts and shas are placeholders, as elsewhere here.

```
==== AGENT-GATE SUMMARY ====
commit: <short-sha> branch: <branch> dirty: yes|no
datasets: <N> Data.db files under <CQLITE_DATASETS_ROOT>
schemas: <N>/<N> canonical .cql readable under <root> (checkout-relative|CQLITE_SCHEMAS_ROOT override)
ci-pins: DATASET_TAG: <tag>  DATASET_ASSET: <asset>  DATASET_SHA256: <sha>  
tree-start: <head-sha12> dirty: yes|no digest: <digest12>
tree-end:   <head-sha12> dirty: yes|no digest: <digest12>
tree-integrity: PASS
census: <A>/<N> components AFFIRMED a count; <G> DECLARED-GAP (RECOGNISED); <U> NOT-MEASURED (RECOGNISED); <Z> measured-ZERO (RECOGNISED); <X> not-applicable (component did not PASS); <Y> no-subject (PASSed; the run had nothing to measure); <D> UNDECLARED; <W> unrecognised; <V> row(s) carry a VACUOUS status. NON-EXHAUSTIVE: the gap set is CURATED, so an unaffirmed component is UNMEASURED, never verified (#3625; the remaining gaps are tracked in #3162).
fmt:               PASS|FAIL|VACUOUS (<Ns>)  [fmt workspace features=n/a]  {no census — cargo fmt --all --check emits no per-file tally to count}
clippy:            PASS|FAIL|VACUOUS (<Ns>)  [clippy workspace(excl 5) --all-features | clippy cqlite-core --features 33:all-compression,arrow,bench-internals,+30 more | ...]  {no census — cargo clippy emits a per-crate tally only COLD; a warm run prints Finished alone}
core-tests:        PASS|FAIL|VACUOUS (<Ns>)  [test cqlite-core --features cli-helpers]  {verified: <n> tests passed (across <k> result line(s))}
format-compat:     PASS|FAIL|VACUOUS (<Ns>)  [test format-compatibility-tests default-features]  {verified: 10 tests passed (across 1 result line(s))}
integration-tests: PASS|FAIL|VACUOUS (<Ns>)  [test cqlite-integration-tests default-features x2]  {verified: <n> tests passed and <k> test binaries built/verified}
write-tests:       PASS|FAIL|VACUOUS (<Ns>)  [test cqlite-core --features write-support x3]  {verified: <n> tests passed (across <k> result line(s))}
cli-tests:         PASS|FAIL|VACUOUS (<Ns>)  [test cqlite-cli default-features | test cqlite-cli --features write-support]  {verified: <n> tests passed (across <k> result line(s))}
minimal-build:     PASS|FAIL|VACUOUS (<Ns>)  [build cqlite-core --no-default-features --features all-compression | test cqlite-core --no-default-features --features all-compression]  {verified: <n> test binaries built/verified}
all-features-check: PASS|FAIL|VACUOUS (<Ns>)  [check cqlite-core --all-features | clippy cqlite-core --all-features]  {no census — cargo check/clippy passes execute no tests; the subject is a feature set, not a count}
pub-surface:       PASS|FAIL|VACUOUS (<Ns>)  [no-cargo]  {no census — shell/python guard prints no AGENT-GATE-CENSUS contract line yet (#3162)}
python-bindings:   PASS|SKIP (<Ns>)  [via maturin: feature set NOT observed]  {verified: <n> pytest tests passed}
tooling-tests:     FAIL (<Ns>)  [test ws0-corpus-gen default-features | + cargo not observable: cargo may run inside ~60 nested test scripts (child processes)]  {no census: component ended FAIL, so there is no PASS to affirm}
smoke:             PASS|FAIL|VACUOUS (<Ns>)  [build cqlite-cli default-features]  {no census — smoke-test-all-tables.sh prints no machine-readable table count (#3162)}
logs: /tmp/agent-gate.<random>
summary-file: <AGENT_GATE_SUMMARY_FILE or $PWD/.agent-gate-summary.txt>
RESULT: PASS
==== END AGENT-GATE SUMMARY ====
```

**If `--only` was used** (PARTIAL run — never counts as gate):

```
==== AGENT-GATE SUMMARY ====
commit: <short-sha> branch: <branch> dirty: yes|no
datasets: <N> Data.db files under <CQLITE_DATASETS_ROOT>
ci-pins: ...
tree-start: <head-sha12> dirty: yes|no digest: <digest12>
tree-end:   <head-sha12> dirty: yes|no digest: <digest12>
tree-integrity: PASS
mode: PARTIAL (--only fmt,clippy) - does NOT count as the gate
census: <A>/<N> components AFFIRMED a count; <G> DECLARED-GAP (RECOGNISED); ... (as above)
fmt:               PASS (<Ns>)  [fmt workspace features=n/a]  {no census — cargo fmt --all --check emits no per-file tally to count}
clippy:            PASS (<Ns>)  [clippy workspace(excl 5) --all-features | ...]  {no census — cargo clippy emits a per-crate tally only COLD; a warm run prints Finished alone}
logs: /tmp/agent-gate.<random>
summary-file: <AGENT_GATE_SUMMARY_FILE or $PWD/.agent-gate-summary.txt>
RESULT: PARTIAL
==== END AGENT-GATE SUMMARY ====
```

### Every component line STATES WHAT IT VERIFIED, and a component that verified nothing cannot PASS (#3625)

`PASS (0s)` is indistinguishable, in a pasted block, from a component that did nothing. A
duration is a PROXY for work; a COUNT is the work. So every component line carries a census
suffix after its feature matrix, and the block carries one aggregate `census:` line.

The suffix renderings a reader will actually see:

| suffix | meaning |
|---|---|
| `{verified: 3562 tests passed (across 41 result line(s))}` | an affirmative count — the component's real subject |
| `{verified: 2 test binaries built/verified}` | a `--no-run` lane, whose honest subject is binaries, not tests |
| `{no census — <reason>}` | a DECLARED GAP: no count is derivable for this component yet, and the reason prints every run |
| `{no census: component ended FAIL, so there is no PASS to affirm}` | the component did not pass, so there is no PASS to census |
| `{census NOT-MEASURED: <reason>}` | the census could not be taken (an unreadable log, a suppressed cargo status). **Never read as verified**, and deliberately non-fatal |
| `{verified NOTHING: <reason>}` | the subject count was MEASURED and is zero — see `VACUOUS` below |

**`VACUOUS` is a fourth component status beside `PASS`/`FAIL`/`SKIP`.** A component whose PASS
carries a measured-zero census is recorded as `VACUOUS`, and **it fails the run** — `RESULT` is
still only `PASS`/`FAIL`/`PARTIAL`, because the aggregation treats `PASS` and `SKIP` as the only
non-failing component statuses and everything else, `VACUOUS` included, as a failure.

Two conventions in the aggregate line are load-bearing and are not tidied away: every
non-affirmed class prints as `N RECOGNISED` — never a bare `N` — and the line **declares its own
non-exhaustiveness**, because the gap set is curated. A component that affirms nothing is
UNMEASURED, which is not the same statement as verified.

**Why cargo's warm `0s` rows are legitimate, and how to tell:** cargo caches COMPILATION, never
test EXECUTION. A warm `cargo test` re-prints `test result: ok. N passed`, and a warm
`cargo test --no-run` still prints one `Executable ` line per binary. So a `0s` row whose suffix
carries a count really did re-verify its subject; the duration collapsed because the build was
cached. That is the distinction the census exists to make visible.

### Every component line NAMES the feature matrix it ran (#3453)

Owner ruling, 2026-08-30: *"the gate SUMMARY should name the feature matrix each
component ran so a pasted block states what it certified."* A bare
`core-tests: PASS (412s)` cannot distinguish a run that certified the OTLP stack from
one that never enabled it — which is the whole subject of #3453 (220 cqlite-core
`--lib` tests execute in `pr-gate-core`'s `--all-features` lane and nowhere in the gate
of record). So every component line, in **every** mode (full, `--lite`, `--delta`),
carries a bracketed feature matrix. Read it as `<subcommand> <scope> <features>`, one
entry per distinct invocation, `xN` when the same set ran N times.

**It is DERIVED, not curated** (the `#3453` block in `scripts/agent-gate.sh`, kept INLINE
because eight hermetic self-tests build a synthetic repo by copying that one file):

- `cargo` and `env` are shell **functions** in the gate, so every cargo invocation made
  in the gate's own shell is described **from the real argv about to execute**. There is
  no second copy of a feature list to drift from.
- The eight components whose cargo calls live inside a single-quoted `bash -c` body
  (`core-tests`' nextest branch, `memory-budget`, `integration-tests`, `write-tests`,
  `cli-tests`, `compaction-byte-parity`, `minimal-build`, `smoke`) hoist their
  package/features into **one variable** that is expanded both into the argv and into the
  recorded matrix. The cargo/env **interceptors** are deliberately **not** `export -f`-ed:
  exporting an interceptor would make every bash descendant record too, so
  `tooling-tests` (which runs nested agent-gate self-tests) would attribute a nested
  run's cargo invocations to itself — a false rationale in a gate log is worse than none.
  Each body instead calls the **explicitly named** recorder `_fm_observe_child` (which IS
  exported, and which intercepts nothing) with the gate's own `_fm_describe_cargo`, so
  there is no second formatter either.

**It records EXECUTION, not intent** (roborev job 269, blocker 2). Those eight records
used to be written by the *parent*, before the child body ran: `cli-tests: FAIL` then
named **both** of its feature sets even when Pass 1 — or the fail-closed target
derivation above it — died before Pass 2 ever started, and `write-tests` claimed the same
set `x3` after failing on the first of three `&&`-chained passes. A failure summary that
claims an invocation which never occurred is affirmatively false, and strictly worse than
silence: it is what stops the next person looking. Every record is now appended on the
line immediately **before** its own cargo command, from inside the body, so a
short-circuit records nothing later. A body that dies before its first cargo call
legitimately leaves an empty sidecar, and that state is named too.

**It never renders blank.** A component with no observed invocation renders an explicit
`[UNDECLARED]`; one that runs no cargo at all renders `[no-cargo]`; a `SKIP` that never
reached cargo — or a `FAIL` before its first cargo call — says exactly that (and names the
metadata-probe exclusion, because `cargo tree`/`metadata` probes are deliberately not
recorded and three components FAIL exactly there).
And **observation beats declaration**: a component declared `no-cargo` that is observed
running cargo shows the observed sets with a `!declared-no-cargo` marker, so a
mis-declaration self-corrects instead of being believed.

**THE CLASS DECIDES WHAT MAY BE CLAIMED — four classes, and three rules that came out of
one family of findings (roborev job 273).**

1. **A component whose cargo runs only in a CHILD PROCESS is never class `cargo`.** The
   interceptors are unexported by design, so `cargo` means *observable in the gate's own
   shell, or self-recorded from a `bash -c` body*. `tooling-tests` was declared `cargo`
   while its only cargo runs inside ~60 nested test scripts — so a PASS read
   `[UNDECLARED]` and, worse, a FAIL could claim it *"FAILed before its first cargo
   invocation"* after a child `cargo build` really ran. It is now the fourth class,
   `unobservable:<why>`, rendered `[cargo not observable: <why>]`: it asserts **nothing**
   in either direction and takes no SKIP/FAIL note, because "nothing ran" is precisely
   what that shell cannot know. An in-shell observation rides beside it additively.
2. **An `indirect:<driver>` component RECORDS whether its driver was REACHED**, from an
   explicit signal — a build-verify rc, or a recorder call on the line immediately before
   the driver runs — **never inferred from the terminal status**. `python-bindings` can die
   in venv/pip before maturin and `node-bindings` in `npm ci` before `npm run build`, and
   both used to report `[via maturin: …]` / `[via npm run build (napi): …]` for a cargo
   invocation that never happened. One shared helper pair does the mapping for all of them
   (`_fm_note_driver` + `_fm_note_maturin_rc`, plus the exported child-callable
   `_fm_observe_driver`), so a fourth indirect component cannot get the direction wrong by
   writing its own text. An indirect component with **no** record renders `UNDECLARED`
   **naming the driver** — a visible recording gap, never a claim.
3. **The misclassification is MECHANICALLY DETECTABLE**, which is the part that stops the
   next round: the census that missed rule 1 *read the table* instead of exercising it. The
   guard now RUNS every `cargo`-class component under `--only` with a recording shim
   `cargo` (29 today, ~16 s for the whole guard) and treats an `UNDECLARED` annotation as a
   FAIL — either the component's cargo runs in a child process, or a record is missing —
   while a component that cannot be exercised without recursion (`tooling-tests` runs the
   guard itself) must be declared non-`cargo`, also a FAIL.

**A driver we cannot see is NAMED, not guessed** (roborev job 269, blocker 1).
`python-bindings`, `node-bindings`, and the `--lite` `scoped-tests` **python tier** —
whose `maturin develop` runs in a child process — render
`[via <driver>: feature set NOT observed]` **once their driver is observed to have been
reached**, and the python tier's entry is **additive**:
a mixed rust+python diff reads
`[test cqlite-core --features cli-helpers | via maturin: feature set NOT observed]`,
never one at the expense of the other. It is recorded only for the build-verify exit
codes that mean maturin actually ran (a venv/pip failure or an absent cargo/rustc records
that the tier never reached maturin). This is deliberately **distinct from
`UNDECLARED`**: "nobody said" and "known to be indirect, therefore unobservable" are
different facts, and only one of them is a defect.

**Guard:** `scripts/tests/test_agent_gate_feature_matrix_annotation.sh` (in
`tooling-tests`, hermetic) asserts that every name in `COMPONENTS` resolves to a
declared class — a new component cannot join the set with a blank matrix — that all six
per-component emit sites render through the one `_fm_summary_line`, and, for the
`bash -c` components, that the **declared matrix equals the argv that actually
executed** under a recording PATH-shim `cargo`, described through the gate's own
`_fm_describe_cargo` rather than re-derived in the test. It then re-runs that same
differential under a **failing** shim, where each body must name exactly the one
invocation it reached — with the short-circuit itself proved by measurement (strictly
fewer invocations than the passing run), never assumed. And it drives the real
`run_scoped_tests` for a pure-python and a mixed route, and the real
`run_python_bindings` against a stubbed build-verify child for each rc. RED-verified, one
plant per finding: reintroducing a parent-side pre-record, dropping the python-tier record,
reverting `tooling-tests` to class `cargo`, declaring a cargo-less component `cargo`,
discarding `run_python_bindings`' rc, removing (or merely MOVING) node's in-body recorder,
and dropping the note from `run_scoped_tests`' record_result-bypassing exit each red a case
that NAMES the planted symbol. It also refuses to certify itself vacuously: an EXIT trap
fails any run that does not reach its terminal tally (measured — under real `/bin/bash`
3.2.57 the pre-fix guard ran 1 of 84 cases and exited **0**, so a bash-4-only construct
degraded it to a silent pass rather than a loud red), plus a verdict floor, plus the
associative-array lint over every gate-invoked script in
`test_agent_gate_summary.sh` section 8c.

**A long feature list is abbreviated, and the abbreviation declares itself**:
`33:all-compression,arrow,bench-internals,+30 more`, never a silent truncation. Beyond
six distinct sets the remainder renders as `+K more sets`.

## A mid-run tree mutation invalidates the run (#2926)

The gate captures a **tree identity** at start — HEAD, the dirty flag, and a
content-sensitive digest of every uncommitted tracked change plus every untracked,
non-ignored file — re-verifies it at every component boundary and once immediately
before the terminal emit, and **FAILs CLOSED** on any mismatch:

```
tree-integrity: FAIL (tree-mutated-midrun; head <a>→<b>; changed: <paths…> (+N more); detected-after-component: <c>)
RESULT: FAIL
```

Why it exists: `commit:`/`dirty:` used to be stamped by a fresh `git rev-parse` /
`git status` at *emit* time, so a worktree edited while the gate ran emitted a block
attributing **mixed-tree results to the final sha** — indistinguishable from a real
certification. This is reachable without breaking the one-worker rule (#1930): a lead
legitimately runs a closer (gating) and a fixer (editing) that overlap on one worktree.

Contract:

- **A closer MUST read `tree-integrity:` alongside `RESULT:`** before trusting a
  summary. `RESULT: PASS` with anything other than `tree-integrity: PASS` cannot occur;
  a block whose `tree-start:`/`tree-end:` digests differ is not a certification.
- **The `commit:` line is derived from the verified terminal capture**, never from a
  fresh git read at emit time. The only sha a block can name is one a validated capture
  observed — so a HEAD move landing between the capture and the emit can no longer be
  certified. When no validated capture exists, the line reads
  `commit: unverified … dirty: unverified` and the run is already FAIL-closed.
- The guard covers the full gate, `--lite`, `--delta` and `--only`. Only `--list`,
  `--python-build-verify`, the concurrency stub and the self-test emission modes are
  exempt (they stamp a synthetic `selftest` identity).
- The startup `INCOMPLETE` sentinel carries `tree-start:` (and no `tree-end:`), so even
  a killed gate records the tree it began on.
- **There is no bypass.** No environment variable turns a mutated run green. The one
  knob, `AGENT_GATE_TREE_HASH_CAP_BYTES` (default 8 MiB), only caps content hashing of
  oversized *untracked* files and is itself stamped as `tree-hash-cap:`.
- Exclusions are the repo's own `.gitignore` rules plus the run's own summary file. One
  named non-fatal class: a `Cargo.lock`-**only** difference stamps
  `tree-integrity: PASS (lockfile-settled: …)` (the gate runs cargo without `--locked`,
  #2962); a lockfile change alongside anything else is fatal.
- **Stated limitation**: gitignored *inputs* — chiefly the fetched
  `test-data/datasets/**` SSTable binaries — are outside the digest. Their stability is
  covered by the existing `datasets:` and `ci-pins:` stamps, not by this guard.
- Recovery: re-run on a stable tree. The FAIL names the changed paths; see
  `docs/development/gate-ops.md`.

## Parity CI tier contracts

The agent gate proves a change is *correct*; the **parity CI tiers** define what
each Cassandra-parity gate *promises*. The two are read together: see
`docs/development/parity-ci-tiers.md` for the per-tier contract (purpose, accepted
`evidence.type`, skip/failure policy, artifact retention, promotion rules) and the
gate-strength classification — **smoke** vs **canonical-semantic** vs
**byte-for-byte** — that bounds what a green gate can claim. Smoke alone cannot
satisfy a P0 data-loss scenario without a recorded gap. Before publishing a broad
public parity claim, run `docs/development/parity-release-checklist.md`. A
fast-PR cross-check (`cargo run -p cassandra-parity -- tier-contract-check`) keeps
the documented tier enum in sync with the code (`enums::CI_TIER`) and the manifest
schema (issue #1022).

## CI parity

The gate reads dataset pins from `.github/workflows/sstabledump-parity-gate.yml`
and includes them in the summary block as `ci-pins`. Local validation must target
the same asset CI uses. Current pins (as of the script source):

```
DATASET_TAG:    datasets-v3
DATASET_ASSET:  cassandra5-small-full-v3.4.tar.gz
DATASET_SHA256: 3cae644360e0142a6bb5e96ddab445ff18e3478e7058104842ce1a455fba8a33
```

See [Test data](/cqlite/agents-developing/test-data/) for how `fetch-datasets.sh` uses these pins and why
the SHA256 is the cache key.

## Feature-gated tests

`core-tests` skips one test (`test_legacy_format_allows_blob_fallback_with_feature`)
that requires a feature flag incompatible with `cli-helpers`. This skip is listed in
the script explicitly — it is not a silent omission.

The `minimal-build` component verifies the library compiles without the query engine
(`--no-default-features --features all-compression`). This catches feature-gate
regressions that `clippy --all-features` won't find.
