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
  `scripts/flow/premerge-assert.sh` remains the closer's last look.
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
| `pub-surface` | `bash scripts/ci/check-pub-surface.sh` — cqlite-core public-API snapshot guard (issue #1712): diffs the rustdoc-derived default-feature surface against `cqlite-core/pub-surface.snapshot` and asserts an unconditional crate-root `pub mod` is really in that surface. Granularity is item paths + associated-item names (methods, variants, fields, assoc consts), never signatures; trait/blanket impl members are excluded by design. Regenerate with `--regenerate`; SKIP-aware (loud) only on an absent cqlite-core |
| `tooling-tests` | `bash scripts/tests/test_agent_gate_summary.sh` (SUMMARY-capture regression, #1175; SKIP-aware on missing python3) |
| `minimal-build` | `cargo build -p cqlite-core --no-default-features --features all-compression` |
| `smoke` | `bash test-data/scripts/smoke-test-all-tables.sh` (against a freshly built debug binary) |

All components run even after a failure so one run reports everything.

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

The gate is tiered. `scripts/agent-gate.sh --lite` runs only the fast subset
(file-size + fmt + scoped workspace clippy + blast-radius-scoped tests, ~1–5 min).
It is the **fast iteration loop, NOT the gate of record** — it emits a DISTINCT
`==== AGENT-GATE LITE SUMMARY ====` block (`MODE: lite`) that must **never** be
pasted as the full SUMMARY. Iterate on `--lite` every fix round; run the FULL
`scripts/agent-gate.sh` **exactly once** before merge. `--lite` never replaces the
full gate.

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
accelerators: sccache=on nextest=on lanes=on sccache-health=ok mold=linked perf=ok
```

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

## Machine-checkable summary block

The gate emits a block between `==== AGENT-GATE SUMMARY ====` markers. The last
line is always `RESULT: PASS` or `RESULT: FAIL`. Paste this block verbatim in your
PR report — prose summaries are not accepted.

**Format (exact, as emitted by `scripts/agent-gate.sh`):**

```
==== AGENT-GATE SUMMARY ====
commit: <short-sha> branch: <branch> dirty: yes|no
datasets: <N> Data.db files under <CQLITE_DATASETS_ROOT>
schemas: <N>/<N> canonical .cql readable under <root> (checkout-relative|CQLITE_SCHEMAS_ROOT override)
ci-pins: DATASET_TAG: <tag>  DATASET_ASSET: <asset>  DATASET_SHA256: <sha>  
tree-start: <head-sha12> dirty: yes|no digest: <digest12>
tree-end:   <head-sha12> dirty: yes|no digest: <digest12>
tree-integrity: PASS
fmt:               PASS|FAIL (<Ns>)
clippy:            PASS|FAIL (<Ns>)
core-tests:        PASS|FAIL (<Ns>)
integration-tests: PASS|FAIL (<Ns>)
write-tests:       PASS|FAIL (<Ns>)
cli-tests:         PASS|FAIL (<Ns>)
minimal-build:     PASS|FAIL (<Ns>)
smoke:             PASS|FAIL (<Ns>)
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
fmt:               PASS (<Ns>)
clippy:            PASS (<Ns>)
logs: /tmp/agent-gate.<random>
summary-file: <AGENT_GATE_SUMMARY_FILE or $PWD/.agent-gate-summary.txt>
RESULT: PARTIAL
==== END AGENT-GATE SUMMARY ====
```

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
