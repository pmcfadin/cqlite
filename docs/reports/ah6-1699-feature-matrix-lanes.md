# AH6 — feature-matrix gate lanes: observed to fire (issue #1699)

Issue #1699 added four gate components — `flight-tests`, `legacy-heuristics`,
`feature-iso-parquet`, `feature-iso-delta-scan`. This report is the issue's **AC2**
deliverable: affirmative evidence that each lane **fires on a planted break** and
**stays silent on a clean tree**.

## Why a green lane is not evidence

Presence in `scripts/agent-gate.sh --list` proves a lane is *registered*. A green
SUMMARY line proves it *ran and found nothing*. Neither proves it **can fail**.
`feature-iso-parquet` reports `PASS (0s)` on a warm tree, and from the SUMMARY block
alone that is indistinguishable from a lane that compiles nothing and always exits 0.
Design decision **D5** therefore requires each lane to be *observed* in both
directions, and the delta spec states it as a binding requirement: *"Every new lane is
observed to fire on a planted break and not to fire on a clean tree."*

A planted-break harness that only ever plants breaks is the vacuous-guard shape of
#3229 — it passes just as happily against a lane that fails unconditionally. So the
harness asserts **both** directions per lane, and reports a lane that is red in both
as a **HARNESS FAILURE**, never as a successful observation.

## The harness

`scripts/tests/test_agent_gate_feature_matrix_lanes.sh` — committed, re-runnable,
and **opt-in**: it is deliberately absent from `COMPONENTS`, `LITE_COMPONENTS` and
`DELTA_COMPONENTS`, because it performs real compiles and taxing every full gate to
re-prove a static property is disproportionate (D5). Nightly `gate.yml` enrollment is
out of scope — a workflow change needs #2910 registry enrollment.

```bash
export CQLITE_DATASETS_ROOT=/data/datasets   # the absolute root fetch-datasets.sh prints
bash scripts/tests/test_agent_gate_feature_matrix_lanes.sh              # all four lanes
bash scripts/tests/test_agent_gate_feature_matrix_lanes.sh flight-tests # one lane (exits 3: PARTIAL)
```

Properties that make the observation mean something:

- **It runs the real component**, `bash scripts/agent-gate.sh --only <lane>`, never a
  retyped cargo command. A retyped command would prove that a cargo invocation works;
  the subject here is the gate component.
- **`--only` exit codes are load-bearing and are not the usual 0/1.** A PARTIAL run
  that found nothing exits **3** (the gate refuses to let a partial run be scripted
  into a green claim); a PARTIAL run with a failed component exits **1**. The harness
  additionally parses the component's own SUMMARY line and requires exit code and
  status to agree, so a gate that mis-reported one of them could not be mistaken for
  an observation.
- **All mutation happens in a throwaway `git worktree add --detach` copy.** #2926
  makes a mid-run tree mutation a gate FAIL, so a harness that edited the tree its own
  gate was running in would be the very defect it exists to catch. Plants are applied
  and reverted **between** runs, never during one; the copy is removed on an `EXIT`
  trap including on failure.
- The copy gets its **own `CARGO_TARGET_DIR`** (outside the copy, so the revert cannot
  sweep it), so a lane's clean and planted runs share compilation.
- Reverts are uniform (`git checkout -- . && git clean -fd`) and **verified** —
  a tree that will not revert is reported as a harness error rather than silently
  contaminating the next lane.

## Observed results

Run at `94833d510` on the worker box, `CQLITE_DATASETS_ROOT=/data/datasets`.
Harness elapsed: **581 s** (8 runs — one clean and one planted per lane). All four
lanes fired.

| lane | planted break | clean tree | planted tree | attributed to |
|------|---------------|-----------|--------------|---------------|
| `feature-iso-parquet` | a `#[cfg(feature = "parquet")]` fn at the root of `cqlite-core/src/lib.rs` calling a `#[cfg(feature = "delta-scan")]` fn — compiles with both features on (clippy's ~30-feature cqlite-core arm), unresolved with parquet alone. #1978's class. | **PASS** (exit 3, 112 s) | **FAIL** (exit 1, 20 s) | `ah6_planted_delta_scan_marker` |
| `feature-iso-delta-scan` | the mirror: a `#[cfg(feature = "delta-scan")]` fn calling a `#[cfg(feature = "parquet")]` fn. | **PASS** (exit 3, 46 s) | **FAIL** (exit 1, 16 s) | `ah6_planted_parquet_marker` |
| `legacy-heuristics` | a **new** `cqlite-core/tests/ah6_planted_legacy.rs` holding a `#[cfg(feature = "legacy-heuristics")] #[test]` with an inverted assertion. | **PASS** (exit 3, 176 s) | **FAIL** (exit 1, 5 s) | `ah6_planted_legacy_heuristics_break` |
| `flight-tests` | a **new** `cqlite-flight/tests/ah6_planted_flight.rs` with a failing `#[test]`. | **PASS** (exit 3, 178 s) | **FAIL** (exit 1, 27 s) | `ah6_planted_flight_break` |

Two of the plants do extra duty beyond "the lane can fail":

- The `legacy-heuristics` plant is a **new file**, so the lane's red also proves its
  `--test` target set is genuinely **derived** from the committed source (it picked up
  a sixth gated file with no gate edit; a hard-coded list would have ignored it and
  stayed green) and that the lane **executes** rather than merely compiles — a
  compile-only lane stays green on a failing assertion, which is exactly D3's premise.
- The `flight-tests` plant is a target the gate names **nowhere**, so its red proves
  the lane reaches past the three cqlite-flight targets already covered
  (`query_semantics_flight_parity`, `issue_3095_flight_static_columns`, and
  `memory-budget`'s dhat target).

**Attribution.** A bare red is not evidence: a lane that broke for an unrelated reason
produces the same exit code and the same SUMMARY line. The harness therefore requires
each planted run's output to **name the planted symbol** (the right-hand column above);
a red that does not is reported as `FIRED-UNATTRIBUTED` and fails the harness.

**Exit codes.** `--only` on a component that found nothing exits **3** (`PARTIAL` — the
gate refuses to let a partial run be scripted into a green claim); with a failed
component it exits **1**. The harness checks the exit code and the SUMMARY status line
and requires them to agree.

The durations in the table are the harness's own `--only` runs from a fresh throwaway
worktree against a shared, partly-warm `CARGO_TARGET_DIR`. They are neither the cold
figures nor the gate's warm figures below; they are recorded for reproducibility, not
as the lanes' cost.


## `flight-tests`: a whole-package invocation was NON-DETERMINISTIC (#3383)

The lane shipped as `cargo test --no-fail-fast -p cqlite-flight` — the whole package.
That form turned out to red **2 out of 3 runs**, for a reason that has nothing to do
with any defect the lane exists to find.

`cqlite-flight`'s `fast_arm_stream_stops_when_the_client_drops_it` (in
`issue_3058_bypass_path_taken`) asserts a **race outcome**: the client's stream drop
must beat the producer. Run alone it wins reliably; run alongside 39 other integration
binaries competing for the same 16 cores it does not.

| how the target was run | host load | result |
|---|---|---|
| alone, `cargo test -p cqlite-flight --test issue_3058_bypass_path_taken` (×3) | 74 | **3 / 3 PASS** |
| inside the whole-package `flight-tests` lane (×3) | — | **2 of 3 runs FAILED** |

**A merge-gate lane that reds 2-in-3 carries no information**, and it is worse than
uninformative: it teaches every worker that a red from this lane means "re-run it",
which is precisely the habit that lets a real red through. Since `cargo test -p` cannot
exclude one target, the invocation became an explicit list:

```
cargo test --no-fail-fast -p cqlite-flight --lib --bins --test <T1> --test <T2> …
```

The list is **derived from `cargo metadata` at run time**, so the #2039 "a hand-picked
list is a second registry that drifts silently" lesson still holds — adding
`cqlite-flight/tests/foo.rs` puts `--test foo` on the command line with no gate edit,
and the harness above still proves it by planting a brand-new target the gate names
nowhere. Measured after the change, on the same box:

| lane invocation | declared | run | skipped (`required-features`) | skipped (flaky) | secs |
|---|---|---|---|---|---|
| whole package `-p` | 42 | 41 | 1 (silently) | — | 128 (cold) / 27 (warm) |
| derived `--test` list | 42 | **40** | 1 (named) | 1 (named, `#3383`) | 60 |

Two properties are worth separating, because only one of them is curated:

- the `required-features` subtraction is **derived** — `cargo test --test X` is a *hard
  error* on a target whose features are unmet, where `-p` skipped it silently;
- the flake subtraction is **curated, and labelled as such in code**. Flakiness is not
  mechanically decidable from source: nothing in a file says "this assertion races". So
  instead of a derivation that pretends to measure it, both halves of every entry are
  enforced — a numeric issue number (so the list cannot grow without a filed issue
  obliging its removal) and a target the package actually declares (so a rename or
  deletion reds rather than quietly excusing nothing).

`--bins` is explicit and load-bearing. An explicit selector suppresses every target kind
not named, so omitting it would have silently stopped executing `main.rs`'s 2 unit tests
— the change would itself have opened the never-executed hole this lane exists to close.

## `flight-tests`: the descope to `--lib` (#3384) — the non-determinism was the SUITE, not one test

The amendment above (an explicit derived list minus one flaky victim) was **withdrawn on
further measurement**. Four consecutive whole-package runs of the lane were recorded, and
the pattern is not "one racing assertion":

| run | lane result | victim |
|---|---|---|
| 1 | **PASS** | — |
| 2 | **FAIL** | `issue_3058_bypass_path_taken::fast_arm_stream_stops_when_the_client_drops_it` |
| 3 | **PASS** | — |
| 4 | **FAIL** | `issue_2370_gauge_readback_test` |

**~50% non-deterministic, with TWO DISTINCT VICTIMS in four runs.** Four hypotheses were
ruled out by measurement rather than by argument:

| hypothesis | how it was tested | verdict |
|---|---|---|
| whole-box load (other lanes' gates) | the victim target alone at host load 74, ×3 | **RULED OUT** — 3/3 PASS |
| the gate's `nice`/`taskpolicy` wrapper | lane re-run without it, ×2 | **RULED OUT** — 2/2 PASS |
| intra-package test parallelism, fixable by throttling | lane at `--test-threads=2`, ×2 | **RULED OUT** as a *discriminator* — 2/2 PASS, i.e. it did not distinguish |
| concurrent MAIN-lane compilation | failures reproduced under `--only flight-tests`, where MAIN runs nothing | **RULED OUT** |

**Quarantining victims one at a time was rejected** (owner ruling). Two distinct victims in
four runs is not a converging series — nothing suggests a widened lane would not find a
third — so the list would grow once per red with no visible end, turning the quarantine into
the dumping ground its own design rule forbids. A curated excusal list is legitimate only
while it is small, closed, and every entry is on a path out; none of those held. The general
suite-hygiene defect is filed as **#3384**, with **#3383** as its first individual victim.

**The lane therefore runs `cargo test --no-fail-fast -p cqlite-flight --lib --bins`** — 387
`--lib` unit tests plus `main.rs`'s 2, observed deterministic in every run of this session,
and re-verified **PASS 3/3** through the real component after the change (6s / 7s / 8s warm).

**The deliverable of the descope is the DECLARATION, not the narrowing.** This whole issue
exists because a lane that omits coverage looks identical to a lane that covers it, so a
narrowed lane that stayed quiet would reintroduce the defect one level down. On every run,
pass or fail, the lane prints a coverage census to **both** the gate's stdout (as `>>>`
lines) and its component log. Verbatim from `flight-tests.log`:

```
==== [flight-tests] COVERAGE CENSUS (issue #1699 / #3384) ====
cargo test --no-fail-fast -p cqlite-flight --lib --bins (UNIT tests only, #1699/#3384)
COVERAGE CENSUS — WHAT THIS LANE DOES NOT RUN:
  cqlite-flight declares 42 integration (test) targets. THIS LANE EXECUTES NONE OF THEM.
  (1 of the 42 could not run here in any case: unmet required-features.)
  WHY: the integration half of this package is ~50% NON-DETERMINISTIC under
       intra-package parallelism — 4 whole-package runs went PASS/FAIL/PASS/FAIL
       with 2 different victims (issue_3058_bypass_path_taken,
       issue_2370_gauge_readback_test). Ruled out by measurement: box load,
       nice, --test-threads=2, concurrent MAIN-lane compilation.
       Issues: #3384 (the general suite-hygiene defect), #3383 (first victim).
  WHO DOES RUN THEM: CI's Flight tier — .github/workflows/flight-ci.yml line 229,
       'cargo test --package cqlite-flight', mandated on cqlite-flight/** AND
       cqlite-core/**, with the 'required' check failing closed on it (#2910).
       Locally, flight-query-semantics-oracle runs 2 of these targets and
       memory-budget runs 1 (--test issue_1494_producer_mem_budget).
  This omission is DECLARED, not silent: widening the lane back is a small
       change once #3384 is fixed (the derivation machinery is retained).
declared targets with unmet required-features: issue_1494_producer_mem_budget(required-features[dhat-heap]:off[dhat-heap])
enabled features (cargo metadata): default test-util
==== end census ====
```

The `42` is **counted from `cargo metadata` at run time**, never hard-coded, so the stated
gap cannot drift into a false claim; a failed count is a FAIL naming the derivation, because
an understated gap is exactly the silent omission this issue exists to eliminate.

**Consequences in code, both of them about not leaving a vacuous guard behind:**

- The flake-quarantine plumbing (`FLIGHT_FLAKE_SKIPS` + its validator) is **retired, not
  kept inert**. It existed only to paper over #3384; with no lane executing those targets it
  has no subject, and an empty curated list plus a caller-less validator is a guard
  reporting OK having measured nothing.
- `check_no_unexpected_zero_tests` keys on `Running tests/<name>.rs` and **explicitly
  disclaims `--lib`**, so calling it on a `--lib --bins` selection would be that same
  empty-subject guard. Its `--lib` analogue `check_unittest_targets_ran` is called instead:
  each selected unittest target must be OBSERVED *and* must have run a NON-ZERO count, and
  the pass prints them — `src/lib.rs(387 tests) src/main.rs(2 tests) executed`.
- `_package_test_targets` stays **called** (it feeds the census);
  `check_declared_test_targets_observed` is retained **uncalled**, saying so at the top,
  because it is what the widened lane calls again once #3384 is fixed.
- The observation harness's `flight-tests` plant moved from a new
  `cqlite-flight/tests/*.rs` integration target to a new `cqlite-flight/src/` unit-test
  module wired into `src/lib.rs`. The old plant could no longer fire, and **a plant that
  cannot fire turns the harness into the vacuous green it exists to prevent**.

**Cost of the descope, stated plainly.** Local pre-push execution of ~38 `cqlite-flight`
integration targets is lost. Three still run locally in other components
(`flight-query-semantics-oracle` ×2, `memory-budget` ×1); all of them still run on CI's
Flight tier before merge, mandated on `cqlite-flight/**` and `cqlite-core/**`, and
`required` cannot go green without it (#2910).

## Cost

Two different numbers, and the second **cannot be derived from the first**.

### Per-component durations

| lane | measurement | secs | note |
|------|-------------|------|------|
| `feature-iso-parquet` | `cargo check --no-default-features --features all-compression,parquet` | 18 | **cold**; lib-only `cargo check` — the *superseded* shape (D2 replaced it with `cargo test --lib --no-run`) |
| `feature-iso-delta-scan` | `cargo check --no-default-features --features all-compression,delta-scan` | 10 | **cold**; same superseded shape |
| `legacy-heuristics` (build half) | `cargo build -p cqlite-core --features legacy-heuristics` | 26 | **cold** |
| `flight-tests` | `cargo test -p cqlite-flight` (whole package) | 128 | **cold**; the *superseded* shape (#3383 replaced it with a derived `--test` list) |
| `legacy-heuristics` (component) | first green run of the component as shipped | 37 | first green run |
| `flight-tests` (component) | SUMMARY line | 27 | **warm cache**; superseded whole-package shape |
| `flight-tests` (component, derived `--test` list) | SUMMARY line | 60 | **warm cache**, 40 of 42 targets — the shipped shape (#3383) |
| `legacy-heuristics` (component) | SUMMARY line | 7 | **warm cache** |
| `feature-iso-parquet` (component) | SUMMARY line | 0 | **warm cache** — *not* the lane's cost |
| `feature-iso-delta-scan` (component) | SUMMARY line | 1 | **warm cache** — *not* the lane's cost |

The warm numbers are labelled as warm on purpose. A warm `0s` is the cost of cargo
deciding there is nothing to rebuild; it is not what the lane costs on a tree that
actually changed. The cold `cargo check` figures for the two isolation lanes measure
the **superseded** instrument (lib-only `cargo check`); the shipped lanes run
`cargo test --lib --no-run`, which additionally compiles the lib's `#[cfg(test)]`
modules — the #1978 incident class a bare `cargo check` is blind to.

### Added full-gate wall time

**METHOD CHANGED, AND WHY — a baseline-vs-after subtraction is not measurable on this
fleet, so the question is answered a better way.**

The plan was a baseline full gate at `origin/main` versus the gate of record on this
branch, run sequentially. It was attempted and **abandoned mid-run, deliberately**: this
worker box hosts five lane worktrees, and while the baseline ran the 16-core box sustained
a load average of **52–86** from co-scheduled gates in lanes 1697/1701/1705. A four-component
delta cannot be recovered from two totals taken under load that varies by more than the
delta itself, and the "after" run would sit under different load again. Publishing the
subtraction would have been a number with no measurement behind it. (The same load is the
prime suspect in #3380, an intermittent guard failure observed during this work.)

**The load-independent answer, which is also the one that matters: is the SIDE lane the
critical path?** All four lanes are dispatched to the concurrent SIDE lane by
`_component_lane`, each in its own `CARGO_TARGET_DIR`, because each builds cqlite-core at a
feature set diverging from MAIN's and would otherwise thrash MAIN's shared target dir
(#2657). Concurrent work adds wall time **only insofar as it outlasts MAIN**. So the added
wall time is read off a SINGLE run by comparing the SIDE lane's total against MAIN's:

- if MAIN still finishes last, the four lanes cost **zero** added wall time — they hid
  entirely inside MAIN's long pole;
- if SIDE now finishes last, the added wall time is `SIDE_total - MAIN_total`, and only
  that excess.

This needs no baseline, is immune to whole-box load (both lanes are inside the same run,
under the same load), and is computed from the gate of record's own per-component
durations. The figure is reported in the PR from that SUMMARY block.

**These are different numbers, and summing the per-component durations does not yield
the second.** The lanes run in the concurrent **SIDE** lane, each in its own
`CARGO_TARGET_DIR` — `_component_lane` (`scripts/agent-gate.sh:2217`) dispatches all
four there, because each builds cqlite-core at a feature set that diverges from MAIN's
and would otherwise thrash MAIN's shared target dir (#2657). Concurrent work adds wall
time only to the extent it outlasts MAIN, so the naive sum of component seconds
overstates the added wall time, possibly to ≈0. Both numbers are reported; neither is
presented as the other.

## References

- Design: `openspec/changes/feature-matrix-gate-lanes/design.md` (D2, D3, D4 + its #3383 amendment and #3384 second correction, D5, D6)
- Integration-suite non-determinism (the descope's subject): #3384; first individual victim: #3383
- Harness: `scripts/tests/test_agent_gate_feature_matrix_lanes.sh`
- Registration pin: `scripts/tests/test_agent_gate_summary.sh` (runs in `--lite` via `tooling-tests`)

## Does this change make #3380 more likely? (disclosure)

#3380 is an intermittent failure of `test_roborev_review_guard.sh`'s #3312 structural assert, reproduced on
clean `origin/main` and correlated with box load. Since this change adds four SIDE-lane components, the
question is fair and is answered from the gate's own concurrency model rather than guessed:

**Peak concurrency: UNCHANGED.** `AGENT_GATE_JOBS` defaults to `min(4, ncpu/2)`; MAIN takes one slot and the
SIDE lane runs at most `AGENT_GATE_JOBS - 1` of its members **at once**. So SIDE peak stays 3 heavy processes
whether SIDE has 7 members or 11. This change adds no simultaneous load.

**Exposure WINDOW: modestly longer.** The four lanes add total SIDE work, so the SIDE lane runs longer, and
`tooling-tests` / `roborev-lints` — both deliberately pinned to the strictly-serial MAIN lane *because* their
embedded shell self-tests "starved under co-scheduled SIDE-lane load" (#2657) — now have a longer interval in
which they can overlap SIDE work.

**So: this change does not create #3380 and does not raise its peak trigger condition, but it plausibly
widens the window in which the trigger can occur.** "#1699 didn't cause it" is true and is not the whole
answer; the whole answer is the one worth recording.

Not mitigated here, deliberately. The available mitigations are moving the lanes to the MAIN lane — which
reintroduces exactly the shared-target thrash (#2657) the SIDE placement exists to avoid — or lowering
`AGENT_GATE_JOBS`, a fleet-wide performance decision. Both are worse than the flake and both are the owner's
call. The right fix is #3380 itself, whose assert appears to be a false positive on heredoc prose
independently of load.

## The round-3 `-D warnings` fix was itself INERT until round 5 (recorded, not quietly repaired)

Round 3 correctly identified that `RUSTFLAGS="-D warnings" cargo build && cargo test` guarded only the
build. The fix put `env RUSTFLAGS="-D warnings"` on both invocations — and **that form is silently
ignored whenever `CARGO_ENCODED_RUSTFLAGS` is set in the environment**, because cargo reads the encoded
variable first and disregards `RUSTFLAGS` entirely when it is present. Round 5 found it (Medium).

So for two rounds this lane advertised a warnings-as-errors guard that, in such an environment, enforced
nothing — while its SUMMARY line stayed green and the report above recorded a 255 s cost increase as the
"price of the lane actually enforcing what it claims". The cost was real; the enforcement was
conditional on an environment variable nobody had checked.

**Measured in both directions**, because the whole point of this issue is that a guard's own claim is not
evidence. A crate containing an unused variable, with `CARGO_ENCODED_RUSTFLAGS` set to the **empty
string**:

| form | result |
|---|---|
| `env RUSTFLAGS="-D warnings" cargo build` (the round-3 fix) | **rc=0** — a warning, nothing more |
| `_deny_warnings cargo build` (the round-5 fix) | **rc=101** — hard error |
| `_deny_warnings` with a non-empty operator value (`--cfg=operator_flag`) | **rc=101**, operator flag preserved |

An **empty-but-set** value is enough to suppress it, which is the quietest available route to a vacuous
guard, so the plain branch **unsets** the encoded variable rather than assuming it is absent. Where the
operator did set flags they are **appended to** (cargo's `\x1f` element separator) rather than replaced —
discarding them would trade one silent behaviour change for another — and the append is announced on
stderr, which every caller redirects into its component log.

This is worth recording beyond the fix itself: **the lane's own doctrine caught the lane**. Two
consecutive review rounds accepted `RUSTFLAGS=` as self-evidently sufficient because it *reads* as the
CI-equivalent invocation. What surfaced it was asking the question this issue is built on — not "is the
flag set?" but "is it in effect?" — which is the same distinction as "which lane compiles this feature?"
versus "which lane executes it?".

## The co-required-feature census miscounted its own gap (round 5, Low)

The round-4 census grepped a single-line, fixed-**order** cfg pattern and counted matching
**attributes** as "test bodies". Both error directions were live:

- **over-report** — the gated `use cqlite_core::Value` import in `database_interface_tests.rs` was
  counted as an omitted test body, so the lane announced **three** where **two** exist;
- **under-report** (the permissive direction) — a reordered or multi-line cfg matched nothing at all.

For a change whose entire deliverable is an accurate declaration of what a lane does not run, a census
that miscounts its own gap is not a cosmetic defect. The replacement parses the attribute cluster:
multi-line attributes accumulate until their parens balance, `feature = "…"` tokens are read in any
order, the attached item is classified from its own first code line, and test-ness comes from an
attribute **path ending in `test`** — a path test rather than a substring search, so a feature named
`test-util` cannot pose as `#[test]`. Non-test gated items are reported separately and never folded into
the body count: support code compiling out alongside its callers is not omitted coverage.

A cfg carrying `not(feature = "X")` is deliberately **not classified**. It means the body compiles when X
is **off** — the opposite of a gap — and guessing either way would be a silent miscount, so such sites
are counted and **reported as unclassified**. That is the same principle as the `flight-tests` census:
the omission a reader cannot see is the one that does damage.

## Cost change from the roborev round-3 `-D warnings` fix (recorded, not absorbed)

Round 3 found that `RUSTFLAGS="-D warnings" cargo build && cargo test` applied the flag to the **build only**,
leaving the `cargo test` recompile of `cfg(test)` code unguarded — the exact #1981 dead-code shape the lane
exists to catch. Fixing it (`env RUSTFLAGS=` on both invocations) means the test compile no longer shares the
build's artifacts, so the lane pays a full recompile at this feature set:

| `legacy-heuristics` component | measured |
|---|---|
| before the fix (test compile unguarded) | **37 s** |
| after the fix (both halves under `-D warnings`) | **292 s** |

That is a ~255 s increase on a cold cache for this lane, and it is the price of the lane actually enforcing
what it claims. It is recorded here rather than left for someone to discover as an unexplained slowdown. The
lane remains in the concurrent SIDE lane, so per the wall-time method above the added *gate* wall time is
still `max(0, SIDE_total - MAIN_total)` — this increase only matters if it makes SIDE the critical path, which
the gate of record's own component durations will show.
