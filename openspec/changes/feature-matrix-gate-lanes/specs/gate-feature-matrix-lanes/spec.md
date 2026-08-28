# gate-feature-matrix-lanes — delta for feature-matrix-gate-lanes (issue #1699)

**Architecture note (read this first).** `scripts/agent-gate.sh` is the gate of record; its
`==== AGENT-GATE SUMMARY ====` block is the verdict. At `2bde26a7c` that verdict covers `cqlite-flight` by
**compiling** it (clippy `--all-targets`) and **running three** of its ~44 test targets by name; it covers
`legacy-heuristics`' 95 cfg sites by **compiling** them inside a ~30-feature clippy union and executing
**none** of the positively-gated bodies; and it covers `parquet` / `delta-scan` only **together**, which is
the shape that masks cross-feature coupling. This delta adds four lanes that close those three gaps, and
holds each to the #3272 standard: **a lane is credited only when it has been OBSERVED to fire.**

**Acceptance-criterion → requirement map** (issue #1699):

| AC / Fix item | Requirement(s) |
|---|---|
| Fix 1a — `cargo test -p cqlite-flight` lane | ADDED *The full gate executes the `cqlite-flight` test suite locally* |
| Fix 1b — `legacy-heuristics` build + gated tests | ADDED *The full gate builds AND executes `legacy-heuristics` at its own feature set* |
| Fix 1c — isolated `parquet` / `delta-scan` builds | ADDED *The full gate compiles `parquet` and `delta-scan` in mutual isolation* |
| Fix 2 — lane cost proportionate | ADDED *The new lanes' cost is measured, reported, and kept off the fast loop* |
| Fix 3 / AC2 — planted-break regression simulation | ADDED *Every new lane is observed to fire on a planted break and not to fire on a clean tree* |
| AC1 — components in `--list` and the SUMMARY | ADDED *Each new lane is declared once per registry and is visible in `--list` and the SUMMARY* |
| AC3 — added wall-time posted | ADDED *The new lanes' cost is measured, reported, and kept off the fast loop* |
| AC4 — full gate PASS with the new components shown | ADDED *Each new lane is declared once per registry and is visible in `--list` and the SUMMARY* |
| — (doctrine obligation, CLAUDE.md) | ADDED *Doctrine states what the full gate now certifies* |

## ADDED Requirements

### Requirement: The full gate executes the `cqlite-flight` UNIT test suite locally, and DECLARES the integration targets it does not run

The full `scripts/agent-gate.sh` SHALL include a component that **executes** `cqlite-flight`'s unit tests,
not merely compiles them: it SHALL run `cargo test --no-fail-fast -p cqlite-flight --lib --bins`. A failing
assertion in a `cqlite-flight` unit test that no existing component names SHALL make the full gate FAIL.
`--bins` SHALL be named explicitly, since an explicit target selector suppresses every kind not named and
its omission would silently stop executing `main.rs`'s unit tests.

**DESCOPED FROM INTEGRATION TARGETS, ON MEASUREMENT (issue #3384).** Two earlier cuts of this requirement
promised execution reach across `cqlite-flight`'s integration (`test`) targets — first via
`cargo test -p cqlite-flight`, then via an explicit `--test` list derived from `cargo metadata` minus a
curated flake quarantine (#3383). Both are withdrawn. The integration half of this package is **~50%
non-deterministic** under intra-package parallelism: four consecutive whole-package runs went
**PASS / FAIL / PASS / FAIL** with **two different victims**
(`issue_3058_bypass_path_taken::fast_arm_stream_stops_when_the_client_drops_it` and
`issue_2370_gauge_readback_test`). Four hypotheses were ruled out by measurement rather than by argument:
whole-box load (3/3 PASS standalone at load 74), `nice` (2/2), `--test-threads=2` (2/2), and concurrent
MAIN-lane compilation (the failures reproduced under `--only`, where MAIN runs nothing). A merge-gate lane
that reds ~1-in-2 carries no information — it trains agents to re-run and to waive, which is worse than not
having the lane. Per-victim quarantining was **considered and rejected** (owner ruling): two victims in four
runs is not a converging series, so the quarantine has no visible end and would become the dumping ground
its own design rule forbids. The general suite-hygiene defect is **#3384**; **#3383** is its first
individual victim.

Consequently this requirement SHALL NOT claim any execution reach across integration targets, and the
curated flake-exclusion list SHALL be **retired**, not kept inert: with no lane executing those targets it
has no subject, and an empty curated list plus a validator with no caller is a guard reporting OK having
measured nothing.

**THE OMISSION SHALL BE DECLARED, ON EVERY RUN.** This is the load-bearing half. A lane that silently omits
coverage is indistinguishable from a lane that covers it, so a narrowed lane that stayed quiet about the
narrowing would reintroduce this change's own defect one level down. The component SHALL therefore print a
**coverage census**, to BOTH the gate's stdout (as `>>>` lines) and the component log, on every run —
never only in a source comment, which is not read on a run — stating:

1. how many integration (`test`) targets `cqlite-flight` declares, **counted from `cargo metadata` at run
   time** and never hard-coded, so the stated gap cannot drift into a false claim;
2. that this lane **executes none of them**, in those terms;
3. which lane does: CI's Flight tier (`.github/workflows/flight-ci.yml` line 229,
   `cargo test --package cqlite-flight`), mandated on `cqlite-flight/**` **and** `cqlite-core/**`, with the
   `required` check failing closed on it per #2910 — and, locally, that
   `flight-query-semantics-oracle` runs two of those targets and `memory-budget` one; and
4. the issues that own the gap: **#3384** (general) and **#3383** (first victim).

A failed derivation of either the enabled feature set or the declared-target count SHALL be a FAIL that
**names the derivation** — never a pass, and never a census claiming a gap of unknown or zero size.

The component SHALL run under a zero-tests guard **that has a subject at this scope**. The gate's existing
`check_no_unexpected_zero_tests` keys on cargo's `Running tests/<name>.rs` lines and explicitly disclaims
`--lib` (`Running unittests src/lib.rs`), so calling it on a `--lib --bins` selection would be a guard with
an empty subject set reporting OK. Its `--lib` analogue SHALL be used instead: each selected unittest target
SHALL be **observed** and SHALL have executed a **non-zero** test count, and the passing verdict SHALL print
those counts as an affirmative measurement.

**That guard's SUBJECT SET SHALL itself be derived from `cargo metadata`, never hard-coded** (roborev
round-7 finding). `--bins` selects EVERY binary target, so a hard-coded list of unittest paths beside a
wildcard selector is a second registry that drifts silently: adding a binary would let it execute zero
tests while the guard still reported OK — the exact vacuous pass the guard exists to prevent. A failed
derivation SHALL record FAIL, never a partial subject list, which would shrink the guard the same way.

The derivation machinery (`_package_test_targets`, `check_declared_test_targets_observed`) SHALL be
**retained**, so that widening the lane back once #3384 is fixed is a small change; the retained-but-uncalled
reconciliation SHALL say in code that it is retained and what will call it again.

The component SHALL run in the SIDE lane with its **own** `CARGO_TARGET_DIR`, because `cqlite-flight` is a
separate crate built against a divergent `cqlite-core` feature set and sharing MAIN's target dir would
thrash it. It SHALL remain declared in `DATASET_COMPONENTS`: its `--lib` suite includes a real-fixture test
(`stats.rs`) that SKIPS with a printed notice when `CQLITE_DATASETS_ROOT` is unset, which is precisely the
silent-skip shape that set guards.

There SHALL be **no environment variable that disables this component**. `cqlite-flight` is a committed
workspace member and is never legitimately absent; fixture-dependent sub-targets may SKIP only through the
gate's existing dataset machinery, which reports the skip.

The pre-existing `flight-query-semantics-oracle` component SHALL be left functionally unchanged, including
its per-lane fixture SKIP predicates.

#### Scenario: A broken Flight unit test outside the named oracle targets fails the gate
- **GIVEN** a `cqlite-flight` unit test (in `src/`, reached by `--lib`) that no gate component names
  individually
- **AND** an assertion in it is made to fail
- **WHEN** the full gate runs
- **THEN** the Flight component records FAIL and the run's `RESULT:` is `FAIL`

#### Scenario: The Flight component cannot pass without executing tests
- **GIVEN** an invocation of the Flight component whose `--lib` unit suite compiles but executes zero tests
- **WHEN** the component completes
- **THEN** it records FAIL, naming the unittest target and the zero-tests condition, and never PASS

#### Scenario: A newly added binary target cannot escape the zero-tests guard
- **GIVEN** a new `[[bin]]` target added to `cqlite-flight` whose unit tests execute zero tests
- **WHEN** the Flight component runs
- **THEN** the guard's subject set — derived from `cargo metadata` — includes that binary
- **AND** the component records FAIL, with no edit to `scripts/agent-gate.sh` having been required

#### Scenario: A selected unittest target that stops being selected fails the gate
- **GIVEN** the component's cargo invocation no longer selects one of `src/lib.rs` / `src/main.rs`
- **WHEN** the component completes
- **THEN** it records FAIL, naming the unobserved unittest target — an explicit selector silently dropping a
  target kind is the never-executed hole this lane exists to close

#### Scenario: The lane NAMES the integration-target count it does not run, and its issue
- **WHEN** the Flight component runs, whether it passes or fails
- **THEN** its stdout `>>>` lines AND its component log both state the number of declared
  `cqlite-flight` integration targets, derived from `cargo metadata` at run time
- **AND** state that this lane executes none of them
- **AND** name CI's Flight tier (`.github/workflows/flight-ci.yml` line 229) as what does cover them, with
  its path mandate and the `required` fail-closed behaviour (#2910)
- **AND** name issues **#3384** and **#3383** as owning the gap

#### Scenario: A failed target-count derivation fails the gate rather than understating the gap
- **GIVEN** `cargo metadata` cannot be read, or the declared-target count comes back zero
- **WHEN** the Flight component runs
- **THEN** it records FAIL naming the DERIVATION, never a census reporting a zero or unknown gap — an
  understated gap is the silent omission this change exists to eliminate

#### Scenario: The oracle component keeps its own fixture predicates
- **WHEN** the committed `test_compaction_tombstone_ttl` keyspace is absent but `test_deltas`/`test_tomb`
  are present
- **THEN** `flight-query-semantics-oracle` still selects and runs `issue_3095_flight_static_columns` and
  reports the other lane as skipped, exactly as before this change

### Requirement: The full gate builds AND executes `legacy-heuristics` at its own feature set

The full gate SHALL include a `legacy-heuristics` component that does both of the following:

1. builds `cqlite-core` at `default + legacy-heuristics` under `RUSTFLAGS="-D warnings"` — a feature set
   distinct from the ~30-feature union clippy already lints, so a warning-class defect visible only at this
   feature set surfaces here;
2. **executes** the `legacy-heuristics`-gated tests, `--lib` included.

The set of test targets executed SHALL be **derived mechanically**, never hard-coded, so adding a sixth
gated test target extends the lane without a second edit.

**The candidate set SHALL come from `cargo metadata`, not from a `tests/*.rs` glob** (roborev round-7
finding). A target is included when EITHER its `required-features` name `legacy-heuristics` (cargo gates
it in the manifest, and its source may carry no cfg string at all) OR its own `src_path` contains a cfg
reference to the feature. An earlier cut of this requirement specified the glob, which cannot see either a
manifest-gated target or a **directory-style** one (`tests/foo/main.rs`) — so the requirement itself, not
merely the code, understated the derivation it promised.

The derivation SHALL be **fail-closed**: if it yields zero targets, the component SHALL record FAIL naming
the derivation, and SHALL NOT record PASS. A lane with no subject has no verdict to give. The component
SHALL additionally run under the gate's existing zero-tests guard.

Compile-only SHALL NOT satisfy this requirement: `legacy-heuristics` is already test-compiled by the clippy
component, so a lane that only compiles adds no coverage. The distinguishing property is that a
**positively-gated test body whose assertion is inverted** makes this component FAIL.

#### Scenario: An inverted assertion in a positively-gated test body fails the gate
- **GIVEN** a `#[cfg(feature = "legacy-heuristics")]` test body in `cqlite-core/tests/` whose assertion is
  inverted so it must fail when executed
- **WHEN** the full gate runs
- **THEN** the `legacy-heuristics` component records FAIL
- **AND** the `clippy` component — which compiles the same body — still records PASS, demonstrating that
  execution, not compilation, is what caught it

#### Scenario: The target derivation finds no subject
- **GIVEN** a tree in which no `cqlite-core` test target references or requires `legacy-heuristics`
- **WHEN** the `legacy-heuristics` component runs
- **THEN** it records FAIL and its output names the failed derivation
- **AND** it does not record PASS or SKIP

#### Scenario: The target enumeration itself cannot be taken
- **GIVEN** a checkout in which `cargo metadata` cannot enumerate `cqlite-core`'s test targets
- **WHEN** the component runs
- **THEN** it records FAIL naming the failed derivation
- **AND** it SHALL NOT fall back to a `tests/*.rs` glob, because that fallback would silently omit
  manifest-gated and directory-style targets while appearing to succeed

#### Scenario: A new gated test file is picked up without editing the gate
- **GIVEN** a newly added `cqlite-core` test target carrying a `legacy-heuristics` cfg site
- **WHEN** the component runs
- **THEN** that target is among the ones executed, with no change to `scripts/agent-gate.sh`

#### Scenario: A target gated ONLY in the manifest is picked up
- **GIVEN** a `cqlite-core` test target declared with `required-features = ["legacy-heuristics"]` whose
  source contains no `legacy-heuristics` cfg string
- **WHEN** the component runs
- **THEN** that target is among the ones executed, because membership is decided by `required-features`
  as well as by a cfg site

### Requirement: The `legacy-heuristics` lane DECLARES the co-required cfg SITES it does not execute

The lane SHALL print, on every run, the `legacy-heuristics`-gated cfg **sites** whose co-required
features are not enabled at its feature set, with each site's file and line, and SHALL state that
whatever each site gates does not execute in this lane.

**It SHALL report SITES, not a count of test bodies.** Reporting bodies was specified in earlier cuts of
this requirement and produced a review finding in FOUR consecutive rounds (attributes counted as bodies;
`any(...)` assumed conjunctive; stacked attributes missed; a gated `mod tests` classified as support code
while crate-level `#![cfg(...)]` was ignored). A body count is not derivable without parsing Rust — one
site can gate an entire module — and it was never needed: the census exists so a human knows where gated
code is silently absent. The site claim is also strictly more honest, because "whatever this site gates
does not execute here" is true of a test, an import, a module or a crate root alike.

The subject SHALL cover everything the lane executes — the selected integration targets **and**
`cqlite-core/src/**`, since the lane runs `--lib` — and SHALL scan each target's `src_path` from cargo
metadata rather than a reconstructed path.

Feature tokens SHALL be accumulated across the whole attribute **cluster**, because Rust ANDs stacked
`#[cfg]` attributes; an inner `#![cfg(...)]` attribute SHALL be its own site, since it gates the
enclosing scope and attaches to no following item. A site whose Boolean shape the census does not
evaluate — `not(...)`, `any(...)`, `cfg_attr` — SHALL be reported as **unclassified** and SHALL NOT be
counted as omitted: `any(feature = "legacy-heuristics", feature = "X")` is reachable in this lane, so
calling it omitted would be a false claim.

An unreadable included source SHALL be a FAIL, and a census that cannot be taken SHALL NOT be reported
as empty.

#### Scenario: A gated module and a crate-level attribute are both reported
- **GIVEN** a source with a crate-level `#![cfg(all(feature = "legacy-heuristics", feature = "X"))]` and,
  separately, a `#[cfg(all(feature = "legacy-heuristics", feature = "Y"))] mod tests { … }`
- **WHEN** the component runs
- **AND** neither `X` nor `Y` is enabled at this feature set
- **THEN** the census reports TWO distinct sites with their line numbers
- **AND** it does not claim how many test bodies they gate

#### Scenario: A reachable gated body is not reported as omitted
- **GIVEN** a test gated only on `legacy-heuristics`, and another gated on
  `any(feature = "legacy-heuristics", feature = "X")` with `X` disabled
- **WHEN** the component runs
- **THEN** neither is counted as an omitted site
- **AND** the `any(...)` one is reported as unclassified rather than as a gap

### Requirement: The full gate compiles `parquet` and `delta-scan` in mutual isolation

The full gate SHALL include two components that compile `cqlite-core` with `--no-default-features
--features all-compression,parquet` and `--no-default-features --features all-compression,delta-scan`
respectively — each **without** the other feature, and neither via `--all-features`.

Each SHALL compile **the library together with its inline `#[cfg(test)]` modules**, via
`cargo test --lib --no-run`, under `RUSTFLAGS="-D warnings"`. A bare `cargo check`/`cargo check --lib` is
blind to the incident class this change exists to catch (#1978: an ungated `#[cfg(test)]` module referencing
a feature-gated item), because neither compiles `cfg(test)` code.

Each SHALL NOT use `--all-targets`. **CORRECTION:** this requirement previously mandated `--all-targets`.
The argument for compiling test code stands; the instrument was wrong. `--all-targets` additionally compiles
cqlite-core's ~100 **integration** test files, which are written against the **default** feature set and
therefore fail here on modules the lane deliberately configures out — measured:
`issue_1004_primitive_codec_vectors.rs:23` (`storage::serialization`), `issue_2412_wraparound_scan.rs:42`
(`storage::write_engine`), `contract_stability_tests.rs:23` (`cqlite_core::query`). Those failures are
**noise**, not cross-feature leakage, and a lane that reds on its own scaffolding is a lane agents learn to
waive. The #1978 incident class lives in `cqlite-core/src/**`'s inline `#[cfg(test)]` modules, which
`--lib --no-run` compiles and no integration target is needed for; `minimal-build` already uses this exact
shape for this exact reason.

The two SHALL be **separately named** components, so a FAIL in the SUMMARY identifies which direction of
coupling broke without reading the log.

#### Scenario: A parquet-gated item reaching into delta-scan-gated code fails the parquet lane
- **GIVEN** a `#[cfg(feature = "parquet")]` item in `cqlite-core` that references an item gated behind
  `#[cfg(feature = "delta-scan")]`
- **WHEN** the full gate runs
- **THEN** `feature-iso-parquet` records FAIL
- **AND** the `clippy` component, which enables both features at once, still records PASS

#### Scenario: The mirror direction is caught by its own lane
- **GIVEN** a `#[cfg(feature = "delta-scan")]` item referencing a `#[cfg(feature = "parquet")]`-gated item
- **WHEN** the full gate runs
- **THEN** `feature-iso-delta-scan` records FAIL and names itself in the SUMMARY

#### Scenario: A feature-orphaned test helper inside the lib fails the lane
- **GIVEN** a helper in an inline `#[cfg(test)]` module in `cqlite-core/src/**` whose only caller is gated
  out at the isolated feature set, producing a dead-code warning
- **WHEN** the isolation lane runs
- **THEN** it records FAIL, because `cargo test --lib --no-run` compiles the lib's `cfg(test)` modules under
  `-D warnings`
- **AND** the same helper is invisible to a bare `cargo check --lib`, which never compiles `cfg(test)` code

### Requirement: Each new lane is declared once per registry and is visible in `--list` and the SUMMARY

Each new component SHALL appear in `scripts/agent-gate.sh --list` output and, after a full run, as its own
line in the `==== AGENT-GATE SUMMARY ====` block with a status and a duration.

The `--lite` and `--delta` component sets SHALL NOT gain any of the four lanes: they are full-gate
components. `--lite`'s existing behaviour SHALL be unchanged.

A **structural self-test** SHALL pin the registration, so dropping a lane from the component array reds a
run rather than silently shrinking the gate of record. It SHALL assert, for each of the four lanes, that it
is in the `COMPONENTS` array, that it is reachable in the dispatch table, and that `--list` prints it — and
additionally that no lane has leaked into `LITE_COMPONENTS`.

**MEASURED CORRECTION (this change).** This requirement was first written claiming the self-test would red
`--lite`. That is **false** and the claim is withdrawn rather than engineered around:
`scripts/tests/test_agent_gate_summary.sh` is executed by the **`tooling-tests`** component
(`scripts/agent-gate.sh`, `run_tooling_tests` — cited by FUNCTION rather than line number, because the
line moved twice during this change and a stale line citation is what the C re-audit kept finding), and
`LITE_COMPONENTS` is `(file-size fmt clippy roborev-lints scoped-tests)` — `tooling-tests` is not in it. So the assert is enforced by the **FULL gate**, i.e. the gate
of record, which every issue must pass before merge; it is NOT enforced by the fast loop.

The self-test SHALL NOT be forced into `--lite` to make the original sentence true. Two reasons, both
measured or structural: it runs **141 assertions in 14 s**, against a `roborev-lints` charter of sub-second
hermetic checks; and `roborev-lints` exists to mechanize specific roborev **finding classes**, so
gate-registration asserts are a category error there. Catching a dropped lane at the gate of record — before
any merge — is the property that actually matters; catching it seconds earlier is not worth widening a
component's charter.

#### Scenario: `--list` and the SUMMARY both name the new lanes
- **WHEN** `scripts/agent-gate.sh --list` runs
- **THEN** all four new component names are printed
- **AND** after a full run each appears in the SUMMARY block with a status and a duration

#### Scenario: Silently dropping a lane reds the full gate
- **GIVEN** one of the four names removed from the gate's registries
- **WHEN** `scripts/tests/test_agent_gate_summary.sh` runs (as the full gate's `tooling-tests` component runs
  it)
- **THEN** it exits non-zero and the failing assertion **names the missing lane**
- **AND** the demonstration is performed in a throwaway `git worktree`, leaving the live checkout clean

#### Scenario: The fast loop does not inherit the new lanes
- **WHEN** `scripts/agent-gate.sh --lite` runs on an unmodified tree
- **THEN** none of the four new components is executed, and the LITE SUMMARY's `MODE: lite` component set is
  unchanged from before this change

### Requirement: Every new lane is observed to fire on a planted break and not to fire on a clean tree

Presence SHALL NOT be accepted as evidence. A committed, re-runnable harness SHALL, for **each** of the four
lanes, plant the minimal break of that lane's own subject and assert the lane exits **non-zero**, and SHALL
assert the same lane exits **zero** on the unbroken tree — so the harness cannot pass by failing everything.

The harness SHALL perform its mutations in a **throwaway `git worktree`**, never in the live checkout: a
mid-run tree mutation is itself a gate FAIL (#2926), and a harness that edited the tree its own gate was
running in would be the defect it exists to catch.

Its output SHALL name, per lane, what was planted and what fired.

The harness SHALL NOT be added to the default component set — it performs real compiles, and taxing every
full gate to re-prove a static property is disproportionate. It SHALL be runnable on demand, and the
observation of all four lanes firing SHALL be recorded in the repository.

**ATTRIBUTION (added during implementation, and it is the sharper half of this requirement).** A non-zero
exit SHALL NOT by itself count as an observation. A lane that broke for an unrelated reason produces an
IDENTICAL exit code and an IDENTICAL SUMMARY line to one that detected the plant, so a bare red is not
evidence either. Each planted run's output SHALL **name the planted symbol**, and a red that does not SHALL
be reported as `FIRED-UNATTRIBUTED` and SHALL fail the harness. This is the same rule as "a positive verdict
requires an affirmative measurement", applied to the harness's own verdict.

#### Scenario: Each lane fires on its own planted break, and the red is attributable
- **WHEN** the harness runs
- **THEN** for each of `flight-tests`, `legacy-heuristics`, `feature-iso-parquet`,
  `feature-iso-delta-scan`, the lane's `--only` run exits **1** on the planted break with that component
  recorded `FAIL`
- **AND** that run's output NAMES the planted symbol, so the red is shown to be this plant's
- **AND** the harness names the planted break and the lane that fired

#### Scenario: The harness checks the negative direction
- **WHEN** the harness runs each lane against the unbroken worktree copy
- **THEN** each lane's `--only` run exits **3** (`PARTIAL` — the gate deliberately refuses to let a partial
  run be scripted into a green claim, so zero is NOT the clean-direction exit code) with that component
  recorded `PASS`
- **AND** a harness run in which a lane reds in BOTH directions is reported as a harness FAILURE, not as a
  successful observation

#### Scenario: The harness never mutates the live checkout, and VERIFIES it
- **WHEN** the harness runs from a clean worktree
- **THEN** `git status --porcelain` in that worktree is unchanged by the run
- **AND** the harness itself MEASURES that invariant rather than only asserting it in prose: it captures
  the live checkout's `git status --porcelain` at start and re-compares it **before printing any verdict**
  and again **in cleanup** (so an interrupted run is covered too), reporting any difference as a HARNESS
  FAILURE — a successful observation accompanied by a mutated live checkout is not a success

#### Scenario: The harness refuses to run from a dirty checkout
- **WHEN** the live checkout has uncommitted changes
- **THEN** the harness exits **2** without running any lane, because the throwaway worktree is created from
  committed `HEAD` and uncommitted changes are therefore silently excluded from every run — a PASS would
  describe code other than the code being reviewed
- **AND** there is no override flag or env var: the only thing one could buy is a green about unchanged code

### Requirement: The new lanes' cost is measured, reported, and kept off the fast loop

The change SHALL report **two distinct cost numbers**, neither presented as the other:

1. the **per-component durations** of the four new lanes, read from the full-gate SUMMARY block;
2. the **added wall-clock time** of the full gate, measured as `max(0, SIDE_total − MAIN_total)` from the
   gate of record's OWN per-component durations.

The second number SHALL NOT be derived by summing the first: all four lanes run in the SIDE lane
concurrently with MAIN, so the sum overstates the added wall time — possibly to ≈0.

**The measurement METHOD is prescribed, and an earlier version of this requirement prescribed one that
cannot be taken here.** It mandated a baseline full run at the merge base versus the gate of record, run
sequentially on one machine. That was attempted and **abandoned mid-run**: this worker box hosts five lane
worktrees and sustained a load average of 52–86 on 16 cores during the baseline, so a four-component delta
cannot be recovered from two totals whose noise exceeds the delta, and the "after" run would sit under
different load again. Publishing that subtraction would have been a number with no measurement behind it.

The replacement is **load-independent and single-run**: concurrent work adds wall time only insofar as it
outlasts MAIN, so if MAIN still finishes last the four lanes cost **zero** added wall time, and if SIDE
finishes last the added time is exactly its excess. Both lanes are inside the same run under the same load,
so the figure is immune to whole-box contention. Where the reported figure was taken with build caches
pruned, that SHALL be stated, and both lanes' caches SHALL have been pruned **symmetrically** — pruning only
SIDE would make SIDE look slow against a warm MAIN and inflate the very number being reported.

The instrument the isolation lanes use is governed by the mutual-isolation requirement above
(`cargo test --lib --no-run`), NOT by this requirement. An earlier version mandated `cargo check` here,
which contradicted it: `cargo check` does not compile the lib's `#[cfg(test)]` modules and is therefore blind
to the #1978 incident class these lanes exist to catch.

#### Scenario: Both cost numbers are posted
- **WHEN** the PR is opened
- **THEN** its body carries the four per-component durations from the SUMMARY **and** the
  `max(0, SIDE_total − MAIN_total)` figure computed from that same SUMMARY, each labelled as what it is
- **AND** neither is presented as the other, and the sum of the per-component durations is not offered as the
  added wall time

#### Scenario: A cost figure whose method could not be applied is not invented
- **GIVEN** a measurement method this requirement prescribes that cannot be applied on the host in question
- **WHEN** the cost is reported
- **THEN** the report states which method was used and why the prescribed one was not applicable
- **AND** it does NOT publish a figure produced by a method that was not actually carried out

### Requirement: Doctrine states what the full gate now certifies

CLAUDE.md's gate table describes what the **Full** mode covers; that description SHALL be updated in this
same change to name the added coverage, and the website `agents-developing/gate-contract/` page SHALL be
updated to match.

The publish SHALL be accepted only on the **new content being served** — a phrase this change introduces,
found in the fetched page — never on an HTTP 200 or a green deploy alone (#3042: the CDN can serve the
previous page for ~3 minutes after a successful deploy).

#### Scenario: The doctrine change ships with the code change
- **WHEN** the PR is reviewed
- **THEN** CLAUDE.md's Full-gate row names the added feature-matrix coverage
- **AND** the matching website page is updated in the same PR

#### Scenario: Publication is verified by served content
- **WHEN** the site deploy completes
- **THEN** fetching the `agents-developing/gate-contract/` page and grepping for the newly introduced phrase
  returns a non-zero count
- **AND** a zero count is reported as not-yet-published, never recorded as done
