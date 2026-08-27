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

### Requirement: The full gate executes the `cqlite-flight` test suite locally

The full `scripts/agent-gate.sh` SHALL include a component that **executes** `cqlite-flight`'s tests, not
merely compiles them. Its reach SHALL extend beyond the test targets the gate already names
(`query_semantics_flight_parity`, `issue_3095_flight_static_columns`, and the `memory-budget` dhat target):
a failing assertion in a `cqlite-flight` test target that no existing component names SHALL make the full
gate FAIL.

The component SHALL run in the SIDE lane with its **own** `CARGO_TARGET_DIR`, because `cqlite-flight` is a
separate crate built against a divergent `cqlite-core` feature set and sharing MAIN's target dir would
thrash it. It SHALL be declared in `DATASET_COMPONENTS` if any target it runs consumes fixtures, so the
existing dataset preflight applies to it. It SHALL run under the gate's existing zero-tests guard, so
"compiled, executed 0 tests" cannot be recorded as a pass.

There SHALL be **no environment variable that disables this component**. `cqlite-flight` is a committed
workspace member and is never legitimately absent; fixture-dependent sub-targets may SKIP only through the
gate's existing dataset machinery, which reports the skip.

The pre-existing `flight-query-semantics-oracle` component SHALL be left functionally unchanged, including
its per-lane fixture SKIP predicates.

#### Scenario: A broken Flight test outside the named oracle targets fails the gate
- **GIVEN** a `cqlite-flight` integration test target that no gate component names individually
- **AND** an assertion in it is made to fail
- **WHEN** the full gate runs
- **THEN** the new Flight component records FAIL and the run's `RESULT:` is `FAIL`

#### Scenario: The Flight component cannot pass without executing tests
- **GIVEN** an invocation of the Flight component that compiles but executes zero tests
- **WHEN** the component completes
- **THEN** it records FAIL, naming the zero-tests condition, and never PASS

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

The set of test targets executed SHALL be **derived mechanically from the committed source** (the
`cqlite-core/tests/*.rs` files that reference `legacy-heuristics`), never hard-coded, so adding a sixth
gated test file extends the lane without a second edit.

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
- **GIVEN** a tree in which no `cqlite-core/tests/*.rs` file references `legacy-heuristics`
- **WHEN** the `legacy-heuristics` component runs
- **THEN** it records FAIL and its output names the failed derivation
- **AND** it does not record PASS or SKIP

#### Scenario: A new gated test file is picked up without editing the gate
- **GIVEN** a newly added `cqlite-core/tests/*.rs` carrying a `legacy-heuristics` cfg site
- **WHEN** the component runs
- **THEN** that target is among the ones executed, with no change to `scripts/agent-gate.sh`

### Requirement: The full gate compiles `parquet` and `delta-scan` in mutual isolation

The full gate SHALL include two components that compile `cqlite-core` with `--no-default-features
--features all-compression,parquet` and `--no-default-features --features all-compression,delta-scan`
respectively — each **without** the other feature, and neither via `--all-features`.

Each SHALL compile **test targets as well as the library** (`--all-targets`) under
`RUSTFLAGS="-D warnings"`. A lane that compiles only the library is blind to the incident class this change
exists to catch (#1978: an ungated `#[cfg(test)]` module referencing a feature-gated item), because a
library-only compile never builds test targets.

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

#### Scenario: A feature-orphaned test helper fails the lane
- **GIVEN** a `#[cfg(test)]` helper in `cqlite-core` whose only caller is gated out at the isolated feature
  set, producing a dead-code warning
- **WHEN** the isolation lane runs
- **THEN** it records FAIL, because it compiles test targets under `-D warnings`

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
(`scripts/agent-gate.sh:7020`), and `LITE_COMPONENTS` is `(file-size fmt clippy roborev-lints
scoped-tests)` — `tooling-tests` is not in it. So the assert is enforced by the **FULL gate**, i.e. the gate
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

#### Scenario: Each lane fires on its own planted break
- **WHEN** the harness runs
- **THEN** for each of `flight-tests`, `legacy-heuristics`, `feature-iso-parquet`,
  `feature-iso-delta-scan`, the lane exits non-zero on the planted break
- **AND** the harness names the planted break and the lane that fired

#### Scenario: The harness checks the negative direction
- **WHEN** the harness runs each lane against the unbroken worktree copy
- **THEN** each lane exits zero
- **AND** a harness run in which a lane fails in BOTH directions is reported as a harness FAILURE, not as a
  successful observation

#### Scenario: The harness never mutates the live checkout
- **WHEN** the harness runs from a clean worktree
- **THEN** `git status --porcelain` in that worktree is unchanged by the run

### Requirement: The new lanes' cost is measured, reported, and kept off the fast loop

The change SHALL report **two distinct cost numbers**, neither presented as the other:

1. the **per-component durations** of the four new lanes, read from the full-gate SUMMARY block;
2. the **added wall-clock time** of the full gate, measured as a baseline full run at the merge base versus
   the gate of record on this branch, run **sequentially on one machine** (one gate at a time, #2640).

The second number SHALL NOT be derived by summing the first: three of the four lanes run in the SIDE lane
concurrently with MAIN, so the sum overstates the added wall time.

The isolation lanes SHALL use `cargo check` rather than a full build, keeping their cost proportionate to
their purpose.

#### Scenario: Both cost numbers are posted
- **WHEN** the PR is opened
- **THEN** its body carries the four per-component durations from the SUMMARY and the baseline-versus-after
  full-gate totals, each labelled as what it is

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
