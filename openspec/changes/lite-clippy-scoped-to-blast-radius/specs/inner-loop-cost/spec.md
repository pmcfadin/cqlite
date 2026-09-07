# inner-loop-cost Specification

## Purpose

The cost of a `--lite` run SHALL be a function of the diff it certifies.  A narrowing SHALL be
disclosed, and an unmeasurable scope SHALL never read as a clean one.

## Requirements

### Requirement: `--lite` clippy SHALL lint the diff's blast radius unioned with a declared floor

The `clippy` component under `--lite` SHALL lint exactly `blast_radius ∪ FLOOR`, where
`blast_radius` is the package set `--lite` already derives for `scoped-tests` (including the #2658
direct-dependent fan-out when a changed path is under `cqlite-core/src/`) and `FLOOR` is a declared
list of packages linted on every lite run regardless of diff.  Each selected package SHALL be invoked
at the identical feature flags the existing four-stage matrix at `scripts/agent-gate.sh:11193` uses
for that package, preserving stage 1's exclusion set so `--all-features` never activates the duckdb
or OpenTelemetry features.  The full gate's call to `run_clippy` SHALL be unchanged and SHALL continue
to lint the whole workspace.

#### Scenario: FLOOR is linted on a diff that does not touch it
- **WHEN** `--lite` runs on a diff touching only `cqlite-core/src/`
- **THEN** every package in `FLOOR` is linted
- **AND** `cqlite-py` is among them

#### Scenario: A scoped run cannot lint a package at narrower features than the gate of record
- **WHEN** a package in the scope is linted
- **THEN** its feature string is the one the four-stage matrix uses for that package
- **AND** stage 1's exclusion of `cqlite-core`, `cqlite-cli`, `cqlite-flight`, `cqlite-py` and `cqlite-node` is preserved

#### Scenario: A narrow diff lints only its own package set
- **WHEN** `--lite` runs on a diff touching one file in one non-`cqlite-core` package
- **THEN** the `clippy` component lints that package's set and not the whole workspace
- **AND** the elapsed clippy time is recorded in the measurement artifact against the unscoped baseline

#### Scenario: A core-src diff retains the dependent fan-out
- **WHEN** `--lite` runs on a diff touching a path under `cqlite-core/src/`
- **THEN** the scoped set includes every workspace member that directly declares a dependency on `cqlite-core` and owns a `--test` target
- **AND** a clippy violation introduced in such a dependent by the core change FAILs the scoped run

#### Scenario: Per-package flags match the unscoped matrix
- **WHEN** a package is linted under the scoped run
- **THEN** its feature and target flags are the same ones the #1844 matrix uses for that package
- **AND** no package is linted at a narrower feature set than the gate of record would use

#### Scenario: The full gate is unaffected
- **WHEN** the full `scripts/agent-gate.sh` runs
- **THEN** its `clippy` component lints the unscoped per-package matrix
- **AND** its `AGENT-GATE SUMMARY` carries no scoped disclosure

### Requirement: A scoped run SHALL disclose what it did not check, with an affirmative zero

The `AGENT-GATE LITE SUMMARY` line for `clippy` SHALL name the count of packages CHECKED, the count
NOT CHECKED, and where the excluded names are recorded.  A run that excluded nothing SHALL print
`0 NOT-CHECKED RECOGNISED` rather than a bare `0`, so a full-coverage lite run is textually
distinguishable from a narrowed one.  The line SHALL carry counts and gate-authored text only; the
package names SHALL live in the component log, and the value SHALL pass through the existing
`_status_detail` boundary that strips control characters and withholds any value carrying the
completion probe's `RESULT:` token.

#### Scenario: A narrowed run names its exclusions by count and log
- **WHEN** a scoped clippy run excludes at least one package
- **THEN** the summary line states N of M packages CHECKED and K NOT CHECKED
- **AND** it points at the component log where the excluded package names are written
- **AND** it contains no package name inline

#### Scenario: A full-coverage scoped run is not mistakable for an unmeasured one
- **WHEN** the blast radius happens to include every workspace package
- **THEN** the line reads `0 NOT-CHECKED RECOGNISED`
- **AND** it does not read as a bare `0`

#### Scenario: A package name cannot break the summary grammar
- **WHEN** a workspace package path contains a control character or the substring `RESULT:`
- **THEN** the disclosure line is emitted with that value withheld or sanitized rather than rewritten
- **AND** the completion probe `grep -qE 'RESULT: (PASS|FAIL)'` still reads the run's own verdict correctly

### Requirement: An unmeasurable scope SHALL SKIP naming the cause and SHALL NOT lint nothing

If the blast-radius derivation cannot be completed, the `clippy` component under `--lite` SHALL
report `SKIP` with the cause named, and SHALL NOT proceed with an empty package set.  A run that
linted zero packages SHALL NOT be able to report `PASS`.  Because `FLOOR` is non-empty by
construction, a computed scope that is empty SHALL be treated as evidence the derivation is broken
rather than as an instruction to lint nothing.  `cargo metadata` failing or unavailable, a changed
path resolving to no workspace member, and a base ref that does not resolve are each a named cause.

#### Scenario: A failed derivation skips rather than passes
- **WHEN** `cargo metadata` fails during blast-radius derivation under `--lite`
- **THEN** the `clippy` component reports `SKIP` naming that cause
- **AND** it does not report `PASS`
- **AND** it does not lint an empty package set

#### Scenario: An empty package set is unreachable as a PASS
- **WHEN** the derived package set is empty for any reason
- **THEN** the component does not emit a `PASS` verdict
- **AND** the summary states that the scope is unknown

### Requirement: FLOOR SHALL preserve the #1893 python compile backstop

`FLOOR` SHALL contain `cqlite-py`, and its definition SHALL cite #1893 at the definition site.
`--lite`'s python tier classifies a venv, pip or maturin toolchain failure as `SKIP` rather than
`FAIL`, and that SKIP is safe only because the clippy pass still compiles `cqlite-py` in the same run.
Every `FLOOR` entry SHALL carry a named reason at the definition site.

#### Scenario: A broken python binding source cannot pass a lite run via the SKIP route
- **WHEN** `bindings/python/src` does not compile
- **AND** `--lite` runs on a diff that touches no python path, so the python tier reports `SKIP`
- **THEN** the `clippy` component FAILs, because `cqlite-py` is in `FLOOR`
- **AND** the lite run does not report `OVERALL=PASS`

#### Scenario: Removing cqlite-py from FLOOR is caught
- **WHEN** `cqlite-py` is removed from `FLOOR`
- **THEN** `scripts/tests/test_agent_gate_lite_clippy_scope.sh` fails

### Requirement: A discriminating regression test SHALL pin the behaviour inside tooling-tests

`scripts/tests/test_agent_gate_lite_clippy_scope.sh` SHALL be wired to the `tooling-tests` component
and SHALL red under each of four named mutations: the scoped set replaced by the empty set, the
disclosure line removed from the summary, the #2658 fan-out set dropped for a `cqlite-core/src/`
path, and `cqlite-py` removed from `FLOOR`.  Each mutation SHALL be applied within the test's own
scratch copy and never in the checkout, so the test cannot perturb the tree and trip the #2926
mid-run mutation check.

#### Scenario: The empty-set mutation reds the test
- **WHEN** the scoped derivation is mutated to return an empty package set
- **THEN** `test_agent_gate_lite_clippy_scope.sh` fails

#### Scenario: The removed-disclosure mutation reds the test
- **WHEN** the scoped disclosure line is removed from the lite summary
- **THEN** `test_agent_gate_lite_clippy_scope.sh` fails

#### Scenario: The dropped-fan-out mutation reds the test
- **WHEN** the direct-dependent fan-out is dropped from the scope for a `cqlite-core/src/` path
- **THEN** `test_agent_gate_lite_clippy_scope.sh` fails

#### Scenario: The dropped-FLOOR-entry mutation reds the test
- **WHEN** `cqlite-py` is removed from `FLOOR`
- **THEN** `test_agent_gate_lite_clippy_scope.sh` fails

#### Scenario: The test does not perturb the checkout
- **WHEN** the test runs inside `tooling-tests` during a gate run
- **THEN** the gate's `tree-integrity:` check reports no mutation and `dirty: no`

### Requirement: Doctrine SHALL state the scoped default and cite the measurement

`CLAUDE.md`'s Lite row SHALL state that `--lite` clippy is scoped to `blast_radius ∪ FLOOR` by
default, SHALL retain `CQLITE_CLIPPY_FULL=1` as the explicit whole-workspace escape, and SHALL cite
the committed measurement artifact for the cost bands rather than restating superseded numbers.  Its
stale citations SHALL be corrected: `run_clippy` is one definition called by both modes, not the
`:17233` / `:18220` pair the row currently names.  `run_lite`'s internal disagreement SHALL be
resolved, where its function comment says "FULL-workspace clippy" and the banner it prints says
"scoped workspace clippy."  Every correction SHALL land in the same change as the behaviour.

#### Scenario: Doctrine matches the code on merge
- **WHEN** `CLAUDE.md` is read after this change merges
- **THEN** its Lite row states the scoped clippy default, names `FLOOR`, and names the escape flag
- **AND** it no longer states that lite clippy is not diff-scoped
- **AND** it no longer cites `:17233` or `:18220` as two dispatch sites
- **AND** it cites `docs/round-artifacts/lite-clippy-scope-measurements.md` for the bands

#### Scenario: The gate script no longer contradicts itself
- **WHEN** `run_lite`'s comment and its printed banner are read after this change
- **THEN** both describe the same clippy scope
