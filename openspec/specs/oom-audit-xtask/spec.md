# oom-audit-xtask Specification

## Purpose
TBD - created by archiving change oom-audit-xtask. Update Purpose after archive.
## Requirements
### Requirement: A syn-based AST audit, not string matching

The `xtask` crate SHALL implement the `oom-audit` subcommand as an AST audit that parses each in-scope
`.rs` file with `syn` and evaluates rules over the parsed syntax tree, and SHALL NOT decide violations
by regex or substring matching. It SHALL restrict analysis to a committed set of scope roots, so files
outside scope are neither parsed nor reported.

#### Scenario: A renamed helper with the same shape is still caught

- **GIVEN** a scoped scan function containing an unbounded `.collect::<Vec<_>>()` over a row iterator
- **WHEN** the same body is refactored so the collecting helper and its variables are renamed but the
  syntactic shape is unchanged
- **THEN** the audit reports the violation in both the original and renamed forms (it matches on the
  parsed shape, not on identifier text).

#### Scenario: A file outside the configured scope roots is not analyzed

- **GIVEN** a `.rs` file outside the committed scope roots that contains the violating shape
- **WHEN** the audit runs
- **THEN** that file produces no finding (scope is enforced by path, not by content).

### Requirement: STREAM_RETURNS_VEC detects unbounded materialization in scan/producer functions

The audit SHALL implement a `STREAM_RETURNS_VEC` rule that flags, within a reader/producer scan
function in scope, a `.collect::<Vec<_>>()` or a `Vec::push`/`Vec::extend` accumulation loop over a
row/partition/cell iterator when no budget or bound is in scope for that function. A budget/bound SHALL
be recognized as any of: a `ResultBudget` in scope, a `buffer_size` / `batch_size` / `limit` /
`max_*` parameter, or an iterator adaptor that bounds the accumulation (`.take(n)`). The rule SHALL be
evaluated per function using in-scope syntax only; it SHALL NOT attempt interprocedural reachability.

#### Scenario: An unbounded collect on a scan path is flagged

- **GIVEN** a scoped scan function that collects a partition/row iterator into an owned `Vec` with no
  `ResultBudget`, bound parameter, or `.take(n)` in scope
- **WHEN** the audit runs in report mode
- **THEN** it emits a `STREAM_RETURNS_VEC` finding naming the file, the enclosing function, and the
  offending expression.

#### Scenario: The same collect with a budget in scope is not flagged

- **GIVEN** an otherwise identical scan function that threads a `ResultBudget` (or a `batch_size` /
  `.take(limit)` bound) through the accumulation
- **WHEN** the audit runs
- **THEN** it emits no `STREAM_RETURNS_VEC` finding for that function (the bound suppresses it
  structurally, with no allowlist entry required).

### Requirement: Suppression only via a committed allowlist with justification, issue link, and no orphans

Findings SHALL be suppressible only through a single committed allowlist TOML in which every entry
carries a content fingerprint of the allowed site, a non-empty `issue =` reference, and a non-empty
`justification =` string. An entry whose fingerprint matches no current in-scope source ("orphaned")
SHALL fail the audit. When an optional `expiry =` date is present and has passed, that entry SHALL fail
the audit. An allowlist entry missing `issue` or `justification` SHALL fail the audit.

#### Scenario: A fingerprint-matched allowlist entry suppresses its finding

- **GIVEN** a reviewed, sound whole-file read whose site is in the allowlist with a matching
  fingerprint, an issue link, and a justification
- **WHEN** the audit runs in enforce mode
- **THEN** that site produces no failing finding.

#### Scenario: An orphaned allowlist entry fails the audit

- **GIVEN** an allowlist entry whose fingerprint no longer matches any in-scope source (the code was
  removed or changed)
- **WHEN** the audit runs
- **THEN** the audit fails and names the orphaned entry (the allowlist cannot silently rot).

#### Scenario: An allowlist entry missing an issue link or justification fails

- **GIVEN** an allowlist entry with an empty or absent `issue` or `justification` field
- **WHEN** the audit runs
- **THEN** the audit fails and names the malformed entry.

#### Scenario: An expired allowlist entry fails

- **GIVEN** an allowlist entry carrying an `expiry` date in the past
- **WHEN** the audit runs
- **THEN** the audit fails and names the expired entry.

### Requirement: Report-only and enforce modes with a fail-closed enforce exit code

The `oom-audit` subcommand SHALL support a report-only default that prints findings and exits `0`
regardless of findings, and an `--enforce` mode that exits non-zero when any non-allowlisted finding,
orphaned entry, malformed entry, or expired entry is present. A self-test fixture deliberately
introducing the violating shape SHALL cause `--enforce` to exit non-zero.

#### Scenario: A newly introduced violation fails enforce mode

- **GIVEN** the self-test fixture that plants a `.collect::<Vec<_>>()` on a scoped scan path with no
  bound and no allowlist entry
- **WHEN** the audit runs with `--enforce`
- **THEN** the process exits non-zero and the finding is named in the output.

#### Scenario: Report-only mode never fails the build

- **GIVEN** the same fixture present and unallowlisted
- **WHEN** the audit runs in report-only mode (no `--enforce`)
- **THEN** findings are printed but the process exits `0`.

### Requirement: A SKIP-aware oom-audit agent-gate component

`scripts/agent-gate.sh` SHALL run `oom-audit` as a component that invokes the audit in `--enforce`
mode, is a hard FAIL on a real violation, and is loudly SKIP (never a silent PASS) when the `xtask`
crate cannot build or `cargo` is unavailable — following the `delivery-telemetry` SKIP-aware model.
The component SHALL be listed in the full `COMPONENTS` set and documented in the gate contract.

#### Scenario: A violation on a scoped path fails the gate component

- **GIVEN** a scoped scan path carrying an unallowlisted violating shape
- **WHEN** the `oom-audit` gate component runs
- **THEN** it records FAIL for that component.

#### Scenario: The component skips loudly when the tool cannot build

- **GIVEN** an environment where the `xtask` crate cannot be built (e.g. `cargo` absent)
- **WHEN** the `oom-audit` component runs
- **THEN** it records SKIP with a stated reason and never records a silent PASS.

### Requirement: The audit lands green via a seeded allowlist and a stated false-positive budget

Delivery SHALL land the component green: an initial report-only run SHALL be triaged, every reviewed
sound site SHALL be seeded into the allowlist with an issue link and justification, and the enforce
flip SHALL occur only after the report is clean. The change SHALL document the seeding process and the
false-positive posture so the component does not land red.

#### Scenario: The enforce flip happens only on a clean report

- **GIVEN** the seeded allowlist covering every triaged sound site in scope
- **WHEN** the audit runs with `--enforce` at the point the component is added to the gate
- **THEN** it exits `0` (no unallowlisted findings), so the gate component lands green.

### Requirement: The tool stays small and fast

The `xtask` crate SHALL remain small (campsite rule: source files ~800 lines) and depend only on
lightweight parsing/support crates (`syn`/`quote`/`walkdir`/`toml`), adding no dependency to
`cqlite-core` or `cqlite-flight`. The audit over the v1 scope SHALL complete in under ~30 seconds.

#### Scenario: The audit completes within the runtime budget

- **GIVEN** the v1 scope roots
- **WHEN** the audit runs end to end
- **THEN** it completes in under ~30 seconds on a developer machine.

