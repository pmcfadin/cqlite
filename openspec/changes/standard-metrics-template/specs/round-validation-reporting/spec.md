# round-validation-reporting

## ADDED Requirements

### Requirement: A canonical 14-point standard metrics template exists as committed doctrine

The project SHALL commit a canonical round-validation metrics template at
`docs/development/round-validation-metrics.md` that enumerates all 14 field-proposed metrics from the
#2367 round-9 "Proposed standard metrics" section, organized into the four groups **A. Correctness**,
**B. Hang/liveness**, **C. Throughput/scale**, and **D. Hygiene**. Each metric SHALL retain its
field-assigned number (A1–A3, B4–B8, C9–C11, D12–D14) so a report is unambiguously mappable to the
standard. The template SHALL state that its A/B "round gate" is a live-cluster field-validation verdict
distinct from `scripts/agent-gate.sh` (the pre-PR gate of record).

#### Scenario: Template enumerates all 14 metrics in four groups

- **GIVEN** the committed `docs/development/round-validation-metrics.md`
- **WHEN** a reader scans it
- **THEN** it lists exactly the 14 metrics A1–A3, B4–B8, C9–C11, D12–D14 under the four A/B/C/D group
  headings, each with its field number and a one-line description of what it protects
- **AND** it explicitly notes that the A/B round gate is a field-validation verdict, NOT the agent gate.

### Requirement: Each metric is classified GATE or TRACKED

The template SHALL mark every metric as either **GATE** (pass/fail — a failure makes the round verdict
FAIL) or **TRACKED** (a recorded number that never blocks the verdict but MUST be present for
round-over-round comparability). Group A (A1–A3) and group B (B4–B8) SHALL be GATE items; group C
(C9–C11) and group D (D12–D14) SHALL be TRACKED, adopting the field's own recommendation to "bake A/B
into the pass/fail gate and report C/D as tracked numbers." Binary hygiene items (D12 snapshot-leak,
D14 digest-pin) SHALL be marked `TRACKED (binary)` to signal they resolve to yes/no.

#### Scenario: Every row carries a GATE or TRACKED classification

- **GIVEN** the template
- **WHEN** a reader reads any of the 14 metric rows
- **THEN** that row is tagged either `GATE` or `TRACKED`
- **AND** A1–A3 and B4–B8 are `GATE`, C9–C11 and D12–D14 are `TRACKED`
- **AND** a round report that omits any GATE item cannot be marked a passing round.

### Requirement: The template pre-fills the round-9 baseline

The template SHALL pre-fill each metric's round-9 measured value (sourced from the #2367 round-9
report) as the comparison anchor, so the next round is directly comparable. Where round-9 did not
report a value, the row SHALL be marked `baseline: to establish`. The template SHALL state that the
baseline is comparison context, not a pass threshold, and that each subsequent round supersedes it.

#### Scenario: Round-9 measured values are present per metric

- **GIVEN** the template's baseline column
- **WHEN** a reader inspects the B5, B7, C9, and D14 rows
- **THEN** B5 reads `index_parses_total = 22 across the round, flat on warm (≤ #generations)`, B7 reads
  `~15–60s kill→baseline, no DaemonSet restart`, C9 reads `~0.9 qps / p50 9.4s / p99 17.7s / 0 errors
  (8 threads × 180s)`, and D14 reads the round-9 INDEX digest `round9@sha256:4dfad858…`
- **AND** any metric round-9 did not report (e.g. D12 snapshot-leak count) is marked `baseline: to
  establish`.

### Requirement: New round trackers are seeded from the template

The project SHALL provide a GitHub issue template `.github/ISSUE_TEMPLATE/round-tracker.yml` that seeds
a new round-validation tracker pre-populated with the 14-point checklist (GATE items as pass/fail
checkboxes, TRACKED items as fill-in fields) and a link back to
`docs/development/round-validation-metrics.md` for rationale and baseline. The canonical doc SHALL
remain the single source of truth; the issue template SHALL reference it rather than duplicate the
baseline.

#### Scenario: Creating a round tracker yields a pre-populated checklist

- **GIVEN** the `round-tracker.yml` issue template is selected when opening a new issue
- **WHEN** the tracker issue is created
- **THEN** its body contains the 14 metrics as GATE checkboxes + TRACKED fill-in fields grouped A/B/C/D
- **AND** it links to `docs/development/round-validation-metrics.md`
- **AND** the doc, not the issue template, holds the round-9 baseline (no duplication that could drift).

### Requirement: Report items are backed by in-repo local mirrors where cheap

The template SHALL, for each metric, state whether it is backed by an in-repo local mirror or is
field-only, so a reader knows which regressions the repo already catches between field rounds. It SHALL
cross-link the existing pins for B5 (index-parses-delta: the #2370 single-flight suite, #2383 resolve
pins, #2385 single-parse), B7 (cancellation reclaim: the #2383 cancel pins), and A3 (access-path
route/rows-scanned pins), and SHALL delegate C9 loadtest gating to #2377 (driver `--gate` mode) by
reference rather than re-implementing it.

#### Scenario: Existing pins are cross-linked, C9 delegated

- **GIVEN** the template's local-mirror column
- **WHEN** a reader inspects B5, B7, A3, and C9
- **THEN** B5/B7/A3 name their existing in-repo test files as the local mirror
- **AND** C9 is marked "loadtest gating delegated to #2377 (driver `--gate` mode)" and no new gate code
  is added by this change for C9.

### Requirement: The D12 snapshot-leak check is added to the testbed E2E

This change SHALL add exactly one new local mirror: a D12 hygiene assertion in the testbed E2E that,
after the query workload completes, no leaked `cqlite-`-prefixed snapshots remain — the local mirror of
the field's `nt listsnapshots | grep cqlite- == 0` check. The assertion SHALL fail the E2E if a
`cqlite-` snapshot is left behind, and SHALL NOT pass vacuously when the workload did not run, when the
check was not configured, **or when the check's own probe command fails to run** (a nonzero exit from
the operator-provided listing command SHALL be reported as a check failure, never treated as "ran
cleanly, 0 snapshots found").

#### Scenario: Leaked snapshot fails the testbed E2E

- **GIVEN** the testbed E2E after its query workload has run
- **WHEN** a `cqlite-`-prefixed snapshot remains on any target node
- **THEN** the E2E snapshot-leak check FAILS
- **AND** when zero `cqlite-` snapshots remain the check PASSES
- **AND** the check does not pass when the workload was skipped or produced no queries (no vacuous pass).

#### Scenario: A failing probe command reports FAIL, never a vacuous PASS

- **GIVEN** the D12 check is configured with a listing command
- **WHEN** that command exits nonzero (e.g. an unreachable node, an auth failure, or a malformed
  one-liner) and therefore produces no usable output
- **THEN** the check reports FAIL with the command's error detail
- **AND** it does NOT report PASS merely because the empty/partial output happened to contain zero
  `cqlite-`-prefixed lines.

### Requirement: The template is offered to the field and cross-linked in doctrine

The finalized template SHALL be posted on the round tracker (#2367 round channel) for round-10 adoption,
taking up the field's "happy to formalize" offer. The template SHALL be cross-linked from
`docs/development/pm-operating-loop.md` and the validation-playbook so it is discoverable as the
round-reporting standard.

#### Scenario: Template offered back and discoverable

- **GIVEN** the merged change
- **WHEN** a maintainer looks for the round-reporting standard
- **THEN** a comment on #2367 presents the finalized template for round-10 adoption
- **AND** `docs/development/pm-operating-loop.md` (and the validation-playbook) link to
  `docs/development/round-validation-metrics.md`.
