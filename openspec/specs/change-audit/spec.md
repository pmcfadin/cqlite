# change-audit Specification

## Purpose
TBD - created by archiving change spec-driven-audit. Update Purpose after archive.
## Requirements
### Requirement: Spec-anchored intent audit (C) runs before merge
Every design-driven OpenSpec change SHALL be audited against its own specs by a
read-only spec-anchored audit (C) before it is merged. C's criteria SHALL come
from `openspec/changes/<name>/specs/**` (requirements + scenarios), never from a
GitHub issue body or other prose.

#### Scenario: Audit reads the change's specs as criteria
- **WHEN** C audits change `<name>`
- **THEN** it loads the requirements and scenarios from `openspec/changes/<name>/specs/**`
- **AND** it produces a verdict for each requirement of `satisfied`, `partial`, or `unmet`

#### Scenario: Auditor never edits code
- **WHEN** C runs
- **THEN** it only reports findings and SHALL NOT modify any file in the working tree

### Requirement: Coverage and blocking semantics
C SHALL block merge when intent is unverifiable. A requirement whose scenario
has no test that exercises it from the public surface SHALL be reported `unmet`.
A `partial` verdict SHALL include written justification or be treated as `unmet`.

#### Scenario: Uncovered requirement blocks merge
- **WHEN** a requirement's scenario has no test exercising it from the public surface
- **THEN** C reports that requirement `unmet`
- **AND** the change is blocked from merge

#### Scenario: All requirements satisfied with evidence passes
- **WHEN** every requirement is `satisfied` and each verdict names the test and public-surface call chain that exercises it
- **THEN** C passes and the change may proceed to code review

#### Scenario: Partial without justification is treated as unmet
- **WHEN** C reports a requirement `partial` with no written justification
- **THEN** the change is blocked from merge as if the requirement were `unmet`

### Requirement: Correctness precedes intent
C SHALL run only after `scripts/agent-gate.sh` reports PASS for the change. The
gate (correctness) and parity goldens (behavior) remain the source of truth for
whether the code works; C audits only whether the work matches the specs.

#### Scenario: Audit does not run on a red gate
- **WHEN** the gate has not reported PASS for the change
- **THEN** C does not run and the change cannot reach the intent-audit stage

### Requirement: Optional roborev escalation (B)
The workflow SHALL provide an optional spec-anchored roborev review (B) that can
be invoked using the change's proposal, design, and specs as review criteria,
for an independent semantic second opinion. B SHALL be on-demand and SHALL NOT
be required for every change.

#### Scenario: Escalation triggers
- **WHEN** C reports any requirement `partial`, OR the change is high-stakes, OR it touches doctrine (no-heuristics or cross-binding parity)
- **THEN** B (a spec-anchored roborev design review) MAY be invoked with the change's artifacts as criteria

#### Scenario: Escalation is not mandatory
- **WHEN** C reports all requirements `satisfied` and the change does not touch doctrine
- **THEN** B is not required and the change may proceed on C's verdict alone

### Requirement: Defined place in the merge flow
The audit SHALL occupy a single, ordered stage so enforcement layers do not
overlap: `apply → gate (correctness) → C (intent) → roborev (code) → merge →
archive`, with B available as an escalation off C.

#### Scenario: Ordering is enforced
- **WHEN** a change progresses toward merge
- **THEN** C (intent) runs after the gate (correctness) passes and before the change is merged and archived

