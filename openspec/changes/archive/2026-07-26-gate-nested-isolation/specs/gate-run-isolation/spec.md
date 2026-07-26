# gate-run-isolation — the gate of record is immune to nested/concurrent gate activity

## ADDED Requirements

### Requirement: A nested gate invocation SHALL NOT write the enclosing checkout's default summary path

`scripts/agent-gate.sh` SHALL export a parent-run marker (`AGENT_GATE_PARENT_RUN_ID`) for the
duration of its component runs. Any gate invocation that starts with this marker present in its
environment and no explicit `AGENT_GATE_SUMMARY_FILE` of its own SHALL default its summary file to a
private per-invocation path inside its own mktemp log directory — never the checkout-level default
(`.agent-gate-summary.txt` / `.agent-gate-lite-summary.txt` / `.agent-gate-delta-summary.txt`). An
explicit `AGENT_GATE_SUMMARY_FILE` set by the nested caller SHALL still be honored.

#### Scenario: nested gate with inherited parent env cannot alter the parent summary
- **WHEN** a parent gate is mid-run with summary file S, and a nested `agent-gate.sh` invocation is
  launched from the same checkout with the parent's environment and no explicit
  `AGENT_GATE_SUMMARY_FILE`
- **THEN** the nested run SHALL write its summary to a private path inside its own log directory
- **AND** S SHALL be byte-identical before and after the nested run (asserted by a regression
  self-test executed inside the `tooling-tests` component)

#### Scenario: nested caller's explicit summary path still wins
- **WHEN** a self-test launches a nested gate with `AGENT_GATE_SUMMARY_FILE` pinned to its own
  mktemp path
- **THEN** the nested run SHALL write exactly that path (existing self-test assertions keep working)

### Requirement: The gate SHALL fail loudly with a named cause on mid-run summary clobber

At each component boundary, the gate SHALL verify its summary file still carries its own
`run-id`. On detecting a foreign run-id (or a summary missing its run-id), the gate SHALL stop,
write a summary containing a named integrity failure line (`summary-integrity: FAIL` naming the
expected run-id) with `RESULT: FAIL`, and exit non-zero. A mid-run clobber SHALL never manifest as a
bare `INCOMPLETE` death with no named cause.

#### Scenario: foreign run-id detected mid-run
- **WHEN** a gate is mid-run and its summary file is externally overwritten with content stamped by
  a different run-id
- **THEN** at the next component boundary the gate SHALL terminate with `RESULT: FAIL` and a
  `summary-integrity: FAIL` line naming the expected run-id
- **AND** the exit code SHALL be non-zero

### Requirement: Gate self-tests SHALL be hermetic per run

Every gate self-test under `scripts/tests/` SHALL derive all fixture, sentinel, and temporary paths
from per-run `mktemp` namespaces with terminal `XXXXXX` templates (macOS-safe); no fixed shared
names. EXIT traps SHALL remove only paths created by the same run. In particular, the
parity-report self-test's mutated-manifest fixture SHALL be a per-run unique file (remaining under
the real repo root's `test-data/` as its tooling requires). A structural self-check SHALL fail the
`tooling-tests` component if a fixed `.tmp-*` fixture name or a non-terminal-`XXXXXX` mktemp
template is reintroduced in `scripts/tests/*.sh`.

#### Scenario: two concurrent self-test lanes in one checkout both pass
- **WHEN** two instances of the gate self-test files are executed concurrently in the same checkout
- **THEN** both instances SHALL pass (asserted by a bounded concurrency self-test inside
  `tooling-tests`)

#### Scenario: fixed-name fixture regression is caught structurally
- **WHEN** a change reintroduces a fixed shared fixture name or a macOS-unsafe mktemp template in
  `scripts/tests/*.sh`
- **THEN** the `tooling-tests` component SHALL FAIL naming the offending file and pattern

#### Scenario: tooling-tests wall-clock does not regress materially
- **WHEN** the `tooling-tests` component runs with the new self-tests included
- **THEN** its wall-clock SHALL be within ±10% of the pre-change baseline (timings recorded on the
  PR)
