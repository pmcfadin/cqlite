# cli-graceful-shutdown-test-oracle — delta for cli-tests-sigint-flake (issue #3515)

**Scope.** `cqlite-cli/tests/graceful_shutdown_tests.rs` — both tests in the file. Test code only; no
`src/` change. The subject property is unchanged (SIGINT in a writable session flushes durably and
exits cleanly); what changes is the **oracle** used to observe it.

**Acceptance-criterion → requirement map** (issue #3515):

| AC | Requirement(s) |
|---|---|
| AC1 — no longer fails on a contended host while the handler works; prefer a property-observing oracle with liveness confirmation | ADDED *The shutdown oracle confirms the handler was entered before attributing a timeout*; ADDED *Wall-clock ceilings are calibrated from in-band measurements taken on the same host*; ADDED *The exit wait is progress-checked* |
| AC2 — the failure message must not assert a cause the measurement cannot establish | ADDED *Every wait failure reports only what its measurement establishes* |
| AC3 — RED-verify by actually breaking the shutdown handler | ADDED *The new oracle is observed to red on a genuinely broken handler* |
| AC4 — check the sibling test for the same shape | ADDED *The sibling threshold-flush test carries the same oracle* |
| — (design obligation, `design.md`) | ADDED *The test owns a total budget below the harness hard-kill* |

## ADDED Requirements

### Requirement: The shutdown oracle confirms the handler was entered before attributing a timeout

After delivering `SIGINT`, the test SHALL wait for the child's own **handler-entry progress marker**
on `stderr` (the `Received Ctrl-C — flushing memtable before exit` text emitted by
`cqlite-cli/src/main.rs`) **before** it waits for process exit, and SHALL treat observation of that
marker as establishing that the signal was delivered, that a shutdown handler exists and was entered,
and that the child was scheduled.

The test SHALL NOT attribute a subsequent exit timeout to a missing or absent handler once that marker
has been observed.

`stderr` SHALL be drained by a reader for the lifetime of the child, so that the marker is observable
and so that an undrained pipe cannot wedge the child.

#### Scenario: handler entry observed, exit slow
- **WHEN** the handler-entry marker is observed and the child then fails to exit within the exit budget
- **THEN** the failure SHALL state that the shutdown flush did not complete within the budget
- **AND** the failure SHALL NOT state or imply that a shutdown handler is missing, absent, or unimplemented

#### Scenario: handler entry never observed
- **WHEN** no handler-entry marker is observed within its budget
- **THEN** the failure SHALL name the exact substring it awaited
- **AND** SHALL print the transcript of what the child actually emitted
- **AND** SHALL name signal non-delivery, handler non-entry, and product marker-text drift as the candidate causes, without selecting between them

### Requirement: Wall-clock ceilings are calibrated from in-band measurements taken on the same host

Every wait budget in the file that follows a completed measurement SHALL be derived as
`clamp(base × scale, base, cap)`, where `scale = max(1, observed / quiet_baseline)` and `observed` is a
duration **measured during this same test run on this same host**:

* the spawn → readiness-banner duration (`t_boot`) SHALL calibrate the write-acknowledgement budget;
* the write → `OK` round-trip (`t_ack`) SHALL calibrate the handler-entry and exit budgets.

`quiet_baseline` SHALL be large enough that an unloaded host yields `scale == 1`, so that calibration
can only loosen a budget and never tighten one.

**This requirement is deliberately NOT universal, and the exception is the point.** The first wait in
each test — for the readiness banner — has no prior measurement to calibrate against and SHALL remain a
bare wall-clock deadline. It is the irreducible bound identified in `design.md`; it covers only process
spawn and engine init, and it is exempt from this requirement rather than silently non-compliant with it.

#### Scenario: quiet host
- **WHEN** the calibrating measurement is well under `quiet_baseline`
- **THEN** the derived budget SHALL equal `base`

#### Scenario: contended host
- **WHEN** the calibrating measurement is inflated by host contention
- **THEN** the derived budget SHALL be inflated in proportion, up to `cap`

#### Scenario: budget derivation is reported
- **WHEN** any calibrated wait fails
- **THEN** the failure SHALL report the derived budget, the `base`, the `scale`, and the measured duration the `scale` came from

### Requirement: The exit wait is progress-checked

The post-`SIGINT` wait for process exit SHALL NOT be a single opaque `wait_timeout` call. It SHALL poll
in slices and SHALL treat each of the following as evidence of progress:

* a newly observed line on the child's `stderr` or `stdout`;
* an increase in the count of durable `-Data.db` artifacts under the write directory.

#### Scenario: flush is landing slowly
- **WHEN** the child has not exited but durable artifacts are still appearing
- **THEN** the wait SHALL continue rather than fail on a stall

#### Scenario: no progress at all
- **WHEN** the child has neither exited nor produced any new output or artifact for the stall window, and the total budget is exhausted
- **THEN** the wait SHALL fail with the attribution required above

### Requirement: Every wait failure reports only what its measurement establishes

No failure message in this file SHALL assert a cause that its own measurement cannot establish. The
string `no graceful shutdown handler` SHALL NOT appear. Each such message SHALL name what was awaited,
the budget and its derivation, what was observed (including the child transcript), and — where the
measurement is genuinely ambiguous — the candidate causes without selecting one.

#### Scenario: no unestablishable cause survives in the file
- **WHEN** the file's wait-failure messages are read
- **THEN** none SHALL claim a missing handler, a dead-ended session, or an unused code path as an established fact on the strength of a timeout alone

### Requirement: The sibling threshold-flush test carries the same oracle

`writable_session_auto_flushes_mid_session_across_threshold` SHALL use the same staged, calibrated,
progress-checked waits. Specifically its per-write acknowledgement wait, its mid-session
durable-artifact wait, and its stdin-EOF exit wait SHALL each be calibrated and SHALL each report only
what they measure — replacing the present claims that the session "dead-ended" or that the interactive
loop "did not use the threshold-flushing path", neither of which a timeout establishes.

#### Scenario: sibling under contention
- **WHEN** the host is contended and the threshold-flushing path is working
- **THEN** the sibling test SHALL NOT fail on any of its three waits

### Requirement: The test owns a total budget below the harness hard-kill

Each test SHALL track its elapsed time across stages against a total budget set below nextest's
configured hard kill (`.config/nextest.toml`: `slow-timeout` period `60s`, `terminate-after` 4 = **240s**),
and SHALL emit its own attributed failure rather than being killed by the harness. Per-stage caps SHALL
be chosen so their sum cannot exceed that total budget.

#### Scenario: everything is slow
- **WHEN** stages are slow enough that the total budget is reached
- **THEN** the test SHALL fail with its own attributed message naming the stage that consumed the budget
- **AND** SHALL NOT be terminated by the harness slow-timeout instead

### Requirement: The new oracle is observed to red on a genuinely broken handler

The change SHALL be RED-verified by actually breaking the product shutdown path — not by a mocked or
simulated failure — and the observation recorded in the PR body. Two defects SHALL be exercised:

* the shutdown handler **removed** (so `SIGINT` takes its default disposition), and
* the shutdown flush **hung**.

For each, the test SHALL fail, and the reported cause SHALL be the true one.

#### Scenario: handler removed
- **WHEN** the `ctrl_c` branch of the interactive loop is removed and the test is run
- **THEN** the test SHALL fail, and SHALL do so at the handler-entry stage

#### Scenario: flush hung
- **WHEN** the shutdown flush is made to hang and the test is run
- **THEN** the test SHALL fail at the exit stage, reporting that the flush did not complete — not that a handler is missing
