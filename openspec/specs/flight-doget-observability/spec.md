# flight-doget-observability Specification

## Purpose
TBD - created by archiving change doget-error-taxonomy. Update Purpose after archive.
## Requirements
### Requirement: do_get aborts carry an authoritative fine-grained abort category
Every server-side do_get failure SHALL be attributed to exactly one closed-set abort reason on the
`cqlite.errors.total` counter via a bounded attribute `cqlite.flight.abort_reason`, whose value is
stamped at the abort construction site from authoritative local knowledge. No known abort path SHALL
land in an unattributed/`other` bucket, and the reason SHALL NOT be inferred from the gRPC status
code or the error message text.

The closed value set is: `superseded_split`, `client_cancel`, `admission_shed`, `snapshot_retired`,
`internal`, `ticket_invalid`.

#### Scenario: client disconnect is attributed to client_cancel
- **GIVEN** a do_get stream is in progress over the public Flight service surface
- **WHEN** the client drops the stream before completion
- **THEN** `cqlite.errors.total` increments once with `cqlite.subsystem = "flight"` and
  `cqlite.flight.abort_reason = "client_cancel"`
- **AND** the increment is NOT attributed to `internal`.

#### Scenario: admission shed is attributed to admission_shed
- **GIVEN** the service is at `--max-concurrent-scans` capacity
- **WHEN** a further do_get is rejected by admission control
- **THEN** `cqlite.errors.total` increments once with `cqlite.flight.abort_reason = "admission_shed"`.

#### Scenario: a superseded/retired snapshot is attributed to a teardown reason, never internal
- **GIVEN** a do_get resolves a snapshot generation
- **WHEN** that split/snapshot is torn down or retired under the streaming reader
- **THEN** `cqlite.errors.total` increments once with
  `cqlite.flight.abort_reason ∈ {"superseded_split", "snapshot_retired"}`
- **AND** the increment is NOT attributed to `internal`.

#### Scenario: a genuine internal fault is attributed to internal
- **GIVEN** a do_get whose merge/convert/predicate/discovery step raises a genuine fault (or panics)
- **WHEN** the do_get aborts
- **THEN** `cqlite.errors.total` increments once with `cqlite.flight.abort_reason = "internal"`.

#### Scenario: classification is not derived from status code or message
- **GIVEN** two distinct abort paths that surface the same gRPC code (client-disconnect and
  cooperative merge-cancel both surface `Aborted`)
- **WHEN** each aborts
- **THEN** each is attributed by its site-stamped reason
- **AND** no code path parses the error message text to choose the reason.

### Requirement: do_get abort path emits a structured debug event with attribution context
The do_get abort path SHALL emit a structured log/trace event carrying the `abort_reason`, the
ticket/split identity, and the snapshot generation, at a level appropriate to the reason (benign
teardown/cancel/shed at debug, genuine internal fault at error, client-fault ticket at warn). The
high-cardinality context (ticket/split identity, snapshot generation) SHALL appear only on the
event/span, never as a metric label.

#### Scenario: benign abort logs at debug with attribution context
- **GIVEN** a do_get aborts for a benign reason (`client_cancel`, `superseded_split`,
  `snapshot_retired`, or `admission_shed`)
- **WHEN** the abort is recorded
- **THEN** a structured event is emitted at `debug` level carrying `cqlite.flight.abort_reason`, the
  ticket/split identity, and the snapshot generation
- **AND** the ticket/split identity and snapshot generation do NOT appear as `cqlite.errors.total`
  metric labels.

#### Scenario: genuine internal fault logs at error
- **GIVEN** a do_get aborts with `abort_reason = "internal"`
- **WHEN** the abort is recorded
- **THEN** a structured event is emitted at `error` level carrying the same attribution context.

### Requirement: the abort_reason attribute is bounded-cardinality
`cqlite.flight.abort_reason` SHALL be a closed value set enforced by the bounded-attribute test so a
future unbounded value cannot be introduced.

#### Scenario: abort_reason is in the bounded-key allowlist
- **GIVEN** the metrics capture bounded-attribute assertion runs
- **WHEN** it validates emitted attributes against the bounded-key allowlist
- **THEN** `cqlite.flight.abort_reason` is in the allowlist
- **AND** only closed-set values are ever observed for it.

