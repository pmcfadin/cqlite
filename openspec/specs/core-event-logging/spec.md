# core-event-logging Specification

## Purpose
TBD - created by archiving change logging-facade-tracing. Update Purpose after archive.
## Requirements
### Requirement: One event facade — `tracing`

`cqlite-core` SHALL emit all runtime log **events** through the `tracing` facade
only. No `log::{warn,info,debug,error,trace}!` event macro SHALL remain in
`cqlite-core/src`. This is enforced structurally so the two-facade mix cannot
reappear.

#### Scenario: No `log` event macros remain in core

- **WHEN** `cqlite-core/src` is searched for `log::{warn,info,debug,error,trace}!`
  event macros (word-boundary matched, so unrelated identifiers such as
  `catalog::` do not count)
- **THEN** the count is exactly zero
- **AND** a committed grep-guard test asserts this and FAILS on the pre-migration
  tree (proving the migration actually happened, not just that the test is lax).

#### Scenario: Spans and message content are untouched

- **WHEN** the migration is applied
- **THEN** existing `tracing` spans are unchanged, and every migrated call keeps
  its message and structured fields byte-identical — only the macro path changes
  from `log::` to `tracing::` (facade-only; message content is owned by AG5 #1694).

### Requirement: Corruption events reach a bridge-less tracing subscriber

Core events SHALL reach a `tracing` subscriber even when no `tracing-log`
`LogTracer` bridge is installed. Every `cqlite-core` event — including the
issue-#586 "never silently swallow" corruption warning — SHALL be delivered to a
consumer that installs only a `tracing` subscriber, with no bridge required.

#### Scenario: #586 corruption warning captured with no LogTracer

- **GIVEN** a `tracing`-only subscriber is installed as the default, with NO
  `LogTracer`/`tracing-log` bridge wired
- **WHEN** the code path that emits the issue-#586-class corruption warning runs
- **THEN** the subscriber receives the warning event (correct level and message)
- **AND** this capture test FAILS on `main` (where the `log`-facade event is
  silently dropped without a bridge) and passes after the migration.

#### Scenario: Event levels are preserved

- **WHEN** an event previously emitted at a given `log` level (`warn`/`info`/
  `debug`/`error`/`trace`) is migrated
- **THEN** it is emitted at the identical `tracing` level, so no diagnostic is
  silently up- or down-graded by the migration.

### Requirement: `log` dependency removed or justified

Once no event site remains, the `log` crate SHALL be removed from
`cqlite-core`'s dependencies, OR its continued presence SHALL be explicitly
justified in the change if a transitive requirement prevents removal.

#### Scenario: `log` dep dropped and build is clean

- **WHEN** the migration is complete and `log` is removed from `cqlite-core`'s
  `Cargo.toml`
- **THEN** `cqlite-core` builds and the full agent-gate passes with no reference
  to the `log` crate from core source
- **OR** if `log` cannot be removed, the change documents the exact transitive
  reason and confirms no first-party event site depends on it.

