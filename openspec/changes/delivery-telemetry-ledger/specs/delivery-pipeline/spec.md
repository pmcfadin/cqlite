## ADDED Requirements

### Requirement: Delivery telemetry ledger
The workflow SHALL maintain an append-only telemetry ledger at
`docs/reports/delivery-telemetry.jsonl` (one JSON record per line, one record per completed
issue) governed by a versioned JSON Schema at `docs/reports/delivery-telemetry.schema.json`.
A telemetry tool (`scripts/delivery-telemetry.py`) SHALL provide a `record` subcommand that
builds a schema-valid record and appends exactly one line, and a `lint` (alias `validate`)
subcommand that schema-validates every line and exits non-zero naming any malformed line.

#### Scenario: Record subcommand appends one schema-valid line
- **WHEN** `delivery-telemetry.py record` is run for a completed issue (GitHub-derived fields supplied via `--from-json` in tests, or pulled live from `gh`) with the required run counters
- **THEN** it appends exactly one JSON line to the ledger that validates against `delivery-telemetry.schema.json`
- **AND** the record carries the issue/PR numbers, routing, priority, the GitHub timestamps, the durations computed from those timestamps, and the supplied counters

#### Scenario: Lint rejects a malformed record
- **WHEN** `delivery-telemetry.py lint` runs against a ledger containing a line that violates the schema
- **THEN** it exits non-zero and names the offending line number
- **AND** a ledger whose lines all conform exits zero

### Requirement: Telemetry is authoritative data only
The ledger SHALL record only observed events — GitHub-sourced timestamps/labels and
run-observed counters explicitly supplied by the stamping step — and SHALL NOT infer,
estimate, or guess any value. Durations computed by arithmetic over authoritative timestamps
are permitted; a counter that was not observed SHALL NOT be defaulted to a fabricated value.

#### Scenario: Missing required counter is an error, not a silent zero
- **WHEN** `delivery-telemetry.py record` is invoked without a required run counter
- **THEN** it fails with an error rather than writing a record with an invented count
- **AND** every numeric field in a written record traces to a supplied counter or to arithmetic over GitHub-sourced timestamps

### Requirement: Finalize stamps the ledger
`flow-finalize` SHALL, as a step on a merged issue, write the issue's telemetry record by
invoking the `record` subcommand, so that every issue completed through the pipeline produces
exactly one ledger record.

#### Scenario: Finalize produces one record per completed issue
- **WHEN** `flow-finalize` completes for a merged issue
- **THEN** the ledger gains exactly one new record for that issue
- **AND** that record passes `lint`

### Requirement: Recurring retro ranks failures and files a deduped improvement issue
The workflow SHALL provide a `retro` subcommand that reads the ledger and the open
`flow-meta` issues, ranks the recorded failure categories by total recorded occurrences
weighted by a documented fixed weight table (a deterministic tally, not an inferred model),
and reports the single highest-cost recurring failure. By default it SHALL dry-run print the
ranked summary; with an explicit flag it SHALL file a `flow-meta` improvement issue, skipping
the filing when a matching open `flow-meta` issue already exists (dedupe). The manager
doctrine SHALL run this step on a cadence.

#### Scenario: Retro ranks a fixture ledger to the expected top failure
- **WHEN** `delivery-telemetry.py retro` runs against a fixture ledger whose dominant recorded failure category is known
- **THEN** it prints a ranked summary whose top entry is that category
- **AND** in the default mode it does not create any GitHub issue (dry-run)

#### Scenario: Retro dedupes against an existing flow-meta issue
- **WHEN** retro would file an improvement issue for a category that already has a matching open `flow-meta` issue (matched by a stable category marker)
- **THEN** it skips filing and reports that the category is already tracked

### Requirement: Telemetry tool is covered by a gate component
`scripts/agent-gate.sh` SHALL include a SKIP-aware `delivery-telemetry` component that runs
the telemetry tool's unit tests. The component SHALL record SKIP (loudly, never silent PASS)
when no `python3` is available and FAIL on any test failure.

#### Scenario: Gate runs the telemetry tests
- **WHEN** `scripts/agent-gate.sh --only delivery-telemetry` runs with `python3` available
- **THEN** it executes the telemetry unit tests and reports PASS only if they all pass
- **AND** with no `python3` it reports SKIP rather than PASS
