# parity-report-integrity Specification

## Purpose
TBD - created by archiving change harden-parity-report-staleness. Update Purpose after archive.
## Requirements
### Requirement: A stale committed parity report cannot persist on `main` undetected and self-heals

The system SHALL detect, on push to `main`, when `docs/reports/cassandra-test-parity.md` differs from a
fresh render of the parity manifest (i.e. `cassandra-parity report ... --check` would fail on the `main`
tip), and SHALL automatically open a regeneration pull request that updates the report to match the
manifest. The healing path SHALL NOT push directly to the protected `main` branch, and SHALL NOT require a
human to manually regenerate the report to clear the red.

#### Scenario: Post-merge drift opens an automated regeneration PR
- **WHEN** a push to `main` results in a committed report that does not match a fresh render (e.g. two
  manifest-changing PRs merged in an order that left the report rendered against a stale base)
- **THEN** the parity workflow's push-to-`main` run detects the staleness via `report --check`
- **AND** it opens (or updates) a single automated pull request that regenerates
  `docs/reports/cassandra-test-parity.md` and changes no other file
- **AND** it does not push the regeneration directly to `main`

#### Scenario: The regeneration PR terminates the drift (idempotent, non-recursive)
- **WHEN** the automated regeneration PR is merged
- **THEN** `cassandra-parity report ... --check` is green on the new `main` tip
- **AND** the push-to-`main` healing job opens no further regeneration PR for that drift
- **AND** if a healing PR is already open, the job updates that one rather than opening a duplicate

### Requirement: The agent gate catches a forgotten single-PR report regeneration before push

`scripts/agent-gate.sh` SHALL include a `parity-report` component that runs the report staleness check
(`cassandra-parity report --manifest test-data/cassandra-parity-manifest.yml --output
docs/reports/cassandra-test-parity.md --check`). The component SHALL be SKIP-aware: when the
`cassandra-parity` tool or the manifest is unavailable (e.g. a minimal checkout), it SHALL report SKIP, not
FAIL. When both are present and the committed report is stale, it SHALL FAIL the gate and name the report
file.

#### Scenario: Stale report fails the local gate
- **WHEN** an author changes the manifest without regenerating the report and runs `scripts/agent-gate.sh`
- **THEN** the `parity-report` component FAILs
- **AND** the AGENT-GATE SUMMARY block lists `parity-report` as failed, naming
  `docs/reports/cassandra-test-parity.md`

#### Scenario: Component is SKIP-aware when the tool is absent
- **WHEN** the gate runs in a checkout where the `cassandra-parity` crate or the manifest is not present
- **THEN** the `parity-report` component reports SKIP
- **AND** the gate's overall pass/fail is not changed by that SKIP

#### Scenario: Component appears in the gate's component inventory
- **WHEN** `scripts/agent-gate.sh --list` is run
- **THEN** `parity-report` is listed among the components

### Requirement: The derived-artifact merge-race hazard is documented for future authors

The project SHALL document, in the parity CI doctrine (`docs/development/parity-ci-tiers.md` or the
manifest doctrine page) and in `CLAUDE.md`, that `docs/reports/cassandra-test-parity.md` is a committed
derived artifact that can drift on `main` via a semantic merge race even when each PR regenerates it
correctly, and SHALL state the mechanism that protects against it.

#### Scenario: Doctrine explains the race and the safeguard
- **WHEN** a contributor reads the parity doctrine after this change
- **THEN** it states that the report is derived from the manifest and is regenerated per PR
- **AND** it explains that two manifest-changing PRs can still leave `main`'s report stale (the merge race)
- **AND** it names the safeguard (the self-healing regeneration PR and the `parity-report` gate component)

