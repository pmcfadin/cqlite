# verify-audit — delta (generation-discovery, issue #4626)

`verify` (all modes) resolves a generation's components and its Statistics through the
`generation-discovery` module. A0 to A12 and the Quick/Full behaviour are unchanged. All requirements
are ADDED.

## ADDED Requirements

### Requirement: GD11 — verify classifies through the module's typed outcomes and keeps its own verdicts

`verify` SHALL obtain a generation's component set and `Statistics.db` content through the module.
`verify` classifies damage itself (verdict rows and the parity guard), so it SHALL map the module's
typed outcomes to its existing verdicts and SHALL NOT change any verdict: `TocMissing` is the existing
A5 missing-`TOC.txt` finding; `Corrupt` is the existing Statistics integrity finding; `UnsupportedVersion`
is a refusal naming the floor; `Orphan` is reported, not verified as live. A directory input SHALL be
Scope Live, and verify SHALL NOT enter `snapshots/` or `backups/` unless the caller names that Scope.

#### Scenario: GD11.1 verdicts are unchanged on the corrupted-component corpus
- **Given** the `sstable-verify-parity` Cassandra-written corrupted-component fixtures
- **When** `verify` runs after the migration
- **Then** every per-fixture verdict equals the verdict recorded before the migration.

#### Scenario: GD11.2 below-floor is a refusal
- **Given** `ma-1-big-Data.db`
- **When** `verify` runs
- **Then** it refuses with `UnsupportedVersion` and opens no component.
