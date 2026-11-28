# core-diagnose — delta (generation-discovery, issue #4626)

The cheap-tier Statistics read in `core-diagnose` goes through the trusted-metadata accessor of
`generation-discovery`. R1 to R5 are unchanged. All requirements are ADDED.

## ADDED Requirements

### Requirement: GD9 — The cheap tier reads Statistics only through trusted metadata

The cheap tier SHALL obtain every `Statistics.db` field through the module's accessor (G11), so a
second consumer in the same run (the `--deep` tier, R4's overlap) reuses the one read and parse. No
`Statistics.db` read or lenient parse SHALL remain in the diagnose code. R1's "every reported number
states its source" is unchanged: the source remains `Statistics.db`.

#### Scenario: GD9.1 one read per generation per run
- **Given** a three-generation table and a counting file-open shim
- **When** `diagnose --deep` runs
- **Then** each `*-Statistics.db` is opened exactly once.
