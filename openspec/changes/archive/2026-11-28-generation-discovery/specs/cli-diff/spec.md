# cli-diff — delta (generation-discovery, issue #4626)

Each side of `diff` is resolved through the `generation-discovery` module. R9 to R13 are unchanged.
All requirements are ADDED.

## ADDED Requirements

### Requirement: GD6 — Each side is listed by the module with Scope Live, and refusals name the side

`diff` SHALL list `<snapshot-a-dir>` and `<snapshot-b-dir>` independently with Scope Live and
TocMissing accepted. R9's "flat table directory" becomes: exactly the module's Scope Live. A condition
that refuses on one side (below-floor, `Corrupt`, `Unreadable`, `TxnInconsistent`) SHALL exit `2` with
`side=a` or `side=b`, as R12 states for an unreadable generation.

#### Scenario: GD6.1 TocMissing on side B is visible
- **Rule detail and Cassandra reference**: When a side is a snapshot directory (a path that is itself `<table-dir>/snapshots/<tag>`), its generations are its own Live set.
- **Given** side B's `nb-1-big-TOC.txt` deleted
- **When** `diff` runs
- **Then** the first line reads `side_b: generations=1 ["nb-1-big (toc-missing)"]` (the condition is shown; a name containing a space is quoted) and the exit code is `0`.

#### Scenario: GD6.2 below-floor on side A
- **Given** side A holds `ma-1-big-Data.db`
- **When** `diff` runs
- **Then** exit `2`, stderr states `side=a` and `UnsupportedVersion`, stdout carries no partition findings.
