# cli-diagnose — delta (generation-discovery, issue #4626)

`diagnose` lists generations through the `generation-discovery` module and reads each generation's
Statistics through its trusted-metadata accessor. R6 and R7 are unchanged. All requirements are ADDED.

## ADDED Requirements

### Requirement: GD8 — Conditions map to diagnose's existing vocabulary, never to a refusal

`diagnose` SHALL list the table directory with Scope Live and TocMissing accepted. A generation whose
trusted metadata is `Corrupt`, `Unreadable` or `UnsupportedVersion` SHALL be an entry in
`unreadable_generations` (`{data_path, cause}`) with the condition named in `cause`, per R6, and SHALL
NOT make the run exit non-zero.

#### Scenario: GD8.1 TocMissing reported, exit 0
- **Rule detail and Cassandra reference**: A `TocMissing` generation SHALL be reported in `generations[]` with a `conditions` array containing `"TocMissing"`. An `Orphan` SHALL appear in `unreadable_generations` with cause `Orphan`. A `TxnInconsistent` directory SHALL exit `1` (usage error: the directory cannot be resolved to a table state) naming the log.
- **Given** `nb-1-big-TOC.txt` deleted from a `test_basic` copy
- **When** `cqlite diagnose <table-dir> --out json` runs
- **Then** exit `0` and `generations[]` carries `nb-1-big` with `conditions: ["TocMissing"]`.

#### Scenario: GD8.2 below-floor generation is listed, not dropped
- **Given** a directory with `nb-2-big-*` and `ma-1-big-Data.db`
- **When** `diagnose --out json` runs
- **Then** `unreadable_generations` has an entry for `ma-1-big` whose `cause` names `UnsupportedVersion`.
