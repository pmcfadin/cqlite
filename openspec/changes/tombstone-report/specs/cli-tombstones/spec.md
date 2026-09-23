# cli-tombstones — new capability (tombstone-report, issue #4200 slice 1)

`cqlite tombstones` SHALL expose the tombstone density/coverage scan from the binary, read-only,
with `--now` stated first in every output. All requirements are ADDED.

## ADDED Requirements

### Requirement: R5 — The verb exists, resolves like `query`/`explain`, states its clock first

The CLI SHALL expose `cqlite tombstones <keyspace.table | table-dir> [--schema <path>] [--now <ts>]
[--top N] [--out text|json]`, resolving the table through the same `--schema`/`--data-dir`/
`--dataset` globals `query`/`explain` use.

#### Scenario: R5.1 first line states the clock and generations
- **Given** the built `cqlite` binary, `--dataset test_tomb`, table `wide_range_tombstone`
- **When** `cqlite tombstones test_tomb.wide_range_tombstone --now 1757400000` runs
- **Then** the first line of output states `now=1757400000 (<RFC3339>)` and the generation set,
  exit code `0` (`cqlite-cli/tests/tombstones_cli_tests.rs`, named in the gate's `cli-tests` target
  list).

#### Scenario: R5.2 `--now` absent is wall clock and says so
- **When** the same command runs without `--now`
- **Then** the first line carries the wall-clock value and the suffix `(wall clock)`, matching
  `explain`'s R5.2 convention exactly; tests never assert on its value.

### Requirement: R6 — Output is machine-readable and matches design.md §D3 exactly

`--out json` SHALL produce the design.md §D3 shape; `--out text` (default) SHALL render the same
data as human-readable text, `--top N` bounding the `range_tombstones` listing with an affirmative
`range_tombstones_truncated` count when the cap engages.

#### Scenario: R6.1 json shape matches §D3
- **When** `cqlite tombstones test_tomb.wide_range_tombstone --out json` runs
- **Then** stdout after the first line is one JSON object with exactly the keys `now`, `table`,
  `generations`, `gc_grace_seconds`, `totals`, `by_generation`, `range_tombstones`,
  `range_tombstones_truncated`, deep-equal (volatile `now` normalised) to a committed expected file.

#### Scenario: R6.2 `--top N` truncates affirmatively, never silently
- **Given** a synthetic staged table with more range tombstones than `--top 2` allows
- **When** the command runs with `--top 2`
- **Then** exactly 2 entries render in `range_tombstones`, ordered widest-shadow-first, and
  `range_tombstones_truncated` equals the true remaining count (never `0` when entries were cut).

### Requirement: R7 — Never opens the input for writing

The `tombstones` verb SHALL have no output-path argument at all — no flag exists that could cause it
to write to the table directory or anywhere else.

#### Scenario: R7.1 no write-path flag exists
- **When** `cqlite tombstones --help` runs
- **Then** the listed flags are exactly `--schema`, `--now`, `--top`, `--out` (the RENDER-format
  selector, `text|json` — distinct from a write path) and the global resolution flags `query` uses;
  none names a filesystem output location for table data.

#### Scenario: R7.2 input bytes are unchanged after a run
- **Given** a staged copy of `test_tomb/wide_range_tombstone`
- **When** `cqlite tombstones <staged-dir> --now <ts> --out json` runs
- **Then** every file under the staged directory is byte-identical before and after
  (`cqlite-cli/tests/tombstones_cli_tests.rs::input_unchanged_after_run`).

### Requirement: R8 — Fail closed on an unreadable generation

`tombstones` SHALL exit non-zero naming the generation when any generation cannot be read, and MUST
NOT emit a partial or silently-narrowed report in its place.

#### Scenario: R8.1 unreadable generation
- **Given** a temp table dir with one generation's `Statistics.db` truncated
- **When** `tombstones` runs
- **Then** exit code is `2`, stderr names the generation's path, and stdout carries no totals line.
