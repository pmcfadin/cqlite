# cli-export-progress Specification

## Purpose
TBD - created by archiving change export-progress-stats. Update Purpose after archive.
## Requirements
### Requirement: Determinate progress bar with percent and ETA when a total is known

The CLI SHALL render a determinate progress bar — showing percent complete, current position
over total (`pos/len`), and an estimated time remaining (ETA) updated as rows are written —
when the `export` command runs interactively (not `--quiet`, stdout is a TTY) and the total
number of rows to export is known.

The total is considered **known if and only if** the user supplies an explicit `--limit N`;
in that case the total is `N`. The CLI SHALL NOT infer a total from data shape, SSTable
metadata, or a pre-count pass.

#### Scenario: Determinate bar shown for a limited interactive export
- **WHEN** `export` runs with `--limit N`, without `--quiet`, and stdout is a TTY
- **THEN** the progress display is a determinate bar showing percent, `pos/N`, and an ETA
- **AND** the position advances toward `N` as chunks are written

#### Scenario: ETA is rendered only when the total is known
- **WHEN** the export has a known total (`--limit N` on a TTY, not `--quiet`)
- **THEN** the progress display includes an ETA field
- **WHEN** the export has no known total
- **THEN** no ETA is displayed

### Requirement: Indeterminate spinner when the total is unknown

The CLI SHALL render an indeterminate spinner with a live exported-row count — and SHALL NOT
display a percent, a `pos/len` ratio, or an ETA — when the `export` command runs interactively
and the total row count is not known (no `--limit`, or a `WHERE`-filtered / raw `SELECT`
source).

#### Scenario: Spinner shown for an unbounded interactive export
- **WHEN** `export` runs without `--limit`, without `--quiet`, and stdout is a TTY
- **THEN** the progress display is a spinner with a live row count
- **AND** no percent and no ETA are shown

### Requirement: Final export statistics summary

On successful completion of an interactive export, the CLI SHALL print a final summary that
includes the number of rows exported, the output file size in bytes (human-readable), the
elapsed duration, and the throughput rate in rows per second.

#### Scenario: Summary printed after a successful interactive export
- **WHEN** an `export` completes without `--quiet` and stdout is a TTY
- **THEN** stdout contains a summary reporting rows, size, time, and rate

### Requirement: Suppress all progress and statistics for quiet or non-TTY output

The CLI SHALL emit no progress display and no statistics summary when `--quiet` is set or
when stdout is not a terminal (piped or redirected). In those cases the only product of the
command is the export file itself.

#### Scenario: Quiet export emits no progress or summary
- **WHEN** `export` runs with `--quiet`
- **THEN** stdout contains no progress output and no summary
- **AND** the export file is still written

#### Scenario: Piped export emits no progress or summary
- **WHEN** `export` runs without `--quiet` but stdout is not a TTY (piped/redirected)
- **THEN** stdout contains no progress output and no summary
- **AND** the export file is still written

### Requirement: Total-resolution and ETA-eligibility are independently verifiable

The decision of whether an export has a known total (and is therefore ETA-eligible) SHALL be
encapsulated so it can be verified without a terminal: given the CLI `--limit`, it resolves
to a determinate total when a limit is present and to an unknown total (spinner, no ETA)
when absent.

#### Scenario: Limit present resolves to a determinate total
- **WHEN** the total-resolution logic is given `--limit = Some(n)`
- **THEN** it resolves the progress total to `Some(n)` and marks the export ETA-eligible

#### Scenario: Limit absent resolves to an unknown total
- **WHEN** the total-resolution logic is given `--limit = None`
- **THEN** it resolves the progress total to `None` and marks the export not ETA-eligible

