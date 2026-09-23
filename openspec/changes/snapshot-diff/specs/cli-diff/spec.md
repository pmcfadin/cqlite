# cli-diff — new capability (snapshot-diff, issue #4201)

`cqlite diff` SHALL expose the two-source diff scan from the binary, read-only, with `--now` stated
first in every output, resolving each side exactly the way `query`/`explain`/`tombstones` resolve a
single `--data-dir`. All requirements are ADDED.

## ADDED Requirements

### Requirement: R9 — The verb exists, resolves each side independently, states its clock first

The CLI SHALL expose `cqlite diff <snapshot-a-dir> <snapshot-b-dir> --table <ks.table> [--schema
<path>] [--partition <key>]... [--now <ts>] [--out text|json]`. Each of `<snapshot-a-dir>` and
`<snapshot-b-dir>` SHALL be resolved as a flat table directory of generations — the same shape
`--data-dir` already resolves for `query`/`explain`/`tombstones` (proposal.md open decision 1,
adopted for this slice). `--schema` resolves the table exactly as those verbs do.

#### Scenario: R9.1 first line states the clock and both sides' generations
- **Given** the built `cqlite` binary and `test_tomb/resurrection_gc_positive` staged as two
  directories per design.md §D6
- **When** `cqlite diff <a-dir> <b-dir> --table test_tomb.resurrection_gc_positive --now 1783205955`
  runs
- **Then** the first line states `now=1783205955 (<RFC3339>)`, `side_a: generations=2 [nb-1-big,
  nb-2-big]`, `side_b: generations=1 [nb-1-big]`, exit code `0`
  (`cqlite-cli/tests/diff_cli_tests.rs`, named in the gate's `cli-tests` target list).

#### Scenario: R9.2 `--now` absent is wall clock and says so
- **When** the same command runs without `--now`
- **Then** the first line carries the wall-clock value and the suffix `(wall clock)`, matching
  `explain`/`tombstones`' convention exactly; tests never assert on its value.

### Requirement: R10 — Output matches design.md §D4 exactly and is machine-readable

`--out json` SHALL produce the design.md §D4 shape; `--out text` (default) SHALL render the same
data as human-readable text, one finding per line, `summary` last.

#### Scenario: R10.1 json shape matches §D4
- **When** `cqlite diff <a-dir> <b-dir> --table test_tomb.resurrection_gc_positive --now 1783205955
  --out json` runs
- **Then** stdout after the first line is one JSON object with exactly the keys `now`, `table`,
  `gc_grace_seconds`, `side_a`, `side_b`, `partitions`, `summary`, deep-equal (volatile fields
  normalised) to a committed expected file.

#### Scenario: R10.2 identical snapshots render an affirmative, non-vacuous report
- **Given** the same table staged identically as both sides
- **When** the command runs
- **Then** exit code `0`, every cell `equal`, `summary.only_a == 0`, `summary.only_b == 0`, and
  `summary.partitions_compared` names the true count — never a report indistinguishable from "the
  scan did not run" (issue AC 2).

### Requirement: R11 — `--partition` restricts the scan through the point-read path

When `--partition <key>` is given, `diff` SHALL resolve and compare only that partition through the
point-read merger on both sides (design.md §D3), never the full enumeration path. Without it, `diff`
SHALL stream a full merge of both sides under the same `max_result_bytes` budget the query surface
honors.

#### Scenario: R11.1 `--partition` scopes to one partition
- **Given** the §D6 fixture
- **When** `cqlite diff <a-dir> <b-dir> --table test_tomb.resurrection_gc_positive --partition 2
  --now 1783205955` runs
- **Then** `partitions[]` contains exactly partition `2`'s findings (the partition tombstone and its
  6 shadowed cells), and no data from partition `1` is read or reported.

#### Scenario: R11.2 full-table diff respects the byte budget
- **Given** `test_wide_rows` staged with a deliberate divergence, and `max_result_bytes` set below
  the full report's rendered size
- **When** `cqlite diff <a-dir> <b-dir> --table <ks.tbl> --out json` runs without `--partition`
- **Then** it terminates with the same budget-exceeded error the query surface emits, never OOM, and
  never a truncated-but-`0`-exit report.

### Requirement: R12 — Exit codes and read-only guarantee

`diff` SHALL exit `0` when the report renders (including when divergence or resurrection risk is
found — a finding is not a failure), `1` on usage errors (a directory that does not resolve to a
table, `--table` naming a table absent from `--schema`), and `2` naming the side and generation when
a generation cannot be read on either side. `diff` MUST NOT write to either input directory.

#### Scenario: R12.1 unreadable generation on either side
- **Given** a temp copy of the §D6 fixture with side B's `nb-1-big-Statistics.db` truncated
- **When** `diff` runs
- **Then** exit code is `2`, stderr states `side=b` and the truncated path, and stdout carries no
  partition findings.

#### Scenario: R12.2 usage error
- **Given** `<snapshot-a-dir>` pointing at a directory with no `Data.db` for the named table
- **When** `diff` runs
- **Then** exit code is `1`, stderr names the cause, and nothing is written anywhere.

#### Scenario: R12.3 read-only on both sides
- **Given** staged copies of both directories and their recursive sha256 listings
- **When** `diff` runs against them (all formats, with and without `--partition`)
- **Then** every file under both staged directories is byte-identical before and after
  (`cqlite-cli/tests/diff_cli_tests.rs::inputs_unchanged_after_run`).

### Requirement: R13 — Symmetry is observable from the binary

Running `diff <a-dir> <b-dir>` and `diff <b-dir> <a-dir>` SHALL produce label-swapped mirror reports
(design.md §D5 / core R6), observable end-to-end through the CLI, not just the library.

#### Scenario: R13.1 CLI-level mirror
- **Given** the §D6 fixture
- **When** both `cqlite diff <a-dir> <b-dir> --table ... --now 1783205955 --out json` and
  `cqlite diff <b-dir> <a-dir> --table ... --now 1783205955 --out json` run
- **Then** relabeling one output's `a`/`b` tags (per core R6's rule) and comparing to the other
  yields structural equality.
