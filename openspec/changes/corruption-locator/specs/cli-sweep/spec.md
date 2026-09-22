# cli-sweep — new capability (corruption-locator, issue #4194)

`cqlite sweep` SHALL verify every SSTable GENERATION under a data directory in one pass, name a
severity and cause for each, and exit non-zero when anything short of fully healthy is found. All
requirements are ADDED.

## ADDED Requirements

### Requirement: S1 — The verb, its walk, and its severities

The CLI SHALL provide `cqlite sweep <data-dir> [--mode quick|full] [--out text|json] [--jobs N]`,
walking every `<keyspace>/<table>-<id>/` directory under `<data-dir>`, enumerating every `*-Data.db`
GENERATION found in each (a real table directory routinely holds several — `verify_sstable` alone
resolves only the lexicographically-first, which would silently skip the rest), verifying each
generation via `verify_sstable_generation`, and reporting exactly one row per GENERATION with
severity `ok | degraded | corrupt | unreadable` (roborev round-2 HIGH finding — corrected from an
earlier per-DIRECTORY design, whose S1.1 wording below is updated to match).

#### Scenario: S1.1 sweep over the whole committed corpus is all-ok
- **Given** the built binary and a `CQLITE_DATASETS_ROOT`-resolved corpus root holding every
  committed table
- **When** `cqlite sweep <root> --mode quick --out json` runs
- **Then** exit `0`, every row's severity is `ok`, and the row count equals the number of SSTable
  GENERATIONS (not table directories — a table directory with N generations contributes N rows)
  under the root (`cqlite-cli/tests/sweep_cli_tests.rs`, named in the gate's `cli-tests` list per
  #3522).

#### Scenario: S1.5 a table directory with multiple generations reports one row PER generation (roborev round-2 HIGH finding)
- **Given** a temp dir containing a table directory with TWO SSTable generations (distinct base
  names, e.g. `nb-1-big-*` and `nb-2-big-*`), only the second corrupted
- **When** `cqlite sweep <temp-dir> --mode full` runs
- **Then** exit `2`, there are exactly two rows for that table directory (one per generation), the
  first-generation row is `ok`, and the second-generation row is `corrupt` — never silently reporting
  only the lexicographically-first generation's verdict for the whole directory.

#### Scenario: S1.2 one corrupted copy makes exactly that row corrupt
- **Given** a temp dir containing a copy of a healthy table plus
  `test_comp_corrupt/data_db_bit_flip`'s corrupted copy as a second table directory
- **When** `cqlite sweep <temp-dir> --mode full` runs
- **Then** exit `2`, the corrupted table's row has severity `corrupt` and names its
  `ChunkDecompressionError` finding, and the healthy table's row is `ok`.

#### Scenario: S1.3 a Filter.db-only finding is degraded, not corrupt
- **Given** a temp dir holding `test_comp_corrupt/filter_db_bit_flip` (the one fixture whose only
  finding is `FilterFalseNegative`, a CQLite-only detection with Cassandra verdict `clean`)
- **When** `cqlite sweep <temp-dir> --mode full` runs
- **Then** that row's severity is `degraded`, not `corrupt`, and the row still contributes to a
  non-zero data-dir exit code only via the `corrupt`/`unreadable` classes — `degraded` alone does not
  flip the sweep's own exit code (§S1.4 states the exact exit contract).

#### Scenario: S1.4 a directory with no readable Data.db is a row, never an omission
- **Given** a temp dir with a `Data.db` file present but no `TOC.txt` and no readable
  `Statistics.db`
- **When** `cqlite sweep <temp-dir>` runs
- **Then** exit `2`, and the row for that directory has severity `unreadable` and names the cause —
  the row count still includes it (no silent skip).

### Requirement: S2 — Exit code is a closed function of the row severities

Sweep SHALL exit `0` when every row is `ok`, exit `2` when any row is `corrupt` or `unreadable`
(regardless of how many rows are merely `degraded`) OR when zero rows were discovered at all, and
exit `1` on a usage error (e.g. `<data-dir>` does not exist, or is not a directory).

#### Scenario: S2.1 all-degraded-no-worse still exits non-zero-free of corrupt/unreadable
- **Given** a temp dir whose only table directory is `test_comp_corrupt/filter_db_bit_flip`
- **When** `cqlite sweep <temp-dir>` runs
- **Then** exit `0` — `degraded` alone (no `corrupt`/`unreadable` row) does not trip the failing exit
  code, matching the design's "worth attention, not proof of unreadability" rationale (design.md §D3).

#### Scenario: S2.2 usage error on a missing data dir
- **When** `cqlite sweep /does/not/exist` runs
- **Then** exit `1`, stderr names the missing path, and no verification is attempted.

#### Scenario: S2.3 zero rows discovered is its own non-zero exit (roborev round-2 MEDIUM finding)
- **Given** a temp dir that exists, is a directory, and holds zero `<keyspace>/<table>-<id>/`
  subdirectories carrying at least one `*-Data.db` generation (either genuinely empty, or the caller
  pointed `sweep` one level too high, e.g. directly at a table directory instead of its data root)
- **When** `cqlite sweep <temp-dir>` runs
- **Then** exit `2`, stderr names the "no table directories found" condition, and stdout carries no
  report — a sweep that verified nothing must not print a report claiming a clean bill of health, and
  must not be indistinguishable from an all-healthy corpus (affirmative-zero doctrine).

### Requirement: S3 — Sweep memory stays bounded

Sweep SHALL hold at most one table's `VerifyReport` (and the FULL-mode scan behind it) fully
resident per concurrent worker, and `--jobs` SHALL bound the number of tables verified concurrently.

#### Scenario: S3.1 wide tables under the memory-budget lane
- **Given** `test_wide_rows` (every table) swept with `--mode full --jobs 1`
- **Then** peak heap stays within the existing `memory-budget` gate lane's threshold for a
  single-table `verify --mode full` run — sweep with `--jobs 1` introduces no additional resident
  structure beyond what running `verify` once per table already holds.

#### Scenario: S3.2 --jobs bounds concurrency, not correctness
- **Given** a temp dir with N ≥ 4 table directories (a mix of healthy and the S1.2 corrupted pair)
- **When** sweep runs once with `--jobs 1` and once with `--jobs 4`
- **Then** both runs produce the identical set of `(directory, severity, cause)` rows and the same
  exit code — concurrency bounds throughput, never which rows appear or their severities.

### Requirement: S4 — The manifest is the contract; text is a rendering of the same rows

The JSON sweep report SHALL list every row with its directory, severity, cause (when not `ok`), and
the underlying `VerifyReport` findings (including `location` per the `verify-location` capability);
the text rendering MUST be derived from the same rows.

#### Scenario: S4.1 JSON report shape
- **Given** any sweep run
- **When** `--out json` is used
- **Then** the JSON document has a `rows` array, one entry per swept directory, each with `path`,
  `severity`, `cause` (`null` when `ok`), and `findings` (the `VerifyReport.findings` shape,
  `location` included when present), plus a `totals` object counting each severity —
  `totals.<severity>` is always present even at `0`, never omitted for an unreached severity
  (affirmative-zero doctrine).

#### Scenario: S4.2 text rendering matches the JSON rows
- **Given** the same sweep run rendered as `--out text`
- **Then** every row appears with its directory and severity, and every `corrupt`/`unreadable`/
  `degraded` row's cause/finding summary is present in the text output — text and JSON never
  disagree on which rows exist or their severities.
