# cli-sweep Specification

## Purpose
TBD - created by archiving change corruption-locator. Update Purpose after archive.

## Requirements

### Requirement: S1 — The verb, its walk, and its severities

The CLI SHALL provide `cqlite sweep <data-dir> [--mode quick|full] [--out text|json] [--jobs N]`,
walking every `<keyspace>/<table>-<id>/` directory under `<data-dir>`, enumerating every `*-Data.db`
GENERATION found in each (a real table directory routinely holds several — `verify_sstable` alone
resolves only the lexicographically-first, which would silently skip the rest), verifying each
generation via `verify_sstable_generation`, and reporting exactly one row per GENERATION with
severity `ok | degraded | corrupt | unreadable` (roborev round-2 HIGH finding — corrected from an
earlier per-DIRECTORY design, whose S1.1 wording below is updated to match).

#### Scenario: S1.1 an all-healthy sweep is all-ok
- **Given** the built binary and a temp data directory holding two `<keyspace>/<table>/`
  directories, each a staged copy of the clean `test_comp.lz4_table` generation
- **When** `cqlite sweep <temp-dir> --mode full --out json` runs
- **Then** exit `0`, every row's severity is `ok`, no `ok` row carries a `cause`, and the row count
  equals the number of SSTable GENERATIONS (not table directories — a table directory with N
  generations contributes N rows; here two dirs x one generation each = 2 rows)
  (`cqlite-cli/tests/sweep_cli_tests.rs`, named in the gate's `cli-tests` list per #3522).
- **Note** this case stages its OWN fixture rather than sweeping a `CQLITE_DATASETS_ROOT` corpus
  root, as an earlier draft of this scenario specified: a fetched root is not a stable "the WHOLE
  corpus is clean" oracle — measured on at least one fleet box, several `sstables/system/*` and
  `sstables/test_deltas/*` directories are git-tracked while their `*-Data.db` binaries were never
  materialized, which is itself a CORRECT `unreadable` row and would fail an all-ok assertion for a
  reason unrelated to this verb.

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
- **Given** a temp dir holding one staged copy of the clean `test_comp.lz4_table` generation whose
  `Filter.db` is then mutated to produce a `FilterFalseNegative` — a CQLite-only detection whose
  Cassandra verdict is `clean` — by clearing the manifest-pinned bit `0x10` at byte 8 (asserted SET
  before the flip, so the mutation can never silently become a no-op)
- **When** `cqlite sweep <temp-dir> --mode full` runs
- **Then** that row's severity is `degraded`, not `corrupt`, `totals.degraded` is 1 with
  `totals.corrupt` and `totals.unreadable` both 0, and `degraded` alone does not flip the sweep's own
  exit code (§S2.1 asserts the exit `0`; §S2 states the exact exit contract).
- **Note** the mutation is applied to a CLEAN copy rather than reading a
  `test_comp_corrupt/filter_db_bit_flip` fixture, as an earlier draft specified: deriving the one
  byte in-test keeps the case independent of which corruption fixtures a given box has fetched,
  and the pre-flip assertion is what makes it an oracle rather than a guess.

#### Scenario: S1.4 a directory with no readable Data.db is a row, never an omission
- **Given** a temp dir holding a `<keyspace>/<table>/` directory with NO `*-Data.db` at all (only a
  stray non-component file)
- **When** `cqlite sweep <temp-dir>` runs
- **Then** exit `2`, and that directory still produces exactly one row, severity `unreadable`, with a
  non-empty `cause` — the row count includes it (no silent skip).
- **Note** this replaces an earlier draft's Given ("a `Data.db` present but no `TOC.txt` and no
  readable `Statistics.db`"), which does NOT reach the branch it meant to exercise: a present
  `Data.db` alone is sufficient for `resolve_components` to succeed, so that shape returns
  `Ok(report)` with `MissingComponent` findings and is correctly classified `corrupt`, not
  `unreadable` (verified directly before the test was written). A directory with no `*-Data.db` is
  the shape design.md §D3's own pseudocode names for `unreadable`.

### Requirement: S5 — A discovery entry is never silently dropped (roborev jobs 92/102 MEDIUM)

The walk SHALL NOT drop a discovery entry it cannot stat. `is_file()`/`is_dir()` collapse EVERY
stat failure into `false`, so a dangling symlink, an `EACCES`, or a race with a concurrent `mv`
previously vanished with no row, no cause and — for the directory levels — no effect on the exit
code, which is the dangerous direction because the sweep could still report success. Every level of
the walk (keyspace dir, table dir, `*-Data.db` entry) SHALL therefore probe metadata EXPLICITLY and
record an unstattable entry as an `unreadable` subject naming the path and the OS error.
`DirEntry::file_type()` SHALL NOT be used as the probe: it does not follow symlinks, so it reports a
dangling link as a symlink instead of surfacing the broken target.

#### Scenario: S5.1 an unstattable keyspace or table directory is counted, not skipped
- **Given** a path under the data dir whose metadata cannot be read
- **When** the walk reaches it
- **Then** `descendable_dir` returns `Err(cause)` naming the stat failure and the path, the caller
  increments the aggregated `unreadable_*_entries` count, and a genuine non-directory (a stray file)
  is still skipped silently — `Ok(false)`, distinct from the error case
  (`cqlite-cli/src/commands/sweep_tests.rs`'s `descendable_dir_*`).

#### Scenario: S5.2 a `*-Data.db` whose metadata cannot be read is an unreadable entry
- **Given** a table directory holding a `*-Data.db` NAME that cannot be stat'ed
- **When** `classify_table_dir_entries` classifies it
- **Then** it is NOT counted as a generation, `unreadable_file_entries` increments, and the recorded
  cause names the stat failure and the file — while a non-`*-Data.db` name is out of scope entirely
  and does NOT inflate the count. The NAME is tested BEFORE metadata, which is what makes the entry
  attributable at all (`classify_counts_an_unstattable_data_db_as_unreadable`,
  `classify_ignores_a_non_data_db_entry_entirely`).

#### Scenario: S5.3 readable generations alongside unreadable entries earn their own row
- **Given** a table directory with both readable generations and unreadable entries
- **When** the walk classifies it
- **Then** `mixed_readability_row` returns a row naming BOTH counts and the last error, and the
  readable generations are still verified — and it returns `None` when either count is zero, so the
  zero-generation case is reported by its own arm and never double-reported. PINNED BY MUTATION:
  inverting the guard FAILs these cases (`mixed_readability_row_*`).

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
resident per concurrent worker, and `--jobs` SHALL bound the number of tables verified concurrently,
CLAMPED to a fixed maximum (`MAX_JOBS = 8`) regardless of source. **Omitting `--jobs` SHALL mean `1`
— sequential, one table open at a time** (issue #4194 AC5's literal wording; sweep runs
against damaged, possibly stressed production hosts, so the default is the safest one — roborev M3,
owner ruling); an explicit `--jobs N` OPTS IN to parallelism and is clamped to `[1, MAX_JOBS]`. A
single `verify` check
(`check_digest`, Check 2, runs in QUICK mode too) reads the WHOLE `Data.db` into memory, so peak
resident memory scales with `jobs x largest Data.db`, not `O(1)` in `jobs` (roborev round-4 MEDIUM
finding — an earlier draft of this requirement asserted the per-worker bound alone, without stating
that `jobs` itself must therefore be bounded too). Every completed row's `VerifyReport.findings`
(including every `Location`) IS additionally accumulated in `rows: Vec<SweepRow>` for the duration of
the sweep, ahead of any rendering — bounded per-row by `MAX_RESOLVED_KEYS` (spec verify-location L5)
but `O(generations)` overall, not `O(1)` (roborev round-2 MEDIUM finding — a true streamed/`O(1)`
render is a separate, larger change, not attempted in this change; `execute_sweep_command`'s own doc
states this bound precisely).

#### DECLARED GAP — Scenario S3.1 (wide tables under the memory-budget lane) has NO implementing target
- **Given** `test_wide_rows` (every table) swept with `--mode full --jobs 1`
- **Intended assertion**: peak heap stays within the existing `memory-budget` gate lane's threshold
  for a single-table `verify --mode full` run.
- **Status**: NOT IMPLEMENTED in this change. Integrating a new dhat-instrumented case with the
  existing `memory-budget` gate component's conventions is a separate, non-trivial undertaking;
  tracked as a follow-up rather than attempted here. The PER-WORKER bound this scenario would verify
  (one `VerifyReport` at a time per concurrent worker) is unchanged from `verify --mode full`'s own,
  already-bounded behavior — this change does not introduce new per-worker resident structure, only
  the cross-row accumulation named above, which S3.1 as originally scoped would not have measured
  either.

#### Scenario: S3.3 omitting --jobs is sequential
- **Given** `cqlite sweep` invoked WITHOUT `--jobs`
- **Then** the effective concurrency is exactly `1` — one table open at a time, never derived from
  the host's core count — and `cqlite sweep --help` states that default; an explicit `--jobs N`
  still opts in to parallelism, clamped to `MAX_JOBS = 8`.

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
- **Then** the JSON document has a `rows` array, one entry per swept GENERATION (not per
  directory — see §S1/§S1.5), each with `path` (that generation's exact `Data.db` file),
  `severity`, `cause` (`null` when `ok`), and `findings` (the `VerifyReport.findings` shape,
  `location` included when present), plus a `totals` object counting each severity —
  `totals.<severity>` is always present even at `0`, never omitted for an unreached severity
  (affirmative-zero doctrine).

#### Scenario: S4.2 text rendering matches the JSON rows
- **Given** the same sweep run rendered as `--out text`
- **Then** every row appears with its generation `path` and severity, and every
  `corrupt`/`unreadable`/
  `degraded` row's cause/finding summary is present in the text output — text and JSON never
  disagree on which rows exist or their severities.
