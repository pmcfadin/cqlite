# cli-scrub — new capability (sstable-scrub, issue #4198)

`cqlite scrub` SHALL expose the scrub library with a stable report and exit codes scripts can branch
on, and a `--dry-run` mode with no equivalent in Cassandra's own scrub tools (epic #4192 convention:
"`--dry-run`-first repair"). All requirements are ADDED.

## ADDED Requirements

### Requirement: R7 — The verb, its inputs, and its exit codes

The CLI SHALL provide `cqlite scrub <table-dir> --out <dir> [--now <ts>] [--dry-run] [--purge]
[--skip-corrupted] [--reinsert-overflowed-ttl] [--manifest <path>] [--out-format text|json]`,
resolving the schema through the `--schema` global, exiting `0` only when nothing needed reporting
across every generation, `3` when something was written or reported (out-of-order sidecar, losses,
TTL rewrites — any combination), `2` when refused (a corrupted partition without `--skip-corrupted`,
a counter table, or an unreadable boundary source), `1` on usage errors.

#### Scenario: R7.1 healthy table dir, default flags
- **Given** the built binary, `--dataset test_basic`, a healthy multi-generation table
- **When** `cqlite scrub <table-dir> --out <tmp> --out-format json` runs
- **Then** exit `0`, `<tmp>` holds one complete generation set per input generation, and the report's
  `generations[]` each have empty `out_of_order`/`losses` and `ttl_rewrites: 0`
  (`cqlite-cli/tests/scrub_cli_tests.rs`, named in the gate's `cli-tests` list).

#### Scenario: R7.2 out-of-order input exits 3 with the sidecar written
- **Given** the R2.1 fixture
- **When** `cqlite scrub <table-dir> --out <tmp>` runs
- **Then** exit `3`, `<tmp>/<generation>-outoforder` exists and verify-clean, and the report names the
  diverted partition(s).

#### Scenario: R7.3 corrupted partition without `--skip-corrupted` exits 2
- **Given** `test_comp_corrupt/data_db_bit_flip` staged as a one-generation table dir
- **When** `cqlite scrub <table-dir> --out <tmp>` runs (no `--skip-corrupted`)
- **Then** exit `2`, stderr names the corrupted partition and the `--skip-corrupted` remedy, and
  `<tmp>` has no output for that generation.

#### Scenario: R7.4 usage errors
- **When** `--out` is a non-empty dir, or no `--schema` resolves the table, or the table dir has no
  generations
- **Then** exit `1` with the cause on stderr and nothing written.

#### Scenario: R7.5 destructive path arguments reuse salvage's ONE write guard

Issue #4196 round-23 found and fixed three distinct ways a destructive path argument
(`--manifest`, `--out`) could land on a byte the operator did not intend to touch (planned-output
collision, a symlink into the input, `--out` nested inside the input). Scrub has the identical
hazard shape for the identical two arguments.

- **Given** the same three adversarial path shapes R7.8 of the salvage spec covers
- **When** scrub is invoked with an equivalent `--manifest`/`--out` argument
- **Then** the SAME shared guard helper (not a second, independently-maintained one) refuses each,
  resolved-before-open, re-checked immediately before `File::create`
  (`cqlite-cli/tests/issue_4198_scrub_write_guard.rs`, reusing
  `cqlite-cli/src/commands/salvage/write_guard.rs`'s helper directly rather than duplicating it).

### Requirement: R8 — `--dry-run` reports exactly what the wet run would do, and writes nothing

`--dry-run` SHALL compute the full report (out-of-order verdicts, `--skip-corrupted` losses,
`--reinsert-overflowed-ttl` rewrite counts) without writing any byte under `--out`, and its report
SHALL equal the wet run's report field-for-field except the `dry_run` flag itself.

#### Scenario: R8.1 dry-run vs wet-run equality
- **Given** the R7.2 out-of-order fixture, `--skip-corrupted` and `--reinsert-overflowed-ttl` both
  set
- **When** `cqlite scrub ... --dry-run --out-format json` and the same command without `--dry-run`
  both run against independent copies of `--out`'s parent
- **Then** a recursive sha256 of `--out`'s parent is unchanged after the dry run; the two reports are
  equal after removing the `dry_run` field; and the dry run's exit code equals the wet run's
  (`cqlite-cli/tests/issue_4198_scrub_dry_run.rs`).

### Requirement: R9 — The report is the contract; text is a rendering of it

The JSON report SHALL follow `design.md` D6's field order and shape exactly; the default text output
SHALL be a rendering of the same fields, never a second source of truth.

#### Scenario: R9.1 JSON report field order matches D6
- **Given** any scenario above run with `--out-format json`
- **When** the report is parsed
- **Then** every top-level and per-generation field named in D6 is present in D6's order, verified
  against the COMPILED BINARY's actual output (not read off the Rust struct definition in isolation)
  (`cqlite-cli/tests/scrub_cli_tests.rs::report_schema_matches_design`).
