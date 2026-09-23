# cli-diagnose — new capability (sstable-diagnose, issue #4204)

`cqlite diagnose` SHALL expose the diagnose library as a read-only report, with `--out` naming a
RENDERING format (never a write destination — a deliberate divergence from this epic's writing
verbs, stated in `--help`). All requirements are ADDED.

## ADDED Requirements

### Requirement: R6 — The verb, its inputs, and its (trivial) exit contract

The CLI SHALL provide `cqlite diagnose <table-dir> [--now <ts>] [--deep] [--top N] [--out
text|json]`, resolving generations from the table dir directly (no `--schema` required for the
cheap tier — Statistics.db/Index.db need no schema; `--deep`'s partition/clustering rendering uses
`--schema` when given, and renders raw key bytes when it is not, never refusing for its absence).
Exit `0` for any successful report (a generation this run cannot read is a field in the report, not
a refusal); `1` only for usage errors.

#### Scenario: R6.1 cheap-tier default run
- **Given** the built binary, `--dataset test_basic`, a healthy multi-generation table
- **When** `cqlite diagnose <table-dir> --out json` runs
- **Then** exit `0`, and the report's `generations[]` has one entry per input generation, each with
  every cheap-tier field populated with a `source`
  (`cqlite-cli/tests/diagnose_cli_tests.rs`, named in the gate's `cli-tests` list).

#### Scenario: R6.2 `--deep --top N`
- **Given** `test_wide_rows`
- **When** `cqlite diagnose <table-dir> --deep --top 5 --out json` runs
- **Then** exit `0`, and each generation's `deep` field is populated with histograms and exactly up
  to 5 entries in each top-N list.

#### Scenario: R6.3 usage errors
- **When** `<table-dir>` has no generations, or does not exist
- **Then** exit `1` with the cause on stderr, and (trivially, since this verb never writes) nothing
  is written anywhere.

#### Scenario: R6.4 `--out` never writes a file
- **Given** any successful run with `--out json`
- **When** the command completes
- **Then** the JSON report is printed to stdout only — `--out` selects a rendering, not a
  destination; no new file exists anywhere under the table dir or any `--out`-adjacent path
  (`cqlite-cli/tests/diagnose_cli_tests.rs::out_flag_is_a_render_format_not_a_destination`).

### Requirement: R7 — The report is the contract; text is a rendering of it

The JSON report SHALL follow `design.md` D5's field order and shape exactly, verified against the
compiled binary's actual output.

#### Scenario: R7.1 JSON report field order matches D5
- **Given** any scenario above run with `--out json`
- **When** the report is parsed
- **Then** every top-level and per-generation field named in D5 is present in D5's order, verified
  against the COMPILED BINARY's actual output, not read off the Rust struct definition in isolation
  (`cqlite-cli/tests/diagnose_cli_tests.rs::report_schema_matches_design`).

#### Scenario: R7.2 text rendering names #4200/#4195 where diagnose stops short
- **Given** any run whose report includes `estimated_droppable_tombstone_ratio` or
  `top_tombstone_heaviest_partitions`
- **When** rendered as text
- **Then** the field's description names `cqlite tombstones` as the authoritative, reconciled
  alternative — never silently presenting the estimate/raw count as if it were the reconciled
  answer (`cqlite-cli/tests/diagnose_cli_tests.rs`).
