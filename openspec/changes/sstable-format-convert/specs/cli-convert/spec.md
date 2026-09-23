# cli-convert — new capability (issue #4202)

`cqlite convert` SHALL expose the format-convert scan from the binary, one output generation per
input generation, never modifying its input. All requirements are ADDED.

## ADDED Requirements

### Requirement: R5 — The verb, its inputs, and its refusals

The CLI SHALL provide `cqlite convert <table-dir> --out <dir> --format big|bti [--compression
none]`, resolving the schema through the `--schema` global `query`/`salvage` use, converting each
generation of the table dir separately into `--out`.

#### Scenario: R5.1 healthy table dir, per-generation outputs, both directions
- **Given** the built binary, `--dataset test_basic`, a multi-generation table
- **When** `cqlite convert <table-dir> --out <tmp> --format bti` runs, then a second invocation
  converts that output back `--format big`
- **Then** exit `0` both times, `<tmp>` holds one output generation per input generation each time,
  with source generation numbers preserved (`cqlite-cli/tests/convert_cli_tests.rs`, named in the
  gate's `cli-tests` target list).

#### Scenario: R5.2 `--format` is required, no implicit default
- **When** `cqlite convert <table-dir> --out <tmp>` runs without `--format`
- **Then** exit `1` (clap usage error) — there is no "convert to whatever the input already is"
  default, since that is a no-op an operator should not reach for by omission.

#### Scenario: R5.3 `--compression` other than `none` refuses closed
- **When** `--compression lz4` (or any value other than `none`) is passed
- **Then** exit `1`, stderr names `Error::UnsupportedFormat` and cites #1406, and `--out` receives no
  output for that generation.

### Requirement: R6 — `--help` states the format and compression boundaries

`cqlite convert --help` SHALL state the uncompressed-output boundary (#1406) and that `--format`
has no default, so an operator reads the tool's limits before running it rather than discovering
them from a refusal.

#### Scenario: R6.1 help text
- **When** `cqlite convert --help` runs
- **Then** it states: output is uncompressed only (#1406) with the exact remedy wording `salvage`'s
  help already uses for the same boundary, `--format` has no default, and one generation in
  produces exactly one generation out (never merged).

### Requirement: R7 — The shared write guard protects the input and the run's own output

`convert` SHALL use the SAME destructive-path guard `salvage` uses (promoted to
`cqlite-cli/src/commands/write_guard.rs` by whichever of #4199/#4202 lands first — design.md §D4),
protecting the input table directory and the run's own planned `--out/<keyspace>/<table>/`
generation directory, resolved-then-checked exactly as `salvage`'s R7.8 scenarios require (symlink
following, lexical resolution of a not-yet-existing path, fail-closed on an unresolvable candidate).

#### Scenario: R7.1 `--out` inside the input tree is refused
- **Given** a staged healthy generation
- **When** `--out <input>/converted` is passed
- **Then** exit `1` naming the collision with the INPUT, and no bytes are written under `--out`
  (`cqlite-cli/tests/convert_cli_tests.rs::out_inside_input_is_refused`, same shape as salvage's
  `f5_out_inside_the_input_tree_is_refused...`).

#### Scenario: R7.2 input bytes are unchanged after every run, including a refused one
- **Given** a staged copy of a committed table dir
- **When** `convert` runs to completion, and separately when it is refused by R5.2/R5.3/R7.1
- **Then** every file under the staged input directory is byte-identical before and after, in both
  cases (`input_unchanged_after_run`, `input_unchanged_after_refusal`).

### Requirement: R8 — Fail closed on an unreadable or unhealthy input

`convert` SHALL fail the whole generation, never emitting a partial or silently-narrowed output,
when any component cannot be read or a partition fails to decode — it assumes a healthy input
(design.md's explicit non-goal: it is not a recovery tool).

#### Scenario: R8.1 unreadable component refuses the generation
- **Given** a temp table dir with one generation's `Statistics.db` truncated
- **When** `convert` runs
- **Then** exit `2` naming the generation and the unreadable component, and no output is written for
  that generation; any OTHER healthy generation in the same table dir still converts
  (matching salvage's per-generation independence).
