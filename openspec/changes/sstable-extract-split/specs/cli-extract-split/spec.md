# cli-extract-split — new capability (issue #4199)

`cqlite extract` and `cqlite split` SHALL expose the library entry points from `extract-split-core`
with a stable manifest, exit codes scripts can branch on, and the shared destructive-path write
guard `salvage`/`rebuild` already established. All requirements are ADDED.

## ADDED Requirements

### Requirement: R-CLI-1 — `extract`'s verb shape and exit codes

The CLI SHALL provide `cqlite extract <table-dir> (--partition <key> | --token-range <a>,<b> |
--keys-file <f>) --out <dir> [--raw] [--table <name>] [--manifest <path>] [--out-format text|json]`,
exactly one selection flag required, exiting `0` when every selected key was found and recovered
cleanly, `3` when the manifest's `not_found` is non-empty (some requested keys never existed) but
everything that WAS found is written, `2` when a found partition could not be read cleanly (refused,
nothing written), `1` on usage errors.

#### Scenario: R-CLI-1.1 single partition, reconciled
- **Given** the built binary, a committed table with a known live key
- **When** `cqlite extract <table-dir> --partition <key> --out <tmp> --out-format json` runs
- **Then** exit `0`, `<tmp>` holds one output generation, and the manifest's `not_found` is empty
  — `cqlite-cli/tests/extract_cli_tests.rs`.

#### Scenario: R-CLI-1.2 keys-file with absent keys exits 3
- **Given** a keys-file mixing live and guaranteed-absent keys (R3.1's fixture)
- **When** `cqlite extract <table-dir> --keys-file <f> --out <tmp>` runs
- **Then** exit `3`, stderr and the manifest both name the absent keys, and `<tmp>` holds the live
  keys' output — `cqlite-cli/tests/extract_cli_tests.rs`.

#### Scenario: R-CLI-1.3 a decode failure on the requested partition exits 2 and writes nothing
- **Given** R6.1's corrupted fixture and a request for exactly that partition
- **When** `cqlite extract` runs
- **Then** exit `2`, stderr names the offset and the `salvage`/`rebuild` remedy, `<tmp>` has no
  `Data.db` — `cqlite-cli/tests/extract_cli_tests.rs`.

#### Scenario: R-CLI-1.4 usage errors
- **When** two selection flags are given together, no selection flag is given, `--out` is
  non-empty, `--schema` does not resolve the table, or `--token-range` is malformed (non-numeric,
  or `a >= b`)
- **Then** exit `1` with the cause on stderr and nothing written —
  `cqlite-cli/tests/extract_cli_tests.rs`.

#### Scenario: R-CLI-1.5 `--raw` writes one generation per matching input generation
- **Given** the R2.1 two-generation fixture
- **When** `cqlite extract <table-dir> --partition <key> --raw --out <tmp> --out-format json` runs
- **Then** exit `0`, `<tmp>` holds two output generations named after the two source generations,
  and the manifest's `mode` is `"raw"` — `cqlite-cli/tests/extract_cli_tests.rs`.

### Requirement: R-CLI-2 — `split`'s verb shape and exit codes

The CLI SHALL provide `cqlite split <Data.db | table-dir> (--parts <N> | --max-bytes <B>) --out
<dir> [--table <name>] [--manifest <path>] [--out-format text|json]`, exactly one boundary flag
required, exiting `0` when every part was written and independently verified `verify --mode full`
clean, `2` when a partition could not be read cleanly OR a produced part failed its own verify
self-audit (nothing published), `1` on usage errors including a multi-generation input with no
explicit `Data.db` path.

#### Scenario: R-CLI-2.1 `--parts N` produces N verified parts
- **Given** the built binary and a committed table restaged into one generation
- **When** `cqlite split <Data.db> --parts 4 --out <tmp> --out-format json` runs
- **Then** exit `0`, `<tmp>` holds 4 part directories, the manifest lists 4 `parts` entries each
  with `"verify": "pass"`, and `source_partitions` equals the sum of the parts' `partitions` —
  `cqlite-cli/tests/split_cli_tests.rs`.

#### Scenario: R-CLI-2.2 `--max-bytes B` produces byte-bounded parts
- **Given** the same staged source
- **When** `cqlite split <Data.db> --max-bytes <B> --out <tmp>` runs
- **Then** exit `0` and every part's `bytes` in the manifest is `<= B` plus at most one partition's
  width — `cqlite-cli/tests/split_cli_tests.rs`.

#### Scenario: R-CLI-2.3 a multi-generation table dir without an explicit Data.db is a usage error
- **Given** a committed table dir with 2+ generations
- **When** `cqlite split <table-dir> --parts 2 --out <tmp>` runs (no explicit `Data.db`)
- **Then** exit `1`, stderr lists every generation found and asks for an explicit `Data.db` path —
  `cqlite-cli/tests/split_cli_tests.rs`.

#### Scenario: R-CLI-2.4 a decode failure anywhere in the source refuses the whole split
- **Given** R6.2's corrupted single-generation fixture
- **When** `cqlite split <Data.db> --parts 4 --out <tmp>` runs
- **Then** exit `2`, stderr names the offending offset, and `<tmp>` has no part directories (not
  even the ones that would have decoded cleanly) — `cqlite-cli/tests/split_cli_tests.rs`.

### Requirement: R-CLI-3 — the manifest is the contract, text is a rendering

Both verbs' JSON manifests SHALL follow design.md §D4 exactly and each verb's text rendering MUST
be derived from the same manifest object, never computed independently.

#### Scenario: R-CLI-3.1 text and JSON renderings agree
- **Given** R-CLI-1.2's and R-CLI-2.1's runs
- **When** each is run once with `--out-format text` and once with `--out-format json`
- **Then** every fact the text rendering states (found/not-found keys for extract; per-part token
  range and verify status for split) is present and matching in the JSON manifest for the same run
  — `cqlite-cli/tests/extract_cli_tests.rs`, `cqlite-cli/tests/split_cli_tests.rs`.

#### Scenario: R-CLI-3.2 `--help` states the uncompressed-output and refuse-don't-skip boundaries
- **When** `cqlite extract --help` and `cqlite split --help` run
- **Then** each states: output is uncompressed (#1406); a partition that cannot be read cleanly
  REFUSES the run rather than being skipped and reported (unlike `salvage`); and, for `split`, that
  a multi-generation input needs an explicit `Data.db` path —
  `cqlite-cli/tests/extract_cli_tests.rs`, `cqlite-cli/tests/split_cli_tests.rs`.

### Requirement: R-CLI-4 — both verbs reuse the shared destructive-path write guard

`--out` and `--manifest` for both verbs SHALL be validated by the SAME promoted `WriteGuard`
(`commands/write_guard.rs`) that resolves each candidate path (following symlinks; refusing an
unresolvable candidate) and refuses one landing on or inside the input or the run's own planned
output — not a fourth independently-written copy of that logic.

#### Scenario: R-CLI-4.1 `--out` inside the input is refused for both verbs
- **Given** a staged healthy table dir
- **When** `cqlite extract <table-dir> --partition <key> --out <table-dir>/recovered` or
  `cqlite split <Data.db> --parts 2 --out <same-dir>/parts` runs
- **Then** exit `1`, the resolved victim is named, and the input directory gains no new files —
  `cqlite-cli/tests/extract_write_guard_tests.rs`, `cqlite-cli/tests/split_write_guard_tests.rs`.

#### Scenario: R-CLI-4.2 a symlinked `--manifest` into the input is refused for both verbs
- **Given** a staged healthy table dir and a symlink named `manifest.json` pointing at a real
  component inside it
- **When** either verb is pointed at that symlink via `--manifest`
- **Then** exit `1`, the victim component is byte-identical before and after, and nothing is
  written under `--out` — `cqlite-cli/tests/extract_write_guard_tests.rs`,
  `cqlite-cli/tests/split_write_guard_tests.rs`.

#### Scenario: R-CLI-4.3 the documented manifest location still works for both verbs
- **When** `--manifest <--out>/extract.json` or `--manifest <--out>/split.json` is used
- **Then** the run exits `0` (for a request that would otherwise succeed) and writes a parseable
  manifest at that path — `cqlite-cli/tests/extract_write_guard_tests.rs`,
  `cqlite-cli/tests/split_write_guard_tests.rs`.
