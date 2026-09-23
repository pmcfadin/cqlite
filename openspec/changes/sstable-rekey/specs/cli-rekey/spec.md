# cli-rekey — new capability (issue #4203)

`cqlite rekey` SHALL expose `rekey_table` with a stable manifest, exit codes scripts can branch on,
and the shared destructive-path write guard `salvage`/`rebuild`/`extract`/`split` already
established (#4196/#4197/#4199) — never a fourth independently-written copy of that logic. All
requirements are ADDED.

## ADDED Requirements

### Requirement: R-CLI-1 — the verb's shape and exit codes

The CLI SHALL provide `cqlite rekey <table-dir> --table-id <uuid> --out <dir> [--version nb|da]
[--manifest <path>] [--out-format text|json]`, accepting `--table-id` in either the standard
36-char dashed UUID form or the bare 32-hex directory form, exiting `0` when the rekeyed directory
was published and verify-clean, `2` when refused (a real `--version` change requested, a
verify-gate failure, mixed source versions with `--version` given), `1` on usage errors.

#### Scenario: R-CLI-1.1 successful rekey with the dashed UUID form
- **Given** the built binary, a committed table with a known source id
- **When** `cqlite rekey <table-dir> --table-id xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx --out <tmp>
  --out-format json` runs (a valid, distinct id)
- **Then** exit `0`, `<tmp>/<table>-<32-hex-of-that-uuid>/` holds a complete, verify-clean
  generation set, and the manifest's `new_table_id` is the normalized 32-hex form —
  `cqlite-cli/tests/rekey_cli_tests.rs`.

#### Scenario: R-CLI-1.2 successful rekey with the bare 32-hex form
- **When** the same request uses `--table-id` in bare 32-lowercase-hex form instead
- **Then** the result is identical to R-CLI-1.1 — `cqlite-cli/tests/rekey_cli_tests.rs`.

#### Scenario: R-CLI-1.3 a real `--version` change exits 2 and names #4202
- **Given** a committed `nb` table
- **When** `cqlite rekey <table-dir> --table-id <valid-new-id> --version da --out <tmp>` runs
- **Then** exit `2`, stderr names `cqlite convert` (#4202) as the remedy, `<tmp>` gains no directory
  — `cqlite-cli/tests/rekey_cli_tests.rs`.

#### Scenario: R-CLI-1.4 usage errors
- **When** `--table-id` is malformed (wrong length, non-hex, bad dash positions), names the SAME id
  the source already has (without an explicit override), `--out` is non-empty, or `<table-dir>`
  does not match the `<name>-<32-hex>` directory convention at all
- **Then** exit `1` with the cause on stderr and nothing written — `cqlite-cli/tests/rekey_cli_tests.rs`.

#### Scenario: R-CLI-1.5 pre-na input exits 2 with the inherited error
- **Given** a pre-`na` fixture (or synthesized filename)
- **When** `cqlite rekey` runs against it
- **Then** exit `2`, stderr states the `UnsupportedVersion` floor `na` (the same message
  `verify`/`salvage`/`rebuild` already produce for the same condition, not new wording) —
  `cqlite-cli/tests/rekey_cli_tests.rs`.

### Requirement: R-CLI-2 — the manifest is the contract, text is a rendering

The JSON manifest SHALL follow design.md §D5 exactly, and the text rendering MUST be derived from
the same manifest object.

#### Scenario: R-CLI-2.1 text and JSON renderings agree
- **Given** R-CLI-1.1's run
- **When** it is run once with `--out-format text` and once with `--out-format json`
- **Then** every fact the text rendering states (source id, new id, generations copied, verify
  status) is present and matching in the JSON manifest — `cqlite-cli/tests/rekey_cli_tests.rs`.

#### Scenario: R-CLI-2.2 `--help` states the copy-not-rewrite and version-delegation boundaries
- **When** `cqlite rekey --help` runs
- **Then** it states: output components are byte-identical to the input (no compression/format
  change occurs here); a `--version` change is refused and delegates to `cqlite convert` (#4202);
  and the table NAME is unchanged, only the id — `cqlite-cli/tests/rekey_cli_tests.rs`.

### Requirement: R-CLI-3 — `--out`/`--manifest` reuse the shared destructive-path write guard

`--out` and `--manifest` SHALL be validated by the SAME promoted `WriteGuard`
(`commands/write_guard.rs`, established by #4199) that resolves each candidate path and refuses one
landing on or inside the input or the run's own planned output.

#### Scenario: R-CLI-3.1 `--out` inside the input is refused
- **Given** a staged healthy table dir
- **When** `cqlite rekey <table-dir> --table-id <valid-new-id> --out <table-dir>/rekeyed` runs
- **Then** exit `1`, the resolved victim is named, and the input directory gains no new files —
  `cqlite-cli/tests/rekey_write_guard_tests.rs`.

#### Scenario: R-CLI-3.2 a symlinked `--manifest` into the input is refused
- **Given** a staged healthy table dir and a symlink named `manifest.json` pointing at a real
  component inside it
- **When** `cqlite rekey` is pointed at that symlink via `--manifest`
- **Then** exit `1`, the victim component is byte-identical before and after —
  `cqlite-cli/tests/rekey_write_guard_tests.rs`.

#### Scenario: R-CLI-3.3 the documented manifest location still works
- **When** `--manifest <--out>/rekey.json` is used for an otherwise-successful request
- **Then** the run exits `0` and writes a parseable manifest at that path —
  `cqlite-cli/tests/rekey_write_guard_tests.rs`.
