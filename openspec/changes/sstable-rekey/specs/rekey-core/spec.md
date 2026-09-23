# rekey-core — new capability (issue #4203)

`cqlite-core` SHALL expose `rekey_table` as a new, unconditional (`pub mod rekey`) entry point under
`storage/write_engine/`, implemented as a verified byte-for-byte component copy under a renamed
table directory — never a decode/re-encode — because the table id is confirmed (proposal.md, against
`cassandra-5.0.8` source) to live only in the directory name for every format CQLite reads. All
requirements are ADDED.

## ADDED Requirements

### Requirement: R1 — Rekeyed output is byte-identical to the source, addressable under the new id

`rekey_table` SHALL copy every component of every generation under `table_dir` byte-for-byte into
`<out_dir>/<table_name>-<new_id_hex>/`, and the result SHALL be readable by the CQLite reader under
the new directory, dump-equal to the source's own golden.

#### Scenario: R1.1 single-generation BIG rekey is byte-identical and dump-equal
- **Given** a committed `test_basic` table with a known source table id
- **When** `rekey_table` runs with a distinct, valid new id
- **Then** every component file's sha256 under the new directory equals the source's, the new
  directory's own dump (via the CQLite reader, opened at the NEW path) equals the source's
  sstabledump JSONL golden, and the source directory is byte-for-byte unchanged —
  `cqlite-core/tests/issue_4203_rekey_parity.rs`.

#### Scenario: R1.2 BTI rekey
- **Given** a committed `test_da` table
- **When** `rekey_table` runs with a distinct, valid new id
- **Then** the same sha256-identity and dump-equality hold for the BTI component set
  (`Partitions.db`, `Rows.db`, `Statistics.db`, `Summary.db`, `Filter.db`, `Digest.crc32`, `TOC.txt`)
  — `cqlite-core/tests/issue_4203_rekey_parity.rs`.

#### Scenario: R1.3 multi-generation rekey copies every generation under the one new id
- **Given** a committed table with 2+ generations
- **When** `rekey_table` runs
- **Then** every generation appears under the SAME new directory (`<table>-<new_id>/nb-1-big-*`,
  `nb-2-big-*`, …), each byte-identical to its source generation —
  `cqlite-core/tests/issue_4203_rekey_parity.rs`.

### Requirement: R2 — the output self-audits before publish

`rekey_table` SHALL run `verify --mode full` against the copied output BEFORE it is visible under
`out_dir`, refusing (nothing published) on any verify failure.

#### Scenario: R2.1 a source with a component that fails verify refuses the whole rekey
- **Given** a staged source generation with one deliberately corrupted component (any
  `test_comp_corrupt/*` fixture, or `corrupt_byte_fixture.rs`'s mutation)
- **When** `rekey_table` runs
- **Then** the run refuses, names the failing component/check, and `out_dir` contains no
  `<table>-<new_id>/` directory at all (not even a partial one) —
  `cqlite-core/tests/issue_4203_rekey_verify_gate.rs`.

### Requirement: R3 — version-floor and `--version` boundaries are inherited, never reimplemented

`rekey_table` SHALL refuse a pre-`na` source via the existing `BigVersionGates`/`BtiVersionGates`
`from_version` checks (`Error::UnsupportedVersion`) without a rekey-specific reimplementation of the
floor, and SHALL refuse any `--version` request that names a format actually different from the
source's own version, naming `cqlite convert` (#4202) as the remedy.

#### Scenario: R3.1 pre-na source refuses with the inherited error
- **Given** a synthesized or fixture directory whose component filenames carry a pre-`na` version
  marker
- **When** `rekey_table` runs
- **Then** it returns `Error::UnsupportedVersion { floor: "na", .. }`, the SAME error variant/floor
  `BigVersionGates::from_version` already produces elsewhere, not a distinct rekey error —
  `cqlite-core/tests/issue_4203_rekey_version_floor.rs`.

#### Scenario: R3.2 a real `--version` change is refused and points at #4202
- **Given** a committed `nb` (BIG) table
- **When** `rekey_table` runs requesting `version: Some("da")`
- **Then** the run refuses BEFORE copying anything, names `cqlite convert` (#4202) as the remedy,
  and `out_dir` gains no new directory — `cqlite-core/tests/issue_4203_rekey_version_delegates.rs`.

#### Scenario: R3.3 `--version` matching the source's own version proceeds normally
- **Given** the same committed `nb` table
- **When** `rekey_table` runs requesting `version: Some("nb")`
- **Then** it proceeds exactly as R1.1 (byte-identical copy) — `--version` naming the source's own
  format is not a refusal — `cqlite-core/tests/issue_4203_rekey_version_delegates.rs`.

### Requirement: R4 — the input is never modified

`rekey_table` SHALL NOT write, truncate, or otherwise alter any byte of `table_dir`, on any code
path including a refusal.

#### Scenario: R4.1 sha256 of every input component is unchanged across success and refusal
- **Given** a committed table directory with a recorded sha256 of every component
- **When** `rekey_table` runs to completion, and separately is made to refuse (R2.1's corrupted
  fixture, R3.2's version-change request)
- **Then** every input component's sha256 is unchanged across every run —
  `cqlite-core/tests/issue_4203_rekey_input_immutability.rs`.
