# cli-rebuild — new capability (sstable-rebuild, issue #4197)

`cqlite rebuild` SHALL expose component regeneration from the binary with a stable manifest and
exit codes scripts can branch on, and SHALL refuse `--in-place` until its #4195 dependency ships.
All requirements are ADDED.

## ADDED Requirements

### Requirement: R7 — The verb, its inputs, and its exit codes

The CLI SHALL provide `cqlite rebuild <Data.db | table-dir> --components
index,summary,filter,digest,toc,crc,statistics --out <dir> [--in-place]`, resolving the schema
through the `--schema` global, rebuilding each generation of a table dir separately, and exiting
`0` only when every requested component was regenerated (or correctly `skipped_not_applicable`),
`2` when refused, `1` on usage errors. There is no partial-success exit code (design.md §D3).

#### Scenario: R7.1 healthy table dir, per-generation outputs
- **Given** the built binary, `--dataset test_basic`, a table with 2 generations, all 7
  components deleted from each generation in a temp copy
- **When** `cqlite rebuild <table-dir> --components
  index,summary,filter,digest,toc,crc,statistics --out <tmp> --out-format json` runs
- **Then** exit `0`, `<tmp>` holds two complete generation component sets (Data.db itself untouched
  — same file, verified by sha256), and the manifest has two entries each with `refused: null`
  (`cqlite-cli/tests/rebuild_cli_tests.rs`, named in the gate's `cli-tests` list).

#### Scenario: R7.2 damaged Data.db exits 2 with the manifest naming salvage
- **Given** `test_comp_corrupt/data_db_bit_flip` (skip-clean if absent; required under
  `CQLITE_REQUIRE_FIXTURES=1`)
- **When** `cqlite rebuild <Data.db> --components index --out <tmp> --manifest <tmp>/m.json` runs
- **Then** exit `2`, stderr and `m.json` both name `data-corrupt` and the `salvage` (#4196) remedy,
  and `<tmp>` contains no output.

#### Scenario: R7.3 usage errors
- **When** `--out` is a non-empty dir, no `--schema` resolves the table, the input dir has no
  `Data.db`, an unknown component name is passed, or `--in-place` is passed
- **Then** exit `1` with the specific cause on stderr and nothing written.

### Requirement: R8 — `--in-place` refuses until #4195 ships `verify --mode audit`

The CLI SHALL refuse `--in-place` with a named-dependency usage error today, and — once #4195
lands `VerifyMode::Audit` — SHALL follow the temp-file/atomic-rename/audit protocol of design.md
§D4.

#### Scenario: R8.1 `--in-place` refuses today, naming the dependency
- **Given** the built binary (current state: `VerifyMode` is `Quick`/`Full` only, confirmed in
  `cqlite-core/src/storage/sstable/verify.rs`)
- **When** `cqlite rebuild <table-dir> --components index --in-place` runs
- **Then** exit `1`, stderr states `--in-place requires verify --mode audit (#4195), not yet
  available`, and nothing on disk changes (sha256-verified).

#### Scenario: R8.2 help states the boundaries
- **When** `cqlite rebuild --help` runs
- **Then** it states: Data.db is never modified, Statistics.db rebuild is opt-in and lossy (naming
  which fields), a damaged Data.db needs `salvage` first, and `--in-place` is gated on #4195.

### Requirement: R9 — The manifest is the contract, text is a rendering

The JSON manifest SHALL follow design.md §D5 exactly and the text output MUST be derived from it,
including the per-field `classification` map whenever `statistics`, `summary`, or `filter` was
requested.

#### Scenario: R9.1 committed expected manifests
- **Given** committed expected manifest files under `cqlite-cli/tests/fixtures/rebuild/` for
  R7.1–R7.2 and R3.1/R3.2's classification cases (volatile fields `now`, `cqlite_version`,
  absolute paths normalised)
- **When** the tests run
- **Then** the produced JSON deep-equals the expected file, and the `text` rendering contains every
  `classification` entry and the `regenerated`/`skipped_not_applicable` lists.

### Requirement: R10 — The rebuilt output is a valid SSTable readable end-to-end

Every generation rebuild writes SHALL pass `cqlite verify --mode full` and MUST be readable by the
CQLite reader with row-for-row parity against the original's own reader output.

#### Scenario: R10.1 verify and read-back parity
- **Given** every output generation from R7.1
- **When** `cqlite verify --mode full` and `cqlite read-sstable` run on the rebuilt directory
- **Then** verify reports zero findings (exit 0), and the read-back rows deep-equal a
  `read-sstable` run against the ORIGINAL (pre-deletion) component set for the same Data.db —
  proving the rebuilt Index.db/Summary.db/Filter.db actually serve reads correctly, not merely that
  they parse.
