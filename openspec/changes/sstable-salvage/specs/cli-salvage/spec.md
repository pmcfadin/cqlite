# cli-salvage — new capability (sstable-salvage, issue #4196)

`cqlite salvage` SHALL expose the salvage scan from the binary with a stable manifest and exit
codes scripts can branch on. All requirements are ADDED.

> **DEFERRED SCENARIOS (issue #4196, roborev finding — not implemented in the #4196 PR, tracked as
> follow-up work):** R8.1's committed expected-manifest fixtures under
> `cqlite-cli/tests/fixtures/salvage/` and R9.1 (`cqlite verify --mode full` + read-back of every
> salvaged generation) are NOT implemented. R7.1-R7.4 and R8.2 are implemented and pass against real
> fixtures via the compiled binary (`cqlite-cli/tests/salvage_cli_tests.rs`); R7.2's scenario runs
> against the corpus's actual (total-loss) outcome rather than its literal partial-recovery text —
> see that test's doc comment. R8.2's specific test is
> `help_states_uncompressed_whole_partition_and_rebuild_boundaries` in that file, NAMED here because
> the C-audit on this issue found this paragraph asserting R8.2 coverage while no test invoked
> `salvage --help` at all — an unlocatable coverage claim reads exactly like a covered one.

## ADDED Requirements

### Requirement: R7 — The verb, its inputs, and its exit codes

The CLI SHALL provide `cqlite salvage <Data.db | table-dir> --out <dir> [--manifest <path>]
[--out-format text|json]`, resolving the schema through the `--schema` global, salvaging each
generation of a table dir separately, and exiting `0` only with zero losses, `3` with losses and
output written, `2` when refused, `1` on usage errors.

#### Scenario: R7.1 healthy table dir, per-generation outputs
- **Given** the built binary, `--dataset test_tomb`, table `resurrection_gc_positive` (2 generations)
- **When** `cqlite salvage <table-dir> --out <tmp> --out-format json` runs
- **Then** exit `0`, `<tmp>` holds two complete generation sets with the source generation numbers,
  and the manifest has two entries each with `losses: []`
  (`cqlite-cli/tests/salvage_cli_tests.rs`, named in the gate's `cli-tests` list).

#### Scenario: R7.2 damaged input exits 3 with the manifest
- **Given** `test_comp_corrupt/data_db_bit_flip` (skip-clean if absent; required under
  `CQLITE_REQUIRE_FIXTURES=1`)
- **When** `cqlite salvage <Data.db> --out <tmp> --manifest <tmp>/m.json` runs
- **Then** exit `3`, `m.json` validates against the D5 shape with `losses.length > 0`, and
  `<tmp>` holds a complete generation set.

#### Scenario: R7.3 refusal exits 2 and writes no Data.db
- **Given** `test_comp_corrupt/index_db_bit_flip_big`
- **When** the command runs
- **Then** exit `2`, stderr names `boundary-source-unreadable` and the `rebuild` remedy, and
  `<tmp>` has no `Data.db`.

#### Scenario: R7.4 usage errors
- **When** `--out` is a non-empty dir, or no `--schema` resolves the table, or the input dir has no
  `Data.db`
- **Then** exit `1` with the cause on stderr and nothing written.

### Requirement: R8 — The manifest is the contract, text is a rendering

The JSON manifest SHALL follow design.md §D5 exactly and the text output MUST be derived from it.

#### Scenario: R8.1 committed expected manifests
- **Given** committed expected manifest files under `cqlite-cli/tests/fixtures/salvage/` for
  R7.1–R7.3 (volatile fields `now`, `cqlite_version`, absolute paths normalised)
- **When** the tests run
- **Then** the produced JSON deep-equals the expected file, and the `text` rendering of the same
  run contains every loss key and the `losses: N` line.

#### Scenario: R8.2 help states the boundaries
- **When** `cqlite salvage --help` runs
- **Then** it states: output is uncompressed (#1406), a partition is recovered whole or not at
  all, and a damaged Index/Partitions component needs `rebuild` first.

### Requirement: R9 — The salvaged output is a valid SSTable

Every generation salvage writes SHALL pass `cqlite verify --mode full` and MUST be readable by the CQLite reader.

#### Scenario: R9.1 verify and read back
- **Given** every output generation from R7.1 and R7.2
- **When** `cqlite verify --mode full` and `cqlite read-sstable` run on it
- **Then** verify reports zero findings (exit 0) and the read-back row count equals
  `partitions.recovered`'s row total from the manifest.
