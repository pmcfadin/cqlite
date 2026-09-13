# cli-salvage — new capability (sstable-salvage, issue #4196)

`cqlite salvage` SHALL expose the salvage scan from the binary with a stable manifest and exit
codes scripts can branch on. All requirements are ADDED.

> **DEFERRED SCENARIOS (issue #4196, roborev finding — not implemented in the #4196 PR, tracked as
> follow-up work):** R8.1's committed expected-manifest fixtures under
> `cqlite-cli/tests/fixtures/salvage/` and R9.1 (`cqlite verify --mode full` + read-back of every
> salvaged generation) are NOT implemented. R7.1-R7.7 and R8.2 are implemented and pass against real
> fixtures via the compiled binary (`cqlite-cli/tests/salvage_cli_tests.rs`, plus
> `cqlite-cli/tests/issue_4196_salvage_publication_barrier.rs` for R7.7 and R7.2's partial-loss arm —
> its own target because the first file sits at ~1420 of the ~1500-line #1135 threshold).
>
> **R7.2 now covers BOTH arms, across two targets (C intent audit on this issue).**
> `salvage_cli_tests.rs::damaged_input_manifest_names_every_loss` runs against the corpus's actual
> outcome for `data_db_bit_flip`, which is a TOTAL loss (one partition, entirely inside the flipped
> chunk) and so exits **2** — it accepts `2 || 3` and its exit-3 branch never executes. The C audit
> found that design D3's central row — "some partitions lost and some recovered → exit 3 with a
> complete generation set still written" — was therefore unexercised end-to-end at the CLI, and that
> the previous justification for the gap ("no corpus fixture demonstrates a genuinely PARTIAL
> chunk-crc loss today") was wrong in two ways: this change's own
> `issue_4196_salvage_corruption_corpus.rs::swapped_index_entry_keys_classify_key_mismatch` already
> produces a partial loss at the core level, and the committed, fully git-tracked
> `test_da.multiclustering_table` holds THREE partitions. The partial arm is now
> `issue_4196_salvage_publication_barrier.rs::damaged_input_exits_3_with_losses_and_a_complete_generation_set`,
> which corrupts one compressed chunk of that fixture — leaving the chunk's CRC trailer alone, the
> same bit-rot model R2.1 uses — and asserts exit **exactly 3**, one `chunk-crc` loss naming the
> partition with its `data_offset` and intersecting chunk index, `partitions.recovered == 2`,
> `refused: null`, and a COMPLETE generation set: every component the output's own `TOC.txt` names.
> The corrupted chunk is chosen by DERIVATION from two Cassandra-written components (the committed
> `sstabledump` golden's per-partition `position` and `CompressionInfo.db`'s chunk table) so exactly
> one partition can intersect it, every step of that derivation is asserted, and a control leg on the
> same UNCORRUPTED staging exits 0 — so exit 3 cannot be dismissed as that fixture's permanent
> outcome. R8.2's specific test is
> `help_states_uncompressed_whole_partition_and_rebuild_boundaries` in that file, NAMED here because
> the C-audit on this issue found this paragraph asserting R8.2 coverage while no test invoked
> `salvage --help` at all — an unlocatable coverage claim reads exactly like a covered one.

## ADDED Requirements

### Requirement: R7 — The verb, its inputs, and its exit codes

The CLI SHALL provide `cqlite salvage <Data.db | table-dir> --out <dir> [--manifest <path>]
[--out-format text|json]`, resolving the schema through the `--schema` global, salvaging each
generation of a table dir separately, and exiting `0` only with zero losses AND every verification
the run depends on having actually RUN, `3` with losses or with a verification GAP (output written
either way), `2` when refused, `1` on usage errors — including a `--manifest` path that would
destroy part of the input.

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

#### Scenario: R7.5 a verification that could not RUN exits 3, not 0

Roborev, issue #4196, round-22 Medium finding: `component_findings` influenced the exit code not at
all, so an uncompressed input with no `CRC.db` — which disables chunk-CRC loss detection ENTIRELY —
reported `losses: 0 RECOGNISED` and exited `0`, indistinguishable to `$?` from a fully CRC-verified
clean run. D3's "an unmeasured run cannot read as clean" held for the rendered text only.

- **Given** the committed uncompressed generation `test_comp/uncompressed_table` staged twice,
  differing by exactly one file (`nb-1-big-CRC.db` absent / present)
- **When** `cqlite salvage <staged-dir> --out <tmp> --out-format json` runs on each
- **Then** the CRC-less leg exits `3` with a `ChunkCrcUnavailable` component finding in the manifest,
  `losses: []`, and the output generation still written; and the CRC-ful leg exits `0` with no such
  finding (`uncompressed_input_without_crc_db_exits_3_and_names_the_verification_gap` in
  `cqlite-cli/tests/salvage_cli_tests.rs`).

#### Scenario: R7.6 `--manifest` may not destroy part of the input

Roborev, issue #4196, round-22 Low finding: `--out` was guarded fail-closed while `--manifest`
accepted any path and is written with `File::create`, which truncates — so
`--manifest <input-dir>/nb-1-big-Statistics.db` destroyed a component of the very input the tool
exists to preserve, violating R5.1.

- **Given** a staged healthy generation
- **When** `--manifest` names a path that resolves inside the input directory, or an existing SSTable
  component (`*.db` / `*-TOC.txt` / `*-Digest.crc32`)
- **Then** exit `1` naming the collision on stderr, every input file byte-identical, nothing written
  under `--out`; and the documented `--manifest <--out>/salvage.json` invocation still exits `0` and
  writes the manifest
  (`manifest_inside_the_input_is_refused_and_the_input_is_untouched`, same file).

#### Scenario: R7.7 an explicit `Data.db` overrides the publication barrier, but never silently

Roborev, issue #4196, round-22 Low finding: the single-FILE input path did not probe for the sibling
`-TOC.txt` at all, so it salvaged an unpublished generation SILENTLY at exit `0`, while the
table-DIRECTORY path on the same file named it a skipped input and exited `3`.

- **Given** a staged healthy generation whose `-TOC.txt` has been removed
- **When** `cqlite salvage <that Data.db> --out <tmp> --out-format json` runs
- **Then** the generation IS salvaged (an explicit file path is the operator's choice) AND exit is
  `3` with an `UnpublishedInputGeneration` component finding in the manifest, matching what the same
  file gets through its table directory; with the `-TOC.txt` present the same input exits `0` with no
  such finding (`cqlite-cli/tests/issue_4196_salvage_publication_barrier.rs`).

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
