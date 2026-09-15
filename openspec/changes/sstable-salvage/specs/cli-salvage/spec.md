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

#### Scenario: R7.8 EVERY destructive path argument is resolved and checked by ONE guard

Roborev, issue #4196, round-23 — two High/Medium findings and one more Medium, all confirmed by an
independent Cassandra/SSTable-format expert review **with working reproductions against the compiled
binary and real Cassandra-written bytes**. R7.6's guard was correct about the property it checked and
checked the wrong things: it validated narrow local proxies — does this literal path *exist*, is its
own *parent* inside the input, is its *file name* component-shaped — rather than the invariant that
matters, namely *will any write this run performs land on a byte the operator did not intend to
overwrite, once every path is fully resolved and once the run's OWN planned outputs are counted*.
Three reproduced consequences, each of which **exited `0` with a clean console summary**:

1. `--manifest <--out>/<ks>/<tbl>/nb-1-big-Data.db` passed validation (that path did not exist yet),
   the run recovered every partition and wrote the real `Data.db`, then `write_manifest_file`
   truncated it to **607 bytes of manifest JSON**. Validation ran before the work; the write ran
   after. R7.6's own "a non-existent component-named path outside the input is allowed" test pinned
   this as intended, on a rationale ("nothing would be truncated") true at validation time and false
   at write time.
2. A **symlink** named `salvage.json` — not component-shaped, so the name rule never fired, and with
   its own parent outside the input, so the `parent()` containment rule never fired either — pointed
   at a real `nb-1-big-Statistics.db` **inside the input**. `File::create` followed it: **5265 → 531
   bytes** on the reviewer's fixture, **4847 → 619** on the committed one.
3. `--out <input>/recovered` wrote a full recovered generation **inside the input tree**, which a
   re-run then rediscovers as an additional generation to salvage.

- **Given** a staged healthy generation
- **When** any destructive path argument (`--manifest`, `--out`) is supplied
- **Then** it is **resolved before it is judged** — canonicalized when it exists, so a symlink is
  followed to its target; otherwise resolved through its nearest EXISTING ancestor with the
  remaining components applied lexically (`.` dropped, `..` popped, so a traversal through a
  not-yet-existing directory cannot escape) — and refused when the resolved path lands on or inside
  EITHER the input's own directory OR the run's own planned output generation
  `<--out>/<keyspace>/<table>/`
  (`f1_manifest_inside_the_planned_output_generation_is_refused_and_out_stays_empty`,
  `f4a_manifest_symlink_into_the_input_is_refused_and_the_victim_is_byte_identical`,
  `f5_out_inside_the_input_tree_is_refused_and_the_input_gains_no_recovered_dir` in
  `cqlite-cli/tests/issue_4196_salvage_write_guard.rs`).
- **And** the refusal exits `1`, names the **resolved victim** rather than only the path typed, and
  says whether the collision was with the INPUT or with the run's own OUTPUT — the operator's next
  action differs.
- **And** resolution **FAILS CLOSED**: an unresolvable candidate is REFUSED with the cause named,
  never admitted. This includes a **dangling symlink**, for which `canonicalize` reports a plain
  `NotFound` while `File::create` follows it and creates the target
  (`an_unresolvable_candidate_is_refused_with_a_named_reason`, `write_guard.rs`). The ONE preserved
  allow is an input that is neither an existing directory nor an existing file — nothing to protect,
  not a failure to decide (`a_nonexistent_input_protects_nothing`).
- **And** the check is re-applied **immediately before `File::create`**, not only at argument-parse
  time: a path that does not exist yet is indistinguishable from one that never will until the run
  itself creates it, so a single up-front check cannot close consequence (1) whatever it validates.
- **And** the guard is ONE helper serving every path argument. Two independently-maintained guards
  drifting is precisely how consequences (1) and (3) arose — `--manifest` had a containment check and
  `--out` never got the equivalent.
- **And** the legitimate invocations still WORK, asserted positively, because a guard that refuses
  everything is not a fix: `--manifest <--out>/salvage.json` and `--manifest <unrelated-dir>/m.json`
  both exit `0`, write a parseable non-empty D5 manifest, AND leave exactly one recovered `*-Data.db`
  under `--out` which **must not itself parse as JSON** — consequence (1)'s exact failure shape,
  checked affirmatively (`both_documented_manifest_locations_still_succeed_and_recover_real_bytes`).
- **And** the residual is NAMED, not implied closed: resolution-then-open narrows but does not close
  the TOCTOU window, since `canonicalize` is a snapshot. `O_NOFOLLOW` on the final open is tracked as
  **#4231** and deliberately not a blocker here — the realistic hazard is a stale symlink from an
  earlier run, which resolution does stop; an adversary racing the guard is not this tool's threat
  model.

#### Scenario: R7.9 a `--schema` declaring a DIFFERENT table is refused, whatever the file format

Roborev, issue #4196: round 12 hardened the CQL branch of `load_compaction_table_schema_for_table`
against selecting the wrong table's `CREATE TABLE`, and round 23 found the JSON branch had never
been given the equivalent — it discarded `target_table` entirely, so `--schema s.json` declaring
table `b` was accepted for an input of table `a` and reported a clean recovery. The round-23 expert
review rates this **borderline High rather than Medium** because it COMPOUNDS with R1.3: once salvage
trusts the input's own header, decoding table `a`'s partitions with table `b`'s column layout is a
wrong-table recovery whose only remaining safety net is the normalization. Both therefore ship in the
same round.

The root cause was reasoning recorded in a doc comment — *"JSON schema files are inherently
single-table … so `target_table` does not filter that branch — there is nothing to select among"* —
which conflates **nothing to SELECT among** (true: one table per JSON file) with **nothing to
VALIDATE** (false: that one table can still disagree with the table being salvaged). The comment is
corrected in the same change, because a comment that licenses the defect is where it returns.

- **Given** a `--schema` file, CQL or JSON, and a target table derived from `--table` when given and
  otherwise from the input directory's `<table>-<32-hex-id>` name
- **When** the file's declared table does not match the target
- **Then** the load FAILS CLOSED with a message naming BOTH tables, for **either** file format
  (`json_schema_declaring_another_table_fails_closed_naming_both`, and the pre-existing
  `selecting_absent_table_fails_closed_naming_present_tables` for CQL, in
  `cqlite-cli/src/commands/write.rs`).
- **And** validation is reached by BOTH branches through ONE unconditional post-load step that the
  public entry point cannot return without — selection stays per-format, validation does not — so a
  future third branch cannot reintroduce the gap by omission (`schema_load::assert_table_matches`).
- **And** a matching table still loads, including a case-insensitive match
  (`json_schema_file_resolves_when_its_declared_table_matches_the_target`,
  `json_schema_table_match_is_case_insensitive`), and a caller with no target table is unaffected
  (`json_schema_with_no_target_table_still_loads` — `compact`'s "first table wins" is unchanged).
- **And** the case-folding residual is recorded at the comparison site rather than hidden: Cassandra
  preserves case for QUOTED identifiers, but the JSON schema format has no quoting concept at all,
  so two tables differing only by case in a quoted name compare equal here. Unexpressible in that
  format either way; named, not silently accepted.

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
- **And** (round 23) the `--out` and `--manifest` flags state what R7.8 REFUSES and why — that a
  path is resolved before it is judged, that landing inside the input or inside the run's own planned
  output is refused, and that `<--out>/salvage.json` is the recommended manifest location. Help that
  understates the refusals sends the operator to file a bug against a working guard.

### Requirement: R9 — The salvaged output is a valid SSTable

Every generation salvage writes SHALL pass `cqlite verify --mode full` and MUST be readable by the CQLite reader.

#### Scenario: R9.1 verify and read back
- **Given** every output generation from R7.1 and R7.2
- **When** `cqlite verify --mode full` and `cqlite read-sstable` run on it
- **Then** verify reports zero findings (exit 0) and the read-back row count equals
  `partitions.recovered`'s row total from the manifest.
