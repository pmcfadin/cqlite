# Tasks — corruption-locator (issue #4194)

Ordered. Group 0 is premises, 1–2 the library, 3 the CLI, 4 review-first, 5 the endgame. Commit after
every group (#3042). `--lite` after every fix round; ONE full gate in `flow-closer`.

## 0. Premises — re-confirm in the worktree, STOP and report if false

- [x] 0.1 `IndexReader::get_partition_entries` yields every BIG partition's key + `data_offset: u64`
      + a size/next-offset companion sufficient to build a closed `[start, end)` extent per
      partition (`index_reader/mod.rs:264`, `PartitionIndexEntry` at line 49). Confirmed the exact
      field name used for the extent's upper bound: `data_size` is always 0 (a parser placeholder)
      and is NOT used; extents are built from sorted successor `data_offset`s + the logical end.
- [x] 0.2 `CompressionInfo::chunk_for_offset` (`compression_info.rs:301`) and `chunk_length: u32`
      (line 70) are sufficient to map a `Data.db` byte offset to its compressed chunk index and that
      chunk's byte range. Confirm no rounding/edge case at the last (possibly short) chunk. Confirmed
      `chunk_for_offset`/`chunk_length` give the LOGICAL range only; the PHYSICAL range comes from
      `compressed_chunk_offset`/`compressed_chunk_size`, used separately for `byte_offset`/`byte_len`.
- [x] 0.3 `CRC_CHUNK_SIZE = 64 * 1024` (`writer/crc_writer.rs:67`) is the CQLite writer's DEFAULT
      only; read-side location uses the per-file `CrcDb::chunk_size()` header (confirmed in
      `check_uncompressed_crc_db`).
- [x] 0.4 BTI: `iterate_partitions_in_bti_file` (`bti/parser/traversal.rs:384`) yields every
      partition's key + `BtiPartitionLocation` in byte-comparable/file order, and is ALREADY used by
      `verify.rs`'s own FULL-mode checks (confirmed present by this activation's read of both files
      — no new BTI primitive is needed, matching sibling change #4196's premise 0.3 finding).
      Re-confirm `BtiPartitionLocation`'s exact fields carry enough to build a `[offset, next_offset)`
      extent (next entry's offset, or EOF for the last). Confirmed `RowsOffset` leaves resolve their
      raw key + `Data.db` position via `Rows.db` (`BtiResolvedLeaf::inline_raw_key`), never via the
      raw trie offset directly.
- [x] 0.5 Corruption corpus present under the fetched root (`test_comp_corrupt/*`, this activation
      verified 12 fixture entries in `corruption-manifest.yml` incl. `data_db_bit_flip`,
      `uncompressed_data_bit_flip`, `data_db_truncation`, `index_db_bit_flip_big`,
      `bti_partitions_footer_flip`, `bti_rows_truncation`, `filter_db_bit_flip`), and
      `corrupt_byte_fixture.rs` exposes `stage_control_and_mutated`, `BIG_COMPOSITE`,
      `BTI_MULTICLUSTERING`, and `index_partition_positions(dir) -> Vec<(Vec<u8>, usize)>` exactly as
      design.md assumes.
- [x] 0.6 `verify.rs` is 2761 lines today (well over the ~800-line campsite target) — re-measure at
      task-start time (it may have grown further) and confirm the file-size ratchet posture in
      design.md §D4 (new logic in `verify_location.rs`, minimal touch to `verify.rs` itself, document
      `CQLITE_ALLOW_FILE_GROWTH=1` + epic #1116 if any net growth remains unavoidable).

## 1. Location types + BIG resolution — surface: `storage::sstable::verify_location`

- [x] 1.1 `verify_location.rs` (new file): `Location`, `PartitionResolution`, `KeyRef` (proposal.md
      shape, `derive(Debug, Clone, PartialEq, Eq)` to match `VerifyFinding`'s existing derives).
      Declared `mod verify_location;` in `sstable/mod.rs` next to `pub mod verify;`; `pub use` the
      three types from `verify.rs` (or re-export directly) so `VerifyFinding.location`'s type is
      reachable through the existing `verify` module path — no caller-visible new import path is
      required for the common case.
- [x] 1.2/1.3 Implemented as ONE unified `resolve_partitions(damaged, sorted_boundary_entries,
      logical_len) -> PartitionResolution` over a generic `BoundaryEntry = (u64, Option<Arc<[u8]>>)`
      (a `(position, raw_key)` pair), rather than separate `resolve_big`/`resolve_bti` functions —
      the half-open-interval intersection logic is identical for both formats once each format's
      boundary source is mapped to that shape; `verify.rs`'s `finalize_locations` does the per-format
      mapping (BIG via `IndexReader`, BTI via the already-resolved `bti_leaves`). Boundary source
      unreadable (its OWN `IndexEntryCorrupt` / `BtiRootPointerCorrupt` / `BtiTrieCorrupt` finding
      present, OR — round-1 finding — `IndexReader::is_fully_parsed()` disagrees with
      `check_big_index`'s own structural walk) ⇒ `Unresolved("boundary-source-unreadable")` for
      every OTHER finding in the same report (§D2).
- [x] 1.4 Wire `verify.rs`: add `location: Option<Location>` to `VerifyFinding`, populate it at each
      chunk/offset-anchored finding call site (`ChunkDecompressionError`,
      `UncompressedChunkCrcMismatch`, truncation-classified findings), leave every other
      `VerifyFinding::new` call passing `location: None` unchanged in behavior.
- [x] 1.5 `--lite`; commit.

## 2. Tests — surface: `verify_sstable`'s `location` field

- [x] 2.1 `cqlite-core/tests/issue_4194_verify_location.rs` (L1.1–L1.4, L2.1–L2.3): expected
      partition sets computed independently in the test from the CLEAN source's `Index.db`
      positions / BTI trie + chunk table — never from `verify_sstable`'s own output on the corrupt
      copy. Corpus gating per #1094 (`CQLITE_REQUIRE_FIXTURES=1` hard-requires).
- [x] 2.2 `scripts/tests/test_verify_location_no_resync_scan.sh` (L3.1), registered in
      `tooling-tests`, modeled directly on `sstable-salvage`'s
      `test_salvage_no_resync_scan.sh`.
- [x] 2.3 Confirm `sstable_parity_corruption_verify.rs` (unmodified) still passes unchanged (L4.1) —
      this is a regression check, not new test code.
- [x] 2.4 `cqlite-cli/src/commands/verify.rs`: extend `print_text`/`print_json` to render `location`
      when present (L4.2). `cqlite-cli/tests/verify_location_cli_tests.rs` (L4.2), named in the
      gate's `cli-tests` list per #3522 — confirm it is picked up by the `cqlite-cli/tests/*.rs` glob
      with no `required-features` needed (pure read path, no `write-support` dependency).
- [x] 2.5 Confirm the new `cqlite-core` target executes under `core-tests` (feature `cli-helpers`,
      #3522) — no gate-script edit expected since both `core-tests` and `cli-tests` auto-cover new
      files; if either turns out to need an explicit list edit (e.g. a quarantine), do it here and
      say so.
- [x] 2.6 `--lite`; commit; push.

## 3. `cqlite sweep` (S1–S4) — surface: the built binary

- [x] 3.1 `Commands::Sweep(SweepArgs { data_dir, mode, out, jobs })` in `cli_types.rs`; reuse
      `VerifyModeArg`/`VerifyOutputArg` (identical value sets, per design.md §D4 — no new arg enums).
      `long_about` states: walks every table dir under `<data-dir>`, one row per table, severity
      taxonomy, exit-code contract (mirrors `verify`'s existing `long_about` style).
- [x] 3.2 `cqlite-cli/src/commands/sweep.rs` (new file, wired like `read_commitlog.rs`): directory
      walk (readdir over `<data-dir>`, one row per `<ks>/<table>-<id>/` found), `--jobs`-bounded
      concurrent `verify_sstable` calls (a semaphore or bounded task pool — no data-dir-wide
      structure materialized ahead of rendering), severity mapping (D3: `FilterFalseNegative`-only
      ⇒ `degraded`, otherwise any finding ⇒ `corrupt`, unopenable/no-`Data.db` ⇒ `unreadable`), exit
      codes via `std::process::exit` per S2 (mirrors `verify`'s and `sstable-salvage`'s established
      pattern).
- [x] 3.3 `cqlite-cli/tests/sweep_cli_tests.rs` (S1.1–S1.4, S2.1–S2.2, S4.1–S4.2) against the real
      compiled binary via `CARGO_BIN_EXE_cqlite` (exit-code assertions need a subprocess). Named in
      the gate's `cli-tests` list per #3522.
- [ ] 3.4 `cqlite-core/tests` or a dedicated dhat-instrumented target for S3.1 (`memory-budget` gate
      lane over `test_wide_rows`, `--jobs 1`) — follow the existing `memory-budget` component's
      pattern for a new dataset-dependent dhat case; name the exact target added.
- [x] 3.5 `cqlite-cli/tests/sweep_cli_tests.rs` (or a sibling) for S3.2 (`--jobs 1` vs `--jobs 4`
      identical rows/exit-code) — this asserts determinism under concurrency, not a timing bound.
- [x] 3.6 Docs: `dev-cookbook.md` "Sweep a data directory for corruption" entry.
- [x] 3.7 `--lite`; commit; push.

## 4. Review-first

- [x] 4.1 roborev on the lite-green diff BEFORE any full gate (default per CLAUDE.md's implement
      loop; `rust-reviewer` NOT run — no `Agent`/`Task` tool available in the implementing session).
      Three rounds run (`--agent claude-code --model claude-opus-5`, the repo's configured default
      `codex` agent being broken on the box independent of this diff — see the PR body); 18 findings
      across rounds 1-2 fixed and verified, round 3's 7 findings addressed in this commit. Pre-empted
      the standing findings classes: GHA injection (n/a — no workflow files touched), integer
      overflow in offset/chunk-index arithmetic (`saturating_mul`/`saturating_add` throughout), no-
      heuristics (the no-resync-scan guard from task 2.2 covers this mechanically).

## 5. Endgame — `flow-closer`

- [ ] 5.1 Rebase; ONE full gate (`AGENT_GATE_SUMMARY_FILE` redirect); `RESULT: PASS`, tree integrity
      clean, own run-id. NOT RUN by the implementing session — `--lite` only (per the gate/closer
      division of labor); the closer runs the full gate of record.
- [ ] 5.2 C: `spec-auditor` vs `openspec/changes/corruption-locator/specs/**`.
- [ ] 5.3 Roborev LAST via the sanctioned script; a FRESH round after the full gate (any rebase voids
      the three implementer-session rounds above).
- [ ] 5.4 `premerge-assert`; `HOLD:` re-read; `gh pr merge --auto --squash --delete-branch`.
- [ ] 5.5 `flow-finalize`: archive, telemetry via PR-in-worktree, close #4194, tick #4192.
