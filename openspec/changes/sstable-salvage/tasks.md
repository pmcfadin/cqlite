# Tasks — sstable-salvage (issue #4196)

Ordered. Group 0 is premises, 1–3 the library, 4 the CLI, 5 the endgame. Commit after every group
(#3042). `--lite` after every fix round; ONE full gate in `flow-closer`.

## 0. Premises — re-confirm in the worktree, STOP and report if false

- [x] 0.1 `IndexReader::get_partition_entries` yields every BIG partition's key + data position in
      file order (index_reader/mod.rs); note the entry field carrying the Data.db offset. CONFIRMED:
      `PartitionIndexEntry.data_offset: u64`, `.raw_key: Option<Arc<[u8]>>`. NOTE: production
      `get_partition_entries` silently truncates on a corrupt entry (see `verify::check_big_index`'s
      doc), so salvage's own boundary walk uses `parse_big_index_entry` directly instead (exhaustive,
      refuses on any parse failure) — see `salvage/boundaries.rs` module doc.
- [x] 0.2 A decode-one-partition-at-offset primitive exists (`SinglePartitionCompaction` on the
      point-read path) and returns `Err` on a mid-partition decode failure (#3782 fix in place).
      CONFIRMED: `point_compaction.rs`'s `seek_partition_compaction_rows` is the pattern; salvage adds
      its own `decode_partition_at_offset_for_salvage` (same decoder, classified-not-fallback outcome)
      alongside it, `not(tombstones)` like the primitive it wraps.
- [x] 0.3 BTI: a full `Partitions.db` trie walk yielding (key, data position) ALREADY EXISTS —
      `iterate_partitions_in_bti_file` (traversal.rs) + `resolve_rows_db_entry_uncounted` (rows.rs)
      for `RowsOffset` leaves, mirroring `verify::check_bti_structure`'s resolution. Task 1.3 is NOT
      needed as a new primitive.
- [x] 0.4 Corruption corpus present under the fetched root (`test_comp_corrupt/*`, verified: 17
      subdirectories incl. `data_db_bit_flip`, `uncompressed_data_bit_flip`, `data_db_truncation`,
      `index_db_bit_flip_big`, `bti_partitions_footer_flip`, `bti_rows_truncation`), and
      `corrupt_byte_fixture.rs` stages BIG + BTI mutated copies as documented.
- [x] 0.5 `compact_sstables` with `purge_safe=false, gc_before=None, now=None` over ONE input
      produces a complete generation (R1's oracle). CONFIRMED signature at merge/mod.rs:1388.

## 1. Boundary enumeration + chunk pre-flight — surface: `write_engine::salvage`

- [x] 1.1 `salvage/mod.rs`: `SalvageOptions`, `SalvageReport`, `Loss`, `LossClass`, `Refusal`
      (D5 shape, serde). `pub mod salvage;` declared in `write_engine/mod.rs`
      `#[cfg(all(feature = "write-support", not(feature = "tombstones")))]` — NOT gated a second time
      inside the module (pub-surface guard's actual concern); the `not(tombstones)` clause is
      inherited from the point-read decode primitive the recovery loop is built on (see design note
      below task 2.1).
- [x] 1.2 `salvage/boundaries.rs`: `BoundarySourceKind::{Index, BtiTrie}` → `Vec<BoundaryEntry{expected_key,
      data_offset}>`; unreadable ⇒ `Refusal::BoundarySourceUnreadable`.
- [x] 1.3 N/A — premise 0.3 found the primitive already exists; reused directly.
- [x] 1.4 `salvage/chunks.rs`: bad-chunk set from inline CRCs / `CRC.db`; partition→chunk-range
      mapping from CompressionInfo chunk length or CRC.db chunk size.
- [x] 1.5 `--lite`; commit.

## 2. Recovery loop + writer — surface: `salvage_sstable`

- [x] 2.1 `salvage/recover.rs`: per partition decode-at-offset → key cross-check → chunk-set
      check → `build_merge_entry` + one-shot `KWayMerger::from_row_iterators` reconcile (reusing the
      SAME reconciliation `compact_sstables` uses, not a hand-rolled shadowing rule) →
      `merge_entry_to_mutation` → `SSTableWriter::write_partition`; loss on any anomaly;
      `rows_decoded_before_failure` captured; D2 atomicity by construction (mutations are built only
      after the whole partition decoded — nothing is written on any error path).
      DESIGN NOTE: the decode-at-offset primitive
      (`SSTableReader::decode_partition_at_offset_for_salvage`,
      `reader/data_access/point_compaction.rs`) lives in a module gated
      `#[cfg(not(feature = "tombstones"))]` (every existing consumer of the point-read seek machinery
      is gated the same way). `salvage` inherits that gate rather than reimplementing a
      tombstones-feature decode path — `tombstones` is a non-default feature with no default/CLI
      build enabling it, so the shipped `cqlite salvage` binary is unaffected; only `cqlite-core`'s
      own `--all-features` gate lane sees `salvage` absent, and does so cleanly (verified: `cargo
      check -p cqlite-core --all-features --all-targets` passes with the module compiled out).
- [x] 2.2 Writer setup mirrors compaction's (`with_format`, `set_repair_state` from source stats
      when readable, `mark_compaction_output`). DEFERRED: the `.salvage-incomplete` marker (D3's
      "interrupted runs" clause) — not yet implemented; tracked as a follow-up nit, not blocking
      R1–R9 correctness (the writer's own TOC-last completion already makes a partial output dir
      detectable by ABSENCE of TOC.txt, which is the same signal `compact_sstables` relies on).
- [ ] 2.3 Tests: `issue_4196_salvage_healthy_parity.rs` (R1.1, R1.2),
      `issue_4196_salvage_corruption_corpus.rs` (R2.1–R2.3, R4.1, R5.2),
      `issue_4196_salvage_partition_atomicity.rs` (R2.4, R3.1, R4.2), R5.1 listing helper shared.
      Expected loss sets computed from the CLEAN source's positions + chunk table in the test.
- [ ] 2.4 `scripts/tests/test_salvage_no_resync_scan.sh` (R4.3), registered in `tooling-tests`.
- [ ] 2.5 Name the new targets in the gate's `write-tests` list (#3522); confirm they EXECUTE.
- [ ] 2.6 `--lite`; commit; push.

## 3. Review-first (library half)

- [ ] 3.1 `rust-reviewer`; `bash scripts/flow/roborev-review.sh --agent <agent> --model <model>`;
      blockers → fix → `--lite` → re-review.

## 4. `cqlite salvage` (R7–R9) — surface: the built binary

- [ ] 4.1 `Commands::Salvage { input, out, manifest, out_format }`; help per R8.2.
- [ ] 4.2 `commands/salvage.rs`: schema via `--schema`; table dir ⇒ generations discovered with
      `write.rs`'s ordering helper, salvaged one by one; exit codes per D3; manifest to
      `--manifest` or stdout (json) / stderr summary (text).
- [ ] 4.3 `cqlite-cli/tests/salvage_cli_tests.rs` (R7.1–R7.4, R8.1, R9.1) + committed expected
      manifests under `cqlite-cli/tests/fixtures/salvage/`. Name the target in `cli-tests`.
- [ ] 4.4 R6.1: add salvage over `test_wide_rows` to the `memory-budget` lane's case list.
- [ ] 4.5 Docs: `dev-cookbook.md` (one entry), website CLI page if it lists verbs; epic #4192
      checklist tick on merge.
- [ ] 4.6 `--lite`; commit; push; review-first on the CLI half (rust-reviewer + roborev); open PR.

## 5. Endgame — `flow-closer`

- [ ] 5.1 Rebase; ONE full gate (`AGENT_GATE_SUMMARY_FILE` redirect); `RESULT: PASS`, tree
      integrity clean, own run-id.
- [ ] 5.2 C: `spec-auditor` vs `openspec/changes/sstable-salvage/specs/**`.
- [ ] 5.3 Roborev LAST via the sanctioned script; PASS.
- [ ] 5.4 `premerge-assert`; `HOLD:` re-read; `gh pr merge --auto --squash --delete-branch`.
- [ ] 5.5 `flow-finalize`: archive, telemetry via PR-in-worktree, close #4196, tick #4192.
