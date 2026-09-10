# Tasks — sstable-salvage (issue #4196)

Ordered. Group 0 is premises, 1–3 the library, 4 the CLI, 5 the endgame. Commit after every group
(#3042). `--lite` after every fix round; ONE full gate in `flow-closer`.

## 0. Premises — re-confirm in the worktree, STOP and report if false

- [ ] 0.1 `IndexReader::get_partition_entries` yields every BIG partition's key + data position in
      file order (index_reader/mod.rs); note the entry field carrying the Data.db offset.
- [ ] 0.2 A decode-one-partition-at-offset primitive exists (`SinglePartitionCompaction` on the
      point-read path) and returns `Err` on a mid-partition decode failure (#3782 fix in place).
- [ ] 0.3 BTI: does a full `Partitions.db` trie walk yielding (key, data position) exist under
      `storage/sstable/bti/`? If not, it is task 1.3 (a bounded new primitive; cite the definitive
      guide Ch.17 and `cassandra-5.0.8` `PartitionIndex` for the leaf payload layout).
- [ ] 0.4 Corruption corpus present under the fetched root (`test_comp_corrupt/*`), and
      `corrupt_byte_fixture.rs` stages BIG + BTI mutated copies as documented.
- [ ] 0.5 `compact_sstables` with `purge_safe=false, gc_before=None, now=None` over ONE input
      produces a complete generation (R1's oracle).

## 1. Boundary enumeration + chunk pre-flight — surface: `write_engine::salvage`

- [ ] 1.1 `salvage/mod.rs`: `SalvageOptions`, `SalvageReport`, `Loss`, `LossClass`, `Refusal`
      (D5 shape, serde). `pub mod salvage;` unconditional.
- [ ] 1.2 `salvage/boundaries.rs`: `BoundarySource::{Index, BtiTrie}` → `Vec<(key, offset)>`;
      unreadable ⇒ `Refusal::BoundarySourceUnreadable`.
- [ ] 1.3 (if 0.3 says absent) BTI trie full walk.
- [ ] 1.4 `salvage/chunks.rs`: bad-chunk set from inline CRCs / `CRC.db`; partition→chunk-range
      mapping from CompressionInfo chunk length or CRC.db chunk size.
- [ ] 1.5 `--lite`; commit.

## 2. Recovery loop + writer — surface: `salvage_sstable`

- [ ] 2.1 `salvage/recover.rs`: per partition decode-at-offset → key cross-check → chunk-set
      check → `merge_entry_to_mutation` → `SSTableWriter::write_partition`; loss on any `Err`;
      `rows_decoded_before_failure` captured; D2 atomicity by construction (write only after the
      whole partition decoded).
- [ ] 2.2 Writer setup mirrors compaction's (`with_format`, `set_repair_state` from source stats
      when readable, `mark_compaction_output`); `.salvage-incomplete` marker until finish.
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
