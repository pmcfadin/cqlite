# Epic #842 — byte-for-byte compaction parity — progress ledger

Branch: worktree-epic-842-byte-parity

- #886 foundation: complete (commit 62d1a34d, roborev job 707 PASS, gate green)

- #844 (per-cell collection union): IMPLEMENTED then REVERTED (commit 1c997021 reverted by 20b879ca). roborev job 716 FAIL (timestamp promotion + complex-deletion resurrection). Root cause: reader collapses collections / row-timestamp granular. DEFERRED.
- #849 (clustering reversal): impl 9b4943f0; roborev job 718 FAIL (1 Medium: absent trailing component); fix c961cefd; re-review in flight.
- DECISION (user): defer per-cell cluster, ship the rest.
- #899 filed = real per-element/per-cell foundation. Blocks #844/#846/#848/#887/#888 (all commented + epic updated). Tasks 2-6 deleted.
- Shipping set: #845, #847, #849, #850, #851, #852, #853, #889.
- Tracks: A(merge.rs→data_writer): #845→#847→#850→#853 ; B(writer mod.rs/filter): #851→#852.

- #845 (gc purge): BLOCKED → deferred to #899 (needs tombstone localDeletionTime seconds, discarded at merge boundary). Task 7 deleted, #845 commented, #899 updated.
- Track A reorder: #853 → #850 → #847 (data_writer/merge, verify-first). Track B: #851(running) → #852.

- #853 (marker size): impl cc4748da. Gate FAIL only on test_flush_throughput (perf flake under concurrent load); passes in isolation. Needs clean gate re-run + roborev. Concern: same i64-widening in range-bound + tombstone-cell paths (out of scope; possible follow-up).
- LESSON: concurrent full-gate implementers flake test_flush_throughput. SERIALIZE implementers from here.

- #853 (marker size): clean gate PASS over HEAD; roborev job 729 PASS. COMPLETE (commit cc4748da).
- #851 (static stats): impl 9903779c; roborev job 728 FAIL (1 Medium: guard over-suppresses pure-PK live rows → totalRows undercount; test used no-static schema). FIX QUEUED behind #852 (both touch writer mod.rs — must serialize).
- #852 (disabled bloom): implementing.

- #852 (disabled bloom): impl fee39077; roborev job 731 FAIL (2 Medium: (1) SSTableInfo.filter_path mandatory → compaction rename fails for disabled filter, needs optional + compaction test; (2) CQL parser drops WITH options so fp_chance=1.0 from CREATE TABLE falls back to 0.01, needs table-options plumbing). FIX QUEUED behind #851-fix (both touch sstable/writer/mod.rs).

- #851 re-review (job 736) FAIL again (2 Medium): per-mutation stats loop still diverges from DataWriter::merge_row_group (counts key-col ops; misses static+regular = 2 rows). NEXT FIX: derive stats from DataWriter's actual row/col emission (single source of truth), not a parallel re-derivation. HOLD behind #852 fix (shared sstable/writer/mod.rs).

- #852 fix (73a65c95) re-review job 741 FAIL (1 Medium): cql_parser table_options can't skip map/list-valued options before bloom_filter_fp_chance → fallback to 0.01. NEXT FIX in cql_parser.rs only (disjoint from #851 fix). Running concurrently.
- #851 robust fix (3rd attempt) running: derive stats from DataWriter emission.

- #852 parser-fix agent DIED (API error, 5 tool uses); left 47-line partial cql_parser.rs edit → DISCARDED (git checkout). Will redo cleanly after #851 fix commits. No #852 parser commit exists yet (still need to fix job 741 finding).

- #851 re-review (job 748) FAIL (1 Medium): PartitionEmitCounts uses ops.len()/merged.len() but write_merged_cells SKIPS null-valued Write ops → totalColumnsSet drift on INSERT(...,null). NEXT FIX: count cells where physically written (write_merged_cells returns actual count). HOLD until #852 parser fix commits (then run alone, clean gate). 4th #851 attempt.

- #852 COMPLETE: commits fee39077 + 73a65c95 + 1daa4417 (final review job 750 PASS). All findings resolved.
- #851 4th fix running (count cells physically written).

- #851 COMPLETE after 4 fix iterations: commits 9903779c→5afce78c→87f25b5c→60070d5d (final review job 752 PASS). Stats now counted at physical cell-write — cannot drift from Data.db.
- Starting #850 (static presence from headers), verify-first, alone.

- #889 COMPLETE: d3fc656d + fix b3300079 (final review job 758 PASS). CI workflow wired, logical tier hard gate, byte tier opt-in hook.
- #850 still running (now editing merge.rs/write_engine/mod.rs + rules doc — awaiting report).

- #847 (dropped-column filter): BLOCKED → deferred to NEW prereq #904 (no drop-time metadata anywhere in CQLite; distinct from #899). Task 8 deleted, #847 commented, epic updated.
- #850 review in flight (commit 769b7834).

- #850 review loop: fix1 4ee9808d (job 760 FAIL static-only partition) → fix2 b9fa59f2 (job 762 FAIL static+tombstone; added reconcile guard) → job 766 FAIL (guard over-suppresses tombstone-only None bucket) → fix3 running (bounded: suppress shadowing only when static LIVE cells present).
- Filed #912 (latent: clustering-row tombstones lose clustering identity → None bucket). Noted on epic.

- #850: DEFERRED to #912 after 5 review iterations (job 760/762/766/768/772). None-bucket conflation of static row + clustering-row tombstone reappears through reconcile→mutation→writer; needs #912 clustering-identity plumbing. Reset branch to b3300079 (dropped 769b7834/4ee9808d/b9fa59f2/c3e07e50/0ee4632c — recoverable from reflog). Task 9 deleted, #850/#912/epic commented.

=== SHIPPABLE SET COMPLETE (all reviewed clean) ===
- #886 (62d1a34d, job 707), #849 (9b4943f0+c961cefd, job 721), #853 (cc4748da, job 729), #851 (9903779c+5afce78c+87f25b5c+60070d5d, job 752), #852 (fee39077+73a65c95+1daa4417, job 750), #889 (d3fc656d+b3300079, job 758).
- DEFERRED w/ tracking: #844/#846/#848/#887/#888 → #899; #845 → #899; #847 → #904; #850 → #912.
- NEXT: final whole-branch review, then merge to main + close issues + close epic.
