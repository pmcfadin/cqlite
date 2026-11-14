# Tasks — find-key-probe (issue #4205)

Ordered. Group 0 is premises, 1 the SSTable-side probe, 2 the CommitLog decode extension, 3 fixtures,
4 tests, 5 the CLI, 6 review-first, 7 the endgame. Commit after every group (#3042). `--lite` after
every fix round; ONE full gate in `flow-closer`.

## 0. Premises — re-confirm in the worktree, STOP and report if false

- [x] 0.1 Re-read `partition_lookup.rs::might_contain_partition`, `point_compaction.rs:: — DONE: BIG consults `bloom_filter` directly; BTI IS `lookup_partition_via_bti_trie` (re-read at 7f9abe36).
      read_single_partition_for_compaction`, and `point_read.rs`'s `PathProbe`/`probe_reader_async`
      against `origin/main` at task-start time (this activation read them 2026-09-23; #4193/#4195 may
      have touched neighboring code by then). Re-confirm design.md §D1's claim: BIG's
      `might_contain_partition` consults `Filter.db` directly; BTI's IS the trie descent
      (`lookup_partition_via_bti_trie`), with no separate bloom check.
- [x] 0.2 Locate the exact "fail-safe scan, filtered to a key set" primitive the production — DONE: the production fail-safe scan is `stream_all_partitions_for_compaction` (the `NeedsScan` arm builds an adapter over it); reused via its `_observed` form, which also reports the partition offset.
      `IndexUnavailable`/`NeedsScan` path already drives (`point_read.rs`'s `probe_reader_async`
      degrades to `NeedsScan`, and the k-way merger scans it — trace exactly which function performs
      the actual filtered scan) so `find`'s `Scanned` outcome reuses it rather than re-implementing a
      second scan (design.md §D1's "must not become a second scan implementation").
- [x] 0.3 Confirm `CompactionRowData`'s partition-tombstone variant's exact field name(s) for its — DONE: `CompactionRowData::PartitionDelete { deletion_time, .. }` (also `RangeMarker`/`Tombstone` `deletion_time`, `Live.row_deletion`).
      deletion timestamp (design.md §D2 flagged this as not yet traced precisely).
- [x] 0.4 Verify the CommitLog delta-timestamp reconstruction (design.md §D4) against — DONE, DESIGN CORRECTED: `EncodingStats.Serializer` writes `minTimestamp - TIMESTAMP_EPOCH` (2015-09-22T00:00:00Z = 1442880000000000 µs); a row/cell timestamp is `minTimestamp + uvint delta` (`SerializationHeader.readTimestamp`). Design D4 omitted the epoch; verified against the committed Cassandra-written `commitlog_find` fixture (explicit `USING TIMESTAMP`, exact equality).
      `cassandra-5.0.8` source — `EncodingStats.java`, `UnfilteredSerializer.java`'s
      `LivenessInfo`/`Cell` (de)serialize methods, `Cell.Serializer` — via
      `git show cassandra-5.0.8:<path>` at `$CQLITE_CASSANDRA_REPO` if set, else the GitHub tree at
      that tag. Confirm `actual = min_timestamp + delta` (non-negative uvint offset) is the real
      convention, not merely CQLite's own never-executed code shape. Correct design.md §D4 before
      task 2 if the real convention differs.
- [x] 0.5 Confirm `FrameWalker`'s exact cursor field to capture as a record's byte offset — DONE: `FrameWalker.cursor` at the record header is the record offset (absolute in the segment file).
      (design.md §D3) — re-read `frame.rs::next_frame` fully (this activation read only the struct
      fields, not the full method body).
- [x] 0.6 Confirm no `commitlog_test`-keyspace SSTable fixture exists today (this activation found — DONE: no `commitlog_test` SSTable existed.
      none) — if one has since been added, re-scope task 3 to extend rather than create it.
- [x] 0.7 Re-measure `mutation.rs` (779 lines at refresh), `point_compaction.rs`, and — DONE: `mutation.rs` 779 lines -> its inline tests were moved to `mutation_tests.rs` (505 lines) so the additions fit; `point_compaction.rs` (918) changed by one visibility keyword only.
      `point_read.rs` against the ~800-line campsite target before assuming no split is needed
      (design.md §D7).

## 1. SSTable-side probe — surface: a new `find_probe` module

- [x] 1.1 `GenerationProbe`/`HitDetail`/`ByteLength`/`ScanOutcome` (proposal.md's shape, — DONE in `reader/data_access/find_probe.rs`. DEVIATION (additive, #4159 doctrine): `HitDetail.data_offset: Option<u64>` and `ByteLength::Unmeasured(cause)` — a fail-safe-scan hit resolves no end bound (and may report no boundary), and a guess is forbidden.
      `derive(Debug, Clone, PartialEq)`), in a new file under `cqlite-core/src/storage/sstable/`
      (task 0.7/general judgment picks the exact path — `find_probe.rs` or under
      `reader/data_access/`).
- [x] 1.2 `probe_generation` (design.md §D1): BIG branch (bloom check, Index.db lookup, decode,
      classify) and BTI branch (trie descent, decode, prefix-collision → `IndexMiss` per §D1.1).
- [x] 1.3 `HitDetail` construction from the decoded `CompactionRow`s (design.md §D2): offset,
      byte_length (Known/ToEof), max_writetime (incl. the partition-tombstone variant per 0.3),
      has_partition_deletion.
- [x] 1.4 Wire the fail-safe scan reuse from 0.2 for the `Scanned` outcome.
- [x] 1.5 `--lite`; commit.

## 2. CommitLog decode extension — surface: `storage::commitlog::mutation`/`reader`/`frame`

- [x] 2.1 `PartitionUpdate.min_timestamp: i64` (promoted); `DecodedRow.writetime: Option<i64>`; — DONE. DEVIATION: `PartitionUpdate.min_timestamp: Option<i64>` (an empty-partition carrier has no EncodingStats; never 0) plus `undecoded_cause` naming why rows were not decoded; `DecodedRow` also gains `deletion_timestamp`.
      `DecodedCell.writetime: Option<i64>` (design.md §D4, corrected per 0.4's finding if the delta
      convention differs from the initial assumption).
- [x] 2.2 `FrameStep::Record { offset: usize, body: &'a [u8] }` (design.md §D3); `MutationIter::next()` — DONE (`FrameStep::Record { offset, body }`, `MutationIter` yields `(u64, Result<Mutation>)`; 5 call sites updated).
      yields `(u64, Result<Mutation>)`. Update the ONE existing call site,
      `cqlite-cli/src/commands/read_commitlog.rs`.
- [x] 2.3 A closed `mutation_writetime(update: &PartitionUpdate) -> Writetime` helper (design.md §D4's
      "max over row/cell writetimes and deletion mfda" rule) — `Writetime::Known(i64) |
      Unmeasured(&'static str)`, naming the cause for partition-deletion / clustered / complex-column
      mutations.
- [x] 2.4 `--lite`; commit.

## 3. Fixtures

- [x] 3.1 Extend `test-data/scripts/generate-commitlog-fixtures.sh` (or a sibling script) to ALSO
      flush a SUBSET of `commitlog_test.users`' committed inserts into a matching SSTable set
      (`test-data/datasets/sstables/commitlog_test/users-.../`), leaving at least one insert
      genuinely unflushed — commit both the SSTable dir and confirm the existing CommitLog segment(s)
      still cover the unflushed row(s) (per task 0.6's finding).
- [x] 3.2 Record the exact expected `(segment, position, writetime, unflushed)` per mutation in a — DONE: `commitlog-find-ground-truth.json` (position from an independent Python frame walker, writetime = the explicit INSERT timestamp, flushed flag).
      committed ground-truth extension (alongside or extending `commitlog-ground-truth.json`) — the
      test computes its expectations from this file, never from `find`'s own output.
- [x] 3.3 Commit; `.sha256`/binary conventions matching the existing CommitLog fixture set.

## 4. Tests — surface: `probe_generation` + `find`'s commitlog path

- [x] 4.1 `cqlite-core/tests/issue_4205_find_corpus.rs`: for every key in a table's JSONL goldens
      (per-table roots via `sstables_root_for_table`, #3220), `probe_generation` reports `Hit` in
      exactly the generations whose golden contains the key, `BloomNegative`/`IndexMiss` elsewhere —
      asserted per case, both BIG and BTI tables in the corpus. Corpus gating per #1094
      (`CQLITE_REQUIRE_FIXTURES=1` hard-requires).
- [x] 4.2 A key absent from every generation on a bloom-present table → `BloomNegative` or
      `IndexMiss`, never `Hit`; the SAME table with `Filter.db` deleted (temp copy) → `IndexMiss`
      (never `unmeasured`) — pins proposal.md establishment 2 / AC2.
- [x] 4.3 CommitLog: using 3.1/3.2's fixture, the unflushed insert → `unflushed: yes` with correct
      segment/position/writetime; the flushed ones → `unflushed: no`.
- [x] 4.4 A clustered-table (or complex-column) CommitLog mutation → `writetime: unmeasured(<cause>)`, — DONE at unit level (`mutation_tests.rs`: clustered / complex / partition-deletion / no-schema causes) and end to end in `find_cli_tests.rs` (clustered declaration of the committed segment).
      `unflushed: unmeasured(<cause>)`, never a guess (design.md §D4/§D5) — use an existing clustered
      fixture's CommitLog capture if one exists, else a minimal synthetic segment via the existing
      test helpers in `mutation.rs`'s own `#[cfg(test)]` module (matches how that module already
      builds synthetic frames for its unit tests).
- [x] 4.5 Confirm existing suites unaffected: `mutation.rs`'s own unit tests, `read_commitlog.rs`'s
      CLI tests (position field added, existing assertions unaffected by the additive change).
- [x] 4.6 Confirm the new `cqlite-core` target executes under `core-tests` (#3522) — no gate-script — DONE: both new targets are picked up by the existing `tests/*.rs` derivation of `core-tests`/`cli-tests`; no gate edit needed for them. The F6.1 guard is registered in `roborev-lints` (see 4.1 note).
      edit expected (glob auto-coverage); say so if one is needed.
- [x] 4.7 `--lite`; commit; push.

## 5. CLI — surface: the built binary

- [x] 5.1 `Commands::Find(FindArgs { table_dir, partition_key, commitlog: Option<PathBuf>, out })` in
      `cli_types.rs`; partition-key literal parsing reused from `query`/`explain`.
- [x] 5.2 `cqlite-cli/src/commands/find.rs` (new file): per-generation + optional `--commitlog`
      rendering (design.md §D6), exit-code wiring.
- [x] 5.3 `cqlite-cli/tests/find_cli_tests.rs`: text + JSON rendering, exit codes, named in the gate's
      `cli-tests` list per #3522.
- [x] 5.4 Docs: `dev-cookbook.md` "Find which generations hold a key" entry.
- [x] 5.5 `--lite`; commit; push.

## 6. Review-first

- [ ] 6.1 roborev on the lite-green diff BEFORE any full gate (`--agent claude-code --model — (lead / flow-closer, outside the implementer)
      claude-opus-5`). Pre-empt: integer overflow in offset/position arithmetic
      (`saturating_add`/`saturating_mul`), no-heuristics (D8's reuse-only primitives), the delta-
      timestamp reconstruction's correctness (flag explicitly for extra scrutiny given 0.4's
      Cassandra-source dependency — this is the change's highest-risk correctness claim).

## 7. Endgame — `flow-closer`

- [ ] 7.1 Rebase; ONE full gate (`AGENT_GATE_SUMMARY_FILE` redirect); `RESULT: PASS`, tree integrity — (lead / flow-closer, outside the implementer)
      clean, own run-id.
- [ ] 7.2 C: `spec-auditor` vs `openspec/changes/find-key-probe/specs/**`. — (lead / flow-closer, outside the implementer)
- [ ] 7.3 Roborev LAST via the sanctioned script; a FRESH round after the full gate. — (lead / flow-closer, outside the implementer)
- [ ] 7.4 `premerge-assert`; `HOLD:` re-read; `gh pr merge --auto --squash --delete-branch`. — (lead / flow-closer, outside the implementer)
- [ ] 7.5 `flow-finalize`: archive, telemetry via PR-in-worktree, close #4205, tick #4192. — (lead / flow-closer, outside the implementer)
