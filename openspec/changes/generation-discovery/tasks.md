# generation-discovery — tasks (issue #4626)

Seam 1 first: no task below starts until the owner approves this change. Each task names the surface it
exercises. Commit after each unit (#3042). Each migration PR deletes the copy it replaces.

**Phase closing tasks (repeat at the end of phases 1 to 6).** Each is its own checkbox in the phase.

- Full gate on Linux, detached, summary read from `AGENT_GATE_SUMMARY_FILE` (`tree-integrity`, `dirty: no`,
  `run-id` verified).
- C intent audit by `spec-auditor` against this change.
- Roborev, only through `bash scripts/flow/roborev-review.sh --agent codex --model gpt-5.6-sol --repo $PWD`
  (pushed first; never a bare `roborev`).
- `scripts/flow/premerge-assert.sh <pr> <sha>`, then `gh pr merge --auto --squash --delete-branch`.

## 1. The module plus Cassandra-oracle tests (PR 1)

- [ ] 1.1 Create `cqlite-core/src/storage/sstable/generation/` with its cfg on the `mod` declaration
      (#1712); record the new public surface for the `pub-surface` ratchet. Surface: `cqlite_core` public API.
- [ ] 1.2 Generation id: parse (UUID first; sequence 1-10 digits within `i32`), order by
      `SSTableIdFactory.COMPARATOR`. Surface: unit tests in the module, G5.
- [ ] 1.3 Descriptor-first floor check with no file access (G9). Surface: a test that fails on any open.
- [ ] 1.4 Listing for the four Scopes, one directory each, no recursion, ignored-file set (G8, G13).
      Surface: the module's listing call over committed corpus tables.
- [ ] 1.5 Transaction-log reader and classification (G1, G2), including `TxnInconsistent`. Surface: listing
      over the txn-log fixtures.
- [ ] 1.6 TocMissing and Orphan conditions (G3, G4). Surface: listing over mutated copies.
- [ ] 1.7 Trusted-metadata accessor: floor, bounded read, `statistics_reader/integrity.rs` check, single
      parse, cached value or failure (G11); make the lenient parse private. Surface: accessor tests and a
      counting open shim.
- [ ] 1.8 Next-id allocation over Live + backups, across formats and versions, with the int-range refusal
      (G6, G7). Surface: allocation tests on BIG/BTI/UUID corpora.
- [ ] 1.9 Generate a UUID-id dataset on the local Cassandra 5.0.8 VM (`incus` `wintermute:` remote, see
      `/usr/local/share/wintermute/native-servers.md`) with `uuid_sstable_identifiers_enabled: true`; commit the
      fixtures and update `test-data/cassandra-parity-manifest.yml` and the dataset bundle notes. Surface:
      G5.1 over real Cassandra-written bytes.
- [ ] 1.10 Generate transaction-log fixtures: a committed compaction (`nb_txn_compaction_<uuid>.log` with
      obsolete inputs still on disk), an in-progress one, and an inconsistent one (old files removed, log
      incomplete); plus `snapshots/<tag>` and `backups/` fixtures. Surface: G1, G2, G6.2, G6.3, G8.
- [ ] 1.11 Per-case fail-closed fixtures via `sstables_root_for_table` (#3220); no suite-wide `ran > 0`.
- [ ] 1.12 Phase closing tasks.

## 2. Write engine and compact (PR 2; closes #4622 and #4623)

- [x] 2.1 Replace `WriteEngine::scan_generations` / `determine_next_generation`
      (`write_engine/mod.rs:1548`) with the module's next-id allocation. Surface: `WriteEngine::new` then a
      flush on a directory holding `oa-4-big`, `da-6-bti` and a UUID id (G6, G7).
- [x] 2.2 Replace `discover_input_sstables` / `collect_data_files`
      (`cqlite-cli/src/commands/sstable_inputs.rs`) with the Live set; refuse a format or id form compact
      cannot handle by name; refuse a TocMissing generation by name (G3.2, G10). Surface: `cqlite compact`.
- [x] 2.3 `cqlite compact` never reads `snapshots/` or `backups/` (G8.1). Surface: the binary over a
      directory with both. Closes #4622.
- [x] 2.4 `compact --generation` defaults to the module's next id and never overwrites an existing name
      (the #4623 overwrite hazard); publishing onto an existing name is refused. Surface: `cqlite compact
      --output <live table dir>`. Closes #4623.
- [x] 2.5 Delete the two replaced scanners; the `sweep.rs` and `info.rs` directory listings move to the
      module in this PR if they feed compact's inputs, otherwise in phase 3.
      (Done: neither feeds compact's inputs, so `sweep.rs` and `info.rs` stay for phase 3; the two
      replaced scanners are deleted.)
- [ ] 2.6 Phase closing tasks.

## 3. explain, find, diagnose, diff (PR 3)

- [x] 3.1 `explain_resolve::discover_live_generations` becomes a call to the module (Live, TocMissing
      accepted); `cli-explain` GD5. Surface: `cqlite explain`.
- [x] 3.2 `find.rs` (`:322`) lists through the module; `find-key-probe` GD7. Surface: `cqlite find`.
- [x] 3.3 `diagnose` and `core-diagnose` use the listing and the accessor; GD8, GD9. Surface: `cqlite
      diagnose --out json`.
- [x] 3.4 `diff` resolves both sides through the module; GD6. Surface: `cqlite diff`.
- [x] 3.5 Delete `parse_sstable_filename`'s UUID-skip path in `sstable/directory/scan.rs:128-155` or route it
      to the module if the directory reader still needs it. Surface: `SSTableDirectory` scan over a UUID
      corpus.
- [ ] 3.6 Phase closing tasks.

## 4. rebuild and salvage (PR 4)

- [x] 4.1 `rebuild/simple.rs:87` and `cqlite-cli/src/commands/rebuild.rs:113` list through the module;
      `rebuild-components` GD1 to GD3, `cli-rebuild` GD4. Surface: `cqlite rebuild` over a `ma-1-big` input
      (UnsupportedVersion) and a TOC-deleted copy.
- [x] 4.2 `salvage` lists through the module; `cli-salvage` GD10. Surface: `cqlite salvage`.
- [ ] 4.3 Phase closing tasks.

## 5. merge/mod.rs Statistics reads, and the file split (PR 5)

- [x] 5.1 Move the raw `Statistics.db` reads in `write_engine/merge/mod.rs` to the accessor (G11). Surface:
      `compact_sstables` over a corrupt-Statistics fixture (fails closed as today, #4612).
- [x] 5.2 Split `merge/mod.rs` by responsibility under the #1116/#1135 `file-size` ratchet (no
      `CQLITE_ALLOW_FILE_GROWTH`). Surface: the gate's `file-size` component.
- [ ] 5.3 Phase closing tasks.

## 6. verify (PR 6)

- [x] 6.1 `verify` resolves components and Statistics through the module; `verify-audit` GD11. Surface:
      `cqlite verify` over the `sstable-verify-parity` fixtures; every verdict unchanged.
- [ ] 6.2 Phase closing tasks.

## 7. Done

- [x] 7.1 A grep proves no discovery or raw Statistics.db read survives outside the module:
      `read_dir`-based generation scans, `starts_with("nb-")` filename filters, `Statistics.db` opens, and
      `StatisticsReader` lenient parses, across `cqlite-core/src`, `cqlite-cli/src`, `bindings/` and
      `tools/`. Record the command and its empty output in the final PR.
- [ ] 7.2 Archive the change after merge (`flow-finalize`); close #4622, #4623 and #4626.
