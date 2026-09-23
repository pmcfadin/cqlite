# Tasks — sstable-extract-split (issue #4199)

Ordered. Group 0 is premises, 1 the shared write-guard promotion, 2 extract's library half, 3
split's library half, 4 the CLI, 5 the endgame. Commit after every group (#3042). `--lite` after
every fix round; ONE full gate in `flow-closer`.

## 0. Premises — re-confirm in the worktree, STOP and report if false

- [ ] 0.1 `build_single_partition_merger(paths, keys, schema, scan_cancel) -> Result<Option<KWayMerger>>`
      (`cqlite-core/src/storage/write_engine/merge/point_read.rs:246`) reconciles across every
      generation in `paths` with no purge, and its `MergeStep` loop is the same shape
      `compact_sstables` drives (`merge/mod.rs:1350`). Confirm the exact `MergeStep` variants and
      that `write_partition` accepts what a `MergeStep::Partition` yields directly.
- [ ] 0.2 `decode_partition_at_offset_for_salvage` (`cqlite-core/src/storage/write_engine/salvage/
      recover_helpers.rs`) is the decode-at-offset primitive salvage drives; confirm its signature,
      its partition-atomicity guarantee (D2: a mid-partition failure yields nothing, never a
      prefix), and whether it is `pub(crate)` (needs to be reachable from a new `extract_split`
      sibling module) or needs a visibility bump.
- [ ] 0.3 The BIG boundary walk (`salvage/boundaries.rs`, `BoundarySourceKind`) and the BTI
      `Partitions.db` trie walk (`iterate_partitions_in_bti_file`, `bti/parser/traversal.rs`) both
      yield `(key, data_offset)` in ascending Murmur3-token file order. Confirm this ordering
      claim directly against Cassandra source (`cassandra-5.0.8`, `Index.db`/`Partitions.db` writer)
      — design.md D3.2 depends on it structurally, not just empirically.
- [ ] 0.4 Confirm `cassandra_murmur3_token` (`cqlite-core/src/util/cassandra_murmur3.rs:171`) is the
      SAME token function Index.db/Summary.db ordering and `select_optimizer.rs`'s token-range CQL
      support already use, so `--token-range` reuses one token authority, not a second one.
- [ ] 0.5 Corruption/multi-generation fixtures needed for R2/R6 tests are present under the fetched
      dataset root (`test_tomb`, `test_comp_corrupt/*`, `corrupt_byte_fixture.rs`'s
      `ClusteringTextLiteral` mutation) — confirmed available for BIG; note BTI availability for
      the same mutation or scope BTI split/extract corruption tests to what's fixture-backed.

## 1. Promote the shared write guard (design.md D7) — FIRST, so both new verbs use the hardened one

- [ ] 1.1 Move `cqlite-cli/src/commands/salvage/write_guard.rs` to
      `cqlite-cli/src/commands/write_guard.rs`, widening its `pub(super)` items to `pub(crate)`.
      Pure relocation — no logic change. Update `salvage/mod.rs`'s `use` path.
- [ ] 1.2 Re-run salvage's existing write-guard test suite
      (`cqlite-cli/tests/issue_4196_salvage_write_guard.rs`) unchanged against the moved module to
      confirm the relocation changed nothing observable.
- [ ] 1.3 Commit this group alone before touching extract/split code (#3042 — a clean, reviewable,
      low-risk relocation commit).

## 2. Library — `extract_partitions` (design.md D1, D2)

- [ ] 2.1 New module `cqlite-core/src/storage/write_engine/extract_split/mod.rs` +
      `selection.rs` (D1.1: `Selection` enum, key-literal parsing reusing the existing schema-driven
      literal parser, `--keys-file` line format, token-range-to-key-list resolution via the
      boundary walk).
- [ ] 2.2 `extract_partitions` reconciled mode (D1): resolve `Selection` to a key list per D1.1,
      call `build_single_partition_merger` over every input generation's path, drive its
      `MergeStep` loop into one `SSTableWriter`, run `verify --mode full` on the output before
      declaring success (D5).
- [ ] 2.3 `raw_copy.rs` (D2): `--raw` mode — per-generation boundary walk + filter to the resolved
      selection + `decode_partition_at_offset_for_salvage` + direct `write_partition`, one output
      generation per input generation with a match.
- [ ] 2.4 `not_found` accounting (R3): track which resolved keys were never matched in ANY
      generation across both modes; wire into the report.
- [ ] 2.5 Refusal contract (D5, R6): a decode/CRC failure on a selected partition aborts the whole
      run with no output published (temp-dir-then-publish, mirroring salvage/rebuild's `--out`
      protocol) — never a partial write.
- [ ] 2.6 Unit/integration tests for R1–R3, R5 (extract-half), R6 (extract-half) per
      `specs/extract-split-core/spec.md`; name every new target in the gate's `core-tests`/
      `write-tests` component list (#3522).

## 3. Library — `split_sstable` (design.md D3)

- [ ] 3.1 `split.rs`: single-generation-input resolution (table dir → exactly one generation, else
      a named usage error listing every generation found).
- [ ] 3.2 Sequential boundary walk driving N `SSTableWriter`s per D3.1's rollover rule (`--parts N`
      partition-count target with last-part remainder absorption; `--max-bytes B` byte-span
      rollover at the next partition boundary at or past `B`).
- [ ] 3.3 Per-part self-audit: `verify --mode full` each finished part BEFORE any part is published
      under `--out`; on any part's failure, REFUSE the whole run (D5) — publish either all parts or
      none, matching extract's whole-run refusal discipline (R6) rather than a partial part set.
- [ ] 3.4 Token-range-per-part accounting for the manifest (min/max observed token per part) and the
      D3.2 disjoint-ascending assertion helper shared by the test suite.
- [ ] 3.5 Unit/integration tests for R4, R5 (split-half), R6 (split-half) per
      `specs/extract-split-core/spec.md`; named in the gate's component lists (#3522).

## 4. CLI (design.md D4, D7; specs/cli-extract-split/spec.md)

- [ ] 4.1 `cli_types.rs`: `Commands::Extract(ExtractArgs)`, `Commands::Split(SplitArgs)`,
      `ExtractOutFormatArg`/`SplitOutFormatArg` (or a shared enum if identical to `SalvageOutFormatArg`
      — check before duplicating), `long_about` text per R-CLI-3.2.
- [ ] 4.2 `commands/extract.rs`: arg validation (exactly one selection flag; `--token-range`
      numeric + `a < b`), schema/table resolution reusing `write.rs` helpers, `WriteGuard`
      construction from the promoted module (group 1), dispatch to `extract_partitions`, manifest
      render (JSON D4 shape + text rendering derived from it, R-CLI-3.1), exit-code mapping (D5 /
      R-CLI-1).
- [ ] 4.3 `commands/split.rs`: same shape for `split_sstable` (exactly one boundary flag;
      multi-generation-without-explicit-Data.db usage error, R-CLI-2.3), exit-code mapping
      (D5 / R-CLI-2).
- [ ] 4.4 `commands/mod.rs` dispatch wiring for both verbs, following `dispatch_salvage`'s pattern
      (owns its whole exit-code space; a build without the needed feature gate — confirm whether
      `write-support` alone suffices or `tombstones`-adjacent gating from salvage carries over —
      prints an informative message rather than a raw panic).
- [ ] 4.5 CLI tests per `specs/cli-extract-split/spec.md` R-CLI-1 through R-CLI-4, named in the
      gate's `cli-tests` component list (#3522): `extract_cli_tests.rs`, `split_cli_tests.rs`,
      `extract_write_guard_tests.rs`, `split_write_guard_tests.rs`.

## 5. Endgame

- [ ] 5.1 `openspec validate sstable-extract-split --strict` clean; commit.
- [ ] 5.2 `--lite` after every fix round (summary-file redirect); review-first: `rust-reviewer` +
      `roborev-review.sh --agent claude-code --model claude-opus-5` on the lite-green diff BEFORE
      any full gate.
- [ ] 5.3 Open PR.
- [ ] 5.4 Hand to `flow-closer`: rebase → ONE full gate (`AGENT_GATE_SUMMARY_FILE=... bash
      scripts/agent-gate.sh`, Linux box) → `spec-auditor` (C) intent audit against
      `specs/extract-split-core/spec.md` + `specs/cli-extract-split/spec.md` → roborev LAST →
      `premerge-assert` → arm `gh pr merge --auto --squash --delete-branch` → finalize.
