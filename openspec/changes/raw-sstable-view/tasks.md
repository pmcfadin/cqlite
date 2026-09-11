# Tasks: #4222 — raw SSTable view

Ordered so every later task has a red test to turn green. Surfaces named per `openspec/config.yaml`.
Data.db binaries are gitignored — export `CQLITE_DATASETS_ROOT` per
`bash test-data/scripts/fetch-datasets.sh`'s printed line before running any dataset-backed test.

## 1. Schema synthesis (red first)

- [ ] 1.1 Write a unit test pinning the synthesized raw-view `TableSchema` for
      `test_tomb.dropped_regular_col_raw_sstable_data` against design.md D7's column contract (base
      columns + `<col>_timestamp`/`_ttl`/`_local_deletion_time`/`_tombstone`, row/partition/source/
      `row_kind` columns). *Surface*: the schema-synthesis function itself (new, `cqlite-core`). **Must
      be RED** (function doesn't exist yet).
- [ ] 1.2 Implement the raw-view schema synthesis: given a base `TableSchema`, produce the expanded
      `TableSchema` per D7. Wire it into `resolve_table_schema`
      (`cqlite-core/src/query/select_executor/mod.rs:1289`) behind a `_raw_sstable_data` suffix check
      that strips the suffix, resolves the base schema via the existing
      `SchemaRegistry::find_schema_by_table` unchanged, then expands. *Surface*:
      `SelectExecutor::resolve_table_schema`.
- [ ] 1.3 Confirm how `TableSchema` records a historically-dropped column's on-disk type (design.md
      D7's open item) against `test_tomb.dropped_regular_col`/`dropped_static_col`; if no such
      bookkeeping exists yet, add the minimal lookup needed so a dropped column can be decoded with
      its authoritative on-disk marshal type. *Surface*: schema resolution + `row_data.rs` decode.

## 2. Row producer — point-key path (red first)

- [ ] 2.1 Write `cqlite-core/tests/issue_4222_raw_view_point_read_test.rs`: query
      `test_tomb.resurrection_gc_positive_raw_sstable_data WHERE <pk> = <key present in both
      generations>`, assert exactly 2 rows (one per generation) with per-generation values matching
      each generation's `Data.db.jsonl` golden independently — spec's "one row per physical row per
      generation" and "dropped-column exposure" requirements. **Must be RED.**
- [ ] 2.2 Implement the point-key row producer: per generation, `might_contain_partition` →
      offset resolution (BIG Index.db / BTI trie descent, `partition_lookup.rs`) →
      `SSTableReader::read_single_partition_for_compaction`
      (`data_access/point_compaction.rs:98`) → map each `CompactionRow`/`SimpleCell`/`ComplexElement`/
      `Value::Tombstone` into the D7 column set, emitting `row_kind = 'row'` or `'partition_tombstone'`
      rows and a pair of `range_tombstone_start`/`range_tombstone_end` rows per `RangeMarker`. Never
      route through `KWayMerger`. *Surface*: new function beside `build_single_partition_merger`
      (`storage/write_engine/merge/point_read.rs`), reusing its per-generation candidate loop
      structure without its merge step.
- [ ] 2.3 Extend 2.1 with `test_deltas.range_tombstones` (prefix-bound + mixed-inclusivity cases,
      pk=1/pk=3) and `test_deltas.partition_tombstones`, `row_tombstones`, `cell_tombstones`,
      `collection_ops` — one test per spec scenario naming that fixture.
- [ ] 2.4 `test_da.wide_table` (BTI): assert the point-key path resolves via
      `lookup_partition_via_bti_trie`, not a scan, and returns every clustering row for the partition.
      *Surface*: same row-producer function, BTI branch.

## 3. Row producer — bounded full scan (red first)

- [ ] 3.1 Write a test asserting a no-predicate query against a `_raw_sstable_data` view with a small
      configured `max_result_bytes` returns the existing budget-exceeded error rather than
      materializing the corpus (spec's bounded-scan requirement). **Must be RED.**
- [ ] 3.2 Implement the full-scan row producer as a true per-generation stream modeled on
      `stream_all_partitions_for_compaction` (`data_access/compaction.rs:589`) — one callback-driven
      pass per generation, emitting `QueryRow`s as `CompactionRow`s arrive, never collecting into a
      `Vec`. Wire the existing `enforce_result_budget`/`enforce_materialized_rows`
      (`query/result_budget.rs`) unchanged. *Surface*: `SelectExecutor` table-backed scan path,
      routed for the suffixed name via `extract_table_id` (`select_executor/mod.rs:1275`).
- [ ] 3.3 Run `cargo run -p xtask -- oom-audit --enforce` against the new producer function and fix
      any `STREAM_RETURNS_VEC` finding before proceeding (design.md D9). *Component*: `oom-audit`.

## 4. Typed errors + dropped-column exposure (red first)

- [ ] 4.1 Write a test asserting `SELECT * FROM test_tomb.nonexistent_table_raw_sstable_data` exits
      with `CliExitCode::SchemaError` (3), not zero rows. **Must be RED** against current silent-empty
      behavior. *Surface*: `cqlite query` CLI, `cqlite-cli/src/error.rs::classify_error`.
- [ ] 4.2 Implement: raise `Error::Table`/`Error::Schema` explicitly from the raw-view resolution path
      (1.2) when the base table/schema doesn't resolve — never falling through to the base path's
      existing permissive behavior. *Surface*: same resolution function as 1.2.
- [ ] 4.3 Confirm the dropped-column scenario (2.1's `dropped_regular_col` case) actually surfaces
      `drop_col` with `dropped = true` end-to-end, not just in the schema-synthesis unit test.

## 5. Joinability + column-snapshot pin (red first)

- [ ] 5.1 Write `cqlite-core/tests/issue_4222_raw_view_correlation_test.rs`: run the logical `SELECT`
      and the raw-view `SELECT` for the same key against `test_compactionparity.live_clustering`,
      assert the logical row's values match the reconciled-winner physical row's values and every
      physical row shares the logical row's key-column values/types (spec's joinability requirement,
      design.md D4's correlated-query substitute for literal `JOIN`). **Must be RED.**
- [ ] 5.2 Add the `SELECT DISTINCT sstable, generation ... WHERE <pk> = ?` test against
      `test_tomb.resurrection_gc_positive` (folds #4205's SSTable-generation enumeration, design.md
      D2).
- [ ] 5.3 File a follow-up issue against epic #941 (DataFusion/Design-A) recording that literal
      `JOIN ... USING (...)` syntax (issue #4222 AC5's illustrative form) has no home in the current
      query engine and is tracked there, not invented in this change (design.md D4). Link it from this
      issue with a comment.
- [ ] 5.4 Add the column-contract snapshot test for
      `test_tomb.dropped_regular_col_raw_sstable_data` (spec's pinned-public-surface requirement).
      *Surface*: schema-synthesis function output, snapshot-tested.

## 6. CLI surface + physical-dump parity sweep

- [ ] 6.1 Write `cqlite-cli/tests/issue_4222_raw_sstable_view_cli_test.rs`: run
      `test_deltas.cell_tombstones_raw_sstable_data` through the built CLI binary in `table`, `json`,
      and `csv` formats, asserting column presence and NULL rendering per each writer's existing
      contract. *Surface*: `cqlite query` end-to-end (wiring evidence — public CLI, not a helper-only
      unit test).
- [ ] 6.2 Document the view in the SSTable guide (`docs/sstables-definitive-guide/`) with a worked
      example (#4222 AC7), naming which fixture the example uses.
- [ ] 6.3 Physical-dump-parity sweep test: for every fixture named in `specs/raw-sstable-view/spec.md`
      (`test_tomb/*`, `test_deltas/*`, `test_compactionparity/live_clustering`, `test_da.wide_table`,
      `test_comp/lz4_table`, `test_comp/uncompressed_table`), assert every metadata column value
      matches the `Data.db.jsonl` golden byte-exact, resolved per-table via `sstables_root_for_table`
      (#3220) and asserted per case (never a suite-wide `assert!(ran > 0)`). *Surface*: the raw view's
      public query surface, both row producers.

## 7. Certification

- [ ] 7.1 `--lite` green each fix round, summary-file redirect:
      `AGENT_GATE_SUMMARY_FILE=/tmp/gate-4222-lite.txt bash scripts/agent-gate.sh --lite > /tmp/lite.log 2>&1 < /dev/null`.
- [ ] 7.2 `rust-reviewer` + sanctioned roborev
      (`bash scripts/flow/roborev-review.sh --agent claude-code --model claude-opus-5`) on the
      lite-green diff, BEFORE any full gate. Blockers fixed and re-reviewed; nits batched into one
      follow-up issue.
- [ ] 7.3 Open the PR with `Closes #4222`, noting the JOIN-scope decision (D4) and the follow-up issue
      filed in 5.3.
- [ ] 7.4 `flow-closer`: ONE full `scripts/agent-gate.sh` (the gate of record, including `oom-audit`
      and `pub-surface`) → `spec-auditor` C audit against `openspec/changes/raw-sstable-view/specs/**`
      → final roborev (`bash scripts/flow/roborev-review.sh --agent claude-code --model claude-opus-5`)
      → `scripts/flow/premerge-assert.sh` → `gh pr merge --auto --squash --delete-branch`.
- [ ] 7.5 `flow-finalize`: `openspec archive raw-sstable-view`, telemetry record, worktree/branch/claim
      cleanup, closing comment on #4222.
