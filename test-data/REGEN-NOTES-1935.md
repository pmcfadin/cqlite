# Issue #1935 — TTL corpus regeneration notes (#1896 cluster A, owner-decided)

Owner decision (2026-07-08, ref #1896): strip `default_time_to_live` from the
five TTL-carrying corpus tables that time-bombed the fixtures, **keep**
`test_basic.ttl_test_table` with its TTL as the dedicated #1853 seam.

## What is DONE in this PR (local, reviewable — no binary regen)

1. **Schema edits** — `default_time_to_live` removed (and no `USING TTL` inserts
   exist for these tables):
   - `test-data/schemas/time-series.cql` — `app_metrics` (was 2592000),
     `log_entries` (was 604800), `tick_data` (was 86400).
   - `test-data/schemas/da-test.cql` — `ttl_table` (was 86400).
   - `test-data/schemas/oa-test.cql` — `ttl_table` (was 86400).
   - `test-data/schemas/basic-types.cql` — `ttl_test_table` **UNCHANGED**
     (keeps `default_time_to_live = 86400`; it is the #1853 seam).
   - `test-data/scripts/regenerate-datasets.sh` — comment updated (no TTL).
2. **Test assertions** made TTL-aware / robust:
   - `cqlite-cli/tests/comprehensive_select_test.rs` — the four TTL tables
     (`ttl_test_table`, `app_metrics`, `log_entries`, `tick_data`) moved out of the
     `> 0` row-count cases into `test_select_ttl_aware`, which derives the expected
     LIVE count from the sstabledump JSONL golden (same TTL-aware logic as the
     Python/Node parity harnesses) and asserts the CLI matches it, capped at the
     query LIMIT. Passes at 0 today (all rows wall-clock-expired) and will track
     the regenerated physical count automatically. Guards against a vacuous pass by
     requiring the golden to have > 0 physical rows.
   - `bindings/python/tests/test_cli_parity.py` — clarifying comments only; the
     `(table, 10)` tuples are query LIMITs, not counts, and the test asserts
     Python==CLI equality (holds at 0 and post-regen). No hardcoded count existed.
   - The Python `test_parity.py::test_row_count` and Node `parity.test.js` already
     derive TTL-aware expected counts from goldens — no change needed; they
     auto-adjust after regeneration.
3. **#1853 seam preserved** — `cqlite-core/tests/issue_694_writetime_ttl_parity.rs`
   is UNTOUCHED (only a trailing doc comment updated); both
   `writetime_parity_test_basic_ttl_test_table` (pinned-now) and
   `ttl_test_table_fully_expired_returns_zero_live_rows_at_wall_clock` (wall-clock)
   still cover `test_basic.ttl_test_table` with TTL.

## What is CI-GATED / owner-owned (NOT done here)

The whole-corpus binary regeneration (rm -rf + fresh UUIDs + new release asset +
dataset-pin bump) is owned by CI + owner. Exact recipe:

1. **Regenerate binaries** — run the `exhaustive-regeneration.yml` workflow
   (`workflow_dispatch`), which runs `test-data/scripts/regenerate-datasets.sh`
   against real Cassandra 5, then `cassandra-parity -- corpus-audit` (must be
   clean) and refreshes JSONL goldens. It packages the corpus to compute the asset
   name + SHA256 but by design does NOT publish.
2. **Publish the asset** — `test-data/scripts/publish_datasets.sh --type full
   --tag datasets-v3` (uploads `cassandra5-small-full.tar.gz` with `--clobber`;
   version lives in the TAG, not the filename — but the fetch pin keys on the
   versioned asset name `cassandra5-small-full-v3.5.tar.gz`, so cut/upload the v3.5
   asset). Mind the macOS tar AppleDouble gotcha when packing.
3. **Bump the dataset pin** — `test-data/scripts/bump-dataset-pin.sh
   --new-sha <sha256-of-new-asset> --new-asset cassandra5-small-full-v3.5.tar.gz`
   (never tag-only: `DATASET_TAG` / `DATASET_ASSET` / `DATASET_SHA256` must ALL be
   set consistently across `.github/workflows/*.yml` and
   `test-data/scripts/fetch-datasets.sh`). Current pin: `datasets-v3` /
   `cassandra5-small-full-v3.4.tar.gz` /
   `3cae644360e0142a6bb5e96ddab445ff18e3478e7058104842ce1a455fba8a33`.
   Do NOT invent a SHA — it does not exist until the asset is built.
4. **Round-trip verify** — `bash test-data/scripts/fetch-datasets.sh` pulls the
   new asset, verifies the SHA, and the parity/CLI tests then see the regenerated
   (no-TTL) fixtures returning their physical row counts.

## Expected outcome after regen

- `app_metrics`, `log_entries`, `tick_data`, `test_da.ttl_table`,
  `test_oa.ttl_table` return their physical (non-expired) row counts.
- `test_basic.ttl_test_table` still returns 0 LIVE rows at wall clock (TTL kept) —
  covered by the #1853 seam, not by a `> 0` assertion.
- Node CI `parity.test.js`, Python `test_parity.py`, CLI `comprehensive_select`,
  and `sstabledump-parity` all green (the TTL-aware assertions track the goldens).
