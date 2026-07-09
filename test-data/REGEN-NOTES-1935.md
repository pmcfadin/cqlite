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

## What is CI-GATED / owner-owned (regen + asset cut — how v3.5 was produced)

The whole-corpus binary regeneration + new release asset + dataset-pin bump is
owner-owned. The `exhaustive-regeneration.yml` CI workflow **audits** the corpus
(runs `regenerate-datasets.sh` against real Cassandra 5 + `cassandra-parity --
corpus-audit`) but **by design exports no tarball** — the published asset is cut
from a **local regeneration**. Exact flow that produced v3.5:

1. **Regenerate binaries locally** — `test-data/scripts/regenerate-datasets.sh`
   against real Cassandra 5, then refresh the JSONL goldens + manifest
   (`cassandra-parity -- corpus-audit` must be clean).
2. **Package** — `test-data/scripts/package_datasets.sh --full --suffix v3.5`
   produces `cassandra5-small-full-v3.5.tar.gz` (version in the suffix; the fetch
   pin keys on this versioned asset name). Mind the macOS tar AppleDouble gotcha.
3. **Upload** — `gh release upload datasets-v3 cassandra5-small-full-v3.5.tar.gz
   --clobber` and capture its SHA256.
4. **Bump the dataset pin** — `test-data/scripts/bump-dataset-pin.sh
   --new-sha <sha256-of-new-asset>` (defaults: asset
   `cassandra5-small-full-v3.5.tar.gz`, tag `datasets-v3`, old v3.4). Never
   tag-only: `DATASET_TAG` / `DATASET_ASSET` / `DATASET_SHA256` must ALL be set
   consistently across `.github/workflows/*.yml` and
   `test-data/scripts/fetch-datasets.sh`; the script's self-check fails on any
   stale v3.4 pin. Pin after this PR: `datasets-v3` /
   `cassandra5-small-full-v3.5.tar.gz` /
   `13d8da00743d9780c7ee89478649c280f9d91519a4561f6909cc4ce3bb7a3631`.
   Do NOT invent a SHA — it does not exist until the asset is built.
5. **Round-trip verify** — `bash test-data/scripts/fetch-datasets.sh` pulls the
   new asset, verifies the SHA, and the parity/CLI tests then see the regenerated
   (no-TTL) fixtures returning their physical row counts.

> **WARNING — do NOT wholesale fresh-regen the aged TTL fixtures.** TTL expiry
> derives from the *real insertion wall-clock*, so a fresh regen of
> `test_basic.ttl_test_table` re-seeds live rows and breaks the #1853
> fully-expired seam (`ttl_test_table_fully_expired_returns_zero_live_rows_at_wall_clock`)
> for ~24h until the new TTLs lapse. The executed v3.5 flow deliberately
> **preserved the aged v3.4 fixtures** for `test_basic.ttl_test_table` (and for
> `test_da/wide_table` from `gen-wide-bti.sh`) rather than regenerating them.

## Expected outcome after regen

- `app_metrics`, `log_entries`, `tick_data`, `test_da.ttl_table`,
  `test_oa.ttl_table` return their physical (non-expired) row counts.
- `test_basic.ttl_test_table` still returns 0 LIVE rows at wall clock (TTL kept) —
  covered by the #1853 seam, not by a `> 0` assertion.
- Node CI `parity.test.js`, Python `test_parity.py`, CLI `comprehensive_select`,
  and `sstabledump-parity` all green (the TTL-aware assertions track the goldens).

## Regen gotcha — references.yml must be refreshed too (fold into #2222)

- The regen must also refresh `test-data/datasets/references.yml` (it is packaged
  *inside* the dataset asset tarball, not committed). A v3.x regen that rewrites
  the sstable dirs + `metadata.yml` with new table UUIDs but leaves
  `references.yml` pinning the OLD UUID basenames silently breaks **every**
  manifest-resolving suite: `resolve_table_dir_via_manifest`
  (`cqlite-core/src/testing/dataset_helpers.rs`) trusts the stale `sstable_dir`
  basename and returns a nonexistent path, **shadowing** the metadata.yml glob
  fallback → ENOENT in core/cli/python/memory-budget/scan-offload suites and the
  `sstabledump-parity` CI lane. Regenerate it via `export.sh` (or remap the
  `sstable_dir` basenames to the on-disk v3.x dirs) as part of the same regen.
