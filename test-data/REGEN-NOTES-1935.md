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

## How v3.5 was actually produced — the v3.4-SPLICE strategy (FINAL, owner-decided)

**v3.5 == v3.4 byte-identical EXCEPT the 5 TTL tables.** An earlier attempt did a
whole-corpus `regenerate-datasets.sh --rows 50` uniform regen; that drifted EVERY
table's UUID/shape/golden and was **discarded**. The shape-bearing generator that
produced the original v3.4 corpus was never committed (ref #2222), so v3.5 is cut
by **splicing** a bespoke no-TTL re-cut of only the 5 TTL tables onto the v3.4
base tree. Exact flow that produced v3.5:

1. **Base tree** — fetch the v3.4 asset (`cassandra5-small-full-v3.4.tar.gz`,
   sha `3cae644360e0142a6bb5e96ddab445ff18e3478e7058104842ce1a455fba8a33`) and
   extract it into `test-data/datasets/` (binaries are gitignored). This is the
   byte-for-byte base for the ~40 unchanged tables.
2. **Re-cut ONLY the 5 TTL tables** in `cassandra:5.0.2` docker, WITHOUT
   `default_time_to_live`, replaying each table's EXACT v3.4 shape. Rows are
   reconstructed VERBATIM from the committed v3.4 goldens (keys, clustering,
   cell values) and inserted with `USING TIMESTAMP <v3.4-micros>` and no TTL, so
   the only golden delta vs v3.4 is TTL removal + row repack:
   - nb phase (`storage_compatibility_mode: CASSANDRA_4`): `test_timeseries`
     `app_metrics` (200 rows/200 parts), `log_entries` (200/200),
     `tick_data` (200/24, deterministic bucket distribution).
   - oa phase (`storage_compatibility_mode: NONE`): `test_oa.ttl_table` (3 rows).
   - da/BTI phase (`NONE` + `sstable.selected_format: bti`):
     `test_da.ttl_table` (2 rows).
   Flush, then **rename the fresh dirs to the v3.4 UUID basenames + component
   prefixes** (`nb-1-big` / `oa-2-big` / `da-2-bti`) and splice them over the v3.4
   base. Because the UUIDs/prefixes are reused, `references.yml`, `metadata.yml`
   (`row_count` preserved) and `cassandra-parity-manifest.yml` stay
   v3.4-identical — ONLY the 5 tables' `Data.db.jsonl` / `Digest.crc32` /
   `Statistics.db.txt` goldens change (TOC.txt is byte-identical). `test_basic`
   `ttl_test_table` keeps its aged TTL fixture UNTOUCHED (#1853 seam).
3. **Re-export goldens** for the 5 tables (in-container `sstabledump -l` +
   `sstablemetadata`); `cassandra-parity -- corpus-audit` clean; manifest lint +
   `report --check` green.
4. **Package** — `test-data/scripts/package_datasets.sh --full --suffix v3.5`
   produces `cassandra5-small-full-v3.5.tar.gz`. Mind the macOS tar AppleDouble
   gotcha.
5. **Upload + pin (owner/lead)** — `gh release upload datasets-v3
   cassandra5-small-full-v3.5.tar.gz --clobber`, capture its SHA256, then
   `bump-dataset-pin.sh --new-sha <sha256>` (asset
   `cassandra5-small-full-v3.5.tar.gz`, tag `datasets-v3`). `DATASET_TAG` /
   `DATASET_ASSET` / `DATASET_SHA256` must ALL be set consistently across
   `.github/workflows/*.yml` and `fetch-datasets.sh`. The sha does NOT exist
   until the asset is built — do NOT invent one.
6. **Round-trip verify** — `bash test-data/scripts/fetch-datasets.sh` pulls the
   new asset, verifies the SHA; the parity/CLI tests then see the no-TTL fixtures
   returning their physical row counts.

> **WARNING — a wholesale fresh regen leaves leftover UNTRACKED (gitignored)
> binary dirs.** The discarded uniform-50 regen wrote `<table>-<new-uuid>/` dirs
> whose `Data.db` binaries are gitignored; `git rm` of the tracked goldens does
> NOT delete them, so they linger as DUPLICATE table dirs that make the reader
> resolve a spurious multi-generation table (breaks `issue_1333`, the #1143
> windowed-scan guard, etc.). When restoring the v3.4 base, delete every sstable
> dir NOT present in the canonical v3.4 tree.

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
