# compressed-flight-e2e-coverage

## Why

The #2363 coverage-matrix audit found hole **FMT-1**: the compressed (chunk-stitching) BIG-`nb`
read path and the in-repo Flight test surface barely overlap. All Flight fixtures built by
`build_multi_sstable_fixture` are WriteEngine-produced and therefore **uncompressed**
(`export.rs:786-796` asserts no `CompressionInfo.db` is ever emitted — the #1406 claim boundary means
CQLite *cannot* synthesize a compressed SSTable in-repo), so they route only the non-stitching arm
(`compaction.rs:597`). The LIVE testbed (cassandra-easy-stress, LZ4 by default) exercises
`requires_chunk_stitching() == true` — the branch the field actually runs — with almost nothing
in-repo underneath it.

**The issue's premise is overstated and this change corrects it.** Two in-repo tests do reach the
compressed path today: `cqlite-flight/tests/issue_2412_do_get_cold_warm_e2e_compressed.rs` (Snappy,
`test_basic.compression_test_table`) and `do_get_transport_test.rs:299-342`
(`do_get_over_transport_real_compressed_fixture`). But the first calls `svc.do_get(...)`
**in-process, not over transport**, and is gated behind `--features observability-testing`; the second
goes over transport but asserts only `rows > 0` — **no routing evidence, no golden comparison, one
incidental codec**. The real gap is therefore narrower and sharper than "never reaches do_get":

- **No per-codec coverage.** The dedicated `test_comp` corpus (LZ4, Snappy, Deflate, Zstd, plus the
  `short_final_chunk` and `incompressible_uncompressed_chunk` edge tables) never reaches Flight at all.
- **No routing assertion.** Nothing proves the stitch branch was taken rather than green-by-accident —
  precisely the #2362/`issue_1578` lesson (a test that passes without exercising the path under test).
- **No midstream-drop-over-compressed.** All three existing midstream-drop tests
  (`do_get_transport_test.rs:685`, `issue_2370_concurrent_doget_test.rs`, `issue_2370_gauge_readback_test.rs`)
  use uncompressed WriteEngine fixtures, so producer-release under backpressure is untested on the
  stitching path.
- **The corpus is not CI-real.** `test_comp` Data.db/CompressionInfo.db are gitignored (`.gitignore:45`),
  so on a stock checkout these tests would SKIP rather than run.

## What Changes

A new transport-level Flight integration test over the **real `test_comp` corpus**, one case per
codec, asserting golden-matched rows AND proving the stitching branch was routed:

- **Routing evidence** via the public `SSTableReader::decompress_call_count()` /
  `reset_decompress_calls()` counters (`data_access/mod.rs:146,151`, bumped per chunk decompress in
  `chunk_source.rs:133,173`) — the same evidence pattern already used at
  `chunk_decode_single_plane.rs:242-300`. A non-zero count after a scan proves the decompress plane
  ran; the uncompressed control table must yield zero. (`requires_chunk_stitching()` itself is
  `pub(super)` and cannot be called from `cqlite-flight`.)
- **Per-codec scenarios**: full scan vs the committed `nb-1-big-Data.db.jsonl` golden, LIMIT-k, and a
  midstream client drop that must release the producer — the uncovered combination.
- **Corpus made CI-real**: the `test_comp` components the tests need are force-committed (`git add -f`),
  following the #2372/#1190 precedent that tracked `test_da`'s 24 BTI `.db` files. Cost is small —
  the four codec tables total ~52 KB; including the two edge tables ~336 KB (vs `test_da`'s 2.1 MB).
- **Fixture helper gap closed**: `cqlite-flight/tests/fixture_support/mod.rs::table_dir_if_present`
  takes a hardcoded `table_with_uuid`, but `test_comp` directory UUIDs are regeneration-unstable
  (`references.yml:891` already carries stale ones). A prefix-globbing variant is added, mirroring
  `issue_1082_deflate_zlib.rs::fixture_dir`.
- **Anti-vacuous-pass discipline**: fixture-presence-gated and SKIP-aware, but a *present* fixture
  returning zero rows is a FAILURE, and `CQLITE_REQUIRE_FIXTURES=1` turns the skip into a hard failure
  for the dataset-fetching CI lane (pattern: `chunk_cache_wiring_tests.rs:26,214`).

## Non-goals

- **Compressed SSTable *writing*.** Out of scope and fail-closed by the #1406 claim boundary; the
  fetched corpus is the only source of compressed fixtures.
- **Trino-testbed BTI/compressed provisioning** — the live-kit half of the matrix; separate follow-up
  (same split #2372 made).
- **Regenerating the corpus.** `test_comp` is pinned by `dataset-pin.env` (datasets-v3 /
  cassandra5-small-full-v3.5, sha 4141950…ba16). Wholesale regen is forbidden until #2222, and #1935's
  rule stands: never strip a seam on regen. This change only *commits* existing fetched bytes.
- **New compression codecs or read-path optimization.** Coverage only; no behavior change.

## Doctrine impact

None to CLAUDE.md or the website. The coverage matrix
(`docs/architecture/issue-2363-coverage-matrix-audit.md`) is updated to mark FMT-1 closed for the
in-repo Flight axis.
