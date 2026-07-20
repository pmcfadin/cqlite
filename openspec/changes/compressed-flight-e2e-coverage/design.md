# Design — compressed (chunk-stitching) Flight E2E coverage (#2373)

## Context

- `requires_chunk_stitching()` (`data_access/mod.rs:180-184`) is `pub(super)` — true iff
  `V5CompressedLegacy && is_nb_format()`. A `cqlite-flight` integration test **cannot call it**, so
  routing must be proven indirectly.
- The Flight query path forks at `summary_scan.rs:448`:
  `stream_partitions_summary_guided_compaction` (stitch) vs `stream_partitions_summary_guided`.
- `test_comp` corpus: 7 tables, one DDL shape `(pk INT, ck INT, body TEXT, PRIMARY KEY (pk, ck))`
  (`compression-parity.cql:69-121`); LZ4/Snappy/Deflate/Zstd at 16 KB chunks, plus
  `short_final_chunk` (LZ4, 4 KB, final chunk < 4096), `incompressible_uncompressed_chunk`
  (LZ4 `min_compress_ratio=1.0` → raw-chunk fallback), and an `uncompressed_table` control.
- Existing transport harness to copy: `bti_do_get_transport_test.rs:68-114`
  (`do_get_batches_over_transport` — ephemeral port, `TcpIncoming::from_listener`, tonic server,
  `FlightRecordBatchStream` decode) + sync wrapper `run_do_get` at :120-125.

## Decision 1 — routing evidence: `decompress_call_count()`, with an uncompressed control

Assert the stitch/decompress plane actually ran by bracketing the scan with
`reset_decompress_calls()` / `decompress_call_count()` (public, `data_access/mod.rs:146,151`) and
requiring `count >= 1`. **The control is what makes it evidence**: the same assertion run against
`test_comp.uncompressed_table` must yield **zero**, so a passing compressed case cannot be explained
by an unrelated decompress elsewhere in the process.

The counter is **process-global**, so every test touching it runs `#[serial]` (or in a single-test
binary, the pattern `issue_2412_do_get_cold_warm_e2e_compressed.rs` already uses).

Alternatives considered:
- **OTel `cqlite.read.scan.window_refill`** (`observability/catalog.rs:268-273`) — its doc literally
  says a non-zero value proves the stitch boundary path ran, but it only fires on the *windowed* plane
  with a straddling partition, so it cannot cover small single-chunk tables, and it needs
  `--features observability-testing`. **Rejected as the primary signal; acceptable as a supplement.**
- **`chunk_read_call_count()`** — point-read site only (`mod.rs:637-641` explicitly forbids scan
  callers from bumping it), so it cannot evidence a scan. Rejected.
- **Asserting `CompressionInfo.db` exists** on the fixture dir — structural, not routing. Rejected as
  the primary signal (it proves the fixture is compressed, not that the code took the stitch arm); kept
  as a cheap precondition assert.

## Decision 2 — corpus becomes CI-real via force-commit (precedent #2372/#1190)

Force-commit (`git add -f`) the components the tests read for the six compressed/edge tables plus the
uncompressed control: `nb-1-big-{Data,CompressionInfo,Statistics,Index,Summary,Filter}.db` + `TOC.txt`
(the `.jsonl` goldens and decoded sidecars are already tracked). ~336 KB total, against `test_da`'s
2.1 MB already in-tree.

Rejected alternative — **SKIP in a stock checkout, enforce only in the dataset-fetching lane**: it
leaves the headline claim ("the field's branch is covered") true only in one lane, and #2373 exists
precisely because coverage that isn't real where people look isn't coverage. Force-commit is the
precedent the sibling issue set one day earlier.

**Verification discipline (from #2372 `tasks.md:35-37`):** confirm the tests pass against the
**committed tree** via `git worktree add --detach HEAD`, never the dirty working tree — a gitignored
binary present locally but unstaged is the classic false green.

## Decision 3 — prefix-globbing fixture helper

Add `table_dir_by_prefix(keyspace, table_prefix, component_prefix) -> Option<PathBuf>` to
`cqlite-flight/tests/fixture_support/mod.rs`, globbing `<root>/sstables/<ks>/<table>-*` and requiring
`<component_prefix>-Data.db` to be a file — the `issue_1082_deflate_zlib.rs:63-83` pattern. The existing
`table_dir_if_present` hardcodes a UUID, which would break on any corpus regeneration; `test_comp`'s
UUIDs are already stale in `references.yml:891`. Existing callers keep working (additive).

## Decision 4 — scenario shape

Per codec (LZ4, Snappy, Deflate, Zstd): **full scan vs golden** (row count from the committed
`nb-1-big-Data.db.jsonl`, and value equality on `(pk, ck, body)`), plus **LIMIT-k** bounding.
Once (on LZ4, the field default): **midstream drop** — reusing the `do_get_drop_after` +
`await_in_flight_settled` pattern (`do_get_transport_test.rs:595-670`, `batch_size=1` to force
`blocking_send` parking) to prove the producer is released on the *stitching* path. Edge tables
`short_final_chunk` and `incompressible_uncompressed_chunk` get a full-scan case each — they are the
two chunk-boundary shapes most likely to break stitching.

## Risks / residuals

- **Serial execution cost**: the decompress counter forces `#[serial]` across these cases; the suite is
  small (7 tables, short scans) so wall-clock impact is minor.
- **Golden coupling to the dataset pin**: committing bytes pins these tests to datasets-v3 v3.5. A
  future regen (#2222) must refresh both bytes and goldens together — noted in the test header.
- **Counter is a global**: a future parallel-test change could make `>= 1` flaky. Mitigated by the
  uncompressed-control zero-assert being in the same serial group, and by an explicit header comment.

## Acceptance mapping (issue #2373)

- Flight integration tests over real compressed `test_comp` (LZ4 minimum, ideally per codec) →
  Requirement 1 + per-codec scenarios.
- Full scan + LIMIT + midstream drop through real transport → Requirement 1 scenarios.
- Fixture-presence-gated like `issue_1082`, SKIP-aware, never vacuous-pass → Requirement 3.
- Verify the stitch branch is actually routed (#2362/`issue_1578` lesson) → Requirement 2.
