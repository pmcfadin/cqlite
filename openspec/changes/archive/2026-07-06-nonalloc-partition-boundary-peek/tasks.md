# Tasks — Non-allocating partition-boundary peek (K2)

## 1. Drift-guard proptest (TDD, write first)

- [ ] 1.1 Add a proptest asserting, for arbitrary byte slices and both the oa/da
      and nb parsers, that `peek_partition_boundary(data, 0) == Header` ⟺
      (`data[0]` is NOT an END_OF_PARTITION/range-tombstone marker AND
      `parse_partition_header_full(data, 0).is_ok()`), and that
      `peek_is_partition_header` returns the same boolean. Un-writable on `main`
      (there is no non-allocating peek); becomes writable once it exists.

## 2. Wide-partition work-counter wiring test (TDD, write first)

- [ ] 2.1 Add a `work-counters` scan test over `test_timeseries/sensor_data`
      (~200 rows/partition) asserting `PARTITION_HEADER_TRY_PARSES` after a full
      scan is `< rows` and `>= distinct partitions` (the per-partition bound).
      FAILS on `main` (the per-row peek try-parses ⇒ count `>= rows`); PASSES
      after K2. Skip-keys off fixture presence (0 rows stays a hard failure).

## 3. Shared structural scan

- [ ] 3.1 Add private `PartitionHeaderLayout { key_range, next_offset,
      partition_deletion }` and extract `scan_partition_header(data, offset) ->
      Result<PartitionHeaderLayout>` from `parse_partition_header_full` — the exact
      byte walk, minus the gauge and the key `to_vec`, producing identical errors.
- [ ] 3.2 Reduce `parse_partition_header_full` to: record the gauge →
      `scan_partition_header` → `to_vec` the key range → `RowKey`. Values and
      error messages unchanged.

## 4. Non-allocating peek

- [ ] 4.1 Add `BoundaryPeek { Header, NotHeader, NeedMoreBytes }` +
      `peek_partition_boundary(data, offset)` (marker pre-check → #1741 readiness
      gate → strict `scan_partition_header` under `Ready`). No gauge, no alloc.
- [ ] 4.2 Rewrite `peek_is_partition_header` as
      `matches!(peek_partition_boundary(..), BoundaryPeek::Header)`; every existing
      caller (driver post-row peek + `block_emit`/`block_emit_windowed`) is
      unchanged and now allocation-free.

## 5. Surfaces named / roborev pre-empt

- [ ] 5.1 New surfaces (`pub(super)`): `BoundaryPeek`, `peek_partition_boundary`;
      private `PartitionHeaderLayout`, `scan_partition_header`. No public (`pub`)
      API change; `peek_is_partition_header` signature unchanged.
- [ ] 5.2 Pre-roborev self-check: peek accept/reject byte-identical to `main`
      (proptest); no `unwrap`/`expect` in library code; no-heuristics (structural
      scan, not byte-pattern guessing); no `manual_range_contains`; no dangling
      refs; no wall-clock races in the added tests; minimal-features build clean.

## 6. Validation

- [ ] 6.1 `openspec validate nonalloc-partition-boundary-peek --strict` — clean.
- [ ] 6.2 Fast iteration gate (`scripts/agent-gate.sh --lite`) PASS each round.
- [ ] 6.3 Full `scripts/agent-gate.sh` PASS (run by the lead) — multi-partition
      sstabledump parity + compaction byte-parity green (row order + boundaries
      byte-identical).
- [ ] 6.4 Intent audit **C** (spec-auditor) PASS + roborev clean before merge.
