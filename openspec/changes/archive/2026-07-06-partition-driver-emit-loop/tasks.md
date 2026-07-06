# Tasks — PartitionDriver: one partition/row emit loop (K1)

## 1. Lockstep guard test (TDD, write first)

- [ ] 1.1 Add a lockstep guard test asserting `row_write_timestamp` (the #932
      row-write-timestamp coexistence rule) yields the liveness timestamp — not
      `markedForDeleteAt` — for a header carrying BOTH `HAS_DELETION` and a
      liveness timestamp, and the `markedForDeleteAt` fallback for a pure row
      tombstone. This is un-writable as a single-site assertion on `main` (the
      rule is hand-copied); it becomes writable once the rule is one helper.

## 2. Centralize the #932 row-write-timestamp rule

- [ ] 2.1 Add `partition_driver.rs` with `pub(super) fn row_write_timestamp`.
- [ ] 2.2 Route `parse_one_partition_with_timestamps` and
      `parse_one_partition_for_compaction` through it (delete the two inline
      copies).

## 3. Extract the sliding-window driver

- [ ] 3.1 Add `SlidingPartitionPolicy` trait + `MarkerOutcome` +
      `drive_partition_sliding` (the skeleton: readiness → header parse →
      per-row loop → `pending` flush → `ParseStep`).
- [ ] 3.2 Implement `TimestampPolicy` (in `block_emit_windowed.rs`) and
      `CompactionPolicy` (in `compaction.rs`) as the two policy hooks.
- [ ] 3.3 Rewrite both sliding emit functions as thin adapters over the driver
      (public signatures unchanged).

## 4. Surfaces named / roborev pre-empt

- [ ] 4.1 New surfaces (`pub(super)`): `partition_driver::{row_write_timestamp,
      SlidingPartitionPolicy, MarkerOutcome, drive_partition_sliding}`. No public
      (`pub`) API change; all adapter signatures unchanged.
- [ ] 4.2 Pre-roborev self-check: row-order/semantics parity preserved across
      both sliding copies; no `unwrap`/`expect` in library code; no-heuristics;
      no `manual_range_contains`; no dangling refs to removed inline copies; no
      wall-clock races in the added test.

## 5. Validation

- [ ] 5.1 `openspec validate partition-driver-emit-loop --strict` — clean.
- [ ] 5.2 Fast iteration gate (`scripts/agent-gate.sh --lite`) PASS each round.
- [ ] 5.3 Full `scripts/agent-gate.sh` PASS (run by the lead) — 33-table
      sstabledump parity + compaction byte-parity green through the refactor.
- [ ] 5.4 Intent audit **C** (spec-auditor) PASS + roborev clean before merge.
