## Why

The July 2026 parser performance audit
(`docs/reports/parser-performance-audit-2026-07-01.md`, Epic K finding **K2**)
found the V5CompressedLegacy partition-BOUNDARY peek — the "is the next thing a
new partition header?" check the emit loop runs **after every row** — implemented
as a FULL allocating partition-header try-parse:

- `peek_is_partition_header` calls `parse_partition_header_full`, which
  (a) allocates a throwaway partition key `to_vec`, (b) increments the H5
  `PARTITION_HEADER_TRY_PARSES` gauge, and (c) constructs eager `format!(..)`
  error strings that are used purely as a "not a header" control-flow sentinel.

Cost: one throwaway key allocation + up to one error-string allocation **per
row** in every scan/compaction emit path (the driver's post-row boundary peek at
`partition_driver.rs`, plus the `block_emit`/`block_emit_windowed` peeks). For a
wide partition the peek runs once per row while a real header parse is needed only
once per partition.

**Routing: design-driven.** This is a hot-path mechanics change (introduce a
non-allocating peek primitive + a shared structural classifier), not an
oracle-driven parse-correctness bug, so it is captured as an OpenSpec change per
the spec-driven doctrine. The design and priority are owner-approved via the
read-path/parser performance audit (standing owner Seam-1 approval, v0.14 perf
wave); this change encodes that decision rather than re-litigating it.

Milestone: **v0.14 performance wave** (Epic #1604, row/cell hot-loop mechanics).
This is a **pure factoring** — identical observable output. Parity is the proof:
the multi-partition sstabledump JSONL harness and the compaction byte-parity
suite must stay green (row ordering and partition boundaries byte-identical).

## What Changes

- **Add** a non-allocating boundary peek `peek_partition_boundary(data, offset)
  -> BoundaryPeek` where `BoundaryPeek` is a small `{ Header, NotHeader,
  NeedMoreBytes }` enum. It performs the same structural checks (marker-flag
  rejection, key-length plausibility, DeletionTime framing) by reading bytes and
  returning the enum — **no `to_vec`, no `format!`, no `Err`-as-control-flow, no
  `PARTITION_HEADER_TRY_PARSES` increment.**
- **Extract** the exact structural walk of `parse_partition_header_full` into one
  shared non-allocating helper (`scan_partition_header` → byte-range layout) that
  BOTH the full parser and the peek derive from, so a peek can never accept what
  the full parser rejects (no drift, no new heuristic).
- **Rewrite** `peek_is_partition_header` as a thin wrapper over
  `peek_partition_boundary` (unchanged boolean semantics for every existing
  caller). The real `parse_partition_header_full` (which legitimately allocates
  the key + increments the gauge) runs ONLY at a confirmed partition start, once
  per partition.
- **Add** a proptest proving `peek_partition_boundary == Header` ⟺ (not a
  marker AND `parse_partition_header_full == Ok`) on the same prefix, for both the
  oa/da and nb DeletionTime forms — the drift guard.
- **Add** a work-counter wiring test scanning a genuinely wide-partition fixture
  (`test_timeseries/sensor_data`, ~200 rows/partition) asserting
  `PARTITION_HEADER_TRY_PARSES` is bounded by the partition count, not the row
  count (FAILS on `main`, where the per-row peek try-parses).

## Non-goals

- **K3 (#1642) positional row emit** and **K4 (#1643) `Arc` key handles** — the
  rest of the K-emit chain. K2 does NOT fold in that work; where a K3/K4 seam
  appears it is left as a `TODO` referencing the owning issue.
- **No emit-semantics change of any kind** — no change to row ordering, partition
  boundary detection outcomes, tombstone handling, or any observable output. This
  is a factoring; the parity gates are the proof.
- **Not** weakening any validation the full parse performs at a true boundary
  (the peek's structural rules are IDENTICAL to the full parser's, derived from
  the same helper).
- **Not** re-litigating the pre-`na` version floor or the no-heuristics mandate.
