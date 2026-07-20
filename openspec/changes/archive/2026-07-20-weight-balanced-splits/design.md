# Design — weight-balanced split→pod assignment (#2680)

## Context

- Split generation: `CqliteFlightSplitManager.buildSplits` emits **one split per Sidecar
  read-replica token range** (~48 on the standing rig); primary owner = deterministic rotation
  `rotate()` keyed by `range.startToken()` (PR #2409). Count-balanced only.
- No per-range weight signal exists at plan time: `TokenRangeReplicasResponse` carries only
  `start`/`end`/`replicasByDatacenter`; `table_stats` is whole-table (no token-range parameter).
- The scan ticket (`FlightTicketJson`) already carries `tokenStart`/`tokenEnd`/`wraparound`, and the
  server filters by `tokenInRange` — arbitrary sub-ranges are wire-feasible with **zero server
  changes**.
- `getSplitWeight()` is never overridden (`CqliteFlightSplit`, `CqliteFlightAggregateSplit`) — both
  inherit `SplitWeight.standard()`. `io.trino.spi.SplitWeight` is on the classpath (trino-spi 481).

## Decision: deterministic K-way sub-splitting + per-range slice rotation + SplitWeight

1. **Slice at the source.** Immediately after fetching `replicas.readReplicas()` and before ANY
   consumer (scan split construction, aggregate fan-out host selection, `distinctReplicaHosts`
   snapshot chooser, #2679 pruning), expand each range `(start, end]` into K slices of equal token
   span. Slices inherit the parent's `replicasByDatacenter` verbatim. One seam; every downstream
   path sees only sliced ranges.
2. **Rotation at slice granularity.** Reuse the existing `rotate()` chooser keyed by each **slice's**
   start token. Within one parent range, consecutive slices land on consecutive owners of the
   rotated owner list (slice *i* → `rotated(parent)[i % n]`), so each owner receives
   `floor(K/n)`..`ceil(K/n)` slices of every range; the per-range remainder owner varies with the
   parent's rotation key, so remainders spread across ranges. Each owner therefore carries ~1/n of
   each range's span — per-owner total weight ≈ total/n **independent of inter-range weight skew**.
3. **`getSplitWeight()`.** `CqliteFlightSplit` reports weight proportional to its slice's token span
   relative to the mean slice span (mean-span slice = `SplitWeight.standard()`), clamped to Trino's
   valid range (`fromProportion` minimum 0.01 .. cap 1000). `CqliteFlightAggregateSplit` reports the
   sum over its assigned slices, same clamping.
4. **Config.** `cqlite.sub-splits-per-range` (int, default **4**, min 1, max 64). K=1 reproduces
   today's behavior exactly (identity slicing). Default 4 balances skew reduction against DoGet
   fan-out (~48→~192 per full scan); the field round tunes it.

### Token arithmetic (overflow-safe, wraparound-correct)

Span is computed as **unsigned wrapping subtraction** `end - start` in 64-bit token space (a
wraparound range where `start >= end` has span `2^64 - (start - end)` up to the MIN_VALUE
normalization already in `Murmur3Token`). Slice boundaries: `boundary_i = start + (span * i) / K`
using unsigned 128-safe math (`Math.multiplyHigh`/`Long.divideUnsigned` or BigInteger — implementer's
choice, but NO signed overflow; this is a known roborev blocker class). The last slice's end is the
parent's `end` exactly (no drift); each slice keeps the half-open `(start, end]` convention and its
own `wraparound` flag (`sliceStart >= sliceEnd` unsigned-wise per the existing convention). Degenerate
spans (span < K) emit fewer, non-empty slices — never an empty `(x, x]` slice, which the wraparound
convention would misread as full-ring.

## Alternatives considered

- **(1) Weight-aware greedy LPT bin-packing with per-range size estimates** — rejected for now:
  needs a per-range stats RPC that doesn't exist (whole-table `table_stats` only), i.e. a
  cross-component (Rust flight + connector) change, and estimates would come from per-SSTable
  histograms of varying quality. Kept as the follow-up if field spread persists (it is the only
  design that can see intra-range bytes-per-partition skew).
- **Token-span-weighted LPT without sub-splitting** — assigns whole ranges by span-greedy packing.
  Rejected: with only ~48 unequal ranges the packing bound is weak (one huge range still pins one
  pod), determinism across re-planning requires careful tie-breaking, and it changes primary
  ownership wholesale, churning the snapshot chooser. Sub-splitting achieves a strictly tighter
  bound with simpler, local reasoning.
- **Re-slicing the whole ring uniformly (ignore range boundaries)** — rejected: a slice crossing a
  replica-set boundary has no single owner set; ranges are the locality unit and must be respected.

## Risks / residuals

- **DoGet fan-out ×K on full scans** (48→192 at K=4): per-DoGet overhead is small (#2681 attributes
  do_get at 0.89%); point reads are unaffected (#2679 prunes to one slice). Knob exists; field round
  validates.
- **Intra-range weight skew** (few huge partitions, uneven bytes/partition): invisible to any
  span-based scheme; explicitly deferred to design (1) follow-up per the issue.
- **Split-count regression in existing tests** that assume one split per range: updated to slice
  granularity as part of this change.

## Acceptance mapping (issue #2680)

- Per-owner Σ weight ≤1.25× mean under RF==N unequal-weight fixture → new
  weight-spread test (weight := token span; justified by Murmur3 uniform placement).
- Determinism across re-planning → existing `selectionIsDeterministicAcrossInvocations` extended to
  slices.
- #2241 full ordered failover set + #2227 snapshot cover → existing tests hold at slice granularity.
- `getSplitWeight()` proportional to range weight → new unit assertions.
- Field CPU spread (C10) → report-only, next image round (tracked on #2680, not CI).
