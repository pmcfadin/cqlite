# Tasks — weight-balanced-splits (#2680)

## 1. Token-range slicing helper

- [ ] 1.1 Add a pure slicing helper (e.g. `TokenRangeSlicer`) that expands one `(start, end]` range
      into K equal-span slices using unsigned 64-bit wrapping arithmetic (no signed overflow —
      `Long.divideUnsigned`/`Math.multiplyHigh` or BigInteger), preserving wraparound semantics and
      never emitting an empty `(x, x]` slice; degenerate spans (< K tokens) emit fewer slices.
      Surface: unit tests over normal, wraparound, seam-crossing, and degenerate ranges assert exact
      coverage (no gap/overlap), boundary ownership under `Murmur3Token.tokenInRange`, and K=1 identity.
- [ ] 1.2 Add config `cqlite.sub-splits-per-range` (default 4, min 1, max 64) to
      `CqliteFlightConfig`. Surface: config unit test (bounds, default).

## 2. Wire slicing into the single assignment seam

- [ ] 2.1 In `CqliteFlightSplitManager`, expand `replicas.readReplicas()` through the slicer BEFORE
      all consumers (scan `buildSplits`, aggregate fan-out host selection, `distinctReplicaHosts`,
      `pruneToBoundPartitionKey`); slices inherit the parent's owner set; per-slice primary via the
      existing `rotate()` chooser with within-range consecutive-owner distribution (slice i →
      rotated(parent)[i % n]). Surface: `CqliteFlightReplicaRotationTest` extended to slice granularity.
- [ ] 2.2 Verify #2679 pruning composes: fully-bound PK prunes to exactly one covering slice.
      Surface: pruning test with K=4 asserting one split, strictly narrower than the parent range.

## 3. SplitWeight

- [ ] 3.1 Override `getSplitWeight()` on `CqliteFlightSplit` (proportional to slice span; mean-span
      slice = standard; clamped) and `CqliteFlightAggregateSplit` (clamped sum). Surface: unit tests
      for the 3×-span ratio and extreme-span clamping scenarios.

## 4. Acceptance tests (spec scenarios)

- [ ] 4.1 New weight-spread test: RF==N==3 fixture, deliberately unequal spans (≥8× variation),
      assert per-owner Σ span ≤ 1.25× mean AND the ceil(count) cap at slice granularity.
- [ ] 4.2 Extend `selectionIsDeterministicAcrossInvocations` and
      `snapshotHostSetCoversEveryRotatedPrimary` to sliced topologies (K=4).
- [ ] 4.3 Failover invariant: every slice retains the full ordered owner set (#2241).

## 5. Docs

- [ ] 5.1 Document `cqlite.sub-splits-per-range` in the connector config docs (same change).

## 6. Certification

- [ ] 6.1 `scripts/agent-gate.sh --lite` each fix round (summary-file redirect); connector test suite
      (`trino-connector` Gradle tests) run explicitly — the Rust gate does not cover Java.
- [ ] 6.2 rust-reviewer + roborev on the lite-green diff (review-first), blockers fixed pre-PR.
- [ ] 6.3 flow-closer endgame: ONE full gate → C intent audit vs this spec → final roborev →
      merge-on-green → finalize.
- [ ] 6.4 Post-merge (next image round, report-only on #2680): field C10 — busiest pod ≤ ~1.3× median
      CPU at 32 threads under scan/count; aggregate qps off ~39.
