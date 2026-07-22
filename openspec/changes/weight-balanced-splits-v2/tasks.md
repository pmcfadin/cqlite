# Tasks: Weight-balanced splits via drain-hardened sub-splitting

## 1. Drain fix (prerequisite — lands + provable FIRST, #2782 ROOT CAUSE)
- [x] 1.1 ROOT FIX: run the blocking Arrow `FlightStream.next()` read OFF the Trino driver thread — on a
      background executor with a non-blocking `isBlocked()` signal — so the driver thread is never pinned in
      a deadline-less `next()`. That is what lets Trino run `close()` on the freed driver thread; the prior
      close→cancel wiring was correct but could never execute while the driver was blocked (the real #2782
      hang). `close()` cancels the active Flight `DoGet` stream (`FlightStream.cancel()`, cross-thread), which
      unblocks the parked background read; idempotent, non-blocking, propagated to the active underlying
      stream. (surface: `CqliteFlightPageSource.getNextSourcePage`/`isBlocked`/`close`,
      `ReplicaFailoverStream.close`/`cancel`)
- [x] 1.2 Unit test the root cause with an instrumented BLOCKING stream: a poll does not pin the driver
      thread even though `next()` blocks; an early close cancels + unblocks the parked read without hanging
      (bounded, `assertTimeoutPreemptively`); second close is a no-op; cancel reaches the active failover
      stream. Verified to FAIL on the reverted `b18b9a05` (deadline-less driver-blocking) page source.
- [x] 1.3 SERVER hardening (defense-in-depth): the `do_get` response stream (`MeteredDoGetStream::poll_next`)
      races its parked inner batch poll against the shared `CancelFlag` async cancellation, so a cancellation
      from any source ends egress at the next poll rather than only when the merge notices it between steps.
      Rust test: a parked stream (receiver alive, no batch ready) ends within a bound once the flag trips.

## 2. LIMIT-pushed + bound-key point read → K=1 (defense in depth)
- [x] 2.1 In `getSplits`, compute effective K = 1 when `handle.limit().isPresent()` OR fully-bound point
      read; else configured K. (surface: `CqliteFlightSplitManager.getSplits`)
- [x] 2.2 Tests: LIMIT-pushed handle → split count == range count; fully-bound point read → one covering
      split; unbounded scan → range count × 4.

## 3. Token-range slicer + config
- [x] 3.1 Pure `TokenRangeSlicer`: equal-span K slices, overflow-safe unsigned 64-bit, half-open
      `(start,end]`, wraparound-correct, exact cover, no empty slice, inherits parent owner set. (new class)
- [x] 3.2 `cqlite.sub-splits-per-range` config (default 4, min 1, max 64). (surface: `CqliteFlightConfig`)
- [x] 3.3 Tests: K=4 exact cover + spans differ ≤1; wraparound seam; K=1 identity; boundary token assigned
      to exactly one slice under `tokenInRange`.

## 4. Slicing seam + weight-balanced rotation + aggregate exemption
- [x] 4.1 Apply the slicer at the single split-manager seam before scan-split construction, the snapshot
      chooser, and pruning. Aggregate handle builds at K=1; its snapshot chooser evaluated at K=1.
- [x] 4.2 Slice primaries by deterministic per-parent-range rotation; per-owner assigned span ≤1.25× mean.
- [x] 4.3 Tests: unequal-weight contiguous-ring fixture balances ≤1.25× mean + count-cap `ceil(M/N)` holds;
      deterministic across two `buildSplits` invocations (augments `CqliteFlightReplicaRotationTest`, which
      today asserts count only); aggregate exempt (one finalize split, member ranges == parent ranges).

## 5. SplitWeight
- [x] 5.1 `CqliteFlightSplit.getSplitWeight()` ∝ token span, mean-span → `standard()`, clamped to
      `fromProportion` bounds. `CqliteFlightAggregateSplit.getSplitWeight()` = clamped sum of member
      proportions, additionally clamped to a documented aggregate maximum.
- [x] 5.2 Tests: weight tracks span ~3×; aggregate saturates at the aggregate cap; extreme spans stay in
      valid range (no zero, no exception).

## 6. Invariants at slice granularity
- [x] 6.1 Every slice split retains the parent's full ordered owner set (#2241); `distinctReplicaHosts`
      == set of slice primaries (#2227 fail-closed); pruning operates on slices.
- [x] 6.2 Tests: full owner set retained per slice; snapshot chooser covers every slice primary.

## 7. E2E LIMIT-hang regression (hard merge gate — the lane that caught #2782)
- [x] 7.1 Add to `docker/e2e-test.sh`: `SELECT count(*) FROM (SELECT id FROM <table> LIMIT 2)` and a
      partial-predicate `LIMIT 2` complete + return expected rows within the harness timeout, at the
      default `cqlite.sub-splits-per-range`. Wired into `flight-trino-e2e`.
- [x] 7.2 A red `flight-trino-e2e` blocks merge — the closer MUST NOT arm `--auto` over it for this issue.

## 8. Docs (same change)
- [x] 8.1 Connector README: document `cqlite.sub-splits-per-range` (default 4, K=1 = pre-change identity,
      LIMIT/point-read auto-K=1). Update the flight-trino docs page.

## 9. Quality gates
- [x] 9.1 Trino connector build + Java units green (`./gradlew build`: 429 tests, 0 failures);
      `flight-trino-e2e` GREEN (mandatory — see 7.2, runs in CI); `scripts/agent-gate.sh` PASS
      (Rust gate of record, inside flow-closer).
- [ ] 9.2 C intent audit (spec-auditor) PASS — every requirement satisfied with a public-surface test.
- [ ] 9.3 roborev clean (blockers fixed; nits → follow-up issue).
