# trino-split-weight-balance

## ADDED Requirements

### Requirement: Early operator close cancels the Flight DoGet stream (P0 #2782 root fix)

The scan page source SHALL explicitly cancel the underlying Flight `DoGet` stream when Trino closes the
operator early — before the stream is fully drained, as happens once a pushed `LIMIT` is satisfied and the
remaining splits are cancelled — rather than merely releasing the handle. Cancellation
SHALL be idempotent and non-blocking, SHALL propagate through the replica-failover stream wrapper to the
active underlying stream, and SHALL NOT wait to consume remaining server batches. This holds at ANY split
count, so an un-consumed stream can never block query completion. This requirement is independent of
sub-splitting and fixes the #2782 root cause even at K=1.

#### Scenario: Early close cancels an un-drained stream without blocking
- **GIVEN** a scan split whose Flight `DoGet` stream has delivered some but not all batches
- **WHEN** Trino closes the page source early (operator cancelled after a satisfied LIMIT)
- **THEN** the page source cancels the underlying Flight stream and returns from `close()` without
  blocking on remaining batches
- **AND** calling `close()` again is a harmless no-op (idempotent)

#### Scenario: Cancellation reaches the active failover stream
- **GIVEN** a replica-failover stream wrapping an active underlying `DoGet` stream
- **WHEN** the page source cancels on early close
- **THEN** the cancel propagates to the currently-active underlying stream (not only the wrapper)

### Requirement: LIMIT-pushed and bound-key point reads plan at K=1 (defense in depth)

Sub-splitting SHALL be suppressed for the query shapes that both do not benefit from it and historically
triggered the hang: when the table handle carries a pushed-down `LIMIT`, OR the read is a fully-bound
partition-key point read, `getSplits` SHALL plan at **K=1** (parent-range granularity) regardless of the
configured `cqlite.sub-splits-per-range`. Only unbounded scan / aggregate-free full-range reads (and, per
the aggregate requirement below, NOT the aggregate finalize split) are eligible for K>1. This keeps the
LIMIT shape structurally out of the multi-stream path even if the drain fix regresses.

#### Scenario: A LIMIT-pushed handle plans at K=1
- **GIVEN** `cqlite.sub-splits-per-range=4` and a table handle carrying a pushed-down `LIMIT n`
- **WHEN** `getSplits` runs
- **THEN** the emitted scan split count equals the read-replica range count (K=1), not range count × 4

#### Scenario: A fully-bound point read plans at K=1
- **GIVEN** `cqlite.sub-splits-per-range=4`, split pruning enabled, and a constraint fully binding the
  partition key to a single value
- **WHEN** `getSplits` runs
- **THEN** exactly one split is emitted for the covering range (the point read is not sub-split)

#### Scenario: An unbounded scan is eligible for K>1
- **GIVEN** `cqlite.sub-splits-per-range=4` and an unconstrained scan handle (no LIMIT, no bound key)
- **WHEN** `getSplits` runs
- **THEN** the emitted scan split count is range count × 4

### Requirement: Read-replica token ranges are deterministically sub-split into K equal-span slices

`CqliteFlightSplitManager` SHALL expand each Sidecar read-replica token range into K slices of equal token
span (K = `cqlite.sub-splits-per-range`, integer config, default 4, minimum 1, maximum 64) before any
downstream consumer runs — scan split construction, the snapshot host chooser, and plan-time pruning all
operate on slices. Sub-splitting applies to the eligible SCAN path only (see the K=1-exemption and
aggregate requirements). Slicing SHALL be deterministic (a pure function of the range tokens and K), use
overflow-safe unsigned 64-bit token arithmetic, preserve the half-open `(start, end]` convention and the
wraparound flag per slice, cover the parent range exactly (no gaps/overlaps; last slice ends at the
parent's `end`), never emit an empty `(x, x]` slice, and each slice SHALL inherit the parent range's
replica owner set verbatim. With K=1 the emitted split set SHALL be identical to current behavior.

#### Scenario: K=4 slices cover each range exactly
- **GIVEN** read-replica ranges with unequal token spans and `cqlite.sub-splits-per-range=4`
- **WHEN** `getSplits` runs for an unconstrained scan
- **THEN** each parent range yields 4 contiguous half-open slices whose spans differ by at most 1 token,
  whose union is exactly the parent `(start, end]`, and whose owner sets equal the parent's
- **AND** the emitted split count is 4× the range count

#### Scenario: Wraparound range slices correctly
- **GIVEN** a range with `start >= end` (wraparound) spanning the `Long.MAX_VALUE`→`Long.MIN_VALUE` seam
- **WHEN** it is sliced with K=4
- **THEN** exactly one slice carries the wraparound, the others are ordinary ranges, the four slices cover
  the parent exactly, and every boundary token is assigned to exactly one slice under `tokenInRange`

#### Scenario: K=1 is the identity
- **GIVEN** `cqlite.sub-splits-per-range=1`
- **WHEN** `getSplits` runs
- **THEN** the emitted splits (ranges, hosts, owner sets, count) are identical to the pre-change
  one-split-per-range behavior

### Requirement: Aggregated handle is exempt from sub-splitting

An aggregated table handle SHALL plan exactly ONE finalize split whose page source fans out to its member
ranges sequentially on a single driver; sub-splitting it would multiply its serialized `DoGet` round trips
K× with no work to spread. The aggregate path SHALL build at K=1 (parent-range granularity), and its
snapshot host chooser SHALL be evaluated at that same K=1 granularity so every host a member reads has a
pinned snapshot.

#### Scenario: Aggregated handle is exempt from sub-splitting
- **GIVEN** an aggregated table handle and `cqlite.sub-splits-per-range=4`
- **WHEN** `getSplits` runs
- **THEN** exactly one finalize split is emitted and its member ranges are the PARENT read-replica ranges
  (count == range count, not range count × 4)
- **AND** the non-aggregated eligible scan path over the same topology and config still emits range
  count × 4 splits

### Requirement: Slice-to-owner assignment is weight-balanced and deterministic

Each slice's primary owner SHALL be chosen by deterministic rotation across its parent range's owner set
such that, within any single parent range, per-owner slice counts differ by at most one, and the owner
receiving the remainder varies with the parent range's rotation key. Under an RF==N fixture in which every
range shares the same N owners and ranges have deliberately unequal token spans, the per-owner sum of
assigned token span SHALL be within **≤1.25×** of the mean across owners. Assignment SHALL be deterministic
and stable across re-planning: two independent `buildSplits` invocations over the same topology SHALL yield
identical per-slice primaries and failover orderings.

#### Scenario: Unequal-weight fixture balances within 1.25x of mean
- **GIVEN** an RF==N==3 fixture whose ranges share one owner set, contiguously tile the ring from
  `Long.MIN_VALUE` to `Long.MAX_VALUE`, and whose token spans are deliberately unequal (varying by 8× or
  more across ranges)
- **WHEN** splits are built with K=4
- **THEN** for each owner, Σ(token span of slices whose primary is that owner) is ≤ 1.25× the mean
  per-owner Σ span
- **AND** no owner is primary for more than `ceil(totalSlices / N)` slices (the count-spread cap still holds)

#### Scenario: Assignment is deterministic across invocations
- **GIVEN** the same topology and configuration
- **WHEN** `buildSplits` is invoked twice independently
- **THEN** slice boundaries, per-slice primary hosts, and per-slice failover host orderings are identical

### Requirement: Connector splits report SplitWeight proportional to assigned token span

`CqliteFlightSplit.getSplitWeight()` SHALL return a weight proportional to the slice's token span, scaled
so a mean-span slice reports `SplitWeight.standard()`, clamped to Trino's valid proportion range.
`CqliteFlightAggregateSplit.getSplitWeight()` SHALL return the clamped sum of the same per-member
proportions over the ranges it covers, additionally clamped to a documented scheduler-meaningful aggregate
maximum (the finalize split occupies ONE driver, so past a node's admission budget more weight changes no
scheduling decision and only starves the finalize node of co-scheduled work). No split SHALL report the
SPI default by omission.

#### Scenario: Weight tracks token span
- **GIVEN** two slices whose token spans differ by a factor of ~3
- **WHEN** their splits are constructed
- **THEN** their `getSplitWeight()` values differ by a factor of ~3 (within clamping), and a mean-span
  slice reports the standard weight

#### Scenario: Aggregate weight saturates at the aggregate cap
- **GIVEN** a finalize split whose member proportions sum well past the aggregate maximum
- **WHEN** `getSplitWeight()` is called
- **THEN** it returns exactly the aggregate maximum (not the 1000 single-split maximum), while a fan-out
  below that maximum stays strictly proportional to the summed member proportions

#### Scenario: Extreme spans stay within Trino's valid weight range
- **GIVEN** a topology producing a near-zero-span slice and a very large-span slice
- **WHEN** their splits are constructed
- **THEN** both `getSplitWeight()` values fall within Trino's accepted `fromProportion` bounds (no
  exception, no zero weight)

### Requirement: Existing failover, snapshot-cover, and pruning invariants hold at slice granularity

Every slice's split SHALL retain the parent range's full ordered owner set as failover (#2241). The
snapshot host chooser (`distinctReplicaHosts`) SHALL return exactly the set of per-slice primary hosts, so
snapshot-mode reads never target a host without a pinned snapshot (#2227, fail-closed). Plan-time pruning
(#2679/#2806) SHALL operate on slices: a constraint that fully binds the partition key SHALL prune to
exactly the covering slice(s) — for a single bound key, exactly one slice (which, per the K=1-exemption
requirement, is planned at parent-range granularity).

#### Scenario: Full owner set retained per slice
- **GIVEN** any sliced topology
- **WHEN** splits are built
- **THEN** every split's replica-host list contains the parent range's complete owner set, primary first,
  in a deterministic failover order

#### Scenario: Snapshot chooser covers every slice primary
- **GIVEN** a sliced RF==N topology
- **WHEN** `distinctReplicaHosts` computes the snapshot host set
- **THEN** it equals exactly the set of primary hosts over all emitted slices

### Requirement: A docker-compose E2E LIMIT hang regression gates merge

The `Flight ↔ Trino E2E` docker-compose lane SHALL include an assertion that a small pushed-`LIMIT` query
(e.g. `SELECT count(*) FROM (SELECT id FROM <table> LIMIT 2)`) and a partial-predicate `LIMIT` query
complete and return the expected rows within the harness timeout, exercised with the default
`cqlite.sub-splits-per-range`. A hang or timeout on this lane SHALL block merge — the certification MUST NOT
be armed for auto-merge over a red `flight-trino-e2e` result. This is the exact lane that caught #2782.

#### Scenario: LIMIT 2 completes under the default sub-split configuration
- **WHEN** the E2E runs `SELECT count(*) FROM (SELECT id FROM <table> LIMIT 2)` at the default
  `cqlite.sub-splits-per-range`
- **THEN** the query returns the expected row(s) within the harness timeout (no 180s hang)
- **AND** a partial-predicate `LIMIT 2` query likewise returns exactly its expected rows

#### Scenario: A red E2E blocks merge
- **WHEN** the `flight-trino-e2e` lane reports a hang/timeout on the LIMIT regression
- **THEN** the change is not merged (auto-merge is not armed over the red result)
