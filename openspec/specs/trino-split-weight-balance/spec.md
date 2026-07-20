# trino-split-weight-balance Specification

## Purpose
TBD - created by archiving change weight-balanced-splits. Update Purpose after archive.
## Requirements
### Requirement: Read-replica token ranges are deterministically sub-split into K equal-span slices before split construction

`CqliteFlightSplitManager` SHALL expand each Sidecar read-replica token range into K slices of equal
token span (K = `cqlite.sub-splits-per-range`, integer config, default 4, minimum 1, maximum 64)
**before** any downstream consumer runs — scan split construction, the snapshot host chooser, and
plan-time pruning all operate on slices, never on unsliced ranges. Sub-splitting SHALL apply to the
SCAN path only: an aggregated table handle plans exactly ONE finalize split whose page source fans
out to its members SEQUENTIALLY on a single driver, so sub-splitting it would multiply its
serialized DoGet round trips K× with no work to spread across nodes. The aggregate path SHALL
therefore build at K=1 (parent-range granularity), and its snapshot host chooser SHALL be evaluated
at that same K=1 granularity so every host a member reads still has a pinned snapshot.
Slicing SHALL be deterministic (a pure function of the range tokens and K), SHALL use overflow-safe
unsigned 64-bit token arithmetic, SHALL preserve the half-open `(start, end]` convention and the
existing wraparound flag semantics per slice, SHALL cover the parent range exactly (no gaps, no
overlaps, last slice ends at the parent's `end`), SHALL never emit an empty `(x, x]` slice, and each
slice SHALL inherit the parent range's replica owner set verbatim. With K=1 the emitted split set
SHALL be identical to current behavior.

#### Scenario: K=4 slices cover each range exactly

- **GIVEN** a topology of read-replica ranges with unequal token spans and `cqlite.sub-splits-per-range=4`
- **WHEN** `getSplits` runs for an unconstrained scan
- **THEN** each parent range yields 4 contiguous half-open slices whose spans differ by at most 1 token,
  whose union is exactly the parent `(start, end]`, and whose owner sets equal the parent's
- **AND** the emitted split count is 4× the range count.

#### Scenario: Wraparound range slices correctly

- **GIVEN** a range with `start >= end` (wraparound) spanning the `Long.MAX_VALUE`→`Long.MIN_VALUE` seam
- **WHEN** it is sliced with K=4
- **THEN** exactly one slice carries the wraparound (its `start >= end` unsigned-wise), the others are
  ordinary ranges, the four slices cover the parent exactly, and every slice boundary token is assigned
  to exactly one slice under the server's `tokenInRange` semantics.

#### Scenario: Aggregated handle is exempt from sub-splitting

- **GIVEN** an aggregated table handle and `cqlite.sub-splits-per-range=4`
- **WHEN** `getSplits` runs
- **THEN** exactly one finalize split is emitted and its member ranges are the PARENT read-replica
  ranges (count == range count, not range count × 4)
- **AND** the non-aggregated scan path over the same topology and config still emits range count × 4
  splits.

#### Scenario: K=1 is the identity

- **GIVEN** `cqlite.sub-splits-per-range=1`
- **WHEN** `getSplits` runs
- **THEN** the emitted splits (ranges, hosts, owner sets, count) are identical to the pre-change
  one-split-per-range behavior.

### Requirement: Slice-to-owner assignment is weight-balanced and deterministic

Each slice's primary owner SHALL be chosen by deterministic rotation across its **parent range's**
owner set such that, within any single parent range, per-owner slice counts differ by at most one,
and the owner receiving the remainder varies with the parent range's rotation key. Under an RF==N
fixture in which every range shares the same N owners and ranges have **deliberately unequal token
spans**, the per-owner sum of assigned token span SHALL be within **≤1.25×** of the mean across
owners. Assignment SHALL be deterministic and stable across re-planning: two independent
`buildSplits` invocations over the same topology SHALL yield identical per-slice primaries and
failover orderings.

#### Scenario: Unequal-weight fixture balances within 1.25x of mean

- **GIVEN** an RF==N==3 fixture whose ranges share one owner set, CONTIGUOUSLY TILE the ring from
  `Long.MIN_VALUE` to `Long.MAX_VALUE` (so the topology passes the fail-closed ring-coverage guard
  and can actually reach `buildSplits`), and whose token spans are deliberately unequal (spans
  varying by 8× or more across ranges)
- **WHEN** splits are built with K=4
- **THEN** for each owner, Σ(token span of slices whose primary is that owner) is ≤ 1.25× the mean
  per-owner Σ span
- **AND** the existing count-spread cap still holds: no owner is primary for more than
  `ceil(totalSlices / N)` slices.

#### Scenario: Assignment is deterministic across invocations

- **GIVEN** the same topology and configuration
- **WHEN** `buildSplits` is invoked twice independently
- **THEN** slice boundaries, per-slice primary hosts, and per-slice failover host orderings are
  identical between the two invocations.

### Requirement: Connector splits report SplitWeight proportional to assigned token span

`CqliteFlightSplit.getSplitWeight()` SHALL return a weight proportional to the slice's token span,
scaled so a mean-span slice reports `SplitWeight.standard()`, clamped to Trino's valid proportion
range. `CqliteFlightAggregateSplit.getSplitWeight()` SHALL return the clamped sum of the same
per-member proportions over the ranges it covers, additionally clamped to a documented
scheduler-meaningful aggregate maximum — the finalize split occupies ONE driver, so past roughly a
node's admission budget more weight changes no scheduling decision and only starves the finalize
node of co-scheduled work. No split SHALL report the SPI default by omission.

#### Scenario: Weight tracks token span

- **GIVEN** two slices whose token spans differ by a factor of ~3
- **WHEN** their splits are constructed
- **THEN** their `getSplitWeight()` values differ by a factor of ~3 (within clamping), and a
  mean-span slice reports the standard weight.

#### Scenario: Aggregate weight saturates at the aggregate cap

- **GIVEN** a finalize split whose member proportions sum well past the aggregate maximum
- **WHEN** `getSplitWeight()` is called
- **THEN** it returns exactly the aggregate maximum (not the 1000 single-split maximum), while a
  fan-out below that maximum stays strictly proportional to the summed member proportions.

#### Scenario: Extreme spans stay within Trino's valid weight range

- **GIVEN** a topology producing a near-zero-span slice and a very large-span slice
- **WHEN** their splits are constructed
- **THEN** both `getSplitWeight()` values fall within Trino's accepted `fromProportion` bounds
  (no exception, no zero weight).

### Requirement: Existing failover, snapshot-cover, and pruning invariants hold at slice granularity

Every slice's split SHALL retain the parent range's **full ordered owner set** as failover (#2241).
The snapshot host chooser (`distinctReplicaHosts`) SHALL return exactly the set of per-slice primary
hosts, so snapshot-mode reads never target a host without a pinned snapshot (#2227, fail-closed).
Plan-time pruning (#2679) SHALL operate on slices: a constraint that fully binds the partition key
SHALL prune to exactly the covering slice(s) — for a single bound key, exactly **one** slice.

#### Scenario: Full owner set retained per slice

- **GIVEN** any sliced topology
- **WHEN** splits are built
- **THEN** every split's replica-host list contains the parent range's complete owner set, primary
  first, in a deterministic failover order.

#### Scenario: Snapshot chooser covers every slice primary

- **GIVEN** a sliced RF==N topology
- **WHEN** `distinctReplicaHosts` computes the snapshot host set
- **THEN** it equals exactly the set of primary hosts over all emitted slices.

#### Scenario: Fully-bound point read prunes to one slice

- **GIVEN** split pruning enabled and a constraint binding the full partition key to a single value
- **WHEN** `getSplits` runs with K=4
- **THEN** exactly one split is emitted — the single slice whose `(start, end]` contains the key's
  Murmur3 token — and its token range is strictly narrower than the parent range's (for K>1 on a
  non-degenerate span).

