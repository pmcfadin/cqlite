# reconcile-overlap-measurement — delta for reconcile-overlap-multiplier (issue #2043)

## ADDED Requirements

### Requirement: A k-parameterized reconcile overlap benchmark
The repository SHALL carry a Criterion benchmark that reports per-row merge cost for row clusters
spanning **k** SSTable generations, for k ∈ {1, 2, 5, 10, 20} crossed with collision mixes
{`disjoint`, `lww_overwrite`, `tombstone`, `ttl_expiring`, `field_blend`}, driven through the public
`KWayMerger` surface. It SHALL be runnable by a single documented command and SHALL NOT require the
fetched dataset corpus.

#### Scenario: The benchmark runs from a documented one-line command with no vendored datasets
- **WHEN** `cargo bench -p cqlite-core --features write-support --bench reconcile_overlap` is run in a checkout where `CQLITE_DATASETS_ROOT` is unset and no dataset binaries are present
- **THEN** every arm of the k × mix matrix executes and reports a per-row time, and no arm errors, skips, or reports zero rows

#### Scenario: Each k arm actually spans k generations
- **GIVEN** a fixture built for a given k
- **WHEN** the fixture's data directory is inspected before the merge is driven
- **THEN** it contains exactly k `Data.db` files, and the merge input reports k readers

#### Scenario: Cost is reported per row with collision density alongside
- **WHEN** an arm completes
- **THEN** it reports Criterion `Throughput::Elements` over the merged output row count, and the run records the observed collisions-per-row and purge counts for that arm

### Requirement: TTL expiry is deterministic under the release bench profile
The benchmark SHALL pin reconcile-time `now` through `KWayMerger::with_now_secs` and SHALL NOT depend
on the `CQLITE_TTL_NOW_OVERRIDE_SECS` environment seam, which is `#[cfg(debug_assertions)]` and
compiles out of the release profile that `cargo bench` uses.

#### Scenario: The expiring-TTL arm yields identical expiry across repeat runs
- **WHEN** the `ttl_expiring` arm is run twice on the same commit, in release, at different wall-clock times separated by more than the fixture's TTL
- **THEN** both runs report the same number of expired cells and the same surviving output row count

#### Scenario: No-TTL arms keep the expiry machinery out of the measurement
- **WHEN** an arm whose mix contains no expiring cells is constructed
- **THEN** it passes `None` for `now_secs`, and no TTL expiry work is performed for that arm

#### Scenario: The environment seam is absent from the benchmark
- **WHEN** the benchmark source and its fixture builder are scanned for `CQLITE_TTL_NOW_OVERRIDE_SECS`
- **THEN** there are no occurrences

### Requirement: The harness is anchored to the published singleton baseline
The `disjoint` / k=1 arm SHALL be validated against the published ~2.0 µs/row narrow-disjoint-singleton
figure (`docs/research/phase2-verify-stage2.md:226-232`) before any multiplier is derived, so that a
mis-built harness cannot silently produce a wrong slope.

#### Scenario: An anchor outside the accepted band voids the run
- **WHEN** the `disjoint` k=1 arm reports a per-row cost that differs from the published singleton baseline by more than the band stated in the record
- **THEN** the run is declared void, no multiplier is derived from it, and the record states the discrepancy instead of a verdict

### Requirement: Measurements are rejected when taken on a loaded machine
The record SHALL carry the machine specification, the commit SHA, and the 1-minute load average
observed at run start; results taken above a stated load ceiling SHALL NOT be used to derive the
multiplier.

#### Scenario: A run above the load ceiling is not used
- **WHEN** the benchmark completes on a machine whose recorded load average at run start exceeds the ceiling stated in the record
- **THEN** the numbers are not substituted into the derate and the record marks that run as discarded with its load figure

### Requirement: A research record carrying the derived overlap multiplier
The change SHALL add `docs/research/issue-2043-reconcile-overlap-multiplier.md` containing the measured
k→ns/row table, the derived multiplier `cost(k)/cost(1)` per mix, and a verdict that either tightens
the §3 gen-overlap band or states why it cannot. Any field-k selection SHALL be labeled an assumption,
not a measurement, and SHALL name its basis.

#### Scenario: The multiplier is derived and the derate band is addressed
- **WHEN** the record is read after a valid run
- **THEN** it presents the k→ns/row table with machine specs and commit SHA, the per-mix `cost(k)/cost(1)` ratios, and an explicit statement of the tightened §3 gen-overlap band or a reasoned refusal to tighten it

#### Scenario: The field point is labeled as an assumption
- **WHEN** the record substitutes a multiplier into the §3 arithmetic
- **THEN** the k it assumes for field compaction state is stated inline, marked assumption-not-measurement, its basis (STCS-derived expected-k band) is named, and #2818 is cited as the measurement that would replace it

#### Scenario: The L3 disposition is resolved conditionally with written arithmetic
- **WHEN** the record's verdict section is read
- **THEN** it states which k-band makes `P2:stage2`'s ~1.20× correct and which makes `P2:row-engine`'s ~1.03–1.08× correct, gives the arithmetic connecting cluster shape to the achievable speedup, and states plainly that the final disposition follows once field k is known

### Requirement: The program document cites the measurement
The program document SHALL cite the new record at both consuming sites — the §3 derate discussion of
the generation-overlap term and the §4 L3 tension flag.

#### Scenario: Both program-doc sites are updated
- **WHEN** the program document is read after this change
- **THEN** §3's gen-overlap term and §4's L3 tension flag each link the record, and the §4 flag reflects its post-measurement state rather than describing the data as absent

### Requirement: The benchmark is registered advisory-only
The new benchmark IDs SHALL appear in `cqlite-core/benches/perf-gate.json` under `advisory_benches`
with no `threshold_pct` entry, so the instrument never blocks a merge.

#### Scenario: No strict gate entry is added
- **WHEN** `perf-gate.json` is inspected after this change
- **THEN** the new bench IDs appear only in `advisory_benches`, and the `benches` array contains no entry for them
