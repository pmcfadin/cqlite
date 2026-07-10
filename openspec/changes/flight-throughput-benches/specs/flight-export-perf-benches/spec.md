## ADDED Requirements

### Requirement: A reproducible export + Flight throughput bench suite over the public read path
The project SHALL provide a Criterion bench suite that measures throughput of the export/Flight read
path against pinned datasets, runnable both locally and in CI. The suite SHALL cover, at minimum: the
CQL→Arrow conversion data plane (`cqlite-core::export::arrow_convert::rows_to_record_batch`), the
json/csv/parquet export writers plus delta export, and an end-to-end Flight `do_get` streaming
throughput measurement. Each bench SHALL be reproducible from a documented command using
`CQLITE_DATASETS_ROOT`, and SHALL NOT exist on `main` prior to this change (its absence is the
fail-on-today state).

#### Scenario: The export/conversion benches run against pinned datasets
- **WHEN** `cargo bench -p cqlite-core --features cli-helpers,write-support,parquet --bench <export-bench>` runs with `CQLITE_DATASETS_ROOT` set to the fetched canonical datasets
- **THEN** a Criterion measurement is produced for CQL→Arrow conversion and for the json/csv/parquet + delta export writers
- **AND** the same commands are reproducible locally and in CI

#### Scenario: An end-to-end Flight do_get throughput bench exists
- **WHEN** the Flight bench target is built and run against a pinned fixture
- **THEN** it measures streaming throughput of a full `do_get` over a fixture
- **AND** the bench target did not exist on `main` before this change (fail-on-today)

### Requirement: Allocation and peak-memory budgets asserted by direct observation, never vacuously
The conversion hot path and the Flight producer SHALL each have a budget guard that asserts an
allocation-count / bytes / peak-memory bound by **direct** observation (dhat or equivalent global
allocator instrumentation, reusing the epic-H machinery — not duplicating it). A guard SHALL be
non-vacuous: it SHALL fail when its fixture is present but yields zero rows, and SHALL fail when zero
allocations/bytes were observed, so a run that measured nothing can never pass under the bound. The
guards SHALL land passing against the current-main figures (baseline locks); the aggressive target
bounds are owned by the AB/AE consumer issues.

#### Scenario: The producer / converter budget guard observes real allocation
- **WHEN** the budget guard runs against a present, non-empty fixture with the dhat allocator installed
- **THEN** it observes a non-zero allocation count / byte total for the producer or converter path
- **AND** it asserts that figure is within the documented current-main bound (with headroom)

#### Scenario: A present-but-empty fixture fails the guard rather than passing vacuously
- **WHEN** the fixture directory exists but the path under test yields zero rows or zero observed allocations
- **THEN** the guard fails loudly with an actionable message
- **AND** it never records a passing "0 ≤ budget" result

#### Scenario: An entirely absent fixture skip-registers without a fake measurement
- **WHEN** an optional fixture is not present in the checkout
- **THEN** the guard skip-registers (the gate reports SKIP) rather than producing a 0-row measurement or failing spuriously

### Requirement: A SKIP-aware, load-deterministic perf-gate entry covers the export/Flight path
The performance regression gate SHALL track the export/Flight path. The CPU-bound conversion + export
micro-benches SHALL be STRICT median-regression entries (`threshold_pct` ≥ 10) in
`cqlite-core/benches/perf-gate.json`, evaluated as a same-runner PR-vs-`main` ratio by
`scripts/ci/check_perf_regression.py`. The runtime/transport-dominated end-to-end Flight `do_get`
throughput bench SHALL be an ADVISORY entry (reported, never failing CI). A bench absent from a
baseline SHALL be reported SKIP (first landing stays green). The hard, mandatory per-gate signal for
this path SHALL be the load-deterministic dhat budget guard (allocation counts), NOT a wall-clock
number in `scripts/agent-gate.sh`.

#### Scenario: The export/conversion micro-benches gate as strict regression ratios
- **WHEN** `perf-gate.json` is read
- **THEN** it contains STRICT entries for the CQL→Arrow conversion and export benches, each with `threshold_pct >= 10`
- **AND** those benches are wired into both the PR and `main` `cargo bench` invocations in `.github/workflows/perf-regression.yml`

#### Scenario: The end-to-end Flight throughput bench is advisory, not blocking
- **WHEN** the Flight `do_get` throughput bench regresses past its threshold on the same runner
- **THEN** `check_perf_regression.py` reports it as an advisory regression
- **AND** the check exits zero (the advisory entry never fails CI), matching the `write/ingest_wal_on` policy

#### Scenario: A newly-added bench with no main baseline stays green
- **WHEN** a tracked export/Flight bench has no data in the `base` (main) baseline
- **THEN** `check_perf_regression.py` reports it as SKIP and does not fail the gate

#### Scenario: A regressed conversion path reds the strict gate
- **WHEN** the CQL→Arrow conversion is artificially slowed and re-measured against the fast baseline
- **THEN** `scripts/ci/check_perf_regression.py` flags the conversion bench as a REGRESSION and exits non-zero

### Requirement: A committed baseline artifact with a documented refresh procedure
The change SHALL commit the perf-gate policy artifact (`cqlite-core/benches/perf-gate.json`: tracked
bench list, thresholds, advisory classification) and SHALL record the human-readable current-main
baseline numbers in `cqlite-core/benches/README.md`, explicitly noting that this baseline already
includes the merged #1495 arrow-convert win (it is the post-#1495 `main` floor, not a pre-optimization
number). There SHALL be no committed absolute-timing baseline that could drift; the `base` baseline is
re-measured on `main` every CI run. The README SHALL document the refresh procedure.

#### Scenario: The baseline record names its post-#1495 provenance
- **WHEN** `cqlite-core/benches/README.md` is read
- **THEN** it records the current-main export/Flight baseline figures
- **AND** it states the baseline already contains the #1495 (PR #2312) win and is the reference #1496 and the AB/AE children measure against

#### Scenario: Refreshing a threshold is a documented, drift-free edit
- **WHEN** a threshold or tracked bench is retuned
- **THEN** the procedure edits `perf-gate.json` and updates the README numbers in the same PR
- **AND** no stale committed absolute-timing number exists to drift, because the base is re-measured each run

### Requirement: The benches exercise the public Flight/export surface (wiring evidence)
The end-to-end Flight bench SHALL drive the public Flight surface (the tonic `FlightService::do_get`
RPC over the in-process transport, or the public `FlightProducer` streaming API), not an internal-only
helper, and the export benches SHALL drive the public export/conversion entry points. Each bench SHALL
prove at setup that it exercised the real surface and returned at least one row; a zero-row or
non-exercising setup SHALL panic rather than silently record a measurement.

#### Scenario: The Flight bench drives the public do_get surface and returns rows
- **WHEN** the Flight `do_get` bench setup runs a request through the public Flight surface against the fixture
- **THEN** the streamed result contains at least one record batch / row
- **AND** the bench measured the public surface, not an internal helper

#### Scenario: A non-exercising setup panics instead of recording a fake measurement
- **WHEN** the bench setup returns zero rows or fails to engage the intended path against a present fixture
- **THEN** the bench setup panics with an actionable message
- **AND** no throughput measurement is produced from the non-exercising run
