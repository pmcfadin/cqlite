# read-perf-gate Specification

## Purpose
TBD - created by archiving change read-perf-gate. Update Purpose after archive.
## Requirements
### Requirement: The perf gate benches the real point-read path, not a LIMIT-1 scan proxy
The performance regression gate SHALL measure a benchmark that drives the real point-read path — a
fully-constrained `WHERE pk = ?` lookup through the public `Database` query API, which engages the
partition-targeted access path (bloom/BTI presence prune → single-candidate seek → chunk decode). The
benchmark SHALL NOT be a `SELECT … LIMIT 1` scan. The benchmark SHALL prove at setup that the real
targeted path engaged by asserting the returned `QueryResult.access_path` is a targeted path
(`PartitionLookup`), never a `FallbackFullScan`.

#### Scenario: The gated point-read bench drives the targeted access path
- **WHEN** the `read/get_partition` bench setup runs a `SELECT * … WHERE id = <uuid-literal>` against the fixture through `Database::execute`
- **THEN** `QueryResult.access_path` is `Some(PartitionLookup)` (a targeted path), not `FullScan` or `FallbackFullScan`
- **AND** the query returns at least one row

#### Scenario: An accidental full-scan fallback fails the bench loudly
- **WHEN** the point query would fall back to a full scan (targeted path did not engage)
- **THEN** the bench setup panics rather than silently measuring the scan path
- **AND** no `read/get_partition_*` measurement is produced from the fallback

### Requirement: Both BIG (multi-chunk) and BTI point-read variants are gated
The gate SHALL track a BIG-format point-read bench over a fixture whose Data.db spans more than one
compression chunk, and a BTI-format point-read bench, each with a median-regression failure threshold of
at least 10%. The old `read/point_lookup` LIMIT-1 proxy SHALL NOT be present in the gate configuration.

#### Scenario: perf-gate.json tracks the real point-read benches
- **WHEN** `cqlite-core/benches/perf-gate.json` is read
- **THEN** it contains a BIG point-read bench id and a BTI point-read bench id, each with `threshold_pct >= 10`
- **AND** it does NOT contain a `read/point_lookup` entry

#### Scenario: The BIG fixture spans multiple compression chunks
- **WHEN** the BIG fixture's `CompressionInfo.db` is parsed via `CompressionInfo::parse`
- **THEN** the parsed chunk count (`chunk_offsets.len()`) is greater than 1
- **AND** a committed test asserts this, so the multi-chunk guarantee cannot silently erode

### Requirement: The point-read benches never silently measure an empty dataset
A dataset-dependent point-read bench SHALL error loudly (panic at setup) when its fixture is present but
yields zero rows or a non-targeted access path, and SHALL be skipped (not registered, so the gate reports
SKIP and does not fail) only when its fixture table directory is entirely absent.

#### Scenario: Present-but-broken fixture panics
- **WHEN** the fixture table directory exists but the point query returns zero rows
- **THEN** the bench setup panics with an actionable message (never records a 0-row measurement)

#### Scenario: Absent optional fixture skips without failing the gate
- **WHEN** an optional fixture (e.g. the BTI `test_da` table) is not present in the checkout
- **THEN** that bench variant is not registered and `check_perf_regression.py` reports it as SKIP without failing the gate

### Requirement: The gate demonstrably fails on a regressed point path
A slowdown on the real point-read path SHALL cause the gated point-read bench to fail the regression
check. This SHALL be demonstrated by a red-run (artificially slowing the point path, then showing
`check_perf_regression.py` reports the bench as a REGRESSION with a non-zero exit).

#### Scenario: A slowed point path reds the gate
- **WHEN** the point-read path is artificially slowed and the bench is re-measured against the fast baseline
- **THEN** `scripts/ci/check_perf_regression.py` flags the point-read bench as a REGRESSION and exits non-zero

### Requirement: The perf gate enforces a concurrent-scan scaling floor
The performance regression gate SHALL enforce a machine-independent concurrency-scaling floor on the
existing `concurrent_scan` benchmark for each backend (`buffered` and `mmap`): the ratio
`throughput(n4)/throughput(n1)`, computed within a single benchmark run as `degree_ratio ·
median(n1) / median(n4)`, SHALL be at least a configured `min_scaling`. The floor SHALL be evaluated on
the PR (`new`) baseline alone (it does not compare against `main`), so it is immune to cross-machine
timing variance. `cqlite-core/benches/perf-gate.json` SHALL carry the floor policy in a `scaling_floors`
array, and the floor value and its derivation SHALL be documented. Because a scaling floor is intra-run
(its data is always present on any run that benches the target), missing data for a configured floor
SHALL fail the gate loudly rather than silently skip it — so a typo'd id, an omitted `--bench`, or a
bench that produced no data cannot quietly disable the gate. A floor MAY opt into skip-on-absent with
`"optional": true` for a genuinely optional fixture.

#### Scenario: A serialized read path reds the scaling floor
- **WHEN** the `concurrent_scan` n4 median is approximately four times the n1 median (the signature of a
  re-serialized scan path, e.g. a reintroduced shared `Mutex`), so scaling ≈ 1.0
- **THEN** `scripts/ci/check_perf_regression.py` reports the `concurrent_scan/*/n4` scaling entry below
  its floor and exits non-zero

#### Scenario: A healthy parallel scan passes the floor
- **WHEN** the `concurrent_scan` n4 and n1 medians yield scaling at or above `min_scaling` (healthy
  parallel scans measure ≈ 3.0)
- **THEN** the scaling entry is reported `ok` and does not fail the gate

#### Scenario: Missing required scaling data fails the gate loudly
- **WHEN** the n1 or n4 median for a configured (non-optional) scaling floor is missing from the
  evaluated baseline
- **THEN** `scripts/ci/check_perf_regression.py` reports the entry as MISSING DATA and exits non-zero
  (it does not silently skip and pass)

#### Scenario: An optional scaling floor skips when absent
- **WHEN** a scaling floor marked `"optional": true` has no data in the evaluated baseline
- **THEN** that entry is reported SKIP and does not fail the gate

### Requirement: The perf gate tracks the read-while-write median under concurrent writes
The performance regression gate SHALL track the existing `read_while_write/readers6_writers2` benchmark
as a strict median-regression entry in `cqlite-core/benches/perf-gate.json` with a documented threshold,
so a reader-side regression under concurrent write load can no longer merge silently. The gated metric
SHALL be the Criterion median (reader-side aggregate latency under write load); the reader-side p99 tail
is explicitly out of scope for this gate and owned by the A2 tail-latency harness (#1563), noted in the
policy.

#### Scenario: read_while_write is a strict gated bench
- **WHEN** `cqlite-core/benches/perf-gate.json` is read
- **THEN** it contains a `read_while_write/readers6_writers2` entry with a `threshold_pct`
- **AND** the entry is not in `advisory_benches` (it is strict)

#### Scenario: The gate has data for both concurrency benches
- **WHEN** `.github/workflows/perf-regression.yml` runs the Criterion benchmarks
- **THEN** both `concurrent_scan` and `read_while_write` are included in the PR and main `cargo bench`
  invocations, so the gate script has medians to evaluate

### Requirement: The read-while-write bench guarantees write overlap and never reports zero writer ingest
The `read_while_write` benchmark SHALL guarantee that each writer task performs at least one ingest
before honoring the stop signal (so the writers-ingested correctness floor cannot panic on a scheduling
race), AND SHALL not begin the readers' timed scans until every writer is actively ingesting (so the
measured reader-side latency genuinely overlaps sustained write load rather than an artificially
uncontended window), without changing what the bench measures.

#### Scenario: A late-scheduled writer still satisfies the correctness floor
- **WHEN** a writer task is scheduled only after the readers finish and set the stop flag
- **THEN** the writer still performs at least one ingest, so `total_written ≥ WRITERS` and the bench does
  not panic

#### Scenario: Reader timing overlaps live write pressure
- **WHEN** the bench measures an iteration
- **THEN** the readers' timed scans begin only after all `WRITERS` writers have performed their first
  ingest (a readiness barrier), so the gated median reflects reader latency under active write contention

