## ADDED Requirements

### Requirement: The perf gate enforces a concurrent-scan scaling floor
The performance regression gate SHALL enforce a machine-independent concurrency-scaling floor on the
existing `concurrent_scan` benchmark for each backend (`buffered` and `mmap`): the ratio
`throughput(n4)/throughput(n1)`, computed within a single benchmark run as `degree_ratio ·
median(n1) / median(n4)`, SHALL be at least a configured `min_scaling`. The floor SHALL be evaluated on
the PR (`new`) baseline alone (it does not compare against `main`), so it is immune to cross-machine
timing variance. `cqlite-core/benches/perf-gate.json` SHALL carry the floor policy in a `scaling_floors`
array, and the floor value and its derivation SHALL be documented.

#### Scenario: A serialized read path reds the scaling floor
- **WHEN** the `concurrent_scan` n4 median is approximately four times the n1 median (the signature of a
  re-serialized scan path, e.g. a reintroduced shared `Mutex`), so scaling ≈ 1.0
- **THEN** `scripts/ci/check_perf_regression.py` reports the `concurrent_scan/*/n4` scaling entry below
  its floor and exits non-zero

#### Scenario: A healthy parallel scan passes the floor
- **WHEN** the `concurrent_scan` n4 and n1 medians yield scaling at or above `min_scaling` (healthy
  parallel scans measure ≈ 3.0)
- **THEN** the scaling entry is reported `ok` and does not fail the gate

#### Scenario: Absent concurrent-scan data skips the floor without failing
- **WHEN** the `concurrent_scan` n1 or n4 median is missing from the evaluated baseline
- **THEN** that scaling entry is reported SKIP and does not fail the gate

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

### Requirement: The read-while-write bench never spuriously reports zero writer ingest
The `read_while_write` benchmark SHALL guarantee that each writer task performs at least one ingest
before honoring the stop signal, so the bench's writers-ingested correctness floor cannot panic due to a
scheduling race, without changing what the bench measures (reader-side latency under sustained write
contention).

#### Scenario: A late-scheduled writer still satisfies the correctness floor
- **WHEN** a writer task is scheduled only after the readers finish and set the stop flag
- **THEN** the writer still performs at least one ingest, so `total_written ≥ WRITERS` and the bench does
  not panic
