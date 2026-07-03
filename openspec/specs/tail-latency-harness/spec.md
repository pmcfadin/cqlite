# tail-latency-harness Specification

## Purpose
TBD - created by archiving change tail-latency-harness. Update Purpose after archive.
## Requirements
### Requirement: A mixed-load tail-latency harness measures point-read percentiles under a background scan
The project SHALL provide a tail-latency harness that opens one shared `Database` over a BIG multi-chunk
fixture, starts one continuous background full-table scan, and issues a fixed-length stream of real
partition-targeted point reads against the same reader set, recording per-op latency. It SHALL also run
the identical point-read stream with no background scan as a scan-free baseline. The harness SHALL be
additive (bench/gate/test only) and change no read-path production code.

#### Scenario: The harness runs a point-read stream under a concurrent background scan
- **WHEN** the harness runs against the present BIG fixture
- **THEN** it drives one continuous background full-table scan concurrently with a fixed-length stream of point reads over the same shared `Database`
- **AND** it also runs the identical point-read stream with the background scan absent (the scan-free baseline)
- **AND** it records per-operation latency for the point-read stream in each mode

#### Scenario: The point-read stream drives the real targeted access path
- **WHEN** the harness sets up the point-read stream
- **THEN** the point read returns at least one row and reports a targeted `AccessPath` (`PartitionLookup`), not a full-scan fallback
- **AND** if the query returns zero rows or a non-targeted path the harness panics with an actionable message rather than measuring the wrong path

### Requirement: The harness emits machine-readable p50/p99/p999 JSON for both modes
The harness SHALL emit machine-readable JSON containing `p50`, `p99`, and `p999` (nanoseconds) for the
point-read stream under both the mixed load and the scan-free baseline, plus the derived gate ratios
`p99_over_p50` and `p99_mixed_over_scan_free`. Percentiles SHALL be computed from the recorded per-op
latencies and SHALL satisfy `p50 <= p99 <= p999`.

#### Scenario: JSON output contains both stat blocks and the gate ratios
- **WHEN** the harness completes a run
- **THEN** its JSON output contains a `mixed` block and a `scan_free` block, each with numeric `p50`, `p99`, `p999`
- **AND** it contains numeric `p99_over_p50` and `p99_mixed_over_scan_free` ratios
- **AND** within each block `p50 <= p99 <= p999`

#### Scenario: Percentiles are computed correctly from recorded latencies
- **WHEN** the percentile function is given a known vector of latencies
- **THEN** the returned p50/p99/p999 match the nearest-rank percentiles of that vector (proven by a unit test with no dataset dependency)

### Requirement: The tail gate is wired advisory-first with a documented flip to enforcing
The project SHALL provide a self-contained ratio gate — a committed policy file with per-ratio
thresholds and an `advisory` flag, and a checker script — that reads the harness JSON and reports each
ratio against its threshold. While `advisory` is true the checker SHALL report breaches but always exit
zero (never fail). Setting the policy to enforcing (or passing an enforce flag) SHALL make a threshold
breach exit non-zero. The flip-to-enforcing procedure SHALL be documented.

#### Scenario: Advisory mode reports a breach but does not fail
- **WHEN** the checker runs on harness JSON whose ratio exceeds its threshold while the policy is advisory
- **THEN** the checker prints the breach with an advisory status
- **AND** it exits zero (does not fail the build)

#### Scenario: Enforcing mode fails on a breach
- **WHEN** the checker runs in enforcing mode (policy `advisory: false` or the enforce flag) on harness JSON whose ratio exceeds its threshold
- **THEN** the checker exits non-zero

#### Scenario: Within-threshold ratios pass in either mode
- **WHEN** the checker runs on harness JSON whose ratios are within their thresholds
- **THEN** the checker exits zero regardless of advisory/enforcing mode

### Requirement: Harness output is persisted to a history ledger
Each harness run SHALL append one JSON record — timestamp, commit, both stat blocks, and the ratios — to
a history ledger, so tail latency over time is inspectable. The ledger holds generated run data and
SHALL NOT be committed to the repository; it is documented to consolidate into the Epic A5 unified
`history.jsonl` when A5 lands.

#### Scenario: A run appends a ledger record
- **WHEN** the harness completes a run
- **THEN** it appends one JSON record containing a timestamp, the commit, the `mixed` and `scan_free` stat blocks, and the ratios to the history ledger
- **AND** the ledger file is gitignored (not tracked in the repository)

### Requirement: The harness self-asserts the tail bound and is deterministic within tolerance
A committed test SHALL assert that the point-read p99 under mixed load is at most `k` times the scan-free
baseline p99, where `k` is chosen from the first measured run on `main` and documented (recording the
current convoy as the "before" number). A committed test SHALL assert that two consecutive scan-free
runs agree within a documented tolerance. Tests SHALL gate on ratios, never wall-clock absolutes, and
SHALL skip (not fail) when the fixture binary is absent while failing loudly when it is present but
yields zero rows.

#### Scenario: Mixed-load p99 is bounded by k times the baseline
- **WHEN** the self-assertion test runs against the present fixture
- **THEN** the point-read p99 under mixed load is at most `k` times the scan-free baseline p99, with `k` a documented const

#### Scenario: Two scan-free runs agree within tolerance
- **WHEN** the determinism test runs two consecutive scan-free point-read streams
- **THEN** their p50 values agree within the documented tolerance
- **AND** each stream satisfies `p50 <= p99 <= p999`

#### Scenario: Absent fixture skips, present-but-empty fails loudly
- **WHEN** the harness fixture binary is absent
- **THEN** the harness self-assertion test skips without failing
- **AND WHEN** the fixture is present but the point read returns zero rows
- **THEN** the harness panics rather than recording a zero-row measurement

