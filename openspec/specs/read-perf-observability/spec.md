# read-perf-observability Specification

## Purpose
TBD - created by archiving change cold-open-ledger. Update Purpose after archive.
## Requirements
### Requirement: A cold-open benchmark measures reader open cost

The bench suite SHALL include a benchmark that times a fresh open of a
`Database`/`SSTableReader` from cold — including component loading
(Statistics/Summary/CompressionInfo and, for BTI, the trie root) — on a BIG
multi-chunk fixture and on a BTI (`test_da`) fixture. The bench SHALL be a
custom-harness (`harness = false`) Criterion target. When a fixture table directory
is entirely absent the corresponding variant SHALL be skipped (not registered), and
when a fixture is present but yields an unusable open it SHALL panic at setup rather
than record a misleading measurement (parity-is-truth).

#### Scenario: Cold-open bench times a real open on a present fixture
- **WHEN** the `open/cold` bench runs against a present BIG multi-chunk fixture
- **THEN** it measures a fresh open that loads the SSTable components (not a cached/warm reuse)
- **AND** it produces an `open/cold_big` measurement

#### Scenario: Absent fixture skips without failing
- **WHEN** the BTI `test_da` fixture directory is not present in the checkout
- **THEN** the `open/cold_bti` variant is not registered and the bench run does not fail on its account

#### Scenario: Present-but-broken fixture panics rather than mis-measures
- **WHEN** a fixture directory exists but a fresh open cannot be established (e.g. components missing)
- **THEN** the bench setup panics with an actionable message and records no `open/cold` measurement

### Requirement: A per-reader memory benchmark records footprint after opening N readers

The bench suite SHALL include a benchmark (`mem/open_n_readers`) that opens N
readers over a fixture and records the process heap/RSS after, so a later change
(bounded `Index.db` mode) has a before/after per-reader memory gauge. The recorded
memory value SHALL be appended to the unified history ledger as a metric.

#### Scenario: The memory bench records a footprint metric
- **WHEN** `mem/open_n_readers` opens N readers over a present fixture
- **THEN** it records a per-reader memory metric (heap or RSS bytes) for N readers
- **AND** that metric is appended to the unified history ledger

### Requirement: A single unified append-only history ledger persists per-metric run records

Perf run history SHALL be persisted to one append-only ledger at
`target/profiling/history.jsonl`, one JSON object per line, with the schema
`{ts, commit, bench, metric, value, unit}` — one record per metric. A single shared
Rust bench-support module SHALL be the append path for the harness benches, and
`scripts/profile_report.py` SHALL write its criterion medians and peak heap in the
same schema. The ledger SHALL be gitignored generated run data (documented as
CI-artifact-uploadable), and the previous bespoke `benches/tail-latency-history.jsonl`
ledger SHALL be retired onto this unified ledger. Ledger append SHALL be
best-effort: a write failure SHALL log and SHALL NOT abort or fail a bench run.

#### Scenario: A harness bench appends one line per metric in the unified schema
- **WHEN** an A-series harness bench emits metrics through the shared ledger module
- **THEN** each metric is appended to `target/profiling/history.jsonl` as one JSON line
- **AND** each line has fields `ts`, `commit`, `bench`, `metric`, `value`, and `unit`

#### Scenario: The tail harness writes to the unified ledger, not a bespoke file
- **WHEN** the `tail_latency` bench runs
- **THEN** its metrics are appended to the unified `target/profiling/history.jsonl`
- **AND** no `benches/tail-latency-history.jsonl` file is produced or referenced

#### Scenario: A ledger write failure does not fail the bench
- **WHEN** the ledger path cannot be written (e.g. an unwritable directory)
- **THEN** the bench logs the failure to stderr and completes its measurement normally

### Requirement: `profile.sh report` reads the unified ledger back

`scripts/profile_report.py` (invoked by `./scripts/profile.sh report`) SHALL read
the unified `history.jsonl` back and render a longitudinal per-metric view (the
latest recorded value per metric and its delta versus the previous distinct commit).
A round-trip SHALL be covered by a test: metrics written to a ledger are read back
and rendered by the report.

#### Scenario: Written metrics round-trip through the report
- **WHEN** metric records are appended to a `history.jsonl` and `profile_report.py` reads that ledger
- **THEN** the report includes those metrics with their latest values
- **AND** the round-trip is asserted by a committed test

### Requirement: Cfg-gated test-only read-work counters exist with reset/read APIs and zero release overhead

A read-work counters module SHALL provide, modeled on the existing
`work_counters`/`SCAN_FOR_KEY_CALLS` convention: `TRIE_WALKS` (BTI descent count),
`DECOMPRESS_CALLS` (per-chunk decompress invocations), `SEEK_CALLS` (block-read seek
count), `FILE_OPENS` (`open(2)` count at the `BlockSource` open sites), and an fd
high-water-mark helper (platform-gated: `/dev/fd` on macOS, `/proc/self/fd` on
Linux, `None` elsewhere). Each counter SHALL have a `reset` and a read accessor. The
counters SHALL be gated behind `#[cfg(any(test, feature = "work-counters"))]` so
that a default/release build pays nothing (the increment call sites SHALL be
unconditional but compile to a no-op in release). Each counter SHALL be documented
in the module with the epic-child that consumes it.

#### Scenario: A default release build pays nothing for the counters
- **WHEN** the crate is built without the `work-counters` feature and without `cfg(test)`
- **THEN** the counter increment call sites compile to no-ops (no atomic operations in the release read path)

#### Scenario: Every counter has a reset and read API under test/feature builds
- **WHEN** the crate is built with `cfg(test)` or the `work-counters` feature
- **THEN** each of `TRIE_WALKS`, `DECOMPRESS_CALLS`, `SEEK_CALLS`, `FILE_OPENS` exposes a reset and a read accessor
- **AND** an fd high-water-mark helper returns the current open-fd count on macOS/Linux (and `None` on unsupported platforms)

#### Scenario: Each counter documents its consumer
- **WHEN** the read-work-counters module is read
- **THEN** each counter's doc names the epic-child (e.g. B1, C1, C2, C3, C4, E3, E4) that consumes it

### Requirement: The counters are exercised via the public/bench surface (wiring evidence)

The counters SHALL be proven to increment on the real read path — not only through a
local instance round-trip — by a self-test that drives a real public operation (a
cold open and/or a point read through the public API) and asserts the corresponding
counter moved by the expected amount. Counter tests SHALL serialize on the shared
test mutex (per the existing counter-test convention) so a stale value cannot
satisfy a later assertion.

#### Scenario: A known single-chunk point read increments the decompress counter
- **WHEN** a point read that decodes one compression chunk runs through the public query API with counters reset
- **THEN** `DECOMPRESS_CALLS` increases by the expected per-chunk amount for that read

#### Scenario: A cold open increments the file-open counter
- **WHEN** a fresh `Database`/reader open runs with counters reset
- **THEN** `FILE_OPENS` increases (the open touched `open(2)` at the `BlockSource` open sites)

#### Scenario: Counters reset between tests
- **WHEN** `reset` is called before a measured operation
- **THEN** each counter reads zero immediately after reset and reflects only the subsequent operation's work

