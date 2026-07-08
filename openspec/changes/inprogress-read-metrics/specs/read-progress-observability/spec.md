# read-progress-observability

## ADDED Requirements

### Requirement: Streaming do_get emits row/byte progress incrementally, per record batch

The streaming `do_get` egress SHALL attribute `cqlite.rpc.rows` and `cqlite.rpc.bytes` to the metrics
pipeline **as a monotonic counter delta each record batch** as the batch passes toward the client,
rather than as a single aggregate emission at stream end. The per-batch delta SHALL be emitted from the
same batch-accounting point that already tallies rows/bytes (`MeteredDoGetStream`), so a long-running
`do_get` shows `cqlite.rpc.rows` climbing while `cqlite.rpc.in_flight` is still non-zero. The sum of the
per-batch deltas over a fully-drained stream SHALL equal the total the single end-of-stream emission
produced before this change (counter total is unchanged; only cadence changes). Emission SHALL be
per-batch, never per-row.

#### Scenario: The rpc.rows counter moves before the stream completes

- **GIVEN** the `observability-testing` `MetricsCapture` harness and a streaming `do_get` over a
  multi-batch fixture served through a deliberately slow consumer that reads only the first few batches
- **WHEN** the consumer has received at least one but not all batches, and the stream has NOT yet been
  drained to completion
- **THEN** `cqlite.rpc.rows` has a non-zero counter value that is strictly less than the fixture's total
  row count (on `main` it is still zero at this point — the only emission is at stream end)
- **AND** `cqlite.rpc.in_flight` for `do_get` is non-zero at that same observation.

#### Scenario: Per-batch deltas sum to the unchanged total

- **WHEN** the same streaming `do_get` is drained to completion
- **THEN** the final `cqlite.rpc.rows` counter total equals the fixture's total row count
- **AND** the final `cqlite.rpc.bytes` total equals the sum of the emitted batches' payload sizes —
  byte-identical to the pre-change single-emission total.

#### Scenario: Progress emission is per-batch, not per-row, and zero-cost when the feature is off

- **GIVEN** a build with the `observability` feature disabled
- **WHEN** a streaming `do_get` runs over the multi-batch fixture
- **THEN** the per-batch progress path compiles to the existing `obs::add_counter` no-op (links no
  OpenTelemetry) and performs at most one counter emission per record batch — never one per row.

### Requirement: do_get records a bounded per-phase duration breakdown

`do_get` SHALL record the wall time it spends in each of a **bounded, closed** set of execution phases
— `resolve`, `merge_setup`, `stream` — via a `cqlite.rpc.phase.duration` histogram (unit seconds) and a
`tracing` span event emitted at each phase transition. The phase SHALL be carried as the bounded
attribute `cqlite.rpc.phase` whose value is a `&'static str` from a fixed table; the metric SHALL NOT
carry any per-query, per-ticket, key, or query-text attribute. A `do_get` whose time is dominated by
opening input SSTables SHALL record that time under the `merge_setup` phase **before the first record
batch is produced**, so a stall that emits zero rows is still localizable to a phase from metrics alone.

#### Scenario: The merge_setup phase is observable before the first row streams

- **GIVEN** the `capture_spans` / `MetricsCapture` harness and a `do_get` over a fixture whose merge
  setup (opening SSTables + building the k-way merger) completes before any batch is emitted
- **WHEN** the phase transition from `merge_setup` to `stream` occurs
- **THEN** a `cqlite.rpc.phase.duration` sample tagged `cqlite.rpc.phase = "merge_setup"` has been
  recorded (and a corresponding span event exists under the `flight.do_get` span) BEFORE the stream's
  terminal batch — so "26-minute do_get producing zero internal series" becomes "time attributed to
  merge_setup".

#### Scenario: The phase attribute is bounded to the closed enum

- **WHEN** a `do_get` runs to completion and every recorded `cqlite.rpc.phase.duration` sample is
  inspected
- **THEN** every `cqlite.rpc.phase` attribute value is one of exactly `"resolve"`, `"merge_setup"`,
  `"stream"`
- **AND** no sample carries a ticket, table name, key, token range, or query-text attribute.

#### Scenario: Every phase is accounted for across a full do_get

- **WHEN** a `do_get` over a present, non-empty fixture completes
- **THEN** a `cqlite.rpc.phase.duration` sample is recorded for each phase the request actually entered,
  and the phases are entered in the order `resolve` → `merge_setup` → `stream` (a request that skips a
  phase — e.g. an empty result — records no sample for a phase it never entered, never a fabricated
  zero).

### Requirement: Core read-path scan counters emit incrementally during a long scan

The core scan path SHALL emit `cqlite.query.rows_scanned` (and, on the read/merge scan, the
`cqlite.read.rows` / `cqlite.read.partitions` counters) as monotonic counter deltas at a **bounded row
threshold** while a scan is in progress, plus a final flush of the remainder, rather than a single
emission after the scan completes. The threshold SHALL be a named constant aligned to the batch size so
emission is per-batch-scale, never per-row. The total emitted over a completed scan SHALL equal the
pre-change single-shot total, and the access-path / bounded attributes carried SHALL be unchanged.

#### Scenario: rows_scanned is emitted as multiple deltas over a threshold-crossing scan

- **GIVEN** a scan whose examined-row count exceeds the progress threshold by at least one full
  threshold, exercised through the public Flight merge surface (a streaming `do_get` full scan) with a
  feature-independent progress-observation seam analogous to the existing `StreamProbe`
- **WHEN** the scan runs to completion
- **THEN** the progress seam records at least two `cqlite.query.rows_scanned` delta flushes (on `main`
  it records exactly one — the single end-of-scan emission)
- **AND** the summed deltas equal the scan's total examined-row count.

#### Scenario: The incremental total matches the single-shot total and cardinality is unchanged

- **GIVEN** the `MetricsCapture` harness over a completed scan
- **WHEN** `cqlite.query.rows_scanned` is collected
- **THEN** its counter total is identical to the value the pre-change single emission produced for the
  same query
- **AND** it still carries only the bounded `cqlite.query.access_path` attribute — no new or unbounded
  attribute is introduced by making the emission incremental.

### Requirement: In-progress emission preserves bounded cardinality and zero-cost-when-off

All incremental and phase emission introduced by this change SHALL preserve the observability contract:
every attribute value SHALL come from a bounded, code-defined set (never a key, token, ticket, or query
text); no new user-facing library/CLI/binding API and no new config knob SHALL be added (the only new
surface is `catalog` metric-name constants and the one bounded `cqlite.rpc.phase` attribute key); and
every emission site SHALL compile to the existing `cqlite_core::observability` no-op, linking no
OpenTelemetry crates, when the `observability` feature is disabled.

#### Scenario: No new unbounded attribute is introduced

- **WHEN** the full set of metrics produced by a streaming `do_get` and a core scan is collected
- **THEN** every attribute key is one already present in `catalog::attr` or the newly added bounded
  `cqlite.rpc.phase`, and every value is from that key's documented bounded value space.

#### Scenario: The change adds no config knob and no public API

- **WHEN** the change's diff is reviewed
- **THEN** it introduces no new environment variable, CLI flag, ticket field, or public library/binding
  method — only `catalog` metric/attribute constants and their emission wiring.

#### Scenario: Feature-off builds link no OpenTelemetry and run identically

- **GIVEN** a build with `observability` disabled
- **WHEN** the incremental `do_get` and core-scan paths execute
- **THEN** the new emission calls are the same `#[inline]` no-ops as the existing catalog calls and the
  crate links no OpenTelemetry, exactly as before this change.
