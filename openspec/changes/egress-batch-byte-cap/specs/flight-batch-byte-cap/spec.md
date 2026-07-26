# flight-batch-byte-cap

## ADDED Requirements

### Requirement: An Arrow egress batch is finished on whichever of the row-cap or the byte-cap trips first

Every Arrow record batch produced by the `cqlite-flight` egress path SHALL be
finished when **either** the configured row-cap (`batch_size`) **or** a configured
byte-cap is reached, whichever occurs first. The byte-cap MUST be enforced at
**both** batch-build sites — `cqlite-flight/src/producer.rs` (`flush_buffer`,
trip at `:951`) and `cqlite-flight/src/producer_stream.rs` (`flush`, trip at
`:206`) — because a cap wired into only one path leaves the other unbounded.

#### Scenario: wide rows finish batches on the byte-cap before the row-cap

- **GIVEN** the synthetic wide-row fixture and a byte-cap chosen so that fewer
  than `batch_size` of its rows fit under the cap
- **WHEN** the table is scanned end to end
- **THEN** every emitted batch except possibly the last has **strictly fewer than
  `batch_size` rows** — the byte-cap, not the row-cap, cut the batch
- **AND** more than one batch is emitted (the fixture is not degenerate)
- **AND** this assertion FAILS on pre-change `main`, where a single 8192-row batch
  is produced regardless of width

#### Scenario: narrow rows still finish batches on the row-cap

- **GIVEN** a narrow fixture whose full `batch_size`-row batch is well under the
  default cap, and the default `DEFAULT_MAX_BATCH_BYTES`
- **WHEN** at least `2 × batch_size` rows are scanned
- **THEN** every non-final batch has **exactly `batch_size` rows** — the byte-cap
  never trips on the narrow path
- **AND** the batch boundaries are identical to those produced with the byte-cap
  effectively disabled, proving no narrow-path behaviour change

#### Scenario: the producer.rs merge path honours the byte-cap

- **GIVEN** the wide-row fixture routed through the `MergeProducer` full-scan path
  (`producer.rs`)
- **WHEN** it is scanned under a byte-cap smaller than a full row-capped batch
- **THEN** the emitted batches are byte-cut, not row-cut, on that path specifically

#### Scenario: the producer_stream.rs path honours the byte-cap

- **GIVEN** the same wide-row fixture routed through the streaming producer path
  (`producer_stream.rs`)
- **WHEN** it is scanned under the same byte-cap
- **THEN** the emitted batches are byte-cut on that path too, with the same
  boundary rule — neither path is left unbounded

### Requirement: The boundary decision is made before the RecordBatch is materialized

The byte-cap SHALL be evaluated against a running estimate accumulated as rows are
pushed into the buffer, **before** `rows_to_record_batch` allocates the batch. The
implementation MUST NOT build a `RecordBatch`, measure it, and then split or
discard it, and MUST NOT use `RecordBatch::get_array_memory_size()` as the
production trigger.

#### Scenario: no oversized batch is ever allocated

- **GIVEN** a wide-row fixture whose row-capped batch would far exceed the cap
- **WHEN** the scan runs
- **THEN** no `RecordBatch` is constructed whose realized payload exceeds the cap
  by more than the declared tolerance — the oversized batch is never allocated at
  all, rather than allocated and then rejected or re-split

#### Scenario: the running estimate is accumulated incrementally

- **GIVEN** a buffer accumulating rows toward a batch boundary
- **WHEN** each row is pushed
- **THEN** the accumulator is advanced by that row's estimate only, and is reset
  when the buffer is flushed — the whole buffer is not re-measured per push

### Requirement: The per-row byte estimator is conservative and never systematically under-estimates

A public estimator SHALL report, for a row and a projected column set, a byte
width greater than or equal to that row's contribution to the resulting Arrow
batch's payload bytes (the sum of Arrow buffer lengths, recursively including
child data). The estimator MUST account for per-cell structural overhead (the
offsets entry and the validity bit) and, for collection/struct columns,
per-element structural overhead — a content-only sum is insufficient, and a fixed
per-row constant is not acceptable. The estimator MUST be driven by the
authoritative `ColumnInfo` CQL types and the decoded values, never by inference
from byte patterns, and MUST be saturating and recursion-bounded so a pathological
value fails closed instead of panicking or hanging.

The estimator SHALL also be bounded ABOVE: the structural slack it charges per
Arrow slot MUST NOT scale with a cell's element count, or a collection-heavy or
wide-schema row would be over-estimated by a multiple and the byte-cap would cut
batches far short of the configured size. Per-buffer costs SHALL therefore be
charged once per projected COLUMN, and the residual per-slot costs SHALL be
derived from Arrow's buffer layout rather than fitted to the validation corpus.

#### Scenario: the summed estimate is at least the realized batch payload across a shape corpus

- **GIVEN** a corpus of row shapes covering fixed-width columns, `text`, `blob`,
  `list`/`set`, `map`, `tuple`/UDT, JSON, collections nested more than one level
  deep (including an empty or null cell of such a type, whose declared type tree
  still materializes one Arrow array per level), a UDT with an empty declared
  field list, all-null rows, empty strings and empty collections
- **WHEN** each shape's rows are both estimated and converted with
  `rows_to_record_batch`
- **THEN** for every shape the summed estimate is **greater than or equal to** the
  realized batch's payload bytes (buffer lengths summed recursively over child
  data), with no shape under-counted

#### Scenario: variable-width content drives the estimate

- **GIVEN** two rows with identical column sets whose only difference is the
  length of a `blob` cell (say 16 B versus 64 KiB)
- **WHEN** each is estimated
- **THEN** the estimates differ by at least the content-byte difference — the
  estimator is width-sensitive, not a per-row constant

#### Scenario: an unmodelled column type fails the conservatism property test

- **GIVEN** a column type that `rows_to_record_batch` converts but the estimator
  does not model
- **WHEN** the conservatism property test runs over that shape
- **THEN** the test FAILS, so a future type addition cannot silently introduce an
  under-estimate

#### Scenario: the estimate stays within a bounded multiple of the realized payload

- **GIVEN** the collection-heavy and multi-column shapes of the corpus — where a
  slack charged per slot rather than per column would inflate the estimate most
- **WHEN** each shape's summed estimate is compared with its realized payload
- **THEN** the estimate is within a small, explicitly asserted multiple of the
  payload, and a wide fixed-width row still leaves room for a full `batch_size`
  batch under the default cap, so the row-cap remains the binding boundary on
  narrow shapes

#### Scenario: a pathological value fails closed rather than panicking

- **GIVEN** a deeply nested collection value beyond the estimator's node budget,
  or a value whose widths would overflow `usize`
- **WHEN** it is estimated
- **THEN** the estimator returns a saturated (large) width and the batch is cut,
  with no panic, no overflow, and no unbounded recursion

### Requirement: The relationship between the estimate, the payload bytes, and the reported Arrow memory size is stated with a named tolerance

The byte-cap SHALL be normatively denominated in Arrow **payload** bytes. Because
`get_array_memory_size()` reports Arrow buffer **capacity**, which the batch
construction path grows by power-of-two doubling, the implementation SHALL publish
a named capacity conversion constant such that every emitted batch satisfies
`get_array_memory_size() <= capacity_factor * max(cap, widest_row_payload)
+ per_array_node_slack * array_nodes`. The specification MUST NOT assert that an
emitted batch's `get_array_memory_size()` is less than or equal to the cap.

The `max(cap, widest_row_payload)` term SHALL be stated explicitly rather than
folded into slack: one row cannot be split across Arrow batches, so a row wider
than the whole cap is an inherent overshoot of the DATA, not of the mechanism. For
a schema whose widest row fits the cap the bound SHALL reduce to
`capacity_factor * cap + per_array_node_slack * array_nodes`.

#### Scenario: every emitted wide-row batch stays within the declared capacity tolerance

- **GIVEN** the wide-row fixture scanned under a configured cap
- **WHEN** each emitted batch's `get_array_memory_size()` is read
- **THEN** every batch satisfies
  `get_array_memory_size() <= capacity_factor * max(cap, widest_row_payload)
  + per_array_node_slack * array_nodes` using the published constants, with no
  batch exceeding the tolerance

#### Scenario: the payload bound is asserted tightly and separately

- **GIVEN** the same emitted batches, over a fixture whose every row fits the cap
- **WHEN** each batch's payload bytes (buffer lengths, recursive) are summed
- **THEN** every batch has payload bytes at or below the cap outright — the
  boundary cuts BEFORE the row that would cross, so there is no `+ one row`
  allowance — and the bound holds in the cap's own currency, independent of
  allocator capacity

#### Scenario: a crossing row that is a large fraction of the cap does not overshoot it

- **GIVEN** a fixture whose rows are each a large fraction of the configured cap
  (so a boundary that appended the crossing row before testing would emit batches
  well above the cap)
- **WHEN** the table is scanned on both egress paths
- **THEN** every emitted batch's payload bytes are at or below the cap

#### Scenario: the tolerance is expressed as named constants

- **GIVEN** the published capacity factor and per-column slack
- **WHEN** the tests and documentation reference them
- **THEN** they are named constants used by both, not inline literals repeated per
  assertion

### Requirement: Every emitted batch carries at least one row

A batch SHALL never be emitted empty, and a single row whose estimated width
exceeds the entire byte-cap SHALL still be delivered as a one-row batch. The
byte-cap MUST NOT drop a row, MUST NOT stall, and MUST NOT loop without progress
for any cap value, including zero.

#### Scenario: a single row wider than the whole cap is emitted as a one-row batch

- **GIVEN** a fixture containing one row whose estimated width exceeds the entire
  configured cap
- **WHEN** the table is scanned
- **THEN** that row is delivered in a batch of exactly one row, the scan completes,
  and no row is dropped

#### Scenario: a stream of over-cap rows terminates with one row per batch

- **GIVEN** a fixture of N consecutive rows each individually wider than the cap
- **WHEN** the table is scanned
- **THEN** exactly N batches of one row each are emitted, the total row count is N,
  and the scan terminates — no empty batch, no repeated flush without progress

#### Scenario: a zero or one-byte cap degrades to one row per batch rather than hanging

- **GIVEN** the cap configured to `0` and, separately, to `1`
- **WHEN** a multi-row narrow table is scanned
- **THEN** each configuration yields one row per batch, all rows are delivered, and
  the scan terminates

### Requirement: The byte-cap is a real, wired configuration knob reachable from the flight server CLI

The byte-cap SHALL be settable through a `--max-batch-bytes` CLI flag and a
`CQLITE_MAX_BATCH_BYTES` environment variable, backed by a named
`DEFAULT_MAX_BATCH_BYTES` constant of 4 MiB, following the
`--max-concurrent-scans` const + env-const + clap-arg + service-field precedent.
Setting the knob MUST change observed batch boundaries end-to-end through a real
streamed `do_get` — the knob is functional, not decorative. The cap SHALL be
active on every service and producer construction path by default, and its
configured value SHALL appear in the server startup log alongside the other knobs.

#### Scenario: two distinct configured cap values produce correspondingly different batch boundaries

- **GIVEN** a flight server started twice over the wide-row fixture with two
  distinct `--max-batch-bytes` values
- **WHEN** the same ticket is streamed under each
- **THEN** the smaller cap yields strictly more batches with strictly fewer rows
  per batch, demonstrating the configured value — not a hard-coded constant —
  governs the boundary

#### Scenario: the environment variable governs a real streamed do_get

- **GIVEN** `CQLITE_MAX_BATCH_BYTES` set with no CLI flag present
- **WHEN** a `do_get` is streamed against the wide-row fixture through the real
  service surface
- **THEN** the observed batch boundaries match the environment-configured cap —
  wiring evidence from the public surface, not a helper-only unit test

#### Scenario: the default applies with neither flag nor environment variable

- **GIVEN** neither `--max-batch-bytes` nor `CQLITE_MAX_BATCH_BYTES` set
- **WHEN** the server starts and a scan runs
- **THEN** the effective cap is `DEFAULT_MAX_BATCH_BYTES` (4 MiB) and the startup
  log records it

#### Scenario: a library embedder constructing the service directly also gets the cap

- **GIVEN** the service and producer constructed through their plain constructors,
  with no explicit cap supplied
- **WHEN** a wide-row scan runs
- **THEN** the default cap is in force — the byte-cap is not opt-in, because an
  unbounded batch is a memory hazard rather than a policy choice

### Requirement: The byte-cap does not alter result content

Applying the byte-cap SHALL change only where batch boundaries fall. The
concatenation of the emitted batches MUST contain identical rows, in identical
order, with identical values and an identical Arrow schema, compared against a run
at an effectively unbounded cap.

#### Scenario: capped and uncapped runs concatenate to identical results

- **GIVEN** the wide-row fixture and a ticket exercising a projection and a
  predicate
- **WHEN** it is streamed once under a small cap and once under an effectively
  unbounded cap
- **THEN** the concatenated rows, their order, their values and the Arrow schema
  are identical between the two runs — only the batch boundaries differ

#### Scenario: lowering the cap does not change the total row count

- **GIVEN** the same fixture scanned under a descending series of cap values
- **WHEN** the rows of each run are counted
- **THEN** the total row count is identical for every cap value

### Requirement: Byte-cap behaviour is proven on a self-contained synthetic wide-row fixture

The wide-row coverage SHALL use a deterministic, self-contained fixture defined in
the test sources — a wide-blob and/or many-column shape — and MUST NOT depend on
the fetched `test_wide_rows` dataset, so the tests cannot pass vacuously when the
dataset binaries are absent. The fixture MUST produce more than one batch under
the configured cap.

#### Scenario: the wide-row fixture is generated in-process with no fetched dataset

- **GIVEN** a checkout with no `CQLITE_DATASETS_ROOT` and no fetched Data.db
  binaries
- **WHEN** the byte-cap test suite runs
- **THEN** the wide-row fixture is generated in-process and every byte-cap test
  executes with real rows — no skip, no zero-row pass

#### Scenario: the wide-row tests fail rather than pass vacuously

- **GIVEN** the wide-row byte-cap tests
- **WHEN** a scan yields zero rows or a single batch
- **THEN** the test FAILS on the non-vacuity assertion before any byte assertion
  is evaluated

#### Scenario: the wide-row byte-cap tests fail on pre-change main

- **GIVEN** the new tests applied to pre-change `main`
- **WHEN** they run
- **THEN** they FAIL, because no byte-cap exists and batches are row-cut only —
  the tests are real guards, not tautologies

### Requirement: Byte-cap correctness is asserted on measured bytes and row counts, never on wall-clock

No correctness-path test for the byte-cap SHALL assert on elapsed wall-clock time
or a throughput threshold. Any throughput comparison against the pre-change
behaviour SHALL live in an `#[ignore]`d performance test annotated
`perf-gate-allow`. Library code introduced by this change MUST contain no
`unwrap()`/`expect()` and MUST compile clean under `RUSTFLAGS="-D warnings"`.

#### Scenario: correctness assertions reference only byte totals, row counts and batch counts

- **GIVEN** the byte-cap correctness test suite
- **WHEN** its assertions are inspected
- **THEN** every one compares measured byte totals, row counts or batch counts —
  none compares an elapsed duration against a threshold

#### Scenario: the throughput comparison is an ignored perf test marked perf-gate-allow

- **GIVEN** the ~1.0–1.1× throughput expectation from the issue
- **WHEN** it is evidenced
- **THEN** the comparison lives in an `#[ignore]`d test annotated
  `perf-gate-allow`, excluded from the correctness path and from the default gate run

#### Scenario: the change passes the roborev-lints wall-clock guard and clippy with warnings denied

- **GIVEN** the completed change
- **WHEN** the agent gate runs
- **THEN** the `roborev-lints` component passes (no wall-clock threshold in the
  correctness path) and clippy passes with `RUSTFLAGS="-D warnings"`

### Requirement: The per-batch bound is published so the streaming egress budget can compose on it

The change SHALL publish a per-batch bound expressed in **both** payload and
capacity currency, stated in terms of the named constants, so that issue #2821's
per-stream in-flight ceiling can derive its guaranteed bound of
`ceiling + one maximum batch` and demonstrate it fits the ratified B4 budget of
≤16Mi per-query working set at concurrency 1.

#### Scenario: the worst-case per-batch capacity is derivable from the named constants

- **GIVEN** `DEFAULT_MAX_BATCH_BYTES`, the capacity factor and the per-array-node
  slack
- **WHEN** a consumer computes the worst-case resident size of one emitted batch
- **THEN** the value follows from those constants alone, with no undocumented
  fudge factor, and is documented next to the knob

#### Scenario: the published bound names the single-over-cap-row term

- **GIVEN** a schema whose widest single row exceeds the configured cap
- **WHEN** the worst-case per-batch bound is computed
- **THEN** the published function takes that row's payload as an explicit
  parameter and reports `capacity_factor * max(cap, widest_row_payload) + slack`,
  so a dependent budget cannot silently inherit a bound that omits the term

#### Scenario: the B4 arithmetic for the dependent issue is recorded in the metered currency

- **GIVEN** that the egress path meters batch bytes with
  `get_array_memory_size()` (capacity currency)
- **WHEN** the composition with issue #2821's per-stream ceiling is documented
- **THEN** the arithmetic is stated in that same capacity currency and shown to sit
  inside B4's ≤16Mi at concurrency 1, rather than mixing payload and capacity figures

### Requirement: The stale 57,344-row egress figure is corrected in the throughput manifest

The M11 manifest line in `docs/architecture/throughput-program-2026-07.md` SHALL
cite the real production egress residency of approximately 49,152 rows rather than
the stale 57,344-row figure, which over-counts by folding in the
`#[cfg(test)]`-only `IN_FLIGHT_ALLOWANCE` constant. The dated research snapshots
under `docs/research/` SHALL NOT be modified — they are analysis records and the
correction is already recorded in one of them.

#### Scenario: the M11 manifest line cites the production residency figure

- **GIVEN** `docs/architecture/throughput-program-2026-07.md` §7 item M11
- **WHEN** the line is read after this change
- **THEN** it cites approximately 49,152 rows as the production egress residency
  and no longer asserts 57,344

#### Scenario: the dated research snapshots are left untouched

- **GIVEN** the five dated research documents under `docs/research/`
- **WHEN** the change's diff is inspected
- **THEN** none of them appears in it

#### Scenario: the documentation footprint is confined to the manifest line and the new knob

- **GIVEN** the change's documentation diff
- **WHEN** it is inspected
- **THEN** it consists of the single M11 manifest correction plus the operator
  documentation for `--max-batch-bytes`, and nothing else
