# read-path-forcing

## ADDED Requirements

### Requirement: Explicit read-path forcing surface

CQLite SHALL expose an explicit `CQLITE_READ_PATH` environment variable and an equivalent
`QueryConfig` field that force the `SELECT` access-path decision to one of `auto`, `point`, or `full`,
resolved once per process (config over env over the `auto` default) and read via a `OnceLock` so an
unset knob adds no per-query cost beyond a single relaxed load. An unrecognized value SHALL be a loud
error, never a silent fall-through to `auto`.

#### Scenario: Unset knob is byte-for-byte auto
- **GIVEN** neither `CQLITE_READ_PATH` nor the `QueryConfig` forcing field is set
- **WHEN** any `SELECT` executes
- **THEN** the access-path decision is exactly today's `classify_partition_lookup` result
- **AND** no forced marker is recorded in `AccessPath`.

#### Scenario: Env selects a forced mode
- **WHEN** `CQLITE_READ_PATH=point` (case-insensitive) is set
- **THEN** every `SELECT` in the process runs under the forced `point` policy.

#### Scenario: Config overrides env
- **GIVEN** `CQLITE_READ_PATH=full` is set in the environment
- **AND** `QueryConfig` explicitly sets the forcing field to `point`
- **WHEN** a `SELECT` executes
- **THEN** the `point` policy is applied (config takes precedence over env).

#### Scenario: Invalid value fails loudly
- **WHEN** `CQLITE_READ_PATH=compact` (or any value outside `auto|point|full`) is set
- **THEN** resolving the mode returns a distinct error naming the invalid value and the allowed set
- **AND** no query silently runs under `auto`.

### Requirement: point mode fails closed on any non-targeted execution

Under forced `point` mode CQLite SHALL fail the query with a distinct
`Error::ForcedReadPathUnavailable` naming the concrete fallback reason whenever the executor would not
run a genuinely partition-targeted lookup — a classification `Fallback`, an unwired targeted surface
(such as a metadata `IN` fan-out), or a build/path that does not actually prune (`engaged == false`).
It SHALL NEVER silently execute a full scan under `point`.

#### Scenario: Partial partition key under point fails closed
- **GIVEN** `CQLITE_READ_PATH=point`
- **WHEN** a `SELECT` binds only a proper subset of the partition-key columns
- **THEN** the query returns `Error::ForcedReadPathUnavailable` naming
  `partition_key_not_fully_constrained`
- **AND** no rows are returned from a silent full scan.

#### Scenario: Classifiable point query takes the targeted path
- **GIVEN** `CQLITE_READ_PATH=point`
- **WHEN** a `SELECT` binds every partition-key column with `=`
- **THEN** the query executes via the partition-targeted lookup
- **AND** `AccessPath::last()` reports a targeted path, not a fallback.

#### Scenario: Execution-time degradation under point fails closed
- **GIVEN** `CQLITE_READ_PATH=point` on a build where the classified targeted surface does not prune
  (`engaged == false`)
- **WHEN** the fully-constrained `SELECT` executes
- **THEN** the query returns `Error::ForcedReadPathUnavailable` naming the no-prune reason rather than
  reporting a fake targeted success or silently full-scanning.

### Requirement: full mode forces the full-scan path with an honest forced marker

Under forced `full` mode CQLite SHALL execute every eligible `SELECT` via the full-scan +
reconciliation path regardless of classification, recording `AccessPath::FallbackFullScan` with a
distinct `FallbackReason::ForcedFullScan` so the forced fallback is never mistaken for an organic one,
and the returned rows SHALL be identical to the `auto` result for that query.

#### Scenario: Targeted-eligible query forced to full
- **GIVEN** `CQLITE_READ_PATH=full`
- **WHEN** a `SELECT` that `auto` would serve via a partition-targeted lookup executes
- **THEN** `AccessPath::last()` reports `fallback_full_scan` with the `forced_full_scan` reason
- **AND** the result set (rows, values, order) equals the `auto` result for the same query.

### Requirement: A single forcing gate wraps the classifier at every call site

The forcing decision SHALL be applied through one shared wrapper over
`classify_partition_lookup`'s outcome at every executor call site (materializing, metadata WRITETIME/
TTL, schemaless, streaming, and streaming-aggregation) — no call site SHALL re-implement the forcing
policy — and the forced choice SHALL be recorded in `AccessPath` identically across surfaces.

#### Scenario: Forcing is uniform across streaming and materializing surfaces
- **GIVEN** `CQLITE_READ_PATH=full`
- **WHEN** the same targeted-eligible query is run through the materializing surface and the streaming
  surface
- **THEN** both record `AccessPath::FallbackFullScan` with `forced_full_scan`
- **AND** neither surface reports a targeted path.

### Requirement: Point-vs-full differential-equality lane

CQLite SHALL provide a differential test lane that runs the point-read-eligible corpus query matrix
under forced `point` and forced `full` and asserts identical rows, values, and order between the two
paths. The lane SHALL be a query-semantics-class oracle: TTL expiry SHALL be evaluated at a pinned
`now` (never wall-clock), it SHALL fail closed (`CQLITE_REQUIRE_FIXTURES=1`) when the committed corpus
is present, and it SHALL demonstrably fail if either path is broken.

#### Scenario: Differential corpus run is equal for every eligible query
- **GIVEN** the fetched corpus fixtures (single-key `=`, `IN`, clustering-pushdown, WRITETIME/TTL,
  multi-generation, tombstone, and TTL tables) and a pinned `now`
- **WHEN** each eligible query runs under `CQLITE_READ_PATH=point` and under `CQLITE_READ_PATH=full`
- **THEN** the normalized result sets (rows, values, order) are equal for every table and query.

#### Scenario: Lane catches a seeded divergence
- **GIVEN** one read path is temporarily altered to return a different or reordered row set for a
  queried partition
- **WHEN** the differential lane runs
- **THEN** the lane fails and names the diverging query and the row-level difference.

#### Scenario: Absent corpus skips loudly, fails closed under the gate
- **GIVEN** the committed corpus fixtures are absent
- **WHEN** the differential lane runs without `CQLITE_REQUIRE_FIXTURES=1`
- **THEN** it skips loudly rather than passing vacuously
- **AND** with `CQLITE_REQUIRE_FIXTURES=1` an absent/empty fixture is a hard failure.

### Requirement: Forcing changes routing only, never decoding semantics

The forcing knob SHALL change only which access path serves a query; it SHALL NOT change value
decoding, tombstone/timestamp reconciliation, or WRITETIME/TTL semantics, and the routing decision
SHALL be taken only from explicit operator config — never inferred from data-byte patterns
(no-heuristics mandate).

#### Scenario: Values are byte-identical across modes
- **GIVEN** a query whose result is non-empty under `auto`
- **WHEN** it is run under `auto`, `full`, and (when eligible) `point`
- **THEN** every returned value is byte-identical across the modes that return rows
- **AND** the mode is chosen from config/env alone, with no byte-pattern inference.

### Requirement: The knob is documented as a test/debug surface

CQLite user/CLI documentation SHALL describe `CQLITE_READ_PATH` as a test and debugging control and
SHALL state explicitly that it is not a performance recommendation, including the fail-closed behavior
of `point`.

#### Scenario: Docs state intent and fail-closed behavior
- **WHEN** a reader consults the CLI/user docs for `CQLITE_READ_PATH`
- **THEN** the docs list the `auto|point|full` values, mark the knob test/debug-only, and describe that
  `point` errors (never silently full-scans) on an unclassifiable query.
