# decode-benches Specification

## Purpose
TBD - created by archiving change decode-benches. Update Purpose after archive.
## Requirements
### Requirement: Per-CQL-type decode is benched through the live decode entry
The measurement suite SHALL provide a criterion bench that decodes a fixed representative byte buffer for
each CQL type — all scalars plus `list`, `set`, `map`, `tuple`, UDT, and `frozen` — through the **live**
block-path decode entry (`SSTableReader::parse_value_with_schema_type`), reached via an opt-in
`#[doc(hidden)]` bench-only shim, never a re-implemented copy of the dispatch. The bench SHALL NOT bench
dead decode paths (`optimized_complex_types`, `zero_copy_parser`).

UDT wire-decode fidelity boundary: the live string entry resolves types via the registry-free
`ComparatorType::from_data_type`, which maps a UDT reference to `Custom("udt:…")` and therefore can never
yield a `ComparatorType::Udt` arm from a type string (a genuine UDT comparator only arises from the
registry-aware entry, a different code path that is out of scope here). The UDT decode is therefore
benched via its structural twin `tuple` — in Cassandra `UserType extends TupleType`, so the on-wire decode
(i32-BE field lengths, per-field recursion) is identical — and this equivalence SHALL be documented at the
bench.

#### Scenario: Each CQL type is decoded through the real entry
- **WHEN** the `decode/type_<name>` bench runs for a given CQL type
- **THEN** the representative buffer is decoded by calling the crate's `parse_value_with_schema_type` (via the `bench-internals` shim), not a copy
- **AND** the bench setup asserts the decode yields the expected `Value` variant, so a no-op or wrong-path decode fails loudly

#### Scenario: UDT decode is covered via its structural-twin tuple wire format
- **WHEN** the per-type group covers UDT
- **THEN** it benches the UDT wire-decode via the `tuple` arm (identical i32-BE field-length wire format), because the registry-free live string entry cannot produce a `ComparatorType::Udt`
- **AND** the equivalence and its reason are documented at the bench

#### Scenario: The bench-internals shim is opt-in and invisible in default builds
- **WHEN** `cqlite-core` is built without the `bench-internals` feature
- **THEN** no `decode_value_for_bench` symbol is compiled and the public API is unchanged

### Requirement: Wide-row and text-heavy decode throughput are benched
The suite SHALL provide a `decode/wide_row_primitives` bench reporting rows/sec for decoding a wide
all-primitive row (about 20 primitive columns) and a `decode/text_heavy` bench reporting rows/sec for a
block dominated by `text`/`blob` values (the K5/K6 measurement). Both SHALL report criterion throughput in
elements (rows).

#### Scenario: Throughput benches report rows/sec
- **WHEN** the `decode/wide_row_primitives` and `decode/text_heavy` benches run
- **THEN** each configures criterion `Throughput::Elements` over its decoded row count so the reported number is rows/sec

### Requirement: Decode throughput benches are gated for regressions
`cqlite-core/benches/perf-gate.json` SHALL track `decode/wide_row_primitives` and `decode/text_heavy` as
STRICT benches with a median-regression failure threshold of at least 10%, and the perf-regression
workflow SHALL run the `decode` bench on both the PR and the base checkout (guarded so a base checkout
lacking the bench target does not break the run), so a decode-throughput regression fails the lane.

#### Scenario: perf-gate.json tracks the decode throughput benches
- **WHEN** `cqlite-core/benches/perf-gate.json` is read
- **THEN** it contains `decode/wide_row_primitives` and `decode/text_heavy`, each with `threshold_pct >= 10`

#### Scenario: A decode-throughput regression reds the gate
- **WHEN** decode throughput regresses past its threshold versus the base baseline
- **THEN** `scripts/ci/check_perf_regression.py` flags the bench as a REGRESSION and exits non-zero

#### Scenario: First landing with no base data stays green
- **WHEN** the base checkout does not yet contain `cqlite-core/benches/decode_bench.rs`
- **THEN** the workflow omits `--bench decode` on the base run and `check_perf_regression.py` reports the decode benches as SKIP without failing the gate

### Requirement: allocations-per-row and per-cell are measured and gated at current-main values
The dhat allocation lane SHALL measure allocations-per-row and allocations-per-cell for a full-table
`SELECT *` driven through the real public query path (`Database::execute`) over a real wide SSTable
fixture, and SHALL assert each stays within a ceiling pinned at the current-main measured value (a ratchet
that later Epic J/K children lower). The measurement SHALL start after fixture open/ingest so only the
read path is attributed, using `dhat::HeapStats::total_blocks` for the allocation count.

#### Scenario: allocs/row and allocs/cell are computed from real full-scan allocations
- **WHEN** the dhat budget test full-scans the wide fixture through `Database::execute` with the profiler started after `open_read_db`
- **THEN** allocs/row = `total_blocks / row_count` and allocs/cell = `total_blocks / (row_count * column_count)` are computed and each asserted `<=` its pinned current-main ceiling

#### Scenario: The budget lane reds on an allocation regression
- **WHEN** the allocs/row ceiling is set below the measured value (a stand-in for a regression)
- **THEN** the `memory-budget` agent-gate lane fails (non-zero exit), demonstrating the budget catches regressions

### Requirement: Decode measurement never silently passes on an empty or absent dataset
A dataset-dependent decode measurement SHALL fail loudly (panic) when its fixture is present but yields
zero rows, and SHALL be skipped (not registered / SKIP) only when its fixture directory is entirely
absent — never recording a 0-row measurement as a pass.

#### Scenario: Present-but-empty fixture panics
- **WHEN** the wide fixture directory exists but the full scan returns zero rows
- **THEN** the dhat budget test panics with an actionable message rather than dividing by zero or recording a 0-row pass

#### Scenario: Absent optional fixture skips without failing
- **WHEN** an optional wide fixture is entirely absent from the checkout
- **THEN** the measurement is skipped and the lane does not fail on its absence

