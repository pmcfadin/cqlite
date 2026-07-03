# dhat-alloc-lane Specification

## Purpose
TBD - created by archiving change dhat-alloc-lane. Update Purpose after archive.
## Requirements
### Requirement: A dhat allocation/memory-budget test lane measures the read path against pinned budgets
The project SHALL provide a dhat-based test lane, compiled only under the `dhat-heap` feature, that
opens a real SSTable fixture via the shared bench fixture loader, drives the public query path, and
asserts allocation/peak-byte budgets read from `dhat::HeapStats`. The lane SHALL be additive (test/gate
machinery only) and change no read-path production code. It SHALL install the dhat global allocator only
within its own test binary, so default builds and the default `core-tests` run are unaffected.

#### Scenario: The lane runs only under the dhat-heap feature
- **WHEN** the test suite is built without the `dhat-heap` feature
- **THEN** the memory-budget test target is not compiled and does not run
- **WHEN** the test suite is built with `--features cli-helpers,dhat-heap`
- **THEN** the memory-budget tests compile, install the dhat global allocator in their own binary, and run

#### Scenario: The budget tests drive the real query path over a real fixture
- **WHEN** a budget test runs against the present fixture corpus
- **THEN** it opens the fixture through the shared `benches/fixtures/mod.rs` loader and executes a real `SELECT` via `Database::execute`
- **AND** it reads allocation statistics from `dhat::HeapStats` collected during that query

### Requirement: An allocation-count/bytes budget test pins today's measured read-path allocations
The project SHALL provide `select_full_scan_alloc_budget`, which runs a full-table `SELECT *` over the
largest available real fixture and asserts total bytes allocated during the query do not exceed a
ceiling pinned to today's measured value plus documented slack. The ceiling constant SHALL carry a
comment stating the measured number and the later-epic (E2/E3) target it will be ratcheted toward. The
test SHALL be constructed so that lowering the ceiling below today's measured value makes it fail.

#### Scenario: Allocation total within the pinned ceiling passes
- **WHEN** `select_full_scan_alloc_budget` runs on current `main` against the present fixture
- **THEN** the measured total bytes allocated are at or below the pinned ceiling
- **AND** the test passes

#### Scenario: A tighter-than-measured ceiling fails (ratchet honesty)
- **WHEN** the ceiling constant is set below today's measured total bytes allocated
- **THEN** `select_full_scan_alloc_budget` fails, demonstrating the budget is a real regression net

### Requirement: A peak-bytes ceiling test pins materializing-read peak heap and defends the 128 MiB budget
The project SHALL provide `materialized_select_byte_ceiling`, which runs a materializing `SELECT *` over
the type-heavy real fixture and asserts peak heap bytes (`dhat::HeapStats::max_bytes`) do not exceed a
ceiling pinned to today's measured value plus documented slack, AND do not exceed the project's 128 MiB
memory budget. The ceiling constant SHALL carry a comment stating the measured number and its ratchet
target.

#### Scenario: Peak bytes within the pinned ceiling and under 128 MiB passes
- **WHEN** `materialized_select_byte_ceiling` runs on current `main` against the present fixture
- **THEN** the measured peak heap bytes are at or below both the pinned ceiling and 128 MiB
- **AND** the test passes

#### Scenario: A tighter-than-measured peak ceiling fails (ratchet honesty)
- **WHEN** the peak-bytes ceiling constant is set below today's measured peak
- **THEN** `materialized_select_byte_ceiling` fails

### Requirement: A compile-time layout pin guards `size_of::<Value>()` against growth
The `cqlite-core` crate SHALL contain a compile-time assertion of the form
`const _: () = assert!(std::mem::size_of::<Value>() <= N);` beside the `Value` enum, where `N` is
today's measured `size_of::<Value>()`. The assertion SHALL carry a comment stating the measured value
and Epic E #1517 E1's smaller target. It SHALL fire in every build regardless of feature set, so a
`Value` that grows past the pin fails to compile.

#### Scenario: A Value that fits the pin compiles
- **WHEN** `cqlite-core` is built with the current `Value` layout
- **THEN** the `size_of::<Value>()` const assertion holds and the crate compiles

#### Scenario: A Value that grows past the pin fails to compile
- **WHEN** a change grows `size_of::<Value>()` above the pinned `N`
- **THEN** the crate fails to compile at the const assertion, blocking the regression

### Requirement: The memory-budget lane is wired into the agent gate and fails closed on empty datasets
The `scripts/agent-gate.sh` component set SHALL include a `memory-budget` component that runs the dhat
lane with `--features cli-helpers,dhat-heap` and single-threaded test execution. The component SHALL be
treated as dataset-dependent so the gate's dataset preflight FAILs loudly when no Data.db is present.
When datasets are present but a fixture yields zero rows, the budget tests SHALL fail rather than pass,
so a present-but-empty dataset can never produce a green run.

#### Scenario: The component appears in the gate component set
- **WHEN** `scripts/agent-gate.sh --list` is run
- **THEN** `memory-budget` is listed among the components

#### Scenario: A missing dataset fails the lane loudly
- **WHEN** the `memory-budget` component is selected and no Data.db files are present under the datasets root
- **THEN** the gate's dataset preflight fails loudly rather than skipping the lane

#### Scenario: A present-but-empty fixture fails rather than passes
- **WHEN** the datasets root is present but the target fixture yields zero rows
- **THEN** the budget test fails (it asserts a non-empty result before reading dhat stats), never reporting a green pass

