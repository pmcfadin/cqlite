# parser-fuzz-safety-net Specification

## Purpose
TBD - created by archiving change parser-fuzz-crate. Update Purpose after archive.
## Requirements
### Requirement: A cargo-fuzz crate exists, isolated from the workspace and gate
The repository SHALL contain a cargo-fuzz / libFuzzer crate at `fuzz/` that is EXCLUDED from the main
Cargo workspace, so that `scripts/agent-gate.sh` and all default `cargo build`/`clippy`/`test`
invocations neither compile nor depend on it. The fuzz crate SHALL reach `cqlite-core` internals only
through a `#[doc(hidden)]`, feature-gated (`--features fuzz`) support surface, leaving the default
public API of `cqlite-core` unchanged.

#### Scenario: The default build is unaffected by the fuzz crate
- **WHEN** `scripts/agent-gate.sh` runs (or any default `cargo build --workspace`)
- **THEN** the `fuzz/` crate is not compiled and is not a workspace member
- **AND** the gate result is unaffected by the fuzz crate's presence

#### Scenario: Internals are reached without widening the default public API
- **WHEN** `cqlite-core` is built without the `fuzz` feature
- **THEN** no `fuzz_support` module or fuzz-only re-export is present in its public API
- **AND** the fuzz targets can only reach the internal parsers when built with `--features fuzz`

### Requirement: Five fuzz targets cover the untrusted-byte decode surface
The fuzz crate SHALL provide five targets under `fuzz/fuzz_targets/`, each accepting arbitrary bytes:
`fuzz_vint` (`parse_vint`/`parse_vuint`/`parse_vint_length`), `fuzz_value_decode` (the schema-typed
value decoder over a fixed type list covering every scalar plus `list<int>`, `set<text>`,
`map<text,int>`, a tuple, and nested `frozen<list<list<int>>>`), `fuzz_block_emit` (the decompressed
partition-loop entry `parse_block_emit` under one fixed simple schema), `fuzz_bti` (BTI node decode +
DFS traversal via the footer-based loader entry), and `fuzz_schema_parse` (arbitrary strings to
`parse_create_table` / the nom `cql_type` / `cql_type_to_type_id`).

#### Scenario: Each target builds and runs on nightly
- **WHEN** `cargo +nightly fuzz run <target> -- -max_total_time=30 -rss_limit_mb=2048 -timeout=25` is
  run for each of the five targets
- **THEN** the target compiles and executes against its corpus without a build error
- **AND** the run exits cleanly (no crash) OR reports a reproducible crash artifact

#### Scenario: The schema-parse target generalizes the #1690 depth guard
- **WHEN** `fuzz_schema_parse` is fed a deeply nested type string (e.g. many nested `frozen<…>`)
- **THEN** the parser returns `Ok` or `Err` (the max-depth guard fires as `Err` beyond the limit)
- **AND** it does not stack-overflow, abort, or hang

### Requirement: Every target enforces the never-panic / never-hang / never-OOM contract
Each fuzz target SHALL treat any input as either successfully decoded (`Ok`) or rejected (`Err`) and
SHALL NOT panic, abort, hang, or exhaust memory on any input. A decode `Err` is a PASS, not a finding.
The target bodies SHALL NOT contain `assert!`/`unwrap()`/`expect()` that could panic on a valid `Err`,
and no `unwrap()`/`expect()` SHALL be introduced into `cqlite-core` library code by this change. The
hang/OOM bounds SHALL be enforced via libFuzzer's `-timeout` and `-rss_limit_mb` flags in every
invocation (local smoke, PR smoke, nightly).

#### Scenario: Arbitrary bytes never crash the parser
- **WHEN** any target is run against arbitrary bytes (corpus-seeded or mutated)
- **THEN** the process does not panic or abort
- **AND** every input results in an `Ok` or an `Err`, never an unhandled failure

#### Scenario: A decode error is not treated as a fuzz finding
- **WHEN** an input causes the parser to return `Err`
- **THEN** the fuzz target continues (the `Err` is ignored)
- **AND** the run is not marked as a crash

### Requirement: Seed corpora are committed from real component data
The fuzz crate SHALL ship a small committed seed corpus under `fuzz/corpus/<target>/`, sourced from
real Cassandra component files under `test-data/datasets/sstables/` (e.g. a Data.db chunk for
`fuzz_block_emit`, a BTI component for `fuzz_bti`) plus representative inputs for the string/byte
targets. Seeds SHALL be small (a few KB per target) and SHALL be committed (force-added if the source
files are gitignored) so the corpus is present in a clean checkout.

#### Scenario: Corpus is present in a clean checkout
- **WHEN** the repository is checked out fresh (no dataset fetch)
- **THEN** each target's `fuzz/corpus/<target>/` directory contains at least one committed seed input
- **AND** the seeds are real-derived, small component bytes / strings

### Requirement: CI runs a bounded PR smoke lane and a nightly long-run, without touching the stable gate
A dedicated fuzz workflow SHALL run every CI-runnable target for a short, bounded time on pull requests
(so a PR can never hang on fuzzing) and for a long budget on a nightly schedule. The workflow SHALL
install nightly Rust + cargo-fuzz and pass `-rss_limit_mb` and `-timeout` on every run; a crash SHALL
fail the job and upload the reproducer as an artifact. This workflow SHALL be separate from
`scripts/agent-gate.sh` and the stable CI so the gate remains on stable Rust and unaffected. A target
whose driver requires a fetched dataset fixture to exercise the real path (e.g. `fuzz_block_emit`, which
needs a real `SSTableReader`) SHALL NOT be run in CI as a silent no-op that reports false-green
coverage; such a target MAY be excluded from the CI matrix (while remaining a standing target runnable
via the local smoke script) until its fixture is wired, with the exclusion documented and tracked.

#### Scenario: PR smoke is bounded and isolated
- **WHEN** a pull request touches the fuzz crate or parser sources
- **THEN** every CI-runnable target runs for a bounded `-max_total_time` with `-rss_limit_mb`/`-timeout` set
- **AND** the stable agent gate / CI jobs run unchanged on stable Rust

#### Scenario: A dataset-gated target does not report false-green CI coverage
- **WHEN** a target's driver would no-op in CI because a required dataset fixture is absent
- **THEN** that target is excluded from the CI matrix (not run as a silent always-green no-op)
- **AND** it remains a standing target runnable locally with datasets, with the exclusion documented and tracked (e.g. re-add after the fixture/blocking fix lands)

#### Scenario: A crash found in CI is surfaced, not swallowed
- **WHEN** a fuzz run (PR smoke or nightly) discovers a crashing input
- **THEN** the job fails
- **AND** the crashing reproducer input is uploaded as a downloadable artifact for triage/filing

### Requirement: Crashes found by fuzzing are filed, not silently patched
When a fuzz run discovers a panic/hang/OOM, it SHALL be filed as its own bug issue with the reproducer
input attached, rather than being silently "fixed" within this change — unless the fix is a single-line
guard obviously in scope. This change delivers the standing safety net; discovering a crash later is a
success tracked as a separate issue.

#### Scenario: A discovered crash becomes a tracked bug
- **WHEN** fuzzing finds a reproducible crash outside this change's scope
- **THEN** a separate bug issue is filed with the reproducer input attached
- **AND** the crash is not silently patched inside the fuzz-crate change

