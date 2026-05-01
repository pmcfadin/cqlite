# Issue #396 Implementation Plan

## Objective

Implement real end-to-end validation that SSTables written by CQLite can be loaded into Cassandra 5.0 with `sstableloader`, then queried successfully through CQL.

The primary constraint for this work is maximum reuse. The repository already contains most of the required building blocks; the implementation should consolidate and harden them instead of creating a parallel test framework.

## Current State Review

### Reusable assets already in the repository

1. Write-path fixture and schema patterns:
   - `cqlite-core/tests/write_integration.rs`
   - `cqlite-core/tests/sstabledump_parity_data.rs`
   - `cqlite-core/src/storage/write_engine/export.rs`

2. Cassandra/Docker infrastructure:
   - `test-data/docker/docker-compose-cassandra5.yml`
   - `test-data/scripts/start-clean.sh`
   - `test-data/scripts/shutdown-clean.sh`
   - `test-data/scripts/generate.sh`

3. Existing helper abstractions:
   - `tests/helpers/docker.rs`
   - `tests/helpers/cassandra_test.rs`

4. Existing architecture and CI scaffolding:
   - `docs/architecture/sstabledump_parity_test_architecture.md`
   - `.github/workflows/cassandra-validation.yml`

5. Existing issue-396 test target:
   - `cqlite-core/tests/sstableloader_integration.rs`

### What is missing today

The current issue-396 implementation is only partial. The main test target compiles, but it does not yet prove Cassandra acceptance:

1. The tests in `cqlite-core/tests/sstableloader_integration.rs` mostly verify local SSTable creation and print intent comments instead of running `sstableloader` and querying Cassandra.
2. The Docker helper in `tests/helpers/docker.rs` is not wired into the `cqlite-core` test target, so the existing helper is not actually being reused where it matters.
3. The helper logic itself needs hardening before it can be the canonical path for issue 396.

## Code Review Findings

### Finding 1: the current issue-396 tests do not validate the issue objective

`cqlite-core/tests/sstableloader_integration.rs` is presented as real `sstableloader` validation, but the tests stop after `WriteEngine::flush()` and never assert that Cassandra accepted or served the data.

Examples:

- `test_sstableloader_single_partition()` only checks component files and logs that actual `sstableloader` execution is not performed.
- `test_sstableloader_select_verification()` says full verification requires future work.
- The workflow `.github/workflows/cassandra-validation.yml` currently reports these tests as proof of compatibility, which overstates what is actually being tested.

### Finding 2: the current Docker helper cannot reliably copy SSTables

`tests/helpers/docker.rs` uses:

```rust
Command::new("docker").args([
    "cp",
    &format!("{}/*", local_dir.display()),
    &format!("{}:{}", self.container, container_path),
])
```

This does not invoke a shell, so the `*` is not expanded. In practice, `docker cp` receives a literal path ending in `*`, which makes the copy path unreliable or broken.

### Finding 3: the current `run_sstableloader()` path is too synthetic

`tests/helpers/docker.rs` copies files into `/var/lib/cassandra/data/{keyspace}/temp_load` and then runs `sstableloader` against that path. That bypasses the actual exported table directory layout that CQLite already produces and hides path/layout bugs that issue 396 is supposed to catch.

The test harness should load from the real exported directory shape, not a helper-created synthetic destination.

### Finding 4: helper reuse is split across crates instead of centralized

The reusable Docker/Cassandra helper lives under `tests/helpers/`, while the issue-396 tests live under `cqlite-core/tests/`. As a result, the test target that needs the helper most is not using it. This is the opposite of the desired reuse model.

## Implementation Strategy

### Principle

Keep one canonical path for each layer:

1. One canonical Cassandra lifecycle helper.
2. One canonical schema + mutation fixture layer.
3. One canonical execute-load-query assertion flow.

Do not duplicate the same logic in CLI tests, root `tests/`, and `cqlite-core/tests/`.

### Recommended architecture

1. Move or re-home the Docker/Cassandra helper into a location directly consumable by `cqlite-core/tests`.
2. Reuse `WriteEngine` fixture builders from `write_integration.rs` by extracting shared schema/mutation helpers into a small test support module.
3. Rebuild `cqlite-core/tests/sstableloader_integration.rs` around a single reusable scenario runner:
   - create schema in Cassandra
   - write mutations with `WriteEngine`
   - flush or export
   - load with `sstableloader`
   - query via `cqlsh`
   - assert rows, ordering, and expected values
4. Keep the existing tier structure from the issue, but implement it as data-driven scenarios rather than bespoke test bodies.

## Proposed Team

### 1. Harness Lead

Scope:
- Own the canonical Docker/Cassandra helper.
- Remove copy/load path bugs.
- Standardize Cassandra readiness, schema setup, load execution, and teardown.

Quality bar:
- No shell-dependent wildcard behavior inside `Command`.
- Deterministic container selection.
- Clear failure messages with captured stdout/stderr.

### 2. Write Validation Lead

Scope:
- Extract reusable schema and mutation builders from existing write tests.
- Ensure all issue-396 scenarios reuse shared fixture constructors.
- Keep scenario definitions compact and auditable.

Quality bar:
- No duplicated schema literals across tests unless strictly necessary.
- Each scenario must express expected rows explicitly.
- Stress tests must use generated fixtures, not hand-written loops duplicated per test.

### 3. Query Verification Lead

Scope:
- Define post-load CQL assertions for the Tier 2 tests.
- Normalize query output parsing and row comparison.
- Own tombstone, TTL, clustering, and row-count assertions.

Quality bar:
- Assert behavior, not log intent.
- Use table-specific `SELECT` statements that match the issue acceptance criteria.
- Make row comparisons stable and order-aware where required.

### 4. CI and Quality Lead

Scope:
- Align `.github/workflows/cassandra-validation.yml` with the real coverage.
- Ensure the workflow only claims compatibility once `sstableloader` and query assertions are actually executed.
- Add local invocation notes that match the real prerequisites.

Quality bar:
- Workflow names and summaries must match what is truly tested.
- Artifacts must include loader stdout/stderr and query output for failures.
- `cargo clippy -p cqlite-core --all-targets --all-features -- -D warnings` must stay green.

## Work Breakdown

### Phase 1: Consolidate reusable test support

Deliverables:
- Canonical Docker/Cassandra helper accessible from `cqlite-core/tests`
- Shared schema/mutation builders for simple, clustered, and all-types scenarios
- Shared helper for creating isolated temp directories and exported SSTable paths

Exit criteria:
- No duplicate container lifecycle code in issue-396 tests
- No duplicate schema literals beyond scenario-specific differences

### Phase 2: Implement true Tier 1 acceptance tests

Deliverables:
- Real `sstableloader` execution for:
  - simple table
  - clustering table
  - multiple partitions
  - Stage 0 types

Exit criteria:
- Each test creates the Cassandra schema first
- Each test runs `sstableloader`
- Each test fails if `sstableloader` exits non-zero

### Phase 3: Implement Tier 2 query verification

Deliverables:
- Post-load `SELECT *`
- partition-key `WHERE`
- clustering-key `WHERE`
- row-count verification

Exit criteria:
- Assertions are against parsed CQL output, not log messages
- Expected rows are compared to written fixtures

### Phase 4: Implement Tier 3 stress tests

Deliverables:
- large partition scenario
- many partitions scenario
- mixed operations scenario

Exit criteria:
- Stress tests reuse the same scenario runner
- Execution bounds are clear enough for CI

### Phase 5: Truthful CI wiring

Deliverables:
- workflow updated to reflect actual coverage
- artifacts expanded for debugging
- local command documentation aligned with the real flow

Exit criteria:
- CI summary only claims compatibility when the load and query phases ran

## High Code Quality Rules

1. Prefer extraction over copy-paste.
2. Keep one helper per concern; avoid overlapping helper APIs.
3. No placeholder tests that print intent instead of asserting behavior.
4. Every external command must capture and surface stdout/stderr on failure.
5. Use deterministic names for keyspaces/tables per test to avoid cross-test contamination.
6. Make stress-test sizes configurable if CI stability becomes an issue.
7. Run compile, test, and clippy gates before merge.

## Verification Plan

Minimum required checks before merge:

1. `cargo test --package cqlite-core --test sstableloader_integration --features docker-integration,write-support --no-run`
2. `cargo test --package cqlite-core --test write_integration --features write-support`
3. `cargo test --package cqlite-core --test sstableloader_integration --features docker-integration,write-support -- --test-threads=1 --nocapture`
4. `cargo clippy -p cqlite-core --all-targets --all-features -- -D warnings`

If Docker-backed tests are gated locally, the CI workflow must still execute the real loader path on every relevant PR.

## Recommended First PR

Keep the first implementation PR narrow:

1. Consolidate the Docker helper into the right crate/test support location.
2. Fix file copy and load-path handling.
3. Implement one real end-to-end acceptance test for the simple table.
4. Update the CI summary language so it no longer overclaims current coverage.

That PR will de-risk the harness first. After that, the remaining Tier 1, Tier 2, and Tier 3 scenarios can be added quickly through the shared scenario runner.
