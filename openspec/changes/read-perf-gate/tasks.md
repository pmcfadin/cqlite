# Tasks — read-perf-gate

## 1. TDD tests first (must regress-detect on current main)
- [ ] 1.1 Committed integration test: parse the BIG fixture (`test_basic.simple_table`)
      `CompressionInfo.db` via `CompressionInfo::parse` and assert `chunk_offsets.len() > 1`
      (skip-not-fail when the fixture is absent).
- [ ] 1.2 Committed integration test: run `SELECT * FROM test_basic.simple_table WHERE id = <first-row uuid>`
      through `Database::execute` and assert `access_path == Some(AccessPath::PartitionLookup)` and
      `rows.len() >= 1` (skip-not-fail when absent). This proves the gated bench drives the real path.

## 2. Bench implementation (additive; no read-path production changes)
- [ ] 2.1 In `cqlite-core/benches/read.rs`, add bench group functions `read/get_partition_big` and
      `read/get_partition_bti`: full-scan once at setup to pick the first row's `id`, format as the
      canonical unquoted UUID literal, then bench `Database::execute("SELECT * … WHERE id = <lit>")`.
- [ ] 2.2 Setup asserts `res.rows.len() >= 1` and `res.access_path` is targeted (`PartitionLookup`);
      panic loudly otherwise.
- [ ] 2.3 Add a BTI `ReadFixture` for `test_da.simple_table` (schema `basic-types.cql`) in
      `benches/fixtures/mod.rs`; skip-register the BTI bench when its table dir is absent.
- [ ] 2.4 Remove the old `bench_point_lookup` function and its `criterion_group!` target.

## 3. Gate config
- [ ] 3.1 In `cqlite-core/benches/perf-gate.json`: remove `read/point_lookup`; add
      `read/get_partition_big` and `read/get_partition_bti` (each `threshold_pct: 10`).
- [ ] 3.2 Update `cqlite-core/benches/README.md` (and read.rs module doc) to describe the real
      point-read benches and drop the stale `#548` "LIMIT-1 proxy" rationale.

## 4. Validation
- [ ] 4.1 Run the two new committed tests green (with `CQLITE_DATASETS_ROOT` set).
- [ ] 4.2 Local baseline + demonstrate the gate reds on a slowed point path (red-run for the PR).
- [ ] 4.3 `scripts/agent-gate.sh` PASS — paste the AGENT-GATE SUMMARY block in the PR.
- [ ] 4.4 `RUSTFLAGS="-D warnings"` clean; no `unwrap()`/`expect()` in library code.
