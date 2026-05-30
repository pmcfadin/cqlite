# cqlite-core benchmarks

Single-threaded [Criterion](https://github.com/bheisler/criterion.rs)
micro-benchmarks for the `cqlite-core` public API. This is **Phase 1** of the
performance plan (Epic #541): the in-repo regression wall. It answers one
question — "did this PR get slower?" — against small, fixed, deterministic
fixtures. Headline numbers, volume tiers, concurrency sweeps, and codec sweeps
are Phase 2 and live in the external `cqlite-perf` harness, not here.

## Running

```bash
# Full measurement run (writes HTML to target/criterion/)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo bench -p cqlite-core

# Single bench
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo bench -p cqlite-core --bench partition_lookup

# Compile + smoke run only (one iteration per bench — used in CI gating)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo bench -p cqlite-core --bench partition_lookup -- --test
```

Benches read real Cassandra 5.0 SSTables from `test-data/datasets`. If the
binaries are missing, fetch them first:

```bash
bash test-data/scripts/fetch-datasets.sh
```

`CQLITE_DATASETS_ROOT` is honored by every bench. When unset, benches fall back
to the workspace-relative `test-data/datasets` path (derived from
`CARGO_MANIFEST_DIR`), so a checkout with fetched datasets runs with no
environment setup.

## Benches

| Bench | Status | What it measures |
|-------|--------|------------------|
| `partition_lookup` | kept | Index.db partition-key lookup (`IndexReader::lookup_partition`) — cold/warm cache, throughput, access-pattern distribution. The latency-sensitive read path. |
| `m1_performance` | kept | M1 baseline targets: partition-lookup latency plus multi-SSTable read throughput (MB/s). |

## Audit (Issue #536)

The three benches wired here dated to Aug 2025 (pre-format-maturity). Issue #536
revived them against current code and decided keep/fix/delete per bench:

- **`partition_lookup` — kept (fixed).** Compiles and runs against the current
  public API. Fixed a stale hard-coded fallback dataset path
  (`/Users/patrick/...`) to a portable `CARGO_MANIFEST_DIR`-relative path.
- **`m1_performance` — kept.** Compiles and runs unmodified; provides the
  partition-lookup and read-throughput baselines the new read suite (Issue #538)
  builds on.
- **`component_flattening` — deleted.** It benchmarked `Vec::with_capacity` vs
  `Vec::new` from the Rust standard library and never called any `cqlite-core`
  code, so it could not catch a regression in CQLite. Removed here and from
  `Cargo.toml`; the optimization it once illustrated (Issue #209) is already in
  the codebase and covered by unit tests.
