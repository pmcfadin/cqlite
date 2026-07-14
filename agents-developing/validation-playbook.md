---
title: sstabledump Validation Playbook
description: JSONL golden files, parity tests, and smoke-test-all-tables — how CQLite validates parsing correctness against Cassandra's reference output.
sidebar:
  label: Validation playbook
  order: 6
---

CQLite validates parsing correctness by comparing output against `sstabledump` — the
Cassandra tool that produces authoritative JSON from SSTable files. Golden JSONL files
are committed alongside the binary SSTables so CI can run parity checks without a live
Cassandra cluster.

## Two oracles: physical-dump parity vs query-semantics parity

There are **two independent parity oracles**, and they answer different questions.
Confusing them hides read-time-reconciliation bugs (issue #1741/#1742):

| | **Physical-dump parity** | **Query-semantics parity** |
|---|---|---|
| Oracle | `*-Data.db.jsonl` (sstabledump `-l`) | `test-data/query-semantics-oracle.json` |
| What it captures | Every cell **physically on disk** — *including* tombstones, deleted rows, and expired-but-uncompacted TTL cells | The **post-reconciliation result set** a real Cassandra `SELECT` returns |
| Question answered | "Did CQLite parse the bytes the same as Cassandra wrote them?" | "Does a CQLite `SELECT` return what a Cassandra `SELECT` returns?" |
| TTL / tombstone rows | Present (enumerated) | Absent (shadowed / expired away) |
| Test lane | the seven `golden_path_*` integration tests, Python/Node parity | `cqlite-core/tests/query_semantics_oracle_parity.rs` (gate component `query-semantics-oracle`) |

**Why both are needed.** A physical dump enumerates shadowed/expired rows, so a
row-count/value comparison against it **structurally cannot catch a read-time
reconciliation bug**: when CQLite fails to apply partition deletions, range
tombstones, or TTL expiry, *both* sides still contain those rows and physical parity
passes green — while a real `SELECT` diverges. `test_basic.ttl_test_table`'s golden
holds all 100 expired rows; CQLite returned all 100; physical parity was green; a real
Cassandra `SELECT` returns 0. That gap is exactly what the query-semantics oracle
closes.

The query-semantics oracle records, per fixture, the canonical `SELECT` and its
expected result set evaluated at a **pinned `now`** (`pinned_now_secs`, epoch seconds)
so TTL expiry is deterministic — never wall-clock-flaky. The reader honors the pin via
the debug-only `CQLITE_TTL_NOW_OVERRIDE_SECS` seam (`now_clock.rs`). Each case carries a
non-empty `expected_rows` and re-asserts the committed golden's physical row count, so a
0-row or unreconciled result fails loudly (anti-empty-pass) and the divergence
(physical count > semantic count) is visible in the test output.

Run it directly:

```bash
env CQLITE_REQUIRE_FIXTURES=1 CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test -p cqlite-core --features "state_machine cli-helpers" \
  --test query_semantics_oracle_parity -- --nocapture
```

`CQLITE_REQUIRE_FIXTURES=1` (which the gate component sets) makes an absent/empty
fixture a **hard failure**; without it, a minimal checkout that lacks the committed
`test_compaction_tombstone_ttl` fixtures SKIPs loudly.

## Golden JSONL files

Every Data.db in the dataset has a companion `.jsonl` file containing
`sstabledump -l` output (one JSON object per line, one line per row):

```
test-data/datasets/sstables/
└── test_basic/
    └── simple_table-<hash>/
        ├── nb-1-big-Data.db         ← binary (not in git; fetch separately)
        └── nb-1-big-Data.db.jsonl   ← sstabledump golden (committed to git)
```

The JSONL files ARE in git. You can run parity tests without fetching binary SSTables,
but you need the binaries to run the parser itself.

## Smoke test: all tables

```bash
# Runs CLI against every table, checks exit codes and row counts
bash test-data/scripts/smoke-test-all-tables.sh
```

The gate runs this against a freshly built debug binary:

```bash
cargo build --package cqlite-cli --bin cqlite
CQLITE_CLI="$PWD/target/debug/cqlite" bash test-data/scripts/smoke-test-all-tables.sh
```

Using a freshly built binary prevents the failure mode where a stale release binary
(from a previous run) passes smoke while current code is broken. This was an actual
failure caught in the first full gate run.

**Expected output**: 33/33 tables PASS; the 3 da/BTI tables are SKIP-PENDING.

## Integration parity tests (Rust)

The seven CI-enforced integration tests compare parsed rows against the JSONL goldens:

```bash
cargo test --package cqlite-integration-tests \
  --test golden_path_scan_operations_tests \
  --test golden_path_get_operations_tests \
  --test golden_path_partition_lookup_tests \
  --test golden_path_summary_index_integration_tests \
  --test chunked_data_reader_direct_test \
  --test comprehensive_component_integration_tests \
  --test fixture_specific_integration_tests
```

These are the same seven targets the gate's `integration-tests` component runs. Run
them individually when a specific area fails.

## Python parity tests

```bash
# All 33 tables: row count + value-level parity
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  pytest bindings/python/tests/test_parity.py -v

# Python vs CLI output equivalence
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  pytest bindings/python/tests/test_cli_parity.py -v
```

`test_parity.py` has three test classes:
- `TestRowCountParity` — row count per keyspace (33/33 must pass)
- `TestValueParity` — cell-level comparison for representative tables
- `TestE2ESummary` — asserts all 33 tables pass (explicit failure if count drops)

**Known xfail**: none as of Dec 2025. Prior xfails (`static_columns_table` #480,
`typed_collections_table` #481) are resolved. Issue #493 (set element tombstones)
is tracked as out-of-scope for v0.9.1.

## Node.js parity tests

```bash
# Requires CQLITE_DATASETS_ROOT
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets npm run test:parity --prefix bindings/node
```

39 parity tests in `bindings/node/__test__/parity.test.js`. Uses JSONL utilities in
`parity-utils.js` for parsing and type normalization.

## Manual parity check workflow

When investigating a single table:

```bash
# 1. Parse with cqlite CLI
cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables/test_basic/simple_table-<hash> \
  --query "SELECT * FROM test_basic.simple_table" \
  --out json > /tmp/cqlite.json

# 2. Reference is the JSONL golden
cat test-data/datasets/sstables/test_basic/simple_table-<hash>/nb-1-big-Data.db.jsonl \
  | jq -s '.' > /tmp/reference.json

# 3. Normalize and diff
jq -S '.' /tmp/cqlite.json > /tmp/cqlite-sorted.json
jq -S '.' /tmp/reference.json > /tmp/ref-sorted.json
diff /tmp/ref-sorted.json /tmp/cqlite-sorted.json
```

Type differences between sstabledump JSON and CQLite JSON are expected and documented:
Python uses native types (v0.13 mapping: `timestamp`→`datetime`, `uuid`→`UUID`,
`blob`→`bytes`, `time`→`int` ns since midnight, `duration`→`cqlite.Duration` — see the
v0.13 Migration Guide, `docs/development/v0.13-migration-guide.md`); CLI uses JSON
strings. Normalization is needed for comparison — see
`bindings/python/tests/test_cli_parity.py` for the normalization logic.

## Adding a new table to parity coverage

1. Add schema to `test-data/schemas/` and `schemas/core.list`
2. Regenerate: `bash test-data/scripts/regenerate-datasets.sh`
3. JSONL golden is generated automatically during regeneration
4. Add table to the relevant parity test file
5. Run gate: `scripts/agent-gate.sh`

## Fail-closed guards vs non-deterministically-regenerated sources

A fail-closed guard must key on something the source actually reproduces. CI regenerates
clean SSTables from a live `cassandra:5.0.2` container on every run, and several
components are **not byte-reproducible** between runs:

- the **BTI (`da`) trie** — `Partitions.db` / `Rows.db`
- **`Statistics.db`** — embeds wall-clock, host, and repair metadata

**Rule (L1):** when a source is regenerated non-deterministically, gate on the
**semantic verdict**, not on a whole-file byte identity. For corruption/verify corpora,
assert that *CQLite detects the corruption the same way Cassandra does on whatever bytes
are present* (the parity TEST) — never bind a captured verdict to the `sha256` of the
whole fixture. Keep the *authoring* check (empty/missing verdict) fail-closed; that part
IS reproducible. Per-component / full-directory binding is the proper future form
(tracked in #1294).

Why this matters: a whole-file-sha guard added in #1236 passed locally every run (the
local gate uses pre-existing, non-regenerated datasets) but tripped in CI on a
*different* regenerated fixture each round (first the BTI trie, then
`statistics_db_header_damage`) — three CI-failure root-cause cycles *after* the local
gate reported PASS. Origin: flow-meta #1310.

## Row count 0 — silent-pass trap

If parity tests pass but show 0 rows, `CQLITE_DATASETS_ROOT` is unset or points to a
directory without binary Data.db files. The tests return empty results (not an error)
when no files are found. This is the failure mode the gate's dataset preflight prevents.

```bash
# Verify data is present
find "$CQLITE_DATASETS_ROOT/sstables" -name "*-Data.db" | wc -l
# Must be > 0 (should be 33+ for the full corpus)
```

## Fuzzing the parser (issue #1614)

The parser decodes untrusted bytes, so a standing **cargo-fuzz / libFuzzer** harness at
`fuzz/` proves it never panics/hangs/OOMs on arbitrary input (every input yields `Ok` or
`Err`). The crate is its own workspace and is **excluded from the main workspace**, so the
stable `scripts/agent-gate.sh` and every default `cargo` build are unaffected — fuzzing
needs **nightly** Rust and is out of the stable gate. Five targets (`fuzz_vint`,
`fuzz_value_decode`, `fuzz_block_emit`, `fuzz_bti`, `fuzz_schema_parse`) reach
`cqlite-core` internals via the feature-gated `#[doc(hidden)] cqlite_core::fuzz_support`
module (`--features fuzz`), which leaves the default public API unchanged. Run one target
with `cd fuzz && cargo +nightly fuzz run fuzz_vint -- -max_total_time=45 -rss_limit_mb=2048
-timeout=25` (or all via `fuzz/smoke.sh`); `.github/workflows/fuzz.yml` runs a bounded PR
smoke lane plus a nightly long-run, uploading any crash reproducer as an artifact. A crash
is a **success** for the net — it is filed as its own bug issue with the reproducer, not
silently patched.

## Field round validation (live-cluster reporting standard, issue #2399)

The oracles above validate correctness locally/in CI against fixtures. A separate,
complementary standard covers the **live 3-node field validation round** run against a
real Cassandra + Flight + Trino deployment (the round tracker channel, e.g. #2367):
`docs/development/round-validation-metrics.md` — a 14-point checklist (A correctness, B
hang/liveness — both pass/fail GATE items; C throughput, D hygiene — tracked numbers),
pre-filled with the round-9 baseline. That round gate is a live-cluster verdict, distinct
from `scripts/agent-gate.sh` above. New round trackers seed from
`.github/ISSUE_TEMPLATE/round-tracker.yml`.
