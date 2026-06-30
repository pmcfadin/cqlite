# Test Corpus Coverage Policy (Issue #1229)

This is the **single source of truth** for which committed keyspaces under
`test-data/datasets/sstables/` MUST be covered by the comprehensive
"all tables" parity/smoke suites, and which are intentionally excluded
(and why).

## Why this exists

The corpus is discovered by **walking the committed directory structure**
(`test-data/datasets/sstables/<keyspace>/<table>-<uuid>/`), NOT by a
hand-typed allowlist and NOT by `Data.db` presence (worktrees and clean
checkouts lack the gitignored `*-Data.db` binaries). Enumeration is based
on the committed JSONL goldens / schema + the directory layout.

Historically the count `33` was duplicated in 5+ places, none derived from
disk, and several "coverage" assertions were tautologies (a literal
asserted against itself). Eight committed keyspaces were covered by zero
comprehensive test while everything reported "100%". Dynamic enumeration
plus this explicit skip-set fixes that: a newly-committed keyspace is
**automatically in scope** unless it is added here with a documented reason.

## The rule

Every keyspace directory present in `test-data/datasets/sstables/` is
**in-scope for the comprehensive read-parity corpus** UNLESS it appears in
the skip-set below. The enumeration code fails loudly if a discovered
keyspace is neither covered nor listed here.

## Skip-set (intentionally excluded keyspaces)

| Keyspace | Category | Reason for exclusion |
|----------|----------|----------------------|
| `system` | system | Cassandra-internal metadata SSTables; no user-facing CQLite schema; not a read-parity target. |
| `system_auth` | system | Cassandra-internal auth metadata; not a read-parity target. |
| `system_schema` | system | Cassandra-internal schema catalog; not a read-parity target. |
| `test_writeparity` | parity-fixture | Write byte-parity fixtures validated by dedicated Rust parity tests (`cqlite-core/tests/issue_*_parity.rs`), not the comprehensive read-parity corpus. |
| `test_compactionparity` | parity-fixture | STCS compaction byte-parity fixtures validated by the differential-compaction harness, not the read-parity corpus. |
| `test_compactionparityudt` | parity-fixture | Compaction-parity UDT fixtures (compaction harness only; may be local-only). |

`system*` are matched by the `system`/`system_auth`/`system_schema` names;
`*parity` keyspaces are matched by the explicit names above.

## In-scope read-parity corpus (everything else)

As of Issue #1229 the dynamic enumeration discovers these keyspaces as
in-scope (each has one or more committed table directories with JSONL
goldens):

`test_basic`, `test_collections`, `test_timeseries`, `test_wide_rows`,
`test_oa`, `test_da`, `test_deltas`, `test_big`, `test_comp`, `test_tomb`,
`test_types`.

### Run-mode tiers within the in-scope corpus

Not every in-scope keyspace can be *executed* in every harness yet (some
need a schema the harness does not map, or `Data.db` binaries that are not
in the published dataset asset). These are NOT silently dropped — they are
discovered, listed, and reported explicitly:

- **enforced** — run through the reader; failures fail the suite.
- **skip-pending** — discovered and listed explicitly as SKIP-PENDING, but
  not executed (binaries not yet in the published dataset asset, e.g.
  `test_deltas` per #701). Flip to enforced once the dataset pin is bumped.

The smoke script (`smoke-test-all-tables.sh`) and the Python parity suite
(`bindings/python/tests/test_parity.py`) both load this policy from the
shared helpers so the skip-set lives in exactly one place per language.

## How to add a keyspace

1. Commit the new keyspace under `test-data/datasets/sstables/<keyspace>/`
   with its JSONL goldens (and schema if it is to be executed).
2. The dynamic enumeration picks it up automatically as **in-scope**.
3. If it must be excluded, add a row to the skip-set table above AND to the
   skip-set constant in the harness (`smoke-test-all-tables.sh`
   `SKIP_KEYSPACES` and `bindings/python/tests/corpus.py SKIP_KEYSPACES`)
   with a one-line reason. Do not silently drop it.
