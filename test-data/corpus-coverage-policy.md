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
need a schema the harness does not map, contain only zero-live-row partitions
validated by dedicated Rust tests, or have `Data.db` binaries that are not
in the published dataset asset). These are NOT silently dropped — they are
discovered, listed, and reported explicitly:

- **enforced** — run through the reader; failures fail the suite.
- **skip-pending** — discovered and listed explicitly as SKIP-PENDING, but
  not executed through the comprehensive row-count corpus. Flip to enforced
  once the listed constraint is lifted.

The skip-pending set + per-keyspace reason is the single source of truth here
and MUST be classified identically across all harnesses
(`smoke-test-all-tables.sh` `SKIP_PENDING_KEYSPACES`,
`bindings/python/tests/corpus.py` `SKIP_PENDING_KEYSPACES`,
`bindings/node/__test__/parity-utils.js` `SKIP_PENDING_KEYSPACES`):

| Keyspace | Skip-pending reason |
|----------|---------------------|
| `test_deltas` | `Data.db` binaries not yet in the published dataset asset (#701); flip to enforced once the `fetch-datasets.sh` pin is bumped. |
| `test_tomb` | Tombstone parity fixtures that legitimately contain partitions with ZERO live rows (e.g. partition-delete-only). The comprehensive corpus's "must emit ≥1 row" check would mis-flag those valid empty results; validated instead by dedicated Rust tombstone/TTL parity tests. |
| `test_types` | CQL-type / schema-evolution parity fixtures that legitimately contain zero-live-row cases (e.g. deleted-counter shadowing). Same "≥1 row" mismatch as `test_tomb`; validated instead by dedicated Rust CQL-type parity tests. |

## How to add a keyspace

1. Commit the new keyspace under `test-data/datasets/sstables/<keyspace>/`
   with its JSONL goldens (and schema if it is to be executed).
2. The dynamic enumeration picks it up automatically as **in-scope**.
3. If it must be excluded, add a row to the skip-set table above AND to the
   skip-set constant in EVERY harness (`smoke-test-all-tables.sh`
   `SKIP_KEYSPACE_NAMES`, `bindings/python/tests/corpus.py SKIP_KEYSPACES`,
   `bindings/node/__test__/parity-utils.js SKIP_KEYSPACES`) with a one-line
   reason. If it is in-scope but cannot be executed yet, add it to the
   skip-pending table above AND to each harness's `SKIP_PENDING_KEYSPACES`
   instead — the skip-pending set MUST be identical across all three harnesses
   and this doc. Do not silently drop it.
