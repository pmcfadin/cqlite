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

Every **committed** keyspace directory under `test-data/datasets/sstables/` is
**in-scope for the comprehensive read-parity corpus** UNLESS it appears in
the skip-set below. The enumeration code fails loudly if a committed keyspace
is neither covered nor listed here.

### The classification/enforcement set is the COMMITTED corpus (Issue #1319)

Classification and enforcement are scoped to the **committed corpus** — a
keyspace counts only if git tracks at least one `*-Data.db.jsonl` golden under
`test-data/datasets/sstables/<keyspace>/`. They are **NOT** derived from raw
live-disk enumeration of `CQLITE_DATASETS_ROOT`.

Rationale: the dataset asset (and a concurrent session's WIP) can drop a
keyspace onto disk whose JSONL goldens are **not yet git-tracked** (e.g.
`test_signed_coll`). Enumerating raw live disk would flag such an untracked WIP
keyspace as "unclassified" and red the integrity guard on every PR, even though
it is not a coverage gap in the committed corpus. So:

- A keyspace present on disk but with **no git-tracked file at all** is
  **IGNORED** — neither enforced nor flagged as unclassified.
- A genuinely-**committed** keyspace (has at least one git-tracked file under a
  table dir) that is unclassified **still reds** the guard — the integrity check
  is not neutered.

"Committed" is the presence of **any** git-tracked file under
`<keyspace>/<table-dir>/` (Data.db, TOC, Statistics, a JSONL golden, ...) — it
is deliberately **decoupled** from "has a JSONL golden" (#1312). A committed
table dir that ships SSTable metadata but is **missing** its golden must still
count as committed so its absent golden is surfaced **loudly** by the separate
golden-presence / coverage check (the #1229 guarantee), not silently dropped
as "uncommitted".

Tracked-ness is computed via a single `git ls-files -z` (no pathspec — **any**
tracked file), parsed into the set of `keyspace/table-dir` (and first-level
keyspace dir names), in each harness
(`committed_keyspaces`/`_git_tracked_keyspaces` in `corpus.py`,
`committedKeyspaces`/`gitTrackedKeyspaces` in `parity-utils.js`,
`compute_committed_keyspaces`/`is_committed_keyspace` in
`smoke-test-all-tables.sh`). The query is rooted at **this source tree's**
`test-data/datasets/sstables` (the repo that owns the harness + this policy),
**NOT** at the live `CQLITE_DATASETS_ROOT`: the datasets root can be a *different*
checkout whose index already contains a concurrent session's WIP fixtures that
this branch has not adopted, so measuring tracked-ness there would mis-classify
that WIP as committed. The guard must reflect what **this branch** considers
committed. Disk *enumeration* still uses the live datasets root; only the
git-tracked *filter* is rooted at the source tree. If `git` is unavailable /
this is not a work tree (no `.git`), every harness **falls back** to treating all
discovered keyspaces as committed so the guard is not silently disabled. In CI
and local dev `.git` is always present.

## Skip-set (intentionally excluded keyspaces)

| Keyspace | Category | Reason for exclusion |
|----------|----------|----------------------|
| `system*` (all) | system | **All `system*` keyspaces are excluded — Cassandra-internal metadata, not user-data parity targets.** Matched by PREFIX (`system`, `system_auth`, `system_schema`, `system_distributed`, `system_traces`, `system_views`, and any future `system*`), so a dataset subset that ships additional `system*` keyspaces auto-excludes them. |
| `test_writeparity` | parity-fixture | Write byte-parity fixtures validated by dedicated Rust parity tests (`cqlite-core/tests/issue_*_parity.rs`), not the comprehensive read-parity corpus. |
| `test_compactionparity` | parity-fixture | STCS compaction byte-parity fixtures validated by the differential-compaction harness, not the read-parity corpus. |
| `test_compactionparityudt` | parity-fixture | Compaction-parity UDT fixtures (compaction harness only; may be local-only). |
| `test_signed_coll` | parity-fixture | signed set/map element-order byte-parity fixtures (dedicated Rust parity test issue_1295_*). |
| `test_compaction_tombstone_ttl` | parity-fixture | tombstone/TTL compaction byte-parity fixtures validated by dedicated Rust parity test `issue_1387_tombstone_ttl_compaction_byte_parity.rs`, not the read-parity corpus. |
| `test_comparator_order` | parity-fixture | `inet`/`time` multicell-collection ORDERING fixture (issue #3790): `SET<INET>`, `SET<TIME>`, `MAP<INET,TEXT>`, `MAP<TIME,TEXT>` and `SET<FROZEN<TUPLE<INET,TIME>>>`, validated by the dedicated Rust ordering test `issue_3790_*`. Excluded for two reasons: a row-count smoke pass over it proves nothing about element ORDER, and the ordering it pins is the very property that was wrong — enrolling a known-divergent fixture as enforced would red every lane's gate for a defect the fixture documents rather than regresses. See the README beside the fixture. |

`test_comparator_order` (#3790) is the newest member of that table; like the
`*parity` keyspaces it is matched by exact name in each harness's skip constant.

`system*` keyspaces are matched by the `system` **prefix** in every harness
(`is_system_keyspace` in `smoke-test-all-tables.sh` and `corpus.py`,
`isSystemKeyspace` in `parity-utils.js`); the `*parity` keyspaces are matched by
the explicit names above. Because `system*` is a prefix rule, it is NOT
enumerated in the per-harness `SKIP_KEYSPACE*` constants — only the
`test_*parity` exact names are.

## Dataset subsets and `Data.db` presence (skip-on-absence)

The CI dataset asset ships a **subset** of the full local corpus: every
keyspace's committed TOC/schema/JSONL files are present, but the gitignored
`*-Data.db` binaries for some fixtures are not. Discovery and classification are
based on the committed directory structure (so the corpus is identical
everywhere), but **enforcement is gated on `Data.db` presence**:

- An enforced table whose `Data.db` is **absent** in this environment's dataset
  is **SKIPPED** (reported explicitly as "Skipped (no Data.db)"), not failed.
  This keeps the smoke suite robust to any dataset subset
  (cf. the `local-only-fixtures-skip-on-presence` pattern, e.g.
  `test_da/wide_table`, `test_big.wide_partition` — enforced locally where the
  `Data.db` is present, skipped in CI where it is absent).
- An enforced table whose `Data.db` **is present** but yields **0 rows** remains
  a **FAILURE** — parity is truth; only ABSENCE of `Data.db` triggers a skip.

## In-scope read-parity corpus (everything else)

As of Issue #1229 the dynamic enumeration discovers these keyspaces as
in-scope (each has one or more committed table directories with JSONL
goldens):

`test_basic`, `test_collections`, `test_timeseries`, `test_wide_rows`,
`test_oa`, `test_da`, `test_deltas`, `test_big`, `test_comp`, `test_tomb`,
`test_types`, `test_nested_udt_keys`.

`test_nested_udt_keys` (issue #3500) is **in-scope and ENFORCED — not a skip and
not skip-pending.** It is a first-class type-fidelity fixture: a UDT reached
through a tuple or through a nested collection inside a *hashable position* (a
set element or a map key), which is the shape the Python/Node bindings must
reduce to a hashable object. Every one of its partitions has live rows, so the
"must emit >=1 row" rule applies unchanged and nothing about it needs the
zero-live-row exemption `test_tomb`/`test_types` carry. It therefore appears in
the `IN_SCOPE_KEYSPACES` map of `bindings/python/tests/corpus.py` and
`bindings/node/__test__/parity-utils.js`, and in **no** `SKIP_KEYSPACE*` /
`SKIP_PENDING_KEYSPACES` set anywhere — including
`smoke-test-all-tables.sh`, which enumerates in-scope keyspaces from the
committed directory structure and mirrors only the SKIP sets.

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
