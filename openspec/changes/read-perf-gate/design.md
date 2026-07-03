# Design — read-perf-gate

## Context

Source of truth: `docs/reports/read-path-performance-audit-2026-07-01.md` §Epic A; issue #1562 (child
of Epic A #1513). Guardrail: **no read-path production code changes** — additive bench/gate/test only.

## Decision 1 — which surface is "the real point-read path"

Three candidate surfaces were considered:

| Surface | Verdict |
|---------|---------|
| `SSTableManager::get()` / `StorageEngine::get()` (KV) | **Rejected.** BIG index is Murmur3-digest-keyed; raw-key `find_entry()` always misses → `scan_for_key` reads+decompresses the whole Data.db per lookup (audit #4a). Benching it measures the O(file) fallback, not the point path. |
| `StorageEngine::scan_partition()` direct | Real path, but requires constructing raw partition-key bytes, a `TableId`, and a schema by hand in the bench — not the surface users hit, and more brittle. |
| `Database::execute("SELECT * … WHERE id = ?")` (query engine) | **Chosen.** The actual public read API; since #956 it engages the #949 partition-targeted point path (bloom/BTI prune → single-candidate seek → chunk decode); returns `QueryResult.access_path` for setup-time wiring proof. |

**Chosen: the query-engine `WHERE pk = ?` path**, exactly matching issue step 1's preferred option and
the wiring-evidence doctrine (a feature is done only when its *public surface* exercises it).

## Decision 2 — wiring-evidence (the bench cannot silently measure the wrong path)

`QueryResult.access_path: Option<AccessPath>` is a public field (`cqlite-core/src/query/result.rs:117`).
At each bench's setup the code asserts:
- `res.rows.len() ≥ 1` (never silently measure 0 rows — issue guardrail), and
- `res.access_path.as_ref().is_some_and(AccessPath::is_targeted)` — i.e. `PartitionLookup`, **not**
  `FallbackFullScan{..}`.

If #956/#949 ever regress so the query falls back to a full scan, the bench **panics at setup** and the
bench binary fails — the gate can never quietly degrade into re-measuring a scan.

## Decision 3 — fixtures (BIG multi-chunk + BTI)

- **BIG variant** — `test_basic.simple_table` (`nb`, `id UUID PRIMARY KEY`, 999 rows, ~647 KB Data.db,
  16 KiB uncompressed chunk length → many compression chunks). It is a canonical fetched fixture
  (`fetch-datasets.sh` preflights exactly this table), so it is always present in perf CI.
- **BTI variant** — `test_da.simple_table` (`da`, same `id UUID PRIMARY KEY` schema). BTI drives the
  authoritative trie descent.

Deterministic key selection: full-scan once at setup, take the **first** returned row's `id` (SSTable
scan order is stable across runs/machines — the existing benches rely on the same determinism), format
it as the canonical unquoted 8-4-4-4-12 UUID literal, and bench `WHERE id = <that literal>`.

## Decision 4 — fixture presence policy (loud-on-broken, skip-register-on-absent)

Reconciling "error loudly on an empty dataset, never silently measure 0 rows" (issue guardrail) with
"don't spuriously fail perf CI when an optional dataset isn't fetched":

- If the fixture's table directory is **present** → open it; setup **panics** if rows == 0 or the
  access path is not targeted (loud, as required).
- If the fixture's table directory is **entirely absent** → **do not register** that bench variant. It
  then simply doesn't appear in the Criterion output, and `check_perf_regression.py` reports it as
  `SKIP (no data)` and never fails the gate — matching the repo convention for dataset-optional tests.

The BIG variant is a canonical fixture (always present). The BTI variant (`test_da`) uses skip-register
so a checkout/CI lane without `test_da` doesn't hard-fail; when `test_da` is present it is fully gated.

## Decision 5 — perf-gate.json edit

Remove the `read/point_lookup` entry (AC: the LIMIT-1 proxy is no longer presented as `point_lookup` in
the gate). Add `read/get_partition_big` and `read/get_partition_bti`, `threshold_pct: 10` each. The old
`bench_point_lookup` function is deleted (a misleading proxy left in the source invites re-gating);
`read/clustering_slice`, `read/full_scan`, `read/type_heavy` are untouched.

Before main also carries the new benches, the PR's perf-CI run reports the two new IDs as `SKIP (no data
in base)` — expected and self-healing: `check_perf_regression.py` never fails on a bench missing from
the base baseline. After merge, both PR and main measure them → the gate is live.

## Decision 6 — committed regression-detecting tests + demonstrated red-run

- **Fixture-sanity (committed integration test)**: parse the BIG fixture's `CompressionInfo.db` via
  `cqlite_core::storage::sstable::compression_info::CompressionInfo::parse` and assert
  `chunk_offsets.len() > 1`. Skips (not fails) when the fixture is absent, per repo convention.
- **Access-path wiring (committed integration test)**: run the exact bench query
  (`WHERE id = <first-row uuid>`) against `test_basic.simple_table` and assert
  `access_path == Some(PartitionLookup)` — this is the committed proof that the gated bench drives the
  real point path (breaks if #956/#949 regress).
- **Gate self-test red-run (PR demonstration)**: temporarily force the point query to a full scan (or
  add latency), save a slowed baseline, and show `check_perf_regression.py` flags
  `read/get_partition_big` as `REGRESSION` with a non-zero exit. Paste in the PR; revert before merge.

## Risks

- **Scan-order key stability**: if a future change reorders scan output, the selected key changes but
  remains a real partition key — parity/targeting still hold; only the exact timed partition shifts.
  Acceptable for a median-regression gate.
- **BTI fixture availability in CI**: mitigated by skip-register; the BIG variant always gates.
