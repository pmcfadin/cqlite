## Why

The read-path performance gate's "point lookup" number is fake. `cqlite-core/benches/read.rs`
defines `read/point_lookup` as a `SELECT * … LIMIT 1` **scan** and gates it in
`cqlite-core/benches/perf-gate.json`. A `LIMIT 1` scan opens the table and reads the first row in
SSTable scan order — it never exercises the real point-read path (bloom/BTI presence prune →
single-candidate seek to the partition offset → single-chunk decode). The July 2026 read-path audit
(`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic A, problem #5) calls this out: a
regression anywhere on the *actual* point path (the C1 O(file) `scan_for_key` fallback, the C2 cursor
convoy, a broken bloom/BTI prune) merges silently because the gate measures a different code path.

This is Epic A (#1513) "measurement first": no optimization claim downstream is trustworthy until the
gate watches the real path. Design-driven, measurement-harness work — Seam-1 pre-approved for the batch.

Audit facts that constrain the design:
- The read module doc's `#548` note ("UUID partition-key equality doesn't resolve to a RowKey") is
  **stale**: issue #956 added the unquoted-UUID literal, so `SELECT * … WHERE id = <uuid>` on a
  UUID-PK table now engages the #949 partition-targeted path end-to-end (proven by
  `cqlite-core/tests/issue_956_uuid_literal_partition_lookup_parity.rs`).
- `StorageEngine::get()` / `SSTableManager::get()` is **not** the real point path — the BIG index is
  keyed by Murmur3 digests, so `find_entry()` on raw keys always misses → `scan_for_key` reads +
  decompresses the entire Data.db per lookup (audit problem #4a). Benching `get()` would measure the
  O(file) fallback, not the point read.
- The correct, production point-read surface is the query engine's `WHERE pk = ?` path, which returns
  `QueryResult.access_path` (a **public** field, epic #951) so the bench can *prove at setup* the real
  targeted path engaged and did not silently fall back to a full scan.

## What Changes

- **Replace the `read/point_lookup` LIMIT-1 scan proxy with a real point-read bench group**
  `read/get_partition`, driving the public `Database::execute("SELECT * … WHERE id = <uuid-literal>")`
  path — the actual query API — which engages the #949/#956 partition-targeted point-read path.
- **Two format variants, both gated**: `read/get_partition_big` over a BIG (`nb`) UUID-PK fixture that
  spans **multiple compression chunks** (so an accidental whole-file decompress fallback is visibly
  slower than a correct single-chunk seek), and `read/get_partition_bti` over a BTI (`da`) UUID-PK
  fixture (authoritative trie descent).
- **Wiring-evidence at bench setup**: each variant asserts `QueryResult.access_path.is_targeted()`
  (`PartitionLookup`), not a `FallbackFullScan`, and asserts `rows.len() ≥ 1`. An accidental full-scan
  fallback fails setup loudly and cannot masquerade as a point read.
- **Update `perf-gate.json`**: remove the `read/point_lookup` entry; add `read/get_partition_big` and
  `read/get_partition_bti`, each with a ≥10% median-regression failure threshold.
- **Committed fixture-sanity test**: assert (via `CompressionInfo::parse`) that the BIG fixture spans
  `> 1` compression chunk, so the gate's "multi-chunk" guarantee cannot silently erode.
- **Demonstrated red-run** (in the PR): artificially slow the point path and show the gate entry FAILs,
  proving the gate now gates the real path.

## Non-goals

- **No change to any read-path production code.** This is additive bench/gate/test code only (issue
  guardrail).
- **No redefinition of the perf-gate mechanism** (`scripts/ci/check_perf_regression.py`,
  `.github/workflows/perf-regression.yml`) beyond the tracked-bench list in `perf-gate.json`.
- **Not** replacing the `read/clustering_slice` bounded-read bench (a distinct real path; out of scope).
- **No tail-latency / concurrent-scan / memory-budget gating** (later Epic A children).
