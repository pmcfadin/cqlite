# Cassandra Test-Parity Audit — 2026-07-01

Deep audit of the parity program (`docs/cassandra_test_index.md` × `test-data/cassandra-parity-manifest.yml`
× the CQLite test suite and production code) for gaps that leave CQLite open to **data loss or silent
errors**. Five parallel audit lanes: index↔manifest coverage, byte-for-byte claim verification (wiring
evidence), compaction/tombstone deep dive, read-path corruption safety, write-path/WAL + out-of-scope
challenge. Critical findings were independently re-verified in code.

**Remediation backlog:** epics [#1378](https://github.com/pmcfadin/cqlite/issues/1378) (compaction
TTL/tombstone), [#1379](https://github.com/pmcfadin/cqlite/issues/1379) (WAL crash-safety),
[#1380](https://github.com/pmcfadin/cqlite/issues/1380) (read-path integrity),
[#1381](https://github.com/pmcfadin/cqlite/issues/1381) (program hygiene) — children #1382–#1408.

## Confirmed production bugs (verified in code)

1. **#1382 — Compaction merge never applies TTL expiry.** `purge_gc_grace`
   (`write_engine/merge/reconcile.rs:386-467`) purges only tombstones; an expired-TTL live cell is
   re-serialized as live. The covering tests (`ref_expired_ttl_drops_cell`,
   `prop_ttl_expiry_no_expired_live_cells`) drive the test-only `reference_merge` oracle, whose rule 3
   implements exactly the rule production lacks — coverage that masks the defect.
2. **#1390 — WAL `open_existing` appends after a torn tail** (`wal.rs:598`): no scan/trim to the last
   valid entry; replay stops at the torn entry, so post-reopen fsync-acknowledged writes are
   unrecoverable on the next crash.
3. **#1391 — WAL replay is silently lossy** (`wal.rs:719-810`): CRC mismatch → warn+skip (offset advanced
   by the corrupt header's length); oversize length → warn+stop; both return `Ok`. The next flush
   truncates the WAL, making the loss permanent and invisible.

Bugs 2–3 sit under the manifest's `commitlog_replay` **out_of_scope** entries — the parity taxonomy
closed a category whose CQLite-native analogue (the WAL) carried the Cassandra CommitLogTest bug class
untested (re-scoping: #1403).

## Silent-error exposures

- **#1396** — uncompressed Data.db has zero read-time integrity; CRC.db is written (#1197) but never read
  by any consumer, including `verify`.
- **#1400** — two `byte_for_byte` static-row scenarios silently SKIP in all normal CI: their fixture
  (`test_deltas/static_with_rows`) was never committed.
- **#1397** — no corrupt-bytes test drives the plain query surface (`Database.execute`); the real chunk-CRC
  check (`block_io.rs:434`) is only exercised via `verify` internals.
- **#1405** — the real Cassandra-vs-CQLite compaction byte tier runs nightly-only; 2 `byte_for_byte`
  scenarios have no PR-gate wiring.
- **#1406** — CompressionInfo.db write path is built-but-unwired; every compressed-write test is a
  self-round-trip. No Cassandra has ever read a CQLite-compressed SSTable.
- **#1398** — bloom bit-flip → silent false negative on BIG point lookups; no Filter.db corruption fixture.
- #1283 (pre-existing) — `perform_integrity_check`'s `checksum_mismatches` counter is never incremented;
  the `Degraded` branch is unreachable.

## Compaction/tombstone test gaps (beyond #1382)

#1383 RT boundary-marker synthesis across multi-SSTable merges (resurrection class); #1384
partial-compaction zombie e2e (excluded-overlap fixture); #1385 gc_grace strict-`<` ±1s boundary
exactness; #1386 wrapped/negative LDT through the purge path; #1388 fully-expired-SSTable drop
(unimplemented, owner scope call); #1387 the cross-cutting fix — **no tombstone/TTL/purge scenario is
diffed against a Cassandra-compacted reference** (the `issue_1017` byte-oracle family covers live
cells/statics/UDTs only; `issue_819`'s Cassandra tier is env-gated off).

## Program/governance findings

- All 115 🔴 high-relevance index files map to ≥1 manifest scenario (zero orphans); no phantom
  `byte_for_byte` claims; core byte-parity suites are genuine and fail-closed.
- 38/115 🔴 files are weak-only (`partial`/`smoke`); **48 of 52 `partial` scenarios have no open tracking
  issue** — ~30 parked on CLOSED epic #968 (#1401).
- 2 scenarios mis-encoded as `out_of_scope` with null category (manifest :3572/:3612) (#1402);
  `memtable_internals` boundary text is factually false (#1403/#1404).
- ~45/46 🟡 medium files in P0 categories unreferenced, incl. `SizeTieredCompactionStrategyTest` — CQLite
  ships STCS (#1407); index/manifest referential hygiene (#1408).
- The "CI blind to write path" project memory is **partially stale**: e2e-readback (`nodetool refresh` +
  cqlsh) and sstableloader CI lanes exist, plus committed-reference byte parity for
  Data/Index/Summary/Digest/CRC/TOC. Remaining write-direction holes: Statistics.db (semantic-only,
  documented), Filter.db (Cassandra rebuilds bad filters, so readback can't catch them),
  CompressionInfo.db (#1406), and no WAL-recovered-then-flushed readback lane (#1395).

## What's solid

Compressed-read chunk CRC is unconditional and fail-fast with typed errors; the corruption-verify oracle
(#1294) uses real bit-flipped Cassandra fixtures with `sstableverify` verdict parity; bloom is wired into
BIG point lookups with a genuine every-present-key false-negative gate; BTI correctly bypasses bloom;
`issue_1014` resurrection safety uses real Cassandra input fixtures; equal-timestamp
tombstone-beats-expiring (#848) drives the real reconcile path.

## Owner decisions (decided 2026-07-01)

1. **#1396** — uncompressed read-time CRC: **default-on** (verify CRC.db per-chunk on every read;
   design-driven, OpenSpec before implementation).
2. **#1406** — compressed writing: **fail-closed guard now, wire later** (guard test + documented claim
   boundary; wiring is a future design-driven issue).
3. **#1388** — fully-expired-SSTable drop: **implement** (design-driven; sequenced after #1382).
