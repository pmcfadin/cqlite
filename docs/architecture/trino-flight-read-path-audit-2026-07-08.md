# Trino-Flight Read-Path Audit — 2026-07-08

Five-lane parallel audit of the trino-flight read codepath (owner-requested), run at `main` @ `79a77287`.
Lanes: (1) Flight do_get/streaming lifecycle, (2) pushdown end-to-end correctness, (3) type/schema
mapping, (4) splits/stats/snapshot/read-mode, (5) test coverage / wiring evidence. Read-only.

## Verified working (fixes are real)

- **#2157 (LIMIT/predicate enforced at execution): VERIFIED FIXED.** `drive_merge`
  (`cqlite-flight/src/producer.rs:641-714`) enforces token filter, predicate, and LIMIT early-break
  inside the merge loop; shared by streaming (`produce_streaming` → `drive_merge`, producer.rs:545)
  and collect (`merge_paths`, producer.rs:622). `limit==Some(0)` returns without stepping. Tests:
  `limit_below_row_count_stops_early`, `limit_counts_rows_after_filtering`,
  `stream_collect_parity_limit_mid_batch`, `stream_collect_parity_predicate`.
- **#2193: wiring verified, observability-only.** Mid-stream merge errors/panics forwarded as
  terminal channel items (`streaming.rs:148-164`) and mapped to FlightError (`streaming.rs:479-490`);
  encoder-stage errors logged+counted (`streaming.rs:325-347`). No framing defect found: only schema
  delta is benign `cqlite:pushdown` field metadata; no Dictionary types produced (`flat_data_type`,
  producer.rs:931-959). Most likely original symptom = legitimate-but-unlogged error status
  (medium confidence). Round-4 field run is the real verdict.
- **#1473 (cancellation): verified.** CancelGuard armed for merge lifetime (`streaming.rs:206`);
  client disconnect → Drop (`streaming.rs:501-537`) → cancel; `drive_merge` polls before each step.
  Caveat: partition-granular only (see N5).
- **Unbounded-channel landmine: NOT PRESENT.** `DO_GET_CHANNEL_CAPACITY=4` (`streaming.rs:46`),
  `blocking_send` backpressure (`streaming.rs:131-133`). Test `slow_consumer_bounds_produced_batches`.
- **#1336 (RF-correct row count): MERGED and working** (8ee71f7e, archived cb523102) —
  `CqliteFlightMetadata.logicalRowCountStatistics:1138`: ROW_COUNT = Σ live_rows / uniformReplicaCount,
  fails closed to `TableStatistics.empty()` on non-uniform replicas / incomplete stats / errors.
  Residual: live_rows counts pre-compaction overlap once per SSTable → upper bound (upward-safe; no
  wrong broadcast-join hazard).
- **#1477 (LIMIT in ticket): appears resolved in-tree** — `tableHandle.limit()` reaches the ticket
  (`CqliteFlightPageSourceProvider.java:63-77`).
- **#1485/#1487: genuinely closed** — fail-closed arms across arrow_convert builders; `rescale_decimal`
  errors rather than truncates.
- **Pushdown design soundness:** summary path always retained by Trino (advisory); all-or-nothing IN
  encoding; Kleene NULL logic matches both sides (shared truth-table tests); null-allowed domains
  declined; ticket JSON schema round-trips with no drift; uuid EQUALITY-only capability honored;
  half-open `(start,end]` token semantics shared via `token_in_half_open_range`.
- **Snapshot machinery:** per-query memoized single snapshot name across splits; cleanup on query end
  + 6h TTL backstop; fail-closed creation (no silent live fallback); path-safety allowlists both sides.
- **LIVE-mode compaction race:** dir listed once per split; merger dedups by partition key (no dups);
  deleted-file mid-read fails loud; cross-split generation skew is the documented LIVE tradeoff.
- **E2E lane exists:** `.github/workflows/flight-trino-e2e.yml` runs
  `trino-connector/docker/e2e-test.sh` on PRs (Cassandra+Sidecar+flight+Trino, ~17 rows) — bugs
  slipped through it, not past its absence (see "Why CI misses").

## New findings (unfiled at audit time; filed as epic AM)

### Tier 1 — user-visible breakage / silent wrong results
- **N1 (BLOCKER):** one unsupported column poisons the entire table. `getColumnHandles`
  (`CqliteFlightMetadata.java:1054-1056`) / `getTableMetadata` (:1073-1074) call
  `ArrowTypeMapper.toTrino` per field, which throws on decimal/varint/time/list/set/map/tuple/UDT →
  table fully unqueryable (no DESCRIBE, no SELECT of supported columns).
- **N2 (BLOCKER):** CQL `time` has no `ArrowTypeMapper` arm (`ArrowTypeMapper.java:68-89`); Rust emits
  Time64(ns) (`arrow_convert.rs:339`). Ordinary scalar; Trino has native TIME.
- **N3 (HIGH):** SNAPSHOT mode (default) creates snapshot via ONE Sidecar (instance-local
  `CreateSnapshotRequest`; `SnapshotManager.java:67-108`, single `sidecarUri`
  `CqliteFlightConfig.java:30`) but splits fan to ALL replica hosts with the same snapshot name;
  Rust `DirSource::resolve` (producer.rs:151-162) has NO live fallback → multi-node = per-split
  NotFound → whole query fails. Verify easy-db-lab Sidecar topology in round-4.
- **N4 (MEDIUM, silent missing rows):** full-ring range `start==end` — `wraparound = start > end`
  (`CqliteFlightSplitManagerTest`-untested; `CqliteFlightSplitManager.java:113-115`) → non-wrap filter
  `token > T && token <= T` = empty → `SELECT *` silently returns 0 rows.
- **N5 (P2, memory):** `KWayMerger::step()` materializes an ENTIRE partition
  (`cqlite-core/src/storage/write_engine/merge/mod.rs:2264-2302`) before the limit/filter loop
  (producer.rs:666) — `LIMIT 1` on a multi-GB wide partition = O(partition) peak memory; breaches
  <128MB target. Also makes cancellation partition-granular (producer.rs:662-666).
- **N6 (MEDIUM, extra rows):** float NaN in Gt/Gte on the EXPRESSION path (removed from Trino plan):
  server NaN-sorts-greatest (`value_ops.rs:51`) → `d > 1.5` keeps NaN; Trino would drop. Repro:
  `WHERE d > 1.5 OR name='zzz'` with a `d=NaN` row → extra row. Summary path is safe (retained).
- **N7 (LOW-MED, extra rows):** `values_equal` (`value_ops.rs:17`) falls back to f64 equality →
  distinct i64 > 2^53 compare equal. Repro: `WHERE bigcol = 9007199254740993 OR name='zzz'` matches
  bigcol=9007199254740992.

### Tier 2 — latent landmines / hardening
- **N8:** Java ignores Arrow temporal unit — any Timestamp → TIMESTAMP_TZ_MILLIS
  (`ArrowTypeMapper.java:85`), reads raw longs as ms (`ArrowToTrino.java:80-82`). Correct only because
  Rust pins ms everywhere; a unit change = silently wrong 1000x.
- **N9:** scalar Utf8/Binary i32 offset overflow unguarded — #1486 `checked_offset` covers only
  List/Map; batches bounded by row count (8192) not bytes → 8192 rows × ≥262KB text overflows
  (`arrow_convert.rs:598,616,746,1485,1503`).
- **N10:** no planning-time ring-coverage validation — Sidecar range overlap/gap mid-topology-transition
  → silent duplicate/missing rows (`CqliteFlightSplitManager.java:97-117`).
- **N11:** missing projected column → silent all-null column (`ArrowToTrino.java:61` null vector →
  appendNull) — schema drift masked.
- **N12:** timestamp advertised FULL pushdown capability (producer.rs:~904) but
  `PredicateTreeTranslator.constantValue` (:340-392) can't encode TimestampWithTimeZone/Date/Decimal/
  Time/Varbinary → dead capability; drift risk if an encoder lands without matching server epoch/tz
  semantics.
- **N13:** LIMIT + partially-pushed predicate soundness rests entirely on Trino inserting a FilterNode
  above the scan (`applyLimit` residual-unaware, `CqliteFlightMetadata.java:272`) — no bug found,
  unproven end-to-end.
- **P3 odds:** per-SSTable tokio-runtime+Platform rebuild in token prune
  (`producer.rs:257-283`); split pinned to single replica, no failover
  (`CqliteFlightSplit.java:33-36`); duration→VARCHAR silently accepted (doc in #2182);
  nullability not propagated to Trino metadata (cosmetic); nested-builder `filter_map` drops None
  elements — currently unreachable, documented footgun (`arrow_convert.rs:977-983`).

## Type-coverage summary
Correct: boolean, tinyint/smallint/int/bigint/counter, float/double, text/ascii, blob, timestamp(ms),
date, uuid/timeuuid, inet. ERROR-whole-table (via N1): time, decimal, varint, list/set/map, tuple,
UDT. Lossy/undocumented: duration→VARCHAR.

## Why CI misses what the field finds
The Docker E2E asserts result COUNTS on ~17 rows, single node, single split. Every field bug
(#2157/#2164/#2193) was bounding/pushdown/error-propagation that keeps results CORRECT (Trino
re-applies residuals + its own LIMIT). Holes: no LIMIT query in e2e-test.sh; ArrowToTrino tested only
on hand-built vectors (never server-emitted bytes); live read-mode server resolution field-only; the
loopback transport tests assert only count>0 with a default ticket. #1475's headline gap partially
closed by #2193's `do_get_transport_test.rs` — re-scope to richer tickets.

## Filed as
Epic AM (trino-flight read-path audit) + children — see the epic for the live list.
