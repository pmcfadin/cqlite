# Millisecond point reads through cqlite-flight — cost-model gap analysis (issue #2310)

Read-only static analysis at `main` @ `84b61a29` (2026-07-09). Feeds epic #2310 (warm-handle
service). No stacks/gates/benches were run; all timings are cited from existing field/harness
artifacts. Cwd for every command: `/Users/patrickmcfadin/local_projects/cqlite`.

## Headline

**A `WHERE pk = X` query through cqlite-flight does NOT do a point read today. It full-scans every
partition of the table through the k-way compaction merge and applies the PK predicate as a per-row
`filter.keeps()` egress filter.** The pushed-down predicate reaches the server in the ticket
(#2164/#2166) but is used only to drop non-matching rows on the wire — it wins 1 row of egress while
performing O(table) I/O + decode. This single term dominates point-read latency by **3–4 orders of
magnitude** over every other cost in the path (snapshot PUT, per-request parse, gRPC/Arrow framing).

Consequence for the epic that commissioned this research: **#2310 (warm generation-keyed readers) is
a Phase-2 optimization, not the lever for ms point reads.** It caches the parse work of a scan that
should not be happening. The ordered levers are: **#2207 first (turn the scan into an index probe),
#2295 + #2302 alongside (make the index actually resolvable/present so the probe has something to
probe), then #2310 (stop re-parsing the index every request), then #2306 (stop the per-query
snapshot PUT + memtable flush).**

## Measured baselines (existing artifacts, not re-run)

| Env | Table shape | Query | Latency | Source |
|---|---|---|---|---|
| 3-node AWS kit (`i4i.xlarge`, nb-big/LZ4) | `keyvalue`, **2.16M partitions/node**, 3 SSTables | `WHERE key='<pk>'` point read | **271s** (full scan, killed) | #2157 round-3 disproof |
| same | same | `SELECT * … LIMIT 5` | **190–433s** (one completed ~189s) | #2157 round-3 |
| same | same | `SELECT count(*)` | 358s+ | #2157 round-3 |
| local docker harness (arm64, all index components present) | ~100k-partition `keyvalue` | `LIMIT 5` | **3s** | #2289 harness run |

The field's 100%-in-`do_get`, `rpc_rows_total` flat-at-0-for-210s signature (#2157 round-3) is the
compound of two stacked amplifiers: (a) #2207 — the scan is O(table), and (b) #2295 — the field's
Sidecar snapshot dir contained **only Data.db** (no Index/Summary/Filter), so each SSTable's
compaction stream *fully materializes* the whole Data.db (414,957 partitions cloned+sorted) before a
byte streams, instead of streaming with backpressure.

## End-to-end point-read path, traced in code

### Trino connector side (per query, on the critical path, synchronous before splits)
1. `CqliteFlightSplitManager.getSplits` (`.../CqliteFlightSplitManager.java:43`) →
   `sidecar.tokenRangeReplicas(keyspace)` — **1 Sidecar HTTP round-trip** (topology).
2. `SnapshotManager.snapshotFor` (`SnapshotManager.java:80`) → `createOnHost` → `SidecarClient.createSnapshot`
   PUT `/api/v1/keyspaces/{ks}/tables/{tbl}/snapshots/{snap}` (`SidecarClient.java:82`) on **every
   replica host** the splits will read. Each PUT is **an HTTP round-trip PLUS a Cassandra memtable
   flush** of the queried table (#2305/#2306 — the Sidecar create-snapshot endpoint has no
   `skipFlush`; only `?ttl=` is exposed). Fail-closed on any primary host.
3. `applyLimit`/predicate reach the ticket via `FlightTicketJson`; one split per token range, each
   pinned to a replica.

### Flight server side (`do_get`, per request, NO warm state)
4. `FlightServiceImpl::do_get` → `do_get_inner` → `do_get_setup` (`service.rs:479`), all inside
   `spawn_blocking`:
   - `FlightTicket::from_bytes` + `build_producer` → `parse_schema` → `parse_cql_schema` — **schema
     parsed from ticket every request** (`service.rs:138`).
   - `DirSource::resolve` — filesystem `is_dir`/`read_dir` of the snapshot (or live) table dir
     **every request** (`producer.rs:146`).
   - `resolve_paths_cancellable` → `data_paths` lists the dir; `prune_paths_cancellable` reads each
     SSTable's `Summary.db` for token-span prune (`producer.rs:618`) — token/split prune only, **no
     PK prune**.
5. `produce_streaming` (`producer.rs:582`) → `KWayMerger::new_cancellable(paths, …)` over **all**
   surviving SSTables → `drive_merge` (`producer.rs:719`).
6. `KWayMerger` opens each SSTable via `SSTableRowIteratorAdapter::open` →
   `stream_all_partitions_for_compaction` (`compaction.rs:547`) — a **full sequential Data.db walk**
   per SSTable. For an index-less input it "fully materialises the whole Data.db in one pass"
   (`merge/mod.rs:2159`).
7. `drive_merge` loop (`producer.rs:719-812`): per partition, per row build the full row, then
   `filter.keeps(&row)` (`producer.rs:788`) — **the PK equality is applied HERE, as a per-row
   filter over every partition in the table.** Token filter drops partitions outside the split
   range only. LIMIT counted post-filter.
8. Rows buffered to `batch_size` (8192), Arrow-encoded, `blocking_send` down the cap-4 do_get
   channel → gRPC.

**The machinery to do a real point read exists but is NOT wired to this path:** per-SSTable
`lookup_partition_with_index` / `lookup_partition_via_bti_trie` / `might_contain_partition` (bloom
presence oracle) in `partition_lookup.rs:25/136/416`, and `point_lookup_rows` /
`execute_point_lookup` in the core query executor (`executor.rs:303/322`). The flight producer never
calls any of them — it only knows the compaction-merge scan.

## Cost model

| Step | Today's cost class | Evidence (file:line) | What removes it | Expected residual after fix |
|---|---|---|---|---|
| Trino plan + split scheduling + gRPC/Arrow fixed | fixed per-query (~tens–~100ms) | Trino coordinator; `CqliteFlightSplitManager.java:43` | inherent to going through Trino | **floors through-Trino at ~50–150ms** |
| Sidecar `tokenRangeReplicas` | per-query network RT | `CqliteFlightSplitManager.java:50` | cache topology (out of scope; minor) | ~ms |
| Sidecar snapshot PUT **+ memtable flush** per replica host | per-query network RT **+ flush latency + cluster churn** | `SnapshotManager.java:80`, `SidecarClient.java:82` | **#2306** (snapshot reuse/TTL, or nodetool `--skip-flush`, or upstream skipFlush) | ~ms (reused snapshot); flush churn eliminated |
| Schema parse from ticket | per-request parse | `service.rs:138` | **#2310** (cache per table) | ~0 |
| `DirSource::resolve` + dir listing | per-request fs stat/readdir | `producer.rs:146`, `service.rs:491` | **#2310** (generation-keyed warm set) | ~0 (staleness probe only) |
| Per-SSTable reader open + Statistics/Summary/Index/bloom parse | **per-request fs+parse × #SSTables** | `merge/mod.rs:528`, `#1599` | **#2310** (parse-once-per-generation) | ~0 (warm hit) |
| **Full-table merge scan + per-row PK filter** | **per-row decode × O(table)** — DOMINANT | `producer.rs:719-812`, `compaction.rs:547` | **#2207** (PK-equality → index point-read + presence-oracle SSTable prune) | index probe (O(log n)) + ~3 partition reads + reconcile ≈ **single-digit ms** |
| Index-less snapshot forces full Data.db materialize | per-request O(Data.db) materialize | `merge/mod.rs:2159`, `#2295`, `sequential.rs:759` | **#2295** (complete snapshots) + **#2302** (resolve present pairs) | streamed/point-addressable |
| Wide-partition full materialize in `step()` | O(largest partition) peak mem | `merge/mod.rs:2264`, `producer.rs:666`, `#2230` | **#2230** (bounded intra-partition) | not on narrow point-read path; matters for wide-partition point reads |

## Phased answer — ordered, minimal set to reach ms-class point reads

The ms target applies to **flight-direct `do_get` latency** (a Flight client hitting `<node>:8815`).
Through-Trino latency carries an irreducible Trino-coordinator floor (planning + split scheduling +
page-source plumbing) of roughly **50–150ms** regardless of how fast the engine is — say so when
reporting: "ms point reads" = flight-direct; through-Trino target is "low-double-digit to ~100ms".

- **Phase 0 (today):** point read = **190–433s** field (full scan of 2.16M partitions). Flight-direct
  same order.
- **Phase 1 — #2207 (PK point-read/prune) + #2295 (complete snapshots) + #2302 (resolve present
  pairs).** These are one package: #2207 turns the scan into an index probe, but the probe only
  exists if the snapshot actually ships Index/Summary/Filter (#2295) and those components resolve for
  the write source (#2302 covers CQLite-written pairs; the field's nb-big are Cassandra-written and
  fine once #2295 stops stripping them). Presence-oracle (bloom) prune drops SSTables that
  definitely lack the key. Expected flight-direct floor: **single-digit to low-double-digit ms** —
  dominated by per-request reader-open + Index/Summary/Statistics parse across the surviving
  SSTables. Reconciliation across all candidate SSTables is preserved (LWW/tombstone-correct — the
  reason #2207 is design-driven, not a patch).
- **Phase 2 — #2310 (warm generation-keyed readers).** Removes per-request schema parse, dir
  resolve, and reader-open/index/summary/bloom parse (now the Phase-1 residual). Cache key =
  generation identity (inode-stable across snapshot dirs), refreshed on the Seam-1-chosen trigger,
  fail-closed. Expected flight-direct floor: **low single-digit ms** — just the index probe from
  warm state + a page-cache partition read + Arrow-encode of a few rows.
- **Phase 3 — #2306 (snapshot amortization) + #2230 (wide-partition bound).** #2306 removes the
  per-query Sidecar PUT + memtable flush from the through-Trino critical path (and the cluster
  churn) — this is a *latency* lever for through-Trino, not only an operational one. #2230 bounds
  the wide-partition case so a point read into a multi-GB partition stays ms and memory-bounded.
  Expected through-Trino floor: **Trino-coordinator-bound (~50–150ms)**; flight-direct unchanged at
  low single-digit ms.

**What dominates the remaining floor after all phases:** flight-direct → the index probe + one
partition read + Arrow encode (low single-digit ms, page-cache-bound). Through-Trino → the Trino
coordinator's own query lifecycle (~50–150ms), which no engine change can remove. If the product
needs true single-digit-ms point reads, they must go **flight-direct (do_get), bypassing Trino** —
Trino is for analytical/federated queries, not an OLTP point-read front door.

## Measurement plan (#2289 harness)

The #2289 local docker harness already records LIMIT-5 timings (3s on ~100k partitions with full
index components) and snapshot-dir listings every run. Per phase:
- **Baseline / after each phase:** run the harness `WHERE key='<pk>'` point read and `LIMIT 5` on
  the ≥100k-partition `keyvalue` table; record `cqlite_rpc_duration_seconds{do_get}` and
  `rpc_rows_total` (must show a series — flat-at-0 = still materializing). Distinguish flight-direct
  (drive `do_get` with a raw Flight client) from through-Trino (Trino query wall time).
- **#2207 proof (already an AC on the issue):** CountingStepper-style probe asserting partitions
  examined ≈ candidate-SSTable point lookups, NOT the table's partition count.
- **#2295 proof:** the harness's snapshot-listing artifact must show Index/Summary/Filter present;
  the partition-lookup strategy probe must show the index path (not `sequential_scan`).
- **#2310 proof:** hit/miss/evict + refresh counters; second identical query on unchanged
  generation shows ~0 parse cost (WS4/WS5 bench evidence on the same harness + #1494 bench suite).
- **#2306 proof:** measured flush/SSTable-creation rate under a query-heavy default-mode workload
  (the harness can host it) before/after.
- The field 3-node kit (#2103) remains the adjudicator for multi-node placement/failover; single
  node cannot adjudicate #2227/#2241.

## Biggest surprise

The epic that requested this research — **#2310, warm handles — is not the lever for ms point
reads, and by itself cannot deliver them.** It caches the parse cost of a full-table merge scan that
should never run for `WHERE pk = X`. The flight producer has **no point-read code path at all**; it
unconditionally builds a `KWayMerger` over every SSTable and applies the pushed-down PK equality as a
per-row `filter.keeps()` (`producer.rs:788`) — so the pushdown that looks "done" on the connector
side (predicate visible in EXPLAIN) buys only 1 row of egress while doing O(2.16M-partition) I/O and
decode. #2207 must land first; #2310 only becomes the dominant residual *after* the scan has been
converted into an index probe. This reorders the epic's own implied phasing.
