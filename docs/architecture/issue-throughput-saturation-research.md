# Maximum Read Throughput Through cqlite-flight — Pipeline, Failure Order, and a Saturation Experiment

Owner-requested research (2026-07-09). Read-only static analysis at `main`. No stacks/gates/benches
run. Purpose: a new epic will be filed from these findings. Companion to the epic-AM read-path audit
(`docs/architecture/trino-flight-read-path-audit-2026-07-08.md`), the point-read causation record
(`docs/architecture/issue-2310-ms-point-reads-research.md`), and the core read-path perf audit
(`docs/reports/read-path-performance-audit-2026-07-01.md`).

**Question:** under a high-volume concurrent-read scenario (many readers, many simultaneous queries),
what does it take to get the most bits off disk through cqlite-flight, where does it fail first, and
what does a saturation experiment that finds the stress point look like?

**One-line answer:** disk is not the first thing to saturate. The server fans a *single* `do_get`
out to `M + M·num_cpus` threads (one full multi-threaded Tokio runtime per input SSTable), has **no
global concurrency limit or admission control**, opens a fresh file handle per SSTable per query, and
on the field's index-less snapshots materializes the whole Data.db per query. Thread/scheduler
collapse, fd exhaustion, and memory blow-ups all arrive well before disk bandwidth does.

---

## A. The throughput pipeline and its ceilings

Concurrent-read data path for one `do_get`, with each stage's ceiling and configured limit
(file:line evidence). The server is `cqlite-flight`; the merge/decode engine is `cqlite-core`.

### A0. gRPC / server config — the front door is wide open
- `main.rs:74-77` — `Server::builder().add_service(...).serve_with_shutdown(...)`. **No**
  `.concurrency_limit_per_connection`, **no** `.max_concurrent_streams`, **no** `.tcp_nodelay`, **no**
  gRPC message compression, **no** timeout layer. Tonic 0.12 defaults apply.
- `#[tokio::main]` (`main.rs:34`) = multi-thread runtime, `worker_threads = num_cpus`, blocking pool
  default max **512** threads.
- Confirmed **no `Semaphore` / concurrency limit / rate limiter anywhere in the flight crate** (grep:
  none). **Concurrent `do_get` count is unbounded** — bounded only by what the OS will spawn before
  it falls over.

### A1. Disk read — buffered, per-scan handle, serial within a reader
- Three backends behind one source: `cqlite-core/.../reader/source.rs:42-55` (Buffered `BufReader`,
  Mapped memmap2, Direct `O_DIRECT`). **Default buffered** (`config.rs:203` `use_mmap=false`;
  `disk_access_mode=Auto`, `config.rs:124`).
- **Compaction always reads buffered regardless of `use_mmap`** (`merge/mod.rs:497-514`,
  `config.rs:79-82`) — the flight merge never mmaps; it always does buffered `read_exact`.
- Per-scan file handle (`source.rs:175-208`): each scan does its own `File::open` (issue #815 — a win
  for parallelism, a cost for fd count under concurrency; see B3). Direct-IO read-ahead window default
  **1 MiB** (`config.rs:151,219`); `Auto` prefetch issues **no `madvise`**.
- **Ceiling:** reads are serial within one reader; the only parallelism is independent per-scan
  handles across queries — i.e. throughput scales by spawning *more concurrent work*, which is exactly
  what breaks first (Section B).

### A2. Decompression — serial, single-threaded, 16 KiB chunks
- Algorithms `None/Lz4/Snappy/Deflate/Zstd` (`compression.rs:11-17`), single choke point
  `Compression::decompress` (`compression.rs:249`).
- **No `rayon`/`par_iter` anywhere in cqlite-core** — chunks decompress **one at a time** on the scan
  task (or its single `spawn_blocking` half).
- **Chunk size default = 16 KiB** (`compression_info.rs:70,203`; fixtures assert 16384) — *not* 64 KB.
  Smaller chunks = more decompress calls per MiB.
- **Ceiling:** single-core decompress per reader. CPU-bound; does not use the box's cores for one scan.

### A3. SSTable decode + k-way merge — whole-partition (or whole-Data.db) materialization
- Flight uses the **compaction merge**, not the query engine: `MergeProducer::produce_streaming`
  (`producer.rs:582`) → `KWayMerger::new_cancellable` (`producer.rs:597`) → `drive_merge`.
- **Per input SSTable, one OS producer thread + one full multi-threaded Tokio runtime:**
  `SSTableRowIteratorAdapter::open` (`merge/mod.rs:456`) does `std::thread::spawn`, and the producer
  thread body builds **`tokio::runtime::Runtime::new()`** (`merge/mod.rs:519`) — which is
  `new_multi_thread` with `num_cpus` eagerly-spawned worker threads. (Contrast the token-prune
  runtime, which is correctly `new_current_thread`, `producer.rs:267`.) **This is the dominant
  concurrency ceiling — see B1.**
- Each run streams partitions through a bounded `sync_channel(STREAMING_CHANNEL_CAPACITY = 256)`
  (`merge/mod.rs:422,452`) — up to 256 `MergeEntry` (partitions) buffered per run.
- **`KWayMerger::step()` materializes an entire partition** before the LIMIT/filter egress loop
  (`merge/mod.rs:2264-2302`, filter at `producer.rs:788`) — `LIMIT 1` on a multi-GB wide partition =
  O(partition) peak (audit N5 / #2230).
- **Index-less snapshots materialize the whole Data.db:** when Summary.db is absent the compaction
  scan fully materializes the entire Data.db in one pass (`merge/mod.rs:2159`, `sequential.rs:759`).
  The field snapshot dirs contained only Data.db (#2295) → 414,957 partitions cloned+sorted before a
  byte streams, defeating backpressure (`issue-2310-...md:33-37`).
- **`Value` is 32 bytes now, hard-pinned ≤40** (`types.rs:98`) — the read-path audit's "88 bytes/value"
  is **stale** (fat variants boxed). Rows are still `HashMap<String,Value>` with per-row column-name
  `String` clones (read-path audit E-epic).

### A4. Arrow conversion + batch assembly
- CQL→Arrow per-cell conversion (`cqlite-core::export::arrow_convert`); known per-cell costs are the
  0.14 arrow_convert perf items (#1495/#1496).
- Batch size default **8192 rows** (`main.rs:30-31`), floored to ≥1 (`service.rs:133`). Not a runtime
  knob beyond the process arg.

### A5. The `do_get` egress channel — bounded, cap 4
- `DO_GET_CHANNEL_CAPACITY = 4` batches (`streaming.rs:46`); merge runs on `spawn_blocking`, sends each
  batch via cancellation-aware `reserve()` backpressure (`streaming.rs:130-167`).
- **Peak resident egress payload ≈ (4 + ~3)·8192 rows** per query — genuinely bounded, independent of
  result size. The **unbounded-channel landmine is NOT present here** (audit confirmed).
- Caveat: this bounds the *Arrow egress buffer only*. It does **not** bound A3's per-run
  256-partition buffer or the whole-partition / whole-Data.db materialization upstream of it.

### A6. tonic/gRPC egress + Tokio runtime
- Response stream is `FlightDataEncoderBuilder` over the receiver (`streaming.rs:359-368`). No gRPC
  compression configured — Arrow IPC bytes go out uncompressed.
- **Where blocking fs I/O runs:** `do_get_setup` (schema parse + dir resolve + token-prune, each reads
  a Summary.db) is **one `spawn_blocking`** (`service.rs:488`); the merge is a **second `spawn_blocking`**
  (`streaming.rs:263`); `gather_table_stats` (do_action) is a **third** (`service.rs:559`). Plus the
  A3 per-SSTable producer threads and their runtimes sit *outside* the blocking pool entirely.

### A7. Byte-budget guard — does NOT protect the flight path
- The v0.13 64 MiB result-byte budget (`query/result_budget.rs`, `config.rs:335`) is a **query-engine
  `SelectExecutor` concept**, checked once post-materialization. **The flight producer bypasses the
  query engine** (it drives `KWayMerger` directly), so `do_get` gets **no result-byte budget at all** —
  its only memory bound is the cap-4 egress channel, which does not cover A3's materialization.

### A8. Cross-query effects
- **Per-query snapshot → memtable flush storm (#2305/#2306):** snapshot creation is on the **connector/
  Sidecar** side, not cqlite-flight (the server only *resolves* a snapshot dir name, `service.rs:491`).
  `SnapshotManager` PUTs a snapshot on **every replica host per query** and **each PUT triggers a
  Cassandra memtable flush** (`SnapshotManager.java:80`, `SidecarClient.java:82`); the Sidecar endpoint
  exposes only `?ttl=`, **no `skipFlush`** (`issue-2310-...md:44-47`). Under many concurrent queries
  this is a flush storm on the co-located Cassandra node, stealing IO/CPU from flight.
- **No warm readers (#2310):** every `do_get` re-parses the DDL (`service.rs:138`), re-resolves the
  dir (`producer.rs:491`), and re-opens every reader — fixed per-query cost × N.
- **Memory target vs N scans:** the <128MB target is *per-scan* and already breached by A3 wide/
  index-less cases; there is **no global memory budget across concurrent queries**, so peak RSS ≈
  N × per-scan-materialization.

---

## B. Where it fails first (ranked by order-of-failure as concurrency rises)

| Rank | Failure mode | Mechanism + evidence | Approx. trigger |
|------|--------------|----------------------|-----------------|
| **1** | **Thread / scheduler collapse (runtime amplification)** | One `do_get` spawns **M producer OS threads, each building a full `num_cpus`-worker Tokio runtime** (`merge/mod.rs:456,519`). On a 16-core node a 3-SSTable snapshot query ≈ 3 + 3·16 ≈ **51 threads** before the 2 blocking-pool threads. N concurrent queries ⇒ ~N·51 threads, mostly idle-but-scheduled. Context-switch storm and runqueue latency spike; CPU burns on scheduling, not decompression. | Low tens of concurrent queries on a multi-core box |
| **2** | **No admission control / unbounded `do_get`** | No concurrency limit, no `Semaphore` (`main.rs:74-77`; grep: none). Each query takes 2 blocking-pool threads (setup + merge, `service.rs:488`, `streaming.rs:263`); blocking pool caps at 512 → setups queue silently past ~256 concurrent, on top of Rank 1. No backpressure to shed or reject load. | ~hundreds concurrent, or far fewer combined with Rank 1 |
| **3** | **fd exhaustion** | Per-scan `File::open` per SSTable (`source.rs:186,203`, no reader/fd pool by #815 design) + a Summary.db read per SSTable during prune. N queries × M SSTables open fds simultaneously; container ulimit often 1024. `EMFILE` → query failures. | N·M approaching the fd ulimit |
| **4** | **Unbounded memory growth → OOM** | A3: whole-partition materialization (`merge/mod.rs:2264-2302`, #2230) and **whole-Data.db** materialization on index-less snapshots (`merge/mod.rs:2159`, #2295); the 256-partition per-run buffer; **no result-byte budget on the flight path** (A7). No global cross-query memory cap ⇒ RSS ≈ N × per-scan peak. | Wide partitions or index-less snapshots × modest N |
| **5** | **Steady-state per-query cost caps aggregate bytes/s** | Even absent 1-4: `WHERE pk=X` **full-scans the whole table** through the merge (egress filter, `producer.rs:788`, #2207); full-scan decode path is **not wired to the DecompressedChunkCache** so every query re-reads+re-decompresses (#2165, `chunk_source.rs:1-9`); decompression serial/single-core (A2). Field: point read = **271s** on 2.16M partitions. | Always present; sets the disk-bound ceiling you *want* to reach |

Cross-query amplifiers (not a single resource, but they multiply the above): the **snapshot flush
storm** (#2305, A8) drives Cassandra-side IO/CPU up as concurrency rises; the **per-request
schema-parse/dir-resolve/reader-open** (#2310) adds fixed CPU per query.

**Instrumentation already present** (good): `cqlite_rpc_requests_total`, `_duration_seconds` (with the
per-phase `rpc.phase.duration` split `resolve → merge_setup → stream`, `obs.rs:191-282`),
`_rows_total`, `_bytes_total`, `_in_flight` gauge (`obs.rs:307-352`), `cqlite_errors_total`, and
`query.rows_scanned` deltas (`scan_progress.rs`). **Missing for saturation work:** fd-count gauge,
thread-count gauge, blocking-pool queue depth, RSS/alloc hook, and the egress channel depth.

**Biggest surprise:** the concurrency ceiling is not I/O and not the cap-4 egress channel everyone
reasoned about — it is **thread amplification inside a single query**. `Runtime::new()` per producer
thread (`merge/mod.rs:519`) means "many simultaneous queries" is already "thousands of threads" before
the second client connects. A single well-formed query on a 10-SSTable table on a 32-core node asks the
OS for ~320 worker threads. This is invisible in every existing metric (no thread gauge) and is the
first thing that will fall over.

---

## C. The saturation experiment — a runnable plan to find the stress point

### C0. What must be built first (epic workstream candidates)
- **A flight-direct load client does not exist.** Exhaustive search of `tools/`, `bin/`,
  `cqlite-flight/{examples,benches}`, and all `.py/.rs/.sh`: the only load tool is
  `easy-db-lab-kits/trino-loadtest/driver.py` — a **through-Trino JDBC** driver (thread pool, p50/p95/p99,
  VictoriaMetrics scrape). All its load is confounded by Trino coordinator/worker overhead
  (irreducible ~50-150ms/query), so it cannot isolate the flight server. The #2310 measurement plan
  explicitly calls for "a raw Flight client" that still needs building.
- **Server-side saturation gauges are missing** (fd, thread, blocking-pool queue, RSS, channel depth).

### C1. Instrument
- **Reuse:** the `cqlite_rpc_*` series + `rpc.phase.duration` split (a `stream`-phase histogram that stays
  flat while `merge_setup` climbs = still materializing, per #2295), `rpc.in_flight`, `errors_total`,
  `query.rows_scanned` (flat-at-0 with `in_flight>0` = the field's "stuck in do_get" signature).
- **Add (WS2):** process gauges sampled every ~2s — `/proc/<pid>/task` count (threads),
  `/proc/<pid>/fd` count (fds), RSS (`/proc/<pid>/statm`), and a Tokio blocking-pool queue-depth /
  egress-channel-depth gauge. Optionally a dhat-heap lane for the merge path.
- **External:** `iostat -x` (disk MB/s + `%util`), per-core CPU (`mpstat -P ALL`), and a CPU flame graph
  (Pyroscope on the pod, already wired for Trino) to attribute CPU to decompress vs merge vs
  arrow_convert **vs kernel scheduling** (the Rank-1 tell).

### C2. Load shape
- **Stage 1 — isolate the server (local, single node):** the **#2289 docker harness** (~100k-122k-partition
  `keyvalue` table *with full index components* — so the index-less pathology is excluded and you measure
  the healthy path). Drive `do_get` with the WS1 flight-direct client, N parallel workers, each looping a
  fixed query mix (full scan, `LIMIT 100`, `LIMIT 1000`, `COUNT(*)`, and a `WHERE pk=X` to expose #2207).
- **Stage 2 — real disks (#2103 3-node kit):** `i4i.xlarge` NVMe, one `cqlite-flight` pod per node,
  flight-direct client on the app node bypassing Trino. Re-run the ramp; add an index-*less* snapshot
  variant to force the #2295 materialization path and watch memory.

### C3. Ramp protocol
- Readers 1 → 2 → 4 → 8 → 16 → 32 → … until a saturation or abort criterion trips. Hold each step long
  enough for steady state (≥30s).
- Record per step: delivered rows/s and **bytes/s** (client side); **bytes/s off disk** (`iostat`);
  latency p50/p95/p99/p999; RSS; fd count; **thread count**; blocking-pool + channel depth; `rpc.in_flight`;
  the `rpc.phase.duration` split.

### C4. Saturation criteria — which metric proves which resource
- **Success = disk-bandwidth-bound:** `iostat %util → ~100`, delivered bytes/s tracks disk MB/s, latency
  rises gracefully. That is the target ceiling.
- **Thread/scheduler-bound (expected Rank 1):** thread count explodes (`/proc/<pid>/task`), per-thread CPU
  low, aggregate CPU high but flame graph shows **scheduler/futex**, throughput plateaus with disk `%util`
  well under 100. → runtime amplification.
- **CPU-stage-bound:** a core pegs and the flame graph concentrates in `decompress`/`arrow_convert`/`merge`
  with disk `%util` low → serial decompression (A2) / per-cell conversion (A4).
- **fd-bound:** fd count hits ulimit, `EMFILE` in logs, `errors_total` climbs.
- **Memory-bound:** RSS climbs ~linearly with N and/or partition width; pod restart / OOM.
- **Queue pileup:** `rpc.in_flight` climbs while delivered rows/s is flat and `query.rows_scanned` is
  flat-at-0 → stuck in `merge_setup` materialization (#2295/#2207), not disk.

### C5. Abort criteria
- OOM / pod restart; `rpc.rows_total` flat with `in_flight>0` for > ~60s (hang); sustained `EMFILE`/
  connection-reset error rate.

### C6. Proposed epic workstreams
- **WS1 — flight-direct load client** (`tools/flight-loadgen`): raw `FlightServiceClient` do_get against
  `<node>:8815`, N workers, ramp harness, percentile + throughput stats, optional traceparent. *Blocks the
  whole experiment.*
- **WS2 — saturation instrumentation:** fd/thread/blocking-pool/channel/RSS gauges into `obs.rs`; optional
  dhat lane. Makes Rank 1-4 observable.
- **WS3 — runtime de-amplification (highest server-side leverage):** stop building a multi-thread
  `Runtime::new()` per producer thread (`merge/mod.rs:519`); share one runtime, or use `new_current_thread`,
  or restructure the merge so N inputs do not spawn N runtimes. Directly targets Rank 1.
- **WS4 — admission control / backpressure:** tonic `concurrency_limit` + a `Semaphore` bounding concurrent
  `do_get` merges; bound/observe blocking-pool usage. Targets Rank 2/3/4.
- **WS5 — wire the full-scan path to `DecompressedChunkCache` (#2165) + pipeline/parallelize
  decompression.** Targets Rank 5 / A2.
- **WS6 — the point-read package (#2207/#2295/#2302):** index-probe instead of O(table) scan; complete
  snapshots so the reader streams instead of materializing. Already scoped in `issue-2310-...md`.
- **WS7 — snapshot amortization / skip-flush (#2305/#2306):** kill the cross-query flush storm.
- **WS8 — run the ramp** on #2289 (Stage 1) then #2103 (Stage 2); file each discovered bottleneck as a
  `bug`+`performance` issue per the #2103 Phase-5 triage table.

---

## Evidence index (primary files)
- Flight server: `cqlite-flight/src/{main.rs,service.rs,streaming.rs,producer.rs,obs.rs,scan_progress.rs}`
- Merge engine: `cqlite-core/src/storage/write_engine/merge/mod.rs` (runtime-per-input at `:519`,
  producer thread at `:456`, whole-partition step at `:2264-2302`, index-less materialization note at
  `:2159`, `STREAMING_CHANNEL_CAPACITY` at `:422`)
- Core read path: `cqlite-core/src/storage/sstable/reader/{source.rs,chunk_source.rs}`,
  `.../compression.rs`, `.../compression_info.rs`, `.../types.rs` (`Value` ≤40 pin at `:98`),
  `query/result_budget.rs`, `storage/cache/mod.rs` (DecompressedChunkCache)
- Prior audits: `docs/architecture/trino-flight-read-path-audit-2026-07-08.md`,
  `docs/architecture/issue-2310-ms-point-reads-research.md`,
  `docs/reports/read-path-performance-audit-2026-07-01.md`
- Harnesses: `easy-db-lab-kits/trino-loadtest/` (through-Trino only),
  `easy-db-lab-kits/test-plans/cqlite-flight-loadtest-3node.md`, #2289 docker harness (referenced only)
</content>
</invoke>
