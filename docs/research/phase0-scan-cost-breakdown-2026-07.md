# Phase 0 — Ground-truth CPU cost breakdown of a sustained single-stream Flight `do_get` full scan

**Date:** 2026-07-21 · **Status:** research (uncommitted) · **Author:** Phase-0 profiling agent

This is the anchor measurement for the throughput research program. Every downstream
multiplier claim should trace back to the numbers here. The headline is deliberately
uncomfortable: **for a single-stream full scan, most of the CPU is NOT in SSTable parsing,
merge compute, Arrow encoding, or gRPC. It is in the merge's per-row cross-thread channel
handoff (kernel park/wake syscalls) and the allocator.** Read the CAVEATS section before
quoting any figure — the local rig cannot see several field costs, and it exaggerates one.

---

## 1. Method

**Primary method: sampling CPU profile of the real Flight server under sustained
single-stream load** (method 1 from the brief). No derivation/arithmetic fallback was
needed.

- **Server:** the real `cqlite-flight` Arrow Flight server binary (`target/release/cqlite-flight`),
  driving the real `FlightService::do_get` warm-reader row path
  (`produce_streaming_from_readers` → `from_readers::drive_query_stream`, the thread-per-input
  k-way merge). Batch size 8192, `--max-concurrent-scans 64` (never shed; single stream).
- **Load driver:** the real `tools/flight-loadgen` binary, `--shape full` (full ring, no limit,
  no token bounds → true full scan), `--ramp 1` (single concurrent stream), `--step-duration 62s`.
  Talks plain gRPC to the server; drains and drops every batch.
- **Profiler:** `samply record` (macOS, sampling at 1000 Hz, all threads). `samply` was made to
  **launch** the server as its child (macOS `task_for_pid` attach needs entitlements; child-launch
  gets the task port for free). ~50 s of the record window overlaps active scanning.
- **CPU attribution is weighted by `threadCPUDelta` (µs of on-CPU time per sample), NOT by sample
  count.** This was validated: idle threads show 0.0 s CPU against 115 s of wall time, so the CPU
  delta is genuine on-CPU time, not wall time. This is what lets us separate real work from
  backpressure parking.
- **Symbolication:** the release profile is stripped (`[profile.release] strip = true`), so the
  server was rebuilt unstripped (`CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=false`)
  and its 4,221 sampled frame addresses were resolved with `atos` against the debug-map binary.
  System-dylib frames (kernel/malloc/platform) resolve only to their library, which is sufficient —
  their stage owner is read from the nearest named CQLite ancestor frame.

Two orthogonal attributions were computed and **they agree**:
1. **By operation** — each sample's CPU charged to the leaf-most *non-generic* owning operation
   (so a `malloc`/syscall is charged to the CQLite function that caused it).
2. **By where cycles retire** — self-CPU by library (kernel vs own-code vs allocator vs memcpy).

### Environment

| | |
|---|---|
| Machine | Apple M1 Pro, 10 cores, 32 GB RAM, macOS (Darwin 25.5) |
| Build | `release` + `debug=true`, `strip=false`, `panic=abort`, LTO on |
| Data | **synthesized** `cassandra_easy_stress.keyvalue` (`key text PRIMARY KEY, value text`) — the real easy_stress field table shape |
| Dataset size | 3,000,000 rows across **4 uncompressed SSTables** (`nb-*-big`, ~62 MB Data.db each, ~248 MB total), disjoint key ranges → genuine 4-way merge |
| Cache | page-cache-warm (data read repeatedly across looped scans) |

The largest *real* local table is 632 KB — a single scan finishes in milliseconds and cannot
sustain a steady-state profile. A ~3 M-row single table was synthesized with the write engine
(`WriteEngine`, durability disabled, auto-compaction off, one flush per SSTable) so a single scan
runs ~6 s and a 62 s loop gives a clean steady state. **These SSTables are uncompressed** — the
CQLite write path never emits `CompressionInfo.db` (claim boundary #1406) — which is the single
most important caveat (see §5).

---

## 2. Throughput anchors (single-stream, warm, release)

Measured at the Flight **client** (loadgen), concurrency 1, full scan:

| Run | rows/s | MB/s (Arrow wire) | p50 latency / 3 M-row scan |
|-----|-------:|------------------:|---------------------------:|
| warm probe (stripped bin) | **537 k** | 157 | 5.54 s |
| profile load A (stripped) | 506 k | 148 | 5.99 s |
| profile load B (unstripped, the profiled run) | 501 k | 147 | 6.01 s |

**Single-stream local anchor: ~500–540 k rows/s, ~150 MB/s of Arrow-IPC wire bytes** (≈ 880 MB
per 3 M-row scan; the Arrow/gRPC wire form is ~3.5× the packed 248 MB on disk). Server-side
processing rate matches (~500 k rows/s/stream).

**Relation to the field's "~10 k rows/s/pod".** The local number is ~50× higher, for expected
reasons, and the two are **not** comparable as-is:
- Local is **RF=1, one SSTable set, no replica fan-out, no Trino**, page-cache-warm, **uncompressed**,
  server-direct over loopback. The field number is **through Trino** (split planning + JDBC connector
  + replica dedup + network) at **RF=3** over compressed SSTables with cold-ish cache and 1.93 M
  partitions/node.
- Narrow 2-column rows here maximize rows/s (little per-row data) while *also* maximizing the
  per-row coordination overhead this report indicts. Wider field rows move MB/s up and rows/s down.

Treat the local figure as the **server-direct single-stream ceiling for this row shape**, not a
field prediction.

---

## 3. The CPU breakdown

Total sampled on-CPU budget over the window: **154.1 CPU-seconds** (the scan is multi-threaded, so
CPU-seconds exceed wall-seconds).

### 3a. Where cycles actually retire (self-CPU by library) — the blunt truth

| Where | CPU-s | % | What it is |
|-------|------:|--:|------------|
| **libsystem_kernel** | 84.4 | **54.7 %** | syscalls — overwhelmingly `sync_channel` park/wake between the per-SSTable reader threads and the merge coordinator |
| **cqlite-flight (own code)** | 34.5 | **22.4 %** | the actual parse / merge / reconcile / materialize / Arrow-build logic |
| **libsystem_malloc** | 27.2 | **17.6 %** | allocator — per-row allocations (MergeEntry, partition-key `Vec` copies, `QueryRow`, Arrow builders) |
| libsystem_platform | 5.0 | 3.3 % | `memcpy`/`memmove`/`memset` |
| dyld / pthread / other | 3.0 | 2.0 % | stubs, misc |

> **Only ~22 % of single-stream CPU is CQLite's own logic. ~55 % is kernel syscalls for per-row
> cross-thread channel coordination, and ~18 % is the allocator.** The data-plane compute is
> dwarfed by coordination + allocation overhead for this workload.

### 3b. By pipeline stage (CPU charged to the causing operation)

Mapped onto the seven requested stages. The merge subsystem is split into its *compute* and its
*cross-thread fan-in coordination* so the seven stages still sum to 100 %.

| # | Stage | CPU-s | % | Notes |
|---|-------|------:|--:|-------|
| 1 | **SSTable IO + decompression** | 0.02 | **0.0 %** | page-cache-warm **and uncompressed** — near-zero. **Field will be materially higher** (see §5). |
| 2 | **Binary parse / decode** (Data.db cell → Value) | 14.9 | **9.7 %** | `row_decoder`, `data_access`, cell/vint decode on the reader threads |
| 3 | **Row materialization / per-cell conversion** | 6.9 | **4.5 %** | `entry_to_row` → `assemble_read_cells` → `build_row_from_scan`, `RowKey::new(pk.to_vec())` |
| 4a | **K-way merge / reconcile compute** | 50.0 | **32.5 %** | `reconcile_cluster_with_overlap_counted`, `refill_heap` (BinaryHeap), `finalize_current_cluster`, `build_merge_entry` |
| 4b | **Merge fan-in channel coordination** (park/wake) | 76.9 | **49.9 %** | `from_readers::forward_row` → `SyncSender::send`, one send **per row** into a cap-256 channel; ~94 % of it is kernel park/wake |
| 5 | **Arrow encode** (RecordBatch build) | 1.6 | **1.0 %** | `rows_to_record_batch` / column builders — cheap for 2 narrow columns |
| 6 | **gRPC / Flight write** (IPC serialize + h2 + socket) | 0.3 | **0.2 %** | near-zero on-CPU on loopback (see §5) |
| 7 | **Everything else** (uncharged kernel/alloc) | 3.5 | **2.2 %** | plus SipHash partition-key hashing ≈ 4.5 %, distributed into stages 3/4a above |

> Stage **4b** is not "merge math" — it is the **overhead of the thread-per-input parallel-decode
> design**: every decoded row crosses a bounded `sync_channel` (capacity 256) to a single
> CPU-bound coordinator, and because the coordinator cannot drain fast enough, the reader threads
> spend ~72 s parked in `send` (kernel futex/semaphore). This overhead would not exist in a
> single-threaded merge of the same data.

**Error bars.** Stage boundaries are drawn by symbol name; a few percent can shift between adjacent
stages (e.g. decode↔materialize, or SipHash between 3 and 4a). The *coarse* split — coordination +
alloc ≫ own-compute, transport ≈ 0, IO ≈ 0 — is robust to ±3–4 pp reclassification. The two
independent attributions (§3a self-by-library vs §3b by-operation) corroborate each other:
kernel-55 % ↔ channel-50 %, malloc-18 % ↔ (charged into stages 3/4), own-code-22 % ↔ decode+materialize+reconcile-compute.

### Thread architecture (why the split looks the way it does)

Per `do_get`, the warm-reader path spawns **one reader thread per input SSTable**
(`from_readers::drive_query_stream`), each decoding its SSTable and pushing rows over a bounded
`sync_channel(256)` to **one k-way merge coordinator** (`produce_streaming` /
`KWayMerger`) that reconciles, materializes, builds RecordBatches, and hands them to the tonic
response via a separate `tokio::mpsc`. In the profile:
- The coordinator thread carries ~62 CPU-s of real reconcile/materialize/hash/heap work — it is the
  throughput limiter.
- The ~44 reader threads carry ~72 CPU-s **almost entirely blocked in `send`** (~74 % of each
  reader's time is park/wake, ~13 % actual decode) — they are starved by the coordinator.

---

## 4. Top-3 single-stream bottlenecks (with code paths)

### #1 — Merge fan-in channel handoff: one `send` per row into a cap-256 `sync_channel` (~47–50 % of CPU)
The dominant cost, and almost pure overhead.
- `cqlite-core/src/storage/write_engine/merge/from_readers.rs:137` — `forward_row` → `sender.send(msg)`,
  called **once per row** (33 M sends per scan).
- `cqlite-core/src/storage/write_engine/merge/from_readers.rs:186` — `std::sync::mpsc::sync_channel(STREAMING_CHANNEL_CAPACITY)`.
- `cqlite-core/src/storage/write_engine/merge/mod.rs:537` — `STREAMING_CHANNEL_CAPACITY = 256`.
- Retire site: `libsystem_kernel` park/wake (Rust's `mpsc` is implemented over `mpmc`;
  `mpmc::array::Channel::send::{{closure}}` was the caller of ~72 CPU-s of kernel time).

**Why it dominates:** the payload is one `MergeEntry` per row, the channel is small (256), and the
single consumer is CPU-bound, so producers constantly hit a full channel and park; each row costs a
context-switch pair. For narrow rows the fixed per-row handoff cost swamps the actual data.
**Lever directions (for downstream design, not measured here):** batch multiple rows per channel
message; widen the channel; or bypass the per-input thread/channel entirely for the
single-stream / few-SSTable case and merge inline.

### #2 — K-way merge / reconcile compute on the coordinator (~32 % of CPU)
The real merge math, and the true throughput limiter once #1's overhead is removed.
- `cqlite-core/src/storage/write_engine/merge/mod.rs:4117` — `KWayMerger::reconcile_cluster_with_overlap_counted` (~9 CPU-s on the coordinator alone).
- `cqlite-core/src/storage/write_engine/merge/mod.rs:2941` — `KWayMerger::refill_heap` (BinaryHeap pop/push).
- `cqlite-core/src/storage/write_engine/merge/streaming.rs:567` — `StreamingMerger::finalize_current_cluster`.
- `cqlite-core/src/storage/write_engine/merge/mod.rs:784` — `SSTableRowIteratorAdapter::build_merge_entry`.

### #3 — Per-row allocation + partition-key hashing (~18 % malloc + ~4.5 % SipHash)
- Allocator churn (17.6 % of CPU, `libsystem_malloc`) is driven by per-row allocations:
  `RowKey::new(partition_key.to_vec())` at `cqlite-flight/src/producer.rs:1000` (copies the PK bytes
  every row), `MergeEntry` construction, `QueryRow`/`RowCells` assembly in
  `entry_to_row` (`cqlite-flight/src/producer.rs:967`), and Arrow builder growth.
- SipHash partition-key hashing: 6.9 CPU-s / 4.5 % at `core::hash::sip` (`sip.rs:257`) via
  `BuildHasher::hash_one` — the default `HashMap` hasher used for per-row partition-key lookups
  (`PartitionKeyCache`). A faster hasher and/or fewer per-row key copies would cut into both this
  and the malloc line.

*(Allocation pressure was not counted with dhat — time-boxed. The 17.6 % malloc-CPU is the anchor;
a dhat run on the producer path would give exact allocs/row and is the recommended follow-up.)*

---

## 5. CAVEATS — what this local rig cannot tell us about the field

Ordered by how much they distort the numbers.

1. **Uncompressed data → Stage 1 (IO+decompress) is ~0 % here; the field is NOT.** The CQLite write
   path emits uncompressed SSTables (no `CompressionInfo.db`; the server logged *"Assuming no
   compression"* for every file). Field Cassandra SSTables are **LZ4-compressed by default**, so the
   field adds a whole decompression-CPU stage this profile is structurally blind to. On a
   compressed corpus, expect Stage 1 to become a real single-digit-to-low-double-digit %, partly
   drawn from the relative share of the other stages.
2. **Page-cache-warm → IO-wait is invisible.** Data was reread every loop, so there is essentially
   no disk `read()` wait. The field's 1.93 M partitions/node will not be fully resident; cold/partly-cold
   scans add real IO-wait (wall time, not CPU) that this CPU profile does not capture. This profile
   measures **CPU cost**, not IO-bound wall time.
3. **Transport (Stage 6) ≈ 0 % is a loopback artifact.** Over loopback, the socket write is a cheap
   kernel copy and the client drains-and-drops; arrow-flight IPC encode of already-columnar batches
   is mostly `memcpy`. Over a real network (field), TLS, congestion control, and genuine
   `send`/`recv` syscalls make Stage 6 materially larger. Do **not** conclude "gRPC is free."
4. **Narrow 2-column `keyvalue` rows, one row per partition.** This is a real field table shape
   (`cassandra_easy_stress.keyvalue`), but it is the *extreme* that **maximizes per-row coordination
   and allocation overhead** (Stages 4b/7) and **minimizes** decode/materialize/Arrow-build per row
   (Stages 2/3/5). Wide rows, clustering keys, and collections would shift weight toward Stages
   2/3/5 and dilute the channel/alloc overhead. The #1 finding (per-row channel handoff) is
   *worst* for this shape; treat 47–50 % as an upper bound for it, not a universal constant.
5. **4 SSTables, disjoint keys, no tombstones/overlap.** Real nodes have more SSTables and genuine
   reconciliation overlap (LWW collisions, tombstones, TTL). More inputs → more reader threads → the
   Stage-4b channel cost gets *worse*; real reconciliation collisions make Stage-4a heavier. Both
   push in the same direction (data plane up), but the exact split will move.
6. **Apple M1 Pro, not i4i.xlarge.** Different core counts, memory bandwidth, syscall/context-switch
   costs, and allocator (macOS `libsystem_malloc` vs glibc/jemalloc). The *shape* (coordination +
   alloc dominate for narrow rows) should carry; the exact percentages will not. In particular the
   macOS park/wake syscall cost may differ from Linux futex cost — the 55 % kernel share is the
   figure most sensitive to OS.
7. **RF=1, server-direct, no Trino.** No split planning, connector, replica dedup, or Trino
   worker-side merge — all of which the field "10 k rows/s/pod" number folds in. This profile is the
   layer *underneath* that number.

---

## 6. Reproduction

```bash
# 1. Build server unstripped with debug (dep tree already at debug=true):
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=false \
  cargo build --release -p cqlite-flight --bin cqlite-flight

# 2. Synthesize a 3M-row / 4-SSTable keyvalue table (throwaway example, see note):
cargo build --release -p cqlite-flight --example gen_bigtable   # uncommitted helper
./target/release/examples/gen_bigtable <data_dir> 750000 4

# 3. Profile the server under single-stream full-scan load:
samply record --save-only --no-open -o profile.json.gz -r 1000 -- \
  ./target/release/cqlite-flight --data-dir <data_dir> --listen 127.0.0.1:8815 \
  --batch-size 8192 --max-concurrent-scans 64
# in parallel:
./target/release/flight-loadgen --endpoint http://127.0.0.1:8815 \
  --ticket-template keyvalue-template.json --ramp 1 --step-duration 62s --shape full
# then SIGTERM the server (by exact comm 'cqlite-flight') so samply finalizes.

# 4. Symbolicate frame addresses with atos (-l 0x100000000) against the unstripped binary
#    and CPU-weight samples by threadCPUDelta. (analysis scripts were ad-hoc Python.)
```

**Note on the generator:** `gen_bigtable.rs` is a throwaway generator (write engine + the
`keyvalue` test fixture, durability disabled, auto-compaction off, one flush per SSTable). It was
NOT committed and is removed after this run; the ticket template is a full-ring `FlightTicket` with
no limit/token bounds.

---

## 7. One-paragraph summary for the program

For a single-stream, warm, uncompressed full scan of a narrow `keyvalue` table, CQLite's Flight
server spends **~55 % of CPU in the kernel** (per-row `sync_channel` park/wake between the
thread-per-input decoders and the single k-way merge coordinator), **~18 % in the allocator**
(per-row `Vec`/row/entry allocations + default-hasher key lookups), and only **~22 % in its own
parse/merge/materialize/Arrow logic** — of which the real merge/reconcile compute is the largest
piece. Actual SSTable IO+decompression (≈0 %, artifact of warm+uncompressed local data) and
gRPC/Flight transport (≈0 %, loopback artifact) are invisible here and **will be real in the
field**. The #1 addressable single-stream inefficiency is the **per-row cross-thread channel
handoff** (`from_readers::forward_row`, cap-256 `sync_channel`); #2 is the k-way reconcile compute
on the coordinator; #3 is per-row allocation + SipHash key hashing. Local single-stream throughput
is ~500 k rows/s (~150 MB/s wire) — a server-direct RF=1 ceiling, ~50× the field's through-Trino
RF=3 number, and not a field prediction.
