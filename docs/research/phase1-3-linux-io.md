# Phase 1, Agent 3/8 — Linux IO + LZ4 decompress: modeling the field cost Phase 0 could not see

**Date:** 2026-07-21 · **Status:** research (uncommitted) · **Scope:** READ-ONLY

Phase 0 measured a **warm-cache, uncompressed, macOS-loopback** single-stream scan and found
Stage 1 (IO + decompress) = **0.0 %** of CPU and Stage 6 (transport) = **0.2 %**. Both are
artifacts of the rig, not the field. This agent owns that blind spot: model what LZ4 decompression
and cold NVMe IO actually cost on the field profile (i4i.xlarge NVMe, 1.93 M partitions/node, LZ4
SSTables, cold scans, B4 cold-start ≤ 3 s), enumerate the Linux IO levers, and — the load-bearing
part — state honestly **when IO becomes the binding constraint** relative to the agent-1/2 CPU work.

**Headline (three claims, derived below):**

1. **The read path is already a mature multi-backend IO engine** (Epic F, closed): buffered / mmap /
   `O_DIRECT`+`F_NOCACHE` direct, an `Auto` RAM-sizing heuristic, `madvise` (SEQUENTIAL / WILLNEED /
   RANDOM), a clamped direct-IO prefetch window, lock-free positional `pread`, and a decompressed-chunk
   cache. Most textbook IO levers are **already built**. `io_uring` and `posix_fadvise` are the only
   two absent from the codebase.
2. **At the 100 k–600 k rows/s targets, neither cold NVMe bandwidth nor LZ4 decompress is the binding
   constraint.** LZ4 decode adds **~0.1 %–1 %** CPU over the Phase-0 budget; cold sequential bandwidth
   needed is **~10–350 MB/s**, an order of magnitude under what an i4i.xlarge NVMe delivers. The engine
   stays **CPU-bound on the Phase-0 per-row coordination** (channel handoff 50 %, reconcile 32 %,
   alloc 18 %).
3. **IO owns exactly one thing on the critical path today: cold-start *latency* (B4 ≤ 3 s), not
   throughput.** The one genuinely-additive code lever is **`posix_fadvise(SEQUENTIAL)` + `WILLNEED`
   on the cold buffered scan / open** — cheap, portable, no downside. Everything heavier (`io_uring`,
   a decompress-ahead thread) should wait behind agent-1/2 and behind a *measurement* that IO is
   binding, which the arithmetic says will not happen until row rates far above 600 k.

---

## 1. The read path, from first principles (what actually touches the disk)

### 1a. Backends (all present; `cqlite-core/src/storage/sstable/reader/`)

The reader resolves a per-SSTable **`DiskAccessMode`** at open (`resolve_disk_access_mode`,
`reader/mod.rs:245`). Default config is `Auto` (`config.rs:124`); the Flight server and CLI do **not**
override it, so the field scan path runs `Auto`:

| Backend | Mechanism | Where |
|---|---|---|
| **Buffered** | `BufReader<tokio::fs::File>`, page cache, per-chunk `seek`+`read_exact` | `source.rs` `BlockSource::Buffered` |
| **Mmap** | `memmap2` map, served from page cache, zero read syscalls | `source.rs` `BlockSource::Mapped`; `reader/mod.rs:1141` |
| **Direct** (unix) | `O_DIRECT` (Linux) / `F_NOCACHE` (macOS), aligned `pread` into a 4 K-aligned bounce buffer, clamped read-ahead window | `source.rs:357` `open_direct_file`, `DirectCursor`; `prefetch_window.rs` |
| **Point-read (positional)** | lock-free `FileExt::read_at` (`pread`) / `MmapReadAt` slice / `DirectReadAt` | `read_at.rs` |

**`Auto` heuristic** (`reader/mod.rs:262`): file `< mmap_min_size` → Buffered; file `> memory_fraction
× RAM` (default 0.5) → Direct; else → **Mmap**. On an i4i.xlarge (32 GiB RAM) the Direct threshold is
**16 GiB** — no single field SSTable approaches that, so **`Auto` resolves to Mmap for essentially
every field Data.db**, i.e. page-cache reads with **kernel-default read-ahead and no explicit hint**
(see §1c).

### 1b. Chunk decompression path (LZ4)

- Cassandra 5.0 LZ4 SSTables store Data.db as a stream of **64 KiB-uncompressed chunks**
  (`CompressionInfo.chunk_length`, `compression_info.rs:14`), each compressed + a trailing 4-byte
  BE CRC32. Chunk offsets live in `CompressionInfo.db`.
- Read → CRC → decompress → cache funnels through **one plane**: `ChunkSource`
  (`reader/chunk_source.rs`). `read_compressed_chunk_at` (`block_io.rs:580`) does one positional read
  of payload+CRC, verifies CRC **before** decompress (guardrail #1411), then
  `Compression::decompress` (`compression.rs:249`) runs the decoder. Decompressed chunks land in the
  **B1 `DecompressedChunkCache`** as `Bytes` (zero-copy into cache; `chunk_source.rs:149`).
- The decoder is **`lz4_flex` with `safe-decode`** (`Cargo.toml:39/145`; `compression.rs:259`
  `decompress_size_prepended`). Safe-decode is the deliberate choice (no `unsafe`), and it is the
  slower of lz4_flex's two modes.
- **Decompress is *inline on the per-input reader thread*** in the thread-per-input merge
  (`from_readers`), so it already overlaps with the coordinator's consumption — the reader threads
  are ~74 % *parked in `send`* (Phase-0 §3), i.e. decompress runs in CPU the reader thread would
  otherwise waste blocked. This matters for the "decompress-ahead thread" lever (§2, low value).

### 1c. The one real gap: no explicit read-ahead on the cold sequential scan

> **STILL ACCURATE, AND RE-CONFIRMED BY MEASUREMENT, 2026-09-01 (issue #2824).** The `WILLNEED`
> flip this paragraph motivates was built, measured and **rejected**; `Auto` still issues no
> `madvise`. Two reasons, both recorded in `docs/reports/issue-2824-artifacts/RESULTS.md`:
> (1) `SSTableManager::new` opens **every** SSTable under the data dir at `Database::open`, so
> advising at open fires whole-file read-ahead for every table of every keyspace before any query
> is seen — point-lookup-only workloads pay it in full; (2) on this lane's EBS volume
> (132 MB/s measured, 128 KiB read-ahead window) the default window already saturates the device,
> so there was no headroom to demonstrate a benefit in either direction. The lever needs
> scan-lifetime plumbing and an i4i rig, both filed separately. The `MADV_SEQUENTIAL` prohibition
> and its drop-behind rationale are UNCHANGED and still binding. The `reader/mod.rs:316` anchor
> below is stale; the function now lives in `reader/backend_resolve.rs`.

`Auto` deliberately issues **no `madvise`** (`mmap_advice_for`, `reader/mod.rs:316`; issue #1143):
`MADV_SEQUENTIAL` couples aggressive read-ahead with **drop-behind**, which evicts hot pages under
concurrent write load and blew up the read p99 tail (~2×). So the cold mmap scan relies entirely on
the **kernel default read-ahead window (typically 128 KiB)**. There is **no `posix_fadvise` anywhere
in the tree** (confirmed: `rg fadvise` → nothing). This is the actionable IO lever — see §2.

---

## 2. Modeling the field IO + decompress cost (the arithmetic)

### 2a. Data volume per node

Phase-0 anchor: **3 M narrow rows (`key text, value text`) = 248 MB uncompressed** across 4 SSTables
→ **~82 B/row uncompressed on disk**. Field rows are **wider** (brief). The exact
`easy_cass_stress.keyvalue` value width is not pinned in the repo, so model a 3-point range for the
field's **~1.94 M rows/node**:

| Field row width (uncompressed) | Uncompressed Data.db/node | LZ4 on-disk (÷2.5 typ. text) | # 64 KiB chunks |
|---|---:|---:|---:|
| ~82 B (narrow, Phase-0 shape) | 159 MB | ~64 MB | ~2,400 |
| ~250 B (moderate) | 485 MB | ~195 MB | ~7,400 |
| ~500 B (wide) | 970 MB | ~390 MB | ~14,800 |

(LZ4 on text/keyvalue typically 2–4×; 2.5× used as a mid estimate. Chunk count = uncompressed ÷ 64 KiB.)

### 2b. Is cold NVMe bandwidth ever binding at 100 k–600 k rows/s?

A full scan reads the **compressed** bytes off disk. Bytes/s off disk needed to sustain a row rate:

```
compressed_bytes_per_row = uncompressed_bytes_per_row / compression_ratio
disk_MBps = rows_per_s × compressed_bytes_per_row
```

| Row rate | 82 B/row (÷2.5 → 33 B) | 250 B/row (→100 B) | 500 B/row (→200 B) |
|---|---:|---:|---:|
| 100 k rows/s | **3.3 MB/s** | 10 MB/s | 20 MB/s |
| 600 k rows/s | **20 MB/s** | 60 MB/s | **120 MB/s** |

**i4i.xlarge NVMe (AWS Nitro SSD) delivers ≥ ~1 GB/s sequential read and ~100 µs-class latency.**
The worst cell above (600 k rows/s, wide) needs **120 MB/s** — **~8× under** the drive's sequential
bandwidth. **Cold IO *bandwidth* is never the binding constraint in the target envelope.** It would
only bind at multi-GB/s row rates (millions of wide rows/s), far above 600 k.

The real cold-IO risks are **latency-shaped, not bandwidth-shaped**:
- **B4 cold-start ≤ 3 s**: first-touch faulting a cold file's Summary/Index/first chunks (see §4).
- **Read-ahead starvation**: with the default 128 KiB window and *synchronous* mmap page faults on
  one thread, a cold sequential scan can stall on fault latency well below the drive's bandwidth —
  this is what `fadvise(SEQUENTIAL)` on a **buffered** fd fixes without the mmap drop-behind hazard.
- **Multi-SSTable interleave**: N reader threads reading N SSTables interleave the device's LBA
  stream, degrading each toward random. NVMe tolerates this far better than spinning disks, and 60 MB/s
  aggregate is trivial, so this is a minor concern until SSTable counts are high.

### 2c. LZ4 decompress CPU, as a % on the Phase-0 breakdown

Prior art: reference LZ4 (C, unsafe) decompresses **~4–5 GB/s/core** (lz4.org: ~4970 MB/s single
core). **`lz4_flex` safe-decode is ~40–60 % of that — model ~1.5 GB/s/core** (its README benches it
against C lz4; safe mode pays bounds checks). Decompress is **per-byte of uncompressed output**:

```
decompress_CPU_s = uncompressed_bytes / 1.5e9
```

Anchoring to the **Phase-0 CPU budget** (154 CPU-s for a 3 M-row / 248 MB scan, at 500 k rows/s):

| Field shape | Uncompressed/scan | Decompress CPU-s @1.5 GB/s | As % of a ~154 CPU-s Phase-0-shaped budget |
|---|---:|---:|---:|
| 82 B/row (matches Phase-0 volume) | 248 MB | 0.17 CPU-s | **~0.11 %** |
| 250 B/row | 485 MB | 0.32 CPU-s | ~0.21 % |
| 500 B/row | 970 MB | 0.65 CPU-s | ~0.42 % |

Even halving the decoder to a pessimistic **0.75 GB/s** only doubles these to **~0.2 %–0.8 %**.

**Why so small:** Phase-0's budget is **per-row-coordination-dominated** (channel handoff + alloc +
reconcile = ~98 %), and those costs scale with **row count**, not bytes. Decompress scales with
**bytes**. For narrow rows the per-row overhead swamps decompress entirely; for wide rows the byte
volume grows but so does per-row parse/materialize, keeping decompress a low-single-digit-percent
tail at most.

**Honest caveat on the denominator:** the Phase-0 154 CPU-s is *inflated* by the channel-handoff
overhead agent-1 targets (~50 %). Once that is removed the budget roughly halves, so decompress's
**relative** share roughly **doubles** — to ~0.2 %–1.6 %. Still small in absolute terms, but this is
the mechanism by which "IO/decompress starts to matter" — it becomes visible *only after* the CPU
coordination cost is gone (§3).

**Corrected Phase-0 Stage 1 for the field:** replace the `0.0 %` warm+uncompressed cell with
**~0.2 %–1 % CPU (LZ4 decode) + a cold-start latency term (wall-time, not CPU)**. Stage 6 (transport)
similarly moves off `~0 %` over a real network, but that is agent-4/5/6 territory, not IO.

---

## 3. When does IO become binding? (the honest ordering claim)

Phase-0 is unambiguous: at 500 k rows/s **warm**, the engine is **CPU-bound on merge plumbing** —
50 % channel park/wake, 32 % reconcile, 18 % alloc; IO and decompress are ~0 %. The field adds a
**cold-latency** term and a **~1 %** decompress-CPU term. Therefore:

> **IO throughput is NOT the binding constraint, and does not become one within the 100 k–600 k rows/s
> target envelope, until the agent-1 (per-row channel handoff) and agent-2 (reconcile/alloc) CPU
> levers land.** Even after they land, cold **bandwidth** still won't bind (§2b arithmetic); what
> surfaces next is cold **read-ahead latency** and the now-relatively-larger **decompress CPU** — both
> addressed by the cheap fadvise lever, not by `io_uring`/`O_DIRECT` batching.

**Dependency order for the program:**

1. **Now, unconditionally — `fadvise(SEQUENTIAL)` + `WILLNEED` (S, portable).** It has no downside,
   directly serves B4 cold-start (§4), and pre-empts the read-ahead-starvation risk. Do it early even
   though IO isn't yet binding — it is a pure win and removes IO as a *variable* before agent-1/2 change
   the CPU picture.
2. **agent-1 + agent-2 land** (remove ~50 % channel + ~18 % alloc + shrink reconcile). Throughput
   roughly doubles; the CPU budget halves; decompress's relative share doubles but stays ~1 %.
3. **Re-measure cold, compressed, on i4i.** Only if that measurement shows a cold scan **bandwidth- or
   IOPS-bound** (the arithmetic says it won't below multi-GB/s row rates) do the heavy levers
   (`io_uring` batching, forced `O_DIRECT` scan) earn their portability cost. Until then they are
   speculative.

---

## 4. Lever table

Multiplier = effect on scan throughput / cold-start unless noted. "Already built" levers are marked;
their entry is *tuning*, not new implementation. Portability cost is against the **macOS dev/gate
parity** rule: Linux-only code must be `cfg`-gated with a portable fallback.

| Lever | Mechanism | Anchored multiplier arithmetic | Cost | Risk | Portability / dev-parity |
|---|---|---|---|---|---|
| **`posix_fadvise(POSIX_FADV_SEQUENTIAL)` on the buffered cold scan** ← **the gap** | Doubles/extends kernel read-ahead on the buffered fd **without** mmap drop-behind (#1143). Frees the cold sequential scan from the 128 KiB default window. | Cold bandwidth need is 20–120 MB/s (§2b); default 128 KiB read-ahead at ~100 µs NVMe fault latency pipelines to ~1.3 GB/s only if perfectly overlapped — a single faulting thread stalls well below that. `SEQUENTIAL` lifts the window ~2–8× → removes cold-scan fault stalls; **~1.0× warm, meaningful on cold p99**. | **S** | Low — advisory hint, kernel may ignore; no correctness effect. | Linux `libc::posix_fadvise`; macOS has no `POSIX_FADV_SEQUENTIAL` → map to `F_RDAHEAD`/`F_RDADVISE` or **no-op fallback**. `cfg`-gate; gate stays green on macOS (no-op path). |
| **`posix_fadvise(WILLNEED)` on Summary/Index/first data chunks at open** | Kick off async read-ahead of the components the first `do_get` touches, so cold-start faults are already in flight. | Directly serves **B4 ≤ 3 s**: overlaps index/summary/first-chunk faults with query setup instead of serializing them. Turns a cold-start's serial fault chain into one async prefetch. | **S** | Low — advisory; wasted read if the file is never scanned. | Same `cfg` story as above; macOS `F_RDADVISE` (range read-ahead) is the near-equivalent; else no-op. |
| **`fadvise(DONTNEED)` after a scan-once full scan** | Evict the just-scanned pages so a one-shot analytic scan doesn't pollute the page cache and evict the working set (the *good* half of drop-behind, without the concurrent-write eviction that hurt #1143 — because it's issued *after* the scan, not as ongoing drop-behind). | No throughput multiplier; protects **other** queries' warm cache under memory pressure (1.93 M partitions/node won't all fit). Guards the hot-set hit rate that keeps warm p50 low. | **S** | Medium — must only fire on genuine scan-once (not on a table being re-scanned); mis-targeting cold-evicts a hot file. Gate behind the ticket's scan-once signal. | `cfg`-gate; macOS `F_NOCACHE`-after-the-fact is not equivalent → no-op fallback. |
| **Read-ahead / prefetch-window tuning** (already built: `direct_io_prefetch_bytes`, clamp) | The direct backend already floors the window at `2×(chunk+4)` and 4 K-aligns it (`prefetch_window.rs`). Tuning = raise the 1 MiB default for cold sequential direct scans. | Direct backend only engages for files > 16 GiB (Auto) — **never** for field SSTables. Value is near-zero **unless** we force Direct for scan-once (below). | **S** | Low. | Already portable (value computed on all platforms; backend degrades to buffered off-unix). |
| **mmap vs `pread` on NVMe for the scan pattern** (already built; a *policy* choice) | Point reads: dedicated `MADV_RANDOM` map (#2210, done). Full scans: mmap faults synchronously on the tokio worker (SIGBUS risk on truncation; #1143 tail). Buffered `pread` + `fadvise(SEQUENTIAL)` pipelines better and is SIGBUS-safe. | For a **cold sequential** NVMe scan, buffered-pread+`fadvise` ≳ mmap-default-readahead (no synchronous per-page fault on the worker thread, no drop-behind). Reinforces lever #1; argues **keep buffered as the scan default** (it already is — mmap is opt-in). | **S** (policy) | Low — buffered is already the safe default. | Fully portable. |
| **Decompress-ahead / pipeline decompress with decode** | Overlap chunk decompress with the merge coordinator's row consumption. | **Already effectively overlapped**: decompress runs on the per-input reader thread, which is ~74 % *parked in `send`* (Phase-0 §3) — decompress consumes otherwise-wasted CPU. A dedicated decompress-ahead thread adds a queue hop for ~1 % of CPU. **~1.0×.** | **M** | Medium — another cross-thread handoff, the exact cost agent-1 is removing. Net-negative until the merge is single-threaded. | Portable (std threads), but low value. |
| **Compression-block prefetch / read-ahead of next chunk's compressed bytes** | Issue the next chunk's positional read before the current chunk finishes decompressing. | Bounded by the same 20–120 MB/s need; the B1 cache + `Auto`→mmap page cache already serve most re-reads. Marginal on cold, nil on warm. **~1.0×.** | **M** | Low. | Portable. Low value given §2b. |
| **`io_uring` batched reads** | Submit many chunk reads via one `io_uring` submission queue; overlap IO with CPU without a thread per input. | Only helps when **IO-bound**, which §2b shows the workload is **not** below multi-GB/s row rates. At 60 MB/s of compressed reads, submission-batching saves ~0 wall-time. **~1.0× until IO binds.** | **L** | High — new unsafe dependency (`tokio-uring`/`io-uring`), runtime integration, kernel ≥ 5.x, and it duplicates the read path. | **Linux-only.** Needs full `cfg`-gate + a buffered/pread fallback the macOS gate exercises; doubles the read-path surface to test. **Highest dev-parity cost.** Defer until a cold measurement proves IO binding. |
| **Forced `O_DIRECT` for scan-once full scans** (backend built; policy new) | Bypass the page cache on a one-shot analytic scan so it doesn't evict the OLTP working set (HTAP isolation). | No throughput multiplier vs warm; protects co-located point-read latency under a big scan. Overlaps with `fadvise(DONTNEED)` (cheaper). At 120 MB/s the aligned-read overhead is affordable. | **M** (policy on built backend) | Medium — `O_DIRECT` alignment + the #2319 bounce-buffer-reuse fragility (regressed once); refused on tmpfs/overlay (already falls back). | Backend already `cfg(unix)` with buffered fallback; macOS `F_NOCACHE` path exists. Moderate. |

**S/M/L** = implementation size. The only **S + portable + no-downside + on-the-critical-path** lever
is the fadvise family (rows 1–2). Everything L is IO-bound-only and gated behind a measurement.

---

## 5. Prior work in the repo + backlog (dedup — no new duplicate filings)

Epic **F #1518 (closed)** already delivered the IO substrate: F1 reader-map `RwLock`, F3 blocking IO
off async, **F6 direct-IO + read-ahead window** (#1596, `prefetch_window.rs`), **F6.1 `MADV_RANDOM`
point map** (#2210, closed), plus the point-read positional `pread` refactor (**C2 #1573**), the
`Auto` disk-access heuristic, and the B1 decompressed-chunk cache (**Epic B #1514**). The direct-IO
bounce-buffer reuse regression (**#2319**) is closed but flagged fragile.

**What is genuinely un-filed and additive** (candidates, do not duplicate F6):
- `posix_fadvise(SEQUENTIAL/WILLNEED/DONTNEED)` on the **buffered** cold-scan + open path — **absent
  from the tree**, distinct from the mmap `madvise` that #1143 disabled. This is the cleanest new
  filing.
- A **Stage-0 cold-compressed decode bench on i4i** to convert this model's §2b/§2c estimates into
  measured numbers (extends the existing `benches/decode_policy_bench.rs` F6.4 decompress-throughput
  arm, which already measures lz4_flex decode-only vs full-scan but on the local warm rig).

`io_uring` and forced-`O_DIRECT`-scan are **not** recommended as filings yet — they are IO-bound-only
levers and the arithmetic says IO does not bind in the target envelope.

---

## 6. One-paragraph summary for the program

CQLite's read path is **already** a mature multi-backend IO engine (buffered / mmap / `O_DIRECT`,
`Auto` RAM-sizing, `madvise`, clamped prefetch window, lock-free `pread`, decompressed-chunk cache —
Epic F, closed); the only textbook levers **absent** are `io_uring` and `posix_fadvise`. Modeling the
field profile Phase-0 could not see: **LZ4 decode via `lz4_flex` safe-decode (~1.5 GB/s/core) adds
only ~0.1 %–1 % CPU** over the Phase-0 budget (decompress scales with bytes; Phase-0's cost is
per-row-coordination-dominated), and **cold NVMe *bandwidth* is never binding** — the target
100 k–600 k rows/s needs just **~10–120 MB/s** of compressed reads against an i4i.xlarge NVMe's ≥ 1 GB/s.
The engine stays **CPU-bound on the Phase-0 merge plumbing**. IO owns exactly one thing on the
critical path today — **cold-start *latency* (B4 ≤ 3 s)** — and the single cheap, portable, no-downside
lever is **`posix_fadvise(SEQUENTIAL)` + `WILLNEED`** on the cold buffered scan/open (map to
`F_RDADVISE`/no-op on macOS to keep the gate green). Do that now; defer `io_uring` and
forced-`O_DIRECT`-scan behind **both** the agent-1/2 CPU levers landing **and** a cold-compressed i4i
measurement proving IO binding — which, by the arithmetic here, will not happen below multi-GB/s row
rates.
