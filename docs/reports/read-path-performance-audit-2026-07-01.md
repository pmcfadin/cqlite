# CQLite Read-Path Performance Audit — 2026-07-01

**Goal:** highest-performing reads with even, predictable latency under high concurrent load.
**Scope:** the read path only — physical I/O, decompression/chunk handling, caching, index/summary/bloom/BTI navigation mechanics, multi-SSTable merge and scan iterators, query-engine execution, allocation/memory behavior, concurrency and tail latency. **Out of scope:** binary format *decoding* correctness (parser), the write path, bindings FFI internals.
**Method:** seven parallel read-only specialist audits (I/O + decompression, lookup path, scan/merge, query engine, memory/caching, architecture + DB best practices, Rust idioms) with all findings verified against source (`file:line`), plus lead-level cross-verification of conflicting claims. `size_of::<Value>()` was measured against the crate, not estimated.

---

## Executive summary

**The I/O substrate is A-grade.** Three-backend block source (buffered / mmap+madvise / O_DIRECT with 4K alignment) with auto-selection, bloom-first point ordering on BIG, an authoritative BTI trie (no heuristics), an 8-deep chunk prefetch pipeline on the windowed scan, hardware-accelerated CRC32, and a genuinely well-engineered bounded streaming scan (`scan_stream_windowed.rs`). This is ahead of what most embedded readers ship.

**Everything above it leaks the win.** Five systemic problems, in descending order of impact:

1. **There is no functioning read cache.** The per-reader `block_cache` is initialized empty and never inserted into (hit rate hardwired to 0.0); a second complete caching subsystem (`MemoryManager`: LRU block cache + row cache + buffer pool, ~725 lines, unit-tested) is instantiated and shelved as `_memory`; the one live cache (`ChunkDecompressor`, CLI path) memcpys the whole chunk on every hit and evicts a random entry despite its "LRU" comment. Every query re-reads and re-decompresses every chunk it touches. The config knobs (`block_cache: 256MB`, `row_cache: 128MB`, `max_memory` validation) describe a budget that does not exist at runtime.

2. **The common query path materializes when it should stream.** `LIMIT 10` on a 1M-row table decodes all 1M rows into per-row HashMaps, then truncates (the reader's `scan()` accepts a `limit` — the executor always passes `None`). `COUNT(*)` buffers the whole table to produce one integer. Any table with >1 SSTable generation silently collapses the bounded-streaming guarantee: the k-way merge collects and sorts the entire reconciled result before the consumer sees row 1. The CLI `query` command bypasses all of it and slurps the whole Data.db into RAM (`BulletproofReader`). The `MAX_RESULTS = 1M` cap converts big-but-legal queries into hard errors. The streaming executor already does all of this correctly — the fix is largely "make the default path behave like the streaming one."

3. **Per-row/per-chunk constant factors are 2–5× what they need to be.** Measured `size_of::<Value>() == 88` bytes (three rare variants inline; boxing them → 32). Rows are `HashMap<String, Value>` with SipHash and per-row column-name `String` clones (a 1M-row × 10-col scan allocates ~10M identical strings). Every chunk read pays 3–4 redundant seeks (file-size re-probe under the cursor lock), 3 allocations and 2 full copies, and on the buffered backend ~5–7 tokio blocking-pool bounces. Every BTI point lookup heap-copies the entire `Partitions.db` — twice (prune walk + seek walk).

4. **Point-read cost is bimodal.** The default-build query-engine path is correct: bloom/BTI candidate prune → single-candidate seek to the partition offset. But three adjacent paths fall off a cliff: (a) the `SSTableManager::get()` KV surface — the BIG index is keyed by Murmur3 digests so `find_entry()` on raw keys *always misses* (documented in code) → `scan_for_key` → read + decompress the **entire Data.db per lookup** ; (b) the multi-candidate (multi-generation) case, which full-decodes every candidate rather than seeking; (c) the `tombstones` build, where `scan_partition` is an honest full-scan+retain. BIG point reads that do hit the index still serialize on a shared `Arc<Mutex<BlockSource>>` held across disk I/O — the pre-#815 convoy, surviving on the point path.

5. **The perf gate measures the wrong things.** `read/point_lookup` is a `LIMIT 1` scan proxy, not the real point-read path; every gated number is a Criterion **median** — tail latency is untracked; `concurrent_scan.rs` and `read_while_write.rs` exist but are **not in `perf-gate.json`**, so a scaling or tail regression merges silently; the <128MB memory target is checked only by a manual dhat profiling step, never in CI; `history.jsonl` is referenced by tooling but not persisted.

**Cross-cutting theme — "built but unwired," again.** The July parity audit found features built but not wired to their public surface; this audit found the performance mirror image: a dead block cache, a shelved MemoryManager, an unused `scan(limit)` parameter, an unused `platform/threading.rs` admission-control module, dead reader stacks (`SchemaAwareReader`, `ChunkedDataReader`, `StreamingDecompressor`), decorative config knobs, and a hit-rate metric structurally pinned to 0. The wiring-evidence doctrine applies to performance machinery too: **a cache/limit/pool exists only when the hot path demonstrably exercises it** — every fix below carries a work-counter or allocation-budget test to prove the wiring.

### What is already good (verified; do not churn)

- Backend auto-selection + O_DIRECT 4K alignment + deliberate no-madvise-in-Auto (measured decision, issue #1143) — `reader/source.rs`, `reader/mod.rs:191-274`
- Bloom-first BIG point ordering with definitive-negative short-circuit; BTI correctly skips bloom for the authoritative trie — `data_access/mod.rs:107-133`, `partition_lookup.rs:648-706`
- Windowed streaming scan: bounded window, 8-deep raw-chunk prefetch, off-async-pool parse, batched cross-thread wakes, documented worst-case memory — `scan_stream_windowed.rs`
- O(1) BIG index HashMap lookup (`index_reader.rs:203`), binary-searched Summary, once-per-open component loading shared via `Arc<SSTableReader>`
- Honest access-path signaling (`AccessPath`/`FallbackReason`, epic #951) and bounded IN-list fan-out (`MAX_IN_TARGETED_LOOKUPS = 64`)
- Clustering-bounded reads seek within the partition via `Rows.db` (they do not scan the whole partition)
- Faithful Murmur3 port (pinned against Cassandra vectors); hardware CRC32 (`crc32fast`); correct LZ4 block-format usage
- No `unwrap()`/`expect()` in library code in scope; no `unsafe`; no `Box<dyn>` on the hot path

---

## Proposed epics

Sequencing rationale: **A first** (no optimization claim is trustworthy without the right benchmarks — and several fixes below claim large wins that must be demonstrated, not asserted). B/C/D are the impact epics and are largely independent; E and F are constant-factor and tail-latency epics that partially fall out of C's `pread` refactor; G is the enabling cleanup that makes every other fix land in one place instead of three.

Every child issue lists its TDD tests — these are written **first** and must fail on current `main` (or be demonstrably un-writable today, e.g. a hit-rate test against a cache that cannot hit).

---

### Epic A — Measurement first: read-path benchmark + regression-gate suite  `P0`

The gate today cannot see the problems this audit found. Land this before (or with) the first optimization so every later claim is pinned.

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| A1 | Bench the real point read | `read/get_partition` bench driving `scan_partition`/`get()` on a UUID-PK fixture (BIG **multi-chunk** + BTI variants), replacing the `LIMIT 1` scan proxy (`benches/read.rs:20-24`) in `perf-gate.json` | gate entry fails on ≥10% median regression; fixture large enough that O(file) fallbacks are visible | S |
| A2 | Tail-latency harness + gate | mixed-load harness (1 continuous scan + point-read stream): emit `{p50,p99,p999}` JSON; gate p99/p50 ratio (advisory first, then enforced) | harness asserts point-read p99 ≤ k× scan-free baseline | M |
| A3 | Gate existing concurrency benches | add `concurrent_scan` (n∈{1,2,4,8}) scaling floor (fail if n4/n1 < threshold) and `read_while_write` read-side p99 to `perf-gate.json` — both benches already exist, ungated | regression sim: reintroduce a shared mutex in a scratch branch → gate must fail | S |
| A4 | Memory + layout gates | dhat-based `cargo test` lane (feature `dhat-heap`): `select_10k_rows_alloc_budget`, `materialized_select_byte_ceiling`; compile-time `const _: () = assert!(size_of::<Value>() <= 40);` | each test fails on current main (Value is 88B; alloc counts are O(rows×cols)) | M |
| A5 | Cold-open bench + persisted ledger | `open/cold` bench (component-load cost); `mem/open_n_readers` RSS; persist `history.jsonl` (commit or CI artifact); test-only work counters: trie walks, decompress calls, seeks, `open(2)` count, fd high-water mark | counters plumbed like existing `work_counters`/`SCAN_FOR_KEY_CALLS`; used by every epic below | M |

---

### Epic B — Ship the read cache  `P0/P1`

One shared, bytes-bounded, concurrent decompressed-chunk cache; retire the two dead subsystems into it or delete them. This is the single biggest lever for even p99 under load.

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| B1 | Shared decompressed-chunk cache | keyed `(sstable_id/generation, chunk_index)` → `Arc<[u8]>`/`Bytes` (hit = refcount bump, never memcpy); bytes-bounded eviction (moka/quick_cache — internally sharded, **no global RwLock**: `lru::get` mutates recency so `RwLock` degrades to a Mutex, `memory/mod.rs:129`); wired into `get_cached_data` (`data_access/mod.rs:428-469`), windowed scan chunk fill, and BTI target-chunk read | second identical point read: 0 decompress calls (counter), 0 underlying reads (counting `Read` double); eviction-order unit test (A,B,A,C cap 2 evicts B); `read/point_lookup_repeated` bench ≥10× cold | L |
| B2 | Resolve the dead cache subsystems | delete `SSTableReader.block_cache`/`block_meta_cache`/`CachedBlock`/hit-miss atomics (`reader/cache.rs:22-31`, `types.rs:233-235`, `data_access/mod.rs:436-437`) and either repurpose `MemoryManager` (`memory/mod.rs`) as B1's backing store or delete it + its decorative config knobs (`config.rs:287-348`: cache sizes, `CachePolicy::{Lfu,Arc}`, `allocator.*`) — **owner decision, see NEEDS-YOU** | config test: removed knobs no longer deserialize (or documented); `get_cache_stats` no longer reports a structurally-0 hit rate | M |
| B3 | Fix or fold `ChunkDecompressor` cache | clone-on-hit (full 16–64KB memcpy, `chunk_decompressor.rs:112-113`) and arbitrary-key eviction mislabeled LRU (`:120-125`) → `Arc` values + real recency, or fold into B1 when the CLI moves off `BulletproofReader` (D5) | read-same-chunk-twice test: decompress invoked once; hit path allocates 0 chunk-sized buffers | S |
| B4 | Key/partition-offset cache | small LRU `(sstable, partition_key) → (offset, size)` so hot point reads skip index/trie descent entirely (Cassandra key-cache analogue) | repeated-key lookup: trie-walk/index-probe counter == 0 on hit; parity: cached offset == fresh resolution | M |
| B5 | Honest cache observability | real hit/miss/eviction stats + byte occupancy on B1/B4; `estimate_memory_usage` counts real residents (today it sums an always-empty map, `cache.rs:42-52`) | stats integration test; hit-rate visible via `DatabaseStats` | S |

---

### Epic C — Point-lookup fast path  `P0/P1`

Make every point-read entry point O(chunk), zero-alloc on the index descent, and convoy-free.

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| C1 | Fix BIG `get()` O(file) fallback | index is keyed by 16-byte Murmur3 digests; raw-key `find_entry()` **always misses** (documented at `data_access/mod.rs:152-155`) → `scan_for_key` → `stitch_and_parse_all_chunks` = whole-file read+decompress per lookup (`sequential.rs:330-395`). Hash the query key to the digest before lookup; seek to the partition's chunk via `CompressionInfo::chunk_for_offset` (as BTI does at `bti.rs:600-640`) | `SCAN_FOR_KEY_CALLS` stays 0 for a present key on a multi-chunk NB fixture; bytes-read counter O(chunk) not O(file); p99 of 1k random `get()`s flat vs file size | M |
| C2 | Positional reads (`pread`) — kill the shared-cursor convoy and per-lookup `open(2)` | BIG point reads serialize on `Arc<Mutex<BlockSource>>` held **across disk I/O** (`types.rs:217`, `data_access/mod.rs:428-445`); BTI lookups instead pay `File::open` per lookup (`source.rs:108-130`, default `use_mmap: false`) with fd-exhaustion risk. Introduce a `ReadAt` trait (pread on shared fd / mmap slice / direct `read_at`): no mutable position, no mutex, no per-op fd | concurrency high-water-mark double: 8 concurrent gets with a 10ms-sleeping source complete in ≪ 8×10ms; fd high-water test: 64 lookups + 8 scans ≤ open-time fds + constant; scaling bench N∈{1..32} ≥0.7×linear to 8 | M/L |
| C3 | Zero-copy, single-walk BTI trie | every lookup allocates + memcpys the entire `Partitions.db` (`bti/parser/partitions.rs:480-517`) though it's resident in `Arc<Vec<u8>>`; the trie is walked **twice** per point read (prune `might_contain_partition` then seek, `mod.rs:1223-1226` → `data_access/bti.rs:114,475`); per-node descent heap-allocates the full child table to follow one byte (`node.rs:216-241`). Slice-based walk from the existing buffer; thread the prune's resolved location into the seek; zero-alloc `find_child_offset` | alloc counter: 0 bytes ∝ trie size per lookup; trie-walk counter == 1 per single-candidate point read (currently 2); byte-parity vs pinned `test_da` offsets (0/63/125) | M |
| C4 | Hoist per-candidate rehashing; local successor resolution | same key Murmur3-hashed + BTI-encoded once **per candidate SSTable** in the prune loop (`partitions.rs:560-571`, `partition_lookup.rs:653-724`) — hoist to once per read; first targeted read DFS-enumerates **every** partition (+ per-wide-partition `Rows.db` reads) to bound one (`partition_lookup.rs:287-387`) — resolve the successor locally or via `CompressionInfo.data_length`, and/or precompute at open (also removes the `OnceLock` first-seek stall for concurrent first-seekers) | hash-count counter == 1 across a 32-generation fan-out; cold first targeted read does not scale with partition count; two concurrent first-seekers → exactly one DFS | M |
| C5 | Range short-circuit + dead scaffolding | O(1) `first_key`/`last_key` bounds check (already parsed from Summary, `summary_reader.rs:97-157`, unused) before bloom/trie — compare in the index's order domain, no heuristics; delete dead `bti/nodes.rs::NodeParser`/`TrieNode` (no non-test callers; wrong pointer decode) and the Dense-returns-empty `BtiNode::get_transitions` footgun (`node.rs:343-352`) | pruned decisions never false-negative vs bloom/trie (parity); out-of-range candidates skip the hash (counter) | S/M |

---

### Epic D — Streaming by default: pushdown + bounded memory  `P1`

Make the paths users actually hit behave like the already-correct streaming executor. Kills the three "silently materializes everything" P1s and the `MAX_RESULTS` cliff.

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| D1 | LIMIT/OFFSET pushdown into the materializing scan | executor never passes the limit (`execute.rs:783-785` passes `None`; truncation happens post-hoc at `mod.rs:721-735`); break the scan loop at `limit+offset` when no Sort/Aggregate follows; pass limit to `storage.scan` when no residual predicates | `scan_rows ≤ limit + slack` for `LIMIT 10` on a 10k-row fixture; `LIMIT 10` latency flat across {1k,100k,1M} rows | M |
| D2 | Streaming aggregates + retire the `MAX_RESULTS` cliff | GROUP-BY-free `COUNT/MIN/MAX/SUM` buffers all rows (`mod.rs:657-699`) and `requires_materialization` forces even the streaming API through it (`mod.rs:339-360`); drive the O(1) accumulator off the scan stream; the 1M-row hard error (`execute.rs:575,681-685,805-809`) becomes a safety valve only | `COUNT(*)` peak resident rows == O(1) (dhat ceiling flat across {1k,1M}); `LIMIT 1_500_000` returns rows, no error | M |
| D3 | Streaming multi-generation merge | >1 generation collapses `scan_stream` to collect-all + sort + dribble (`mod.rs:1871-1907` → `merge_generations_for_read` `:1462-1514`); `KWayMerger::step()` already yields one partition at a time — feed partitions straight to the channel (`blocking_send` batches, reusing the `BATCH_EMIT_ROWS` pattern); same for the WRITETIME/TTL metadata variant (`merge_generations_for_read_with_metadata`) and the multi-candidate point path (decode-and-retain → merge only the target partition) | peak RSS on 2-gen 1M-row partition ≤ window + one partition (fails today); time-to-first-row ~constant as generation count grows; `scan_stream_multigen` bench 1/2/4 gens | M/L |
| D4 | Manager `scan`: merge, don't concat+re-sort | per-reader results are already sorted (token order) yet the manager re-sorts the full concatenation **by raw key bytes** (`mod.rs:880-911`) — double sort, different order key (flagged to parity owners), O(n log n) spike on the async thread; k-way merge with early-exit on limit | ordering parity test (manager vs reader order key); merge-vs-concat bench; sort no longer on async worker (or spawn_blocking) | M |
| D5 | CLI query off `BulletproofReader` | CLI `query` slurps the whole Data.db (`bulletproof_reader.rs:210-265` `read_all_data`) and pre-parses all entries even for its "stream" API; route through `Database`/`scan_stream` like other CLI commands | CLI `LIMIT 10` on a large table: bounded heap (fails today); CLI-vs-`database.execute` output parity | M |
| D6 | Byte-bounded result budget | enforce the memory target as a byte ceiling on materializing results (running `estimate_row_size`), not a 1M row count; make CLI/bindings default to the streaming executor for unbounded SELECTs | wide-row fixture trips the byte guard before the row guard; budget test in the A4 lane | M |

---

### Epic E — Hot-path mechanics: allocations, copies, syscalls  `P1/P2`

The constant-factor epic. Individually small, collectively 2–5× on scan throughput and steady-state allocation load.

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| E1 | Box fat `Value` variants | **measured** `size_of::<Value>() == 88` (Tombstone 88, Udt 72, Json 72 inline; `types.rs:29-84`); box the three rare variants → 32B; every `Vec<Value>`/`Option<Value>`/row slot/clone pays 2.75× today | `const` size assert ≤ 40 (A4); predicate-eval + sort throughput bench; enable `clippy::large_enum_variant` | M |
| E2 | Positional rows | `QueryRow.values: HashMap<String,Value>` (SipHash) + per-row column-name `String` clones (`result.rs:66-85`, `row_build.rs:105-159`, projection re-clone at `select_executor/mod.rs:744-758`) → `Arc<[Arc<str>]>` shared header + `Vec<Value>` positional; interim (S): capacity hints, FxHashMap, hoist names out of the row loop, projection `HashSet`/bitset instead of per-column linear scan, decode partition-key columns once per partition not per row | alloc budget: column-name allocs O(cols) not O(rows×cols); per-row map allocs == 0 (positional) or 1-with-capacity (interim); wide-scan bench | L (interim S) |
| E3 | Copy-chain + codec reduction | per chunk: zeroed compressed `Vec` + fresh decompress `Vec` + `extend_from_slice` into window = 3 allocs/2 copies (`block_io.rs:401`, `compression.rs:267`, `scan_stream_windowed.rs:555`); decompress **into** the window (`lz4_flex::decompress_into`); borrowed/`Bytes` chunk reads on mmap (zero compressed-side copy); reuse zstd `DCtx` per cursor (rebuilt per chunk today, `compression.rs:392`); single `read_exact` for payload+CRC (two reads today, `block_io.rs:400-425`); size `BufReader` ≥ chunk+4 (8KiB default defeats it, `source.rs:54-55`) | dhat: allocs/chunk ≥3 → ≤1; read-op counter: 1 read/chunk; 33-table byte-parity harness stays green | M |
| E4 | Cache the file size; drop redundant seeks | 3–4 seek syscalls per chunk re-derive the immutable file size **under the cursor lock** (`block_io.rs:160-169`; also `chunk_decompressor.rs:165-169`); known at open (`reader/mod.rs:383`); also skip the explicit seek when the cursor is already at the next sequential chunk | seek counter: full scan of K chunks ≤ K+O(1) seeks (currently ~5K); buffered-backend scan bench | S |
| E5 | Query-engine constant factors | resolve schema once per query as `Arc<TableSchema>` (deep-cloned 2–4×/query under async RwLock today, `schema/mod.rs:931-958`); skip the redundant `Project` rebuild for bare-column selects (`mod.rs:738-770` — double projection); decorate-sort-undecorate (comparator clones two `Value`s per comparison, `mod.rs:625-652`); cache optimized plans + stop re-optimizing prepared statements per execute (`prepared.rs:176-186`, plan cache never inserted on the modern path, `engine.rs:250-292`); GROUP BY hash index (linear group scan is O(rows×groups), `aggregation.rs:57-77`) | schema-clone counter == 1/query; HashMap allocs/row == 1 not 2; sort key evals == n not n log n; `optimize` calls ≤1 after prepare (counter); high-cardinality GROUP BY sub-quadratic | M |
| E6 | Deterministic codec + retry semantics | Snappy chunk decompress **guesses** between two formats per chunk — a wasted full decompress attempt on data-dependent input and a latent wrong-bytes hazard (`compression.rs:275-316`; the CLI path is already strict raw-only) — thread the known provenance, no guessing (**no-heuristics**); retry loop sleeps 10/20ms and re-reads on deterministic corruption errors, and the legacy-path retry re-reads from a moved position (`block_io.rs:64-108`) — retry transient `Io` only, re-seek first, fail fast on corruption | adversarial fixture (valid raw-snappy whose head parses as a plausible BE length): single decode attempt + byte-correct; corrupt chunk errors in <10ms with 1 read attempt; transient-once double recovers reading the same offsets | S |
| E7 | Window-drain cursor | `window.drain(0..consumed)` memmoves the residual tail per confirmed partition — Θ(P·W) per window on partition-dense tables (`scan_stream_windowed.rs:720`, same in `compaction.rs:699`); track a `window_start` cursor, compact on refill | bytes-memmoved counter O(W) not O(P·W); dense-tiny-partitions bench | M |
| E8 | Idioms bundle | de-`async` six never-awaiting pipeline fns (+ `clippy::unused_async` lint); FxHash for integer/digest-keyed maps; `OFFSET` via `skip/take` not `drain` prefix-shift (`mod.rs:732`); per-partition-limit key: hash not clone (`mod.rs:711`, `execute.rs:381-499`); IN-expansion prefix clones → index-based buffer (`lookup.rs:476-477`); `#[inline]` on tiny cross-crate accessors (measure first) | clippy lints enforce; dhat: 100-element IN allocs O(list) not O(list²) | S |

---

### Epic F — Concurrency & scheduling: even latency under load  `P1/P2`

The tail-latency epic. C2's `pread` refactor retires the two worst mechanisms; these finish the job.

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| F1 | Snapshot the reader map | `table_readers.read().await` guard held across **entire multi-reader scans** (`mod.rs:822-930` etc.); tokio RwLock is FIFO-fair, so one queued writer (reload/schema-set/remove) parks every subsequently-arriving read behind the longest in-flight scan — classic bimodal tail. Clone the `Vec<Arc<Reader>>` and drop the guard (the `resolve_table_readers` pattern already exists, `mod.rs:1802-1817` — only streaming uses it); better: `ArcSwap` snapshot | gated-scan test: writer + subsequent point read completes before the scan's gate opens; mixed-load p99 harness (A2) | S/M |
| F2 | Batch the public streaming channel | the forwarder re-flattens per row into the public channel (`mod.rs:1982,1887,2013`) — one async wake per row survives the pipeline whose own docs measured per-row seam costs at 31–42%; expose `Vec<Row>` batches (API-additive) | wakeup counter O(rows/batch); streaming scan throughput bench | S/M |
| F3 | Blocking I/O off async workers | mmap page faults and O_DIRECT reads run **inline in `poll_read`** on tokio workers (`source.rs:208-233,415-441`, self-documented) — K cold scans can pin all workers, stalling warm point reads (p99-diverges-under-mixed-load mechanism); route via the blocking half or land C2's sync-core direction; **gate before making mmap the default** | cold-cache harness: 8 cold mmap scans + warm point lookups, point p99 < 10× warm baseline | M |
| F4 | Blocking-pool admission control | one `spawn_blocking` parse task per windowed scan, pool default 512 shared with tokio-fs internals — at high scan concurrency point-read file ops queue behind long-lived parse threads (priority inversion); semaphore-cap scans with fs headroom; decide fate of the dead `platform/threading.rs` semaphores (unused by the read path — wire as the single admission point or delete) | low-`max_blocking_threads` test runtime: point reads complete while 8 scans run | S/M |
| F5 | Per-query shared-line + lock hygiene | `QueryStats` write-locked 2–3×/query (`engine.rs:82-133,717`) → per-field relaxed atomics; plan-cache hit path uses `DashMap::get_mut` (shard write lock) to bump `hit_count` → `AtomicU64` + `get`; global `LAST_ACCESS_PATH: Mutex` written every SELECT (`access_path.rs:212-224`) → `ArcSwapOption`/thread-local with test hook; schema-registry `RwLock` on the per-lookup digest path (`key_digest.rs:16,53`) → `Arc` snapshot at open; `CachePadded` work counters if profiling shows false sharing | prepared point-lookup QPS scaling N=1..32 slope; lock frames out of top profile | S |
| F6 | Hardware-sympathy hints + codec eval | `MADV_RANDOM` for the mmap point-read path (Auto currently no-advice — keep the scan decision from #1143); `posix_fadvise(SEQUENTIAL)` on buffered scan cursors (no drop-behind semantics, unlike `MADV_SEQUENTIAL`); round `direct_io_prefetch_bytes` up to ≥2× chunk+4 so chunks don't straddle window refills; evaluate `lz4_flex` without `safe-decode` behind an opt-in `fast-lz4` feature (upstream ~20–30%; CRC precedes decompress) — measure + fuzz before enabling | per-chunk `read_at` ≤ ceil(chunk/window)+1; `decompress/lz4_16k` A/B bench; corrupt-after-CRC fuzz corpus against unchecked build | S |

---

### Epic G — Reader consolidation (enabling)  `P2`

~11 reader/decoder entry points exist; three are dead or test-only, two duplicate the live stack. Every optimization above currently has to land in 2–3 places. Also pays down the file-size ratchet (bti.rs 67KB, block_io.rs 52KB, sequential.rs 46KB all over threshold).

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| G1 | Delete the dead | `SchemaAwareReader` (constructed only in own tests), `ChunkedDataReader` (zero src consumers), `StreamingDecompressor`/`CompressionReader::read_streaming` (zero consumers), duplicate `CompressionInfo` in `compression.rs:1352-1557` (the open path parses the same file twice with two parsers, plus ~25 `exists()` generation-probing stats per open — derive from `SsTableDescriptor`), `CompressionReader` → plain algorithm field | one-parse-per-open assertion (counting harness); full gate + 33-table parity green; minimal-features build | M |
| G2 | One decode plane | after D5, retire `BulletproofReader` from the query path; single `ChunkSource` (read + CRC + decompress + B1 cache) shared by `get`/`scan`/CLI | CLI-vs-core parity (one scan stack); decompress logic exists in exactly one module (arch test) | L |
| G3 | One `PartitionLocator` | `IndexReader` + `SummaryReader` + `promoted_index_reader` + BTI trie behind one format-tagged `locate(key) → (offset, size)`; optional: Summary-bounded on-disk Index.db search mode for very high partition counts (today the whole Index.db materializes per open, `index_reader.rs:140-186,337-340` — O(1) lookups but unbounded open-time RSS) | locate() parity vs each legacy path on BIG+BTI fixtures; `mem/open_n_readers` (A5) improvement for the bounded mode | L |
| G4 | Confine the legacy | `TombstoneMerger` (O(entries×tombstones) nested loops, off default path) confined behind `tombstones` with doc scoping or its `get()` use replaced by the KWay point path; delete or fix the legacy duplicate-work parallel table scan (`executor.rs:472-515` — N workers each scan the full table, self-documented) | scan-issue counter == 1 per worker-partition; tombstones-build parity unchanged | M |

---

## Priority matrix (deduplicated headline findings)

| Finding | Sev | Epic | Confidence |
|---|---|---|---|
| No functioning read cache (dead `block_cache`, shelved `MemoryManager`, clone-on-hit CLI cache) | P0 | B | Verified |
| Perf gate blind to point reads, tails, scaling, memory | P0 | A | Verified |
| `get()` O(file): BIG index digest mismatch → whole-file decompress per lookup | P0* | C1 | Verified (scoped: `get()` surface, fallbacks, multi-candidate, tombstones build — the default single-candidate query path seeks correctly) |
| LIMIT never pushed into the common scan; aggregates buffer everything; 1M-row error cliff | P1 | D1/D2 | Verified |
| Multi-generation `scan_stream` materializes + sorts the whole merged result | P1 | D3 | Verified |
| CLI query reads entire Data.db into RAM | P1 | D5 | Verified |
| BIG point reads convoy on shared `Arc<Mutex<BlockSource>>` across disk I/O | P1 | C2 | Verified |
| `table_readers` RwLock guard held across whole scans (FIFO writer stall) | P1 | F1 | Verified |
| `Value` = 88 bytes (measured); per-row `HashMap<String,Value>` + name clones | P1 | E1/E2 | Measured/Verified |
| BTI lookup copies whole `Partitions.db`, walks trie twice, allocs per node | P1 | C3 | Verified |
| 3–4 seeks/chunk file-size probe under lock; 3 allocs + 2 copies per chunk; 2 reads/chunk | P1/P2 | E3/E4 | Verified |
| Schema deep-cloned 2–4×/query; double projection; comparator clones; prepared re-optimizes | P2 | E5 | Verified |
| Snappy dual-format guess per chunk (perf + no-heuristics hazard) | P2 | E6 | Verified |
| Blocking I/O inline on async workers; per-row public channel wake; pool inversion | P2 | F2–F4 | Verified |
| Reader zoo: 3 dead stacks, 2 duplicates, double CompressionInfo parse per open | P2 | G | Verified |

## NEEDS-YOU (product/design decisions — not decided here)

1. **Cache strategy (B2):** build the real shared chunk cache (recommended — it is the biggest even-latency lever and `MemoryManager`'s tested LRU/eviction can back it) vs. delete all dead cache machinery and rely on the OS page cache (simpler, but leaves repeated-decompress CPU on the table and the config knobs still need removing either way).
2. **Default disk access mode:** `use_mmap: false` today; mmap is Cassandra's own default for local immutable SSTables and removes the per-op `open(2)`/fd class entirely — but F3's blocking-fault gate should land first. Flip the default, or keep buffered + `pread`?
3. **Sync-core direction (C2/F3):** the `ReadAt` trait refactor is the load-bearing architectural call (kills the cursor mutex, per-op opens, seek races, and most blocking-pool traffic in one move). Endorse as the target architecture?
4. **Ordering flag (D4):** manager concat re-sorts by raw key bytes while readers sort by token — flagged to parity owners as a correctness smell, not just perf.
5. **Snappy guess (E6):** the dual-format attempt can theoretically return silently wrong bytes on adversarial input — decide whether this also warrants an oracle-driven bug issue (parity test) independent of the perf fix.

## Suggested delivery order

1. **A** (gates first — small, unblocks honest claims for everything else)
2. **C1 + C2 + B1** (point-read cliff, convoy, cache: the p99 story)
3. **D1 + D2 + D3** (streaming by default: the memory/predictability story)
4. **E1 + E4 + E6 + F1** (cheap verified constant-factor and tail wins)
5. **E2/E3/E5/E7 + F2–F6** (the long tail of mechanics)
6. **G** (consolidation, interleaved as the enabling refactor when it de-duplicates work above)

---

*Produced by 7 parallel read-only audit agents + lead cross-verification. Source claims are anchored to file:line as of `main` @ 5c080d2a. Companion artifacts: the July 2026 parity audit (`docs/reports/cassandra-test-parity-audit-2026-07-01.md`) — the "built-but-unwired" theme is shared.*
