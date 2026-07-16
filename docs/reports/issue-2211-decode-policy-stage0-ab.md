# Issue #2211 — Runtime DecodePolicy (FastUnsafe lz4): Stage-0 measure-first A/B

**Status:** Stage-0 blocking-gate artifact (OpenSpec change `runtime-decode-policy`, Decision 5).
Measured, awaiting the owner's go/no-go. **No `unsafe` code was written; no fast backend was
committed.** Stages 1–5 remain unbuilt per the design's hard gate.

**Machine:** AWS EC2, Intel Xeon Platinum 8488C (16 vCPU, 2 threads/core, 105 MiB L3), 30 GiB RAM,
Linux 6.17 (`6.17.0-1019-aws`), x86_64. Rust 1.88.0, release/LTO builds.

**Fixture:** `test_timeseries.sensor_data` (`nb-1-big`, LZ4Compressor) — the largest fixture in the
vendored corpus whose chunks are actually lz4-*decoded*. Geometry (from `CompressionInfo.db`):
`chunk_length = 16384`, `data_length = 121,249` bytes, 8 chunks (7 full + a 6,561-byte final chunk),
**all 8 lz4-compressed, none stored raw**. The larger `test_comp` incompressible fixture stores raw
chunks and never calls the lz4 decoder, so it is unusable for this question.

> **Corpus-size caveat (load-bearing).** 121 KiB is tiny — a fixture artifact, not the production
> case (real Cassandra SSTables run tens of MiB to GiB). So the decode numbers below are a **hot-cache,
> CPU-bound decode rate**: the single most favorable regime for FastUnsafe, because it isolates the CPU
> cost of the bounds checks with zero I/O to hide behind. On a real GB-scale, I/O-bound scan the decode
> step would be an even *smaller* fraction of end-to-end time. The Stage-0 conclusion is therefore an
> upper bound on FastUnsafe's benefit, not a floor.

## What FastUnsafe would and would not change

FastUnsafe (design Decision 4) removes the lz4 output-bounds checks at exactly one site:
`chunk_decompressor.rs::decompress_lz4_chunk`'s `lz4_flex::decompress` call. It does **not** remove the
inline CRC32 verify, the file read, the seek, or the size validation that surround it, and it does
**not** touch Snappy/Deflate/Zstd or the small-block path. So the ceiling on any FastUnsafe win is the
cost of the lz4 decode step alone.

## Method

Two committed benches (`cqlite-core/benches/decode_policy_bench.rs`, Criterion, `harness = false`) plus
two throwaway out-of-tree proxies for the unchecked-decode delta. All four measure the **same** 8
on-disk lz4 chunk payloads (the committed bench extracts them mirroring `decompress_lz4_chunk`; the
proxies were fed a byte-for-byte dump of those exact payloads). Throughputs are reported in
decompressed/scanned bytes so all arms share one axis.

Committed arms (run: `CQLITE_DATASETS_ROOT=.../test-data/datasets cargo bench -p cqlite-core
--features cli-helpers --bench decode_policy`):

- `decode_policy/lz4_flex_decompress` — SAFE lz4 decode in isolation (the only step FastUnsafe changes).
- `decode_policy/full_chunk_path` — whole production chunk-decompress path, cold cache per iter
  (seek + read record + CRC32 verify + lz4 decode + size validate).
- `decode_policy/full_scan` — end-to-end `SELECT *` over the same fixture through the public query API,
  measured against the fixture's uncompressed `data_length` for a shared axis.

Throwaway proxies (out-of-tree, `opt-level=3` + LTO, 200k iterations each, **not committed**):

- **liblz4 (Decision-2 backend A)** via `lz4-sys 1.11.1 +lz4-1.10.0`: `LZ4_decompress_safe` vs
  `LZ4_decompress_fast`. All decoders verified byte-identical to `lz4_flex` before timing.
- **`lz4_flex` safe-decode ON vs OFF**: the cleanest apples-to-apples "cost of the bounds checks in the
  actual current decoder" — the same crate/algorithm, only the `safe-decode` feature flipped. This is
  labeled an **upper-bound proxy** for an achievable unchecked-decode win; it is *not* a claim that any
  shippable backend delivers it (see Decision-1 conflict below).

## Results

### Decompress-only (the ceiling on any FastUnsafe win)

| Decoder | ns per full-fixture pass | Throughput | vs current prod |
|---|---:|---:|---:|
| `lz4_flex::decompress` (SAFE, current production) | 47,160 | 2.39 GiB/s | — (baseline) |
| `lz4_flex` UNCHECKED (`safe-decode` OFF) | 23,426 | 4.82 GiB/s | **2.01x, −23.7 µs** |
| liblz4 `LZ4_decompress_safe` (backend A, checked) | 23,965 | 4.71 GiB/s | 1.97x |
| liblz4 `LZ4_decompress_fast` (backend A, UNCHECKED) | 109,493 | 1.03 GiB/s | **0.43x (slower!)** |

Committed Criterion arm `lz4_flex_decompress` independently measured the SAFE path at **45.2 µs median
(2.50 GiB/s)** — cross-validating the 47.2 µs proxy figure.

**Two decisive facts:**

1. **Removing bounds checks buys at most ~2x on the decode step** (`lz4_flex` unchecked: 23.4 µs vs
   47.2 µs, saving ~23.7 µs/pass). That is the theoretical ceiling.
2. **The design's recommended backend (A) cannot deliver it.** liblz4's `LZ4_decompress_fast` is
   *deprecated upstream*, is no longer bound by modern `lz4-sys` (had to be declared by hand to link),
   and measured **3.6x slower than `LZ4_decompress_safe`** on these payloads. Modern liblz4 optimized
   the *safe* path and left `_fast` a slow deprecated shim, so backend (A)'s unchecked entry point is a
   net regression, not a win. The only variant that shows a decode win is `lz4_flex` with `safe-decode`
   off — which is exactly what Decision 1 rejects (a graph-global feature that makes the minimal build
   and every other consumer silently unsafe under feature unification).

### End-to-end context (medians, all over the same ~121 KiB fixture)

| Arm | Median time | Throughput | Share of scan time |
|---|---:|---:|---:|
| `decode_policy/lz4_flex_decompress` (SAFE decode only) | 45.2 µs | 2.50 GiB/s | **1.78%** |
| `decode_policy/full_chunk_path` (decode + CRC + read + validate) | 208.8 µs | 554 MiB/s | 8.22% |
| `decode_policy/full_scan` (`SELECT *`, end-to-end) | 2.54 ms | 45.5 MiB/s | 100% |

- The entire SAFE lz4 decode is **1.78% of end-to-end scan time**. Even if FastUnsafe made decode
  *free*, the scan could improve by at most 1.78%.
- Applying the measured best-case unchecked win (~23.7 µs saved of the 45.2 µs decode) yields a
  projected **~0.90% end-to-end scan improvement**.
- The `full_scan` arm's own run-to-run noise band is **±4.7%** (2.42–2.66 ms across 100 samples). The
  projected FastUnsafe win is ~5x smaller than the measurement noise — i.e. **not detectable
  end-to-end**.
- This corroborates the field evidence cited in the design (#2385, #2397): decode is not the
  bottleneck; ~91% of scan time is query execution / row materialization, not lz4.

## Reproduction

- Committed benches: `cqlite-core/benches/decode_policy_bench.rs` (permanent asset; skip-registers when
  fixtures are absent; not a perf-gate entry). `lz4_flex` was added to `cqlite-core`
  `[dev-dependencies]` so the bench can time the checked decoder in isolation.
- Throwaway proxies (liblz4 backend-A pair; `lz4_flex` safe-on vs safe-off) were built out-of-tree over
  a byte-for-byte dump of the same 8 payloads and are **not committed** (per Task 0.3). Method and
  results are recorded above so the numbers are reconstructable.

## Read on the Stage-0 gate (input to the owner's decision, not the decision)

The evidence points hard toward **close #2211 as measured-but-not-justified** (the outcome Decision 5
anticipated as most likely), for three compounding reasons:

1. **No material end-to-end win exists even in the most favorable regime.** Best case ~0.9%, below the
   ±4.7% noise floor, on a hot-cache CPU-bound micro-scan; a real I/O-bound scan would be smaller still.
2. **The design-sanctioned backend (A) is a regression, not a win** — `LZ4_decompress_fast` is
   deprecated and 3.6x slower than the checked liblz4 path on modern liblz4.
3. **The only variant that *does* speed up decode (`lz4_flex` safe-off) is the exact approach Decision 1
   forbids** — it makes the minimal build silently unsafe. So there is no shippable backend that both
   respects the design's safety invariant and delivers the decode win.

The counter-argument the owner may weigh: this corpus is tiny and hot-cache. If a genuinely
decompress-bound workload is expected — very large, highly compressible SSTables scanned repeatedly from
page cache where decode CPU (not I/O) dominates — the decode fraction would rise, and a ~2x decode
speedup could matter. But even then, backends (A) as-specified does not provide the speedup, so a
FastUnsafe pursuit would first need a different backend (vendored in-tree `unsafe`, Decision-2 option B)
whose UB-liability the design explicitly steers away from.

**Recommendation (owner to confirm):** close #2211, archive `runtime-decode-policy` with a
"measured, not justified" note, and keep `cqlite-core/benches/decode_policy_bench.rs` as the standing
decode-vs-scan measurement. Do not proceed to Stage 1+.
