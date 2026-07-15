# Issue #2210 — MADV_RANDOM on a dedicated point-read mmap: Linux cold-cache A/B

**Machine:** AWS EC2, Linux 6.17 aarch... x86_64, 30 GiB RAM, workspace on EBS gp3
(`/dev/nvme0n1p1`), `read_ahead_kb = 128`. **Genuine cold cache** achieved by
`sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'` before EVERY mapped measurement
(passwordless sudo verified). Backing files created with real random bytes
(`dd if=/dev/urandom`) — non-sparse, so reads hit real EBS blocks.

## Method

Two mappings of the SAME file are compared under identical, page-aligned, seeded random
offsets (fair A/B): **MADV_RANDOM** vs **no-advice** (the `#1143` `Auto` scan regime — the
current point-read behaviour, since the point source shares the scan `Arc<Mmap>`). The
harness models the production point read: `MmapReadAt::read_at` copies a small slice out of
the resident map, so on a cold cache each touch is a major page fault whose cost the mapping's
madvise regime governs. Two signals recorded: (1) `read_bytes` delta from `/proc/self/io` —
bytes pulled from the block layer, a deterministic measure of read-ahead amplification,
independent of EBS timing noise; (2) per-read wall latency (mean/p50/p99).

The A/B regime order is alternated per round to average out any residual EBS server-side
warmth. Harness source: kept out of the shipped tree (scratchpad microbench).

## Realistic point-lookup pattern — few scattered reads per reader open (8 reads × 20 rounds)

This is the true point-read workload: open a reader, do a handful of scattered lookups.

| File size | MADV_RANDOM mean | no-advice mean | Δ mean latency | p99 (RAND / none) | block I/O per read (RAND / none) |
|-----------|-----------------:|---------------:|:--------------:|:-----------------:|:--------------------------------:|
| 1 MiB     | 705 µs | 630 µs | +12% (wash) | **963** / 1665 µs | 4 KiB / 78 KiB (20x) |
| 4 MiB     | 724 µs | 1108 µs | **-35%** | **1167** / 2222 µs | 4 KiB / 107 KiB (27x) |
| 16 MiB    | 707 µs | 1237 µs | **-43%** | **1034** / 2110 µs | 4 KiB / 128 KiB (32x) |
| 64 MiB    | 756 µs | 1335 µs | **-43%** | **1731** / 2656 µs | 4.5 KiB / 128 KiB (29x) |
| 512 MiB   | 794 µs | 1280 µs | **-38%** | **1788** / 2359 µs | 4.5 KiB / 128 KiB (29x) |
| 4 GiB     | 768 µs | 1235 µs | **-38%** | **1578** / 2358 µs | 4.5 KiB / 128 KiB (29x) |

## Heavy-reuse pattern (2000 reads on ONE mapping) — for contrast, NOT the point-read case

With 2000 reads reused on a single small mapping, no-advice's read-ahead pre-warms the whole
small file, so MADV_RANDOM looks bad at ≤64 MiB (e.g. 1 MiB: 468 ms vs 45 ms). That reuse is a
scan-like locality pattern, not a point read. It flips to a MADV_RANDOM win by 512 MiB
(4743 ms vs 6301 ms). Documented so the size gating below is understood as reuse-aware, not a
blanket "large = advise".

## Conclusion — SHIP, size-gated

- **Clear, reproducible win for point reads on SSTables ≳ 4 MiB**: ~35–43% lower cold-cache
  per-read latency, better p99, and ~30x less block-layer I/O (fewer EBS IOPS consumed).
- **Sub-MB files are a wash** (slight mean regression, better tail). All of CQLite's *bundled*
  test SSTables are <1 MiB — a fixture artifact, not the production case (real Cassandra
  SSTables are routinely tens of MiB to GiB).
- Decision: apply `MADV_RANDOM` to a **dedicated 2nd point-read mmap** only when
  `file_size >= 8 MiB` (2x margin above the measured 4 MiB clear-win floor, comfortably above
  the sub-MB wash zone). Below the threshold the point source shares the scan `Arc<Mmap>`
  unchanged — zero extra mapping, zero regression. The scan mapping is **never advised**, so
  the measured #1143 scan behaviour is preserved.
