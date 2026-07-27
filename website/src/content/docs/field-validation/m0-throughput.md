---
title: M0 — Disk / SSTable Read-Path Profile
description: The M0 throughput-program run (cqlite#2818) — a server-direct full-scan IO characterization across v0.16.0 → 0.17-dev. 0.17-dev nearly doubled single-stream throughput via CPU-path fixes, but the block-device read pattern (~4.3 KB requests, one LZ4 chunk a page at a time) did not move. The standing lever is scan-side cross-chunk readahead.
sidebar:
  label: M0 Disk Profile (2026-07-27)
  order: -3
---

# M0 — disk / SSTable read-path profile

The first run of the **0.17 scan-path throughput program** ([epic #2817](https://github.com/pmcfadin/cqlite/issues/2817),
[M0 / #2818](https://github.com/pmcfadin/cqlite/issues/2818)). Unlike the release-validation rounds,
this is a **server-direct** profile — `flight-loadgen → cqlite-flight` with **no Trino** — built to
answer one question: *what does the SSTable read path actually ask the block device for, and is it
the standing throughput lever?* Run as a two-arm A/B, **v0.16.0 (pre-fix) → 0.17-dev (post-fix)**,
same corpus and method.

**Disk verdict: 0.17-dev did not change the disk read pattern.** Across both arms the scan reads the
SSTables in **~4.3 KB block requests — one LZ4-compressed 16 KiB chunk, faulted a page at a time —
and 99.3–99.7% of all reads land in the `[4K, 8K)` bucket.** 0.17-dev nearly doubled single-stream
throughput (107 → 199 MB/s warm) by fixing the **CPU/pipeline** path (#2876 read-plane split, #2825
byte-bounded Arrow batches), so the device now does *fewer* tiny reads per second — util actually fell
from ~74% to ~53% — but the **request size is unmoved**, and the scan still uses only **28% of a
711.8 MB/s device**. The remaining gap is not disk bandwidth.

**The standing lever is scan-side readahead across adjacent compressed chunks** — a ≥256 KiB
scan-only read buffer (gated on `> chunk_length`), the [CASSANDRA-15452](https://issues.apache.org/jira/browse/CASSANDRA-15452)
fix. It is **not in the 0.17 manifest.** With it, the read-size histogram should move to ≥64 KB, the
`stream_cold_fault` sub-phase (4.4 s/scan cold, 42× the warm value) should shrink, and cold /
partly-cold single-stream throughput should rise toward the device ceiling instead of plateauing at
~28% of it.

<a href="/cqlite/field-reports/m0-throughput/report.html" class="not-content" target="_blank" rel="noopener">**Open the full disk report →**</a>

The report covers, disk-first:

- **Block-read size distribution** — side-by-side `bpftrace` histograms for both arms (the reads did not move).
- **iostat** — `rareq-sz` unchanged at ~4.3 KB; IOPS and `%util` fell as the CPU path sped up; `r_await` 0.10 ms proves the device is not the bottleneck.
- **Headroom vs the raw device** — `fio bs=1M` ceiling 711.8 MB/s; the scan uses 15% → 28% across the arms.
- **Read path resolved** — the `MADV_RANDOM` point mapping vs the new 0.17 scan-side Data.db mapping (#2876).
- **#2819 sub-phase timers** — `stream_cold_fault` isolates the cold-IO cost (4.4 s cold → 106 ms warm, a 42× collapse).
- **Disk-access knobs** — `CQLITE_DISK_ACCESS_MODE` / `CQLITE_PREFETCH` A/B: nothing helps; the 4 KB size is intrinsic.

## Method note

Server-direct, unstripped self-built `cqlite-flight` + `flight-loadgen`: Arm 1 = the `v0.16.0` tag;
Arm 2 = origin/main `ed3ac7e` (0.17-dev, `--features observability`). Same LZ4-compressed
`cassandra_easy_stress.keyvalue` corpus (`chunk_length_in_kb=16`, ~3.4M partitions, ~650 MB Data.db)
on i4i.xlarge (4 vCPU / 2 physical cores, XFS on the i4i NVMe). Disk stats via
`bpftrace block_rq_complete`, `iostat -x 1`, `/proc/<pid>/io` (read_bytes / rchar / syscr reported
separately, never divided), and an `fio bs=1M` ceiling. bcc's `xfsslower` will not compile on the
lab's HWE 7.0.0-aws kernel, so `bpftrace` is the substitute; `samply` recorded zero samples on that
kernel, so the CPU decomposition (on the full #2818 write-up) uses `perf record`.

This is a **release delta with four confounded changes** (#2876, #2877, #2765, #2825) — the throughput
numbers are their combined effect, not attributable to any single issue. Full method, the CPU
decomposition, and the C(N) core-scaling result (the plateau tracks *physical* cores, so pod sizing is
a real lever) are on [#2818](https://github.com/pmcfadin/cqlite/issues/2818).
