# Issue #2824 — cold/warm A/B result (lever reverted)

> **OUTCOME: THE LEVER WAS REVERTED AND DOES NOT SHIP (issue #2824, lead ruling on REQ-2824-03,
> 2026-09-01).** `PrefetchMode::Auto` still issues no `madvise`. This document records a measurement
> of a change that was built and then backed out — read every "patched" figure below as *what the
> flip would have done*, never as current behaviour. Why it was rejected:
> `SSTableManager::new` opens **every** SSTable under the data directory at `Database::open`
> (`storage/sstable/manager_open.rs:61` -> `:300`), so advising at open fires whole-file read-ahead
> for every table of every keyspace before any query is seen. See `../../..`-relative
> `openspec/changes/madvise-willneed-dontneed/proposal.md`.


**Verdict: NO detectable effect, and NO regression, on this host — and this host is
structurally incapable of exhibiting the effect AC1 asks about.** The lever is neither
validated nor invalidated here. AC1's cold-i4i clause remains **UNMEASURED**.

Raw artifacts: `ab/summary.csv`, `ab/scan-attributable.csv`, `ab/host.txt`, `ab/construction.md`,
`ab/strace-madvise.{baseline,patched}.txt`. Harness: `cold-warm-ab.sh`.

## What ran

3 rounds x 2 arms x **3 phases**, arm order alternating per round, page cache dropped immediately
before both the floor and the cold phase, each phase a separate run with its own `/usr/bin/time`.
Corpus: the `ws0.events` fixture, `Data.db` 2,774,760,422 bytes (~630,000 pages). Both arms built from
ONE tree differing by one match arm and verified by `strace` to differ by exactly one `madvise` call —
see `ab/construction.md`.

The **floor** phase is the same binary run cold with `--setup-only`: it opens the reader and reads the
index/summary but performs no scan, so its fault count is the non-scan cost of starting this process on
a cold cache. `scan_major_faults = cold - floor`.

| signal | baseline median [min-max] | patched median [min-max] |
|---|---|---|
| **scan-attributable major faults** (`cold - floor`) | **4** [2-5] | **4** [3-6] |
| raw cold major faults | 51 [50-52] | 49 [49-51] |
| floor major faults (startup only) | 47 [47-48] | 45 [45-46] |
| warm major faults | 0 | 0 |
| cold wall secs | 28.46 [22.41-31.32] | 32.52 [23.39-32.59] |
| warm wall secs | 18.12 [17.50-19.24] | 17.54 [17.39-17.72] |

## Reading it

**Scan-attributable major faults are identical: 4 and 4, across ~630,000 file pages.** The kernel's
default read-ahead was already converting essentially everything to minor faults, in *both* arms. There
was nothing for `MADV_WILLNEED` to convert.

**This attribution corrected an earlier misreading of my own data, and it is the reason the floor phase
exists.** The raw cold counts are 51 vs 49, which reads as a small improvement. It is not: the whole
difference sits in the **startup floor** (47 vs 45), i.e. in faulting the executable and its shared
libraries — because the global page cache is dropped, those are cold too and `%F` counts them. `%F` is
per-process, so it is immune to the neighbours; it is **not** isolated to the scan mapping, and
reporting it unqualified attributed process-startup faults to the scan. Subtracting the floor removes a
spurious directional signal and leaves a clean null. (The subtraction is an estimate, not per-mapping
accounting, which `/usr/bin/time` cannot provide.)

**Warm shows no regression**, and warm major faults are 0 in every run, which also confirms the warm
phase really was warm.

**Wall clock is not usable from this box and none of these figures should be quoted.** Three peer lanes
were building concurrently; within-arm spread (22.4-31.3 s baseline, 23.4-32.6 s patched) swamps any
between-arm difference. An earlier run made the confound unambiguous: in **both** orderings the arm that
ran **first** was faster (round 1, baseline first: 20.9 s vs 50.1 s; round 4, patched first: 38.8 s vs
22.4 s). That is load drift, not an arm effect, and it is exactly what alternating the order exists to
expose — run all of one arm then all of the other and that same drift would have produced a
clean-looking, entirely false "patched is 2.4x slower".

## Why this host cannot exhibit the effect

The corpus device is **`Amazon Elastic Block Store`** (recorded in `ab/host.txt`), not local
instance storage — a c7i has none. Measured directly:

```
$ sync; echo 3 > /proc/sys/vm/drop_caches
$ dd if=.../nb-1-big-Data.db of=/dev/null bs=1M
2774760422 bytes (2.8 GB) copied, 21.0097 s, 132 MB/s      major_faults=1
$ cat /sys/block/nvme1n1/queue/read_ahead_kb
128
```

**132 MB/s is gp3 baseline throughput.** At that rate the kernel's default 128 KiB read-ahead
window already saturates the device, so the read is bandwidth-bound and fully pipelined before
any advice is issued — there is no synchronous-fault stall for `MADV_WILLNEED` to remove. The
near-zero major-fault delta is exactly what that predicts.

An i4i is the opposite regime: local NVMe at multiple GB/s, where a 128 KiB window at low queue
depth does **not** saturate the device, and issuing whole-file read-ahead at open raises queue
depth substantially. That is where the lever would pay, and it is why AC1 names i4i specifically.

So the correct statement is not "we could not get an i4i". It is: **on EBS at 132 MB/s the
measurement has no headroom to detect the effect in either direction**, and a null result here
carries no information about the i4i case.

## What this DOES establish

These hold about the change **as built and measured**, before it was reverted:

1. The flip was correct at the syscall boundary: `MADV_WILLNEED` is issued once, on the scan
   mapping, over the whole file; the #2210 point mapping is untouched; `MADV_SEQUENTIAL` is
   never issued by either arm (`ab/construction.md`).
2. **No regression** on an EBS-backed deployment, warm or cold, on either signal.
3. `issue_1143_mmap_prefetch_tail_guard.rs` is green.

## What it does NOT establish — residuals

1. **AC1's cold-p99 improvement on i4i: UNMEASURED.** Needs the rig lane.
2. **`ws0-scan-bench` reports whole-scan wall seconds, not a within-scan latency
   distribution — there is no p99 in this data at all**, on any host. AC1 as literally worded
   needs a harness that records per-operation latencies; this one cannot produce one.
3. **The fixture is uncompressed** (#1406), so the compressed-chunk read path a field scan uses
   is not exercised.
4. **Multi-SSTable read-ahead pressure is not covered.** `WILLNEED` is issued over the entire
   file in one call at open. For one 2.58 GiB file on a 30 GiB box that is fine and it fits; a
   full-ring scan opening many large SSTables concurrently is a different proposition, because
   one file's read-ahead can evict another's hot pages — the warm-regression direction AC1
   forbids. A single-file A/B cannot see this. It should be part of the rig validation.
