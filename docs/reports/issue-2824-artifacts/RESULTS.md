# Issue #2824 slice 1 — cold/warm A/B result

**Verdict: NO detectable effect, and NO regression, on this host — and this host is
structurally incapable of exhibiting the effect AC1 asks about.** The lever is neither
validated nor invalidated here. AC1's cold-i4i clause remains **UNMEASURED**.

Raw artifacts: `ab/summary.csv`, `ab/host.txt`, `ab/construction.md`,
`ab/strace-madvise.{baseline,patched}.txt`. Harness: `cold-warm-ab.sh`.

## What ran

3 rounds x 2 arms x 2 phases, arm order alternating per round, page cache dropped
immediately before every cold phase, each phase a separate single-pass run with its own
`/usr/bin/time`. Corpus: the `ws0.events` fixture, 2,779,188,457 bytes, `Data.db`
2,774,760,422 bytes. Both arms built from ONE tree differing by one match arm and verified
by `strace` to differ by exactly one `madvise` call — see `ab/construction.md`.

| phase | signal | baseline median [min-max] | patched median [min-max] | delta |
|---|---|---|---|---|
| cold | **major faults** | 52 [51-53] | 50 [50-52] | -3.8% |
| cold | wall secs | 23.76 [22.54-31.01] | 22.22 [21.35-30.59] | -6.5% |
| warm | **major faults** | 0 [0-0] | 0 [0-0] | 0 |
| warm | wall secs | 18.95 [18.18-20.18] | 18.53 [17.46-19.19] | -2.2% |

## Reading it

**Major faults is the primary signal and it says nothing happened.** It is per-process and
therefore immune to the neighbours, and it is the mechanism `MADV_WILLNEED` acts on: the
advice exists to convert synchronous major faults on the reading thread into kernel-initiated
asynchronous read-ahead. A 52 -> 50 median difference is **2 faults across ~630,000 file
pages**. There is nothing there.

**Warm shows no regression**, on either signal. Warm major faults are 0 in every single run,
which also confirms the warm phase really was warm.

**Wall clock is not usable from this box and the -6.5%/-2.2% figures must not be quoted.**
`loadavg` was 23.9 at start and 22.3 at end, peaking above 60, against 16 cores — three peer
lanes were building concurrently. Within-arm spread (22.5-31.0 s baseline, 21.4-30.6 s
patched) swamps the between-arm difference. An earlier run made this unambiguous: in **both**
orderings the arm that ran **first** was faster (round 1, baseline first: 20.9 s vs 50.1 s;
round 4, patched first: 38.8 s vs 22.4 s). That is load drift, not an arm effect, and it is
exactly what alternating the order exists to expose — run all of one arm and then all of the
other, and that same drift would have produced a clean-looking, entirely false "patched is
2.4x slower".

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

1. The flip is correct at the syscall boundary: `MADV_WILLNEED` is issued once, on the scan
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
