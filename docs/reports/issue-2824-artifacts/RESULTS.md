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

Every phase runs with `CQLITE_DISK_ACCESS_MODE=mmap` pinned, and the floor phase additionally with
`CQLITE_PREFETCH=off` — both recorded in `ab/host.txt`. The env pin matters twice: those variables
override the compiled default, so an inherited value would give both arms the same policy and compare
nothing; and at `auto` the patched binary's floor run would issue whole-file `MADV_WILLNEED` whose
asynchronous read-ahead outlives the process and pre-warms the cold phase that follows it, biasing the
patched arm only.

**4 rounds** (even by requirement, so each arm runs first exactly twice) x 2 arms x **3 phases**, page
cache dropped immediately before both the floor and the cold phase, each phase a separate run with its
own `/usr/bin/time`. Corpus: the `ws0.events` fixture, `Data.db` 2,774,760,422 bytes (~630,000 pages).
Both arms built from ONE tree differing by one match arm and verified by `strace` to differ by exactly
one `madvise` call — see `ab/construction.md`.

The **floor** phase is the same binary run cold with `--setup-only`: it opens the reader and reads the
index/summary but performs no scan, so its fault count is the non-scan cost of starting this process on
a cold cache. `scan_major_faults = cold - floor`.

| signal | baseline median [values] | patched median [values] |
|---|---|---|
| **scan-attributable major faults** (bounds, does not resolve — see residual) | **5.0** [5, 5, 4, 5] | **3.0** [3, 3, 4, 3] |
| raw cold major faults | 54 | 53 |
| floor major faults (startup only, `prefetch=off`) | 49 | 49.5 |
| warm major faults | 0 | 0 |
| page-cache drain before every cold phase | 16/16 `DRAINED` | 16/16 `DRAINED` |
| work validated identical every phase | `rows=4000000 cells=48000000` | same |

**The attribution residual, stated because it is the same size as the difference.**
`scan_major_faults = cold(auto) - floor(off) = scan(auto) + [setup(auto) - setup(off)]`. The bracketed
term is not zero for an arm whose `auto` setup issues advice, and the advice census below shows the
patched arm does exactly that (`WILLNEED=1` on a `--setup-only` run at `auto`). It is arm-asymmetric and
of the same order as the 2-fault median difference, so **this column bounds the scan cost; it does not
resolve it.** Read it as "single digits in both arms", never as a between-arm signal. The floor is
measured at `off` deliberately — at `auto` the patched floor's asynchronous whole-file read-ahead
outlives its process and pre-warms the cold phase that follows, a far larger and equally
one-sided bias. Both options carry a residual; this one is smaller and is declared.

Advice census (`ab/advice-census.txt`, taken after all rounds so it cannot pollute one):

```
baseline: WILLNEED=0 RANDOM=1 SEQUENTIAL=0 DONTNEED=4
patched:  WILLNEED=1 RANDOM=1 SEQUENTIAL=0 DONTNEED=4
```

The arms differ in exactly one issued advice, `MADV_WILLNEED`; the #2210 `MADV_RANDOM` point mapping is
present and untouched in both; **neither arm ever issues `MADV_SEQUENTIAL`** (#1143), verified at runtime
rather than only by unit assert. The four `MADV_DONTNEED` calls are the runtime releasing thread stacks —
identical in both arms, and not from the reader.

## Reading it

**Scan-attributable major faults are single digits in both arms — medians 5.0 and 3.0 across ~630,000
file pages.** The 2-fault gap is more consistent across rounds than in earlier runs (baseline 5,5,4,5;
patched 3,3,4,3), but it is **the same order as the declared residual below and is not reported as a
signal**: `[setup(auto) - setup(off)]` is arm-asymmetric and unquantified here, and 2 faults out of
~630,000 pages is operationally nil either way. What the data supports is "no large effect"; it does not
resolve a difference this small, and no claim rests on one. The
kernel's default read-ahead was already converting essentially everything to minor faults in both arms.
There was nothing for `MADV_WILLNEED` to convert.

**The attribution matters, and it corrected an earlier misreading of this same data.** Raw cold counts
are 54 vs 53, which looks like a small improvement. It is not: the difference sits in the
**startup floor** (51 vs 48 — the two binaries differ slightly in size, so they fault in a slightly
different number of executable pages), i.e. faulting in the executable and its shared libraries,
which are cold too because the global page cache is dropped, and `%F` counts them. `%F` is per-process,
so it is immune to the neighbours; it is **not** isolated to the scan mapping, and reporting it
unqualified attributes process-startup faults to the scan. Subtracting the floor removed a spurious
directional signal and left a clean null. The subtraction is an estimate, not per-mapping accounting,
which `/usr/bin/time` cannot provide.

**The page cache was verified drained before all 16 cold phases** (`ab/drain.csv`), so the cross-arm
hazard — one arm's whole-file read-ahead outliving its process and surviving the next `drop_caches` —
did not occur in this run. That is measured, not assumed; on a busier device it could report `TIMEOUT`
instead, and the artifact would say so.

**Warm shows no regression**, and warm major faults are 0 in every run, which also confirms the warm
phase really was warm.

**Cold wall clock is not usable from this box and should not be quoted.** Three peer lanes were building
concurrently; baseline's four cold runs were 52.6, 22.9, 33.3 and 22.5 s — a single arm spanning more
than 2x. An earlier run made the confound unambiguous: in **both** orderings the arm that ran **first**
was faster (baseline-first 20.9 vs 50.1 s; patched-first 38.8 vs 22.4 s). That is load drift, not an arm
effect, and it is what alternating the order exists to expose — run all of one arm then all of the other
and the same drift yields a clean-looking, entirely false result. Note the alternation only balances for
exactly two arms and an even round count, which the harness now enforces; an earlier 3-round run of this
same A/B was unbalanced (baseline first twice, patched once) and has been superseded by this one.

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
