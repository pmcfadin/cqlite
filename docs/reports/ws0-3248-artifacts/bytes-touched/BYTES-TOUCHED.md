# Bytes-touched-per-row differential — `do_get` vs bare scan at S=1

The owner's added deliverable ("one measurement, two customers: it locates the footprint delta this
issue's slope-vs-level verdict on encode needs, AND it is the baseline #3288 cannot start without").

**Measured at the L2 boundary, because the LLC boundary is unavailable on this host** — see
`../raw/counter-availability-census.md`. Warm, 3 reps, release binaries, same pinning as AC0. Events:
`cycles,instructions,l2_lines_in.all,mem_inst_retired.all_loads,mem_inst_retired.all_stores`, all at
100% enabled (no multiplexing).

## The result

| quantity | bare scan | Flight `do_get` | Flight / scan |
|---|--:|--:|--:|
| **L2 lines in x 64 B — bytes/row** | **4,578** | **23,745** | **5.19x** |
| retired loads/row | 7,018 | 8,558 | 1.22x |
| retired stores/row | 4,962 | 5,927 | 1.19x |

Per-rep spread (max−min over median): L2 bytes/row — scan 8.2%, Flight 2.3%; loads and stores under
2.3% on both arms.

Against the corpus's **693.69 bytes/row on disk**: the bare scan pulls **6.6x** that into L2, and the
Flight arm **34.2x**.

## What it says, and it is a LOCALITY result rather than a work-volume one

**The Flight arm performs only ~20% more memory ACCESSES but pulls 5.19x more BYTES into L2.**
Loads/row 1.22x and stores/row 1.19x against L2-traffic 5.19x is the signature of the *same order of
accesses missing far more often*: the Flight arm's working set does not fit where the bare scan's
does.

That distinction matters for how the encode cost should be read. A 5.19x footprint differential with
a 1.2x access differential is **not** "the Flight arm does five times the work" — it is "the Flight
arm's accesses are five times more expensive to serve".

## It supplies the mechanism for two earlier findings, which were left as candidates

**1. AC0's arm-specific IPC divergence.** AC0 measured Flight IPC falling ~10% while the bare scan's
did not (1.3601 vs 1.5228 recorded; bare scan unmoved at −0.8%). An IPC drop is a stall result, and
5.19x the L2 traffic per row is a stall cause. The two are consistent, and the footprint measurement
is the independent quantity — it was taken with different events, in a different run.

**2. AC2's "same shared code costs +21.5% more on the Flight arm".** The differential found the
shared bucket at 7,348 cyc/row on the bare scan and 8,926 on the Flight arm — identical code,
+1,578 cyc/row. Cache pressure was named there as a candidate cause with no evidence. **This is the
evidence**: if Arrow buffers and the intermediate `Vec<Option<T>>` per column per batch are evicting
the shared row-build path's working set, that path executes the same instructions with more stalls.

**Stated as support, not proof.** L2 traffic is correlated evidence for a causal story about shared-code
slowdown; it does not isolate it. What would isolate it is an LLC-level footprint measurement plus a
cache-partitioning experiment, and the first of those is exactly what this host cannot do.

## For #3288 specifically

#3288's stated target is to "fit ~1/6 of 54 MiB LLC" — a constraint at the **LLC** boundary, which
**cannot be checked on this host** (`mem_load_retired.l3_hit`/`.l3_miss` return a silent hard `0`;
`LLC-loads`/`LLC-load-misses` are `<not supported>`). So this measurement gives #3288 its per-row
footprint at the L2 boundary — **23,745 bytes/row on the Flight arm** — but cannot confirm or refute
the LLC-fit target. That is a hardware blocker (#3224), not an effort gap.

One arithmetic note #3288 will want: at 23,745 bytes/row of L2 traffic, a 128-row batch moves ~3.0 MB
and an 8,192-row batch would move ~195 MB. The batch size therefore dominates the footprint, so a
locality lever and the batch-size choice are not independent variables.

## Method note — the field this did NOT use

The obvious source for a bytes-per-row figure is the Flight record's own `bytes_total`, which sits
beside `rows_total` and is already named "bytes". **It measures allocated client-side Arrow array
memory including data-structure overhead, not bytes touched or transferred** — 12,661 bytes/row on
this corpus, an 18.25x expansion over on-disk, for reasons that have nothing to do with memory
traffic. See `../raw/loadgen-bytes-metric-mislabelled.md`. Using it here would have produced a
plausible, reproducible, internally consistent differential about the wrong quantity.
