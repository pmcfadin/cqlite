# AC4 — the `1,746 ns/row` vs `+4,697 cycles/row` reconciliation

**AC4 asks:** reconcile these explicitly, "stating the clock basis and whether `cycles/row` is
per-core or sibling-aggregate. If they cannot be reconciled, that is itself the finding."

**Answer, in the order the coordination lead asked for — conclusion first, then the open magnitude.**

## The conclusion, which holds under EVERY reading

**The gap is smaller than the encode region under all four available readings, so the encode region
is NOT wholly additive over the bare-scan arm.** A meaningful part of it is work the bare scan does
too. This conclusion does **not** depend on the clock basis, on the sibling-aggregate question, or on
resolving the overlap described below — those move only the *magnitude*.

| reading | gap (ns/row) | share of the 1,746 ns/row region | needs an external frequency? |
|---|--:|--:|---|
| **wall-clock, same session** (recommended) | **1,313.7** | **75.2%** | **no** |
| cycles ÷ Flight arm's observed sibling-aggregate rate | 802.2 | 45.9% | no |
| cycles ÷ scan arm's observed sibling-aggregate rate | 674.6 | 38.6% | no |
| cycles ÷ an assumed 3.6 GHz single-thread | 1,304.8 | 74.7% | **yes, and it has NO provenance** |

## 1. The clock basis, and the part of AC4 that was already answered

**`cycles/row` is SIBLING-AGGREGATE, and this is RECORDED — it is not an absence.** Stated in four
places and mechanically enforced:

* `abc-interleaved-2026-08-03.md:281-284` — "`cycles/row` here is **sibling-aggregate**: `perf stat
  -C 2,10` is CPU-WIDE over *both* SMT siblings of one physical core — not per-process, not
  per-thread, and not per-core-single-thread."
* `measurement-method.md:270` — "`cycles` is summed over BOTH SMT siblings of the pinned physical
  core."
* `baseline-summary.txt:48`, `abc-interleaved-2026-08-03.md:59,171`.
* Enforced by `perf stat -x, -e cycles,instructions -C 2,10` plus a self-grep refusing any
  `-p`/`--pid` form, and fail-closed sibling verification in both directions.

**This corrects an earlier statement made by this lane.** An interim note on the issue thread said
AC4 "turns entirely on (a) the occupancy-matched true clock and (b) the sibling-aggregate question."
**(b) was already settled in the record.** The correction matters in a specific way: because the
basis is sibling-aggregate, the honest conversion uses the arms' **observed aggregate cycle rates**,
which are derivable from the recorded figures themselves — so **two of the four readings need no
external frequency at all**, and the clock question shrinks to a cross-check rather than the crux.

Observed sibling-aggregate cycle rates, re-derived here from the recorded medians:

* bare scan: 370,134.3 rows/s x 18,813.92 cycles/row = **6.964e9 cycles/s**
* Flight `do_get`: 249,041.1 x 23,511.31 = **5.855e9 cycles/s**

## 2. The recommended reading needs no clock at all: compare like currencies

The region's figures are **wall times**; the gap is **cycles**. Rather than convert cycles to
nanoseconds through an unprovenanced frequency, convert the *gap* to wall time using the **same
session's own rows/s** — the two arms' reciprocal throughputs:

    1/249,041.1 - 1/370,134.3 = 4,015.4 - 2,701.7 = 1,313.7 ns/row

That is **75.2%** of the 1,746 ns/row region, leaving the region
**432.3 ns/row larger than the gap it is supposed to explain.** It assumes nothing
about frequency, occupancy, SMT or thread count.

**And it is a conservative comparison in the direction that matters**, which strengthens the
conclusion: the 1,746 ns/row region was measured **in-process** with no gRPC transport, while
the 1,314 ns/row wall gap is taken across the **real loopback RPC**. So the gap includes
*more* work than the region does — and the region still exceeds it.

## 3. Why the residue does not reconcile, which is the AC4 finding

**`1,746 ns/row` is not a valid single-currency total.** It is `313.0 + 1,432.9`, a **sum of
two wall-time measurements taken on two DIFFERENT THREADS that run CONCURRENTLY**:

* `stream_encode` = 1,432.9 ns/row wraps `flush_buffer`, on the **`spawn_blocking`** merge/encode
  thread (`cqlite-flight/src/streaming.rs:433`);
* `stream_encode_framing` = 313.0 ns/row wraps the encoder poll, on the **async gRPC task** —
  a different thread (`streaming.rs:539`).

The source artifact says so itself (`abc-interleaved-2026-08-03.md:302-306`): the region's ns/row
"are **wall times on concurrent pipeline threads (they overlap and do not sum** to the `stream`
phase)". **Adding two overlapping concurrent wall times inflates the apparent region**, and comparing
that inflated sum against a wall-clock throughput gap — which is inherently a single timeline — is not
a like-for-like comparison. That is a sufficient, named mechanism for the residue, and it is the
answer AC4 asks for when it says "if they cannot be reconciled, that is itself the finding".

## 4. Four further incomparabilities between the two figures, each verified

Not needed for the conclusion, but they bound how precisely the residue can ever be closed:

1. **Different surface.** The sub-phase figures come from an in-process `svc.do_get(Request::new(..))`
   call (`cqlite-flight/tests/issue_3096_framing_subphase.rs:98,151`) — no gRPC transport, no tonic
   body memcpy, no client on separate CPUs. The perf arm is the real loopback RPC with a separate
   `flight-loadgen`. So `313.0 ns/row` **excludes** transport the perf arm pays.
2. **Different binary.** The perf table was taken with metrics **OFF**; the sub-phase run requires
   `--features observability-testing`. Its overhead is a **bound** from poll count, not a measured
   delta.
3. **Build profile not recorded** for the sub-phase run — `--release` is recorded for the perf table
   only. Nothing in the artifacts resolves whether the sub-phase figures are release or debug.
4. **No temperature, pinning, rep count, spread or median** for `1,432.9` / `313.0` — single
   values from one run, against 3-rep medians on the perf side.

## 5. What is NOT recorded, stated plainly

* **The cycles-to-nanoseconds conversion.** The "~3.6 GHz" at `abc-interleaved-2026-08-03.md:286` has
  **no provenance** — no tool, no timestamp, no artifact. Verified: the event list is
  `cycles,instructions` **only**, with no `task-clock`, `cpu-clock` or `ref-cycles`, so neither
  frequency nor per-thread busy fraction is reconstructible from anything recorded. Two of the four
  published conversion rows rest entirely on that number. (Measured on this box today for
  orientation only: 3.41-3.46 GHz at ~0.97 occupancy under load, and #3299 records 3.509 GHz
  quiescent — so ~3.6 GHz is plausible but unsourced and on the high side. This work derives its own
  clock through `scripts/perf/ws0_clock.py`, which refuses to print one at unverified occupancy.)
* **Thread counts / concurrency settings** — recorded nowhere in either results JSON. The bare scan is
  "single-threaded" in prose only.
* **Binary digests for the baseline session** — absent; the artifacts call this "a rig gap, not a
  footnote". md5s exist only for the later re-measurement.

## 6. How the `+4,697` itself was computed, which is worth knowing

`23,511.31 - 18,813.92 = 4,697.395` cycles/row (+25.0%), i.e.
**per-arm median minus per-arm median** — **not** a paired within-round difference. The interleaving
rule was written *after* this session, which ran its arms sequentially. The two arms also carry
different total row denominators (12,000,000 vs 36,000,000), though each rep is normalized before the
median is taken.

