# WS0 #3299 — the bare-scan scaling curve C(S), and the box-level target it derives

**Status: DRAFT.** AC1 and AC2 are measured and complete. AC3 is DEFERRED
(instrument unavailable, per the issue's pre-registered AC5) with the L1d partial
reported. Two sections are placeholders pending runs the delivery lead owns:
the frequency calibration and phase 2. **AC4 is DISCHARGED** in both
halves (§9): the box-level target, and "remaining to target" at **+74.8%**, from
the first same-corpus same-session bare-scan-vs-`do_get` figures in the program.

Host: `i-04ac0a860eef7f241`, `c7i.4xlarge`, Intel Xeon Platinum 8488C, 16 logical
/ **8 physical** cores, 1 NUMA node, `perf_event_paranoid = -1`, kernel
`6.17.0-1019-aws`. Raw artefacts: `docs/reports/ws0-3299-artifacts/`.
**Measurement only — no production code was changed.**

---

## 1. Executive summary

**The bare scan scales far better than `do_get` does, and that raises the target.**
On six physical cores the bare scan sustains **2,732,817 rows/s** (median of 3,
spread 0.74%), which is **93.5%** marginal efficiency against the most one
physical core achieves. #3217 measured `do_get` at **71.1%** marginal efficiency
at the same core count. The issue body's ~1.35M estimate for the box-level target
assumed bare scan would scale like `do_get`; it does not, and the derived target
is **+56% above that estimate**.

**The decay that does exist is not extra work and is barely cache-visible.**
From S=1 to S=6, `instructions/row` is **flat (×0.984)**, `L1-dcache-loads/row`
is **flat (×0.984)**, `L1-dcache-load-misses/row` is **flat (×0.979)**, and
`cycles/row` rises only **×1.041**. A 4% cycles/row rise across the entire range
is the quantitative form of "bare scan does not decay".

**Two standing caveats.** S=1 and S=6 have measured peaks (S=6's bracketed by a
dedicated extension, §4); **S=2–S=5's best-N values sit at the top of their tested
ladders and are lower bounds**. And this rig does not control drift (§6) — the
target's precision is 0.74% within-session but −3.1%/−1.0% across ~1.5–2 h.

---

## 2. What was measured, and how

25 grid points — S ∈ 1..6 physical cores × an N ladder of concurrent bare-scan
streams — at **3 reps each, 75 reps total**, 60 s aligned window per rep.

- **Arm**: `cqlite_core::Database::execute_streaming`, the same surface and setup
  path as the #3096/#3272 rig's `ws0-scan-bench` arm. Equivalence measured on one
  core in one session: **−1.39%**, of which the harness's own known-low
  attribution bias accounts for 0.06 pp. The residual **−1.32% is within
  resolution** — it is inside `ws0-scan-bench`'s own 2.1% three-pass spread and
  below the mission doc's ~1.4% between-binary noise floor (§0). It is not a
  claim that the two arms agree to 1.3%.
- **Corpus B** — the #3096 measurement corpus: 4,000,000 rows, 40,000 partitions,
  12 cells/row, `Data.db` 2,774,760,422 B, **693.69 B/row, UNCOMPRESSED**, no
  `CompressionInfo.db`. Identity verified before every run.
- **S** sets the `perf stat -C` set: the union of S **complete** SMT sibling
  groups (`(c, c+8)` on this box, read from `thread_siblings_list`). **N** streams
  are pinned to that same union. Two physical cores are left unpinned as headroom.
- **Warm protocol**, containment via `test-data/scripts/perf-run-contained.sh`,
  S-order rotated per round.

### The aligned window

Rows and counters are taken over **one** interval, and the interval's contents are
verified rather than assumed. The window is opened and closed through perf's
**control FIFO** (`-D -1 --control fifo:ctl,ack`), so counting brackets exactly
`[T0, T1]`. Rows are attributed to that same interval by **differencing progress
records the workers actually emitted** — never interpolated, never a rate
assumption. Every worker must be observed producing rows **before T0 and after
T1**, which is what makes the window genuinely N-concurrent.

The residual bias has a stated direction: rows are under-counted against a full-
window denominator, so **rows/s is biased LOW and per-row counters HIGH**, by at
most the published shortfall (max **0.0909%** over all 75 reps).

**That the two windows coincide is measured, not argued**: under CPU-wide
counting `task-clock` must read `window × nCPUs`, and it does, to at most
**4.13e-05** across all 75 reps.

---

## 3. AC1 — the curve

Full table with all columns, per-point spreads and bracketing verdicts:
`docs/reports/ws0-3299-artifacts/sweep/CS-table.md`.

| S | best aggregate rows/s | spread | N@peak | per-scan p50 | own N=1 | **marg. eff. vs 1-core peak** | marg. eff. vs 1-core N=1 | cycles/row † | instr/row † | IPC | peak status |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|
| 1 | 487,213 | 4.3% | 2 | 243,729 | 358,869 | **1.000** | 1.358 | 14,229.4 | 23,143.3 | 1.626 | plateau |
| 2 | 933,197 | 3.0% | 8 | 112,403 | 265,471 | **0.958** | 1.300 | 14,863.6 | 22,810.5 | 1.535 | edge-truncated |
| 3 | 1,290,610 | 1.7% | 8 | 162,619 | 254,991 | **0.883** | 1.199 | 15,821.9 | 23,441.8 | 1.482 | edge-truncated |
| 4 | 1,826,004 | 0.5% | 16 | 115,262 | 249,531 | **0.937** | 1.272 | 15,055.1 | 22,911.4 | 1.522 | edge-truncated |
| 5 | 2,177,475 | 1.7% | 16 | 133,267 | 231,424 | **0.894** | 1.214 | 15,572.9 | 23,262.5 | 1.494 | edge-truncated |
| 6 | **2,732,817** | 0.7% | 24 | 113,913 | 239,223 | **0.935** | 1.269 | 14,819.6 | 22,769.4 | 1.536 | edge-truncated |

**† Basis**: every per-row counter is summed over **all pinned hardware threads**
(2S logical CPUs) — the set `perf stat -C` counted, identical at every N for a
given S. It is not a per-hardware-thread figure. IPC is basis-invariant.

**Both denominators are published and neither is silently chosen.** Reference B
(S=1's own peak, 487,213 at N=2) is **primary**: it is the most the engine
achieves on one physical core, it is the conservative choice, and — a second,
measured reason #3217 did not have — it is the **more precise** one. Reference A
(S=1 at N=1) sits in the noisiest regime in the grid (4.2% median spread at N=1
versus 0.45–0.74% at N=16–24), so the column normalised to it inherits that noise.

**The per-arm N=1 decline reproduces #3217 §3.2 on a different arm.** A single
stream given more cores gets *slower*: 358,869 → 265,471 → 254,991 → 249,531 →
231,424 → 239,223 as S goes 1→6. This is why self-normalising each arm to its own
N=1 would flatter the wide points, and why both denominators are mandatory.

**N is load-bearing, more than expected.** At S=1, N=2 delivers 487,213 against
N=1's 358,869 — **+35.8%** from the second hardware thread of one physical core,
against #3217's +16.7% on `do_get`. A single-threaded streaming bare scan leaves
a large fraction of a physical core's execution capacity unused. Consequently
"rows/s per physical core" and "rows/s per hardware thread" diverge by a large
factor on this arm, and neither may be quoted without naming which it is.

**The S=3 and S=5 dips are a ladder artifact, not physics.** Efficiency rising
from 0.883 (S=3) to 0.937 (S=4) is physically implausible. The cause is visible in
the ladder: S=3 and S=5 topped out at 2.7S and 3.2S while S=2/4/6 reached 4S.
**Do not quote the S=3 or S=5 efficiencies as results** until their ladders are
equalised (§4).

---

## 4. Peak bracketing — which best-N values are real

Pre-registered rule, fixed before the data was seen: a peak is **bracketed** when
some tested N above it is lower by more than the relevant point's **own** spread;
**plateau** if within spread (the lower N is then reported — same throughput,
cheaper); **edge-truncated** if nothing above it was tested.

**Two of the six are resolved, and they are the two the acceptance criteria
consume.** **S=1** is a plateau at N=2 (N=4 is 1.88% lower, inside its 3.41%
spread). **S=6** is **bracketed** at N=24 by a dedicated extension — see below.

**S=2 through S=5 remain edge-truncated**: their best N is the largest tried, so
each is a **lower bound on that S's best**, not a measured peak. They shape the
curve; no acceptance criterion reads them. Extending them was deliberately
dropped in favour of resolving S=6, which is AC2's input.

### FINAL VERDICT — S=6's peak is BRACKETED at N=24, a clean interior maximum

Extensions A and B re-measured the incumbent **N=24 interleaved with its
candidates in the same rounds**, so every comparison below is contemporaneous and
drift-robust by construction. With all 3 reps at each point:

| N | reps | median rows/s | spread | vs N=24 |
|--:|--:|--:|--:|--:|
| 16 | 3 | 2,477,956 *(main grid)* | 0.38% | −8.4% |
| **24** | 3 | **2,705,485** | **0.64%** | — |
| 32 | 3 | 2,652,863 | 0.67% | **−1.95%** |

The pre-registered rule: *a peak is bracketed when the best N has a tested N above
it lower by more than the point's own spread.* **N=32 is 1.95% below N=24,
exceeding the larger of the two spreads (0.67%)** — so the curve has demonstrably
turned over. N=16 is below N=24 as well, so this is a **clean interior maximum**:
it rises 16→24 and falls 24→32.

**S=6 is therefore `bracketed`** — not `plateau`, not `edge-truncated`. The
earlier plateau reading came from single reps (extA round 1 put N=32 at +0.68%,
extB round 1 at −1.95% — the sign flipped); replication resolved it, which is
exactly what ≥3 reps are for and a good illustration of why AC1 demands them.

**One precision about which halves are contemporaneous.** The *fall* (24→32) is
measured within one interleaved run and is the load-bearing half. The *rise*
(16→24) pairs the main grid's N=16 against extB's N=24, i.e. across sessions —
but the measured session offsets (−3.1%, −1.0%) are far smaller than the **+9.2%**
rise, so it is robust to them. Said plainly rather than left for a reader to spot.

**N=40 was considered and dropped**, not overlooked: the function is decreasing
past N=24 and N≥48 is unmeasurable (below), so no point at 40 can change the
maximum. Running it would have added box time and no information.

**Separately — and no longer load-bearing for the peak — N≥48 is not a measurable
configuration here.** 48 independent scan processes exceed the 24 GiB containment
cap and the scope is OOM-killed. This is a property of **this harness's
N-independent-process design** (each worker holds its own `Database`, readers and
buffers, because the arm under test is *N independent bare scans*) — **not of
CQLite, and not of production `do_get`**, which shares one server process. Nobody
deploys 48 independent scanners. It is reported because it bounds what future
work can probe on this box, not because the peak rests on it.

## 5. AC2 — the derived box-level target

```
target = bare_scan_aggregate(S=6, best-N) / 1.3
       = 2,732,817 / 1.3
       = 2,102,167 rows/s
```

Inputs, all from this sweep: S=6, N@peak=24, median of 3 reps, **spread 0.74%**
(so the input's own precision is sub-1% — the grid-wide 2.27% median is a summary
across heterogeneous points and is *not* the error bar here).

Byte bases, per mission §1 — never a bare rows/s:

| | rows/s | logical / uncompressed |
|---|--:|--:|
| measured S=6 aggregate | 2,732,817 | 1,895.7 MB/s |
| derived target | 2,102,167 | 1,458.3 MB/s |

The division is `/ 1.3`, not `× 1.3`: mission §6 is normative ("within ~1.3× of")
and §0 does the arithmetic the same way.

**Status of this figure: FIRM.** S=6's peak is **bracketed by measurement** at
N=24 (§4) — a clean interior maximum, with the curve observed rising into it and
falling out of it. Every earlier "lower bound of unknown tightness" hedge is
retired: this is a measurement with a stated error bar, not a floor.

**An independent confirmation two hours later**, at the AC2 configuration:

| source | S=6, N=24 | derived target (÷1.3) |
|---|--:|--:|
| **main grid** — the campaign of record (25 points × 3 reps, S-order rotated) | **2,732,817** (spread 0.74%) | **2,102,167** |
| extension B — targeted confirmation, ~2 h later | 2,705,485 (spread 0.64%) | 2,081,143 |

The main grid's figure is the **headline**, because it comes from the campaign of
record. Extension B reproducing it to **−1.0%** two hours later is a stronger
statement about the target's reliability than the within-session 0.64% alone,
which speaks only to dispersion inside one session (§6).

It is **+56%** above the issue body's ~1.35M estimate, which assumed bare scan
would scale at `do_get`'s 71%.

**The `/ 1.3` is a division, and the multiplication error was not an isolated
slip.** The issue body's "1.3 ×" had **propagated into the tracking document** —
the delivery lead found and corrected it in **two** places in
`0.17-throughput-mission.md` (§0 and the #3299 open-work row). Multiplying would
put the target *above* our own measured ceiling, i.e. unreachable by
construction, so the direction of the error is self-evidencing once written out.

---

## 6. Resolution, and what may be called a value

**Spread is strongly N-dependent** — median over the S values measured at each N:

| N | 1 | 2 | 4 | 8 | 16 | 24 |
|---|--:|--:|--:|--:|--:|--:|
| median spread | 4.20% | 4.27% | 3.36% | 1.98% | 0.45% | 0.74% |

A single stream's throughput turns on scheduler placement and one core's
frequency excursions; an aggregate over sixteen streams averages those away. So
**there is no single error bar for this grid**. Every difference stated anywhere
in this report is compared against the spread of the points being differenced,
and where a difference falls inside that spread it is reported as *within
resolution* rather than as a value. Grid-wide (median 2.27%, max 8.20%) is
reported only as a summary for judging the rig.

### Within-session dispersion is NOT across-session reproducibility

Extension A re-measured S=6/N=24 about **1.5 h** after the main grid, at the
exact configuration AC2 consumes:

**Three independent reads** of S=6/N=24 now bound session drift directly:

| read | S=6, N=24 | vs main grid |
|---|--:|--:|
| main grid (median of 3) | 2,732,817 | — (within-session spread **0.74%**) |
| extension A, ~1.5 h later | 2,647,966 | **−3.1%** |
| extension B, later still | 2,705,485 | **−1.0%** |

Three points spanning **3.1%** with **no consistent direction** is far better
evidence than a single pair — a pair cannot distinguish drift from one bad
measurement.

**These two numbers measure different things and must not be conflated.** The
0.74% is dispersion *within* a session; the −3.1% is reproducibility *across*
sessions. Quoting the former as the figure's uncertainty would understate it by
4×, and this is an **in-band** measurement of that gap — at the AC2-relevant
point, in this rig, rather than the rig's recorded 370,134 → 333,206 anecdote.

**And the drift is uncontrolled, not correctable**: its direction *reversed*
between the two scales — 7 of 9 round-over-round deltas rose *within* the
session, while both across-session reads *fell*, and by different amounts
(−3.1%, then −1.0%) rather than monotonically. A drift with an inconsistent sign
cannot be modelled out, only disclosed. So: **no cross-session absolute may be
derived from these figures**, and the target's precision must be stated with its
scope — 0.74% within-session, −3.1% across ~1.5 h.

The mission doc's ~1.4% between-binary floor (§0) is a **different** quantity —
it concerns comparing separately-built binaries, which no point in this grid
does (one binary throughout). It is cited only for the cross-build equivalence
control in §2.

**Drift is present and is not controlled.** Round-over-round direction counts are
recorded in the artefact table as **inert data explicitly uncontrolled for
drift** (`scripts/perf/README.md`); no round-major claim is made and the S-order
rotation is a reasonable ordering, **not a verified control** — what it does is
distribute drift across points rather than concentrate it in one S, which is why
the curve's *shape* survives a drifting session even though no absolute number
does. The rig's own recorded ~10% same-day drift (370,134 → 333,206 rows/s with
nothing changed) is the standing reason **no cross-session absolute** may be
derived from these figures. And a median of 3 draws from a drifting
distribution: it reduces drift, it does not remove it.

---

## 7. AC3 — DEFERRED, with the L1d partial

**No LLC instrument on this box produces a count.** Census (`artifacts/host/`),
re-run on this instance with #3224's 2 GiB serial-dependency pointer chase as a
positive control: `LLC-loads`/`LLC-load-misses` are `<not supported>`;
`cache-references`, `cache-misses`, `mem_load_retired.l3_{miss,hit}`,
`longest_lat_cache.{miss,reference}` and raw `r4f2e`/`r412e` all return a **hard
0 at 100.00% enabled** — on a workload that, in the same runs, produced
120,303,664 L1-dcache-load-misses. Zero L3 references there is physically
impossible, so those are **unavailable instruments, not measurements of zero**.
No uncore PMU exists on this guest and `/sys/fs/resctrl` is absent, so there is
no substitute route. AC3 is deferred per the issue's pre-registered **AC5**. The
harness refuses those event names *as input*, so no dead counter's 0 can enter
any table.

**The available partial** — `L1-dcache-loads` and `L1-dcache-load-misses` are
real here, and they are exactly the counters #3224 reported flat:

| per-row counter † | S=1, N=2 | S=6, N=24 | ratio |
|---|--:|--:|--:|
| instructions/row | 23,143.3 | 22,769.4 | ×0.984 |
| L1-dcache-loads/row | 5,796.9 | 5,702.8 | ×0.984 |
| L1-dcache-load-misses/row | 109.72 | 107.47 | ×0.979 |
| cycles/row | 14,229.4 | 14,819.6 | ×1.041 |
| IPC | 1.6264 | 1.5363 | ×0.945 |

Flat instructions/row with flat L1d says the decay is **not extra work and not
private-cache pressure**. This is *consistent with* #3224's private-caches-
untouched finding. **It does not discharge AC3**: without an LLC counter the
residual stays **unattributed**.

**Cross-everything caveat.** #3224's endpoints (instructions 38,856.8 → 38,685.6;
L1d loads 9,157.7 → 9,140.8; L1d misses 586.7 → 578.9; cycles ×1.191; IPC 1.2376
→ 1.0384) were measured on a **different host** (`i4i.metal`), a **different
corpus** (Corpus A, LZ4, 196.09 B/row) and a **different arm** (`do_get`). No
ratio between the two sets is computed. What is comparable is the *shape*, and
the shape differs in the interesting direction: bare scan's cycles/row
degradation at six cores is **~4%, against `do_get`'s ~19%**.

> **PLACEHOLDER — frequency calibration.** The C(S) discount conflates two
> effects: cache/memory contention, *and* the package clock falling as more cores
> go active. Both belong in a box-level aggregate, but only the contention part
> is addressable by a footprint lever, so charging turbo loss to #3288 would
> overstate its ceiling. Plan (written, not run):
> `artifacts/freq-calibration/PLAN.md`, using `msr/aperf` + `msr/mperf` with a
> positive control. If the instrument is degenerate the decomposition is
> **dropped**, not approximated. Note the total available is small either way:
> cycles/row rises only 4.1% across the whole range.

---

## 8. What the guards caught — the rig examined itself

Reported because a reviewer should see the bounds *worked*, not trust that they
would. **Zero of 75 reps were discarded**, and that is a measured result: 375
counter rows, none below 100.00% `pct_running`, no window failing its spanning
precondition, no shortfall over bound.

Three real defects were caught by guards firing on genuine data, not fixtures:

1. **`WINDOW_SHORTFALL`** — a fixed **row**-based progress interval samples less
   often as per-worker throughput falls (~N/S), so attribution degraded *along
   the axis being swept*, leaving the widest points worst-attributed. Fired at
   0.5393% against the 0.5% bound. Fixed **at source** (time-based sampling,
   invariant to throughput), not by relaxing the bound; shortfall is now <0.1% at
   every point. The smoke runs had passed because their shorter windows made the
   effect latent — the 60 s grid is what exposed it.
2. **`WINDOW_NOT_SPANNED` / `PERF_CSV_MISSING`** — a *second* concurrent sweep
   sharing a results root: its `rmtree` deleted a completed `perf.csv` and its
   `stop` file ended the first run's window mid-measurement, while every process
   still exited 0. The guards refused it; the file was empty at guard-read time
   and complete later, and the decisive confirmation was the **clock**: the
   collided rep ran at an effective 2.470 versus 3.291 on a clean re-run, a 25%
   package-wide turbo reduction that only occurs with many cores busy.
   `rep.py` now **exclusive-creates** its rundir, so this cannot recur silently.
3. **`WINDOW_SHORTFALL` again, on extension A's first attempt** — worker 16 at
   **0.7524%** against the 0.5% bound. The diagnostic was not the shortfall but
   the **window length**: 63.68 s against the main grid's 60.008 s, i.e. a
   **3.68 s control-FIFO ACK latency**. Probable cause: 34 orphaned workers had
   just been killed mid-scan, disturbing the page cache, and a single prewarm
   pass did not restore it. Fixed with `--prewarm-passes 2`, which touches only
   **pre-measurement** cache state and therefore costs nothing in comparability
   — no counted interval, no counter and no attribution rule changed.

   **Why this one is worth recording: the window length was only visible because
   the harness records `window_ns` per rep instead of assuming the requested
   duration.** A rig that logged "60 s" because it asked for 60 s would have
   shown a shortfall with no cause attached, and the natural (wrong) response
   would have been to loosen the bound.

4. The guard that mattered most is the one that **did not misfire**. A rig that
   had tolerated an empty counter file would have published a rep with no
   counters. The bug was always upstream of the guard.

---

## 9. AC4 — DISCHARGED, both halves, both bases, all same-corpus

The first same-corpus, same-host, same-session figures for both arms in this
program. Corpus B throughout (uncompressed, 693.69 B/row); **no cross-corpus
division appears anywhere below.**

| | S=1 (per physical core) | S=6 (box) |
|---|--:|--:|
| bare scan | 487,213 | 2,732,817 |
| `do_get` | 243,536 | 1,198,673 |
| **`do_get` / bare scan** | **0.500** | **0.439** |
| **gap (bare ÷ `do_get`)** | **2.00×** | **2.28×** |

| basis | target (bare ÷ 1.3) | `do_get` today | **remaining** |
|---|--:|--:|--:|
| **box** (S=6) | **2,102,167** | 1,198,673 | **+75.4%** |
| **per core** (S=1) | **374,779** | 243,536 | **+53.9%** |

Derivations, shown so they can be re-derived rather than trusted:
`2,732,817 / 1.3 = 2,102,167`; `2,102,167 / 1,198,673 = 1.754` ⇒ **+75.4%**.
`487,213 / 1.3 = 374,779`; `374,779 / 243,536 = 1.539` ⇒ **+53.9%**. The ratio
route cross-checks the box figure: `do_get` is 0.439 of bare scan, the bar needs
0.769, and `0.769 / 0.439 = 1.75`.

**Peaks.** `do_get` S=1 best-N=2, **bracketed** (219,401 @ N=1 < 243,536 @ N=2 >
223,835 @ N=4). `do_get` S=6 best-N=16 (spread 1.53%); N=24's 1,197,339 is only
**−0.11%**, inside spread, so that top is a **plateau at N=16–24** and the same
pre-registered rule takes the **lower N**. **`do_get` S=6/N=4 is excluded from
best-N selection**: its 24.61% spread over 5–7 of 8 requests is ramp warm-up, not
a throughput reading — excluded for that stated reason, not dropped quietly.

**The two bases differ for a reason, and the reason is the deliverable for
#3288.** Per-core the gap is 2.00×; at box level it is 2.28×. **The box gap is
worse precisely because `do_get` scales worse** (marginal efficiency 0.820 vs
bare scan's 0.935). So the box-level bar is *harder* than the per-core bar, and
**the difference between the two bases IS the slope component** — cleanly
separated from the constant per-row overhead.

## 10. THE SLOPE-GAP CORRECTION — about half of it was the CORPUS, not the arm

This corrects a program-level assumption, so it gets its own section rather than
a footnote.

| marginal efficiency at S=6 | value | corpus |
|---|--:|---|
| bare scan | **0.935** | B |
| `do_get` (measured here) | **0.820** | **B** |
| `do_get` (#3217) | 0.711 | A |

- **Same-corpus slope gap: 11.5 pp** (0.935 → 0.820).
- **Cross-corpus impression: 22.4 pp** (0.935 → 0.711).

**Comparing bare-scan-on-B against `do_get`-on-A overstated the arm's slope
penalty by ~1.95×. Roughly half of the apparent gap was the corpus.**

**Nobody could have done better before today.** No same-corpus pair existed:
bare scan had only ever been measured on Corpus B and `do_get` only on Corpus A.
The cross-corpus comparison was the only one available, and it is exactly the
error mission §0 warns about — *"earlier figures mixed corpora and could not be
divided"* — arriving in the one place it was hardest to notice, because both
numbers were individually sound.

**What this does to #3288, stated in the direction that is least comfortable:**
the slope headroom a footprint lever could recover at S=6 is **~11.5 pp of
marginal efficiency, not ~22**. The lever is **real but roughly half as valuable
as the cross-corpus reading suggested**. This issue was chartered to calibrate
that ceiling, and "smaller than it looked" is the honest answer; reporting the
larger figure would have overstated a lever on the strength of a comparison the
program's own doctrine forbids.

The discount remains **not intrinsic to the corpus** — `do_get` on B still scales
worse than bare scan on B, by a measured 11.5 pp. What changes is its **size**.

## 11. What still separates the two arms — two disclosures, neither papered over

**(a) Machine state.** Bare-scan S=6 ran 6 cores pinned with **2 idle** and no
client. `do_get` S=6 ran 6 serving with those 2 **busy** driving load. Not
identical states, so the cross-arm comparison is **not a controlled A/B**. It is
still the right comparison — the deployment bar is itself asymmetric, since real
`do_get` has clients and bare scan does not.

**(b) THE TWO ARMS ARE NOT WINDOWED IDENTICALLY**, which is the easier one to
miss. Bare scan uses this issue's **aligned window** (control-FIFO bracketed,
rows differenced from progress records the workers actually emitted). `do_get`
uses **`flight-loadgen`'s own per-step accounting** — the #3100/#3217 arm-B
convention, *not* that window. The two arms' absolute rows/s therefore come from
different instruments and every ratio above inherits whatever systematic
difference that carries.

**The choice was fidelity to the existing arm-B convention over consistency with
this issue's arm-A convention**, so the new `do_get` figures can be set beside
#3100/#3217's. That is a judgement call, it is recorded rather than discovered,
and a reader is free to disagree with it.

### 11.1 How the client-bound objection was settled

An earlier draft shipped AC4's "remaining" half as a **stated hole**: the rig's
calibrated ratio is **1:4** server:client, a `do_get` S=6 point on 8 physical
cores runs **6:2**, and a client-bound figure would not measure `do_get` at all —
understating it, overstating the gap, and **flattering #3288**.

That was posed as a test, not a conclusion, and the test refuted it. Server fixed
at 6 physical cores, identical ramp, only the client varied:

| client cores | rows/s |
|---|--:|
| 2 physical (`6,14,7,15`) | 1,027,268 |
| 1 physical (`6,14`) | 1,027,467 |

**+0.02%** — far inside spread. **Not client-bound; the objection is refuted**,
along with this report's own recommendation to skip S=6. The "needs ≈30+ physical
cores" follow-up is retired. What survives is disclosure (a) above.

### 11.2 Positive control, and operational facts a re-runner needs

**The servability smoke returned exactly 4,000,000 rows** — the corpus row count.
That is both proof Flight serves Corpus B (uncompressed, no `CompressionInfo.db`)
and a correctness signal: the full-shape `do_get` returned the *whole* corpus,
not a truncated or empty one. It is what makes the rest of §9 meaningful, since a
0-row `do_get` presents as a very fast one.

- **The server takes `--listen <addr:port>`, not `--port`.** The recon had this
  wrong; corrected there, and the harness fix is queued (`SELF-REVIEW.md` Q6).
- **`flight-loadgen --shape` defaults to `mixed`** (weighted ptr/lim/full), so a
  bare-scan comparison **must** pass `--shape full`. A default run would have
  silently measured a different workload and produced a plausible, wrong ratio.
- **`max_concurrent_scans` is DERIVED from visible CPUs and silently caps any N
  sweep**: 32 unpinned, **24** at 6 physical cores, **4** at 1 physical core.
  Every ladder here stays under its ceiling; a future reader sweeping N will hit
  it with no error that says so.

## 12. Reproduction

```bash
bash docs/reports/ws0-3299-artifacts/harness/selftest.sh          # 41 guard cases, hermetic
bash docs/reports/ws0-3299-artifacts/host/census.sh               # the PMU census
bash docs/reports/ws0-3299-artifacts/harness/sweep.sh --equivalence --results <dir>
bash docs/reports/ws0-3299-artifacts/harness/sweep.sh --results <dir> --reps 3 --duration-s 60
python3 docs/reports/ws0-3299-artifacts/harness/derive.py --results <dir>
```

Harness design, the aligned-window convention and the N-ladder rationale:
`docs/reports/ws0-3299-artifacts/harness/README.md`. Reviewer-facing defect
analysis: `harness/SELF-REVIEW.md`.

### Operating hazards anyone re-running this campaign must know

These cost real time here and none of them is visible from the code alone.

1. **Bash reads a script INCREMENTALLY as it executes.** Editing `sweep.sh` while
   a sweep is running can corrupt the running parse mid-run. Python is not
   affected (it reads the whole file at process start), but `rep.py` must still
   not be edited mid-campaign for the separate reason below. **Edit nothing while
   a run is live.**
2. **One launcher, and a UNIQUE results root per run.** Two sweeps sharing a
   results root is silently destructive in both directions: the second's `rmtree`
   deletes the first's completed evidence, and its `stop` file ends the first
   run's window mid-measurement while every process still exits 0. `rep.py` now
   exclusive-creates its rundir so this fails loudly, but the discipline is
   cheaper than the diagnostic.
3. **Anything CPU-heavy spoils a live rep — including the agent gate.**
   `--lite` compiles. A spoiled rep still exits 0 and still reports `100.00%
   pct_running`; the only tell is the clock. One rep in this campaign was
   contaminated exactly this way and is flagged in the artifacts.
4. **Do not kill workers by NAME.** `pkill -x ws0-3299-scan-worker` can never
   match (the kernel's `comm` is 15 chars, the name is longer) and `pkill -f
   <pattern>` matches the killer's own shell. Both report success having killed
   nothing. Kill by PID, or — better — write the `stop` barrier file into the
   rundir and let the workers exit through their own path.
5. **Orphaned workers self-terminate within `--max-secs`** (900 s default), so a
   killed run degrades rather than wedging the box. Do not start the next
   measurement inside that window: an orphan holding cores corrupts the next rep
   with the cause already invisible.

### Harness provenance — the code that measured vs the code that ships

> **PLACEHOLDER.** One defect found by self-review (`SELF-REVIEW.md` Q3: a
> partial worker spawn is orphaned because `launch_workers` sits outside its
> `try` and returns a local list) is fixed **after** the campaign, never during
> it — no measurement-path change is made while reps are in flight. The fix is
> **provably success-path-neutral**: it is observable only on an exception
> between the first `Popen` and the guarded block, so no counted interval,
> counter, attribution rule or recorded value can differ. The measuring commit
> and the shipped commit will be named here on both sides once it lands.
