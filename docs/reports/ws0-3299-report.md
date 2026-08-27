# WS0 #3299 — the bare-scan scaling curve C(S), and the box-level target it derives

**Status: DRAFT.** AC1 and AC2 are measured and complete. AC3 is DEFERRED
(instrument unavailable, per the issue's pre-registered AC5) with the L1d partial
reported. Two sections are placeholders pending runs the delivery lead owns:
the S=6 bracketing extension and the frequency calibration. AC4 is not
discharged here — see §7.

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

**Every figure here carries two standing caveats.** Every best-N except S=1's sits
at the top of its tested N ladder, so those are **lower bounds** (§4). And this
rig does not control drift (§6).

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

## 4. Peak bracketing — why most of this table is a lower bound

Pre-registered rule, fixed before the data was seen: a peak is **bracketed** when
some tested N above it is lower by more than the relevant point's **own** spread;
**plateau** if within spread (the lower N is then reported — same throughput,
cheaper); **edge-truncated** if nothing above it was tested.

Only **S=1** is resolved (plateau at N=2; N=4 is 1.88% lower, inside its 3.41%
spread). **S=2 through S=6 are all edge-truncated** — their best N is the largest
N tried, so each is a **lower bound on that S's best**, not a measured peak.

For S=6 this is not marginal: **N=24 beats N=16 by +9.8%**, far outside that
point's 0.4–0.7% spread, so the curve was still climbing when the ladder ended.

> **PLACEHOLDER — extension A.** `--grid "6:24,32,48,64"`, 3 reps, with the
> incumbent N=24 **re-measured interleaved** with each candidate so every
> comparison is contemporaneous. This is required, not precautionary: the session
> drifts directionally (§6), so comparing a late N=32 against an early N=24 would
> let drift answer the bracketing question in the direction that inflates the
> target. Verdict and the incumbent's early-vs-late values go here.

---

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

**This target is a LOWER BOUND** until S=6's peak is bracketed (§4), because its
input is edge-truncated. It is **+56%** above the issue body's ~1.35M estimate,
which assumed bare scan would scale at `do_get`'s 71%.

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
3. The guard that mattered most is the one that **did not misfire**. A rig that
   had tolerated an empty counter file would have published a rep with no
   counters. The bug was always upstream of the guard.

---

## 9. AC4 — NOT discharged here

AC4 needs "remaining to target", which needs both the target (§5) **and** where
we are today, which is `do_get`. The only box-level `do_get` figure is #3217's
1,076,917 rows/s on **Corpus A** (LZ4, 196.09 B/row); this target is **Corpus B**
(uncompressed, 693.69 B/row). Dividing across them is what R1 forbids and what
mission §0 calls out — 3.5× the bytes per row and no per-row decompression are
two large opposite-signed effects on exactly the measured quantity.

> **PLACEHOLDER — phase 2.** `do_get` on Corpus B, same host and session, at S=6
> best-N and S=1 only. Scope, comparability judgement and the pinning question
> are open; if the two arms cannot be made comparable at S=6 on 8 physical cores,
> AC4 ships with a stated hole and a proposed follow-up rather than a false ratio.

---

## 10. Reproduction

```bash
bash docs/reports/ws0-3299-artifacts/harness/selftest.sh          # 41 guard cases, hermetic
bash docs/reports/ws0-3299-artifacts/host/census.sh               # the PMU census
bash docs/reports/ws0-3299-artifacts/harness/sweep.sh --equivalence --results <dir>
bash docs/reports/ws0-3299-artifacts/harness/sweep.sh --results <dir> --reps 3 --duration-s 60
python3 docs/reports/ws0-3299-artifacts/harness/derive.py --results <dir>
```

Harness design, the aligned-window convention and the N-ladder rationale:
`docs/reports/ws0-3299-artifacts/harness/README.md`.
