# WS0 #3299 — the bare-scan scaling curve C(S), and the box-level target it derives

**Status: DRAFT.** AC1 and AC2 are measured and complete. AC3 is DEFERRED
(instrument unavailable, per the issue's pre-registered AC5) with the L1d partial
reported. Two sections are placeholders pending runs the delivery lead owns:
the S=6 bracketing extension and the frequency calibration. **AC4 is PARTIAL**:
its box-level target number is discharged (§5); its "remaining to target" half is
a stated hole, because box-level `do_get` cannot be VALIDLY measured on 8
physical cores (§9).

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

## 4. Peak bracketing — which best-N values are real

Pre-registered rule, fixed before the data was seen: a peak is **bracketed** when
some tested N above it is lower by more than the relevant point's **own** spread;
**plateau** if within spread (the lower N is then reported — same throughput,
cheaper); **edge-truncated** if nothing above it was tested.

Only **S=1** is resolved (plateau at N=2; N=4 is 1.88% lower, inside its 3.41%
spread). **S=2 through S=6 are all edge-truncated** — their best N is the largest
N tried, so each is a **lower bound on that S's best**, not a measured peak.

For S=6 this is not marginal: **N=24 beats N=16 by +9.8%**, far outside that
point's 0.4–0.7% spread, so the curve was still climbing when the ladder ended.

### Extension A's verdict: S=6 is a PLATEAU at N=24–32, upper end memory-bounded

Extension A re-measured the incumbent **N=24 interleaved with N=32/48/64 in the
same rounds**, so every comparison below is contemporaneous and drift-robust by
construction.

| N | extA aggregate rows/s | vs N=24 |
|--:|--:|--:|
| 24 (incumbent, re-measured) | 2,647,966 | — |
| 32 | 2,665,993 | **+0.68%** |
| 48 | *unmeasurable* | OOM-killed under the 24 GiB containment cap |

The pre-registered rule, quoted: *"a peak is bracketed when the best N has at
least one tested N above it that is lower by more than the point's own
rep-to-rep spread. If a doubling is within spread, the top is flat — record it as
a plateau and take the lowest N achieving it (cheaper configuration, same
throughput)."* N=32's **+0.68%** sits inside the point's own **~0.7%** spread, so
this is a **plateau**, and the rule takes the **lowest N achieving it — N=24**.

**Consequence for AC2: the target is NOT materially understated.** The main
grid's best-N was already sitting on the plateau, so the §5 figure is not a lower
bound of unknown tightness — it is a plateau value with one bounded residual,
below.

**S=6 is marked `plateau (N=24–32), upper end memory-bounded`** — deliberately
neither of the other two verdicts. Not `bracketed`, because nothing above N=32
could be measured, so the curve was never observed turning over. Not
`edge-truncated`, because N=32 *was* measured and *is* within spread. That
three-way distinction is the honest one and the table carries it verbatim.

**The N=48 limit is MEMORY, not throughput — and it is a property of THIS
HARNESS, not of CQLite.** 48 independent scan processes exceeded the 24 GiB
containment cap and the scope was OOM-killed. Each worker in this harness holds
its **own** `Database`, its own readers and its own buffers, because the arm
under test is *N independent bare scans*; that is the design of the measurement,
not of the engine. **Nobody deploys 48 independent scanners**, and production
`do_get` shares one server process. So: the peak search space here is bounded at
**N ≤ 32 by memory**, and **nothing in this bound is a CQLite memory limit** —
do not read it as one.

> **PLACEHOLDER — extension B** (`6:24,32`, 3 reps) for proper medians on the
> plateau pair. Round-1 verdict above is expected to stand.

> **SUPERSEDED PLACEHOLDER — extension A.** `--grid "6:24,32,48,64"`, 3 reps, with the
> incumbent N=24 **re-measured interleaved** with each candidate in every round,
> so the bracketing verdict is drift-robust by construction. This is required,
> not precautionary: the session drifts directionally (§6), so comparing a late
> N=32 against an early N=24 would let drift answer the bracketing question — in
> the direction that inflates the target.
>
> **The re-measured N=24 is also a result in its own right**, not merely a
> control: it is an independent read on session drift **at the exact
> configuration AC2 consumes**. Its value will be published beside the main
> grid's 2,732,817 and the difference left to speak for itself. If they differ
> materially, **extA's value is the one used for the bracketing comparison**,
> because it is contemporaneous with the candidates it is being compared against.

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

**Status of this figure, after extension A (§4): NOT materially understated.**
The earlier reading — a lower bound of unknown tightness — was correct only until
the plateau was measured. N=32 is within spread of N=24, so the main grid's
best-N was already on the plateau. One residual remains and is bounded rather
than open: **N > 32 could not be measured at all** (memory, §4), so the figure is
a plateau value with an unmeasurable region above it, not a peak observed to turn
over.

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

| | S=6, N=24 | |
|---|--:|---|
| main grid (median of 3) | 2,732,817 | within-session spread **0.74%** |
| extension A, ~1.5 h later | 2,647,966 | **−3.1%** |

**These two numbers measure different things and must not be conflated.** The
0.74% is dispersion *within* a session; the −3.1% is reproducibility *across*
sessions. Quoting the former as the figure's uncertainty would understate it by
4×, and this is an **in-band** measurement of that gap — at the AC2-relevant
point, in this rig, rather than the rig's recorded 370,134 → 333,206 anecdote.

**And the drift is uncontrolled, not correctable**: its direction *reversed*
between the two scales — 7 of 9 round-over-round deltas rose *within* the
session, while the across-session delta *fell*. A drift with an inconsistent sign
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

## 9. AC4 — PARTIAL: target discharged, "remaining" is a stated hole

AC4 needs "remaining to target", which needs both the target (§5) **and** where
we are today, which is `do_get`. The only box-level `do_get` figure is #3217's
1,076,917 rows/s on **Corpus A** (LZ4, 196.09 B/row); this target is **Corpus B**
(uncompressed, 693.69 B/row). Dividing across them is what R1 forbids and what
mission §0 calls out — 3.5× the bytes per row and no per-row decompression are
two large opposite-signed effects on exactly the measured quantity.

So the target half is discharged and the "remaining" half is not. §9.1–§9.4 give
the reason, what phase 2 measures instead, and the exact shape of the hole.

### 9.1 Why box-level `do_get` cannot be measured on this box

The obstacle is **validity, not comparability**. `ws0-baseline.sh` ships
`SERVER_CPUS="2,10"` (**1** physical core) against
`CLIENT_CPUS="4,12,5,13,6,14,7,15"` (**4** physical cores) — a **1:4**
server:client ratio chosen by whoever calibrated the rig. A `do_get` S=6 point on
8 physical cores would run **6:2**, a **12× swing**, i.e. a 2-core client driving
a 6-core server, far below what that author thought a *one*-core server needed.

If such a point is client-bound it is **not a measurement of `do_get` at all** —
it measures the loadgen. And the error direction is the disqualifying part: a
client-bound figure **understates `do_get`**, which **overstates** the
bare-scan-vs-`do_get` gap, which **flatters #3288** — the exact lever this issue
exists to calibrate. That is this issue's signature failure mode.

So **no S=6 `do_get` figure is published** unless the falsification in §9.3
clears it, and the default is not to publish one.

### 9.2 What phase 2 does measure

**`do_get` at S=1, on the rig's own calibrated 1:4 split** (`--server-cpus 2,10
--client-cpus 4,12,5,13,6,14,7,15`), ≥3 reps per N over a small N ladder so
`do_get`'s own best-N is measured rather than assumed. This delivers the
**same-corpus bare-scan-vs-`do_get` ratio R1 promised**, which no existing figure
provides on one corpus.

| piece | how |
|---|---|
| server | `cqlite-flight --data-dir /data/ws0-3096 --port <p>`; table resolved as `<data_dir>/<keyspace>/<table>`. |
| schema | **carried in the TICKET** — `service.rs:424 parse_schema(ticket)` parses CQL DDL per request and caches it, so no server-side schema file and no server change for Corpus B. |
| concurrency (N) | `flight-loadgen --ramp`, one value per rep, so the counted interval matches exactly one step. |
| rows | per-step JSONL `rows_total` / `rows_per_s`. |
| shape | **`--shape full`**, matching the bare scan's `SELECT *`. The loadgen default `mixed` would measure a different workload and void the ratio. |
| window | **#3224's ALIGNED convention verbatim** — perf runs the loadgen as its own child, so the counted interval *is* the row-producing interval. |

**Corpus B is servable**: no schema obstacle; no `CompressionInfo.db` assumption
(the warm-budget accounting is explicitly compression-agnostic and notes `CRC.db`
"can DOMINATE on an uncompressed BIG table"); no Corpus-A assumption. That is a
code read, so a **mandatory servability smoke** — an uncounted `do_get` whose row
count must be **non-zero** — runs before any rep. A 0-row `do_get` would
otherwise look like an extremely fast one, since a server answering `NotFound`
completes every request immediately; #3224 shipped exactly that.

**Disclosed asymmetry at S=1**: the bare-scan S=1 point ran with 7 physical cores
idle; this `do_get` S=1 point loads 5 of 8. Not identical machine states — a
smaller version of the same effect, and far smaller than S=6's.

### 9.3 The client-bound objection is FALSIFIED BY MEASUREMENT, not asserted

The objection in §9.1 is this report's own, so it is tested rather than trusted.
Identical server set, client halved:

- aggregate moves by more than the points' own spread ⇒ **client-bound; the S=6
  number is void**, objection confirmed by measurement;
- aggregate does not move ⇒ objection **falsified**, published as such against
  the expectation that raised it, leaving only the machine-state asymmetry
  (bare-scan S=6 ran 2 cores **idle**; `do_get` S=6 runs them **busy**), which is
  disclosable rather than disqualifying.

> **PLACEHOLDER** — verdict and numbers from `phase2-compare.py falsify`.

### 9.4 AC4 is PARTIAL, and this is the precise shape of the hole

| half of AC4 | status |
|---|---|
| §6 box-level **target** number | ✅ **DISCHARGED** — §5, measured. |
| §0 "remaining to target" **at box level** | ⛔ **NOT DISCHARGED** — needs box-level `do_get` on Corpus B, which this box cannot validly produce. |

The reason is **client provisioning** — not the corpus, not the instrument, not
the harness. A follow-up needs a host with **≈30+ physical cores**, enough to
provision a 6-core server at the rig's own 1:4 client ratio.

This matches what the delivery lead committed to
`docs/architecture/0.17-throughput-mission.md`: the box-level target lands in §6
with its byte basis and an explicit note that it is a **division**; §0's
box-level "remaining" is left **deliberately unstated** with the cause recorded
as client provisioning; Gap B's mechanism is narrowed to
**`do_get`-path-specific**, so **#3288's ceiling is preserved**; and the
precision is stated with its scope (0.74% within-session, −3.1% across ~1.5 h).

**Two bridges are available and both are refused.** #3217's Corpus A figure
cannot be divided into a Corpus B target (3.5× the bytes per row and no per-row
decompression — two large opposite-signed effects on the measured quantity, which
is what R1 forbids). And modelling box-level `do_get` as `S=1 × 6 × 0.711` would
smuggle in the assumption that `do_get`'s slope is corpus-independent — the very
thing that cannot be tested here.

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
