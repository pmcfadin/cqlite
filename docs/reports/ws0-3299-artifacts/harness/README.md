# #3299 S-sweep harness — the bare-scan scaling curve C(S), S = 1..6

Measures a **two-dimensional grid** of bare scans
(`cqlite_core::Database::execute_streaming` over the #3096 measurement corpus)
and reports, per S, the best-N aggregate rows/s, per-scan p50 rows/s, marginal
efficiency against **both** denominators, cycles/row and instructions/row — all
over an **aligned window**.

- **S** = pinned **physical cores** (1..6). It sets the `perf stat -C` set — the
  union of those cores' complete SMT sibling groups — and labels the point.
- **N** = **concurrent bare-scan streams** on those S cores. All N workers are
  pinned to that same union and the scheduler places them, which is the shape
  #3217/#3224 measured (server pinned to S cores, N streams driven into it).

```bash
bash selftest.sh                                        # hermetic; runs anywhere
bash sweep.sh --equivalence --results /data/ws0-3299/eq # worker vs ws0-scan-bench
bash sweep.sh --results /data/ws0-3299/sweep --reps 3 --duration-s 60
```

## The N ladder, and why it is shaped this way

Default grid (`--grid` overrides it):

| S | N values | N/S at the top |
|--:|---|--:|
| 1 | 1, 2, 4, 8 | 8 |
| 2 | 1, 2, 4, 8 | 4 |
| 3 | 1, 4, 8 | 2.7 |
| 4 | 1, 4, 8, 16 | 4 |
| 5 | 1, 4, 8, 16 | 3.2 |
| 6 | 1, 2, 4, 8, 16, 24 | 4 |

25 points. Four constraints fix the shape:

1. **N is not S, and the peak is not at N=S.** #3217 measured the peak at N=2 for
   S=1 but N=8 for S=2 and N=16 for S=4 and S=6. AC1 asks for the *best-N*
   aggregate, so collapsing to N=S would report less than the core budget
   achieves. This harness's own first 2-D smoke reproduced the S=1 shape
   immediately: N=1 → 360,807 rows/s, **N=2 → 492,360**, N=8 → 463,842.
2. **N=1 appears at EVERY S.** It is denominator A's baseline, and it is what
   makes the per-arm N=1 *decline* visible — the reason a self-normalised
   speedup is not cross-comparable (#3217 measured 216,229 → 163,510 as S went
   1→6).
3. **Powers of two up to ~4S.** The pinned set holds 2S hardware threads, and
   #3217's peak sat at or above that thread count at every S, so the ladder has
   to reach past 2S to be sure the peak is bracketed rather than clipped.
4. **Endpoints dense, middle thinned.** AC2 consumes S=6's best-N, and the
   #3224 endpoint comparison consumes **S=1/N=2** and **S=6/N=16** specifically,
   so both of those N values are in the ladder by name. S=3 and S=5 are thinned
   because they inform the curve's shape but no acceptance criterion reads them.

**Measured budget**: the two most expensive points (S=1/N=8 and S=6/N=24, the
highest N/S ratios) took **238 s for two reps at 60 s windows**, i.e. ~119 s per
rep including prewarm, barrier, steady-state ramp and teardown. Cheaper points
scale down with N/S. The full 25-point × 3-rep grid at 60 s windows is
**≈2.0–2.5 h**.

| file | role |
|---|---|
| `scan-worker/` | the arm: ONE bare-scan stream, emitting timestamped progress |
| `rep.py` | ONE rep at ONE (S, N) point: barrier, steady state, the perf window |
| `sweep.sh` | topology, guards, corpus identity, containment, the rep loop |
| `guards.py` | every fail-closed validator, in ONE implementation |
| `derive.py` | medians + spread → the C(S) table |
| `selftest.sh`, `selftest-fixtures.py` | each guard fed the input it must reject |

## The ALIGNED window — the methodological core

**The problem.** With S concurrent scans, "how fast did they go" has a wrong
answer that looks right: time each scan start-to-finish and add the rates up.
Those S intervals are not the same interval. At the head, scans are still
starting; at the tail, the first finisher has left and the rest run at a lower
concurrency than the label claims. A rate computed that way describes a machine
that was running S scans only in the middle of it, and it is biased UP, because
the tail is measured under less contention than S.

**The convention, following #3224.** #3224 states it as: *perf runs the loadgen
as its own child, so the counted interval **is** the row-producing interval —
numerator and denominator share one window by construction, no rate assumption.*
That is written for ONE workload process, so it does not transcribe directly to
S of them; what transcribes is the **property**: rows and counters must be taken
over ONE interval, and that interval must be one whose contents are known rather
than assumed. #3224's own contrast case is the other half — its INTERIOR arm
reproduces #3217's convention, where `rows/s` came from the whole loadgen step
while counters came from a 20 s interior slice, i.e. two different windows
compared as if they were one (#3224's reproducibility gap 2).

**What this harness implements, precisely:**

1. Every worker prewarms (warm protocol), then signals ready and waits at a
   barrier. After the barrier each scans **continuously**, in a loop, so there is
   a steady state in which all S are producing rows at once.
2. The driver waits until **every** worker has emitted ≥3 post-barrier progress
   records — an affirmative observation that all N are concurrently producing,
   not an inference from having launched N processes.
3. The window is then opened and closed through **perf's control FIFO**
   (`perf stat -D -1 --control fifo:ctl,ack`): the driver enables counting, waits
   for perf's ACK, and only then reads `T0`; at `T0 + D` it disables, waits for
   the ACK, and reads `T1`. The counters therefore cover `[T0, T1]` and nothing
   else — not perf's startup, not a child's exec.
4. Rows are attributed to that **same** `[T0, T1]` by **differencing** progress
   records the workers actually emitted: worker *i* contributes
   `rows(b_i) − rows(a_i)` where `a_i` is its first record at or after `T0` and
   `b_i` its last at or before `T1`. Nothing is interpolated; no rate is assumed.
5. Aggregate rows/s = `Σ_i (rows(b_i) − rows(a_i)) / (T1 − T0)`.

**Two guards make the window mean what it says**, and both are exercised in
`selftest.sh`:

- `WINDOW_NOT_SPANNED` — every worker must have emitted a record **at or before
  `T0`** and one **at or after `T1`**. That is the mechanical proof that all N
  scans were producing rows across the *whole* window; a worker that started late
  or stopped early makes the point not-N and the rep is discarded.
- `WINDOW_SHORTFALL` — `[a_i, b_i] ⊆ [T0, T1]`, so each worker's contribution
  misses at most one sample interval at each end. That residual,
  `((a_i − T0) + (T1 − b_i)) / (T1 − T0)`, is computed per worker, **published**,
  and capped at **0.5%**.

**Sampling is by TIME, not by rows, and that was forced by a measurement.** The
first version emitted a record every 16,384 rows. Per-worker throughput falls
roughly as N/S, so that interval stretched from ~45 ms at S=1/N=1 to ~360 ms at
S=1/N=8 — the attribution degraded *along the axis being swept*, leaving the
widest and most interesting points the worst attributed. The guard caught it
(`WINDOW_SHORTFALL`, 0.5393% against the 0.5% bound, at S=1/N=2). The fix was the
worker, not the bound: `--progress-ms` (default 25) with the clock read once per
`--progress-check-rows` (default 256). A time-based interval is invariant to
throughput, so every point in the grid is attributed at the same granularity —
which is also what makes the points comparable to each other. Measured shortfall
after the change: **0.070%** at S=1/N=8 and **0.086%** at S=6/N=24.

**The direction of the residual bias is stated because it is not zero.** Rows are
under-counted (up to the shortfall) while the denominator is the full window, so
**every rows/s figure is biased LOW** and **cycles/row and instructions/row are
biased HIGH** — by at most the published shortfall. Both are the conservative
direction for this issue: they cannot manufacture scaling that is not there. At
the default 16,384-row sample interval and a 60 s window the observed shortfall
is a small fraction of a percent, and the exact value is printed in the results.

**Why not simply bracket each scan's own passes?** A pass over the #3096 corpus
takes seconds, so pass boundaries would leave a double-digit fraction of a 60 s
window unattributed — far past any useful bound. That is the whole reason this
harness has its own worker rather than calling `ws0-scan-bench` (below).

**Residual, disclosed:** perf's ACK is sent after counting is enabled, so
counting starts marginally *before* `T0` is read (and stops marginally after
`T1`); the counted interval is a slight superset of the attributed one, again in
the conservative direction. It is sub-millisecond against a 60 s window.

## Why a separate worker, and how "same code path" is checked

`ws0-scan-bench` reports one wall time per PASS, which is far too coarse for the
attribution above. `scan-worker/` therefore drives the same surface with the same
setup: the same `cli-helpers`-only feature set (no `arrow`), the same
`ingest_with_selection(TableDirSelection::Exact)`, the rig's own
`ws0_corpus_gen::scan_scope::verify_exact_scope` **imported rather than copied**,
and the same `execute_streaming(sql, StreamingConfig::default())` /
`black_box(&row)` loop. It adds a progress record every `--progress-rows` rows
and a steady-state loop.

That claim is not left as a comment. `sweep.sh --equivalence` runs
`ws0-scan-bench --passes 3` and this worker at S=1 **on the same physical core,
in the same session, over the same bytes**, and `derive.py` prints both rates and
their delta. A large divergence means they are not the same code path and the S=1
point is not comparable to the existing rig's.

**The crate is deliberately OUTSIDE the repo workspace.** Its `Cargo.toml`
carries its own `[workspace]` table, so neither `scripts/agent-gate.sh` nor a
root `cargo build` ever compiles it — the same arrangement `fuzz/` uses. Its path
dependencies are **relative** (`../../../../../cqlite-core`), unlike the #3026
precedent's hardcoded `/home/ubuntu/workspace/wt-3026/...`, which is why that
harness cannot be built in any other checkout. Its `Cargo.lock` is deliberately
untracked and gitignored for the same reason `fuzz/Cargo.lock` is, plus one
more: roborev's compiled-in deny-list drops `**/Cargo.lock` from every review
diff (#3278), so committing it would create a file that `prompt-content:`
demands and the reviewer can never receive.

The progress emitter's own cost is inside the counted window and is disclosed
rather than netted out: one buffered `write` + `flush` per sample, ~18 per second
per worker at the default settings.

## What this harness refuses to do

- **No LLC figure, at all.** The Step 1 census (`../host/README.md`) proved every
  LLC instrument on this box unavailable — two `<not supported>`, the rest a hard
  `0` at `100.00%` enabled on a workload that cannot have zero. `guards.py`
  refuses those event names **as input**, so a later edit cannot reintroduce one
  and publish its `0`. AC3 is **deferred**, per the issue's pre-registered AC5.
- **No relaxation knob.** No environment variable loosens any bound here, and
  `selftest.sh` asserts structurally that `guards.py` reads no environment
  variable at all (negative-controlled by injecting a read and watching the
  assertion fire). An escape hatch on a measurement guard can only buy a
  confident wrong number.
- **No cross-corpus division.** The corpus identity (byte count, and the absence
  of a `CompressionInfo.db`) is verified before anything is measured. #3100's
  410,449 rows/s/core and #3217's 1,076,917 rows/s were measured on the
  LZ4-compressed 196.09 B/row "Corpus A"; this is the uncompressed 693.69 B/row
  "Corpus B". Dividing across them is forbidden on this issue.
- **No drift claim.** S order is rotated per round and the order recorded, but
  per `scripts/perf/README.md` this rig does **not** control drift. Within-round
  direction is inert data explicitly uncontrolled for drift — not a verified
  claim, and deliberately not the deleted `round_major_verified` claim.

## Fail-closed inventory

| guard | rejects |
|---|---|
| `CPUSET_NOT_SIBLING_GROUP` | a half-populated physical core (#3217's `2,10` on a `(c, c+8)` box) |
| `CPUSET_COUNT_MISMATCH` / `_HEADROOM` / `_UNKNOWN_CPU` / `_MALFORMED` | a CPU set that is not exactly S cores, or leaves <2 cores of headroom |
| `PERF_CSV_MISSING` / `_EVENT_ABSENT` / `_UNPARSEABLE` | an absent or unreadable counter file — never a fabricated 0 |
| `PERF_EVENT_NOT_COUNTED` / `_MULTIPLEXED` / `_EVENT_ZERO` | `<not counted>`, `<not supported>`, `pct_running < 100.00`, a hard 0 |
| `PERF_FORBIDDEN_EVENT` | any LLC spelling the census proved dead on this host |
| `WINDOW_NOT_SPANNED` | a rep in which fewer than S scans covered the whole window |
| `WINDOW_SHORTFALL` | a window whose boundaries could not be attributed within 0.5% |
| `WINDOW_ZERO_ROWS` | a scan that observed nothing (a failure, not a measurement) |
| `WINDOW_AFFINITY_MISMATCH` | a worker the kernel ran somewhere other than its pinned pair |
| `WINDOW_WORKER_MISSING` / `_SPAN` | an incomplete rep, or a non-positive window |
