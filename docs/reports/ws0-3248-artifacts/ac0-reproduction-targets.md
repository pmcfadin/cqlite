# AC0 — the reproduction targets, and what "reproduces" can mean

**AC0:** re-verify the #3096 recorded figures reproduce on the hardened rig. **A divergence is a
FINDING, not a failure** — the rig is never to be adjusted toward the recorded numbers.

Everything below is transcribed from `docs/reports/ws0-3096-artifacts/baseline-results.json` (the
source of record; n=3 per cell, min/max are the full observed range, `spread_pct_of_median` is the
rig's own field), with human twins at `baseline-2026-08-03.md:112-113,136-137` and
`baseline-summary.txt:33-34,39-40`.

## The recorded figures

| temp | arm | rows/s (median) | min | max | spread | cycles/row (median) | spread | IPC (median) | spread |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|
| warm | bare scan `execute_streaming` | 370,134.276 | 364,370.2 | 372,077.9 | 2.08% | 18,813.92 | 1.54% | 1.452439 | 0.31% |
| warm | Flight `do_get` bypass | 249,041.060 | 244,632.6 | 264,412.9 | 7.94% | 23,511.31 | 4.84% | 1.522772 | 0.74% |
| cold | bare scan `execute_streaming` | 194,637.699 | 194,513.7 | 198,392.2 | 1.99% | 17,939.18 | 0.96% | 1.462290 | 0.58% |
| cold | Flight `do_get` bypass | 197,832.246 | 196,836.8 | 197,846.8 | 0.51% | 24,411.58 | 3.38% | 1.481033 | 1.72% |

Row denominators and setup subtraction (both needed to reproduce the arithmetic):
* warm bare scan `execute_streaming`: denominator **12,000,000** rows; setup cycles subtracted **50,281,138**
* warm Flight `do_get` bypass: denominator **36,000,000** rows; setup cycles subtracted **0** (zero by construction — arm B's setup is outside the perf window)
* cold bare scan `execute_streaming`: denominator **12,000,000** rows; setup cycles subtracted **84,415,448**
* cold Flight `do_get` bypass: denominator **12,000,000** rows; setup cycles subtracted **0** (zero by construction — arm B's setup is outside the perf window)

Derived ratios, as recorded:

* **warm bare/flight = 1.49x** (`baseline-2026-08-03.md:120`, `baseline-summary.txt:35`)
* **cold bare/flight = 0.98x** (`:139`, `baseline-summary.txt:41`) — the artifact's own note: `do_get`
  is marginally FASTER cold, so "the 1.3x is met trivially and uninformatively"; both arms are
  page-in bound.
* the gap **+4,697.395 cycles/row (+25.0%)**, computed as flight-median minus scan-median.

Per-rep wall times, which bound what "warm" even means: warm scan 10.978 / 10.807 / 10.750 s; warm
flight 45.384 / 48.185 / 49.053 s; cold scan 20.162 / 20.551 / 20.564 s; cold flight 20.219 / 20.218
/ 20.321 s.

## What "reproduces" can mean — the spreads decide this, not preference

Three numbers bound the resolving power of any comparison against the table above:

| source of variation | magnitude |
|---|--:|
| warm Flight rows/s **within-session** range (n=3) | **7.94%** of its median |
| documented **cross-session** drift on an untouched path | **~10%** |
| between-binary noise floor (`0.17-throughput-mission.md:83-92`) | ~1.4% |

**Consequence, stated before measuring: the warm Flight rows/s ABSOLUTE has essentially no resolving
power across sessions.** Its own within-session spread (7.94%) is comparable to the entire documented
cross-session drift (~10%), so an AC0 "divergence" in that figure would be indistinguishable from
noise the record already warns about.

The quantities that ARE tight enough to carry a reproduction claim:

* **IPC** — spreads 0.31% / 0.74% / 0.59% / 1.72%. The tightest thing in the table by an order of
  magnitude.
* **cycles/row** — 1.54% / 4.84% / 0.96% / 3.38%.
* **the ratio** and **the cycles/row delta** — same-session, so common-mode drift cancels.
* **the digests, the corpus identity, and guard behaviour** — exact, not statistical.

This is precisely the list `baseline-2026-08-03.md:49-52` itself declares still valid across sessions.

## The tension inside AC0, reported rather than resolved unilaterally

AC0 asks the recorded figures to reproduce. **Every artifact in that directory says the absolutes
cannot** (`baseline-2026-08-03.md:25-34`, `baseline-results.json:2`, `baseline-summary.txt:5-21`) —
and this run is on an identically-specified box of **unrecorded identity** (see
`raw/host-provenance-finding.md`) roughly three weeks later.

So AC0 taken as "do the absolutes match" would **mechanically manufacture a divergence finding that
is just the documented drift** — a true-but-empty result, and exactly the kind of confident
non-finding this issue exists to avoid.

**How this work will report it, absent instruction to the contrary:** measure everything and report
both layers separately —

1. **the invariant layer** (ratio, cycles/row delta, IPC, digests, corpus identity, guard behaviour),
   which is where a genuine divergence would be informative and where a reproduction claim can stand;
2. **the absolute layer** (rows/s per arm), reported with its spread and explicitly marked as
   **outside the resolving power** of a cross-session comparison — neither claimed as a reproduction
   nor reported as a divergence.

A divergence in layer 1 is a finding. A divergence in layer 2 is, on this evidence, drift — and
saying so is not adjusting the rig toward the recorded numbers; it is declining to over-read an
instrument whose own documentation states its limit.

## Two corrections to inherited figures, verified against HEAD

1. **The CI-fixture Arrow digest named in the issue body is STALE.** `0xd0014e42e893f87f`
   (`baseline-2026-08-03.md:204`) was re-pinned to **`0xe6eccf8a9ffbca11`** at commit `3173e9c`, after
   the fixture gained `NullPlan::Pinned` (150 absent cells over 500 rows) — before which all twelve
   cells of every row were non-null, so no validity bitmap ever had content and a misplaced validity
   bit had nothing to misplace. HEAD asserts the new value at
   `cqlite-flight/tests/issue_3096_arrow_buffer_digest.rs:273-274`; record at
   `digest-oracle-repin.md:19-21`. **Do not assert the stale value.**
2. **The measurement-corpus digest `0x0390bfbb81a23fa1` is unchanged — and has NEVER been
   machine-checked.** `tools/ws0-corpus-gen/src/measurement_corpus.rs` states it plainly: the
   `ARROW_BUFFER_DIGEST` and `ARROW_BUFFER_BATCHES` pins are marked **NO** for machine-checked,
   "requires a real 4,000,000-row corpus", because "a gate component may not write 2.8 GB or run for
   minutes".

   **This work is in an unusually good position to close that gap: it HAS the real 4M-row corpus,
   verified byte-for-byte against the pin.** Folding the producer-tap digest during the AC0 run costs
   almost nothing on top of a scan that is happening anyway, and would machine-check a load-bearing
   constant that has only ever been operator-verified. Recorded here as an opportunity taken up during
   AC0, not as a new deliverable.

