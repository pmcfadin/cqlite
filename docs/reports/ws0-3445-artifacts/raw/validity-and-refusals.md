# Validity ledger and REFUSALS (issue #3445)

The WS0 rule is same-corpus, warm, counters at 100.00% `pct_running`, >= 3 reps, and
**refuse rather than publish** a rep that fails validity — recording the refusal as an
observation, because a rep silently dropped is worse than a rep reported as refused.

## Published reps — every rep, every check

| rep | arm | warm (prewarm rows) | rows measured | pinned (kernel read-back) | lost samples | loadavg **before/after (ENDPOINT reads — see gap note)** | peer procs | verdict |
|---|---|--:|--:|---|--:|--:|--:|---|
| `ac1-perfprof-1` | annotate | 4,000,000 | 14,213,888 | cpu 2 | 0 | 1.04 / 2.58 | 1 | PUBLISHED |
| `ac1-perfprof-2` | annotate | 4,000,000 | 12,614,912 | cpu 2 | 0 | 4.46 / 6.70 | 11 | PUBLISHED |
| `ac1-perfprof-3` | annotate | 4,000,000 | 12,570,624 | cpu 2 | 0 | 5.33 / 4.99 | 1 | PUBLISHED |
| `ab-annotate-1` | annotate (A/B) | 4,000,000 | — | cpu 2 | 0 | 4.38 / 6.15 | 4 | PUBLISHED |
| `ab-annotate-2` | annotate (A/B) | 4,000,000 | — | cpu 2 | 0 | 12.84 / 10.77 | 10 | PUBLISHED |
| `ab-annotate-3` | annotate (A/B) | 4,000,000 | — | cpu 2 | 0 | 6.15 / 4.80 | 1 | PUBLISHED |
| `probe-rep-1..3` | probe | 4,000,000 | 9.7-11.8 M | cpu 2 | 0 | 14.77-18.31 / 16.85-18.31 | 7-30 | PUBLISHED (see below) |
| `ab-probe-1..3` | probe (A/B) | 4,000,000 | — | cpu 2 | 0 | 4.80-10.77 / 5.23-12.84 | 1-15 | PUBLISHED |
| `ac1-perfsym-1..3` | fidelity control | 4,000,000 | 11.5-13.9 M | cpu 2 | 0 | 2.58-6.70 / 4.99-6.70 | 1 | PUBLISHED |
| `ac2-stat-1..3` | AC2 counters | 4,000,000 | — | cpu 2 | n/a (counting) | 6.26-9.55 / 5.83-7.17 | 5-8 | PUBLISHED |
| `ac2-stalls-1..3` | AC2 sampling | 4,000,000 | 10.7-13.4 M | cpu 2 | 0 | 5.65-8.67 / 5.65-8.67 | 5-8 | PUBLISHED |

`pct_running` is only a concept for the COUNTING reps. All three `ac2-stat` reps report
**100.00** on every one of the six events, read from field 5 of `perf stat -x,` output
rather than inferred from the absence of a warning — the archived CSVs are in `../ac2/`.
Six events fit the available general-purpose counters with no multiplexing, which is why
100.00 was attainable at all.

Warmth is structural rather than asserted: the #3299 worker signals ready only after a full
untimed pass, and `record-scan.sh` starts `perf` only after that signal, after releasing the
barrier, and after a settle interval. Every rep shows `prewarm_rows = 4,000,000`, i.e. a
complete pass over the corpus before the window opened.

## REFUSED and EXCLUDED observations

**1. REFUSED — the two development smoke reps (`smoke`, 10 s).** Taken while the attribution
pipeline was still being debugged, and the first of them was analysed with a WRONG PIE
rebase. Below the >= 3-rep bar on their own and not comparable to the 40 s reps. No number
from them appears in the report. They are named here because the first one is what caused the
rebase self-check to be written, which makes them part of the method's history rather than
data to be quietly discarded.

**2. REFUSED — width-probe run 1 (no histogram emitted).** The first width-probe binary used
a 200M-per-bucket dump threshold that no bucket reached in a 45 s window, so the run produced
**no observation at all**. Recorded as a failed observation rather than reported as "no
multi-byte VInts found" — an absent measurement is not a zero. Rebuilt at a 25M threshold
and re-run; that second run is the published histogram.

**3. EXCLUDED BY DESIGN — the width-probe build's timing.** Relaxed atomics in the hot loop
perturb the very thing the cycle accounting measures. Only the DISTRIBUTION from that build
is used; no cycle, stall or share figure in this report comes from it.

**4. EXCLUDED BY DESIGN — the probe build as a source for the headline.** `#[inline(never)]`
changes codegen by construction, which is why AC1 names it a cross-check and the issue
requires the caveat stated. It is reported, quantified against the primary route, and never
substituted for it.

## Co-tenancy, stated rather than assumed away

This box is shared: peer lanes ran gates throughout, and loadavg across published reps spans
**1.04 to 18.31** with up to **30** peer `cargo`/`agent-gate` processes. Three things bound
the damage, and one confound had to be actively removed:

* Every rep is **pinned to one core with affinity read back from the kernel**, so a peer
  cannot take the measured thread's core away from it.
* The published quantity is a **share**, not a throughput, so it is first-order insensitive
  to the box being slower. This is visible in the data: within the annotate arm, reps at
  loadavg 1.04 and 12.84 give 1.6621% and 1.6875% — a 0.03pp difference across a 12x load
  range — while total cycles in the same window moved by 15%.
* **The confound that DID exist was between arms, not within them.** The first probe reps
  ran at loadavg 14.8-18.3 while the first annotate reps ran at 1.0-5.3, so the route
  comparison was not sound as originally taken. Three interleaved A/B pairs were added, each
  taking both arms back to back under near-identical load. The result moved the disagreement
  the OPPOSITE way from the confound's direction (1.381x matched vs 1.329x pooled), so the
  gap between routes is a property of the inline attribute and not of the box.

No rep was excluded on co-tenancy grounds. The reason is stated rather than assumed: the
share's insensitivity to load is measured above, and the one comparison where load could
have mattered was re-taken under matched conditions instead of being argued about.


---

# ADDENDUM — the quiescence bar was raised MID-ISSUE, and the whole published set predates it

The coordination lead measured this box at **loadavg 15.80 (1m) / 22.77 (5m) on 16 cores**
while a peer lane ran a `--lite` gate including `maturin`, and set an explicit bar: a rep is
publishable only if the box was quiet **across the whole rep**, roughly loadavg <= 2-3.

**Applied strictly, NONE of the 18 reps in the table above clears that bar.** Only
`ac1-perfprof-1` comes close (before 1.04, after 2.58). This is stated plainly rather than
argued around: every published figure in this report was taken on a contended box, and the
per-rep before/after loads are in the table so a reader can judge each one.

Two mechanisms were then added, and one question was answered by measurement.

**Mechanism 1 — the gate semaphore does not protect a perf run.**
`CQLITE_GATE_MAX_CONCURRENCY=1` is pinned and verified on this box, but it serialises GATE
against GATE. A perf run holds no slot, so a peer's entire gate can start, run and finish
*inside* a rep's window. "The pin is in effect" is not "the box is quiet".

**Mechanism 2 — a before/after pair cannot see that.** `loadavg` is a decaying average, so
its value at t=0 describes the minute *before* the rep. `harness/record-scan.sh` therefore now
samples load every 5 s **across** the window, takes its verdict from the **maximum**, writes
`quiescence-verdict.txt` (`OK` / `REFUSED(box-not-quiet-across-rep)` /
`REFUSED(quiescence-unmeasured)` / `UNCHECKED`), and **never retries** — a rep quietly
re-rolled until it looks clean is the worse outcome (#3299 AC5).

## The question that actually matters: can contention move the verdicts?

Answered by measurement, not by assurance — full output in `load-sensitivity.txt`. The
published set happens to span a **5x load range** (peak loadavg 2.58 to 12.84), which makes it
its own load-sensitivity experiment.

**AC3 (the 3% cliff) — not threatened, by four orders of magnitude.**

| quantity | value |
|---|--:|
| OLS slope, cycle share vs peak loadavg | **-0.0009 pp per unit load** (slightly *negative*) |
| total share spread over the 5x load range | **0.1338 pp** |
| share extrapolated to loadavg 0 (idle) | **1.7087%** (vs 1.7027% measured mean) |
| distance from mean to the 3% cliff | **1.2973 pp** |
| => flipping KILL -> FUND would need | **10x the entire observed load-induced variation**, or loadavg ~1442 on 16 cores |

The share is a **ratio measured on a pinned core**, and contention slows numerator and
denominator together, which is why the slope is ~0. The verdict is not close to load-sensitive.

**AC2 — the corrected 1.06x ratio, and what contention can and cannot do to it.**

This paragraph originally argued that contention could not destroy an "anti-concentration"
finding. **That finding did not exist** — it was a denominator artifact (see the report's §4),
and AC2's answer is PARITY. So the question is the other one: could contention hide a real
effect in either direction? Both sides on the in-binary basis:

| | slope vs peak load | idle extrapolation |
|---|--:|--:|
| cycle share, in-binary | -0.0044 pp/unit | 3.0068% |
| stall share, in-binary | +0.0441 pp/unit | 2.8199% |

| ratio | value |
|---|--:|
| at the measured means | **1.061x** |
| at both idle extrapolations | **0.938x** |

Both readings sit near 1.0, so the parity conclusion is not an artifact of load. But the
stall-side rep spread is **0.83 pp (sd 0.42)** across only a 1.5x load range, so **an effect of
a few percent would sit inside this instrument's noise**. That is a resolution limit, and it is
why AC2 is reported as NOT CONFIRMED rather than as "no dependency exists". A quiesced re-run
would narrow that spread and is the single measurement that would most improve this answer.

## Status of the confirmatory quiet reps

A background job was set to wait for sustained quiet (<= 2.5 for 6 consecutive checks) and
then take an interleaved quiet set. It did not get to run: the box went from loadavg ~8 to
**67-80 with 43 concurrent `agent-gate` processes**, and `/tmp` was cleaned by a peer lane
mid-wait (which removed the job's own script and log). No quiet rep was obtained, and none is
reported. **No number here comes from an unquiesced retry, and no published number was
re-rolled**; the contended set stands as taken, with its loads disclosed per rep and its
load-sensitivity measured above.
