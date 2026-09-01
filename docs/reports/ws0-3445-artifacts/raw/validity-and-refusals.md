# Validity ledger and REFUSALS (issue #3445)

The WS0 rule is same-corpus, warm, counters at 100.00% `pct_running`, >= 3 reps, and
**refuse rather than publish** a rep that fails validity — recording the refusal as an
observation, because a rep silently dropped is worse than a rep reported as refused.

## Published reps — every rep, every check

| rep | arm | warm (prewarm rows) | rows measured | pinned (kernel read-back) | lost samples | loadavg | peer procs | verdict |
|---|---|--:|--:|---|--:|--:|--:|---|
| `ac1-perfprof-1` | annotate | 4,000,000 | 14,213,888 | cpu 2 | 0 | 1.04 | 1 | PUBLISHED |
| `ac1-perfprof-2` | annotate | 4,000,000 | 12,614,912 | cpu 2 | 0 | 4.46 | 11 | PUBLISHED |
| `ac1-perfprof-3` | annotate | 4,000,000 | 12,570,624 | cpu 2 | 0 | 5.33 | 1 | PUBLISHED |
| `ab-annotate-1` | annotate (A/B) | 4,000,000 | — | cpu 2 | 0 | 4.38 | 4 | PUBLISHED |
| `ab-annotate-2` | annotate (A/B) | 4,000,000 | — | cpu 2 | 0 | 12.84 | 10 | PUBLISHED |
| `ab-annotate-3` | annotate (A/B) | 4,000,000 | — | cpu 2 | 0 | 6.15 | 1 | PUBLISHED |
| `probe-rep-1..3` | probe | 4,000,000 | 9.7-11.8 M | cpu 2 | 0 | 14.8-18.3 | 7-30 | PUBLISHED (see below) |
| `ab-probe-1..3` | probe (A/B) | 4,000,000 | — | cpu 2 | 0 | 4.80-10.77 | 1-15 | PUBLISHED |
| `ac1-perfsym-1..3` | fidelity control | 4,000,000 | 11.5-13.9 M | cpu 2 | 0 | 2.58-6.70 | 1 | PUBLISHED |
| `ac2-stat-1..3` | AC2 counters | 4,000,000 | — | cpu 2 | n/a (counting) | 5.65-8.67 | 5-8 | PUBLISHED |
| `ac2-stalls-1..3` | AC2 sampling | 4,000,000 | 10.7-13.4 M | cpu 2 | 0 | 5.65-8.67 | 5-8 | PUBLISHED |

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
