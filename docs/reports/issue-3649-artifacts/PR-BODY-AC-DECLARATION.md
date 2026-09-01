# PR body — acceptance-criteria declaration (lift this verbatim)

*Written to be copied into the pull request body. Every "declared gap" below is a
**transcription** of what the artifacts themselves print, not a summary of them —
so this section and the harness cannot drift. Re-derive by running
`bash docs/reports/issue-3649-artifacts/selftest-analyze.sh` and
`python3 analyze-ab.py --single-stream <manifest>`.*

---

## What this PR is

The **instrument and the runbook** for issue #3649, not the measurement. No rig
was available and none is provisioned here; the measurement is co-scheduled into
#3855's bare-metal window. #3649 stays **open** under `blocked-on-hardware`.

## Acceptance criteria: 1 of 5 met

**AC-1 — `flight-loadgen --shape full` server-direct on the field i4i narrow rig:
NOT RUN.** No rig, no session, no number. Per the lead's R1 ruling this PR ships
the instrument and the criterion is discharged by the rig session when #3855's
window opens.

**AC-2 — report util throughput with dispersion: NOT MET.** The criterion is
about a *report of a measurement*, and there is no measurement. What exists is
the machinery and a test of it: paired per-replicate ratios, a seeded percentile
bootstrap over the pairs, each arm's own interval, and a refusal
(`bootstrap-degenerate`) when the computed interval is merely the observed range.
That is a claim about the instrument. It is not this criterion.

**AC-3 — corpus large enough, and state the corpus size used: NOT MET.** No
corpus was used. The driver censuses the *served* directory the way the server
enumerates it, enforces documented floors (256 MiB, 2 SSTables) that a
measurement may not lower, records the census in the manifest and prints it on
every report. Capability, tested; not the criterion.

**AC-4 — verdict recorded against ~1.1–1.25× narrow / ~1.05–1.1× wide, with
1.5–1.9× named as a ceiling: NOT MET.** No verdict has been recorded, because no
session has run. The rule exists and the ceiling is structurally untestable — the
utilization rule takes no threshold argument, so an attainment claim is not
expressible — and the self-test asserts no ceiling-endorsing token can appear.
Again: the instrument, not the criterion.

**AC-5 — triage the send-reduction oracle before filing a regression: MET.** This
is the one criterion that needs no rig.
`cargo test -p cqlite-core --test issue_2820_merge_fanin_batch` → **2 passed**,
recorded with its output in `FINDINGS.md` §1. The #2820 mechanism is intact and
**no regression is indicated or filed.**

**Read the four NOT-MET verdicts as verdicts.** An instrument built to satisfy a
criterion has not satisfied it; if a criterion needs a number and there is no
number, it is not met however good the machinery is. This issue exists because a
point estimate with overlapping intervals was nearly rounded into a verdict, and
rounding *capability* into *compliance* is the same error one level up.

## Declared gaps

### Printed by the self-test on every run

```
DECLARED GAP 1: the real cargo build, cqlite-flight and flight-loadgen are
        exercised by nothing here -- these cases prove the DRIVER's logic only.
DECLARED GAP 2: the stub is MORE PERMISSIVE than the real binary. It
        parses its own argv, so an argument line Clap would REJECT still runs
        here -- a duplicated option, an unknown flag, a bad value. That class
        is covered structurally instead (the server-argv cases above assert no
        option is emitted twice); nothing in this suite can reproduce Clap.
```

On a host with passwordless sudo the suite also prints:

```
cold fail-closed case skipped: this box HAS passwordless sudo, so the refusal
cannot be provoked (declared, not assumed)
```

— so on such a host the cold-session refusal is **not** exercised. A genuine cold
session (page-cache drop) is exercised nowhere.

### Printed by the analyzer beside every verdict

```
NON-EXHAUSTIVE this compares two commits on ONE host, ONE corpus, ONE workload
  shape and ONE admission setting; nothing here generalises to another shape,
  another row width, or another concurrency regime
NON-EXHAUSTIVE flight-loadgen reports throughput as a SINGLE point estimate per
  step, so all dispersion here is BETWEEN-replicate; within-step variance is not
  observable from its JSONL and is not modelled
NON-EXHAUSTIVE the interval is a percentile bootstrap over N pairs; with a pair
  count this small the interval is itself imprecise, and a wider interval is the
  honest reading, never a tighter one
NON-EXHAUSTIVE a difference measured here is a difference between two commits,
  not evidence about the mechanism; the mechanism oracle is
  cqlite-core/tests/issue_2820_merge_fanin_batch.rs and it is a separate check
NON-EXHAUSTIVE no attribution is performed: this script does not decompose the
  delta into send-count, syscall or cache terms
```

### Recorded in `FINDINGS.md`

- **The disposition tables close "added later and forgotten", not "never
  imagined".** They prove every field that *exists* on either side is reconciled
  or excused; neither can know about a constraint nobody thought of.
- **The wide profile has no fixture.** `--profile wide` tests the 1.05–1.10 band,
  but no wide-row corpus exists in this repository's test data, so that band is
  exercised by the band-selection logic only and by no data.
- **`--merge-path` cannot be corroborated.** `cqlite-flight` does not log it, so
  the pin is recorded and disclosed but never read back — unlike the admission
  ceiling, batch size and wait timeout, which are.
- **Percentile bootstrap under-covers at small n.** The degeneracy refusal removes
  the pathological case (where the interval *is* the observed range); it does not
  remove small-sample optimism. Treat a marginal `MEETS-TARGET` at n = 5
  accordingly.

## What is verified

`bash docs/reports/issue-3649-artifacts/selftest-analyze.sh` — **319 cases, floor
319**, green in-repo and from a copy outside any git checkout. Includes complete
two-arm sessions — a measurement and the step-4 sensitivity control — run end to
end against stub `cargo`/`cqlite-flight`/`flight-loadgen` on `PATH`, subject to
the two declared gaps above.
