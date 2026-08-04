# `scripts/perf/` — the issue #3096 Arrow-encode measurement rig

Runnable end to end from a clean checkout. No path outside the repository, no
uncommitted helper.

```bash
# 1. corpus (~2.8 GB of scratch; corpus binaries are never committed)
cargo run --release -p ws0-corpus-gen --bin ws0-corpus-gen -- --out /data/ws0-3096

# 2. both arms, one session, one verified physical-core sibling pair
scripts/perf/ws0-baseline.sh --corpus /data/ws0-3096
```

| file | role |
|---|---|
| `ws0-baseline.sh` | the driver: both arms, warm+cold, median of N, fail-closed pinning |
| `lib-cpu.sh` | `thread_siblings_list` verification — the pinning is READ, never assumed |
| `ws0_report.py` | aggregation → `results.json` + a human summary |
| `ws0_validate.py` | the fail-closed layer: what the reporter is ALLOWED to aggregate |

Full method, the traps, the recorded pinning and the residual caveats:
**`docs/reports/ws0-3096-artifacts/measurement-method.md`** — read it before
believing any number this rig prints.

Non-negotiables baked into the scripts (issue #3096 spec R1/R2):

* CPU-wide `perf stat -C <cpu-list>`; **never** `perf stat -p` (>2x observer
  cost). `ws0-baseline.sh` greps itself for a `-p` form and refuses to run.
* `taskset` to a **verified** physical-core sibling pair; a non-sibling request
  fails closed rather than silently measuring two different cores.
* **rows/s AND cycles/row**, never a CPU-share.
* **Warm and cold are separate claims**, never blended.
* Setup subtracted from the cycles/row denominator; the row denominator printed
  beside every figure.
* A rep that observes **zero rows exits non-zero** rather than reporting a
  measurement.

## The instrument-integrity contract (issue #3272)

Every guard below was added because its absence was a real defect, and all of them
are the same shape: **an instrument that reports success without having measured.**
A rig whose guards are fail-open is worse than no rig, because it produces confident
numbers nobody re-checks. So the bar here is not "the guard exists" — it is **"the
guard has been observed to fire"** (per #3249, where hardcoding `_PERF_STATE="ok"`
survived 118/118 tests). `scripts/tests/test_ws0_report_guards.sh` feeds each guard
the input it must reject and asserts the exit code *and* the diagnostic; it is wired
into the agent gate's `tooling-tests` component, and it is hermetic (synthetic result
dirs and perf CSVs — no cargo, `perf`, `sudo`, corpus or network).

* **A counter that was not observed is an ERROR, never a fabricated `0`.** An absent
  perf CSV, an absent required event, a perf `<not counted>`/`<not supported>` marker
  and an unparseable value are each fatal. `.get("cycles", 0)` let a run be reported
  "setup-subtracted" having subtracted nothing.
* **The corpus identity is REQUIRED and complete-checked.** Its absence used to
  silently disable the `rows == requests_ok x corpus_rows` assert while the report's
  NOTES claimed the property had been verified.
* **The cold arm's `skipped-cold-arm` prewarm sentinel satisfies a COLD rep only.**
  A temperature-blind acceptance set let an UNPREWARMED WARM rep reach
  `prewarm_all_ok=true` — the prewarm guard satisfied by its own sentinel. Scoped in
  both directions: a *prewarmed* "cold" rep is refused too. An honestly recorded
  failure (`FAILED-exit-N`, `unrecorded`) stays a flagged degradation, not a refusal.
* **Every numeric argument is validated positive up front.** `--reps 0` produced a
  vacuous but *successful* report.
* **Completeness is judged against the SELECTION, and the selection is stated.**
  An unselected temperature/arm is legitimately absent; a selected one that is absent
  is fatal. A narrow run prints `PARTIAL MATRIX` and records
  `results.json .selection`, so it can never later be read as a full matrix.
* **Durations parse as DECIMAL.** `010s` was octal 8s and `08s` was a hard bash
  error; `010000ms` parsed as 4096ms, sneaking a blended cold step under the 5000ms
  ceiling.
* **Host state the rig mutates is RESTORED on every exit path.** It weakens
  `kernel.perf_event_paranoid` and `kernel.kptr_restrict` for CPU-wide counting;
  the priors are captured *before* the mutation and restored from a single
  `EXIT INT TERM HUP` handler that also stops the server. Idempotent and per-step
  non-fatal — cleanup may not fail a run, and may not leave the second knob
  weakened when the first write fails. If the restore itself cannot run, the driver
  says so loudly and prints the `sysctl` command to fix it by hand.

There is deliberately **no environment variable that relaxes any of this.** An
escape hatch on a measurement guard can only ever buy a confident wrong number.

## No cross-session absolutes — interleave or do not compare

**Measured on the delivery box in one day: the untouched warm bare scan read
370,134 rows/s and, an hour later, 333,206 rows/s — a ~10% drift with nothing
changed on the measured path.** So this rig produces **no reusable absolute**.

**The rule: same-session interleaved A/B/C with a drift control that is
code-identical across arms, or NO COMPARISON.** One rep at a time, arm order
rotated per round, the control carried in every run, differenced *within* a
round. Mechanics: `docs/reports/ws0-3096-artifacts/measurement-method.md` §3b.
Worked example with all 30 per-run numbers, and a discarded run with the reason
it was discarded: `docs/reports/ws0-3096-artifacts/abc-interleaved-2026-08-03.md`.

## Reusing this rig for another corpus (issues #3232, #3234)

* **#3232 (publishable absolutes vs stock Cassandra)** required corpus sha
  `22d9ae22…ce922c` **or a geometry-matched regeneration.** `tools/ws0-corpus-gen`
  **is** that regeneration path — deterministic from a recorded seed, driven
  through the production `SSTableWriter`, pinned by its own recorded
  sha256 + row count + byte shape. **That corpus-identity blocker is retired**;
  what remains on #3232 is the provision hold and the absence of Cassandra here.
* **#3234 (BTI `da` perf corpus)** should **mirror this determinism contract
  rather than invent a second one**: byte-identical output across 3 runs from a
  recorded seed, generated on the **production writer**, pinned by its own
  recorded digest + shape, and non-vacuous (observing zero rows exits non-zero).
  Author `gen-perf-corpus-bti.sh` with `tools/ws0-corpus-gen` and
  `scripts/perf/ws0-baseline.sh` as the **template** — two divergent contracts for
  one property is how a corpus stops being comparable to anything.
