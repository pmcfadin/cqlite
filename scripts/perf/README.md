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
| `lib-perf-lint.sh` | the perf-invocation guard: perf is invoked in ONE wrapper, CPU-wide |
| `lib-host-state.sh` | the sysctl capture/mutate/restore — the only state changed outside the process tree |
| `lib-args.sh` | numeric + duration validation: positive, bounded to the reporter's cap, decimal |
| `ws0_report.py` | aggregation → `results.json` + a human summary |
| `ws0_validate.py` | the fail-closed layer: what the reporter is ALLOWED to aggregate |
| `lib-measure.sh` | how ONE rep of each arm is executed, prewarmed and counted |
| `lib-flight-arm.sh` | the two arms no longer run the same way — WHAT differs (pin, allocator) and was it VERIFIED (#3551) |

Full method, the traps, the recorded pinning and the residual caveats:
**`docs/reports/ws0-3096-artifacts/measurement-method.md`** — read it before
believing any number this rig prints.

Non-negotiables baked into the scripts (issue #3096 spec R1/R2):

* CPU-wide `perf stat -C <cpu-list>`; **never** per-process counting (>2x observer
  cost). Enforced in three layers (`lib-perf-lint.sh`), an ALLOWLIST rather than a
  deny-list grep: perf is invoked in exactly ONE wrapper and any other invocation line
  must be explicitly marked; no such line may carry a per-process option TOKEN; and the
  wrapper checks its own argv at runtime. The predecessor was a pattern over source
  text, and five ordinary bash spellings bypassed two successive versions of it — a
  deny-list must anticipate every spelling and is silently permissive the moment it
  misses one.
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
* **The cold arm's `skipped-cold-arm` prewarm sentinel satisfies a COLD rep only, and
  a COLD rep must carry it EXACTLY.** A temperature-blind acceptance set let an
  UNPREWARMED WARM rep reach `prewarm_all_ok=true` — the prewarm guard satisfied by its
  own sentinel. Scoped in both directions: a *prewarmed* "cold" rep is refused too.
  An honestly recorded failure (`FAILED-exit-N`, `unrecorded`) stays a flagged
  degradation **on a WARM rep**, whose prewarm is a real operation that can really fail
  and whose bias is self-limiting (an unprewarmed "warm" rep reads SLOWER). On a **COLD**
  rep any other value — including `unrecorded`, i.e. no `<tag>.prewarm.status` file at
  all — is a **REFUSAL**: a cold rep has no prewarm leg to fail (`lib-measure.sh` writes
  the sentinel unconditionally and only a warm rep runs a prewarm), so such a value means
  nothing establishes the rep was not prewarmed — and that bias is UNBOUNDED, because a
  secretly-warm rep reported cold reads FASTER, flattering its own figure. It used to be
  captioned and its rows/s, ratio and PASS/BELOW-TARGET verdict published anyway.
  `PREWARM_DEGRADATION_ADMITTED` states this per temperature and is read affirmatively,
  so a temperature it has no entry for refuses rather than inheriting the permissive
  branch.
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

## The flight arm can be moved ONE property at a time (issue #3551)

Three flags change the **Flight arm only**; the bare-scan arm always stays on
`--server-cpus` and on the system allocator, which is what makes it a
**code-identical AND pin-identical** leg in the same session — §3b step 3's drift
control, which this rig has never had.

```bash
# arm B vs a Flight server on two DISTINCT physical cores (SMT unpin), bare scan unchanged
scripts/perf/ws0-baseline.sh --corpus /data/ws0-3096   --flight-server-cpus 2,3 --flight-pin-mode distinct-cores

# arm C: the same binary, the Flight SERVER PROCESS ONLY under jemalloc
scripts/perf/ws0-baseline.sh --corpus /data/ws0-3096 --flight-allocator jemalloc
```

* **Omitting all of them changes nothing.** `--flight-server-cpus` defaults to
  `--server-cpus`, the mode defaults to `siblings` and the allocator to `system`, so
  every pre-#3551 invocation produces the same `taskset` lists, the same `perf -C`
  domains and the same recorded manifest values.
* **`--flight-pin-mode` is not a relaxation of the sibling guard.** Both values are
  read from the real `thread_siblings_list` and both fail closed: `siblings` REFUSES a
  distinct-core set, `distinct-cores` REFUSES a sibling pair — and refuses a single-CPU
  list, over which "pairwise distinct" compares nothing.
* **The counting domain follows the arm.** `perf stat -C` counts where each arm's server
  actually ran; counting `--server-cpus` while the Flight server ran elsewhere would
  divide another core's cycles by this rep's rows.
* **`--flight-allocator jemalloc` is VERIFIED FROM THE RUNNING PROCESS, per rep.**
  `LD_PRELOAD` fails open — glibc prints `object ... cannot be preloaded ...: ignored`
  and continues with system malloc, exit 0 — so without reading `/proc/<pid>/maps` arm C
  would be a byte-identical duplicate of arm B under a label saying otherwise. The
  **negative** is asserted on the system arm too (no jemalloc mapping, and any inherited
  `LD_PRELOAD` is emptied for the launch), because a control arm quietly running the
  allocator under test does not add noise — it inverts the comparison.
* **Everything above is recorded in `pinning-verification.json` and asserted where it is
  used**: the manifest's flight pin must EQUAL the list the driver verified (the #3272-F6
  substitution check, at the new field), and `ws0_report.py` prints the property that was
  actually read — a `distinct-cores` pin is never described as `physical-core siblings`.
  Guards: `scripts/tests/test_ws0_flight_arm_guards.sh` (in the gate's `tooling-tests`).

## No cross-session absolutes, and NO DRIFT CONTROL — read every difference as uncontrolled

**Measured on the delivery box in one day: the untouched warm bare scan read
370,134 rows/s and, an hour later, 333,206 rows/s — a ~10% drift with nothing
changed on the measured path.** So this rig produces **no reusable absolute**.

`docs/reports/ws0-3096-artifacts/measurement-method.md` §3b specifies the control
that would make a cross-arm comparison readable — same-session interleaved A/B/C
with a drift control code-identical across arms, "or NO COMPARISON".

**#3551 supplies the CONTROL LEG of that specification and NOT the interleaving.**
The bare-scan arm is now mechanically guaranteed to be code-identical AND pin-identical
across a comparison whose only moving part is the Flight pin or the Flight allocator
(above), which is step 3. Steps 1, 2 and 4 — one rep at a time, rotate the arm order,
difference WITHIN a round — remain **operator procedure that nothing verifies**, so the
paragraph below is unchanged in substance: no session-ordering property is established
by any artifact this rig writes.

**The interleaving control is NOT IMPLEMENTED OR ENFORCED by this rig, and the rig makes
NO INTERLEAVING CLAIM (#3272 review round 4).** What `ws0-baseline.sh` does have is a
loop ordered rounds-outside/arms-inside with the arm order rotated by round and the
bare scan rotating as a peer — a reasonable ordering, but **not a verified control**:
nothing downstream establishes that a session ran that way. An earlier round of
#3272 printed "the reps were INTERLEAVED … OBSERVED FROM THE CLOCK" and carried
`round_major_verified: true` in `results.json`; at the default `--reps 1` there is
one round, so **zero orderings were compared** while the verdict still said true.
The claim and its verdict fields were **deleted** rather than re-worded a third
time. Re-adding an OBSERVED control on real hardware is tracked by **#3287/#3299**.

What the rig *does* do with the per-rep round metadata: `ws0_report.py` requires all
four recorded fields (`round`/`position`/`arms_in_round`/`monotonic_ns`),
**integrity-checks** them (every arm covering the same rounds, positions `1..n`
exactly once, `arms_in_round` matching the arms present, no two reps sharing an
instant, and the round labels not contradicting the recorded instants — any
violation is a REFUSAL), carries them into `results.json` under
`recorded_round_metadata` as **inert recorded data**, and prints the **paired
per-round ratios and the within-round direction count** beside the medians.

Read the direction count, and read it as *uncontrolled for drift*: #3096's lever 4
measured `+2.3%` by medians and **zero** over 8 rounds (median −0.03%, 4 of 8
positive). Worked example with all 30 per-run numbers, and a discarded run with the
reason it was discarded:
`docs/reports/ws0-3096-artifacts/abc-interleaved-2026-08-03.md`.

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
