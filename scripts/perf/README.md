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
| `lib-flight-arm.sh` | the two arms no longer run the same way — WHAT differs (pin, allocator, arena) and was it VERIFIED (#3551) |
| `ws0_flight_admission.py` | the server's admission ceiling, READ BACK from each rep's log and required to AGREE (#3551) |
| `ws0-3551-abc.sh` | the interleaved A/B/C(/C0) driver: one rep at a time, arm order rotated per round, one FROZEN binary set (#3551) |
| `ws0_abc_aggregate.py` | that set's paired per-round deltas and within-round direction count, with the drift control printed FIRST (#3551) |

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

Four flags change the **Flight arm only**; the bare-scan arm always stays on
`--server-cpus` and on the system allocator, which is what makes it a
**code-identical AND pin-identical** leg in the same session — §3b step 3's drift
control, which this rig has never had.

```bash
# arm B vs a Flight server on two DISTINCT physical cores (SMT unpin), bare scan unchanged
scripts/perf/ws0-baseline.sh --corpus /data/ws0-3096 \
  --flight-server-cpus 2,3 --flight-pin-mode distinct-cores

# arm C: the same binary, the Flight SERVER PROCESS ONLY under jemalloc
scripts/perf/ws0-baseline.sh --corpus /data/ws0-3096 --flight-allocator jemalloc

# arm C generalised — the mechanism under test is ARENA CONTENTION (#3217 partC F1's AC2)
scripts/perf/ws0-baseline.sh --corpus /data/ws0-3096 --flight-malloc-arena-max 1
```

* **Omitting all of them changes nothing.** `--flight-server-cpus` defaults to
  `--server-cpus`, the mode defaults to `siblings` and the allocator to `system`, so
  every pre-#3551 invocation produces the same `taskset` lists, the same `perf -C`
  domains and the same recorded manifest values.
* **`--flight-pin-mode` is not a relaxation of the sibling guard.** Both values are
  read from the real `thread_siblings_list` and both fail closed: `siblings` REFUSES a
  distinct-core set, `distinct-cores` REFUSES a sibling pair — and refuses a single-CPU
  list, over which "pairwise distinct" compares nothing.
* **The counting domain follows the arm, and FAILS CLOSED.** `perf stat -C` counts where each
  arm's server actually ran. Counting `--server-cpus` while the Flight server ran on `2,3` would
  collect cpu10's **idle** and miss cpu3's **work**, so the same rows cost FEWER cycles and the
  arm reads as a large win — a fabricated number in the flattering direction, invisible in the
  output. So each leg sets the domain on the line before its own window and `perf_stat_c`
  VALIDATES it against a CLOSED TABLE of the pairings this session verified
  (`<counted>|<affinity of the process in the window>`, derived from the verified lists). An
  empty domain, an absent table and an argv with no `taskset` CPU list are each named refusals:
  there is **no default**, because a silent fall-back to `--server-cpus` is how this defect would
  survive its own fix. Note the two legitimate pairings DIFFER — the Flight window brackets the
  LOAD GENERATOR on the client set while counting the SERVER — so "counted == pinned list" would
  red every correct Flight rep.
* **Every arm's figures name the CPUs they were counted on**, and `results.json` records
  `pinning.counted_cpus_by_arm`: with the pins separable, "cycles/row" means "hardware-thread
  cycles on THESE cpus per row", and the two arms may legitimately name different lists.
* **The ENVIRONMENT is recorded, ambient and injected SEPARATELY** (`config.env_ambient` /
  `config.env_injected`, printed and in `results.json`): with one binary set across all arms —
  deliberate — the environment is the only thing that distinguishes them, and it used to be
  recorded nowhere. `RUSTFLAGS`/`CARGO_ENCODED_RUSTFLAGS` are included AS MEASURED, per
  `docs/reports/ws0-3552-report.md` §4. An **ambient** `LD_PRELOAD`/`MALLOC_*` is REFUSED before
  the first rep — `ws0-scan-bench` would inherit it, putting the drift control on the allocator
  under test where the flight arm's own check cannot see it — and the bare-scan leg asserts per
  rep that it received neither.
* **The admission ceiling is READ BACK and required to agree** (`ws0_flight_admission.py`).
  `cqlite-flight` derives it as `clamp(2 x available_parallelism, 2, 64)` and
  `available_parallelism` respects the CPU affinity mask, so it is a FUNCTION OF THE PIN: a
  2-CPU pin and a 4-CPU pin differ in TWO properties. All three fields are recorded per rep and
  a session whose reps disagree is REFUSED. `--max-concurrent-scans` is deliberately NOT pinned:
  pinning would change the configuration #3248 measured and hide exactly this drift.
* **`--flight-allocator jemalloc` is VERIFIED FROM THE RUNNING PROCESS, per rep, from BOTH
  `/proc/<pid>/environ` AND `/proc/<pid>/maps`** — they prove different things and neither is
  sufficient. `environ` is what the process RECEIVED (and the ONLY way to see
  `--flight-malloc-arena-max` at all: an arena cap leaves no mapping); `maps` is what TOOK
  EFFECT, because `LD_PRELOAD` fails open — glibc prints `object ... cannot be preloaded ...:
  ignored` and continues with system malloc, exit 0 — so without it arm C would be a
  byte-identical duplicate of arm B under a label saying otherwise. `environ` is NUL-separated
  and matched as WHOLE ENTRIES with an exact value compare, so `MALLOC_ARENA_MAX=1` cannot be
  satisfied by `=16`. The **negative** is asserted on the system arm too (no jemalloc mapping and
  no non-empty `LD_PRELOAD` received), because a control arm quietly running the allocator under
  test does not add noise — it inverts the comparison.
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

**#3551 supplies the CONTROL LEG of that specification, and EXECUTES the other three
steps without VERIFYING them.** Read those as three distinct states, because collapsing
them is how this section acquired a false claim once already:

* **Step 3 (the drift control) is mechanically guaranteed.** The bare-scan arm is
  code-identical AND pin-identical across a comparison whose only moving part is the
  Flight pin or the Flight allocator (above), because `--server-cpus` is what it is
  pinned to and only `--flight-server-cpus` varies. `ws0_abc_aggregate.py` prints its
  cross-arm movement FIRST and declares any smaller treatment delta NOT READABLE.
* **Steps 1, 2 and 4 are now EXECUTED AND RECORDED, which they were not before.**
  `ws0-3551-abc.sh` runs one rep at a time, computes the per-round arm rotation itself,
  and writes each session's `round`, `position_in_round`, `arms_in_round` and the round's
  `order` to that session's own `abc-window.json`; `ws0_abc_aggregate.py` takes the
  difference WITHIN a round and prints the direction count. So the blanket sentence
  "no session-ordering property is established by any artifact this rig writes" is
  **true of `ws0-baseline.sh` and no longer true of the rig as a whole** — the A/B/C
  driver's artifacts do record one.
* **Nothing VERIFIES that ordering, and that is the whole of what is still missing.**
  The driver claims the order it EXECUTED, because it executed it; no artifact-side
  check establishes that a recorded order is the rotation step 2 asks for, or that the
  driver followed its own table. That is the OBSERVED control **#3287/#3299** own, and
  it is unchanged by #3551.

The distinction matters operationally: a reader may rely on the control leg, may read the
recorded positions as a description of what ran, and may **not** read either as an
observed interleaving control.

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

## An A/B/C SET is one experiment only if its sessions are comparable (issue #3551)

`ws0-baseline.sh` measures ONE configuration per invocation, so an A/B/C comparison is a SET of
its sessions in one directory. `ws0-3551-abc.sh` runs that set (rounds outside, arms inside,
order rotated per round, one `--bin-dir` measured by every arm) and `ws0_abc_aggregate.py`
reports it. Both had to be taught that a DIRECTORY LAYOUT is not a provenance claim.

* **The resume is CHECKED, not assumed.** A `(round, arm)` already holding a `results.json` is
  SKIPPED, which matters on a shared box — an interrupted set that has to start over loses its
  window. So the first invocation writes `$OUT/abc-run.json`, the RUN FINGERPRINT, and every
  later one VERIFIES it field by field: the corpus PATH **and** its recorded `Data.db` sha256 +
  row count (a path can be repopulated with a different corpus), the `--bin-dir` path **and** a
  digest of every measured binary in it (the arms must measure IDENTICAL BYTES — the whole
  reason `--bin-dir` is not per-arm), the arm SET and each arm's EXACT flag list, plus
  `--step-duration`, `--arena-max`, `--jemalloc-lib` and `--port`. A differing field is a
  REFUSAL naming the field and both values. **`--rounds` is deliberately NOT fingerprinted**:
  extending a set from 3 rounds to 5 over one `--out` is a legitimate resume, and a guard that
  reds on correct input is the guard an operator works around.
* **A skipped session must prove it is the session the slot expects.** `results.json` alone
  establishes no provenance — it is the reporter's output and carries no round, position or arm
  label of this set's vocabulary — so the session's own `abc-window.json` must exist, must name
  the arm and round of the directory it sits in, and must record `exit: 0`. The window is
  written for FAILED sessions on purpose (so a failure can be correlated against the box-load
  timeseries), which is exactly why a failed session's leftover `results.json` may not be
  adopted. Every refusal names the DIRECTORY.
* **Configuration is validated over EVERY `(round, arm)`, not read from the first.** Measurements
  are aggregated from every pairable round, so reading configuration from round one let a later
  round carry a different pin, allocator, arena cap, counter mode or admission ceiling and
  produce a delta ACROSS TREATMENTS. Two DISTINCT requirements: per-arm TREATMENT STABILITY
  (an arm's flight pin, pin mode, allocator, arena cap and counter mode identical in every
  round — a treatment that changed mid-set is not one arm) and CROSS-ARM INVARIANTS (the
  bare-scan pin, the client pin, the corpus identity, every binary digest and the admission
  triple identical across the whole set). All of it read back out of each session's OWN
  `results.json`, never re-derived from the driver's table: the job is to detect a divergence
  between what the driver INTENDED and what was MEASURED. An ABSENT field is COULD-NOT-MEASURE
  and is REFUSED with the field named.
* **`ratio bare/flight` is a ROWS/S ratio here as everywhere else in this rig** —
  `rows/s(bare) / rows/s(flight)`, above 1 when the bare scan is faster, the same quantity
  `ws0_report.py` prints and `ws0-3248-artifacts/ac0/DELTA-TABLE.md` reports — and
  `cycles/row delta` is `flight - bare`, absolute and percent, unconstrained in sign. The
  aggregator's Layer 1 printed the CYCLES quotient under that name, inverted with respect to
  its own label, so its tables were comparable with nothing.
* Guards: `scripts/tests/test_ws0_abc_driver_guards.sh` (in the gate's `tooling-tests`),
  hermetic — synthetic session dirs, corpus-identity and binary fixtures, and a recording STUB
  standing in for the measurement driver beside a scratch copy of the A/B/C driver, so no real
  measurement can run inside the gate.

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
