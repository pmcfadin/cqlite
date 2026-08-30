# Tasks — cli-tests-sigint-flake (issue #3515)

> ## READ THIS BEFORE THE ROUNDS BELOW — ROUNDS 4-7 ARE SUPERSEDED (round 8)
>
> Round 8 **DESCOPED the per-stage calibrated budget layer** to ONE per-test deadline (`design.md`
> D6a). Every symbol the round 4-7 records name — `StageSpec`, `T1_*`/`T2_*` stage specs,
> `STALL_WINDOW`, `SESSION_UP_DEADLINE`, `Budget`, `calibrated`, `bare`, `PollBudget`,
> `progress_checked`, `StageClock`, `clip`, `clip_poll`, `clipped_to_total`, `starved`,
> `declared_max`, `T1_TOTAL_BUDGET`/`T2_TOTAL_BUDGET`, `NON_STAGE_HEADROOM`,
> `MEASURED_QUIET_T_*`, `*_BASELINE_MULTIPLE`, `notice_if_anchor_is_permissive`, `quiet_anchors`,
> `PollGaveUp` and its three variants — **no longer exists**, and so do the unit tests named there
> (`no_wait_is_tighter_than_the_bound_it_replaced`,
> `every_stages_declared_maximum_fits_its_test_total_budget`,
> `the_nominal_cap_sums_stay_under_the_total_budget`,
> `the_baselines_sit_just_above_the_measured_quiet_noise_floor`, the three `calibration_*` tests,
> `a_bare_budget_names_itself_as_uncalibrated`, `the_stage_clock_clips_*`,
> `a_progress_checked_stages_extension_is_inside_its_declared_maximum`).
>
> They are kept verbatim as a record of **what was tried and what it cost** — that census is the
> justification for the descope — not as a description of the code. Read "Round 8" first; the tasks
> in sections 1-5 below are marked done against the round-7 design; the "Round 8" section at the end
> of this file records how each surviving property is re-satisfied.

## 1. Test harness scaffolding (`cqlite-cli/tests/graceful_shutdown_tests.rs`)
- [x] 1.1 Drain `stderr`: take the handle and spawn a reader alongside the existing stdout reader.
- [x] 1.2 Give the readers a shared, lockable **transcript** so a failure can print what the child
      actually said (today every non-matching line is discarded).
- [x] 1.3 Add the calibration helper: `clamp(base × scale, base, cap)` with
      `scale = max(1, observed / quiet_baseline)`; unit-assert `scale == 1` on a quiet observation.
- [x] 1.4 Add a stage/total budget tracker so the test fails with its own message rather than
      running unbounded. (Originally worded "before nextest's 240s hard kill"; **that premise was
      verified FALSE in round 7** — nothing runs `cqlite-cli`'s tests under nextest, so the total
      budget is the ONLY timeout this test has. See "Round 7".)
- [x] 1.5 Rework `wait_for_line` to return the transcript-bearing outcome instead of a bare `Option`.

## 2. `sigint_in_writable_session_flushes_before_exit`
- [x] 2.1 Stage (a): wait for the readiness banner; record `t_boot`. Bare deadline, honest message,
      commented as the irreducible bound.
- [x] 2.2 Stage (b): time the `OK` round-trip → `t_ack`; budget calibrated from `t_boot`.
      Replace the "no interactive writable session" message.
- [x] 2.3 Stage (c): after `SIGINT`, wait for the handler-entry marker; budget calibrated from
      `t_ack`. Message names the awaited substring, prints the transcript, lists candidate causes
      without selecting one.
- [x] 2.4 Stage (d): progress-checked exit wait (new stderr/stdout line **or** new `-Data.db`
      resets the stall window); budget calibrated from `t_ack`.
      **Delete** `no graceful shutdown handler`.
- [x] 2.5 Keep the durability assertions unchanged (independent read-only reopen, row id=7).

## 3. `writable_session_auto_flushes_mid_session_across_threshold` (AC4)
- [x] 3.1 Stage (a) readiness banner + `t_boot`, as above.
- [x] 3.2 Per-write ack waits calibrated; drop the "session dead-ended" claim.
- [x] 3.3 `wait_for_sstable` calibrated + progress-checked; drop the "did not use the
      threshold-flushing path" claim.
- [x] 3.4 EOF exit wait calibrated + progress-checked.

## 4. Verification
- [x] 4.1 Green standalone:
      `cargo test -p cqlite-cli --features write-support --test graceful_shutdown_tests`.
- [x] 4.2 **Green under real contention** — re-run while the box is loaded, and record the
      per-stage timings + derived budgets. This is the AC1 reproduction; an isolated pass is not one.
- [x] 4.3 **RED-verify (AC3), for real, both defects**, each in a throwaway `git worktree` so the
      lane's tree is never left mutated:
      - remove the `ctrl_c` branch of `run_writable_interactive` → must red at **stage (c)**;
      - make the shutdown flush hang → must red at **stage (d)** with the flush-did-not-complete
        message (NOT a handler claim).
      Record both outcomes verbatim in the PR body.
- [x] 4.4 Grep the file to confirm no unestablishable-cause string survives.
- [x] 4.5 `scripts/agent-gate.sh --lite` green each fix round (summary-file redirect).

## 5. Doctrine
- [x] 5.1 This is a test-oracle change with no user-facing or workflow surface, so CLAUDE.md needs no
      edit. Confirm that judgement explicitly rather than skipping the check — and if the
      scheduling-sensitive-oracle class is worth a doctrine line, propose it as a follow-up
      (`coord:follow-up-proposed`) rather than widening this diff.

### Verification record (issue #3515)

Quiet host (16 cores, load ~5, warm build, `--test-threads=1`) — 6/6 pass in 0.30s:

| stage | test 1 measured | derived budget | test 2 measured | derived budget |
|---|---|---|---|---|
| a. session-up (bare) | 24.1ms | 40s (bare) | 21.4ms | 40s (bare) |
| b. write ack | 3.0ms | 15s (scale 1.000 from t_boot) | 43.1ms (slowest of 5) | 8s (scale 1.000) |
| c. handler-entry / mid-session flush | 82.7us | 15s (scale 1.000 from t_ack) | 205us | 20s (scale 1.000) |
| d. clean-exit / eof-exit | 35.6ms | 25s (scale 1.000 from t_ack) | 1.6ms | 20s (scale 1.000) |
| stall window | — | 5s (scale 1.000) | — | 5s (scale 1.000) |

Loaded host, self-generated contention (40 spinners + 4 dd/sync loops, load avg 28-31 on 16
cores) — 6/6 pass in 0.70s: t_boot 66.2ms, t_ack 12.8ms, d.clean-exit 54.0ms; sibling t_boot
44.7ms, t_ack 96.9ms, eof-exit 27.7ms. All budgets still at `base` (scale 1.000).

Heavier: 220 spinners, load avg 96-116 (~7x oversubscription) — 6/6 pass in 1.28s: t_boot
80.8ms, t_ack 76.0ms, d.clean-exit 115.6ms; sibling t_boot 131.9ms, t_ack 133.1ms. Budgets
still at `base`: at 7x oversubscription the slowest stage consumes 0.5% of its budget.

RED verification (AC3), each in a throwaway `git worktree add --detach` (both removed after;
the lane tree was never mutated):

* **handler removed** (the `ctrl_c` branch of `run_writable_interactive` deleted) — FAILED at
  **stage (c) handler-entry** in 0.03s, naming the awaited substring `"Received Ctrl-C"`, the
  budget derivation, the pipes-at-EOF observation, the three candidate causes without selecting
  one, and the transcript.
* **flush hung** (600s sleep before `engine.close()` inside `shutdown_flush_and_exit`) — FAILED
  at **stage (d) clean-exit** after 25.0s with "the shutdown flush did not complete within the
  budget", stating that the handler-entry marker WAS observed 73us after SIGINT and that the
  failure says nothing about whether a handler is present.

5.1: judgement confirmed — this change alters one test file's oracle. It adds no user-facing
surface, no workflow, no gate component and no doctrine-visible behaviour, so CLAUDE.md and the
`agents-developing/` site need no edit. The scheduling-sensitive-oracle class (#3127, #3438,
#3515) may be worth a doctrine line; proposed as a follow-up rather than widening this diff.

### Round 2 (lead review of the first pass)

* `select_rows` was the one remaining unbounded wait on a child process
  (`Command::output()` has no timeout) and sat OUTSIDE the total budget, where an
  overrun lands on nextest's hard kill. It is now **stage (e) durability-read**:
  bounded, calibrated from `t_boot`, pipes drained on threads, with its own
  attributed message. Nominal cap sums re-budgeted to **175s <= 180s** for each
  test (test 1: 40+25+25+50+35; test 2: 40+5x10+25+25+35).
* The handler marker's leading `\n` does split the output but benignly — an empty
  line, then a line CONTAINING the substring — verified from a RED run's `cat -A`
  transcript and recorded in code at the constant, together with why no earlier
  stage can consume the marker line.
* Quiet re-run after both changes: 6/6 in 0.31s. Test 1 a 29.3ms / b 3.3ms /
  c 478.7us / d 40.2ms / **e 7.0ms**; test 2 a 23.8ms / b 40.4ms / c 217.8us /
  d 1.5ms / **e 11.5ms**. All budgets at `base` (scale 1.000).
* Both RED plants re-verified against the FINAL file (the caps had changed, so the
  round-1 evidence was not automatically valid): plant A -> stage (c) in 0.03s;
  plant B -> stage (d) after 25.09s, still reporting the handler-entry marker was
  observed 191us after SIGINT. Worktrees removed; lane tree never mutated.

### Round 3 — the floor invariant (lead blocker: the change was TIGHTER than what it replaced)

The round-1/2 stage (d) was `base 25s` where the old code had a flat 60s, and the hung-flush RED
run failed at exactly **25.0s** — proving the regression, because a silent flush produces no
progress events, so the stall window is already satisfied and the effective bound IS `derived`.

> **ROUND-7 CORRECTION, read before the table below.** Everything in rounds 3-6 that reasons about a
> "240s hard kill" rests on a premise verified FALSE in round 7: nothing runs `cqlite-cli`'s tests
> under nextest, so `.config/nextest.toml`'s slow-timeout never applied here. The DECLARED EXCEPTION
> rows in the table, and the whole "the sibling's guarantee is weaker" line of argument, were
> consequences of that fiction. They are retained as history; see "Round 7" for what replaced them.

**Old bound -> new stages mapping (the floor invariant, BY COMPOSITION).** Each group's BASES must
sum to at least the old bound. Asserted by
`no_wait_is_tighter_than_the_bound_it_replaced`, not by this table.

| old bound (pre-#3515) | what it covered | new stages | new base sum | verdict |
|---|---|---|---|---|
| test 1 `wait_for_line(OK, 60s)` | spawn + boot + read + execute + print | (a) 40s + (b) 25s | **65s** | >= 60s OK |
| test 1 `wait_timeout(60s)` after SIGINT | handler entry + flush + exit | (c) 20s + (d) 60s | **80s** | >= 60s OK |
| — (none: `select_rows` was unbounded) | read-side durability SELECT | (e) 25s | — | NEW ceiling |
| test 2 per-write ack, id=0, 60s | boot + write round-trip | (a) 40s + (b0) 25s | **65s** | >= 60s OK |
| test 2 per-write ack, id=1..4, 60s each | write round-trip | (b1..4) 10s each | 10s | **DECLARED EXCEPTION** |
| test 2 `wait_for_sstable(60s)` | mid-session flush | (c) 35s | 35s | **DECLARED EXCEPTION** |
| test 2 `wait_timeout(60s)` on EOF | flush + finalize + exit | (d) 35s | 35s | **DECLARED EXCEPTION** |
| — (none: unbounded) | read-side durability SELECT | (e) 20s | — | NEW ceiling |

**Why the exception is unavoidable, stated rather than hidden:** the sibling's old bounds were
SEVEN independent 60s deadlines = **420s nominal** against nextest's **240s hard kill**, so they
were never simultaneously realizable — a run that used them would have been KILLED with no message,
the outcome this change exists to prevent. For the sibling, "60s per stage" is a nominal figure and
the realizable old bound on any late stage was "whatever remains of 240s", which is exactly what
`StageClock::clip` now computes, with an attributed message instead of a kill. Those three groups
are floored at `SIBLING_STAGE_FLOOR` (10s — a constant deleted in round 4; this paragraph is the
round-3 position, retained as history) and the sibling's total base sum is held at >= 3x the old
bound (195s >= 180s). This IS a reduction in two nominal ceilings (60s -> 35s).

`TEST_TOTAL_BUDGET` 180s -> **230s**; nominal cap sums 220s (test 1) and 221s (test 2), both under
it and under the 240s kill.

**Baselines cut to the measured noise floor.** 500ms -> **100ms** (t_boot) and 200ms -> **50ms**
(t_ack). Rationale: `scale = observed / quiet_baseline`, so a baseline far above the quiet
measurement makes the mechanism INERT — measured, `scale` stayed at EXACTLY 1.000 in every run
including load average 116. The asymmetry that makes small baselines safe: calibration can only
LOOSEN, so over-eager engagement cannot fail a test, while under-eager engagement is the real hazard.

**FIRST OBSERVED FIRING of the calibration** (220 spinners, load avg 91 -> 151, 10/10 pass in 1.14s):

| | measured | derived budget |
|---|---|---|
| test 1 (b) write-ack | t_boot 206.2ms | **30.00s = clamp(base 25s x scale 2.062, .., cap 30s)** |
| test 1 (e) durability-read | t_boot 206.2ms | **35.00s = base 25s x 2.062, capped** |
| test 1 (d) clean-exit | t_ack 3.0ms | 60.00s (scale 1.000) |
| test 2 (c) mid-session-flush | t_ack 103.7ms | **40.00s = base 35s x scale 2.074, capped** |
| test 2 (d) eof-exit | t_ack 103.7ms | **40.00s = base 35s x 2.074, capped** |
| test 2 stall window | t_ack 103.7ms | **10.37s = base 5s x 2.074** |

**Calibration unit tests (new, all green):** `no_wait_is_tighter_than_the_bound_it_replaced`,
`the_nominal_cap_sums_stay_under_the_total_budget`,
`the_baselines_sit_just_above_the_measured_quiet_noise_floor`,
`calibration_engages_on_a_contended_observation`. 10/10 pass in 0.18s quiet.

**The asserts were themselves RED-verified** (each plant applied in the lane, run, reverted; tree
clean after):

| plant | result |
|---|---|
| stage (d) base back to 25s | RED: `no_wait_is_tighter_than_the_bound_it_replaced` |
| sibling sstable base under the declared floor | RED: same test, the floor loop |
| a cap sum pushed over the total | RED: `the_nominal_cap_sums_stay_under_the_total_budget` |
| `ACK_QUIET_BASELINE` inflated 1000x | **GREEN — a defect in the assert**, fixed; see below |

The fourth plant exposed a real defect in my own test:
`calibration_engages_on_a_contended_observation` derived its observation FROM the baseline
(`ACK_QUIET_BASELINE * 8`), making it invariant to the baseline's value — so the exact defect that
left the calibration inert through every real run left it green. **A test whose input is scaled by
the constant under examination cannot detect a wrong value for that constant.** Split: the formula
test keeps baseline-relative inputs and says so; a new test asserts the baselines against the
recorded MEASURED quiet values (`MEASURED_QUIET_T_BOOT`/`MEASURED_QUIET_T_ACK`) — at or above them,
and no more than 10x above them — plus that an observation 10x the measured floor actually moves
stage (d)'s budget. Re-planted: RED, with
`ACK_QUIET_BASELINE 50s is more than 10x the measured quiet t_ack 43ms: the calibration would be inert`.

**Product RED plants re-run at the new floors:** plant A (handler removed) -> **stage (c)**, 0.02s;
plant B (flush hangs) -> **stage (d)** after **60.08s** (was 25.0s), i.e. exactly the old bound, no
longer sooner than it.

### What the loaded runs do NOT establish (AC1, honest scope)

The three loaded runs (40 spinners/load 30; 220 spinners/load 116; 220 spinners/load 151) show the
tests are fast and pass under synthetic CPU oversubscription, and — after the baseline fix — that
the calibration engages. They **do NOT reproduce #3515's condition.** The reported failure came
from six concurrent `agent-gate.sh` processes, which contend for page cache, memory bandwidth, disk
I/O and process/thread slots as well as CPU; N spin loops contend for CPU alone. The observed
inflation here was ~7x on `t_boot`/`t_ack`, against the ~175x the issue implies on the shutdown
stage. So: **#3515's condition is NOT reproduced.** What is established is (i) the stage that flaked
is no longer bounded below what it was, (ii) it is progress-checked, (iii) the calibration
demonstrably fires under real contention, and (iv) no failure message asserts an unestablishable
cause. Whether the class is closed on the real gate host is not shown by these runs.

### Round 3 addendum — file-size split

The floor-invariant work pushed the single test file to 1664 lines and the lite
gate's `file-size` component FAILed it (limit 1500, `334 -> 1664`). Split by
responsibility per #1135: `tests/graceful_shutdown_support/mod.rs` (1262 lines)
holds the instrument plus the unit tests that constrain it;
`tests/graceful_shutdown_tests.rs` (423 lines) holds only the two integration
tests. Deliberately NOT `tests/common/`: that module is included by ~10 other
test targets, each of which would compile an unused harness and trip `dead_code`
under `-D warnings`. Lite re-run: PASS (all five components).

## Round 4 — roborev job 219 (3 findings, all fixed)

**Finding 1 (BLOCKER) — the sibling's per-operation caps were tighter than what they replaced.**
`T2_ACK_LATER: spec(10, 12)` capped each of writes id=1..4 at 12s against four INDEPENDENT 60s
waits. The "DECLARED EXCEPTION" that justified it (7x60s = 420s nominal vs a 240s kill) is true in
aggregate and **irrelevant per operation**: under the old code any single contended write could use
the full 60s provided its siblings were fast, so a 12s cap failed it with ~200s of envelope unused —
the round-3 blocker relocated. Fixed by separating the two properties:

| stage | before | after | old bound | per-op floor |
|---|---|---|---|---|
| (b1..4) per-write ack | `spec(10, 12)` | `spec(60, 70)`, aggregate bounded by `StageClock::clip` | 60s each | ✓ 60s |
| (c) mid-session flush | `spec(35, 40)` | `spec(60, 70)` | 60s | ✓ 60s |
| (d) EOF exit | `spec(35, 40)` | `spec(60, 70)` | 60s | ✓ 60s |

**`StageClock` IS the group deadline** (settled in round 5; an intermediate version of this fix added
a separate `GroupBudget` type, which was a second mechanism for a job the clock already did — see
round 5). Each sibling stage carries the FULL old 60s bound as its base, and `clip` enforces the
aggregate against what has ACTUALLY been consumed of `TEST_TOTAL_BUDGET`. So a single contended
operation still reaches the full old ceiling when its siblings ran fast, any reduction is contingent
on genuine exhaustion, and a stage that cannot even reach its own base is marked `starved` and says
so. `SIBLING_STAGE_FLOOR` and the DECLARED EXCEPTION are **deleted**, not reworded — the sibling now
satisfies the floor directly.

**The position, stated honestly, because it IS a reduction relative to the `GroupBudget` design:**
with the clock alone, several slow early acks CAN consume envelope that later stages then do not get.
The `GroupBudget` capped the four acks' SUM and so protected the tail. The difference is not that the
loss cannot happen — it is that a stage which loses now NAMES the exhaustion
(`TOTAL BUDGET ALREADY EXHAUSTED BY EARLIER STAGES ... A failure here is about the budget, NOT about
the property under test`) instead of failing as though the property did not hold. On a host slow
enough for four acks to eat ~200s the test cannot pass inside the 240s kill under either design, so
what is actually lost is the ability to attribute the loss to the acks rather than to the tail — and
that is bought back by the `starved` marker naming which stages had already consumed the budget.

**Finding 2 (BLOCKER) — a new uncalibrated 5s bound.** The read-side pipe collection's hardcoded
`recv_timeout(5s)` is now bounded by stage (e)'s CALIBRATED budget, itself bounded by
`clock.remaining()` (wall-clock derived, so it has already absorbed the `wait_timeout`). No fixed
constant survives. A genuine channel disconnect is distinguished from a timeout.

**Finding 3 (roborev Low; BLOCKER under AC2) — `TotalBudgetExhausted` asserted an unestablishable
cause.** It claimed "progress was still arriving" on a branch that only establishes the envelope
expired before the stall window elapsed, and which can fire having observed ZERO lines and ZERO
artifacts (`last_progress` is initialised at poll entry). It now reports only that ordering, says
explicitly that it does not require any progress to have been observed, and prints
`progress observed while polling: NONE` when nothing was seen.

**Baseline anchors re-based on the spec's BINDING rule** (smallest relevant quiet value, because the
anchor forms an UPPER bound and "slowest" is the permissive direction): `MEASURED_QUIET_T_BOOT`
29ms -> 11ms, `MEASURED_QUIET_T_ACK` 43ms -> 3ms, `BOOT_QUIET_BASELINE` 100ms -> 75ms (6.8x),
`ACK_QUIET_BASELINE` 50ms -> 25ms (8.3x). Trade recorded in code: the sibling's quiet t_ack (~42ms)
now sits above the baseline, so it scales ~1.7 on an unloaded host — harmless (calibration only
loosens) and preferable to licensing an inert baseline.

### Round-4 verification

* 10/10 green quiet, 0.28s. `RUSTFLAGS="-D warnings" cargo clippy -p cqlite-cli --tests` clean.
* **New asserts RED-verified** (each plant applied to a COMMITTED tree, run, reverted):
  per-op ack ceiling back to `spec(10,12)` -> RED (`sibling stage (b1..4) per-write ack is 10s,
  tighter than the 60s it replaced`); group clip made unconditional -> RED (`nothing has been
  consumed, so nothing may be clipped`); ack baseline 25->50ms -> RED (`more than 10x the measured
  quiet t_ack 3ms: the calibration would be inert`); boot baseline ->500ms -> RED; ack baseline 1ms
  (below the anchor) -> RED. The cap-sum assert also caught a real 235s>230s over-allocation in the
  first draft of the group fix.
* A probe-method defect was found in this round; see **"The RED-verification method can itself
  return a false GREEN"** at the end of this file, which is where the rule now lives.
* **Product plants re-run at HEAD:** plant A (handler removed) -> stage (c), 0.02s. Plant B (flush
  hangs) -> stage (d) after **60.08s**, `progress observed while polling: NONE`, still reporting the
  handler-entry marker was observed 300.427us after SIGINT. The sibling passed in the same run.

## Round 5 — the clock IS the group deadline (lead design note)

`GroupBudget` is DELETED. `StageClock` already bounded the aggregate, so the new type was a second
mechanism for the same job. Every sibling stage that replaced an INDEPENDENT 60s wait now carries the
FULL old bound as its base and the clock bounds the aggregate by subtracting what has ACTUALLY been
consumed:

| stage | spec | old bound | reaches it from a fresh clock? |
|---|---|---|---|
| (b1..4) per-write ack | `spec(60, 70)` | 60s each | yes (asserted) |
| (c) mid-session flush | `spec(60, 70)` | 60s | yes (asserted) |
| (d) EOF exit | `spec(60, 70)` | 60s | yes (asserted) |

The three consequences, handled rather than absorbed:

1. **The floor assert now measures the DERIVED ceiling from a fresh `StageClock`**, not the spec
   constants — a clip that silently reduced them would otherwise pass — and additionally requires the
   budget to be neither clipped nor starved when nothing has been consumed.
2. **`the_nominal_cap_sums_stay_under_the_total_budget` is re-scoped, not deleted.** Test 1 keeps the
   plain nominal-sum assert. The sibling's nominal ceilings deliberately do NOT fit the envelope, and
   that is now ASSERTED (with a message telling a future editor to promote the sibling to the
   stronger assert if the arithmetic ever changes). In its place the sibling asserts the property that
   actually holds: `clip` never returns more than the remaining total, a stage that cannot reach its
   base is marked, and it names itself. The file states plainly that the sibling's guarantee is weaker
   than test 1's rather than choosing whichever assert passes.
3. **`Budget::starved`** — set when the remaining total is below the stage's own base. `describe()`
   then leads with `TOTAL BUDGET ALREADY EXHAUSTED BY EARLIER STAGES ... A failure here is about the
   budget, NOT about the property under test`, so a stage clipped to near zero failing on its first
   poll is distinguishable from the property genuinely not holding.

`the_stage_clock_clips_a_budget_to_the_remaining_total` now covers both states separately:
clipped-but-not-starved (calibration headroom taken back, base intact) and starved (base unreachable,
distinct message).

**Residual, accepted and stated:** the deleted group total also capped the SUM of the four acks, which
protected the later stages from being starved by slow acks. Under clock-only that scenario instead
starves stage (c)/(d) — which is why consequence 3 is load-bearing rather than cosmetic: the failure
names the exhaustion instead of reading as "no durable artifact appeared".

### Round-5 RED verification, and a METHOD defect worth more than the result

Producer-only plants (targeted by line, against a committed tree):

| plant | result |
|---|---|
| `T2_SSTABLE` base back to 35s | RED: `(c) mid-session flush derives 35s from a FRESH clock, tighter than the 60s it replaced` |
| starvation never marked (`budget.starved = false`) | RED: `T2_ACK_LATER could not reach its 60s base and must be marked starved` |
| starved marker text removed (line 544 only) | RED: `must name the exhaustion` (2 tests) |
| starved disclaimer removed (line 546 only) | RED: `a starved stage must disclaim the property` |

**METHOD DEFECT (second one this issue, same shape).** Two of those plants first reported GREEN when
applied with a whole-file `sed`, because the asserted phrase and the message that produces it are the
same literal in the same file — the substitution rewrote BOTH the producer and the expectation, so the
test could not see the change. That is the artifact-as-its-own-oracle shape: a plant that edits both
sides of an equality proves nothing. Plants against a message string must therefore target the
PRODUCER only (here, by line number), and the earlier lesson still applies (plant against a COMMITTED
tree, and print whether the plant actually applied). Both defects were in the verification method, not
the code — but both would have produced a false all-clear.

## The RED-verification method can itself return a false GREEN

**This is the most transferable finding in this issue, and it is recorded on its own because a false
GREEN in the RED-verification method is strictly worse than a false green in a test: this is the tool
that certifies the tests.** It happened TWICE here, in two different ways, and in both cases the
suite's output was indistinguishable from a correct all-clear.

*Instance 1 — the plant never reached the code under test.* The probe applied a plant, ran the suite,
then `git checkout --` to revert. With UNCOMMITTED work in the tree, that revert discarded the fix
being verified, so the plants ran against the previously COMMITTED constants and two of them reported
GREEN. Nothing in the output said the plant had not applied.

*Instance 2 — the plant edited both sides of the equality.* A whole-file `sed` on a failure-message
phrase rewrote BOTH the message that produces it and the assert that expects it, so the test could not
see the change and reported GREEN. This is the artifact-as-its-own-oracle shape (the same shape as a
baseline test that scales its input by the constant under examination).

The rule, in three parts:

* **(a) A plant must be applied to a COMMITTED tree.** Otherwise the revert step destroys the very
  change under verification.
* **(b) The probe must PRINT whether the plant actually applied** (`git diff --stat` before running,
  or a diff of the planted line). A `sed` that matches nothing is otherwise indistinguishable from an
  assert that does not fire.
* **(c) The general rule: VERIFY THAT THE PLANT TOOK EFFECT, not merely that the suite went red or
  stayed green.** A red proves only that something failed; a green proves nothing at all until the
  plant is known to have reached the compiled code. For a plant on a message string this additionally
  means targeting the PRODUCER only — never a substitution that also rewrites the expectation.

Two review rounds in this issue were spent on constants whose guards looked fine. This is the reason a
guard can look fine.

## Round 6 — roborev job 222 (3 findings, all fixed)

**Finding 1 (BLOCKER) — stage (e) could consume 2x its cap.** The pipe collection got a FRESH
`budget.derived` after the child wait had already spent part of the same stage budget, and the
returned timing excluded the collection. The envelope survived (`clip` reads the real clock), but
`the_nominal_cap_sums_stay_under_the_total_budget` computed a worst case the code could exceed — the
assert was no longer a bound on the thing it names, i.e. a guard measuring a proxy. Fixed exactly as
proposed: the collection gets `budget.derived - started.elapsed()`, clipped to `clock.remaining()`,
and the stage's reported duration now INCLUDES the collection so the timing and the invariant
describe the same quantity. The failure message names how much of the budget the child wait spent.

**Finding 2 (BLOCKER) — the anchor contradicted the measurement recorded above it.**
`MEASURED_QUIET_T_ACK` was 3ms against a recorded BINDING value of 1.4ms, so the `<= 10x` guard
permitted 30ms where 10x the binding value is 14ms, and `ACK_QUIET_BASELINE = 25ms` was ~18x while
its own comment claimed "~8.3x". **Name the asymmetry precisely, because it is the third instance in this issue and the pattern is the
finding:** `MEASURED_QUIET_T_BOOT` was `11ms` against a recorded `11.4ms` — rounded **DOWN**, the
STRICT direction, which is correct. `MEASURED_QUIET_T_ACK` was `3ms` against a recorded binding
`1.4ms` — rounded **UP**, the PERMISSIVE direction. Both anchors form an UPPER bound on their
baseline, so rounding up loosens the guard. That is the same error as the earlier "slowest observed"
anchor (round 4) and as the original "generous quiet_baseline" (round 3): each time, a value that
bounds something from above was chosen as though it bounded from below. So the CLASS is closed rather
than the instance:

* anchors are `from_micros`, rounded DOWN (the strict direction): `8_700us` / `1_400us`. **NOTE the
  `t_boot` anchor is 8.7ms, not the 11.4ms first committed:** the permissive-anchor NOTICE (below)
  fired on its first run and showed 11.4ms was itself permissive on this host, so it was lowered to
  the smallest value actually recorded. `BOOT_QUIET_BASELINE` is therefore 60.9ms, not the 79.8ms that
  11.4ms x 7 would give;
* **both baselines are DERIVED** as `anchor * multiple`, so a constant cannot disagree with the data
  above it — there is no second number to drift. `BOOT_QUIET_BASELINE = 8.7ms x 7 = 60.9ms`,
  `ACK_QUIET_BASELINE = 1.4ms x 8 = 11.2ms`;
* the guard PRINTS the computed multiple, asserts it equals the DECLARED one (so a reintroduced
  literal reds) and bounds it by `MAX_BASELINE_MULTIPLE`;
* hand-written multiples are gone from the doc comments. That prose was the reason nobody noticed:
  hand-written arithmetic decays exactly like a stale comment, and this finding WAS that decay.

**Effect on engagement, which is the point of the fix.** Calibration now engages above 11.2ms of
`t_ack` instead of 25ms:

| host | `t_ack` | scale before (25ms baseline) | scale now (11.2ms baseline) |
|---|---|---|---|
| load avg 116 | 103.7ms | 2.07 (first observed firing) | **~9.2** |
| load avg 30 | 96.9ms | 1.94 | ~8.7 |
| sibling, IDLE | ~43ms | 1.00 | **~3.8** (measured 3.972) |

The last row is the consequence recorded honestly since round 4: the sibling's quiet `t_ack` sits
above the baseline, so that test loosens its own budgets ~3.8x on an idle host. Harmless by the
asymmetry the whole mechanism rests on — calibration can only ever LOOSEN, so an over-eager `scale`
delays a failure and can never cause one — and strictly preferable to a baseline so high that the
mechanism never engages on the host #3515 actually measured.

**Finding 3 (roborev Low; must fix) — the ack stage recorded the wrong quantity.**
`clock.record("b.write-acks", t_ack)` recorded the slowest SINGLE ack as the stage duration.
Measured under-report: **44.171ms recorded for a 209.128ms stage**, ~4.7x. Budget accounting is
untouched (`record` is diagnostic-only, `remaining()` reads the real clock); `t_ack` remains the
calibration input, because a later stage should scale with how slow ONE round-trip is rather than with
how many were done, and the stage's elapsed time is now measured over the whole loop. The report names
both: `5 writes in 209.128ms (slowest single ack 44.171ms, which is the calibration input)`.

### A NEW residual, found by RED-verifying the finding-2 fix

Deriving each baseline from its anchor makes the MULTIPLE undriftable and the ANCHOR unverifiable:
planting a permissive anchor (1.4ms -> 3ms) scales the baseline with it, the ratio stays 8x, and every
assert passes. That is finding 2 one level down, and no unit test can settle it — the anchor is a
MEASUREMENT with nothing to compare against. The integration tests, however, measure `t_boot` and
`t_ack` on every run, so `notice_if_anchor_is_permissive` prints a NOTICE when an observed quiet value
falls BELOW its anchor. Deliberately a NOTICE and not a failure: a host faster than the recorded floor
is not the author's doing, and a lane that reds on correct input is the lane people learn to waive —
FAIL where the author can act, NOTICE where only the information is actionable.

**It fired on its first run** (observed t_boot 9.670ms against an 11.400ms anchor), so the anchor was
in fact permissive; it is now the smallest value actually recorded, 8.7ms. A future NOTICE on a faster
host is expected and is not a treadmill to chase: lowering the anchor only ever tightens the bound.

### Round-6 file-size split

Round 6 pushed `graceful_shutdown_support/mod.rs` to 1584 lines, over the 1500 test threshold, so
`file-size` would have FAILed the next lite. Split by responsibility:
`budgets.rs` (931) holds the clock and the budgets — stage specs, the floor-invariant mapping,
anchors/baselines, `Budget`/`calibrated`/`bare`, `StageClock`, the NOTICE, and every unit test that
pins their invariants (with the constants they constrain, so a constant cannot be edited without its
guard in view); `mod.rs` (672) holds the child harness; `graceful_shutdown_tests.rs` (448) is
unchanged. Constraints re-verified: the test target name is untouched and is still the only
maxdepth-1 `*.rs` matching `graceful`, both files are inside the existing subdirectory so neither
becomes a cargo target, and the test count is unchanged at 10.

### Round-6 RED verification

| plant | result |
|---|---|
| reintroduce an independent literal baseline (25ms) | RED: `25ms is 17.86x its binding anchor ... over the 10x limit` |
| derive the baseline with a multiple disagreeing with the declared one | RED: `computes to 9.0000x its anchor but declares 8x — the baseline is no longer derived` |
| declared multiple raised to 18x | RED: `over the 10x limit: the calibration would be inert` |
| anchor back to the permissive 3ms | **GREEN — the residual above**, now covered by the NOTICE instead |

One plant reported `!! PLANT DID NOT APPLY` (a `sed` whose pattern no longer matched after `cargo fmt`
reflowed the constant onto one line) and was retried. That is rule (b) of the probe method working as
intended: without it, the run would have read as an assert that did not fire.

## Round 7 — roborev job 224 (3 findings), and THE FALSE PREMISE UNDER ALL SIX EARLIER ROUNDS

### 0. The premise, named first, because it dissolves most of finding 1

**Every round of this change up to and including round 6 was designed around
"`.config/nextest.toml` sets `slow-timeout = { period = "60s", terminate-after = 4 }`, so this test
is hard-killed at 240s, and `TEST_TOTAL_BUDGET` must stay under it."** That premise is **FALSE for
this test**. It originated in the lead's design note (`design.md` D6, now corrected there and in the
spec's total-budget requirement) and was never checked against the runners.

Verified, independently, before acting on it:

| claim | verification |
|---|---|
| `cli-tests` does not use nextest | `scripts/agent-gate.sh` runs plain `cargo test --package cqlite-cli` twice (default features, then `--features write-support`) |
| the gate's only nextest run is core | the single `cargo nextest run` in `agent-gate.sh` is `--package cqlite-core` |
| CI does not either | `ci.yml`'s nextest usage is the "Core integration" archive + 3 partitions; its CLI steps are plain `cargo test` and do not run this target at all |
| nothing wraps the run in a timeout | no `timeout`/`timeout-minutes` applies to `cli-tests`; libtest has no per-test timeout |

**So `.config/nextest.toml` never applied to `graceful_shutdown_tests`, and the total budget was
squeezed against a limit that does not exist.** That squeeze is what forced the sibling's nominal
allowances (513s) past the total (230s) — roborev job 224 finding 1 — and it is what generated the
"DECLARED EXCEPTION" and the "weaker sibling guarantee" that between them consumed three review
rounds. **The round-3 mapping table above rests on this premise; it is retained as history, and its
"240s hard kill" column is now known to be fictional.**

Consequence in the other direction, and the reason the total budget is not simply deleted: **it is
now the ONLY timeout this test has.** A wedged product must still be self-terminated with this
file's own attributed message rather than hang the `cli-tests` component, so the total stays, sized
to fit, and bounded above by `MAX_TEST_TOTAL_BUDGET` (900s, anchored on the full gate's own 15-20
minute wall clock).

### 1. THE STRUCTURAL FIX — a stage owns a DEADLINE

Findings 2 and 3, plus both round-2 findings and round-6's finding 1, are **ONE defect at five
sites**: `Budget` exposed a `derived: Duration`, every wait site received that same duration fresh,
and each site was separately responsible for subtracting what the stage had already spent. The sites
that forgot:

| round | site | what went uncharged |
|---|---|---|
| 2 | `select_rows`' `Command::output()` | the whole wait (unbounded, outside the budget) |
| 4 | the read-side pipe collection | a hardcoded 5s, then a hand-computed remainder |
| 6 (job 222 f1) | the collection again | a FRESH full `derived` after the child wait |
| 7 (job 224 f2) | `select_rows`' `wait_timeout` | the child SPAWN |
| 7 (job 224 f3) | `poll_with_progress` | the progress extension, unaccounted |

Patching the fifth would not have stopped a sixth. So **`derived` is deleted.** A `Budget` now
carries a `deadline: Instant` fixed when the budget is derived, and **`Budget::remaining()` is the
ONE place a per-wait timeout is computed**. Every wait — `ChildIo::wait_for`,
`Child::wait_timeout`, `Receiver::recv_timeout`, the poll — takes its timeout from that method, so:

* a stage cannot double-spend its allowance however many waits it performs;
* work between deriving the budget and the wait is charged automatically (which is finding 2's fix,
  and the same fix was applied to stage (a), where the process spawn was also uncharged);
* `poll_with_progress` no longer takes an `envelope` parameter — that was a sixth separately
  remembered subtraction at the call site (`let envelope = clock.remaining();`), now subsumed by
  `StageClock::clip` pulling the deadline in;
* the arithmetic has ONE well-defined quantity to sum (`Budget::span()`, the declared maximum).

**The progress extension is DECLARED, not removed** (finding 3 notes the extension is correct AC1
behaviour). `Budget::progress_checked(&stall_window)` returns a `PollBudget` — the only type
`poll_with_progress` accepts — extending the deadline by exactly one stall window and recording it,
so `declared_max` counts it. Forgetting it is a **compile** error, not an arithmetic discrepancy.
The stall window comes from the `PollBudget` too, so the extension the deadline grants and the
window a stall is judged against are one value and cannot disagree.

**The nominal budget is unchanged by the extension**, which is what keeps the round-3 floor: a
silent (progress-free) hang still fails at `nominal`, not at `nominal + stall`. Asserted in
`a_progress_checked_stages_extension_is_inside_its_declared_maximum`, and **measured**: product
plant B failed at **60.05s**, exactly `T1_EXIT.base`, with `progress observed while polling: NONE`.

### 2. The new totals, and the arithmetic behind them

`declared_max(spec, progress_checked) = spec.cap + if progress_checked { STALL_WINDOW.cap }`.
Per-test totals replace the single `TEST_TOTAL_BUDGET`, so each test's envelope fits ITS stages:

| test 1 stage | declared max | | test 2 stage | declared max |
|---|---|---|---|---|
| (a) session-up | 40s (bare) | | (a) session-up | 40s (bare) |
| (b) write-ack | 30s | | (b0) write-ack id=0 | 28s |
| (c) handler-entry | 30s | | (b1..4) write-ack x4 | 70s each = 280s |
| (d) clean-exit | 85 + 20 = **105s** | | (c) mid-session flush | 70 + 20 = **90s** |
| (e) durability-read | 35s | | (d) eof-exit | 70 + 20 = **90s** |
| | | | (e) durability-read | 25s |
| **sum** | **240s** | | **sum** | **553s** |
| `T1_TOTAL_BUDGET` | **270s** (30s spare) | | `T2_TOTAL_BUDGET` | **600s** (47s spare) |

`NON_STAGE_HEADROOM` = 20s (TempDir create + recursive teardown, schema write, `libc::kill`, JSON
parse, row assertions — all sub-millisecond measured, so this is generous by >1000x). The asserts
are `sum <= total`, `sum + headroom <= total`, and `total <= MAX_TEST_TOTAL_BUDGET`.

**The totals are DECLARED, not derived from the sums.** A total computed as `sum + headroom` would
make the fit assert tautological — the artifact-as-its-own-oracle shape this issue has now hit three
times (the baseline-relative calibration test, the whole-file `sed` plant, the derived anchor).

**600s is a maximum, not a runtime.** It is reachable only on a host that calibrates every stage to
its cap AND then consumes it; measured runtime is 0.3s quiet and 1.3s at load average 116, and a
genuinely hung flush still fails at 60s. Under the OLD code the sibling's seven 60s bounds were 420s
nominal with no harness kill to cut them short, so 600s is the FIRST total bound that test has ever
had, not a new ceiling.

**Deleted, not reworded:** `the_nominal_cap_sums_stay_under_the_total_budget`'s inverted
`sibling_nominal > TEST_TOTAL_BUDGET` assert, its three-part weaker fallback property, the
`NEXTEST_HARD_KILL` assert, and the surrounding "the sibling's guarantee is genuinely WEAKER"
prose. The inverted assert's own message told a future editor to promote the sibling if the
arithmetic ever fit; that instruction has been obeyed. Both tests are now asserted identically by
`every_stages_declared_maximum_fits_its_test_total_budget`. `StageClock::clip` and `Budget::starved`
survive as a **backstop** (non-stage work is bounded only by the headroom) and are documented as
such rather than as the primary bound.

### 3. Four stale claims the correction left behind

`grep -rn "240\|nextest\|hard kill"` across the change after the lead's `ec4b85eb0`:

* `spec.md`'s delta table still named the requirement *"…below the harness hard-kill"* after the
  requirement itself had been renamed;
* the floor-invariant requirement conditioned the GROUP DEADLINE on a group's nominal sum being
  "not simultaneously realizable against the harness hard-kill" — false in both halves now, so the
  group deadline is restated as a backstop and the per-operation floor as unconditional (the
  stronger of the two);
* `proposal.md` cited `.config/nextest.toml`'s `retries = 0` as the mechanism for a non-goal that
  nextest does not govern here (the non-goal holds a fortiori — there is no retry mechanism at all);
* **`select_rows`' stage (e) panic told its reader the message appeared "instead of the harness's
  240s hard kill".** A live failure message asserting a mechanism that does not exist is AC2's own
  defect class, inside the change that removes it.

### Round-7 verification

10/10 → **13/13** green (3 new asserts), 0.50s quiet. `cargo fmt` clean;
`RUSTFLAGS="-D warnings" cargo clippy -p cqlite-cli --tests --features write-support` clean;
`scripts/tests/check-no-wallclock-asserts.sh` OK (the deadline test's only comparison is in the
overshoot-safe direction — a sleep can only make "time was charged" MORE true — and the accessor is
named `spent()` rather than `elapsed()` so the guard's identifier is not shadowed by a legitimate
comparison).

**Product plants**, each applied to a COMMITTED tree in a throwaway `git worktree add --detach`,
with the planted hunk printed before the run (probe rules (a)-(c) below); both worktrees removed,
the lane tree never mutated:

| plant | result |
|---|---|
| handler removed (the `ctrl_c` branch of `run_writable_interactive` deleted) | **RED at stage (c) handler-entry** in 0.01s, naming the awaited substring `"Received Ctrl-C"`, the budget derivation, the pipes-at-EOF observation and the three candidate causes without selecting one |
| flush hung (600s sleep before `engine.close()`) | **RED at stage (d) clean-exit after 60.05s** — exactly `T1_EXIT.base`, i.e. the old bound, NOT extended by the stall window, with `progress observed while polling: NONE` and the statement that the handler-entry marker WAS observed 209.178µs after SIGINT |

**Assert plants** (producer-only, committed tree, plant-application printed; every one names the
line it fired at so the REASON is verified, not just the outcome):

| plant | red test | assert |
|---|---|---|
| `remaining()` returns `span()` (ignores what the stage spent) | `a_stages_waits_share_one_deadline_so_none_can_double_spend` | `only 0ns of 2s was charged across a 200ms gap` |
| `progress_checked` declares the extension but does not extend the deadline | `a_progress_checked_stages_extension_is_inside_its_declared_maximum` | `the declared maximum must be nominal + one stall window: 60s vs 65s` |
| `declared_max` drops the extension | same, **and** `every_stages_declared_maximum_fits_its_test_total_budget` | `declared_max must ADD exactly one stall window` |
| `T2_ACK_LATER` cap 70→120 (sum 753s) | `every_stages_declared_maximum_fits_…` | the `sum <= total` fit assert (line 915) |
| `T2_TOTAL_BUDGET` back to **230s** (the finding-1 regression itself) | `every_stages_declared_maximum_fits_…` | the same fit assert |
| `clip_poll` returns its input unclipped | `the_clock_clips_a_progress_checked_stages_extension_too` | `clipped_to_total: false` in the printed `Budget` |
| `T1_EXIT` base 60→25 (the round-3 regression) | `no_wait_is_tighter_than_the_bound_it_replaced` | the (c)+(d) group floor (line 802) |
| an independent literal `ACK_QUIET_BASELINE` (the round-6 regression) | `the_baselines_sit_just_above_the_measured_quiet_noise_floor` | `25ms is 17.86x its binding anchor … over the 10x limit` |

**What the deadline refactor does NOT close, stated because it is the honest boundary.** The
invariant is now true BY CONSTRUCTION rather than by assert: no field hands out the full span as a
timeout, so a call site cannot receive one by accident. But `Budget::span()` is public (the poll's
failure message reports the declared maximum), and a future edit that deliberately passed
`budget.span()` to a wait would compile and no unit test would see it. That is strictly better than
five sites each needing to remember a subtraction — the family has ONE remaining site and an assert
covers it (plant 1 above) — but it is not the same as impossible, and a source-shape lint over the
call sites is the descoped class (`CLAUDE.md`, #3499), not something to add here.

**File sizes after round 7:** `graceful_shutdown_tests.rs` 438, `graceful_shutdown_support/mod.rs`
719, `graceful_shutdown_support/budgets.rs` **1355** (limit 1500). The budget file has ~145 lines of
headroom; a round 8 of comparable size would need another split by responsibility (the natural seam
is the calibration anchors/baselines and their four guards, ~250 lines, versus the clock + deadline
layer).

## Round 8 — the DESCOPE: one per-test deadline (`design.md` D6a)

### The finding census that decided it

roborev reviewed this change four times (jobs 219, 222, 224, 229). **12 findings. All 12 in the
per-stage calibrated budget layer.** Count per round: **3, 3, 3, 3 — flat.** Over the same four
rounds the *oracle* (the staged waits, `MARKER_SESSION_READY`/`MARKER_HANDLER_ENTERED`, the stderr
draining + transcript, the honest failure messages) produced **zero** findings after round 3.

| round / job | findings, all in the budget layer |
|---|---|
| 219 | group-deadline vs per-operation cap; a hardcoded `recv_timeout(5s)`; `TotalBudgetExhausted` asserting an unestablishable cause |
| 222 | pipe collection given a fresh full budget; the hand-written baseline multiple that had drifted to ~18x; `t_ack` recorded as the stage duration |
| 224 | the cap sums exceeded the totals (starvation); the read-side spawn uncharged; the progress extension outside the declared maximum |
| 229 | summing per-stage caps does not preserve a SHARED old deadline; `clip` rewriting `nominal` mis-reports a starved stage; the poll's step running before the deadline check |

The repository's own precedent is to descope a mechanism whose defect count does not fall rather than
patch it a fifth time (the removed `census-exclusion:` key; the descoped ANSI parse lint → #3499;
#3384's withdrawn integration targets). **The load-bearing point is that the ACs never asked for the
calibration**: AC1 wants liveness confirmation instead of a bare deadline, and that is supplied by
stage (c)'s handler-entry marker — which proves signal delivery, handler entry and scheduling at once
— not by budget arithmetic.

Two of the round-4 findings **dissolve** rather than get fixed: 229/1 (a handler entering at 31s and
exiting at 32s passed the old flat 60s but failed a 30s `T1_HANDLER` cap) is exactly what one shared
deadline restores, and 229/2 (`clip` rewriting `nominal`) has no `clip` and no `starved` to be wrong
about. **229/3 was real and is fixed**: `poll_with_progress` now checks the deadline BEFORE invoking
`step` and passes `min(SLICE, remaining)`, so a stage can no longer succeed ~100ms past its bound.
Measured in the plant below: stage (d) gave up at `179.99s spent, remaining 0.00ns` against a 180.0s
deadline.

### What replaced it

`TestDeadline` — ONE deadline per test:

* `clamp(base × scale, base, cap)` with `scale = max(1, observed / QUIET_OBSERVATION_BASELINE)`,
  folded in via `calibrate(name, observed)` as each in-band measurement lands (`t_boot` after stage
  (a), `t_ack` after stage (b)). It keeps the **LARGEST** scale, so calibration is **monotone** — it
  can only ever move the deadline later, asserted by
  `calibration_takes_the_largest_scale_and_only_ever_loosens`.
* `T1_DEADLINE_BASE` **180s** / cap 360s; `T2_DEADLINE_BASE` **480s** / cap 720s; both caps under
  `MAX_TEST_DEADLINE` 900s (the full gate's own 15-20 min wall clock).
* **ONE** baseline constant, `QUIET_OBSERVATION_BASELINE`, for both observations — they measure the
  same shape of work. It is **bracketed by the recorded measurements**: above the slowest recorded
  QUIET value (43ms, the sibling's slowest ack) so an unloaded host yields `scale == 1` exactly, and
  below the least-scaled recorded CONTENTION CASE (45ms, the SIGINT run at load average 30) so
  contention demonstrably engages it. Both directions are asserted from the recorded table by
  `the_baseline_is_quiet_inert_and_contention_active` — which is what "not inert" actually means, and
  needs no anchor, no derived multiple and no NOTICE.
  *(ROUND 10 CORRECTION, roborev job 233 finding 2 — this bullet originally read
  `QUIET_OBSERVATION_BASELINE = 60ms` bracketed below "the fastest value recorded under real
  contention (81ms, `t_boot` at load average 116)". That label was false against the very table it
  cited, which records loaded observations of 13ms, 45ms and 76ms, and at 60ms the SIGINT test could
  stay entirely UNSCALED at the recorded load-average-30 timings. The value is now **44ms** and both
  ends are DERIVED from the table encoded as data in `budgets.rs`, per contention case. See round
  10.)*

`Stage` — attribution and nothing else: a name, a start instant, and a borrow of the deadline.
`Stage::remaining()` returns the **test's** remaining time, so there is no allowance to hand out, none
to double-spend and none for a call site to subtract. `Stage::finish()` records the stage's own
duration for the report.

### The properties that survive, and how

| property | how it holds now |
|---|---|
| no wait tighter than the 60s it replaced | `base >= OLD_BOUND`, asserted; any single stage may consume the whole deadline |
| the test not tighter in aggregate | `base >= OLD_BOUND × (old_waits + 1)` — the `+1` is the readiness stage the old code did not bound separately (round 9, roborev job 232 finding 2); 3 for T1, 8 for T2, asserted |
| no stage starved by an earlier one | **by construction**: no stage has an allowance to consume |
| no wait is granted or started past the one bound | **by construction**: one bound; nothing extends it; the deadline is checked before each step. Scoped (round 9): it bounds WAITING FOR evidence, not the acceptance of evidence in hand — an observed success is accepted up to one slice + one artifact scan late, deliberately |
| the deadline cannot outlast its gate component | `cap <= MAX_TEST_DEADLINE`, asserted |
| calibration only loosens | `scale` floored at 1, span clamped at `base`, largest scale retained; asserted |
| progress is evidence | `PollFail::observed()` reports `progress observed: NONE` / counts and says the counts do NOT extend the bound; `observed_progress_never_extends_the_deadline` asserts the poll TERMINATES under progress on every slice |

### What was deleted

`StageSpec` + `spec()` + all nine stage specs + `STALL_WINDOW`; `SESSION_UP_DEADLINE`; `Budget`,
`calibrated`, `bare`, `Budget::nominal/span/clipped_to_total/starved/progress_checked`; `PollBudget`;
`StageClock`, `clip`, `clip_poll`; `declared_max`, `t1_stages`, `t2_stages`; `T1_TOTAL_BUDGET`,
`T2_TOTAL_BUDGET`, `NON_STAGE_HEADROOM`, `MAX_TEST_TOTAL_BUDGET`; `MEASURED_QUIET_T_BOOT/ACK`,
`BOOT/ACK_BASELINE_MULTIPLE`, `MAX_BASELINE_MULTIPLE`, `BOOT/ACK_QUIET_BASELINE`,
`notice_if_anchor_is_permissive`, `quiet_anchors`; the three-variant `PollGaveUp`.
Unit tests removed: `no_wait_is_tighter_than_the_bound_it_replaced`,
`every_stages_declared_maximum_fits_its_test_total_budget`,
`the_baselines_sit_just_above_the_measured_quiet_noise_floor`,
`calibration_engages_on_a_contended_observation`,
`calibration_is_the_identity_on_a_quiet_observation`,
`calibration_only_ever_loosens_and_never_exceeds_the_cap`,
`a_bare_budget_names_itself_as_uncalibrated`,
`the_stage_clock_clips_a_budget_to_the_remaining_total`,
`a_progress_checked_stages_extension_is_inside_its_declared_maximum`,
`the_clock_clips_a_progress_checked_stages_extension_too`.
`a_stages_waits_share_one_deadline_so_none_can_double_spend` **survives**, renamed
`a_stages_waits_share_the_one_deadline_so_none_can_double_spend`.

### Untouched, deliberately — this is the change's actual value

The four/five staged waits and their order; `MARKER_SESSION_READY`/`MARKER_HANDLER_ENTERED`; the
stderr draining and shared transcript; every honest failure message, especially stage (c)'s
three-candidate-causes text and stage (d) never claiming anything about handler existence; the deleted
string `no graceful shutdown handler` stays deleted; the sibling test's equivalent treatment (AC4).

### Test count

**13 → 9.** 2 integration + 6 deadline unit tests in `budgets.rs` + 1 new harness unit test in
`mod.rs`. Ten unit tests were removed (listed above) because their subject no longer exists; three are
new (`the_deadline_is_never_tighter_than_the_bounds_it_replaced`,
`any_single_stage_may_consume_the_whole_deadline`,
`observed_progress_never_extends_the_deadline`) and two consolidate the surviving properties of five
old ones. `cargo test -p cqlite-cli --features write-support --test graceful_shutdown_tests`:
**9 passed, 0 failed, 0.30s.**

### Product RED plants (AC3), re-run on the descoped oracle

Both applied to a **COMMITTED** tree in a throwaway `git worktree add --detach`, each printing whether
its anchor matched before the run (round 5 recorded a false green from `git checkout --` reverting an
uncommitted plant).

**Plant A — the `ctrl_c` select branch removed from `run_writable_interactive`** (`PLANT A APPLIED:
ctrl_c select branch removed`, commit `cbdbfbf2a`, 1 file / 4 deletions). RED at **stage (c)** in
0.03s:

```
stage (c) handler-entry: the shutdown handler's entry marker was not observed on the child's stderr
after SIGINT was delivered to pid 129912.
awaited substring on stderr: "Received Ctrl-C"
stage c.handler-entry has been running 283.67µs. ONE per-test deadline 180.0s = clamp(base 180.0s x
scale 1.000, base, cap 360.0s), where scale is the LARGEST of [t_boot 23.500ms => scale 1.000, t_ack
1.477ms => scale 1.000] over quiet baseline 60ms. ...
how the wait ended: the child's stdout AND stderr both reached EOF after 280.156µs, so no further line
could arrive: the child had exited, crashed, or closed its pipes (this measurement does not say which)
CANDIDATE CAUSES (this measurement does NOT select between them): ...
```

**Plant B — `engine.close()` wrapped behind `std::future::pending()`** (`PLANT B APPLIED`, commit
`c4c054051`, 1 file / +2-1). RED at **stage (d)**, at the deadline:

```
stage (d) clean-exit: the shutdown flush did not complete before the deadline.
gave up after 179.99s, when the test's ONE deadline passed while this stage was pending — which is
what attributes the failure to this stage and to nothing else.
stage d.clean-exit has been running 179.99s. ... Spent 180.00s, remaining 0.00ns
progress observed while polling: NONE — 0 new output lines and 0 new durable artifacts in 179.99s
WHAT THIS ESTABLISHES: the handler-entry marker "Received Ctrl-C" WAS observed 68.300µs after SIGINT,
so the shutdown handler exists, was entered, and the child was scheduled. This failure therefore
establishes ONLY that the flush did not complete in time; it says nothing about whether a handler is
present.
durable -Data.db artifacts under /tmp/.tmpbJ7EPb/wd/data: 0
```

`test result: FAILED ... finished in 180.00s` — i.e. **exactly the deadline**, with `remaining 0.00ns`
and no slice-sized overshoot (the 229/3 fix, demonstrated rather than argued).

### The accepted cost, stated

Plant B previously red at **60s** (stage (d)'s nominal per-stage budget). It now reds at **180s**, the
test's whole deadline. **That is the entire price of the descope**, it is paid only on a real failure,
and it buys the elimination of a defect family four review rounds could not close. It is recorded as a
requirement in `spec.md` ("flush hung" scenario) so a future reader cannot mistake it for a
regression. The calibration's only benefit was a tighter bound on a quiet host; it was never what made
the oracle honest.

### File sizes after round 8

`graceful_shutdown_tests.rs` **367** (was 438), `graceful_shutdown_support/mod.rs` **728** (was 719),
`graceful_shutdown_support/budgets.rs` **591** (was 1355). All three well under the 1500-line test
threshold; the round-7 note about needing another split by responsibility is moot — the descope removed
~760 lines. The harness stays in the `graceful_shutdown_support/` subdirectory and the target name
`graceful_shutdown_tests` is unchanged (hardcoded in `agent-gate.sh`).

### Stale-reference sweep

`grep -rn` across code, `tasks.md`, `design.md` and `spec.md` for every deleted symbol. Results:
`spec.md` rewritten (three requirements merged into *The test is bounded by ONE deadline…*, each
withdrawn obligation NAMED where it stood); `design.md` D2/D3/D4/D6 and "The residual" corrected or
explicitly marked superseded by D6a; `tasks.md` rounds 4-7 kept verbatim as a record of what was
tried, under a banner at the head of this file naming every symbol they mention that no longer exists.
A record describing a deleted symbol is a claim about code and decays like a comment, so the banner
states that explicitly rather than leaving the reader to discover it.

## Round 9 — roborev job 232 (3 findings): one OVERRULE, one fix, one written-down limit

A deliberately small round on the descoped oracle. No product behaviour changed, and no harness
behaviour changed either: one finding was overruled and answered by scoping a claim, one strengthened
an assert, one was recorded as a known limit rather than fixed.

### 1. `mod.rs` `poll_with_progress` — OVERRULED. The claim was wrong, the behaviour is right.

**What roborev observed (accurate):** the poll returns `Ok` as soon as `step(SLICE.min(remaining))`
yields `Some`, without rechecking whether the deadline expired *during* `step` — so a stage can
succeed slightly past the deadline. **What it proposed:** recheck the deadline before returning `Ok`
and return `PollFail` when it has expired.

**That fix is rejected (lead ruling), and must not be implemented later either.** On the success path
the property has been OBSERVED — the child exited, or the durable artifact appeared. The deadline
exists to bound how long the test WAITS FOR EVIDENCE, not to reject evidence already in hand. Failing
a stage that observed its signal, because the loop noticed a few hundred milliseconds late, is a
**false failure on a working product** — precisely the flake class #3515 exists to remove. It would
also make the test's verdict depend on how long a `read_dir` walk happened to take, which is the
scheduling sensitivity this whole change eliminates. Two defects would be introduced to satisfy one
claim.

**What WAS wrong is the claim.** The harness stated, at seven sites, a property stronger than it has
("nothing may exceed the one deadline", "no step can complete past it", "the declared bound is the
actual maximum", "the instant no wait may outlive"). What it can support is:

> The one deadline bounds how long the test WAITS FOR EVIDENCE. No wait is GRANTED more than what the
> deadline leaves, and no wait is STARTED past it. A success OBSERVED while the deadline lapses is
> still a success, and accepting it is DELIBERATE — rejecting it would be a false failure on a working
> product.

**The overrun is quantified, so it is bounded rather than mysterious:** at most one
`SLICE.min(remaining)` (≤ 100ms) plus one `count_data_db` scan. That scan is a recursive `read_dir`
walk of the data directory and is **not necessarily quick on a loaded host** — the bound is real but
not tiny, and that is said where the claim is made. The same lag applies to the FAILURE path (declared
at the next loop top, not the instant the deadline passes); `PollFail` reports the stage's real spend,
so no message understates it.

Rescoped at every site the `grep -rn` sweep found: `poll_with_progress`'s doc comment (which owns the
decision and the quantification), the progress-observation block comment in `mod.rs`, `TestDeadline`'s
type doc and its `deadline` field comment, the `a_stages_waits_share_the_one_deadline_so_none_can_double_spend`
doc **and** its second assert message (reworded to "may never be GRANTED more than …", which is what
it actually asserts — `remaining()` arithmetic, never wall clock at the moment of a verdict), the
`graceful_shutdown_tests.rs` module header, `design.md` D3 and D6a, and the surviving-properties table
in this file (whose row is renamed from "a declared cap is the actual maximum").

### 2. `budgets.rs` floor invariant — FIXED. The aggregate omitted the readiness stage.

The aggregate assert summed only the **old** waits (`OLD_BOUND × old_waits` = 120s for T1, 420s for
T2). But `a.session-up` (spawn → readiness banner) is a stage the old code never bounded separately —
boot was folded INSIDE the first 60s `OK` wait — and it now draws on the same one deadline, which any
single stage may consume entirely. So a 120s/420s base passed the guard while permitting readiness to
eat 60s and leave every ORIGINAL wait below its former 60s allowance: the floor invariant violated by
a stage the sum did not mention.

Fixed by a named `NEW_READINESS_WAITS: u32 = 1` (named, not an inline `+ 1`, because it is a claim
about which stages exist and must be revisited when a stage is added) and
`base >= OLD_BOUND × (old_waits + NEW_READINESS_WAITS)` — 180s for T1, 480s for T2, which the shipped
bases meet **exactly**. The `T1_DEADLINE_BASE`/`T2_DEADLINE_BASE` doc comments, which stated the old
weaker arithmetic, were corrected with them.

#### RED verification (committed plants — round 5's false green came from `git checkout --` reverting an *uncommitted* plant)

Every plant was **committed** and the applied plant was re-read **from `git show HEAD:<file>`** with
`git status --porcelain` confirmed empty, so the running binary provably came from the planted source.

```text
(a) both bases planted, fixed assert   HEAD c389f49cb, worktree clean, HEAD grep shows 120s/420s
    RESULT: FAILED (1 failed)
    sigint_in_writable_session_flushes_before_exit: a base of 120s is below the 180s aggregate of
    the 2 independent 60s waits it replaced plus 1 readiness stage (3 stages share this one
    deadline, and any one of them may consume it)

(b) T2 base only, fixed assert         HEAD 4d134c752, worktree clean, HEAD grep shows 180s/420s
    RESULT: FAILED (1 failed)
    writable_session_auto_flushes_mid_session_across_threshold: a base of 420s is below the 480s
    aggregate of the 7 independent 60s waits it replaced plus 1 readiness stage (8 stages share
    this one deadline, and any one of them may consume it)

(c) THE FINDING'S PREMISE — pre-fix assert (budgets.rs restored from 32211006b, NEW_READINESS_WAITS
    absent) + the same 120s/420s bases     HEAD 52c4b6c83, worktree clean
    RESULT: ok (1 passed)   <-- the old guard ADMITTED the reduced bases, which is the finding
```

(b) exists because (a) panics at T1 and would have hidden a T2 term that never fired. (c) is the
control: without it, a red in (a) shows only that *something* fails, not that the previous form was
permissive. All three plant commits were dropped (`git reset --hard`) and the tree restored to
`f548b70e8`.

### 3. `mod.rs` `observed_progress_never_extends_the_deadline` — a KNOWN, WRITTEN-DOWN LIMIT, not fixed

**The weakness, stated precisely.** The assert gives a 300ms deadline and requires the poll to
terminate within 30s (a 100× margin). It therefore proves **TERMINATION under continuous progress —
that observed progress cannot make the poll run forever — and NOTHING about the tolerance to which the
deadline is respected.** A regression that let observed progress extend a 300ms deadline by, say, four
seconds — i.e. a partial return of the pre-descope crediting the round-8 descope removed — would still
pass this assert. The assert's own doc comment already says no timing threshold is asserted, so no
claim in the file is stale; what was missing was anyone writing down that this is a *gap*, not a
choice with no cost.

**Deliberately not fixed in this round (lead ruling).** Asserting a tolerance means asserting that
something completed FAST, which is the #2642 wall-clock flake class — the correct fix is an
**injectable clock** so the property can be asserted without measuring wall time, and that is genuine
design work disproportionate to a claim-scoping round. Recording it here makes it a known limit rather
than an assumed guarantee; the lead files it as a linked follow-up.

### Round-9 verification

* `cargo test -p cqlite-cli --features write-support --test graceful_shutdown_tests` — **9 passed, 0
  failed** (2 product tests + 7 harness unit tests), on the restored tree.
* The three committed RED plants above.
* **The product RED plants (AC3) were NOT re-run, deliberately.** This round changed no product code
  and no harness behaviour — only claim text and one assert's arithmetic — so round 8's plant evidence
  stands unchanged. Re-running a 180s plant to re-observe a result nothing could have moved is cost
  without information.
* `grep -rn` sweep for the rescoped phrases (`nothing may exceed`, `nothing can exceed`,
  `no step can complete past`, `may never exceed`, `actual maximum`, `may outlive`) across
  `cqlite-cli/tests/`, `design.md`, `spec.md`, `tasks.md`: every surviving occurrence is either
  scoped in place or is a historical round-4-7 record already under this file's staleness banner.

### File sizes after round 9

`graceful_shutdown_tests.rs` **363**, `graceful_shutdown_support/mod.rs` **754**,
`graceful_shutdown_support/budgets.rs` **646**. All three under the 1500-line test threshold.
