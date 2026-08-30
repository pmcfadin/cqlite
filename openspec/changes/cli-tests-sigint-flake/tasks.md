# Tasks — cli-tests-sigint-flake (issue #3515)

## 1. Test harness scaffolding (`cqlite-cli/tests/graceful_shutdown_tests.rs`)
- [x] 1.1 Drain `stderr`: take the handle and spawn a reader alongside the existing stdout reader.
- [x] 1.2 Give the readers a shared, lockable **transcript** so a failure can print what the child
      actually said (today every non-matching line is discarded).
- [x] 1.3 Add the calibration helper: `clamp(base × scale, base, cap)` with
      `scale = max(1, observed / quiet_baseline)`; unit-assert `scale == 1` on a quiet observation.
- [x] 1.4 Add a stage/total budget tracker so the test fails with its own message before nextest's
      240s hard kill.
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
