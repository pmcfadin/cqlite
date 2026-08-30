# Design — cli-tests-sigint-flake (issue #3515)

## D1. Where the liveness signal comes from

**Decision: the child's own stderr progress markers, already emitted by the product.**

Alternatives weighed:

* **`/proc/<pid>/stat` CPU time.** Rejected twice over. It is Linux-only (the test is
  `#![cfg(unix)]`, so macOS is in scope), and — decisively — **it does not discriminate**: a child
  deadlocked on a mutex inside `close()` and a child starved by the scheduler both consume **zero**
  CPU. An instrument that returns the same reading for the defect and for the false alarm is not an
  instrument.
* **A control probe at expiry** (spawn a trivial child, time it) to measure host schedulability.
  Kept only as a *reported diagnostic*, not as a verdict input — see D4. It answers "can this host
  schedule *anything*", which is weaker than "did this child get scheduled", and the markers answer
  the stronger question directly.
* **Add a machine-readable marker to the CLI.** Rejected as scope creep: it widens a test-quality
  diff into `src/` for test observability, when the existing human-readable markers already carry the
  fact. Recorded as a follow-up candidate under D5 instead.

The markers are strictly stronger evidence than any external probe, because observing
`Received Ctrl-C — flushing memtable before exit...` proves three things at once: the signal was
delivered, the handler **exists and was entered**, and the child **was scheduled**. That is precisely
the conjunction the current message guesses at.

## D2. How the ceiling is set — calibration from in-band measurement

**SUPERSEDED IN PART BY D6a (round 8).** This section is retained because its *reasoning about the
baseline* is what survives and is load-bearing. What does NOT survive is the per-stage form: there is
now ONE ceiling per test, not one per stage, and `quiet_baseline` is ONE constant rather than a
per-observation anchor with a derived multiple and a NOTICE. Read every "a stage's ceiling" below as
"the test's one deadline", and read the anchor/multiple paragraph as withdrawn — see D6a.

Two measurements are taken before any shutdown ceiling is needed:

* `t_boot` = spawn → readiness banner (process spawn + dynamic link + engine init)
* `t_ack` = INSERT written → `OK` observed (a full read→execute→print round-trip through the child)

The test's one deadline is `clamp(base × scale, base, cap)` where
`scale = max(1, observed / quiet_baseline)`, taken over the LARGEST scale either measurement yields.
On the issue's measured 175× host, `t_ack` inflates by the same factor the shutdown does, so the
ceiling follows.

**Where `quiet_baseline` sits — CORRECTED, and the first draft of this section was wrong (round 3).**
This document originally required `quiet_baseline` to be set *generously*, "seconds, not
milliseconds", reasoning that a large baseline guarantees `scale == 1` on a quiet host and so keeps
the calibration from becoming a flake source itself. **That instruction was wrong and it made the
mechanism inert:** measured, `scale` stayed at **exactly 1.000 in every run**, including load average
116 (~7× oversubscription) — a mechanism with zero observed firings, which is indistinguishable from
one that does not exist.

The error was failing to draw the conclusion from an asymmetry stated two paragraphs later:
**calibration can only ever LOOSEN a budget** (`scale` is floored at 1 and `derived` is clamped at
`base`). That property comes from the *formula*, not from the size of the baseline. So a spuriously
large `scale` cannot fail a test — it can only delay one — and there is **no quiet-side risk to
protect against**. Over-eager engagement is harmless; under-eager engagement is the only real hazard.

Therefore `quiet_baseline` must sit close to the **recorded measured** quiet values, not orders of
magnitude above them.

**How that requirement is met after D6a, and why the anchor factoring was withdrawn.** Rounds 5-7
expressed it as a per-observation ANCHOR (the smallest recorded quiet value) with a derived MULTIPLE
bounded at 10x, plus a run-time NOTICE when a host measured below its anchor. That factoring made the
multiple undriftable and the anchor *unverifiable* in exchange: planting a permissive anchor scaled the
baseline with it, the ratio held, and every assert still passed. With ONE baseline for one deadline the
property can be asserted directly and in BOTH directions, from two recorded measurements that bracket
it: above the SLOWEST recorded quiet observation (43ms), so an unloaded host yields `scale == 1`
exactly; and below the FASTEST observation recorded under real contention (81ms), so contention
demonstrably engages it. That is what "not inert" actually means, and it needs no anchor, no multiple
and no NOTICE.

**Why calibration and not just a bigger constant.** A constant has to be chosen for the worst host
anyone will ever run on, which makes a genuine hang cost that constant on every host. A calibrated
ceiling is tight on a quiet box (fast failure on a real defect) and loose on a saturated one (no
false alarm) — the property a constant cannot have in both directions at once.

## D3. The progress-OBSERVING exit wait

**CORRECTED BY D6a (round 8): progress is observed and reported, and CREDITS NOTHING.** As first
built, each progress event RESET a calibrated stall window and pushed the stage past its nominal
budget. That is exactly what made a declared per-stage cap not the actual maximum — the defect family
four review rounds could not close — so the crediting is gone.

Stage (d) is still not one `wait_timeout(D)`. It polls in short slices and OBSERVES:

* a new stderr line from the child,
* an increase in the durable-artifact count (`count_data_db`), i.e. the flush is landing.

Those observations are reported as EVIDENCE in any failure message — including an explicit
`progress observed: NONE` with zero counts, which is a materially different diagnosis from "the flush
was still landing when the deadline passed". Exit ends the wait successfully; the deadline ends it
otherwise, and is checked BEFORE each step is invoked so no step can complete past it.

This is what AC1's "unbounded-but-progress-checked loop" reduces to once the liveness question is
answered where it actually can be — by stage (c)'s handler-entry marker, not by a bound that progress
could move.

## D4. Cause-honest failure messages (AC2)

Every stage failure reports, and reports *only*, what it measured:

* what was awaited, and for how long;
* **how the one bound was derived** (`clamp(base × scale, base, cap)`, naming every observed
  measurement the scale was taken over);
* what *was* observed — the stderr/stdout transcript, the artifact count, the per-stage timings;
* an explicit statement of what is **not** established.

The string `no graceful shutdown handler` is **deleted**, not softened. It is not replaced by a
hedged version of the same claim at stage (d): at stage (d) the handler has been *observed to run*,
so the only honest statement is that the flush did not complete within the budget.

## D5. Accepted residual — coupling to product stderr text

Matching product strings couples the test to user-facing text. If that text changes, stage (c) stops
observing its marker and fails — a misattribution of a different kind, and a *silent* one.

Mitigations: match a short stable substring rather than the full sentence; and make stage (c)'s
failure message **name the substring it expected** and print the full transcript, so drift is
distinguishable from a real defect at a glance by the person reading the failure. This is a
reduction, not an elimination, and it is the honest cost of D1's choice over a product-side marker.

## D6. Why the envelope is bounded — and the FALSE PREMISE this section originally gave (round 7)

**CORRECTION. The first version of this section asserted that `.config/nextest.toml`'s
`slow-timeout = { period = "60s", terminate-after = 4 }` imposes a 240s hard kill on this test, and
required the total budget to sit "safely under 240s". THAT IS FALSE FOR THIS TEST, and it was the
lead's error, asserted in the original design note without being checked.** Verified:

* `scripts/agent-gate.sh`'s `cli-tests` component runs **plain `cargo test --package cqlite-cli`**;
* the gate's only `cargo nextest run` is `--package cqlite-core`;
* `ci.yml`'s nextest lanes are "Core integration" only.

**Nothing anywhere runs `cqlite-cli`'s tests under nextest**, so that slow-timeout never applies to
`graceful_shutdown_tests`. The cost of the error was not theoretical: squeezing the budget against a
limit that does not exist is what forced the sibling test's promised per-stage allowances to exceed
its total, which in turn produced the "declared exception" that consumed three review rounds and a
roborev blocker (a later stage could be starved into a FALSE failure while the product worked — the
exact flake class this change exists to remove).

**What survives the correction, and why the bound still exists.** Because no harness timeout applies,
the test's own deadline is now the ONLY thing that stops a genuinely wedged run from hanging a gate
component indefinitely. So the test still owns that bound and still emits its own attributed failure.
The self-imposed bound is load-bearing for a different reason than originally stated: not to beat a
harness to the punch, but to be the only bound there is — which is also why it is bounded above
(`MAX_TEST_DEADLINE`, anchored on the full gate's own 15-20 minute wall clock).

*(This paragraph originally continued "…sized so that every stage's promised allowance fits inside
it". D6a withdrew that: there are no stage allowances to fit. The bound's SIZE is now argued directly
against the aggregate of the bounds it replaced, and asserted by
`the_deadline_is_never_tighter_than_the_bounds_it_replaced`.)*

Raising nextest's slow-timeout was considered and rejected in the original draft as a way to buy
headroom. That rejection is now moot rather than right: there was no ceiling to raise.

## D6a. DESCOPE (round 8): the per-stage calibrated budget layer is replaced by ONE per-test deadline

**Decision, on a finding census rather than an argument.** roborev has run four rounds on this change
and returned **12 findings. All 12 are in the per-stage budget layer**, and the count per round is
flat — 3, 3, 3, 3 — while the *oracle* (staged waits, stderr progress markers, honest attribution) has
produced **zero** findings since round 3. This repository's own precedent is to descope a mechanism
whose defect count does not fall across review rounds rather than patch it again (the removed
`census-exclusion:` key, the descoped ANSI parse lint → #3499, #3384's withdrawn integration targets).
The same ruling applies here.

**The load-bearing realisation: the ACs never asked for the calibration.** AC1 asks for
"liveness confirmation rather than a bare deadline". That is supplied by **stage (c)'s handler-entry
marker**, which proves the signal was delivered, the handler was entered, and the child was scheduled.
The per-stage calibrated budgets were an addition of the lead's, and they are what generated every one
of the 12 findings — including the round-4 finding that *the composition rule itself was wrong*: summing
per-stage caps does not preserve a SHARED old deadline, so a handler entering at 31s and exiting at 32s
(which the old flat 60s allowed) now fails against a 30s `T1_HANDLER` cap.

**What replaces it.**

* **ONE deadline per test**, calibrated ONCE from the larger of the `t_boot`/`t_ack` scales, with a
  generous base and a cap. Any single stage may consume the whole of it, so the floor invariant
  ("never tighter than the bound it replaced") holds **unconditionally and trivially**, which is
  stronger than the group-deadline formulation it replaces.
* **Stages remain, purely for ATTRIBUTION.** Which stage was pending when the deadline passed is what
  names the failure — the property AC2 needs — and it no longer depends on any budget arithmetic.
* **Progress observation remains, as EVIDENCE IN THE MESSAGE, not as an input to the bound.** It
  reports `progress observed: NONE` / counts; it no longer extends anything. That removes the
  "declared cap is not the actual maximum" family at the root: there is one bound and nothing may
  exceed it.

**What is deleted:** per-stage `StageSpec` base/cap pairs, the quiet-baseline anchors and their derived
multiples, the permissive-anchor NOTICE, `clip`, `starved`, the floor-by-composition rule and its
assert, and the cap-sum assert. Roughly 900 lines of `budgets.rs`.

**The cost, stated plainly.** A genuine defect now takes the full deadline to surface rather than
failing fast against a tight per-stage cap: the hung-flush plant will red at the deadline instead of at
60s. That is the whole price, it is paid only on a real failure, and it buys the elimination of a defect
family that four review rounds could not close. The calibration's only benefit was a tighter bound on a
quiet host; it was not what made the oracle honest.

## D7. Drain stderr

`stderr` is piped and never read today. Beyond discarding the evidence this change needs, an
undrained pipe is a latent wedge for any future chattier session. A reader thread is added, and both
readers accumulate a transcript for diagnostics (the current `wait_for_line` discards every
non-matching line, so a failure today can report nothing about what the child actually said).

## The residual, stated at the seam

**The deadline's `base` is uncalibrated, and cannot be otherwise.** Calibrating it would need a
measurement taken before the test began, whose own bound would need a measurement before *that* — the
regress terminates only by accepting one bare wall-clock value. After D6a this is one fact in one
place rather than a per-stage exemption: stage (a) runs under the uncalibrated base (no measurement
exists yet), and the deadline loosens as soon as `t_boot` lands. What the design buys is that the base
is generous — above the whole nominal aggregate of the bounds it replaced — and that every failure
message says whether the bound that ended it was still uncalibrated.

Consequently the change is **not** a claim that #3515's class is eliminated for this file. It is a
claim that (i) the stage that actually flaked is now calibrated and progress-checked, (ii) no failure
message asserts a cause its measurement cannot establish, and (iii) the surviving bare bound is named
here rather than left to be rediscovered.

## D8. Preserving the RED property (AC3)

The change must not buy stability by weakening detection. Two real defects, and where each now lands:

* **Handler removed.** Default `SIGINT` disposition terminates the child. Stage (c) observes no
  handler-entry marker and fails first, with a correct message; had it survived to (d), the
  clean-exit and durability asserts still fail. Detection is *improved* — the diagnosis is now right.
* **Flush hangs.** Stage (c) passes (the handler was entered), stage (d) expires with "the flush did
  not complete", which is the true statement.

Both are to be demonstrated by actually breaking the handler, and the evidence recorded in the PR —
a test that no longer reds under load must still red on the real defect.
