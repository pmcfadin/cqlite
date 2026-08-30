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

## D2. How the ceilings are set — calibration from in-band measurement

Two measurements are taken before any shutdown ceiling is needed:

* `t_boot` = spawn → readiness banner (process spawn + dynamic link + engine init)
* `t_ack` = INSERT written → `OK` observed (a full read→execute→print round-trip through the child)

A stage's ceiling is `clamp(base × scale, base, cap)` where
`scale = max(1, observed / quiet_baseline)`. On the issue's measured 175× host, `t_ack` inflates by
the same factor the shutdown does, so the ceiling follows.

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

Therefore `quiet_baseline` SHALL sit just above the **recorded measured** quiet value for its
observation (single-digit multiples), not orders of magnitude above it. Because the constant is then
load-bearing, it is anchored to committed measurements and asserted against them by a unit test — and
that anchor must be the value that BINDS (the smallest relevant quiet measurement), since the anchor
is used as the basis of an UPPER bound on the baseline, where "slowest observed" is the permissive
direction.

**Why calibration and not just a bigger constant.** A constant has to be chosen for the worst host
anyone will ever run on, which makes a genuine hang cost that constant on every host. A calibrated
ceiling is tight on a quiet box (fast failure on a real defect) and loose on a saturated one (no
false alarm) — the property a constant cannot have in both directions at once.

## D3. The progress-checked exit wait

Stage (d) is not one `wait_timeout(D)`. It polls in short slices and treats any of these as
**progress**, resetting its stall window:

* a new stderr line from the child,
* an increase in the durable-artifact count (`count_data_db`), i.e. the flush is landing.

Exit ends the wait successfully. This is AC1's "unbounded-but-progress-checked loop" implemented
inside a bounded envelope, for the reason in D6.

## D4. Cause-honest failure messages (AC2)

Every stage failure reports, and reports *only*, what it measured:

* what was awaited, and for how long;
* **how the bound was derived** (`base × scale`, naming the observed measurement it came from);
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

## D6. Why the envelope is bounded — and it is not a free choice

`.config/nextest.toml` sets `slow-timeout = { period = "60s", terminate-after = 4 }`: a **240s hard
kill**. A literally unbounded loop under nextest therefore does not produce an honest diagnostic — it
produces a **nextest kill**, which is a strictly *worse* message than the one being removed. So the
test **owns its own total budget**, tracked across stages and set safely under 240s, and always emits
its own attributed failure before the harness can kill it.

Raising the slow-timeout for this binary was considered and rejected: it would put a gate-read
config file in the diff (voiding `--delta` re-certification, which fails closed on config) to buy
headroom that only matters on a run that is already failing.

## D7. Drain stderr

`stderr` is piped and never read today. Beyond discarding the evidence this change needs, an
undrained pipe is a latent wedge for any future chattier session. A reader thread is added, and both
readers accumulate a transcript for diagnostics (the current `wait_for_line` discards every
non-matching line, so a failure today can report nothing about what the child actually said).

## The residual, stated at the seam

**One bound remains uncalibrated and cannot be otherwise: stage (a).** Calibrating it would need a
measurement taken before it, whose own bound would need a measurement before *that* — the regress
terminates only by accepting one bare wall-clock deadline. What the design buys is that this one bound
covers the **cheapest** operation in the test (spawn + init, not a flush), and that its message says
exactly what its expiry means and nothing more.

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
