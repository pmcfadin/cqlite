# cli-tests-sigint-flake — retire the scheduling-sensitive wall-clock oracle in `graceful_shutdown_tests.rs`

Issue: #3515 (P2, bug). Routing: **design-driven** — AC1 is an oracle-design decision.

## Problem

`cqlite-cli/tests/graceful_shutdown_tests.rs::sigint_in_writable_session_flushes_before_exit`
fails inside the gate's `cli-tests` component on a contended host while passing standalone in
**0.34s** — a **~175×** gap against a **60s** ceiling. The failure text is

```
child did not exit after SIGINT (no graceful shutdown handler)
```

which asserts a **cause the measurement cannot establish**. A `wait_timeout` expiry means
"did not exit *in time on this box*"; it does not distinguish

* the shutdown handler is broken / absent, from
* the child was never scheduled to run it.

Raising 60s → 120s changes the *frequency* and not the *class*. Per the issue this is the third
recorded instance of one shape — a correctness test whose oracle is a scheduling-sensitive
wall-clock bound (#3127, #3438) — so the fix has to change the instrument, not the constant.

## What makes this tractable without touching product code

`cqlite-cli/src/main.rs` **already emits two in-band progress markers on stderr**:

* `run_writable_interactive` — a readiness banner, `cqlite writable session: enter CQL DML (…)`
* `shutdown_flush_and_exit`'s caller — **handler entry**, `Received Ctrl-C — flushing memtable before exit...`

and the test pipes `stderr` to `Stdio::piped()` and **never reads it**. AC1's "confirm the child was
actually scheduled (e.g. its own progress marker)" is therefore reachable with a **test-only diff**.

## Approach

Replace one bare deadline with a **staged wait**, where every stage's failure names only what its own
measurement establishes, and where the ceiling is **calibrated from measurements taken on this host,
at this moment**, rather than from a guessed constant:

| stage | awaited signal | what its expiry establishes |
|---|---|---|
| a. session up | readiness banner (stderr) | the child never reached the interactive loop |
| b. write ack | `OK` (stdout), **timed** → `t_ack` | the session accepted no write |
| c. handler entered | `Received Ctrl-C — flushing…` (stderr) | signal undelivered, handler not entered, **or** marker text drifted |
| d. clean exit | process exit, with progress **observed and reported** | the flush did not complete — explicitly **not** "no handler" |

`t_boot` (spawn → readiness) and `t_ack` (write → `OK`) calibrate the ceiling. Contention that would
blow a shutdown ceiling inflates those same measurements, so the ceiling scales with observed pressure
instead of guessing it.

**Round-8 correction (`design.md` D6a).** As first built there was one calibrated ceiling PER STAGE,
plus a total-budget clock over them. roborev returned 12 findings across four rounds, all 12 in that
layer, at a flat 3 per round, while the oracle proper produced none after round 3 — so it is
**descoped to ONE deadline per test**. The stages above remain, for ATTRIBUTION: which stage was
pending when the deadline passed is what names the failure. Any single stage may consume the whole
deadline, and observed progress is reported as evidence in the message but extends nothing.

The same treatment is applied to the sibling `writable_session_auto_flushes_mid_session_across_threshold`
(AC4), which carries the identical shape in **three** places, and to the first test's own ack message —
all four of which assert unestablishable causes today.

## Non-goals

* No `src/` change. No new product surface, no test-only product hooks.
* No `retries` for these tests, and a retry would mask exactly the defect the test exists to catch.
  (Round 7 correction: this originally cited `.config/nextest.toml`'s `retries = 0` as the mechanism.
  That file governs `cargo nextest run` only, and **nothing runs `cqlite-cli`'s tests under
  nextest** — `cli-tests` runs plain `cargo test` — so there is no retry mechanism in play at all
  here, and the non-goal holds a fortiori. See `design.md` D6.)
* Not a claim that the wall-clock bound is *eliminated*. The deadline's `base` is irreducibly
  uncalibrated until the first in-band measurement lands; the change makes that base generous (above
  the whole nominal aggregate of the bounds it replaced) and makes every message honest about which
  bound ended it. See `design.md` "The residual, stated at the seam".
