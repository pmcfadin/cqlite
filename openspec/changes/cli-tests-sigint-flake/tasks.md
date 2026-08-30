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
