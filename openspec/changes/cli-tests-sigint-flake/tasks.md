# Tasks — cli-tests-sigint-flake (issue #3515)

## 1. Test harness scaffolding (`cqlite-cli/tests/graceful_shutdown_tests.rs`)
- [ ] 1.1 Drain `stderr`: take the handle and spawn a reader alongside the existing stdout reader.
- [ ] 1.2 Give the readers a shared, lockable **transcript** so a failure can print what the child
      actually said (today every non-matching line is discarded).
- [ ] 1.3 Add the calibration helper: `clamp(base × scale, base, cap)` with
      `scale = max(1, observed / quiet_baseline)`; unit-assert `scale == 1` on a quiet observation.
- [ ] 1.4 Add a stage/total budget tracker so the test fails with its own message before nextest's
      240s hard kill.
- [ ] 1.5 Rework `wait_for_line` to return the transcript-bearing outcome instead of a bare `Option`.

## 2. `sigint_in_writable_session_flushes_before_exit`
- [ ] 2.1 Stage (a): wait for the readiness banner; record `t_boot`. Bare deadline, honest message,
      commented as the irreducible bound.
- [ ] 2.2 Stage (b): time the `OK` round-trip → `t_ack`; budget calibrated from `t_boot`.
      Replace the "no interactive writable session" message.
- [ ] 2.3 Stage (c): after `SIGINT`, wait for the handler-entry marker; budget calibrated from
      `t_ack`. Message names the awaited substring, prints the transcript, lists candidate causes
      without selecting one.
- [ ] 2.4 Stage (d): progress-checked exit wait (new stderr/stdout line **or** new `-Data.db`
      resets the stall window); budget calibrated from `t_ack`.
      **Delete** `no graceful shutdown handler`.
- [ ] 2.5 Keep the durability assertions unchanged (independent read-only reopen, row id=7).

## 3. `writable_session_auto_flushes_mid_session_across_threshold` (AC4)
- [ ] 3.1 Stage (a) readiness banner + `t_boot`, as above.
- [ ] 3.2 Per-write ack waits calibrated; drop the "session dead-ended" claim.
- [ ] 3.3 `wait_for_sstable` calibrated + progress-checked; drop the "did not use the
      threshold-flushing path" claim.
- [ ] 3.4 EOF exit wait calibrated + progress-checked.

## 4. Verification
- [ ] 4.1 Green standalone:
      `cargo test -p cqlite-cli --features write-support --test graceful_shutdown_tests`.
- [ ] 4.2 **Green under real contention** — re-run while the box is loaded, and record the
      per-stage timings + derived budgets. This is the AC1 reproduction; an isolated pass is not one.
- [ ] 4.3 **RED-verify (AC3), for real, both defects**, each in a throwaway `git worktree` so the
      lane's tree is never left mutated:
      - remove the `ctrl_c` branch of `run_writable_interactive` → must red at **stage (c)**;
      - make the shutdown flush hang → must red at **stage (d)** with the flush-did-not-complete
        message (NOT a handler claim).
      Record both outcomes verbatim in the PR body.
- [ ] 4.4 Grep the file to confirm no unestablishable-cause string survives.
- [ ] 4.5 `scripts/agent-gate.sh --lite` green each fix round (summary-file redirect).

## 5. Doctrine
- [ ] 5.1 This is a test-oracle change with no user-facing or workflow surface, so CLAUDE.md needs no
      edit. Confirm that judgement explicitly rather than skipping the check — and if the
      scheduling-sensitive-oracle class is worth a doctrine line, propose it as a follow-up
      (`coord:follow-up-proposed`) rather than widening this diff.
