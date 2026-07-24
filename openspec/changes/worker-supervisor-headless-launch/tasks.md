# Tasks — worker-supervisor headless launch

## 1. Correct the default invocation
- [ ] 1.1 In `scripts/local/worker-supervisor.sh` (~L258), change the default `WORKER_CMD` to
      `claude -p --dangerously-skip-permissions --agent flow-lead '/worker'`. Update the
      surrounding comment to explain each flag (surface: the `if [[ -z "${WORKER_CMD:-}" ]]` block).

## 2. Fix the coupled orphan-detection probe
- [ ] 2.1 In `scripts/local/worker-supervisor.sh` (~L281), change `PROC_MATCH_WORKER` to
      `'[c]laude.*-p.*--agent flow-lead'` (matches the unattended spawn; excludes an interactive
      `--agent flow-lead` lead that has no `-p`, and a plain REPL). Update the comment block that
      documents the `--agent worker` keying (~L290-310, incl. the `#2670` references).

## 3. Close the `-p` watchdog gap (design decision A)
- [ ] 3.1 Ensure the worker's `-p` activity is captured into `iter-N.log` so
      `detect_prompt_signature`/`log_size` keep working — add the stream/verbose output flag to the
      default `WORKER_CMD` (or wrap the capture) so the redirect `>"$logfile"` is non-empty for a
      healthy worker (surface: `run_iteration`'s `bash -c "$WORKER_CMD" >"$logfile" 2>&1`).
- [ ] 3.2 If 3.1's stream format is unavailable/too noisy, fall back to design option C: document
      the watchdog as print-mode-incompatible in the runbook and name the breaker + wall-clock as
      the backstops. (Decision recorded in the PR.)

## 4. Tests (gate of record: `tooling-tests` component)
- [ ] 4.1 `scripts/tests/test_worker_supervisor.sh`: update the orphan-detection pattern test
      (~L1372-1380) to the new `-p.*--agent flow-lead` shape; add assertions that an interactive
      `claude --agent flow-lead` (no `-p`) and a plain `claude` REPL are NOT matched.
- [ ] 4.2 Add a test asserting the resolved default `WORKER_CMD` (caller unset) contains `-p`,
      `--dangerously-skip-permissions`, and `--agent flow-lead`.
- [ ] 4.3 Add/extend a test that a healthy (stubbed) worker produces a non-empty `iter-N.log` and
      that the wedge classifier still fires on a frozen-log + signature stub.

## 5. Doctrine (same change)
- [ ] 5.1 `docs/development/fleet-runbook.md`: correct the launch command + the `--agent worker`
      references (~L134-136); update the monitoring section (transcript vs iter-log; how to watch a
      `-p` worker).
- [ ] 5.2 `CLAUDE.md`: correct any `claude --agent worker` invocation to the validated form.
- [ ] 5.3 Website `agents-developing/` delivery-pipeline page: same launch-command correction.
- [ ] 5.4 Add a short note to the #2090 references so the historical doctrine points at the
      corrected invocation.

## 6. Gate + audit + review (endgame, via flow-closer)
- [ ] 6.1 `--lite` each fix round (summary-file redirect).
- [ ] 6.2 rust-reviewer (n/a — shell/docs) → skip; run `roborev review --branch --base origin/main`
      on the lite-green diff (review-first).
- [ ] 6.3 flow-closer: ONE full `scripts/agent-gate.sh` → spec-auditor (C) anchored to
      `openspec/changes/worker-supervisor-headless-launch/specs/**` → final roborev → merge-on-green
      → finalize.

## Notes
- No production Rust change → no no-heuristics / memory-budget impact.
- Shell + markdown only; the `file-size` ratchet and `fmt`/`clippy` components are unaffected but
  the full gate still runs as the gate of record.
- Do NOT restart or disturb the running fleet while implementing; the fix lands via PR and takes
  effect on the NEXT supervisor launch.
