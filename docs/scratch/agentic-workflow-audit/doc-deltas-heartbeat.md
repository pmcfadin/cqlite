# Doc deltas — claim heartbeat (issue #2089), for phase-2 doctrine wiring

Tooling half of #2089 is done: `scripts/flow/claim-heartbeat.sh` (`beat`/`list`/`clear`
against `refs/heartbeats/<machine>` on origin), its test
(`scripts/tests/test_claim_heartbeat.sh`), and the `flow-board` fleet view + deterministic
reap rule (steps 3a/4 of `.claude/skills/flow-board/SKILL.md`). This note records the
`flow-activate`/`flow-implement` stage-transition wiring a later phase-2 pass should apply
so the heartbeat is actually kept fresh during a claim's lifetime, not just readable by
`flow-board` for claims that happen to beat.

`flow-board` step 4's reap rule is load-bearing on this being wired: it reaps at
"heartbeat age > 4h AND no open PR". An issue's claim heartbeat that is **never beaten past
the initial claim** ages past 4h on every issue whose implementation legitimately takes
longer than 4h — the reap rule alone can't distinguish that from a dead session. Stage
transitions refreshing the heartbeat is what keeps a genuinely-alive multi-hour claim from
being incorrectly reaped.

## Where it belongs

1. **`.claude/skills/flow-activate/SKILL.md` step 3** ("Create the worktree + branch + PUSH
   it as the claim") — immediately after the claim-commit push succeeds and the re-read
   confirms the claim holds (around the `[ "$remote_sha" = "$local_sha" ]` check, currently
   line ~53-61), add:
   ```bash
   scripts/flow/claim-heartbeat.sh beat <N>
   ```
   This is the FIRST beat for the claim — establishes `refs/heartbeats/<machine>` the
   moment the claim is won, before any spec work.

2. **`.claude/skills/flow-activate/SKILL.md` step 6** ("Render INLINE and STOP") — no beat
   needed here; Seam 1 stop is a wait-for-owner state, not a stage transition the machine
   is actively working. (Beating on STOP would say "alive" for a claim that is actually
   idle awaiting the owner — the opposite of what the heartbeat should signal. Leave this
   step as-is.)

3. **`.claude/skills/flow-implement/SKILL.md` step 2** ("Ensure the worktree exists — and
   that you hold the claim") — for the oracle-driven branch that runs the claim protocol
   HERE (since it skips `flow-activate`), add the same first-beat call right after its own
   claim-commit push + re-read succeeds (near line ~49-52, mirroring #1 above):
   ```bash
   scripts/flow/claim-heartbeat.sh beat <N>
   ```
   For the design-driven branch (claim already established in `flow-activate`), add a
   **resume beat** right after "reuse it" (around line ~31-40) — implementation starting
   is itself a stage transition and should refresh the heartbeat even though the claim
   itself is old news:
   ```bash
   scripts/flow/claim-heartbeat.sh beat <N>
   ```

4. **`.claude/skills/flow-implement/SKILL.md` step 6** ("Gate — YOU run the FULL
   `scripts/agent-gate.sh` EXACTLY ONCE before merge") — add a beat right before invoking
   the full gate (near line ~78-86), since this is the single longest-running, most
   silence-prone stage transition (12-25 min, sometimes 20+ min queued per the `#1825`
   slot note) — exactly the stretch a heartbeat should span:
   ```bash
   scripts/flow/claim-heartbeat.sh beat <N>
   ```

5. **`.claude/skills/flow-implement/SKILL.md`** — wherever the PR is opened (the step
   after gate/C/roborev converge; not yet read in detail for this note — the phase-2 pass
   should locate the `gh pr create` call) — add a final beat right after the PR opens:
   ```bash
   scripts/flow/claim-heartbeat.sh beat <N>
   ```
   This matters for the reap rule's second condition ("no open PR"): once a PR is open,
   `flow-board`'s reap check already short-circuits on the open-PR condition regardless of
   heartbeat age, so this beat is a belt-and-suspenders freshness stamp, not
   correctness-critical — lower priority than #1/#3/#4 above.

6. **`.claude/skills/flow-finalize/SKILL.md`** (not audited in detail for this note) —
   add `scripts/flow/claim-heartbeat.sh clear <machine>` to the cleanup sequence, alongside
   the existing worktree/branch cleanup in `scripts/flow/finalize-cleanup.sh`, so a
   normally-completed issue doesn't leave a stale heartbeat ref sitting on origin until
   `flow-board`'s 4h reap window (harmless — `flow-board` only reads `In Progress` items
   against heartbeats — but a needless leak `clear` trivially avoids).

7. **`CLAUDE.md` → "Coordination & concurrency (Path A, #1886)"** — add one sentence after
   the existing claim-protocol paragraph: the claiming session also maintains a liveness
   heartbeat (`scripts/flow/claim-heartbeat.sh beat <N>`, refreshed at claim time and every
   stage transition) that `flow-board` uses for deterministic reaping (age > 4h AND no open
   PR) — replacing the old "no recent commits" guesswork this note's tooling half retired.

8. **Published site page** `agents-developing/delivery-pipeline` — mirror the same
   sentence as #7 (the site is the canonical doctrine source per CLAUDE.md's
   doctrine-mirroring convention) in whichever section documents the claim protocol /
   one-worker-per-machine rule (#1930).

## Threshold + ref layout (single source of truth, do not duplicate the number)

`scripts/flow/claim-heartbeat.sh`'s header comment is the canonical documentation for the
ref layout (`refs/heartbeats/<machine>`, one ref per machine, force-updated) and the 4-hour
reap threshold. `flow-board`'s SKILL.md step 4 already defers to it explicitly ("do not
hardcode a different value here"). Any phase-2 doctrine page that mentions the threshold
should link to the script header rather than restate `4h` as an independent constant, so a
future threshold tune is a one-file change.

## Not done here (out of scope for the tooling PR)

- No `.claude/skills/flow-activate/SKILL.md` edit.
- No `.claude/skills/flow-implement/SKILL.md` edit.
- No `.claude/skills/flow-finalize/SKILL.md` edit.
- No `CLAUDE.md` edit.
- No edit to the published `agents-developing/` site (external repo/build).
- This tooling PR's allowed file list was `scripts/flow/claim-heartbeat.sh`,
  `scripts/flow/fleet-view.sh` (optional; not added — `flow-board` calls `claim-heartbeat.sh
  list` directly and a separate join script didn't earn its keep for a single `In Progress`
  join), `scripts/tests/test_claim_heartbeat.sh`, and
  `.claude/skills/flow-board/SKILL.md` — all four (three, since `fleet-view.sh` was
  skipped) are complete; this file is the handoff for the rest of issue #2089's acceptance
  criteria ("Claim step in flow-activate/flow-implement pushes the heartbeat ref; stage
  transitions refresh it" and the doctrine-page/CLAUDE.md update).
