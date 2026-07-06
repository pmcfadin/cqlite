# Synthesis — Context Economy + Multi-Machine

Theme: the owner's pain is long-running worker agents whose context bloats until they degrade.
This examines what floods a persistent agent's context, where work should be sliced into fresh
short-lived agents, what state must live OUTSIDE any context for clean handoffs, and how the
claim / one-worker-per-machine model should evolve for a multi-machine fleet.

## Findings

**F1. Exactly one agent is long-lived; it is also the one that accretes.** All 7 specialists
(`sstable-developer`, `spec-auditor`, `rust-reviewer`, `test-validator`, `coverage-reviewer`,
`compaction-parity-auditor`) are fresh spawns whose context is discarded on return
(`agents-and-skills.md`). The `flow-lead` (`.claude/agents/flow-lead.md`, 178 lines) is the sole
persistent session and drives groom→activate→implement→address→finalize for issue after issue.
Nothing compacts between issues, so a session that clears N issues carries O(N) of board renders,
gate summaries, roborev findings, PR bodies, and manager comments. Telemetry shows 174 issues in
9 days (`telemetry-analysis.md`) — real sessions clear many issues back-to-back.

**F2. The doctrine names the anti-pattern, then two steps violate it.** `flow-implement/SKILL.md`
step 4 is explicit: *"you do not read source, write code, or run the gate in your own context
(that's what fills it up)."* Yet **step 6 runs the FULL gate in lead context** and **step 8 runs
`/roborev-review-branch` in lead context**. These are the two largest accretion sources: the gate
script is 2,707 lines (`scripts/agent-gate.sh`) and streams a large stdout; roborev reads the full
branch diff and returns 3–8 findings/round (max 40 on #1161, `telemetry-analysis.md`), across a p90
of 3 and up to 9 rounds. Both land in the persistent session.

**F3. The full-gate stdout leak is not defaulted shut.** `flow-implement` step 6 tells the lead to
run the full gate but only mentions `run_in_background` for the *queue* case; it does NOT mandate the
`AGENT_GATE_SUMMARY_FILE=… > gate.log 2>&1` + `cat summary-file` pattern that CLAUDE.md documents as
*recovery*. So the lead's default `Bash` invocation captures the entire streamed gate log (thousands
of lines from a 2,707-line script) instead of the ~50–100 line SUMMARY block. The robust-capture
machinery exists (issue #1175, `gate-and-scripts.md`) but is positioned as fallback, not default.

**F4. The #1855 tension is the structural cause of F2.** A subagent idle-waiting on a 12–25 min gate
is killed by the 600 s stall watchdog, and the dying agent takes its child gate process down (3
implementers lost this way, `flow-implement` step 6). That is *why* the lead runs the gate itself —
but it is exactly what bloats the long-lived agent. The escape (`run_in_background`, harness
re-invokes on exit; or cheap `grep` poll of the summary file at <5 min) is already known but not
made the required path for a dedicated runner.

**F5. `CLAUDE.md` is a 783-line / 6,764-word fixed tax on every spawn.** It is dominated by
gate-ops prose — sccache setup/tuning, the #1825 concurrency-cap internals, disk hygiene / Time
Machine snapshots, `--delta` re-cert mechanics, accelerator degradation. Every subagent
(`sstable-developer`, `rust-reviewer`, …) loads all of it, though most never run the full gate or
tune sccache. This is baseline context burned before the first useful token.

**F6. Seam-1 inline render pushes verbatim spec into lead context.** `flow-lead.md` mandates
"show, don't link" — `flow-activate` renders requirements + `#### Scenario:` blocks verbatim inline
for owner approval. Correct for the owner-facing message, but the lead then *retains* the full spec
body for the rest of the session; the fresh `spec-auditor` re-reads it from `openspec/changes/<slug>/`
anyway, so the lead's copy is pure bloat.

**F7. Implementer return contract is under-specified on size.** Doctrine says "short summary + LITE
block" (`flow-implement` step 4, `sstable-developer.md`), but the LITE *output* is 200–400 lines
(`gate-and-scripts.md`). Nothing caps the paste, so 1–5 fix rounds can dump ~1–2k lines into the lead.

**F8. State already persisted outside context is good and broad.** Worktree files; the origin claim
branch with a hostname-stamped claim commit (`claim issue-<N> $(hostname -s)-${RANDOM}-$$`,
`flow-implement` step 2); issue body (acceptance criteria) + PR body (`Closes #N`) + `🧭 MANAGER`
comments; OpenSpec change files; `.agent-gate-summary.txt`; `delivery-telemetry.jsonl`; board Status
field; and a machine-local `.claude/worktrees/.worker-state/rate.env`. Handoff durability is already
strong — the gap is that the *lead* re-reads little of it and instead carries live copies.

**F9. Multi-machine coordination is lock-only, with no heartbeat.** The claim is purely the origin
`issue-<N>-*` branch (`doctrine-and-process.md`); it is race-safe (0 claim collisions across 174
issues, `telemetry-analysis.md`). But abandoned `In Progress` detection is heuristic ("no recent
commits", manual `flow-board` reaper). `.worker-state` is machine-local, so there is no shared fleet
view of which machine holds what or whether it is alive. One-worker-per-machine (#1930) is
human-enforced with no file lock.

## Recommendations (ranked)

**R1 — Move full-gate + C + roborev out of the lead into a per-issue "closer" subagent.**
Define a short-lived `flow-closer` (opus) that owns steps 6–8: runs the full gate with
`run_in_background` (its turn ends; harness re-invokes on gate exit — no idle-wait, so #1855's
watchdog kill does not apply), spawns `spec-auditor`, runs roborev to clean, and returns ONLY a
terminal packet: `{verdict, PR URL, summary-file path, residual findings ≤10 lines}`. All gate
stdout, roborev diffs, and finding churn die with the closer's context.
*Payoff:* removes the two largest accretion sources (F2) from the one long-lived agent; simultaneously
resolves the #1855 stall-watchdog problem via `run_in_background`. *Cost:* one agent definition + one
hop per issue; discipline that the closer never idle-waits (must background or grep-poll).

**R2 — Make redirect-to-summary-file the DEFAULT gate invocation, not recovery.** Everywhere the gate
runs, require `AGENT_GATE_SUMMARY_FILE=<path> bash scripts/agent-gate.sh > gate.log 2>&1` (background
or long timeout) followed by `cat <summary-file>`; forbid reading raw gate stdout into any persistent
context. Edit `flow-implement` step 6 and `sstable-developer.md`.
*Payoff:* caps per-gate context at ~50–100 lines vs a full 2,707-line-script log. *Cost:* two doc
edits; near-zero risk.

**R3 — Slim `CLAUDE.md`: extract gate-ops prose to a lead-only doc.** Move sccache setup, #1825 cap
internals, disk hygiene, `--delta` mechanics, and accelerator-degradation detail (~250 lines) into
`docs/development/gate-ops.md`, referenced by the lead/closer only, leaving a one-line pointer in
CLAUDE.md. Implementers and reviewers never tune the gate.
*Payoff:* every subagent spawn's baseline shrinks materially (F5). *Cost:* one refactor; mitigate lost
discoverability with the pointer + keeping the deep doc in the closer's brief.

**R4 — Add an explicit inter-issue context reset to `flow-lead`.** After each `flow-finalize`, the
lead writes a one-line disk ledger entry (issue, PR, verdict) and treats the board as the *sole*
re-hydration source for the next item — carrying zero prior-issue history. The lead must be
re-runnable from board state alone (which F8 already guarantees on disk).
*Payoff:* bounds a multi-issue session to O(1 issue) of context instead of O(N) (F1). *Cost:* doctrine
change; risk that cross-issue lessons are dropped — persist those to `MEMORY.md` /
`process_improvements.md`, not context.

**R5 — Cap the implementer return contract.** `sstable-developer.md`: return the LITE SUMMARY block
(~15 lines) + ≤5-line prose per round; never paste the 200–400-line raw lite output.
*Payoff:* strips ~1–2k lines/issue of fix-round noise from the lead (F7). *Cost:* one-line edit.

**R6 — Give the claim a heartbeat and a shared fleet ledger.** Elevate `.worker-state` from
machine-local to an origin-tracked lightweight ref (or a `claim-heartbeat` note) carrying
`(issue, machine, ts)`; `flow-board` reaps claims whose heartbeat is older than T deterministically
instead of by "no recent commits" guesswork.
*Payoff:* safe multi-machine scale without manual reaping; real fleet observability (F9). *Cost:*
modest tooling; keep it a ref update, not a GitHub API call, to avoid the GraphQL/REST bucket pressure
already flagged in `agents-and-skills.md`.

## Risks / Tradeoffs

- **R1 re-introduces #1855 if botched.** The closer is itself a subagent; if it idle-waits on the gate
  it gets watchdog-killed and orphans the gate process — the exact failure #1855 warns of.
  `run_in_background` (or grep-poll) is mandatory, not optional, in the closer's brief.
- **R3 trades discoverability for lean context.** A rule moved out of CLAUDE.md can be missed. Mitigate
  with an explicit pointer line and by loading the deep doc only where it is used (closer/lead).
- **R4 can drop useful cross-issue state.** Aggressive reset loses in-session learning (e.g., a repeated
  rebase conflict pattern). Route durable lessons to persisted files, not the live window.
- **R6 adds origin traffic.** Heartbeats must be cheap ref updates; an API-call heartbeat would worsen
  the existing bucket-throttling friction.
- **General:** every recommendation shifts load from a scarce resource (persistent context) to
  cheaper ones (fresh spawns, disk, git refs). The residual cost is more agent hops and more small
  files — acceptable given accretion is the stated failure mode.
