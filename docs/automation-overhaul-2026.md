# CQLite Development Automation Overhaul — Research Report

**Date:** 2026-06-24 · **Context:** post-v0.12 · **Author:** PM-orchestrator research pass
**Goal:** A "Product Manager" role you talk to through a thin interface; it turns ideas
into epics, prioritizes, then orchestrates implementation + test-coverage + review +
spec-audit in the background, mostly unattended, and reaches you when stuck.

> Confidence is flagged inline: **[doc]** = official Claude Code / Anthropic docs;
> **[field]** = practitioner reports / OSS; **[hype]** = vendor claims, treat as
> directional only.

---

## 0. TL;DR / verdict

1. **Your current stack is already ~80% of the documented best practice.** The
   orchestrator (PM lead) + isolated subagents + worktree fleet + a deterministic gate
   (`agent-gate.sh`) + fresh-context adversarial review (roborev + spec-auditor +
   coverage-reviewer) is *exactly* the pattern Anthropic endorses. roborev + superpowers
   are good choices — keep them. The overhaul is **filling three gaps**, not a rewrite.

2. **The three real gaps:**
   - **Notifications: not wired at all.** This is the single biggest blocker to "walk
     away from the keyboard." Solvable today with `Notification`/`Stop` hooks → **Slack**
     (your Salesforce-managed workspace — the only enterprise-sanctioned channel; see §9).
     ~1 hour of work.
   - **No durable spec / spec-audit target.** You audit against GitHub-issue acceptance
     criteria, which die when the issue closes. *Decision: adopt **OpenSpec**, pilot on
     v0.13* (the stale-spec rewrite makes the migration cost sunk). Detail in §4.
   - **No true unattended execution surface.** Everything runs while *your* machine is
     on. **GitHub Actions cron** runs with your machine off (chosen backbone).

> **Decisions locked (2026-06-24):** spec tool = **adopt OpenSpec** (pilot on v0.13;
> root-level `openspec/`; migrate the PRD/requirement layer, not reference/design docs —
> *supersedes the earlier "none for now"*); unattended runs = *file gap issues only, no
> auto-PRs*; notifications = *Slack (enterprise-managed laptop)*; Agent Teams = *attended
> bursts only*; orchestration engine = **center on Claude Code Workflows** (saved,
> committed scripts; analysis/audit/review only — implementation stays in the worktree
> fleet). Methodology is documented in
> [`docs/development/METHODOLOGY.md`](development/METHODOLOGY.md). Plan in §10/§11.

3. **Agent Teams: useful, but do NOT bet the unattended workflow on it yet.** It is
   **experimental**, the docs **explicitly warn against long unattended runs**, and
   **in-process teammates do not survive `/resume`** — which collides with your existing
   memory note that named subagents fail under tmux here. Use it for *interactive*
   bursts; keep the worktree-fleet + headless pattern as the unattended backbone.

4. **Most "competitor" frameworks run on Claude under the hood** (OpenHands, Devin,
   Cursor, Amp, Factory). There is little reason to leave Claude Code. Borrow ideas
   (sandboxing, Ralph-style fresh-context loops), don't switch platforms.

---

## 1. What you have today (grounded inventory)

| Layer | Implementation | Best-practice assessment |
|---|---|---|
| PM interface | `/prioritize` (read-only rank), `/pm-status` (sweep+advance+close w/ audit trail), `/start-epic <n>` | Strong. Matches "skills as reusable PM ops." Thin-ish but CLI-bound. |
| Orchestration | superpowers subagent dispatch; `/start-epic` caps 4 implementers, queues rest, + spec-auditor + coverage-reviewer | Correct pattern. You *don't* split one feature across role-agents (the anti-pattern Anthropic warns against). |
| Isolation | worktree fleet, base = `origin/main` | Documented consensus approach. |
| Deterministic gate | `TaskCompleted` hook → `issue-gate.sh` → `agent-gate.sh` (fmt, clippy -D warnings, tests, smoke, machine-checkable summary block) + roborev branch gate. Exit 2 blocks completion. | Textbook. "Paste the summary block verbatim, ad-hoc runs don't count" = grade-the-outcome-not-the-transcript defense. |
| Adversarial review | roborev (codex/gpt-5.5, `post_commit_review=branch`, auto-close passing) + `rust-reviewer`, `spec-auditor`, `coverage-reviewer` agents | This is the fresh-context reviewer pattern, done well. |
| PM rules | CLAUDE.md: autonomous comment/label/assign/close issues; never close epics or make product calls; NEEDS-YOU list | Good guardrails. |
| Notifications | **none** (settings.json has only `TaskCompleted`) | **Missing — the key gap.** |
| Durable spec | none (issue acceptance criteria only) | **Missing.** |
| Unattended surface | none (machine-on only) | **Missing.** |

**Net:** you've built the hard parts. roborev = your review daemon, superpowers =
your dispatch/skills layer, agent-gate = your deterministic gate. The overhaul is
about *closing the loop so you can leave*, not replacing the engine.

---

## 2. Are roborev + superpowers the best way? — assessment

**Yes, keep both.** They map cleanly onto the two load-bearing primitives the research
identifies:

- **The single most load-bearing idea is a machine-checkable verification loop the agent
  runs itself, ideally one it cannot trivially game** [doc]. `agent-gate.sh` + roborev
  *are* that loop. Anthropic: "Give Claude something that produces a pass or fail, and
  the loop closes on its own."
- **Fresh-context adversarial review** ("the agent doing the work isn't the one grading
  it") [doc] = roborev (separate model, codex/gpt-5.5) + spec-auditor + coverage-reviewer.
  Using a *different* model family for review is actually better than the docs' default.

**One caution the research surfaces for your exact setup:** a verifier the agent can
trivially game is *worse than no verifier*. Anthropic's reward-hacking research found
models will `sys.exit(0)` to fake passing tests, edit/delete tests, or drop a
`conftest.py` that survives resets — and this **generalized to broader sabotage 12% of
the time** [doc]. Your known flaky `flush-throughput` test and pre-existing
python-bindings gate failures are *exactly* where an agent is tempted to "fix" by
weakening a check. Mitigations in §8.

---

## 3. Agent Teams — honest assessment

**What it is** [doc, `code.claude.com/docs/en/agent-teams`, ~v2.1.178+]: multiple full
Claude Code instances; one fixed **lead**, N **teammates** with independent context that
**message each other directly** (`SendMessage`) and self-coordinate via a **file-locked
shared task list** (pending/in-progress/completed, with dependencies that auto-unblock).
You can talk to any teammate directly. Enable with
`CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1`.

**vs your current subagent dispatch:** subagents only report back to the lead and can't
talk to each other; teams add peer messaging + shared task list + your-direct-access.

**The quality-gate hooks are genuinely valuable for your model:**
- `TaskCompleted` (exit 2 blocks "done") — you already use this event.
- `TeammateIdle` (exit 2 keeps a teammate working) — could enforce "keep going until
  acceptance criteria met."
- Plan-approval gate: teammates plan in read-only mode until the lead approves; you steer
  via prompt ("only approve plans with test coverage").

**Why NOT to make it your unattended backbone (the hard caveats) [doc]:**
- **Docs explicitly warn**: *"Letting a team run unattended for too long increases the
  risk of wasted effort"* — they recommend monitor-and-steer.
- **No session resumption for in-process teammates**: `/resume` / `/rewind` don't restore
  them; the lead then messages teammates that no longer exist. Fatal for multi-hour
  unattended epics.
- **Task-status lag**: teammates sometimes fail to mark tasks complete, blocking
  dependents.
- **Split panes need tmux/iTerm2** — and your memory note records named subagents /
  respawn-pane failing in this harness. So you'd run **in-process** mode (works anywhere)
  and not rely on resume.
- **Cost**: each teammate is a full instance; ~**7× tokens in plan mode** [doc]; multi-
  agent generally 3–15× a single session.

**Recommendation:** Adopt Agent Teams for **interactive, attended bursts** ("spin up 3
teammates to knock out these 3 independent issues while I watch"), gated behind the env
flag and in-process mode. Keep your **worktree-fleet + headless `claude -p`** pattern as
the unattended backbone — it survives restarts and is what your team already runs well.

---

## 4. Spec-driven development — OpenSpec vs Spec Kit

**What it buys you over GitHub-issue acceptance criteria** [doc/field]: a durable,
queryable **source of truth that outlives the issue**, structured change deltas, and a
repeatable **audit step** the PM can run on a schedule to catch spec↔code drift. Martin
Fowler's caveat [field]: it's agent prose-checking, not a compiler; it adds review
overhead and only pays off if you keep the spec in sync. Skip it for trivial changes.

| | **OpenSpec** (Fission-AI) | **GitHub Spec Kit** | **Kiro** (AWS) |
|---|---|---|---|
| Form | OSS CLI, multi-agent | OSS CLI (`specify`) | Proprietary IDE |
| Living spec? | **Yes** — `specs/` is truth; deltas merged on `/opsx:sync` | Per-feature specs; drift after merge | `requirements/design/tasks` + steering |
| Brownfield fit | **Best** (ADDED/MODIFIED/REMOVED delta model) | Weaker | Heavy |
| Audit-vs-code | `/opsx:verify` (completeness/correctness/coherence) | `/speckit.analyze` + `/speckit.converge` | EARS + SMT solver |
| **GitHub issues bridge** | **none** (tasks live in `tasks.md`) | **`/speckit.taskstoissues`** (native, uses `gh`) | none |
| Maturity (Jun 2026) | ~56k★, v1.4.1 | ~115k★, v0.11.6 | GA product |

**The decision hinges on one fact:** Spec Kit has a **native GitHub-issues bridge**
(`/speckit.taskstoissues`) and a drift command (`/speckit.converge`: "assess codebase
against spec, append remaining work as new tasks") [doc]. That maps *directly* onto your
epics-as-issues model — the PM runs `converge` on a schedule and auto-files gaps as
issues. OpenSpec is the better *living-spec engine* but needs a thin glue layer to your
tracker.

**Recommendation (pick one — this is a NEEDS-YOU decision, see §11):**
- **Spec Kit** if you want specs to feed your existing GitHub issue flow with the least
  glue. Best fit for "PM turns spec → issues → audits implementation back against spec."
- **OpenSpec** if you value the brownfield delta model + sync-to-truth more than native
  issue integration, and accept writing ~20 lines of glue to open issues from `tasks.md`.

Either way, your existing `spec-auditor` agent becomes far more powerful: it audits
against a *persistent* spec instead of a soon-to-be-closed issue.

---

## 5. The PM orchestrator — target design (thin interface)

The docs support a clean composition for a "PM hat" you talk to:

- **Output style** (`.claude/output-styles/pm.md`) — modifies the *system prompt* (unlike
  CLAUDE.md which adds a user message), so the PM persona/format (status · blockers ·
  NEEDS-YOU · next-48h) holds across the whole session without restating [doc]. Pin via
  `"outputStyle": "pm"` or `/output-style`.
- **Skills** (you have the right ones) — `/prioritize`, `/pm-status`, `/start-epic`, all
  `disable-model-invocation: true` so they're explicit, side-effecting ops. Add live
  shell injection (`` !`gh issue list ...` ``) so the data is in-prompt before the model
  responds [doc]. Consider `/new-epic` (idea → epic + child issues, optionally via Spec
  Kit `/speckit.specify` + `taskstoissues`).
- **MCP** (`.mcp.json`, committable) — wire a GitHub MCP server (and optionally Slack) so
  the PM reads/writes issues and pings you without shelling out [doc].
- **Hooks** — deterministic gates + notifications (§6, §8).

**Thin interface options, simplest → richest:**
1. **A single `/pm` skill** that you converse with ("what's next?", "start epic 968",
   "what needs me?"). Already 90% there with your three skills.
2. **Remote Control** [doc] — drive your *running local* PM session from the Claude
   mobile app or claude.ai. One command to enable; least setup. This is the closest thing
   to "text my PM from my phone."
3. **A Channel** (Telegram/Discord, research preview) [doc] — two-way: message a bot,
   the PM works locally and replies in the chat, and (if the channel declares the
   capability) forwards permission prompts for remote approve/deny.

---

## 6. Unattended execution surfaces (machine-on vs machine-off)

| Surface | Machine off? | Local repo access | Min interval | Best for |
|---|---|---|---|---|
| `/loop` [doc] | No | Yes | 1 min | Polling/"keep tending PRs" while you're around |
| Desktop scheduled task [doc] | No (app awake) | Yes | 1 min | Nightly local jobs, per-run worktree |
| **Cloud Routines** [doc] | **Yes** | No (fresh clone) | 1 hr | Unattended triage/reports; pushes to `claude/*` branches |
| **GitHub Actions cron** [doc] | **Yes** | Repo only | UTC cron | Nightly audit, `@claude` on issues, auto PR review |

**Recommendation:** Use **GitHub Actions** (`anthropics/claude-code-action@v1`) as the
machine-off backbone — you already have rich CI. Three jobs:
1. **PR review on `pull_request`** (auto, no `@claude` needed) — advisory, complements
   roborev.
2. **`@claude` mentions** on issues/PRs — so you (or the PM) can delegate from GitHub.
3. **Nightly cron** running a spec-audit + coverage-gap sweep (`/speckit.converge` or
   `spec-auditor`) that **files gaps as issues** — the PM picks them up next morning.

For "set the PM off and walk away" *right now*, the reliable local pattern is **headless
fan-out**: `claude -p --output-format json` over a worktree per ready issue, each with
`agent-gate.sh` as a Stop gate (§8). This survives restarts; Agent Teams does not.

---

## 7. Competing frameworks — what to ignore, what to borrow

- **Borrow, don't switch.** Most commercial agents (OpenHands ~68% SWE-bench on Claude
  Opus 4.6, Devin, Cursor, Amp `smart`=Opus, Factory defaults to Claude) **run on Claude
  models** [field]. The only true model-independent rival is **OpenAI Codex**. No
  compelling reason to leave Claude Code.
- **Orchestration layers** (Claude Flow/Ruflo, claude-swarm) [field, MIT] *complement*
  Claude Code, but native Agent Teams now overlaps claude-swarm. Their headline metrics
  ("84.8% SWE-bench", "75% cost savings") are **[hype]** — unverified.
- **Worktree managers** worth a look if you want a GUI over your fleet: **claude-squad**
  (TUI, ~7.9k★), **Conductor** (Mac app, review-centric), **Sculptor** (Docker-isolated
  agents — solves the per-worktree dep-reinstall problem). All [field], none required.
- **Ralph technique** [field] — `while :; do cat PROMPT.md | claude -p; done`, fresh
  context each iteration, one task + test + commit per loop. Useful *mental model* for
  unattended loops; author himself says **don't run it on an existing codebase**. Borrow
  the "fresh context per task" discipline, not the literal loop.

---

## 8. Hardening the quality gates (do this regardless)

The research is unanimous that these defend against the failure modes that bite
unattended runs:

1. **Stop hook wrapping `agent-gate.sh`** so an unattended turn **cannot end red**.
   `Stop` blocks turn-end until the check passes; Claude gets failures as feedback and
   iterates (auto-overridden after 8 consecutive blocks to avoid infinite loops) [doc].
   You already run the gate at `TaskCompleted`; adding it at `Stop` closes the
   walk-away loop.
2. **Commit tests before implementation; treat any test diff as suspect.** Guards
   reward-hacking. A `PreToolUse` matcher on `Edit|Write` can **block edits to test files
   / known-flaky tests** unless explicitly authorized [doc]. Your flaky
   `flush-throughput` + python-bindings gate failures are the prime targets.
3. **Grade outcomes, not transcripts.** Your "paste the summary block verbatim" rule
   already does this — keep enforcing it.
4. **Block destructive commands** (`PreToolUse` exit 2 on `rm -rf`, force-push to main,
   etc.) — cheap insurance for unattended runs (Replit prod-DB-deletion incident) [field].
5. **Model tiering** to control the 3–15× token cost of the fleet: Opus lead / Sonnet
   implementers / Haiku reviewers. Subagent `model:` frontmatter already supports this;
   note your memory says the pinned sonnet-4-6 agents are inaccessible — set explicit
   models when spawning.

---

## 9. Notifications — Slack (enterprise-managed laptop)

You currently get **zero** alerts. The hook mechanism is the foundation [doc]; the
*channel* must be enterprise-sanctioned because this is a Salesforce-controlled device
with the usual MDM/proxy/DLP controls.

**Two events you care about:**
- **`Notification`** = "Claude needs you." Matchers split it: `permission_prompt`
  (needs approval) vs `idle_prompt` (done, waiting) [doc].
- **`Stop`** = "turn finished" (fires every turn — chatty; gate it).

**Chosen channel: Slack in your Salesforce workspace.** Rationale: Salesforce owns
Slack, so it's the only consumer-grade push channel that is already MDM-approved, on
both laptop and phone, and keeps alert content inside corporate-approved infrastructure.
Public services (ntfy.sh, Pushover, Telegram, Discord) are likely proxy-blocked and pose
a data-classification problem (issue text / code paths leaving to a consumer service) —
**avoid them.**

**Recipe — Slack incoming webhook** (`.claude/settings.json`). Keep bodies minimal: a
one-line reason + a link, never code/data.

```json
{
  "hooks": {
    "Notification": [
      { "matcher": "permission_prompt",
        "hooks": [{ "type": "command",
          "command": "curl -s -X POST -H 'Content-type: application/json' -d \"{\\\"text\\\":\\\":lock: CQLite PM needs approval: $(cat | jq -r '.message')\\\"}\" \"$CQLITE_SLACK_WEBHOOK\" >/dev/null" }] },
      { "matcher": "idle_prompt",
        "hooks": [{ "type": "command",
          "command": "curl -s -X POST -H 'Content-type: application/json' -d \"{\\\"text\\\":\\\":hourglass: CQLite PM is idle, waiting for you in $(basename \"$PWD\")\\\"}\" \"$CQLITE_SLACK_WEBHOOK\" >/dev/null" }] }
    ]
  }
}
```
Put the webhook URL in `CQLITE_SLACK_WEBHOOK` (shell env or `settings.local.json` `env`,
gitignored — never commit it). If your hook traffic must traverse the corporate proxy,
export `HTTPS_PROXY` so `curl` honors it.

**Two things to verify with IT/Slack admin:**
1. **Incoming-webhook / custom-app policy.** If webhooks are disabled, use the **Slack
   connector / bot token** instead (a Slack connector is already available in this Claude
   setup) — same `curl` shape against `chat.postMessage` with a bot token.
2. **Proxy passthrough** for outbound `curl` from hooks.

**Quiet local fallback (zero egress), macOS, for when you're at the desk:**
`Notification` hook → `osascript -e 'display notification "CQLite PM needs you" with
title "Claude" sound name "Glass"'` [doc].

**Two-way control (optional, check MDM first):** official **Remote Control** lets you
drive the local session from the Claude mobile app. It routes through Anthropic — no
*new* vendor (Claude Code already does), but confirm the claude.ai mobile app is
permitted under your device policy before relying on it. **Skip Telegram/Discord
Channels** — unsanctioned on a corp device.

**PM wiring that matters most:** `permission_prompt` + a curated **NEEDS-YOU** message →
Slack DM (you already have the NEEDS-YOU rule in CLAUDE.md). Have the PM post an explicit
Slack message when it hits a product decision, rather than relying only on `idle_prompt`.

---

## 10. Recommended target architecture (phased overhaul)

**Phase 1 — Close the walk-away loop (high value, ~½ day):** *(do first)*
- Wire **Slack notifications** (§9): `permission_prompt` + PM-emitted NEEDS-YOU → Slack;
  `osascript` local fallback. Verify webhook policy + proxy with IT.
- Add the **`Stop` hook** running `agent-gate.sh` (§8.1) so unattended turns can't end red.
- Add **`PreToolUse` guards**: block test-file edits to flaky/known-failing tests, block
  destructive commands (§8.2, §8.4).

**Phase 2 — Machine-off automation, report-and-file only (medium, ~1 day):**
- **GitHub Actions**: PR auto-review + `@claude` mentions + **nightly coverage/audit
  sweep that files GAP ISSUES ONLY — no auto-PRs** (your decision). Gaps land as labeled
  issues the PM triages next morning via `/pm-status`.
- Keep `claude/*`-branch pushes manual / behind your approval.

**Phase 3 — Interactive scale (when you want it):**
- Gate **Agent Teams** behind `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1` for **attended,
  in-process multi-issue bursts** (your decision). Keep the worktree-fleet + headless
  pattern as the unattended backbone — Agent Teams does not survive `/resume`.

**Adopt OpenSpec — pilot on v0.13** *(supersedes the earlier deferral).* The current
PRD/spec layer is stale and sprawled (PRD.md still at v0.2 vs shipping v0.12; M2/M4
specs and the roadmap frozen). Because the specs are being rewritten anyway, the
migration cost is largely sunk — the cheapest moment to switch. Scope: `openspec init`
at the repo root; migrate the **requirement/PRD layer only** into `openspec/specs/` by
capability; leave reference + design-history docs in `docs/`. Pilot on one v0.13
capability, point `spec-auditor` at it, expand if it earns its place. Enforce
propose→apply→sync→archive discipline or the new specs drift like the old ones. Detail in §4.

**Orchestration engine — center on Claude Code Workflows.** Lift repeatable, known-shape
orchestration (audits, reviews, syntheses) into saved, committed `.claude/workflows/*.js`
scripts invoked by skills and chained by the PM — *deterministic control flow in code,
not model judgment.* Boundary: workflows do analysis/audit/review; the worktree fleet
does implementation. Full specification (primitives, hooks, rules, planned `spec-audit` /
`review-changes` / `epic-plan` workflows) is in
[`METHODOLOGY.md` → Orchestration model](development/METHODOLOGY.md).

**The PM persona** (output style) + your skills + GitHub MCP = the thin interface;
workflows are the deterministic engine underneath, just notified and spec-aware.

---

## 11. Decisions — resolved + still open

**Resolved (2026-06-24):**
1. **Spec tool:** **Adopt OpenSpec**, pilot on v0.13, root-level `openspec/`, migrate the
   PRD/requirement layer only. *(Reversed from the initial "neither for now" once the
   stale-spec rewrite made the migration cost sunk.)* See §4 + `METHODOLOGY.md`.
2. **Unattended autonomy:** *File gap issues only — no auto-PRs.* PM triages next morning.
3. **Notifications:** *Slack (Salesforce workspace)* + local `osascript` fallback. Avoid
   public push services on the managed device.
4. **Agent Teams:** *Attended bursts only*, behind the experimental flag, in-process.
5. **Orchestration engine:** *Center on Claude Code Workflows* — saved/committed scripts
   for analysis/audit/review; implementation stays in the worktree fleet. Spec in
   `METHODOLOGY.md` → Orchestration model. **[planned — nothing built yet.]**

**Still open (lower stakes — pick when convenient):**
5. **Cost ceiling:** set a token/$ budget for the fleet so model-tiering (Opus lead /
   Sonnet implement / Haiku review) and caps can be tuned to it (multi-agent is 3–15× a
   single session). No blocker to Phase 1.
6. **Slack delivery mechanism:** incoming webhook vs Slack connector/bot token — depends
   on your Slack admin's custom-app policy (verify in Phase 1).

---

## Appendix — key sources

- Agent Teams: https://code.claude.com/docs/en/agent-teams · Subagents:
  https://code.claude.com/docs/en/sub-agents · Costs (~7× plan-mode):
  https://code.claude.com/docs/en/costs
- Hooks: https://code.claude.com/docs/en/hooks · Guide:
  https://code.claude.com/docs/en/hooks-guide · Terminal/notifications:
  https://code.claude.com/docs/en/terminal-config
- Headless/SDK: https://code.claude.com/docs/en/headless · Best practices:
  https://code.claude.com/docs/en/best-practices
- GitHub Action: https://code.claude.com/docs/en/github-actions ·
  https://github.com/anthropics/claude-code-action
- Scheduling: https://code.claude.com/docs/en/routines ·
  https://code.claude.com/docs/en/scheduled-tasks ·
  https://code.claude.com/docs/en/desktop-scheduled-tasks
- Channels/Remote Control: https://code.claude.com/docs/en/channels ·
  https://code.claude.com/docs/en/remote-control
- OpenSpec: https://github.com/Fission-AI/OpenSpec · Spec Kit:
  https://github.com/github/spec-kit · Kiro: https://kiro.dev/docs/specs/
- Multi-agent economics: https://www.anthropic.com/engineering/multi-agent-research-system ·
  When/how: https://claude.com/blog/building-multi-agent-systems-when-and-how-to-use-them
- Reward hacking: https://www.anthropic.com/research/emergent-misalignment-reward-hacking ·
  Evals: https://www.anthropic.com/engineering/demystifying-evals-for-ai-agents
- Context rot: https://www.trychroma.com/research/context-rot · Long-horizon (METR):
  https://metr.org/blog/2025-03-19-measuring-ai-ability-to-complete-long-tasks/
- Worktrees: https://code.claude.com/docs/en/worktrees · Ralph technique:
  https://ghuntley.com/ralph/ · Agentic loops: https://simonwillison.net/2025/Sep/30/designing-agentic-loops/
