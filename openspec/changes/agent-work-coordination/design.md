## Context

The flow-lead pipeline tracks work as GitHub issues + `status:*` labels and selects
"highest-priority `status:ready`". That has no atomic claim, so N independent
sessions collide on the same issue. Separately, the mobile app cannot run the local
pipeline (no local bash/skills/worktrees/CLIs). Research (code.claude.com/docs +
`gh project`) confirms: Agent Teams has a file-locked shared task list but is
desktop/tmux-only and experimental; `gh project` (Projects v2) gives a scriptable
board with a single-select `Status` field + automations; mobile drives either a
Remote Control (local) or a Claude Code on the web (cloud) session.

## Goals / Non-Goals

**Goals:** a visible shared claim board; a claim protocol that prevents two sessions
working one item; a stated concurrency model; a documented mobile/remote path; graceful
fallback to labels.

**Non-Goals:** a custom lock server; requiring Agent Teams; running the local pipeline
on the phone itself; changing gate/C/roborev.

## Decisions

### D1 — GitHub Project (v2) is the claim board, labels are the fallback
A repo-linked Project with a `Status` single-select (`Backlog/Ready/In Progress/In
Review/Done`) is the cross-session board (`gh project item-list` renders it;
`item-edit` moves items). The existing `status:*` labels remain as the degraded
fallback when the `project` scope/board is absent. Rationale: a Project is visible,
automatable, and the thing a human can also drive from mobile.

### D2 — Claim lock = the pushed origin branch; assignee/Status = visibility; re-read = guard
A Project `Status` update is read-modify-write (not atomic), and **assignee is
insufficient as a lock when both sessions authenticate as the SAME GitHub user on
different machines** (assignee `@me` is identical on both). The cross-machine lock is
therefore the **`issue-<N>-<slug>` branch pushed to origin**: a session claims by
pushing that branch (the natural 1:1:1:1 artifact, server-side, per-machine-distinct),
then sets assignee + `Status=In Progress` for board visibility, then **re-reads** and
proceeds only if it holds the claim. Eligibility = `Ready` AND no existing
`issue-<N>-*` branch on origin (`git ls-remote --heads origin`). Rationale: uses
GitHub's own server-side state, no external service; the remote branch distinguishes
machines that share a user; the re-read closes the residual TOCTOU window. Side benefit:
another machine can `fetch` an existing claim's branch to RESUME it.

### D3 — Board freshness = server-side automations + flow-* transitions + flow-board reaper
Three layers keep the board current so freshness never depends on an agent running:
(1) **Project built-in workflows** (server-side) move items on GitHub-side events — PR
merged / issue closed → `Done`, assigned → `In Progress` — so even a merge from the
phone or web UI updates the board with no `flow-*` run; (2) **`flow-*` transitions** set
Status at each stage when an agent drives; (3) **`flow-board` reconciles + reaps** —
flags drift (merged PR still `In Progress`) and **abandoned claims** (an `In Progress`
item whose branch has no recent commits — the claiming session died) for reclaim/finish.
Rationale: layer 1 covers human/mobile actions, layer 3 covers crashed sessions; the
"stuck In Progress" leak is the failure mode this prevents.

### D4 — Concurrency model: lead+subagents default; claim protocol for sessions; Agent Teams optional
Default and recommended: **one `flow-lead` → subagents** (the lead assigns disjoint
work — zero dup by construction). For **multiple independent sessions**, the D2 claim
protocol is mandatory. **Agent Teams** (`CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1`) is the
built-in file-locked shared-task-list option for parallel coordinated sessions, but it
is experimental + desktop/tmux-only (no `/resume`, one team/session) — optional, not
required. Explicit rule: **never run N bare `flow-lead`s without the claim protocol.**

### D5 — Mobile = Remote Control primary, web/cloud secondary, seams via GitHub
Phone-driving paths: (1) **Remote Control** — `claude remote-control` on the laptop;
the phone drives the full local pipeline (laptop must stay online). (2) **Claude Code
on the web** — a cloud session that uses the repo-committed `.claude/` (skills/agents/
hooks) but NOT user-scoped config or local data; a **cloud setup script** fetches
datasets + installs `openspec`/`gh` so `flow-implement` can run the gate there. The two
human seams (approve spec, merge PR) are GitHub-mobile-native regardless. Rationale:
mobile can't execute locally; these are the two supported ways to still drive work.

### D6 — `project` scope is a one-time owner prerequisite; degrade gracefully
Projects v2 needs the `project` token scope (`gh auth refresh -s project`) — a human
action (auth is the owner's). `flow-*` detects the missing scope/board and falls back to
labels, never blocking. Rationale: don't hard-couple the pipeline to a scope the owner
must grant; make it additive.

## Risks / Trade-offs

- **Project Status race** — mitigated by the assignee-lock + re-read (D2); residual risk
  is a brief double-claim resolved on re-read.
- **Token-scope friction** — mitigated by graceful label fallback (D6).
- **Cloud dataset cost/time** — the web setup script fetches the dataset tarball each
  cold session; acceptable for occasional mobile-driven runs, not for tight loops.
- **Agent Teams temptation** — its file-lock is stronger, but desktop-only + experimental;
  keeping it optional avoids coupling the pipeline to an unstable, non-mobile feature.
