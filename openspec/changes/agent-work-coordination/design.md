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

### D2 — Claim = assignee (lock) + Status, then RE-READ (race guard)
A Project `Status` update is read-modify-write, so it is NOT atomic — two sessions can
both read `Ready` and both set `In Progress`. The lock is therefore the GitHub
**assignee** (clear single-owner): claim with `--add-assignee @me` + `Status=In
Progress`, then **re-read** the item; if the assignee is not us, we lost the race →
back off and take the next `Ready` item. Only ever claim items that are currently
**unassigned**. Rationale: cheap, uses GitHub's own data, no external service, and the
re-read closes the TOCTOU window without true locking.

### D3 — Project workflow automations reduce manual bookkeeping
Use the Project's built-in workflows (auto-set `In Progress` when an item is assigned;
`Done` when its PR merges) so `flow-*` doesn't hand-juggle every transition and the
board self-heals from drift. Rationale: less label-flipping code, fewer drift bugs (the
kind roborev flagged in the flow-* skills).

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
