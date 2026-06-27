## Why

Two operational gaps surfaced once the flow-lead pipeline went into real use:

1. **Duplicate work under concurrency.** The backlog is GitHub issues + labels and
   "what's next = highest-priority `status:ready`" with NO atomic claim. Running
   multiple independent `flow-lead` sessions makes each pick the same top issue.
2. **Mobile can't drive the pipeline.** The Claude Code mobile app cannot run local
   bash / skills / worktrees / CLIs, so it can't execute `flow-*` (which need `gh`,
   `openspec`, `agent-gate.sh`, and the dataset binaries) directly.

This change adds a shared **GitHub Project (v2)** claim board with a real claim
protocol, documents the concurrency model, and defines the mobile/remote path.

- **Milestone:** maintenance / process. **Design-driven** (no Cassandra oracle).
- Extends the `delivery-pipeline` capability.

## What Changes

- **Adopt a GitHub Project (v2) as the shared claim board.** A single-select
  `Status` field (`Backlog → Ready → In Progress → In Review → Done`) tracks every
  in-flight issue across sessions. `flow-board` renders the project (`gh project
  item-list`); built-in project workflows auto-move items on assign / PR-merge.
- **Claim protocol (the dup-work fix).** Before working an item a session: (a)
  considers only **unassigned** `Ready` items; (b) atomically claims by
  `--add-assignee @me` + `Status=In Progress`; (c) **re-reads** to confirm it owns
  the item — if another session won the race, it backs off and takes the next item.
  Assignee (single-owner) is the lock; the re-read closes the read-modify-write gap
  in the Project `Status` field.
- **Document the concurrency model** in `flow-lead`: default is **one lead →
  subagents** (disjoint work assigned by the lead — zero dup by design); the claim
  protocol covers **independent sessions**; Agent Teams
  (`CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1`, desktop/tmux-only) is the optional
  parallel-coordination path — never run N bare leads without the claim protocol.
- **Mobile / remote operation.** Document **Remote Control** (`claude
  remote-control` on the laptop; phone drives the full local pipeline) as the
  primary phone path, and add a **cloud setup script** so a Claude Code on the web
  session can run `flow-implement` (fetch datasets, install `openspec` + `gh`). The
  two human seams (approve spec, merge PR) stay GitHub-mobile-native.
- **Graceful degradation.** If the `project` token scope or the board is absent,
  `flow-*` falls back to the existing label-based `status:*` model and never blocks.

## Capabilities

### New Capabilities
- `work-coordination`: the shared claim board (GitHub Project), the atomic-ish
  claim protocol, the concurrency model, and the mobile/remote operating paths.

### Modified Capabilities
- `delivery-pipeline`: the `flow-*` verbs gain claim/board steps (invoked, not
  re-specified here — listed for traceability).

## Impact

- **Skills:** `flow-board`, `flow-groom`, `flow-activate`, `flow-implement`,
  `flow-finalize` gain claim/board steps; `flow-lead` gains the concurrency-model +
  mobile doc.
- **Setup:** a cloud setup script (datasets + `openspec`/`gh`); a documented
  one-time `gh auth refresh -s project` prerequisite (the owner's action).
- **Docs:** website `agents-developing/delivery-pipeline` (board + concurrency +
  mobile section).
- **No cqlite-core / binding / CI code changes.**

## Non-goals

- Not building a custom lock server or external coordination service — GitHub
  assignee + Project Status + re-read is the mechanism.
- Not REQUIRING Agent Teams (it stays an optional desktop path).
- Not changing the gate / C / roborev / done-bar.
- Not making the full local pipeline runnable on the phone itself — mobile drives
  a local (Remote Control) or cloud (web) session; it does not execute locally.
