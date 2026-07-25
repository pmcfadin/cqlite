# Enforced one-way board→label mirror (issue #2855)

## Why

Finding Ready work is both context-expensive and drift-prone today:

- `gh issue list --limit N --json …body…` floods agent context with full issue bodies AND cannot
  see board `Status` (Status lives on the project *item*, not the issue) — so it can't find Ready.
- `gh project item-list` sees Status but Projects v2 has **no server-side field filter** — you
  paginate every item and filter client-side (and `--paginate` can loop; measured live).
- The `status:*` labels ARE server-side filterable and cheap (`gh issue list --label status:ready`
  = 0 core-rate, no bodies) — but they **drift** because multiple parties write them (humans +
  flow-* skills) and nothing reconciles. **Measured 2026-07-24: board Ready = 1 (#1883) vs label
  `status:ready` = 20 open** — a 19-issue disagreement. Per Path A (#1886) the labels are therefore
  declared decorative and forbidden as a selection source.

This change makes the label a *trustworthy* projection so it can serve cheap discovery, without
re-drifting and without weakening the claim authority.

## What Changes

Board `Status` stays the single source of truth. The `status:*` label becomes a **derived
projection written by exactly one automated writer** — `.github/workflows/project-board-sync.yml`:

- On the workflow's existing reconciliation pass (the 30-min `sweep` job + `workflow_dispatch`) and
  on issue events, read each OPEN item's board Status and **force-set the one matching `status:*`
  label, removing the others**. No human or flow-* skill writes a `status:*` label again.
- Add a **drift-detector** step that FAILS the workflow run (red) if any OPEN issue's `status:*`
  label disagrees with its board Status — so a regression is caught, never silently re-drifted (same
  fail-loud posture as the existing PROJECTS_TOKEN guard).
- **Rollout reconciles the current 19-issue drift**: the first mirror pass corrects every OPEN
  issue's label to match its board Status; the drift-detector then passes.
- flow-* skills (activate/implement/address/finalize) **stop writing `status:*` labels** — they set
  board Status only; the mirror follows. flow-board and dispatch READ `status:ready` for cheap
  discovery.

## Authority boundary (preserved — must not weaken)

The label is an **enforced read-mirror for DISCOVERY only**. It is eventually-consistent (there is
an event→workflow lag window), so it narrows the candidate set but is **NEVER the claim authority**:

- The claim ref `refs/claims/issue-<N>` + a fresh board read at claim time remain the sole authority
  that prevents double-work (Path A / #2665 unchanged).
- Two machines seeing a stale-Ready label still both go through `claim.sh`; git arbitrates.

## Non-goals

- **No change to the claim protocol or the dispatch authority.** The label never selects or claims
  work; it only narrows discovery.
- **No new bidirectional sync.** Strictly one-way (Status → label). Writing a label never changes
  Status. (In fact the workflow OVERWRITES any hand-set label on its next pass.)
- **No Rust / product-code change.** GitHub Actions workflow + shell + doctrine + flow-* skills only.
- **No change to the existing sync jobs' other duties** (null-status→Backlog sweep, closed-PR→Done
  safety net, claim reaping) beyond adding the mirror + drift-detector.

## Doctrine impact

Labels move from "purely decorative, never a selection source" (Path A) to "an **enforced
read-mirror for discovery**, still never the claim authority." That is a real doctrine change, so
`CLAUDE.md`, `docs/development/pm-operating-loop.md`, and the website `agents-developing/` page are
updated in this same change, and the flow-* skills are updated to stop writing labels + to use the
cheap label query for discovery.

## Routing

Design-driven (process/tooling; no SSTable/parity oracle). Genuine latitude in the event set, the
Backlog/Done treatment, and the doctrine change → OpenSpec.
