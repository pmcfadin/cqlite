---
name: prioritize
description: Rank open GitHub epics and issues by priority and recommend what to work on next. Read-only — never modifies issues. Invoke as /prioritize (optionally scoped to a label, milestone, or epic).
disable-model-invocation: true
argument-hint: [optional label, milestone, or epic number]
---

# Prioritize

Produce a prioritized view of the backlog. This skill is READ-ONLY — do not edit,
comment on, assign, or close anything.

> **ADVISORY ONLY — NOT a claim or dispatch source (Path A, #1886).** This skill ranks from `gh issue list`
> labels/milestones, which is a *lagging* view: the **GitHub Project `Status` field is the sole dispatch
> authority**, and `status:*` labels are only its ≤30-min-lagging board-derived mirror (#2855). Nothing
> here authorizes picking up work. The "START NEXT" line is a **recommendation for the owner**, not a
> dispatch instruction: actually starting an item goes through `flow-board`/`flow-activate` — a fresh board
> read confirming `Status=Ready` **plus** acquiring the claim ref
> (`bash scripts/flow/claim.sh claim <N>`). Selecting work from a label ranking is the exact wrong-grab
> pattern Path A exists to prevent.

## 1. Gather
Use `gh` to list open work. If $ARGUMENTS names a label, milestone, or epic, scope to
it; otherwise cover all open issues.
- `gh issue list --state open --limit 200 --json number,title,labels,assignees,milestone,createdAt,updatedAt`
- For epics, also read child issues so blocked-by relationships are visible.

## 2. Score each item
Rate every issue on this rubric and show the factors, not just a number:
- Impact: user/business value or how much it unblocks.
- Effort: rough size (S/M/L) inferred from the description.
- Dependencies: is it blocked by, or blocking, other issues?
- Deadline: any milestone or date pressure.
- Staleness: time since last activity (use updatedAt).
Rank highest-leverage first: high impact, unblocks others, deadline-driven, low effort.
Flag anything blocked (and by what) and anything stale that's silently stuck.

## 3. Recommend
Output a ranked, skimmable table (rank, #, title, impact/effort, why) and a short
"START NEXT" line — the single epic or issue you'd pick up now and why. Assume I may be
reading this on my phone.

Label the "START NEXT" line **advisory** and note that it must be confirmed against the board
(`Status=Ready`) and claimed via `bash scripts/flow/claim.sh claim <N>` before any work begins — this
ranking is not the queue.
