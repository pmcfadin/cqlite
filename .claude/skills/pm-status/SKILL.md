---
name: pm-status
description: Sweep in-flight GitHub epics and issues, surface what's stalled or blocked, and advance them — commenting, labeling, assigning, and closing completed issues per the rules in CLAUDE.md. Invoke as /pm-status (optionally scoped to an epic).
disable-model-invocation: true
argument-hint: [optional epic number or label]
---

# PM status sweep

Report the state of in-flight work and keep it moving. You MAY write to GitHub
(comment, label, assign, close issues) under the rules below. You may NOT close epics,
change an issue's scope or title, or make a product decision on your own.

## 1. Assess
Use `gh` to load open issues (scope to $ARGUMENTS if given) with state, assignees,
labels, linked PRs, and last activity. For each epic, read its child issues.

## 2. Classify each issue
- Done: acceptance criteria met AND work clearly complete (e.g. a merged linked PR).
- Blocked: waiting on another issue/PR — identify the blocker.
- Stalled: no activity for a while with no blocker — needs a nudge or reassignment.
- Needs a decision from me: ambiguous scope, conflicting requirements, or a tradeoff.
- On track: leave it alone.

## 3. Act (autonomously, with an audit trail)
- Close issues only in the "Done" class, and post a closing comment stating why (which
  criteria were met, which PR). Never close an epic — instead comment on it summarizing
  progress and ask me to close it.
- Apply/adjust status labels to match the classification.
- Assign or reassign an unassigned in-progress issue to keep it moving, and comment on
  the change.
- For "needs a decision" items, do NOT guess — leave them for me.
Every write must be traceable with a short comment so I can review or reverse it.

## 4. Report
End with a concise, phone-readable summary: what you closed / assigned / relabeled,
what's blocked (and on what), what's stalled, and — most important — a short "NEEDS
YOU" list of decisions only I can make. If nothing needs me, say so in one line.
