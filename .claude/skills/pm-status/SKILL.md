---
name: pm-status
description: DEPRECATED — superseded by flow-board. Kept as a pointer so old references don't 404. Use flow-board for the claim board + the one next thing.
disable-model-invocation: true
argument-hint: [optional epic number or label]
---

# pm-status — DEPRECATED (issue #1855)

Superseded by **`flow-board`**, which renders the shared GitHub Project claim board (status, assignee,
priority), reconciles drift, reaps abandoned claims, and surfaces the single item waiting on the owner —
things this older label-only sweep did not do. Use `flow-board` instead.

Follow-on actions live in the pipeline: `flow-finalize` (close a merged issue), `flow-implement`
(drive one), the `manager`/`worker` commands (run the backlog). Doctrine:
`docs/development/pm-operating-loop.md`.
