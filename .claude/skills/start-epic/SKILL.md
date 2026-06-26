---
name: start-epic
description: Kick off an agent team to deliver a GitHub epic issue and its child issues, with spec-adherence and test-coverage gates. Invoke manually as /start-epic <epic-issue-number>.
disable-model-invocation: true
argument-hint: <epic-issue-number>
---

# Start epic

You are the product-manager lead for delivering GitHub epic #$ARGUMENTS. Do not
implement the issues yourself — coordinate teammates, enforce the gates, and
synthesize. Wait for teammates to finish rather than doing their work for them.

## 1. Ingest the epic
Use `gh` to load the epic and its children:
- `gh issue view $ARGUMENTS --json number,title,body,labels` for the epic.
- Find child issues from the epic body (task-list items and `#NNN` references) and/or
  `gh issue list` filtered by the epic's tracking label.
- `gh issue view <n> --json number,title,body` for each child to capture criteria.
Summarize the epic goal and list the child issues before spawning anyone.

## 2. Plan
Create one task per child issue. Record dependencies so a dependent task stays blocked
until its prerequisites complete. Split any issue too large to finish in one pass.

## 3. Spawn the team
- Spawn at most 4 implementer teammates at once, one per issue or module, so no two
  edit the same files. If more than 4 issues are ready, queue the rest and assign them
  as implementers free up. Put each issue's acceptance criteria in its spawn prompt.
  Use Sonnet unless an issue is unusually complex.
- Always also spawn one spec-auditor (agent type `spec-auditor`) and one
  coverage-reviewer (agent type `coverage-reviewer`).
Tell implementers to commit often so roborev reviews land while context is fresh, and
to clear roborev findings (`/roborev-fix`) before handing an issue off.

## 4. Gate completion
An issue's task is done only when all hold:
1. The deterministic gate passes (tests, coverage, no open roborev failures) — enforced
   by the TaskCompleted hook. If it blocks a completion, route the responsible teammate
   back to fix the specific gap it reports.
2. The spec-auditor confirms the implementation meets the acceptance criteria.
3. The coverage-reviewer confirms the tests are meaningful, not merely present.
Only approve a plan or mark an issue complete when these are met.

## 5. Synthesize
When every issue is done, post a summary to me: what shipped per issue, deviations
from spec and why, and open risks. Do not close the epic yourself.
