---
name: cqlite-development
description: Use when implementing or reviewing CQLite features, bug fixes, or development-process changes, including requests to finish an OpenSpec change or verify that code is ready.
---

# CQLite development

Read repository-root AGENTS.md first. Paths below are repository-relative.

1. Classify the request. Design-driven work uses the existing OpenSpec skills
   under `.codex/skills/`: propose artifacts, validate strictly, then apply the
   approved design. Existing conversation approval is sufficient. Oracle-driven
   fixes use issue criteria plus pinned Cassandra regression evidence. Routine
   edits within an approved change do not require a new proposal.
2. Read CLI-provided OpenSpec context files and maintain its tasks. If a referenced
   skill is not in the current selector, read its SKILL.md directly. Use available
   question tools or conversation for missing information; do not invent tool calls.
3. Implement bounded tasks with relevant domain skills. For behavior changes,
   add a regression/acceptance test that fails for the intended reason before
   the fix where practical, then verify the public behavior. For instruction
   changes, use realistic fresh-agent scenarios rather than Rust unit tests.
4. Follow `ci-cd-validation` for targeted tests, lite iteration, and full pre-merge
   certification. Keep tests exercised, compiled-only, skipped, and unavailable
   distinct. Do not turn unavailable evidence into a passing result.
5. Review the diff before the expensive full gate. Use the Rust reviewer for Rust
   code and the coverage reviewer for test adequacy. For final design certification,
   perform the spec intent audit after the full gate. Delegate read-only reviews
   using configured roles or generic reviewers loaded with
   [spec audit](references/spec-audit.md) and
   [test quality](references/test-quality.md). Supply the actual criteria, diff,
   source revision and test evidence, not an expected verdict. Reviewers report;
   the implementer fixes. If delegation is unavailable, report missing sign-off.
6. Report requirement → public surface/test → execution evidence, plus unresolved
   findings. An OpenSpec `all_done` state means task bookkeeping is complete;
   inspect evidence before describing the feature as verified. Uncovered scenarios
   block completion even if the overall test command exited successfully.
7. Deliver only within requested scope. For GitHub issue delivery, read
   `docs/development/pm-operating-loop.md` for board/claim/worktree rules and
   `docs/development/merge-gate.md` before arming auto-merge. A local edit can be
   implemented and checked without having been PR-certified or merged. Confirm
   the PR is MERGED and represents completed work before invoking
   `openspec-archive-change`; keep slices and unmerged changes active.

Do not reproduce fleet scheduling, model pins, or Claude-specific background APIs.
For unattended owner decisions, persist the unresolved question in the task's
existing durable context and report blocked; post externally only when authorized.
