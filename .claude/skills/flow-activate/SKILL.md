---
name: flow-activate
description: Activate a groomed design-driven issue — create its worktree + branch (1:1:1:1), run OpenSpec propose to produce proposal/design/specs/tasks, render the spec + recommended design INLINE, then STOP for the owner's approval. This is Seam 1; it never implements. Second stage of the CQLite delivery pipeline. Use when the owner says "activate #N".
---

# flow-activate — spec + design, then stop for approval (Seam 1)

You are the CQLite delivery lead. Take a `status:ready` design-driven issue and produce a committed,
owner-approvable OpenSpec change on an isolated worktree. **STOP at approval — do not implement.**

## Steps

1. **Load the issue.** `gh issue view <N> --json number,title,body,labels`. Derive a kebab-case `slug`.
   If the issue is oracle-driven (per its body), say so and route to `flow-implement` instead — no
   OpenSpec.
2. **Create the worktree + branch** (1:1:1:1) from up-to-date `origin/main`:
   ```bash
   git -C <repo-root> fetch origin
   git -C <repo-root> worktree add ".claude/worktrees/issue-<N>-<slug>" -b "issue-<N>-<slug>" origin/main
   ```
   All spec work happens in that worktree.
3. **Propose** with OpenSpec (use the `opsx:propose` skill / `openspec new change <slug>`): author
   `proposal.md` (state milestone + oracle/design + Non-goals + doctrine impact), `design.md`,
   `specs/<capability>/spec.md` (every requirement gets a verifiable `#### Scenario:`), `tasks.md`
   (each task names the surface it exercises; include gate + C + roborev steps). Consult specialists for
   facts where useful (e.g. a parity/format question → `test-validator` / `sstable-developer`), but
   **never decide a product/data-model question** — surface options to the owner.
4. **Validate:** `openspec validate <slug> --strict` (must be clean). Commit the artifacts.
5. **Render INLINE and STOP.** Show the owner, in the conversation:
   - the proposal summary + Non-goals,
   - the spec requirements + `#### Scenario:` blocks **verbatim**,
   - the recommended design (chosen + what it beat),
   then flip the label and wait:
   ```bash
   gh issue edit <N> --remove-label status:ready --add-label status:spec-review
   ```
   Do not start `flow-implement`. Approval is the owner's seam.
