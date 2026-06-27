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
2. **Check eligibility (claim protocol, D2).** An item is claimable only if it is `Ready` (Project
   `Status=Ready`, or `status:ready` label in the fallback) AND **no** `issue-<N>-*` branch already
   exists on origin. The origin branch — NOT the assignee — is the cross-machine lock (assignee `@me` is
   identical for the same GitHub user on two machines):
   ```bash
   git -C <repo-root> fetch origin -q
   if git -C <repo-root> ls-remote --heads origin "issue-<N>-*" | grep -q .; then
     echo "Already claimed (origin has an issue-<N>-* branch) — do not work it; take the next item."
     # exit / pick a different item; another machine may `git fetch` that branch to RESUME instead.
   fi
   ```
   (For the Project-vs-labels detection snippet shared by all flow-* skills, see `flow-board`.)
3. **Create the worktree + branch + PUSH it as the claim** (1:1:1:1) from up-to-date `origin/main`. The
   branch is the lock, so push it to origin **immediately** — before any spec work — to establish the
   claim:
   ```bash
   git -C <repo-root> worktree add ".claude/worktrees/issue-<N>-<slug>" -b "issue-<N>-<slug>" origin/main
   # Push the claim branch FIRST (cross-machine lock). --no-verify keeps the empty claim push fast.
   git -C ".claude/worktrees/issue-<N>-<slug>" push -u origin "issue-<N>-<slug>"
   # Board visibility: assignee + Status=In Progress (or status:in-progress label in the fallback).
   gh issue edit <N> --add-assignee @me
   # Project: gh project item-edit ... --field Status --single-select-option-id <In Progress>
   ```
   Then **re-read** and proceed ONLY if you hold the claim — fetch origin again and confirm the branch on
   origin points at YOUR commit (you won the race). If not, you lost: delete the local worktree/branch and
   take the next eligible item.
   ```bash
   git -C <repo-root> fetch origin -q
   remote_sha="$(git -C <repo-root> ls-remote --heads origin "issue-<N>-<slug>" | awk '{print $1}')"
   local_sha="$(git -C ".claude/worktrees/issue-<N>-<slug>" rev-parse HEAD)"
   [ "$remote_sha" = "$local_sha" ] || echo "Lost the race — back off and take the next item."
   ```
   All spec work happens in that worktree only after the claim holds.
4. **Propose** with OpenSpec (use the `opsx:propose` skill / `openspec new change <slug>`): author
   `proposal.md` (state milestone + oracle/design + Non-goals + doctrine impact), `design.md`,
   `specs/<capability>/spec.md` (every requirement gets a verifiable `#### Scenario:`), `tasks.md`
   (each task names the surface it exercises; include gate + C + roborev steps). Consult specialists for
   facts where useful (e.g. a parity/format question → `test-validator` / `sstable-developer`), but
   **never decide a product/data-model question** — surface options to the owner.
5. **Validate:** `openspec validate <slug> --strict` (must be clean). Commit the artifacts.
6. **Render INLINE and STOP.** Show the owner, in the conversation:
   - the proposal summary + Non-goals,
   - the spec requirements + `#### Scenario:` blocks **verbatim**,
   - the recommended design (chosen + what it beat),
   then flip the label and wait:
   ```bash
   gh issue edit <N> --remove-label status:ready --add-label status:spec-review
   ```
   Do not start `flow-implement`. Approval is the owner's seam.
