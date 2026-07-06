---
name: flow-activate
description: Activate a groomed design-driven issue — create its worktree + branch (1:1:1:1), run OpenSpec propose to produce proposal/design/specs/tasks, render the spec + recommended design INLINE, then STOP for the owner's approval. This is Seam 1; it never implements. Second stage of the CQLite delivery pipeline. Use when the owner says "activate #N".
---

# flow-activate — spec + design, then stop for approval (Seam 1)

You are the CQLite delivery lead. Take a design-driven issue whose **board `Status=Ready`** and produce a
committed, owner-approvable OpenSpec change on an isolated worktree. **STOP at approval — do not implement.**

## Steps

1. **Load the issue.** `gh issue view <N> --json number,title,body,labels`. Derive a kebab-case `slug`.
   If the issue is oracle-driven (per its body), say so and route to `flow-implement` instead — no
   OpenSpec.
2. **Check eligibility (claim protocol, D2).** An item is claimable only if its **board `Status=Ready`**
   AND **no** `issue-<N>-*` branch already exists on origin. **Select by board `Status` ONLY — never by
   the `status:ready` label** (Path A, #1886: labels are decorative; the board is the sole dispatch
   authority). If the board is unreachable, STOP and fix auth — do NOT fall back to labels to establish
   eligibility. Empty Ready → nothing to activate; stop. The origin branch — NOT the assignee — is the
   cross-machine lock (assignee `@me` is identical for the same GitHub user on two machines):
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
   wt=".claude/worktrees/issue-<N>-<slug>"
   git -C <repo-root> worktree add "$wt" -b "issue-<N>-<slug>" origin/main
   # UNIQUE claim commit: an empty commit identifying THIS session, so two sessions
   # branching from the same origin/main base get DISTINCT SHAs (a bare push of an
   # identical SHA would be a no-op "up-to-date" success → both would think they won).
   git -C "$wt" commit --allow-empty -m "claim issue-<N> $(hostname -s)-${RANDOM}-$$"
   # Non-force create: first push creates the ref; a colliding push of a different SHA
   # is REJECTED as non-fast-forward → that session lost. Capture the result.
   if ! git -C "$wt" push -u origin "issue-<N>-<slug>" 2>&1; then
     echo "Push rejected — another session holds the claim. Remove the worktree and take the next item."
   fi
   # Board visibility: assignee + Status=In Progress. Run the flow-board detection snippet FIRST — it
   # does `gh auth switch --user "$project_account"` so the project-capable account is active (the EMU
   # account flip otherwise makes the board write fail and degrade to a label SILENTLY).
   gh issue edit <N> --add-assignee @me
   # have_project=1: gh project item-edit ... --field Status --single-select-option-id <In Progress>
   # (have_project=0 cannot occur here — Path A eligibility in step 2 already required a reachable board.
   #  A status:in-progress label write is a decorative mirror for humans only, never a dispatch source.)
   ```
   Then **re-read** and proceed ONLY if you hold the claim — the origin branch tip must equal YOUR
   claim-commit SHA. If the push was rejected OR the SHAs differ, you lost: remove the local
   worktree/branch and take the next eligible item.
   ```bash
   git -C <repo-root> fetch origin -q
   remote_sha="$(git -C <repo-root> ls-remote --heads origin "issue-<N>-<slug>" | awk '{print $1}')"
   local_sha="$(git -C "$wt" rev-parse HEAD)"
   [ "$remote_sha" = "$local_sha" ] || echo "Lost the race — back off and take the next item."
   scripts/flow/claim-heartbeat.sh beat <N>   # FIRST beat — establishes refs/heartbeats/<machine> (#2089)
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
7. **Drop the spec render after approval (issue #2085).** The inline render exists only to get the owner's
   Seam-1 approval — once approved, **do not retain the verbatim spec/design body** in the session window.
   Every downstream agent re-reads it fresh from `openspec/changes/<slug>/` (the `spec-auditor`/C audit
   re-reads `specs/**` anyway), so keeping the render is pure inter-issue accretion. Render → approve → drop.
