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
   AND **no** claim exists on origin. **Select by board `Status` ONLY — never by the `status:ready`
   label** (Path A, #1886: labels are decorative; the board is the sole dispatch authority). If the
   board is unreachable, STOP and fix auth — do NOT fall back to labels to establish eligibility. Empty
   Ready → nothing to activate; stop. The lock is the slugless fixed-name ref `refs/claims/issue-<N>`
   (#2665) — NOT the assignee (`@me` is identical for the same GitHub user on two machines) and NOT a
   slug-named branch (a different slug or an identical-SHA base once double-claimed; #1632):
   ```bash
   git -C <repo-root> fetch origin -q
   if bash scripts/flow/claim.sh status <N> | grep -q "CLAIM: STATUS issue=<N>" \
      || git -C <repo-root> ls-remote --heads origin "issue-<N>-*" | grep -q .; then
     echo "Already claimed (claim ref or a legacy issue-<N>-* branch on origin) — take the next item."
     # exit / pick a different item; another machine may `git fetch` its branch to RESUME instead.
   fi
   ```
   (For the Project-vs-labels detection snippet shared by all flow-* skills, see `flow-board`.)
3. **Acquire the claim FIRST, then create the worktree + branch (PR plumbing, 1:1:1:1).** `claim.sh` is
   the lock — run it before any worktree/spec work. It pushes a UNIQUE root-commit to the fixed-name ref
   `refs/claims/issue-<N>`, so git's server-side ref arbitration decides the race regardless of slug or
   base (a different-slug or identical-SHA competitor can no longer double-claim):
   ```bash
   if ! bash scripts/flow/claim.sh claim <N>; then
     echo "CLAIM LOST — another session holds refs/claims/issue-<N>. Take the next item."
     # (Adopting a reaped claim instead? Use: bash scripts/flow/claim.sh adopt <N> --expect <old-sha>)
     # (Refused with reason=legacy-branch-lock ... claim-ref=free — a FREE claim ref but an
     #  issue-<N>-* branch still on origin? The sanctioned resume is
     #  `claim.sh adopt <N> --expect none --reason resume-legacy-branch-lock:branch-outlived-claim`
     #  (#2945 — a placeholder reason, or one still carrying an unsubstituted <…>, is
     #  rejected with exit 64, so substitute a concrete why). The refusal
     #  deliberately does NOT print it: CONFIRM the lane is abandoned first —
     #  `claim-heartbeat.sh should-reap <machine>` (age > 4h AND no open PR AND pid-dead if
     #  local), board Status, branch/PR author. Never resume blind, never hand-craft a claim
     #  commit)
   fi
   # CLAIM HELD → set up the worktree + branch. The branch is naming/PR plumbing, NOT the lock:
   wt=".claude/worktrees/issue-<N>-<slug>"
   git -C <repo-root> worktree add "$wt" -b "issue-<N>-<slug>" origin/main
   git -C "$wt" push -u origin "issue-<N>-<slug>"   # PR head — NOT the lock
   # MACHINE-LOCAL lane lock (#3436) — the claim ref's local blind spot. It is a hard
   # control cross-machine (git arbitrates the push) and a pure ADVISORY locally: a
   # session that never runs claim.sh just proceeds, and even one that does is waved
   # through, because claim.sh's re-entrancy is machine+actor and two sessions on one
   # box are BOTH machine=<box> actor=flow. This lock's identity is the full PROCESS
   # identity, so it can tell them apart. Take it before the first write to the lane.
   bash scripts/flow/lane-lock.sh acquire <N> --lane-dir "$(cd "$wt" && pwd)" || {
     # OCCUPIED names the occupant (pid, start identity, age). ALIVE and every
     # UNKNOWN-* REFUSE; only a verifiably DEAD holder is auto-reclaimed. Do not
     # proceed into a lane a live process owns — that IS the #3436 incident.
     exit 0
   }
   # Board visibility: assignee + Status=In Progress. Run the flow-board detection snippet FIRST — it
   # does `gh auth switch --user "$project_account"` so the project-capable account is active (the EMU
   # account flip otherwise makes the board write fail and degrade to a label SILENTLY).
   gh issue edit <N> --add-assignee @me
   # have_project=1 → set the board Status. `--field` is NOT a gh flag (verified gh 2.87.3 offers only
   # --field-id); all four IDs are required or the write fails:
   gh project item-edit --id <item-id> --project-id <project-id> \
     --field-id <status-field-id> --single-select-option-id <In-Progress-option-id>
   # (have_project=0 cannot occur here — Path A eligibility in step 2 already required a reachable board.
   #  Do NOT write a status:in-progress label — the board→label mirror (#2855) derives it from the
   #  board Status you just set; a hand-written board-derived label is reverted on the next mirror pass.)
   # scripts/flow/*.sh blobs are mode 100644 (no +x) — ALWAYS invoke them via `bash`:
   bash scripts/flow/claim-heartbeat.sh beat <N>   # FIRST beat — establishes refs/heartbeats/<machine> (#2089)
   ```
   If `claim.sh claim` reports `CLAIM LOST`, you did NOT win — do not create the worktree; take the next
   eligible item. All spec work happens in that worktree only after `CLAIM HELD`.
4. **Propose** with OpenSpec (use the `opsx:propose` skill / `openspec new change <slug>`): author
   `proposal.md` (state milestone + oracle/design + Non-goals + doctrine impact), `design.md`,
   `specs/<capability>/spec.md` (every requirement gets a verifiable `#### Scenario:`), `tasks.md`
   (each task names the surface it exercises; include gate + C + roborev steps — roborev ALWAYS via
   `bash scripts/flow/roborev-review.sh --agent <agent> --model <model>`, the only sanctioned invocation
   (#2964), never a bare `roborev review --branch`). Consult specialists for
   facts where useful (e.g. a parity/format question → `test-validator` / `sstable-developer`), but
   **never decide a product/data-model question** — surface options to the owner.
5. **Validate:** `openspec validate <slug> --strict` (must be clean). Commit the artifacts.
6. **Render INLINE and STOP.** Show the owner, in the conversation:
   - the proposal summary + Non-goals,
   - the spec requirements + `#### Scenario:` blocks **verbatim**,
   - the recommended design (chosen + what it beat),
   then set the transient spec-review sub-marker and wait:
   ```bash
   # spec-review is a transient skill-managed sub-marker (NOT a board Status option) — the
   # board→label mirror (#2855) does not touch it. Do NOT write status:ready/in-progress/in-review;
   # those labels are the mirror's now, derived from the board Status you set.
   gh issue edit <N> --add-label status:spec-review
   ```
   Do not start `flow-implement`. Approval is the owner's seam.
7. **Drop the spec render after approval (issue #2085).** The inline render exists only to get the owner's
   Seam-1 approval — once approved, **do not retain the verbatim spec/design body** in the session window.
   Every downstream agent re-reads it fresh from `openspec/changes/<slug>/` (the `spec-auditor`/C audit
   re-reads `specs/**` anyway), so keeping the render is pure inter-issue accretion. Render → approve → drop.
