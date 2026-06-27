## 1. Claim board (GitHub Project)

- [ ] 1.1 Create + repo-link a GitHub Project (v2) with a `Status` single-select
      (`Backlog/Ready/In Progress/In Review/Done`); enable built-in workflows
      (auto `In Progress` on assign, `Done` on PR merge). Document the one-time
      `gh auth refresh -s project` prerequisite.
- [ ] 1.2 `flow-board`: render the board from `gh project item-list` (item, status,
      assignee, priority); show each `In Progress` item's owner. Surface = the
      board output; verify against a seeded Project.

## 2. Claim protocol

- [ ] 2.1 Add the claim step to `flow-activate` / `flow-implement` (and the
      `flow-board` "next" pick): unassigned `Ready` only → `--add-assignee @me` +
      `Status=In Progress` → re-read → proceed only if assignee; else next item.
- [ ] 2.2 `flow-finalize`: set `Status=Done` (or rely on the merge automation) and
      release the claim. Verify no item is left `In Progress` after finalize.

## 3. Concurrency model

- [ ] 3.1 Document the model in `flow-lead` (default lead+subagents; claim protocol
      for independent sessions; Agent Teams optional desktop-only; never N bare
      leads without the protocol).

## 4. Mobile / remote

- [ ] 4.1 Document Remote Control (`claude remote-control`) as the phone-driving
      path in the delivery-pipeline page.
- [ ] 4.2 Add a cloud setup script (install `openspec` + `gh`, fetch the dataset)
      so Claude Code on the web can run `flow-implement`; document it + the GitHub
      seams (approve, merge) as mobile-native.

## 5. Graceful degradation

- [ ] 5.1 `flow-*` detects a missing `project` scope/board and falls back to the
      `status:*` label model without error. Verify by running flow-board with the
      scope absent.

## 6. Docs

- [ ] 6.1 Update website `agents-developing/delivery-pipeline` (board + claim
      protocol + concurrency model + mobile section) and cross-link from CLAUDE.md.

## 7. Gate & review (done criteria)

- [ ] 7.1 Confirm no code lanes regress (docs/agent/script-only: no `*.rs`/`Cargo.*`
      changes; if a setup `*.sh` is added, shellcheck it).
- [ ] 7.2 roborev clean on the branch.
- [ ] 7.3 Self-audit with C: every `work-coordination` requirement `satisfied` with
      evidence (incl. the claim-race scenario reasoned through against the skill steps).
