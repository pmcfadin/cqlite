# Tasks — gate-merge-result-premerge-assert (issue #3680)

**Blocked on Seam 1** for the two decisions in design D5 (AC1, the recursion) and D6 (AC1b, the git
version floor). Everything in §1 that does not depend on them can proceed once the spec is approved.

## 0. Preconditions

- [ ] 0.1 Rebase onto PR #3842 (#3752) once it merges — it rewrites `premerge-assert.sh` and three of the
      five disclaimer locations. Adopt its shape (D8); do not race it.
- [x] 0.2 Re-verify every integration point (the issue's map is stale — design D1).
- [x] 0.3 Measure the composition mechanism and the recursion property (design D2, D5).

## 1. The merge-result gate mode (`scripts/agent-gate.sh`)

- [ ] 1.1 Add the mode arm to `case "${1:-}"` at **:5828**, as **argument 1** (no outer loop exists).
      Follow `--delta`'s inner `while`/`shift` shape if options are needed; unknown option ⇒ exit 2.
- [ ] 1.2 Extend the `if`/`elif` marker chain at **:6064-6078** with the fourth marker pair and a
      `MODE:` line. Full stays the default arm with no `MODE:` line.
- [ ] 1.3 Add the per-mode default summary path at **:6744-6763**.
- [ ] 1.4 Compose: `merge-tree --write-tree` → `commit-tree` (two parents) → `worktree add --detach`.
      Refuse with a named cause on conflict or on a failing git command; stamp no certification.
- [ ] 1.5 Add the composed-base / branch-head / composed-commit keys to the summary block.
- [ ] 1.6 Dispatch alongside **:17805/:17814**, before the full-gate fall-through.
- [ ] 1.7 Leave `acquire_gate_slot` (**:17748**) and `_component_set_strict` (**:5280**) alone — the mode
      queues and is fail-closed by falling outside both positive enumerations. Assert both in a test.
- [ ] 1.8 Scratch-worktree lifecycle: create, gate, remove. Do not leave `.git/worktrees` entries behind.
- [ ] 1.9 Usage table at **:748-806**.

## 2. The four unlisted integration points (design D4) — each silent if missed

- [ ] 2.1 `scripts/gate-liveness.sh` — teach the closed dialect regex (:523, :528, :539, :540, :564,
      :685) and the opener/closer dialect-match check (:567-570) the fourth dialect.
- [ ] 2.2 New emit site: stamp `COMPONENT_SET_LINE` or add `# component-set-exempt: <reason>`, or
      `test_agent_gate_component_set.sh:2415-2503` FAILs `tooling-tests`.
- [ ] 2.3 `premerge-assert.sh:441-443` — add the merge-result markers to the by-name refusal list.
- [ ] 2.4 `agent-gate.sh:17413-17415` — `--anchor-summary-file` must reject the merge-result marker too.

## 3. Enforcement (`scripts/flow/premerge-assert.sh`)

- [ ] 3.1 Branch on the advisory `adv_rc` captured at **:385-386** and today only printed (**:398-401**).
      Closed token set, matched token-exactly; `4`, `5`, usage, unrecognised, empty and could-not-run all
      ⇒ stale. Implement as a sourced helper per D8.
- [ ] 3.2 Require the merge-result certification when stale: `RESULT: PASS` + `tree-integrity: PASS` +
      `dirty: no` + affirmative `MODE:` + the composed-base key. Refuse otherwise.
- [ ] 3.3 Apply the recursion (D5) — the advisory, unmodified, with the composed commit as its subject.
- [ ] 3.4 Do not change the positional contract. Decide and document how the certification is supplied.

## 4. Tests

- [ ] 4.1 `scripts/tests/test_premerge_assert.sh` — cases for every scenario in the spec delta.
- [ ] 4.2 **Mutation checks**: each leg reds when removed or inverted. Explicitly, a case that reds if
      exit `5` is treated as fresh, and one that reds if the merge-result requirement is removed.
- [ ] 4.3 **Add a CASE FLOOR** — the suite has none (50 headers, 43 distinct numbers, no Case 42, bare
      `[ "$FAIL" -eq 0 ]` at :2163).
- [ ] 4.4 Pin the recursion property AND its precondition: an on-main branch head is a degenerate fixture
      (`diff-paths 0`), not a fresh verdict.
- [ ] 4.5 Tests for the mode itself, incl. the slot/strict assertions from 1.7 and the liveness dialect.
- [ ] 4.6 Confirm a failing assertion in each new suite makes `tooling-tests` — and the full gate — FAIL.

## 5. Doctrine (same change, per CLAUDE.md)

- [ ] 5.1 `scripts/flow/premerge-assert.sh` header residual 3 (**:100-117**, + the 119-137 continuation).
- [ ] 5.2 Its success output (**:1012-1034**) — the four `PREMERGE: SCOPE` lines.
- [ ] 5.3 `CLAUDE.md:1734-1743`.
- [ ] 5.4 `.claude/agents/flow-closer.md:262-269` and `.claude/skills/flow-address/SKILL.md:74-76`.
- [ ] 5.5 Extend `test_premerge_assert.sh` Case 39 (**:1310-1364**) in the same diff.
- [ ] 5.6 Website `agents-developing/` page. Verify the publish by grepping for new content, not HTTP 200.
- [ ] 5.7 Do NOT edit `openspec/changes/archive/**`.

## 6. Certification

- [ ] 6.1 `scripts/agent-gate.sh --lite` green each fix round, summary-file redirect (#2079).
- [ ] 6.2 `rust-reviewer` + sanctioned roborev on the lite-green diff, BEFORE any full gate. Push first.
- [ ] 6.3 Open PR with `Closes #3680`. #3650 also closes when this lands — state that in the body, but
      let #3650 be closed by the owner or by its own final record, not by this PR's keyword.
- [ ] 6.4 `flow-closer`: ONE full gate of record → `spec-auditor` C → final roborev → `premerge-assert`
      → `gh pr merge --auto --squash --delete-branch`.
- [ ] 6.5 Telemetry per #3550/#3559. This PR **completes** #3680, so it is NOT `--slice`; #3650's own
      disposition is the owner's.
- [ ] 6.6 State the demonstration split (AC9) in the PR body: which half this PR can exercise on itself
      and which cannot.
