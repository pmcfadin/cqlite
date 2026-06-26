## 1. Spec-anchored audit (C)

- [x] 1.1 Re-anchor the `spec-auditor` subagent to read OpenSpec change specs
      (`openspec/changes/<name>/specs/**`) as its criteria instead of a GitHub
      issue body. Surface exercised: the `spec-auditor` agent definition; verify
      by running it against this very change (`spec-driven-audit`) and confirming
      it reports a per-requirement verdict.
- [x] 1.2 Define the verdict contract (per requirement: `satisfied` / `partial` /
      `unmet` + evidence = test path + public-surface call chain). Exercised via
      a sample audit run whose output matches the contract.
- [x] 1.3 Encode blocking semantics: `unmet`, uncovered-requirement, and
      unjustified-`partial` block merge. Verify with a deliberately-incomplete
      change that C correctly blocks it.

## 2. Optional roborev escalation (B)

- [x] 2.1 Document invoking `roborev-design-review-branch` with a change's
      proposal/design/specs as criteria, and the trigger conditions (C
      `partial` / high-stakes / doctrine-touching). Exercised by a dry-run on
      this change.

## 3. Merge-flow integration

- [x] 3.1 Wire C into the attended merge flow at the defined stage
      (`gate → C → roborev → merge → archive`); C runs only on a green gate.
      Verify the ordering on a real change end-to-end.

## 4. Doctrine & docs

- [x] 4.1 Document the audit loop and the superpowers↔OpenSpec mapping (design
      D5) in CLAUDE.md "agent-team conventions" and the website
      `agents-developing/` section (new audit page).
- [x] 4.2 Update the "done" definition to include "C audit passed".

## 5. Gate & review (done criteria)

- [x] 5.1 Run `scripts/agent-gate.sh`; paste the AGENT-GATE SUMMARY block. (Docs/
      agent-only change: confirm no code lanes regress.)
- [ ] 5.2 roborev clean on the branch.
- [x] 5.3 Self-audit: run C against this change and confirm every requirement in
      `specs/change-audit/spec.md` is `satisfied` with evidence (dogfood).
