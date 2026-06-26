## 1. Manager agent

- [x] 1.1 Add `.claude/agents/flow-lead.md` — orchestrator persona: orient from
      the board, drive the pipeline, spawn specialists, never write production
      code, honor the two seams + the pre-authorized merge-on-green model (D4),
      pass explicit model overrides when spawning (D7). Surface: the agent file;
      verify by starting a session as flow-lead and confirming it boards-first.
- [x] 1.2 Set `"agent": "flow-lead"` in `.claude/settings.json`.

## 2. Pipeline skills (thin wrappers)

- [x] 2.1 `flow-groom` — idea → one scoped issue (one P-label, `status:ready`,
      testable criteria). Route oracle-driven bugs to issue+pinned-test (skip
      OpenSpec).
- [x] 2.2 `flow-activate` — worktree+branch (1:1:1:1) + `opsx:propose` + design;
      render inline; STOP at Seam 1.
- [x] 2.3 `flow-implement` — spawn the specialist team in the worktree, run
      `agent-gate.sh` → C (spec-auditor) → roborev, open the PR; do not merge by
      default.
- [x] 2.4 `flow-address` — resolve PR review comments in the worktree, push,
      reply per thread.
- [x] 2.5 `flow-finalize` — `opsx:archive` + remove worktree/branch + close
      issue; only after merge.
- [x] 2.6 `flow-board` — status across in-flight issues (labels + PR/CI) and
      surface the single next item for the owner.

## 3. Roster + autonomy wiring

- [x] 3.1 Map the pipeline's specialist roles to CQLite agents (sstable-developer,
      rust-reviewer, spec-auditor/C, test-validator, coverage-reviewer) + roborev
      + agent-gate (D3), documented in flow-lead + flow-implement.
- [x] 3.2 Encode the pre-authorized merge-on-green autonomy model (D4) and
      reconcile CLAUDE.md "Product-manager behavior (lead)" with it (D6).

## 4. Doctrine & docs

- [x] 4.1 Document the delivery pipeline + the two seams + autonomy model in
      CLAUDE.md and a new website `agents-developing/delivery-pipeline` page;
      cross-link the spec-driven-audit page.

## 5. Gate & review (done criteria)

- [x] 5.1 Confirm no code lanes regress (docs/agent-only: zero `*.rs`/`Cargo.*`/
      `.github/**`/`*.sh` files change → gate code lanes unaffected; full gate
      N/A, this inspection is the evidence).
- [x] 5.2 roborev clean on the branch.
- [x] 5.3 Self-audit with C: run `spec-auditor` against this change and confirm
      every `delivery-pipeline` requirement is `satisfied` with evidence
      (dogfood — and the first change run through the very pipeline it defines,
      once 2.x exist).
