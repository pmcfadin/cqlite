## Context

CQLite already enforces correctness (`scripts/agent-gate.sh`), behavior
(sstabledump parity goldens), public-surface usage (wiring-evidence rule), and
code quality (roborev). Adopting OpenSpec as the front door for design-driven
work adds structured **intent** (requirements + Given/When/Then scenarios). The
missing piece is a check that the implementation satisfies that intent. This
change defines that check (C) and an optional escalation (B), and — because the
team also uses the `superpowers` skill family — pins how the two process
frameworks relate so they nest rather than compete.

## Goals / Non-Goals

**Goals:**
- A read-only, spec-anchored intent audit (C) that blocks merge on unmet/uncovered requirements.
- An optional, on-demand roborev escalation (B) using the change's artifacts as criteria.
- A defined, non-overlapping place for each layer in the merge flow.
- A clear mapping between OpenSpec's lifecycle and the superpowers process skills.

**Non-Goals:**
- Replacing the gate or parity (correctness ≠ intent).
- A CI-blocking Actions job (attended flow first; CI later).
- Auditing oracle-driven bug fixes (issue + pinned test, not OpenSpec).

## Decisions

### D1 — C is the `spec-auditor` subagent, re-anchored to OpenSpec specs
The existing `spec-auditor` agent (read-only; audits against acceptance
criteria) becomes C, with its criteria source switched from a GitHub issue body
to `openspec/changes/<name>/specs/**`. Per requirement it reports
`satisfied | partial | unmet` plus evidence (the test + public-surface call
chain). Rationale: reuse a proven discipline; the only change is a more
structured, checkable input.

### D2 — Merge-flow ordering (no overlap between layers)
`apply → gate(correctness) → C(intent) → roborev(code) → merge → archive`.
C runs only after the gate is green (auditing intent on broken code is wasted).
B (roborev spec-anchored design review) is an escalation off C, not a separate
stage. Rationale: each layer answers a different question; ordering prevents
double-work and false greens.

### D3 — Blocking semantics
Any `unmet` requirement, or any requirement whose scenario has no exercising
test from the public surface, blocks merge. `partial` requires explicit written
justification in the audit verdict or it is treated as `unmet`. Rationale: this
is the whole point — unverifiable intent must not merge.

### D4 — B reuses `roborev-design-review-branch`
B is the existing skill invoked with the change's proposal/design/specs as
criteria; no new tooling. Triggered when C reports `partial`, the change is
high-stakes, or it touches doctrine (no-heuristics / cross-binding parity).

### D5 — Relationship to the superpowers process skills (alignment)
The two frameworks are **complementary, not competing**: superpowers are the
*techniques/discipline*; OpenSpec is the *artifact system + lifecycle*. They
nest. Per the superpowers instruction-priority, explicit user/project
instructions (this doctrine) win where they overlap.

| Lifecycle stage      | OpenSpec (system of record) | superpowers (technique used)            |
|----------------------|-----------------------------|-----------------------------------------|
| Think / clarify      | `explore`                   | `brainstorming` (the method inside explore) |
| Capture intent       | `propose` → proposal/design/specs/tasks | `writing-plans` (technique; OpenSpec is the durable output) |
| Implement            | `apply` (tasks)             | `test-driven-development`, `subagent-driven-development`, `using-git-worktrees` |
| Verify correctness   | gate precondition to C      | `verification-before-completion`        |
| Audit intent (C)     | **change-audit** (this change) | `requesting-code-review` framing        |
| Review code          | roborev                     | `receiving-code-review` (verify, don't perform agreement) |
| Integrate            | `archive`                   | `finishing-a-development-branch`         |

Front-door rule: **design-driven new work** enters via OpenSpec `explore`
(using the brainstorming technique) and is captured via `propose`.
**Oracle-driven bug fixes** skip both — GitHub issue + pinned parity test.
Superpowers' "MUST brainstorm before creative work" is satisfied BY explore;
its "plan" artifact is satisfied BY the OpenSpec proposal/design/tasks (no
parallel ephemeral plan file). This avoids two systems of record.

## Risks / Trade-offs

- **Process weight on small changes.** Mitigation: the oracle-vs-design gate in
  config keeps trivial/oracle work out of OpenSpec entirely.
- **C false-confidence.** A spec with weak scenarios yields a weak audit;
  mitigated by the `spec` rule (every requirement needs a verifiable scenario)
  and B as escalation.
- **Two frameworks confusion.** Mitigated by D5's explicit mapping; documented
  in CLAUDE.md + the website so there is one stated relationship, not folklore.
- **Attended-only initially.** C runs in the human/lead merge flow, not CI, so a
  bypassed merge skips it until a CI integration lands (deferred, by Non-goal).
