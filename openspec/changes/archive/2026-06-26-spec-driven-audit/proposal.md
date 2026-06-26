## Why

CQLite is adopting OpenSpec as the front door for new (design-driven) work. The
point of doing so is to **automate auditing**: verify that an implementation
actually satisfies the intent captured in its change's specs before it merges.

Today the project has strong layers of enforcement — `scripts/agent-gate.sh`
(correctness), sstabledump parity goldens (behavior), the wiring-evidence rule
(public surface), and roborev (code review). What is missing is an **intent
layer**: a check that every requirement in a change's specs is actually met by
the diff. This sprint that role was played by a subagent auditing against a
GitHub *issue body* (unstructured prose). OpenSpec specs are structured
(requirements + Given/When/Then scenarios), so the audit can be anchored to
checkable criteria instead of prose.

This change establishes that intent layer.

- **Milestone:** maintenance / process (enables M6+ to be built spec-first).
- **Oracle vs design:** design-driven — there is no Cassandra oracle for a
  workflow; the intent must be written down. (Textbook OpenSpec candidate, and a
  deliberate dogfood: the audit workflow is itself the first audited artifact.)

## What Changes

- Introduce **C — a spec-anchored audit** that runs per change after the gate is
  green and before merge. It reads `openspec/changes/<name>/specs/**` and the
  diff, and emits a per-requirement verdict (`satisfied` / `partial` / `unmet`)
  with evidence (the test + call chain that exercises each requirement's
  scenario from the public surface). Any `unmet` requirement, or a requirement
  whose scenario has no exercising test, BLOCKS merge.
- Reuse the existing `spec-auditor` subagent as C's basis, re-anchored to read
  OpenSpec change specs instead of a GitHub issue body. The auditor stays
  read-only.
- Add **B — an optional roborev escalation**: invoke the existing
  `roborev-design-review-branch` skill with the change's proposal/design/specs
  as review criteria, for an independent semantic second opinion. B is invoked
  on demand (C reports `partial`, high-stakes change, or a doctrine-touching
  design), not on every change.
- Define the merge-flow ordering so layers do not overlap:
  `apply → gate(correctness) → C(intent) → roborev(code) → merge → archive`,
  with B available as an escalation off C.
- Document the loop in the contributor doctrine (CLAUDE.md agent-team
  conventions + the website agents-developing section).

## Capabilities

### New Capabilities
- `change-audit`: automated verification that an implementation satisfies its
  OpenSpec change's specs before merge — the intent layer (C) plus the optional
  roborev escalation (B), and where they sit in the merge flow.

### Modified Capabilities
<!-- None: no existing OpenSpec capability spec changes (this is the first change). -->

## Impact

- **Process/doctrine:** CLAUDE.md "agent-team conventions" + "done" definition;
  website `agents-developing/` (a new audit page, alongside gate-contract and
  wiring-evidence).
- **Agents:** `spec-auditor` subagent re-anchored to OpenSpec specs (read-only,
  no behavior change to its audit discipline); optional reuse of the
  `roborev-design-review-branch` skill for B.
- **No cqlite-core / binding code changes.** No impact on the no-heuristics
  mandate, the public binding surfaces, or the <128MB memory budget.

## Non-goals

- Not replacing `scripts/agent-gate.sh` or sstabledump parity — C runs *after*
  the gate is green and audits intent, not correctness.
- Not auditing oracle-driven bug fixes (SSTable parsing, compaction parity,
  type decode) — those stay as GitHub issues + pinned parity tests, not
  OpenSpec changes.
- Not a CI-blocking GitHub Actions job in this change (C is invoked in the
  attended merge flow); a CI integration can follow once C is proven.
- Not building new deterministic structural validation beyond `openspec
  validate` (C is the semantic layer; `openspec validate` remains the
  structural one).
