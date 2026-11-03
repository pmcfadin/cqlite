# Local validation — 2026-09-19

## Scope
Instruction/skill configuration only; no production Rust changed. Owner approved
the proposed local parity setup in the conversation. No PR publication, full gate,
roborev certification, merge, or archival was performed. Delivery tasks remain open.

## Structural checks
- Skill Creator quick_validate: cqlite-development and ci-cd-validation PASS.
- YAML metadata parsed for all five adapted OpenSpec skills and both Codex workflow skills.
- TOML parsed for all six Codex agent definitions.
- Six shared domain symlinks resolve to tracked Claude skill targets; relative workflow links resolve.
- AGENTS.md is below the default 32 KiB instruction budget.
- openspec validate codex-development-parity --strict: PASS.
- openspec instructions specs now returns both scenario/test-evidence rules;
  fixed `rules.spec` to `rules.specs` and confirmed the unknown-artifact warning disappeared.
- git diff --check: PASS.

## Independent behavioral evaluation
A baseline reviewer loaded the old AGENTS, CI and OpenSpec apply instructions,
without CLAUDE.md. It identified missing mandatory feature routing, archive-ready
claims based on checkboxes, absent explicit skip/revision safeguards, and conflicting
manual coverage requirements.

A separate reviewer loaded the updated contract and skills without CLAUDE.md and
simulated these cases through the actual agent-facing instruction entrypoints:

| Scenario | Observed decision |
| --- | --- |
| New CLI feature without artifacts | Propose, strictly validate, obtain missing design approval before implementation |
| Approved feature with helper-only tests | Public-surface evidence missing; completion blocked, no repeat design approval |
| Missing fixture corpus | Fetch prescribed corpus, execute tests; skipped coverage remains unverified |
| Production source changed after PASS | Prior revision cannot certify new revision; renew full certification |
| Local implementation, no merged PR | Report local status and leave change active |
| Cassandra decode regression | Issue criteria plus pinned parity regression, no new proposal required |

The reviewer found residual generic OpenSpec template conflicts. These were fixed:
context-based change selection, archive-after-merge language, autonomous debugging,
explore-to-implementation routing, and incomplete audit requirements. It also
identified stale SSTable agent guidance; corrected authority order, corpus setup,
parser location, and test-command scope. Reviewer roles now point to maintained
read-only references instead of duplicating their bodies.

## Limits
These were instruction-following simulations and structural checks, not a real
feature build or proof that every future session complies. Newly added roles may
need a fresh session; the workflow explicitly supports generic read-only reviewers
loaded with the same references. No full-gate or final C/roborev PASS is claimed.
