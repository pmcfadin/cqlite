## Context
The owner approved the focused parity design in this conversation with “do that for me”. Existing Codex skills contain Claude tool names and stale copies; the mandatory entrypoint lacks feature routing.

## Goals / Non-Goals
Make specification, public-surface tests, and honest validation evidence the default. Preserve the existing gate and review contracts without replicating fleet orchestration.

## Decisions
- Put mandatory routing and completion rules in AGENTS.md; a small cqlite-development skill provides procedure. Implicit skill matching alone is insufficient.
- Reuse maintained Claude domain directories through repository-relative symlinks from .agents/skills rather than maintaining divergent copies. Keep the Codex CI adapter separate because execution tools differ.
- Adapt existing .codex OpenSpec skills in place (already discovered in this environment), preserving CLI-derived planning paths. Use ordinary conversation or available input tools, never assumed Claude tools.
- Add spec-auditor and coverage-reviewer TOML roles plus shared review instructions usable by a generic read-only subagent if role discovery needs a restart.
- The gate, not a manual Cargo checklist, remains authoritative. Review current docs/development/gate-ops.md and roborev-contract.md at execution time.
- Archive only a completed, verified, merged delivery; task checkboxes alone are insufficient. Honor existing authorization and do not introduce repetitive approval prompts.

## Risks / Trade-offs
Symlinks require checkout support; verify each target resolves. Existing generated OpenSpec skills may be overwritten by upstream regeneration; retain project completion rules in AGENTS.md as well. New roles may require a fresh Codex session. Behavioral scenarios evaluate instruction following, not Rust runtime correctness.

## Migration Plan
Land the instruction changes together. Rollback by reverting the change; no application data migration. Leave OpenSpec change active until merged and finalized.

## Open Questions
None for the approved local setup scope.
