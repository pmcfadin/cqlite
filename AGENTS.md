# CQLite development contract

CQLite reads and writes Apache Cassandra 5.0 SSTables. For feature implementation,
bug fixes, and development reviews, read `.agents/skills/cqlite-development/SKILL.md`.
Use relevant Rust, SSTable, type-system, test-data, Python, and Node skills from
`.agents/skills/`. Read their SKILL.md, not only the discovery description.

## Scope and specifications

- Design-driven features (CLI, bindings, query API, performance targets, process)
  require an OpenSpec proposal, design, testable scenarios, and tasks before
  implementation. Use the existing `openspec-propose` and `openspec-apply-change`
  skills under `.codex/skills/`. Run `openspec validate <change> --strict`.
- Obtain owner approval of the proposed design before implementation. Explicit
  approval already given in the conversation counts; do not ask again.
- Oracle-driven Cassandra parsing/type/compaction fixes use issue acceptance
  criteria and a pinned regression/parity test; a new OpenSpec proposal is not required.
- Stay within the requested scope. For issue delivery, follow the board, claim,
  worktree, and HOLD rules in `docs/development/pm-operating-loop.md`. Do not select
  unrelated work or treat a local coding request as permission for fleet operations.

## Correctness and tests

- Decode from authoritative schema/serialization metadata, never byte heuristics.
  Legacy fallbacks belong only behind `legacy-heuristics`, off by default.
- Format authority: pinned Apache Cassandra `cassandra-5.0.8` source, then
  sstabledump goldens, then `docs/sstables-definitive-guide/`. CQLite code proves
  what it does, not what the Cassandra format requires. Never read a Cassandra
  clone's arbitrary working branch as the format authority.
- Test each acceptance scenario through the relevant public surface. Name the
  surface, call chain, and test; helper-only tests do not prove a feature is wired.
- Use real Cassandra SSTables for format integration tests. Fetch via
  `bash test-data/scripts/fetch-datasets.sh` and use the export line it prints.
  Missing fixtures/skipped tests are unverified coverage, not passes. Fixtures
  must vary the property under test; CQLite round trips alone are not a Cassandra oracle.
- Respect the <128MB memory target, supported format boundaries, and cross-binding
  behavior. Consult the definitive guide and maintained domain skills for details.

## Validation and review

- Read `.agents/skills/ci-cd-validation/SKILL.md`. Iterate with targeted tests and
  the lite gate; the full `scripts/agent-gate.sh` is the pre-merge gate of record.
  Ad-hoc Cargo success or a lite/partial summary cannot replace it.
- If you change `cqlite-core`, run
  `cargo clippy -p cqlite-core --all-targets --all-features -- -D warnings`
  before pushing. A green `cargo test` run is not sufficient.
- Run read-only spec and test-quality reviews before declaring feature completion.
  Use `spec-auditor` and `coverage-reviewer` when available; otherwise spawn generic
  read-only reviewers with the instructions in
  `.agents/skills/cqlite-development/references/`. Independent review delegation
  is authorized for development work. If unavailable, disclose that limitation;
  do not claim independent sign-off from an implementer's self-review.
- Required acceptance scenarios without meaningful tests block completion. There
  is no numeric coverage-percentage gate to catch these gaps for you.
- Certification is tied to the reviewed source revision. Source changes or rebase
  invalidate prior certification; use only the documented delta path for eligible
  post-gate test/docs changes. Record run-id, SHA, tree integrity, and summary.

## Completion and delivery

Report implementation status separately from validation and merge status. Include
acceptance-to-test evidence, actual checks executed, skips/failures, and review
findings. Checkboxes alone never mean verified or archive-ready.

When PR/merge delivery is in scope, follow `docs/development/roborev-contract.md`
and `docs/development/merge-gate.md`: review first, then final rebase → full gate →
intent audit → final roborev → premerge assertion/HOLD check → auto-merge. Only
confirmed merged, completed deliveries are finalized and OpenSpec-archived; keep
partial deliveries active. Honor the documented primary-source review substitute
for code-free changes. Do not report a local setup change as merged or certified.

## Maintaining instructions

The development and gate documents are the shared process references. Domain skill
symlinks reuse maintained `.claude/skills/` content; edit the target rather than
creating divergent copies. Codex tool adapters live here and in its workflow/CI
skills. Use available Codex tools or ordinary conversation, not assumed Claude
`Task`, `Skill`, or `AskUserQuestion` APIs. Never hard-code a model for a reviewer.
