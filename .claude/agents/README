# CQLite Subagents

Focused subagents for CQLite development. These are automatically invoked by Claude when relevant.

## Available Agents

| Agent | Model | Purpose |
|-------|-------|---------|
| `sstable-developer` | sonnet | SSTable parsing, binary format debugging, Cassandra 5 compatibility |
| `rust-reviewer` | sonnet | Code review, quality standards enforcement |
| `test-validator` | haiku | Test execution, sstabledump validation, failure investigation |

## Usage

Agents are invoked automatically based on task context. You can also explicitly request them:

- "Use the sstable-developer agent to debug this parsing issue"
- "Have the rust-reviewer check this PR"
- "Run test-validator to check current pass rate"

## Related Skills

Skills in `.claude/skills/` provide domain knowledge that's auto-loaded:

- `sstable-parsing` - Format specifications, debugging techniques
- `cql-type-system` - CQL type deserialization
- `rust-patterns` - Rust-specific patterns for this codebase
- `ci-cd-validation` - Pre-push validation checklist
- `test-data-management` - Working with test SSTables
