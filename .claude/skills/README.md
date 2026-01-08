# CQLite Claude Code Skills

This directory contains 5 specialized skills to accelerate M1-M6 development of the cqlite project.

## Skills Overview

### 1. SSTable Format Parsing
**Location:** `sstable-parsing/`

**Purpose:** Guide parsing of Cassandra 5.0+ SSTable components (Data.db, Index.db, Statistics.db, Summary.db, TOC) with compression support.

**Trigger keywords:** SSTable, Data.db, Index.db, binary format, hex dump, compression, BTI index, partition layout, parsing error

**Supports:** M1 (Core Reading Library)

**Files:**
- `SKILL.md` - Main skill guidance
- `cassandra5-format-reference.md` - Complete row format specification
- `compression-formats.md` - LZ4, Snappy, Deflate formats

---

### 2. CQL Type System
**Location:** `cql-type-system/`

**Purpose:** Implement and deserialize all CQL types including primitives, collections, tuples, UDTs, and frozen types.

**Trigger keywords:** CQL type, collection, list, set, map, UDT, user defined type, frozen, tuple, deserialize

**Supports:** M1 (All CQL types), M5 (Write support)

**Files:**
- `SKILL.md` - Type system guidance
- `cql-types-reference.md` - Complete catalog of all CQL types
- `collections-and-udts.md` - Collections and UDT formats

---

### 3. Rust Performance Patterns
**Location:** `rust-patterns/`

**Purpose:** Zero-copy deserialization, async I/O, lifetime management, and memory-efficient parsing.

**Trigger keywords:** zero-copy, async, lifetime, borrow checker, performance, memory, unsafe, Bytes, tokio

**Supports:** M1 (Zero-copy), M6 (Performance targets)

**Files:**
- `SKILL.md` - Performance patterns guidance
- `zero-copy-patterns.md` - Patterns from existing codebase
- `CONTEXT7_REFERENCES.md` - Links to bytes/tokio/serde via Context7

**Context7 Integration:**
- `/tokio-rs/bytes` - Buffer management
- `/tokio-rs/tokio` - Async runtime
- `/serde-rs/serde` - Serialization

---

### 4. Test Data Management
**Location:** `test-data-management/`

**Purpose:** Generate real Cassandra 5.0 test data, export SSTables, and validate parsing against sstabledump.

**Trigger keywords:** test data, generate data, SSTable export, validation, sstabledump, dataset, fixture

**Supports:** M1 (95% test coverage), All milestones (regression testing)

**Files:**
- `SKILL.md` - Test data workflow
- `dataset-generation.md` - Complete generation workflow
- `validation-workflow.md` - sstabledump comparison

**Scripts Used:**
- `test-data/scripts/start-clean.sh`
- `test-data/scripts/generate.sh`
- `test-data/scripts/export.sh`
- `test-data/scripts/shutdown-clean.sh`

---

### 5. CI/CD Validation
**Location:** `ci-cd-validation/`

**Purpose:** Pre-push validation, CI monitoring, merge process, and release quality gates.

**Trigger keywords:** CI, validation, clippy, merge, push, coverage, feature flags, pre-commit

**Supports:** All milestones (quality gates), M6 (Release readiness)

**Files:**
- `SKILL.md` - CI/CD workflow
- `validation-checklist.md` - Complete pre-push checklist
- `merge-process.md` - PR merge workflow

**Validation Commands:**
```bash
cargo fmt --all
cargo clippy --package cqlite-core --lib --all-features -- -D warnings
cargo test --package cqlite-core --lib --all-features
./scripts/ci/validate-cleanup.sh
```

---

## How Skills Work

### Automatic Activation
Claude autonomously decides when to use skills based on:
- Your question content
- Matching trigger keywords
- Description relevance

**Example:**
```
You: "I'm seeing weird bytes at offset 0x2A4F in this Data.db file."
Claude: [Activates SSTable Parsing skill]
        [References cassandra5-format-reference.md]
        [Provides hex analysis guidance]
```

### No Explicit Invocation Needed
You don't need to say "use the SSTable skill". Just ask questions naturally:
- "How do I parse a frozen list?"
- "Need to generate test data with UDTs"
- "Ready to push, what validation should I run?"

### Progressive Disclosure
Skills reference supporting files only when needed:
- Main SKILL.md provides overview
- Supporting docs loaded on demand
- Context7 fetched for latest crate docs

---

## Testing Skills

### Scenario-Based Testing

Test each skill with realistic queries:

**SSTable Parsing:**
```
"The hex shows 0x00 0x10 0x45 0x67 at this offset. What could this be?"
```

**CQL Types:**
```
"How do I deserialize a map<text, frozen<list<int>>>?"
```

**Rust Patterns:**
```
"How can I parse this without copying the entire buffer?"
```

**Test Data:**
```
"I need to generate test data with collections and UDTs"
```

**CI/CD:**
```
"Ready to push my changes. What should I run first?"
```

---

## Skill Coverage by Milestone

| Milestone | Skills Used |
|-----------|-------------|
| **M1** (Core Reading) | 1, 2, 3, 4, 5 |
| **M2** (CLI) | 3, 4, 5 |
| **M3** (Output Writers) | 3, 5 |
| **M4** (Bindings) | 3, 5 |
| **M5** (Write Support) | 1, 2, 3, 4, 5 |
| **M6** (Performance) | 3, 5 |

---

## Updating Skills

### Adding New Information

Edit the relevant SKILL.md or supporting file:
```bash
# Example: Add new CQL type
code .claude/skills/cql-type-system/cql-types-reference.md
```

### Updating Context7 References

As Rust crates update, Context7 automatically provides latest docs. No maintenance needed for:
- bytes crate
- tokio runtime
- serde framework

### Versioning

Skills are versioned with the project. When making significant changes:
1. Update skill content
2. Test with example queries
3. Commit to git (team shares skills)
4. Document changes in commit message

---

## Sharing with Team

Skills are stored in `.claude/skills/` and committed to git:

```bash
# Add skills to git
git add .claude/skills/
git commit -m "feat: add Claude Code skills for M1-M6"
git push origin main
```

Team members automatically get skills when they pull:
```bash
git pull origin main
# Skills immediately available in their Claude Code
```

---

## Documentation Strategy

### Embedded Documentation
**What's embedded:**
- Cassandra 5 SSTable format (stable)
- CQL type specifications (stable)
- Project-specific patterns
- Test workflows

**Why:** These don't change frequently and work offline.

### Context7 Documentation
**What's fetched:**
- Rust crate docs (bytes, tokio, serde)
- Always latest API
- No maintenance burden

**Usage:**
```
Ask Claude: "Fetch bytes crate documentation using Context7"
Claude will use: /tokio-rs/bytes
```

---

## Performance Impact

Skills are designed for minimal overhead:
- Main SKILL.md is ~2-5KB (fast to load)
- Supporting docs loaded only when referenced
- Context7 fetched only when explicitly needed
- Progressive disclosure keeps context manageable

---

## PRD Alignment

### M1: Core Reading Library
- ✅ All skills support M1 development
- ✅ 95% test coverage (Skill 4)
- ✅ All CQL types (Skill 2)
- ✅ Zero-copy patterns (Skill 3)

### M6: Performance & Release
- ✅ <128MB memory target (Skill 3)
- ✅ Performance profiling guidance (Skill 3)
- ✅ Release process (Skill 5)
- ✅ Benchmark validation (Skill 5)

---

## Troubleshooting

### "Claude didn't use my skill"

**Check description specificity:**
```yaml
# Too vague
description: "Helps with files"

# Specific
description: "Parse Cassandra 5.0 SSTable Data.db files. Use when working with SSTable parsing, hex dumps, or compression."
```

**Include trigger keywords:**
Add terms users would naturally mention.

### "Skill has errors"

**Validate YAML frontmatter:**
```bash
# Check for syntax errors
cat SKILL.md | head -10
```

**Ensure proper format:**
```yaml
---
name: Skill Name
description: What it does and when to use it
---

# Skill Name
...
```

### "Multiple skills conflict"

**Make descriptions distinct:**
Each skill should have unique trigger terms and clear scope.

---

## Next Steps

### For M2 (CLI Development)
Consider adding:
- CLI one-shot mode skill
- REPL interaction skill

### For M3 (Output Writers)
Consider adding:
- JSON/CSV/Parquet serialization skill
- Output format validation skill

### For M4 (Language Bindings)
Consider adding:
- PyO3 bindings skill
- WASM patterns skill
- FFI safety skill

---

## Resources

- **Claude Code Skills Docs:** https://docs.claude.com/en/docs/claude-code/skills
- **Project PRD:** `docs/development/PRD.md`
- **Cleanup Agent:** `.claude/agents/cleanup/cleanup-agent-prompt.md`
- **Test Data Scripts:** `test-data/scripts/`

---

## Summary

**5 skills created:**
1. ✅ SSTable Format Parsing (M1 core)
2. ✅ CQL Type System (M1 types)
3. ✅ Rust Performance Patterns (M1+M6)
4. ✅ Test Data Management (All milestones)
5. ✅ CI/CD Validation (All milestones)

**Total files:** 15 (5 SKILL.md + 10 supporting docs)

**Coverage:** M1-M6 development workflows

**Ready to use:** Skills automatically activate based on your questions

**Team-shared:** Commit to git for entire team

---

**Questions or issues?** Update this README or the specific skill documentation.

