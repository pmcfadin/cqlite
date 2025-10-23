# Testing Your Claude Code Skills

## How Skills Work

Skills activate **automatically** when you ask questions that match their descriptions. You don't say "use skill X" - Claude decides based on context.

## Testing Each Skill

### Skill 1: SSTable Format Parsing

**Test queries:**
```
"I'm seeing hex bytes 0x00 0x24 0x10 in my Data.db file. What does flag 0x24 mean?"

"How do I parse a compressed SSTable chunk with LZ4?"

"The offset calculation is wrong - I'm missing 374 bytes. How do I debug this?"

"What's the BTI index format in Cassandra 5?"
```

**Expected:** Claude activates SSTable parsing skill, references format documentation

---

### Skill 2: CQL Type System

**Test queries:**
```
"How do I deserialize a map<text, frozen<list<int>>>?"

"What's the wire format for a UDT with null fields?"

"I need to parse a collection - is it length-prefixed?"

"How do frozen types differ from non-frozen?"
```

**Expected:** Claude activates CQL type system skill, shows type specifications

---

### Skill 3: Rust Performance Patterns

**Test queries:**
```
"How can I parse this buffer without copying it?"

"I'm hitting 500MB memory usage - how do I optimize?"

"Should I use &[u8] or Bytes here?"

"What's the best way to handle async file I/O with tokio?"
```

**Expected:** Claude activates Rust patterns skill, may fetch Context7 docs

---

### Skill 4: Test Data Management

**Test queries:**
```
"I need to generate test data with collections and UDTs"

"How do I export SSTables from Cassandra?"

"What's the workflow for validating against sstabledump?"

"How do I create a dataset with 10,000 rows?"
```

**Expected:** Claude activates test data skill, references your scripts

---

### Skill 5: CI/CD Validation

**Test queries:**
```
"Ready to push my changes - what validation should I run?"

"How do I fix clippy warnings before committing?"

"What's the pre-push checklist?"

"How do I merge a PR properly?"
```

**Expected:** Claude activates CI/CD skill, provides checklist

---

## Viewing Available Skills

Ask Claude:
```
"What skills are available?"
"List all Claude Code skills"
"Show me the skills for this project"
```

Claude will show all 5 skills with descriptions.

---

## How to Tell If a Skill Was Used

Claude may mention:
- "Using the SSTable parsing skill..."
- "Based on the CQL type system skill..."
- References specific files like "See cassandra5-format-reference.md"

Or it may just seamlessly use the skill without announcing it.

---

## Testing Skill Activation

### Example 1: Multi-Skill Query

**Your question:**
```
"I'm implementing a parser for map<text, int> from a compressed Data.db file.
I need to handle this with zero-copy patterns and validate against sstabledump.
What's the approach?"
```

**Skills activated:**
1. ✅ SSTable Parsing (compressed Data.db)
2. ✅ CQL Type System (map<text, int>)
3. ✅ Rust Patterns (zero-copy)
4. ✅ Test Data Management (sstabledump validation)

---

### Example 2: Single Skill Query

**Your question:**
```
"What are the pre-push validation steps?"
```

**Skills activated:**
1. ✅ CI/CD Validation (pre-push)

Claude provides checklist from validation-checklist.md

---

## When Skills Don't Activate

If Claude doesn't use a skill when you expect:

### 1. Make query more specific
❌ "Help with data"
✅ "How do I parse a CQL list<int> from SSTable bytes?"

### 2. Use trigger keywords
- SSTable, Data.db, compression
- CQL type, collection, UDT
- zero-copy, Bytes, async
- test data, sstabledump, generate
- clippy, validation, CI, merge

### 3. Refine skill description
Edit `.claude/skills/[skill-name]/SKILL.md` and update the description.

---

## Real-World Usage Examples

### Scenario: Implementing New Type Support

**You:** "I need to add support for the duration CQL type"

**Claude will:**
1. Activate CQL Type System skill
2. Show duration format (3 VInts: months, days, nanos)
3. Reference cql-types-reference.md
4. Suggest test data generation
5. May activate Test Data Management skill

### Scenario: Debugging Parser

**You:** "My parser fails at offset 405. Expected offset is 31. What's wrong?"

**Claude will:**
1. Activate SSTable Parsing skill
2. Reference row format documentation
3. Suggest hex dump analysis
4. May provide debugging techniques

### Scenario: Performance Optimization

**You:** "This parser is using too much memory"

**Claude will:**
1. Activate Rust Performance Patterns skill
2. Suggest zero-copy patterns
3. Reference zero-copy-patterns.md from your codebase
4. May fetch bytes crate docs via Context7

---

## Context7 Integration

When Claude needs latest docs for Rust crates:

**You:** "How does Bytes::slice work?"

**Claude may:**
1. Activate Rust Patterns skill
2. Check CONTEXT7_REFERENCES.md
3. Fetch latest docs: `/tokio-rs/bytes`
4. Provide up-to-date answer

**Or explicitly request:**
```
"Fetch bytes crate documentation using Context7 and show me slice patterns"
```

---

## Progressive Disclosure

Skills load content progressively:

**First:** SKILL.md (overview, quick reference)
**Then:** Supporting files (when needed)
**Finally:** Context7 docs (if requested)

This keeps responses fast and focused.

---

## Daily Workflow Integration

### Morning: Start Work
```
You: "What test data do I have available?"
→ Test Data Management skill activates
→ Shows datasets in test-data/datasets/
```

### During Development
```
You: "How do I parse this frozen UDT?"
→ CQL Type System skill activates
→ Shows UDT format from collections-and-udts.md
```

### Before Commit
```
You: "Ready to commit - what should I check?"
→ CI/CD Validation skill activates
→ Provides pre-push checklist
```

### During Review
```
You: "How do I merge this PR?"
→ CI/CD Validation skill activates
→ Shows merge-process.md workflow
```

---

## Tips for Maximum Effectiveness

### 1. Be Specific
✅ "How do I deserialize a map<text, frozen<list<int>>> with zero-copy?"
❌ "Help with collections"

### 2. Use Natural Language
✅ "I'm parsing a Data.db file and seeing weird bytes"
❌ "activate sstable skill"

### 3. Combine Concepts
✅ "Generate test data with nested collections and validate with sstabledump"
(Activates both Test Data and CQL Type skills)

### 4. Reference Files
✅ "The format in cassandra5-format-reference.md shows..."
(Claude knows you're working with SSTable formats)

### 5. Ask Follow-ups
Skills provide context for the conversation, so follow-up questions benefit too.

---

## Troubleshooting

### "Claude isn't using my skills"

**Check:**
```bash
# Verify skills exist
ls -la .claude/skills/*/SKILL.md

# Check YAML frontmatter
head -10 .claude/skills/sstable-parsing/SKILL.md
```

**Ensure:**
- Files are in `.claude/skills/` directory
- SKILL.md has proper YAML frontmatter
- Descriptions include trigger keywords

### "Wrong skill activated"

**Fix:** Make descriptions more distinct
```yaml
# Make trigger keywords unique to each skill
# Use specific domain terms
```

### "Skills not showing up"

**Restart Claude Code** if you just created them:
- Close and reopen the workspace
- Skills are loaded on startup

---

## Next Steps

1. **Try the test queries above** - See which skills activate
2. **Ask your real questions** - Skills work with actual dev work
3. **Refine descriptions** - If activation isn't right
4. **Add more skills** - For M2, M3, M4 as needed

---

## Summary

**How to use:**
- Just ask questions naturally
- Skills activate automatically
- No special syntax needed

**How to test:**
- Use queries with trigger keywords
- Ask "what skills are available?"
- Try multi-skill queries

**How to refine:**
- Edit SKILL.md descriptions
- Add more trigger keywords
- Update supporting docs

**Your skills are ready to use right now!** Just start asking questions in Claude Code. 🚀

