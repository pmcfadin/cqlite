# M2 CLI Architecture Review – Quick Reference

**Status**: ✅ **APPROVED WITH RECOMMENDATIONS**  
**Date**: October 7, 2025

---

## TL;DR

**The M2 CLI architecture is sound and ready for implementation.** Address 3 critical documentation items by EOW Friday (Oct 11), then proceed with Phase 1.

---

## Answers to Specific Questions

### Q1: Concerns with deferring `state_machine` path to post-M2?

**Answer**: ✅ **No concerns. This is the right approach.**

- Base `QueryEngine` path is mature and tested
- M2 acceptance criteria fully achievable without advanced optimization
- Exit code 5 with clear messaging provides good user feedback
- Reduces risk and scope appropriately

### Q2: Are schema precedence and two-pass load rules sufficient?

**Answer**: ✅ **Yes, the rules are sufficient and well-designed.**

Precedence: `flags > env > file > defaults`  
Multi-source: last-wins per `keyspace.table`  
Two-pass: types first, then tables  
Error handling: exit 3 with actionable hints

**Recommendation**: Document behavior for circular UDT references (reject with clear error).

### Q3: Is the discovery algorithm and coverage badge policy acceptable?

**Answer**: ✅ **Yes, both are appropriate.**

**Discovery**: Scan data-dir → skip system → validate SSTable presence → output summary  
**Coverage badges**: Green ≥95%, Yellow 50-95%, Red <50% or critical errors

**Recommendation**: Add `--strict-coverage` flag for CI (requires 100%).

### Q4: Preferred source of version hints for `:status`?

**Answer**: ⚠️ **Needs finalization. Recommended precedence:**

1. **User flag** (`--cassandra-version`) if explicitly set
2. **SSTable metadata** (investigate if core exposes this)
3. **Dataset metadata.yml** (present in test-data)
4. **Fallback**: Display "unknown" (don't guess)

**Action Required**: Document this precedence in `ARCH_PLAN_M2_CLI.md` §2.3 by EOW.

### Q5: Do output formatting rules meet cqlsh parity expectations?

**Answer**: ✅ **Yes, with clarifications needed for timestamps.**

**Table**: CqlshTableFormatter exists and is production-ready  
**JSON**: Deterministic column order ✅  
**CSV**: Standard format with cqlsh conventions ✅  
**Values**: UUID (lowercase), collections (cqlsh-style), blobs (0x-hex) ✅

**Open**: Timestamp timezone handling  
**Action Required**: Research cqlsh timestamp format, default to UTC for M2, document in `VALUE_FORMATTING_SPEC.md`.

### Q6: Are error semantics/exit codes appropriate for scripting and CI?

**Answer**: ✅ **Yes, the five-code scheme is well-designed.**

```
0: Success
2: Invalid arguments (user error, don't retry)
3: Schema errors (fix schema, retry)
4: Data-dir errors (fix paths, retry)  
5: Query errors (fix query, retry)
```

**Recommendation**: Document exit codes in `--help` output and examples.

### Q7: Changes to config precedence or `:config save` behavior?

**Answer**: ✅ **No changes needed. Design is correct.**

Precedence: `flags > env > config file > defaults` (industry standard)  
`:config save`: Writes effective config in TOML format  
`:config save [FILE]`: Optional output path

**Nice-to-have**: `:config save --minimal` flag (non-defaults only).

---

## Critical Actions (Required Before Implementation)

| # | Action | Owner | Due Date | Status |
|---|--------|-------|----------|--------|
| 1 | Document canonical schema JSON format in `SCHEMA_JSON_FORMAT.md` | Lead/Architect | Oct 11 (Fri) | ⬜ TODO |
| 2 | Finalize version detection precedence in arch plan | Lead/Architect | Oct 11 (Fri) | ⬜ TODO |
| 3 | Define `QueryResult` interface contract for output writers | Core Engineer | Oct 11 (Fri) | ⬜ TODO |

---

## High Priority for M2 (Implement During Sprint)

| # | Action | Priority | Owner |
|---|--------|----------|-------|
| 4 | Add `:schema validate` command | HIGH | CLI Engineer |
| 5 | Implement atomic schema loading | HIGH | CLI Engineer |
| 6 | Add unsupported query error tests | HIGH | SDET |
| 7 | Document timestamp formatting rules | HIGH | Lead/Architect |
| 8 | Add `:status --json` flag | HIGH | CLI Engineer |

---

## Code Observations

### ✅ Strong Foundations (Ready to Use)

- `cqlite-core::query::QueryEngine` – Mature execution engine
- `cqlite-core::schema::SchemaManager` – Robust schema management
- `cqlite-cli::formatter::CqlshTableFormatter` – Production-ready
- `cqlite-cli::repl::CommandParser` – Clean command routing

### ⚠️ Consolidation Needed

- **Discovery logic**: Appears in 3 places (core storage, CLI session, CLI interactive)  
  → **Action**: Consolidate into `cqlite-cli/src/services/discovery.rs` (as planned)

- **Schema loading**: Multiple paths in core  
  → **Action**: Add unified façade in `cqlite-cli/src/services/schema_loader.rs`

---

## Testing Must-Haves

✅ Add these specific test scenarios:

1. **Multi-file schema loading**: Test last-wins precedence with 3+ overlapping files
2. **UDT dependency resolution**: Type defined after table (should fail gracefully)
3. **Discovery edge cases**: Empty keyspaces, no-schema tables, hidden directories
4. **Unsupported queries**: Validate error messages for each unsupported SELECT form
5. **Config precedence**: Flag overriding env overriding file
6. **Coverage badges**: Synthetic scenarios for Green/Yellow/Red thresholds

---

## Approval Checklist

- ✅ Integration surface validated (CLI ↔ core)
- ✅ SELECT subset appropriate and defensible  
- ✅ Schema ingestion strategy sound (two-pass, precedence)
- ✅ Discovery algorithm practical  
- ✅ Coverage badge thresholds reasonable
- ⚠️ Version detection needs finalization (Action #2)
- ✅ REPL architecture well-designed
- ⚠️ Output formatting needs timestamp clarification (Action #7)
- ✅ Error semantics and exit codes appropriate
- ✅ Testing strategy comprehensive
- ⚠️ Schema JSON format needs documentation (Action #1)
- ⚠️ QueryResult contract needs specification (Action #3)

---

## Timeline

| Milestone | Date | Status |
|-----------|------|--------|
| Critical actions complete | Oct 11 (Fri) | ⬜ Pending |
| Arch plan updated | Oct 14 (Mon) | ⬜ Pending |
| Phase 1 kickoff | Oct 15 (Tue) | ⬜ Pending |
| Phase 1 complete | Oct 22 (estimated) | ⬜ Pending |
| M2 delivery | Nov 5 (estimated) | ⬜ Pending |

---

## Next Steps

1. **Product team** addresses critical actions 1-3 by EOW
2. **Lead/Architect** updates `ARCH_PLAN_M2_CLI.md` with version precedence and timestamp rules
3. **Core Engineer** documents `QueryResult` interface
4. **Engineering team** reviews updated plan and begins Phase 1
5. **All teams** attend M2 kickoff meeting (Oct 15)

---

## Contact

- **Questions on architecture**: Lead/Architect
- **Questions on implementation**: CLI Engineer + Core Engineer
- **Questions on testing**: SDET
- **Questions on scope/priority**: Product Manager

---

## References

- **Full review**: `docs/development/ARCH_REVIEW_M2_CLI.md`
- **Architecture plan**: `docs/development/ARCH_PLAN_M2_CLI.md`
- **Spec**: `docs/development/M2_CLI_SPEC.md`
- **Epic**: `docs/development/EPIC_M2_CLI.md`
- **Examples**: `cqlite-cli/CLI_USAGE_EXAMPLES.md`

---

**Document Version**: 1.0  
**Last Updated**: October 7, 2025
