# Large Source File Audit

**Generated:** 2026-06-28 (epic #1116 close-out, issue #1173)
**Method:** Line counts via `wc -l` over every git-tracked `*.rs` file (excluding `target/`).
A file is classified **test** when its path matches `*_test.rs`, `*_tests.rs`, `**/tests/**`,
`tests/**`, or `**/benches/**`; everything else is **production source**. "Source vs inline-test"
splits for individual files count lines before/after the file's inline `#[cfg(test)]` module(s).
**Campsite-rule thresholds** (`scripts/agent-gate.sh` `file-size` component, total lines incl. inline
tests): source `800`, test `1500`. **Epic #1116 hard goal:** no production *source* file over **2,000**
lines.

## Why this was regenerated

The previous audit (2026-06-26) predated the epic #1116 split fleet (#1117–#1134, merged via PRs
#1165 + #1159). This regeneration measures the post-split state on `main` and verifies the epic's
"Done when" close-out conditions (issue #1173).

## Summary

| Category | Files | Total lines |
|----------|-------|-------------|
| Production source (`*/src/`) | 383 | 243,993 |
| Test / integration code (`tests/`, `*_test*.rs`, `benches/`) | 439 | — |
| Total `.rs` files in repo | 822 | — |

Production source size buckets (total lines, inline tests included):

| Bucket | Count | Pre-split (2026-06-26) |
|--------|-------|------------------------|
| > 5,000 lines | **1** | 3 |
| 2,001–5,000 lines | **5** | 17 |
| 1,001–2,000 lines | 59 | 43 |
| 801–1,000 lines | 31 | — |
| 501–800 lines | 95 | 102 |
| ≤ 500 lines | 192 | — |

Production files over **2,000 total lines: 6** (was **20** pre-split). Production source > 800 lines: 96,
by area — `cqlite-core` 79, `cqlite-cli` 11, `cqlite-flight` 3, `bindings` 2, `tools` 1.

### Post-split reduction (headline)

The three pre-split giants (each > 10,000 lines) were the top refactor targets:

| File | Pre-split | Post-split | How |
|------|-----------|------------|-----|
| `…/parsing/row_decoder.rs` | 13,811 | split into `row_decoder/` submodules (largest child 2,353) | #1117 |
| `storage/write_engine/merge.rs` | 12,673 | `merge/mod.rs` 11,840 — **deferred exception #945** | not split |
| `storage/sstable/writer/data_writer.rs` | 11,900 | split into `data_writer/` submodules | #1118 |

Two of the three are gone from the > 2,000 list. `merge/mod.rs` remains the single largest file; it is
a **documented exception tracked by #945** and was intentionally out of scope for the split fleet.

---

## Close-out criteria (issue #1173)

### ✅ Criterion: no production *source* file exceeds 2,000 lines (excl. documented exceptions)

**PASS, with one caveat.** Measuring non-test source lines (excluding inline `#[cfg(test)]` modules),
**no production file carries more than ~1,750 lines of source**, *except* the two documented
exceptions. Every other file that crosses 2,000 *total* lines does so because of a large inline test
module — see the table below.

Caveat surfaced for the owner/manager: `storage/sstable/mod.rs` is listed in the issue as a
"re-export facade" exception, but it actually carries **~2,055 lines of real source** (the `SSTableId`
type, directory-discovery logic, and module re-exports) plus a 211-line inline test module — it is not
purely a facade. It is on the documented-exception list, so the criterion holds *as written*, but its
justification ("facade") is inaccurate. Recommend either decomposing it (extract `SSTableId` +
discovery into submodules) or re-stating the exception rationale. See **NEEDS-YOU** below.

### ✅ Criterion: `file-size` gate component is green on `main` without `CQLITE_ALLOW_FILE_GROWTH=1`

`scripts/agent-gate.sh --only file-size` reports `PASS` with no changed `.rs` files over threshold and
no growth flagged. (The `file-size` component is a per-change growth ratchet, not an absolute cap — it
fails only when a change grows an over-threshold file. It does not by itself enforce the 2,000-line
epic goal; this audit is the standing record that the goal is met.)

### Files still over 2,000 TOTAL lines — rationale + follow-up

| File | Total | Source | Inline test | Rationale | Follow-up |
|------|------:|-------:|------------:|-----------|-----------|
| `storage/write_engine/merge/mod.rs` | 11,840 | ~908 | ~10,932 | Compaction merge engine; deferred from the split fleet by design. Source is small; the bulk is inline tests. | **#945** (documented exception) |
| `…/row_decoder/row_framing.rs` | 2,353 | ~1,275 | ~1,078 | Split child (#1117); strict Data.db framing tests added by #990/#1171. Source < 2,000. | move inline tests to a sibling test file (epic #1116 "tests follow code" / **#1135**) |
| `storage/sstable/writer/mod.rs` | 2,288 | ~921 | ~1,367 | Split child (#1128). Source < 2,000; over the cap only via inline `#[cfg(all(test, feature="write-support"))]` tests. | move inline tests to a sibling test file (**#1135**) |
| `storage/sstable/mod.rs` | 2,276 | ~2,055 | ~211 | Listed as facade exception, but holds real source (`SSTableId`, discovery). | **documented exception** + see NEEDS-YOU (re-scope or decompose) |
| `storage/write_engine/mod.rs` | 2,070 | ~1,026 | ~1,044 | Split child (#1120). Source < 2,000; over the cap only via inline tests. | move inline tests to a sibling test file (**#1135**) |
| `query/select_executor/mod.rs` | 2,035 | ~1,736 | ~299 | Split child (#1121); three large async pipeline methods relocated intact. | **#1174** (decompose the 3 async methods) |

### Test files over 2,000 lines (tracked by #1135)

| File | Total |
|------|------:|
| `cqlite-cli/tests/parquet_writer_tests.rs` | 4,835 |
| `cqlite-core/tests/sstableloader_integration.rs` | 3,821 |

---

## Production source files over 1,500 lines (watch list)

The next refactor candidates as files are next touched (campsite rule). Files already in the
> 2,000 table above are omitted.

| Lines | File |
|------:|------|
| 1,987 | `cqlite-core/src/storage/write_engine/cql_to_mutation/builders.rs` |
| 1,984 | `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column.rs` |
| 1,976 | `cqlite-core/src/types.rs` |
| 1,928 | `cqlite-flight/src/producer.rs` |
| 1,922 | `cqlite-core/src/storage/sstable/reader/delta_scan/scan.rs` |
| 1,826 | `cqlite-core/src/schema/cql_parser.rs` |
| 1,783 | `cqlite-core/src/query/select_parser.rs` |
| 1,783 | `cqlite-cli/src/repl/engine.rs` |
| 1,749 | `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/udt.rs` |
| 1,740 | `cqlite-core/src/storage/write_engine/mutation.rs` |
| 1,691 | `cqlite-cli/src/config.rs` |
| 1,682 | `cqlite-core/src/export/arrow_convert.rs` |
| 1,658 | `bindings/node/src/database.rs` |
| 1,655 | `cqlite-core/src/storage/sstable/verify.rs` |
| 1,648 | `cqlite-core/src/schema/registry.rs` |
| 1,643 | `cqlite-core/src/storage/sstable/row_cell_state_machine.rs` |
| 1,632 | `cqlite-core/src/cql/visitor.rs` |
| 1,625 | `cqlite-core/src/export/delta_parquet.rs` |
| 1,622 | `cqlite-core/src/storage/write_engine/wal.rs` |
| 1,608 | `cqlite-core/src/cql/mutation_parser.rs` |
| 1,582 | `cqlite-core/src/storage/sstable/compression.rs` |
| 1,579 | `cqlite-core/src/storage/sstable/writer/partitions_writer.rs` |
| 1,566 | `cqlite-core/src/query/result.rs` |
| 1,541 | `cqlite-core/src/storage/write_engine/maintenance.rs` |
| 1,536 | `cqlite-core/src/storage/sstable/reader/parsing/value_parsing.rs` |

---

## Reproduce

```bash
# Per-file total line counts, production vs test split, buckets:
git ls-files '*.rs' | while read -r f; do printf '%s %s\n' "$(wc -l <"$f")" "$f"; done | sort -rn

# file-size gate component (must be PASS, no growth, no override):
scripts/agent-gate.sh --only file-size
```

## NEEDS-YOU (epic #1116 close-out decisions — not a worker call)

1. **`storage/sstable/mod.rs` exception is mischaracterized.** It is ~2,055 lines of real source
   (`SSTableId`, discovery), not a re-export facade. Decide: (a) file a source-decompose follow-up and
   keep #1116's goal strict, or (b) re-state the exception with the real rationale. Either way, the
   "facade" label should be corrected.
2. **Inline-test-heavy split children** (`row_framing.rs`, `writer/mod.rs`, `write_engine/mod.rs`) push
   over 2,000 *total* only via inline tests. Decide whether moving those inline tests to sibling test
   files belongs under #1135 (its current scope is standalone `tests/*.rs` files) or needs its own
   tracker before #1116 can close.
3. **Closing epic #1116** is the owner's/manager's call (workers never close epics). This audit
   confirms the source-line goal is met; the residuals above are the only open close-out items.
