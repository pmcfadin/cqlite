# Agent Skills & Subagents Audit

_Date: 2026-06-19_

> **Update 2026-06-20:** the `rust-skills` pack ([`leonardomso/rust-skills`](https://github.com/leonardomso/rust-skills)
> v1.5.1, MIT — 265 idiomatic-Rust rules across 26 categories) was installed in
> this same change, addressing the general-idiomatic-Rust gap. It lives under
> `.agents/skills/rust-skills/`, is symlinked from `.claude/skills/rust-skills`,
> pinned in `skills-lock.json`, and wired into the `CLAUDE.md` skills table.
> It is the general layer beneath the cqlite-specific `rust-patterns` skill.

This document audits the **project-local** agent tooling under `.claude/skills/`
(each a directory with a `SKILL.md`) and `.claude/agents/` (each a `*.md` file with
frontmatter). It does not cover global/user-level skills installed outside the repo.
For each item it records whether the definition is in place, still accurate against
the current codebase (M5.2 in progress, June 2026 — compaction byte-fidelity +
`cqlite compact`, BTI read-path, WRITETIME/TTL, query-engine completeness, the
`cqlite-flight` Arrow Flight crate, delta-scan planned), and whether it is wired
into `CLAUDE.md`. It also recommends new skills for high-complexity areas that have
no covering skill today.

## Main table

| Skill/Agent | Type | Status | Up to date? | Used? (wired in CLAUDE.md) | Details/Notes |
|---|---|---|---|---|---|
| `sstable-parsing` | skill | ✅ In place | ⚠️ Mostly | ✅ Yes | Paths (`reader/parsing/v5_compressed_legacy.rs`, guide chapters, appendix B/F) all exist. Says BTI is "what doesn't work yet"-adjacent; BTI read-path now landed (extract Data.db offsets from trie payloads, #755/#833). No mention of the `bti/` module that now exists. |
| `cql-type-system` | skill | ✅ In place | ✅ Yes | ✅ Yes | Type catalog, schema-provided deserialization, collection/UDT/tuple formats all current. Aligns with no-heuristics mandate. Generic and stable. |
| `rust-patterns` | skill | ✅ In place | ✅ Yes | ✅ Yes | Zero-copy/async/memory guidance is evergreen. Only stale-ish bit: references `v5_compressed_legacy.rs (1997 lines)` line count, cosmetic. |
| `ci-cd-validation` | skill | ✅ In place (refreshed 2026-06-19) | ✅ Yes | ✅ Yes | **Fixed 2026-06-19:** Quick Validation now leads with the canonical `scripts/agent-gate.sh` gate; the dead `.github/workflows/rust.yml` reference replaced with a table of the real ~20 split workflows (`ci.yml`, `ci-minimal-features.yml`, `quality-gates.yml`, `sstabledump-parity-gate.yml`, `flight-ci.yml`, …); Rust pin updated 1.70 → 1.85+/stable. (Scattered `90%`/`95%` coverage prose left as-is; coverage is now informational, noted in the workflow table.) |
| `test-data-management` | skill | ✅ In place | ✅ Yes | ✅ Yes | `regenerate-datasets.sh`, `start-clean.sh`, `export.sh`, `shutdown-clean.sh` workflow matches scripts on disk. Recently edited (Jun 17). Accurate. |
| `pyo3-maturin-bindings` | skill | ✅ In place | ✅ Yes | ❌ No (not in CLAUDE.md table) | Accurate PyO3 0.22 / `Bound<'_,...>` API, maturin flow, parity references. Real and useful (`bindings/python/` matches), but absent from the CLAUDE.md skills table. Note duplicate `pyo3-maturin-bindings.skill` flat file alongside the dir (see below). |
| `napi-rs-node-bindings` | skill | ✅ In place | ✅ Yes | ❌ No (not in CLAUDE.md table) | Accurate napi-rs 2 patterns, parity references; matches `bindings/node/`. Absent from CLAUDE.md skills table. Duplicate `napi-rs-node-bindings.skill` flat file alongside the dir. |
| `sstable-developer` | subagent | ✅ In place | ✅ Yes | ✅ Yes | Paths (`storage/sstable/`, `v5_compressed_legacy.rs`, `row_cell_state_machine.rs`, guide chapters) all valid. Standards current (no-heuristics, <128MB, sstabledump parity). |
| `rust-reviewer` | subagent | ✅ In place | ✅ Yes | ✅ Yes | Review checklist matches project standards (thiserror, no unwrap, zero clippy warnings, real-data tests). Evergreen. |
| `test-validator` | subagent | ⚠️ Needs update | ❌ No | ✅ Yes | Stale pass rate: claims "27.3% as of last check"; actual is 100% (33/33) per CLAUDE.md. Otherwise structure (4 keyspaces, JSONL goldens, smoke script) is correct. |
| `compaction-parity-auditor` | subagent | ✅ In place (fixed 2026-06-19) | ✅ Yes | ❌ No (not in CLAUDE.md table) | CQLite paths it audits all exist (`write_engine/merge.rs`, `sstable/writer/`, `compaction_integration.rs`, `docs/compaction/byte-parity-rules.md`). **Fixed 2026-06-19:** hardcoded `/Users/jhaddad/dev/cassandra` replaced with a portable resolution order (`$CQLITE_CASSANDRA_REPO` → `~/local_projects/cassandra`) + fetch-on-demand of the `rustyrazorblade/cassandra cursor-compaction-completion` branch; the in-repo `docs/compaction/byte-parity-rules.md` is now the primary checklist so an audit runs even with no Cassandra checkout. Still not listed in the CLAUDE.md subagents table. |
| **`parquet-export`** | skill | ❌ Missing | — | — | Recommended. `parquet` feature + `cqlite-core/src/export/parquet.rs` and bindings `export_parquet`/`exportParquet` exist; no skill covers Arrow schema mapping, row-group sizing, compression codecs. |
| **`write-engine-compaction`** | skill | ❌ Missing | — | — | Recommended. Large, active surface (`write_engine/`, `sstable/writer/`, k-way merge, `cqlite compact` #842). Only the read-only `compaction-parity-auditor` agent touches it; no how-to skill for implementers. |
| **`arrow-flight`** | skill | ❌ Missing | — | — | Recommended. A full `cqlite-flight` crate (producer/service/ticket/filter) plus `flight-ci.yml` and Trino e2e already exist — far beyond the design doc — with zero skill/agent coverage. |
| **`bti-trie-format`** | skill | ❌ Missing | — | — | Recommended (or fold into `sstable-parsing`). BTI trie read-path is now real (`storage/sstable/bti/`, payload-offset extraction) and intricate; current `sstable-parsing` only mentions BTI in passing. |
| **`query-engine`** | skill | ❌ Missing | — | — | Recommended. `cqlite-core/src/query/` (select parser/AST/planner/executor, prepared statements, WRITETIME/TTL validator) is substantial and recently completed (epic #756/#689) with no covering skill. |

Legend: ✅ In place / ⚠️ Needs update / ❌ Missing.

## Stale references found

> **Resolved 2026-06-19:** the two `ci-cd-validation` workflow/gate items and the
> `compaction-parity-auditor` Cassandra-path item below were fixed in this same change.
> Remaining open items: `test-validator` pass rate, `ci-cd-validation` scattered coverage
> prose, and `sstable-parsing` BTI status.

- **✅ FIXED — `ci-cd-validation` → CI workflow file.** Was: _"Located: `.github/workflows/rust.yml`"_. There is no `rust.yml`; replaced with a table of the real ~20 split workflows (`ci.yml`, `ci-minimal-features.yml`, `quality-gates.yml`, `sstabledump-parity-gate.yml`, `python-ci.yml`, `node-ci.yml`, `flight-ci.yml`, …).
- **✅ FIXED — `ci-cd-validation` → canonical gate omitted.** "Quick Validation" now leads with `scripts/agent-gate.sh` (THE pre-PR gate per CLAUDE.md, emits the machine-checkable summary block); `validate-cleanup.sh` demoted to cleanup-specific.
- **⚠️ Partially addressed — `ci-cd-validation` → coverage/version targets.** Rust pin updated `1.70.0` → 1.85+/stable, and the workflow table notes coverage is informational (not a hard 90% gate). Scattered `90%`/`95%` prose elsewhere in the skill left as-is (larger rewrite, low risk).
- **`test-validator` → pass rate.** Says: _"Current pass rate (27.3% as of last check)"_. **Fix:** current pass rate is **100% (33/33 tables)** as of Dec 2025 (CLAUDE.md). The `appendix-f` "known failing tables" framing is also stale (no XFails as of Dec 2025).
- **✅ FIXED — `compaction-parity-auditor` → Cassandra source path.** Was: _"Repo: `/Users/jhaddad/dev/cassandra`"_ (absent here). Replaced with portable resolution (`$CQLITE_CASSANDRA_REPO` → `~/local_projects/cassandra`) + fetch-on-demand of the `rustyrazorblade/cassandra cursor-compaction-completion` branch, and made `docs/compaction/byte-parity-rules.md` the primary checklist so the audit runs with no checkout present.
- **`sstable-parsing` → BTI status.** Implies BTI is forthcoming. **Fix:** BTI read-path now extracts Data.db offsets from trie payloads for O(log n) seeks (#755/#833) and a `storage/sstable/bti/` module exists; update wording from "understanding" to "implemented read-path".
- **`rust-patterns` / `sstable-parsing` → line counts.** Both cite `v5_compressed_legacy.rs (1997 lines)`. Cosmetic; drop the count or stop asserting an exact number.

## Usage / wiring mismatches

- **Bindings skills not wired.** `pyo3-maturin-bindings` and `napi-rs-node-bindings` are present and accurate but missing from the CLAUDE.md "Available Skills" table — they will not be discoverable from the project doctrine even though `bindings/python/` and `bindings/node/` are first-class.
- **`compaction-parity-auditor` not wired.** Present (newest agent) but missing from the CLAUDE.md "Available Subagents" table.
- **Duplicate flat `.skill` files.** `.claude/skills/pyo3-maturin-bindings.skill` and `.claude/skills/napi-rs-node-bindings.skill` sit alongside the canonical `…/SKILL.md` directories. These flat copies are redundant and should be removed to avoid drift.
- **AppleDouble junk.** Numerous `._*` resource-fork files (e.g. `.claude/skills/._README.md`, `.claude/agents/._rust-reviewer.md`) are checked into the skills/agents trees and should be cleaned/`.gitignore`d.

## Recommended new skills

1. **`parquet-export`** — Columnar export is a shipped feature (`parquet` flag, `cqlite-core/src/export/parquet.rs`, plus `export_parquet`/`exportParquet` in both bindings) with real complexity: CQL→Arrow type mapping, row-group sizing, compression codec selection (snappy/zstd/etc.), and large-result streaming. A skill would cover the Arrow schema bridge and the bindings parity surface.
2. **`write-engine-compaction`** — The write/compaction path (`storage/write_engine/`, `storage/sstable/writer/`, streaming k-way merge, the new `cqlite compact` one-shot command for the parity harness #842) is large and the project's most active area, yet only the read-only auditor agent covers it. An implementer-facing skill (writer invariants, merge/reconciliation rules, byte-parity pitfalls, how to use `cqlite compact --gc-before`) would pair naturally with `compaction-parity-auditor`.
3. **`arrow-flight`** — `cqlite-flight` is a full crate (producer, service, ticket, filter, Dockerfile, Trino e2e CI) with no skill or agent. A skill covering the Flight producer model, ticket/filter encoding, and the Trino connector contract would cover a substantial, otherwise-undocumented subsystem.
4. **`bti-trie-format`** — BTI trie indexing now has a real read-path and a dedicated `bti/` module; the format (trie nodes, payload-encoded Data.db offsets) is intricate enough to warrant focused coverage. Could be a standalone skill or a substantial expansion of `sstable-parsing`.
5. **`query-engine`** — `cqlite-core/src/query/` (SELECT parser/AST/planner/executor, prepared statements, WRITETIME/TTL validation) was just completed (epics #756/#689) and has no covering skill, despite being the layer most bindings and CLI users exercise.
