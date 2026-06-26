# Large Source File Audit

**Generated:** 2026-06-26
**Method:** A fleet of Haiku subagents read and analyzed the largest files; line counts via `wc -l` over all `*.rs` files (excluding `target/`).
**Threshold:** Any file over **500 lines** is flagged.

## Summary

| Category | Files > 500 lines | Notes |
|----------|-------------------|-------|
| **Production source** (`*/src/`, excl. test crates) | **165** | 223,110 total lines |
| **Test / integration code** (`tests/`, `*_test*.rs`) | **168** | inline + integration suites |
| **Total `.rs` files in repo** | 717 | 333 exceed 500 lines (46%) |

Production source size buckets:

| Bucket | Count |
|--------|-------|
| > 5,000 lines | 3 |
| 2,001–5,000 lines | 17 |
| 1,001–2,000 lines | 43 |
| 501–1,000 lines | 102 |

Production files > 500 lines by area:

| Area | Files |
|------|-------|
| `cqlite-core` | 121 |
| `cqlite-cli` | 29 |
| `tools` | 6 |
| `cqlite-flight` | 5 |
| `bindings` | 4 |

### Headline concerns

Three files dominate and should be considered the top refactor candidates:

1. **`cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs` — 13,811 lines**
2. **`cqlite-core/src/storage/write_engine/merge.rs` — 12,673 lines**
3. **`cqlite-core/src/storage/sstable/writer/data_writer.rs` — 11,900 lines**

A recurring theme across the worst offenders: very large inline `#[cfg(test)]` blocks
(`merge.rs` ~66% tests, `data_writer.rs` ~59%, `cql_to_mutation.rs` ~43%). Moving these
to sibling `tests/` files would dramatically shrink the production files without losing
coverage, and is the lowest-risk first step.

---

## Detailed analysis of the top offenders

The following 28 production files (the largest by line count) were each read in full by a
Haiku subagent. Severity reflects how strongly the file should be split:
**HIGH** = clearly hurts maintainability; **MEDIUM** = large but cohesive, split when next touched;
**LOW** = large but acceptable (facade / well-tested / inherently complex).

### `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs` — 13,811 lines
- **Purpose:** Core V5CompressedLegacy parser for Cassandra 5.0 SSTable decompressed blocks — partition keys, row headers, clustering prefixes, complex cells (collections/UDTs), cell metadata (timestamps/TTLs), compaction-mode row reconstruction.
- **Why it's large:** Two giant functions — `parse_cell_value_schema_order` (~1,160 lines) and `parse_raw_type_value` (~1,137 lines) — plus 4–5 near-duplicate emit loops (delta-scan, windowed, timestamps, compaction). Test module is ~3,246 lines.
- **Test code share:** ~23.5% (`#[cfg(test)]` from ~line 10,566, 150+ tests).
- **Refactor recommendation:** Extract `cell_parsing.rs` and `value_parsing.rs` submodules; consider unifying the duplicated emit loops. Splitting the two mega-functions alone removes ~2K lines.
- **Severity:** MEDIUM

### `cqlite-core/src/storage/write_engine/merge.rs` — 12,673 lines
- **Purpose:** K-way merge engine for STCS compaction — last-write-wins tombstone reconciliation, GC, mutation conversion.
- **Why it's large:** ~66% is inline tests (131 tests, ~8,412 lines from ~line 4,261). Production code is a 2,076-line `KWayMerger` impl plus large streaming adapters.
- **Test code share:** ~66%.
- **Refactor recommendation:** Move the 131 tests to `tests/merge_tests.rs`; split production into `streaming`, `reconcile`, `mutation_conversion`, `gc_policy` submodules. Drops non-test code to ~1,200 lines.
- **Severity:** MEDIUM

### `cqlite-core/src/storage/sstable/writer/data_writer.rs` — 11,900 lines
- **Purpose:** Writes the Data.db component with V5CompressedLegacy row encoding (flags, delta timestamps/TTL, clustering, cell paths, complex columns, tombstones, static rows).
- **Why it's large:** A 3,612-line `DataWriter` impl (55+ methods) + ~1,100 lines of helpers + a ~7,021-line test module.
- **Test code share:** ~59% (`#[cfg(test)]` from ~line 4,880).
- **Refactor recommendation:** Split into `data_writer_core.rs`, `data_writer_cells.rs`, `data_writer_helpers.rs`; move ~3,500 test lines to `tests/`. Coordinate carefully — touches fragile compaction invariants (#857, #716).
- **Severity:** MEDIUM

### `cqlite-core/src/storage/sstable/bti/parser.rs` — 4,910 lines
- **Purpose:** BTI (Big Trie Index) binary parser — all 16 node-type ordinals, partition/row lookups, DFS traversal.
- **Why it's large:** Comprehensive coverage of all trie node variants plus traversal primitives, densely combined with lookup APIs. ~45% tests.
- **Test code share:** ~45% (`#[cfg(test)]` from ~line 2,683).
- **Refactor recommendation:** Split into `node_parser.rs`, `traversal.rs`, `encoding.rs`; keep `PartitionsParser`/`RowsParser` as top-level API wrappers.
- **Severity:** MEDIUM

### `cqlite-core/src/storage/write_engine/mod.rs` — 4,395 lines
- **Purpose:** Master write orchestrator — memtable buffering, WAL durability, multi-generation flush, STCS compaction.
- **Why it's large:** A single ~1,750-line impl block (30+ methods) covering init, write/execute, thresholds, merge state, compaction loop, orphan cleanup, stats. No inline tests.
- **Test code share:** 0% (tests are in sibling `tests/`).
- **Refactor recommendation:** Extract `active_merge.rs`, `maintenance.rs`, `stats.rs`; reduce core impl to ~600 lines via delegation.
- **Severity:** HIGH

### `cqlite-core/src/query/select_executor.rs` — 4,060 lines
- **Purpose:** SELECT execution engine — scanning, filtering, aggregation, projection, WRITETIME/TTL extraction.
- **Why it's large:** 26 module-level functions + a 30-method `impl SelectExecutor` block (~1,600 lines).
- **Test code share:** ~26% (`#[cfg(test)]` from ~line 2,831, 50+ tests).
- **Refactor recommendation:** Extract `value_ops.rs`, `predicate.rs`, `partition_lookup.rs`, `aggregation.rs`.
- **Severity:** MEDIUM

### `cqlite-core/src/storage/sstable/reader/data_access.rs` — 4,025 lines
- **Purpose:** Data access layer for SSTableReader — point lookups, range scans, BTI trie lookup, sequential scans, compaction/streaming exports.
- **Why it's large:** ~42 methods implementing three distinct read paths (BTI point-lookup, sequential scan, compaction export) plus caching/metadata in one module.
- **Test code share:** ~22% (`#[cfg(test)]` from ~line 3,158).
- **Refactor recommendation:** Split by access pattern into `bti_lookup.rs`, `sequential_scan.rs`, `compaction.rs`.
- **Severity:** HIGH

### `cqlite-core/src/storage/write_engine/cql_to_mutation.rs` — 3,594 lines
- **Purpose:** Converts parsed CQL INSERT/UPDATE/DELETE/BATCH into internal `Mutation` structs with full type coercion (scalars, collections, tuples, UDTs, JSON).
- **Why it's large:** 101 inline tests (~43%) plus per-statement builders (~230 lines each) and per-type literal codecs. Whole file is `#[cfg(feature = "write-support")]`.
- **Test code share:** ~43% (from ~line 1,547).
- **Refactor recommendation:** Split into `mutation_builders.rs`, `literal_codec.rs`, `delta_mutation_helpers.rs`, tests alongside each.
- **Severity:** MEDIUM

### `cqlite-core/src/storage/sstable/reader/delta_scan.rs` — 3,108 lines
- **Purpose:** Streaming CDC-style delta-record API (Issue #698) — emits discriminated `DeltaRecord` variants with per-cell writetimes/TTL/tombstone flags.
- **Why it's large:** ~500 lines of type/builder definitions, a ~330-line async streaming driver, ~35% tests. Well-documented and well-tested.
- **Test code share:** ~35% (`#[cfg(test)]` from ~line 914).
- **Refactor recommendation:** Extract the data model + its unit tests into `delta_record_model.rs`, leaving the streaming driver + integration tests. Low urgency.
- **Severity:** LOW

### `cqlite-core/src/parser/enhanced_statistics_parser.rs` — 3,059 lines
- **Purpose:** Parses Statistics.db (SerializationHeader, EncodingStats) for delta-coded timestamp decoding.
- **Why it's large:** 10+ specialized parsing functions with backtracking, debug logging, and an ASCII-pattern fallback for malformed data.
- **Test code share:** ~15% (from ~line 2,400).
- **Refactor recommendation:** Split into `header_parser.rs`, `encoding_stats_parser.rs`, `serialization_header_parser.rs`, `fallback_parser.rs`.
- **Severity:** MEDIUM

### `cqlite-cli/src/commands/mod.rs` — 2,997 lines
- **Purpose:** Central CLI command dispatcher — query execution, import/export (CSV/JSON/Parquet/CQL), SSTable analysis, benchmarking, validation.
- **Why it's large:** 8+ large async functions mixing query dispatch, import, export (4 format variants), analysis, benchmarking; repeated `#[cfg(feature = "state_machine")]` stubs.
- **Test code share:** ~3% (2 unit tests from ~line 2,927).
- **Refactor recommendation:** Extract `query_execution.rs`, `import.rs`, `export.rs`, `sstable_ops.rs`.
- **Severity:** HIGH

### `cqlite-core/src/parser/types.rs` — 2,856 lines
- **Purpose:** CQL type deserialization — primitives, collections, tuples, UDTs (with registry), V5 + legacy formats.
- **Why it's large:** Dual parsers (V5 + legacy) for 25+ types, UDT registry resolution, collection-element parsers, memory guards.
- **Test code share:** ~30% (from ~line 2,400).
- **Refactor recommendation:** Split into `primitives.rs`, `collections.rs`, `udt.rs`, `tombstones.rs`; keep `CqlTypeId` + dispatcher in root.
- **Severity:** MEDIUM

### `cqlite-core/src/storage/sstable/writer/mod.rs` — 2,705 lines
- **Purpose:** Coordinates writing all SSTable components (Data/Index/Filter/Statistics/Summary/Digest/TOC) for both BIG and BTI formats.
- **Why it's large:** ~2,100-line `impl SSTableWriter` intertwining BIG and BTI paths; a ~600-line `finish()` method.
- **Test code share:** ~18% (`#[cfg(test)]` from ~line 1,339).
- **Refactor recommendation:** Extract BTI logic into a dedicated `BtiSSTableWriter` / trait-based writer to separate the two format paths.
- **Severity:** MEDIUM

### `cqlite-core/src/storage/sstable/writer/stats_writer.rs` — 2,564 lines
- **Purpose:** Writes Cassandra-5-compatible Statistics.db (nb-format TOC, VALIDATION/COMPACTION/STATS/HEADER, per-component CRC32, BTI deletion markers).
- **Why it's large:** ~1,000 lines of SERIALIZATION_HEADER encoding (CQL→marshal, VInt delta encoding) plus ~800 lines of tests.
- **Test code share:** ~31% (from ~line 1,507).
- **Refactor recommendation:** Extract `build_serialization_header_component()` into `serialization_header.rs`; keep STATS/COMPACTION/TOC in the writer.
- **Severity:** MEDIUM

### `cqlite-cli/src/tui.rs` — 2,423 lines
- **Purpose:** Full TUI mode — multi-panel layout, keyboard navigation, status metrics, focus cycling, panel toggles.
- **Why it's large:** ~300 lines state + ~400 rendering + ~800 event handling + ~780 tests, all at top level.
- **Test code share:** ~32% (`#[cfg(test)]` from ~line 1,643).
- **Refactor recommendation:** Extract `tui_events.rs` (key dispatch → `Action` enum) and `tui_render.rs` (render_* helpers).
- **Severity:** MEDIUM

### `cqlite-core/src/schema/discovery.rs` — 2,328 lines
- **Purpose:** Schema discovery/validation/export from SSTables across Cassandra versions (UDTs, collections, frozen types, indexes).
- **Why it's large:** 33 struct/enum definitions + 7 impl blocks, including a full `TypeInferenceEngine`, `SchemaValidator`, and `SchemaExporter`.
- **Test code share:** ~8% (from ~line 2,134).
- **Refactor recommendation:** Extract `inference.rs`, `validator.rs`, `exporter.rs`.
- **Severity:** MEDIUM

### `cqlite-core/src/storage/sstable/mod.rs` — 2,276 lines
- **Purpose:** SSTable umbrella module — re-exports 36 submodules and core types (`SSTableId`, `SSTableManager`, `SSTableStats`).
- **Why it's large:** Mostly module declarations + 8 feature-gated test-module declarations + small utility fns and `SSTableManager` impl.
- **Test code share:** ~9% inline (from ~line 2,065); much higher via gated test modules.
- **Refactor recommendation:** None needed — this is a re-export facade. Add a top-of-file module-contract doc comment.
- **Severity:** LOW

### `cqlite-core/src/schema/aggregator.rs` — 2,185 lines
- **Purpose:** Loads/merges schemas from multiple CQL+JSON sources via two-pass (UDTs first) last-wins merging.
- **Why it's large:** 15 struct/enum defs + full CQL/JSON parsing pipelines + conversion layers. Test module is ~1,222 lines.
- **Test code share:** ~56% (`#[cfg(test)]` from ~line 964).
- **Refactor recommendation:** Extract `json_schema.rs` (JSON types + conversions) and `cql_schema.rs` (CQL parsing); keep orchestration in core.
- **Severity:** MEDIUM

### `cqlite-cli/src/interactive.rs` — 2,185 lines
- **Purpose:** Interactive REPL shell — session management, command execution, result formatting, data exploration.
- **Why it's large:** 1,800+ lines of presentation-layer utilities (help, formatting, paging, config, dir scanning) and no tests.
- **Test code share:** ~0%.
- **Refactor recommendation:** Split into `repl_ui.rs`, `repl_commands.rs`, `repl_config.rs`, `data_dir.rs`; make `interactive.rs` a thin dispatcher (also unlocks unit testing).
- **Severity:** HIGH

### `cqlite-core/src/schema/mod.rs` — 2,025 lines
- **Purpose:** Schema definitions, validation, management (partition/clustering keys, columns, UDTs, dropped columns, comparators).
- **Why it's large:** Data structures (~1,100 lines) + impl (parse, validation, UDT resolution, comparators ~900 lines).
- **Test code share:** ~10% (from ~line 1,649).
- **Refactor recommendation:** Extract `cql_type_parser.rs` and `udt_registry.rs`; move comparator logic out.
- **Severity:** MEDIUM

### `cqlite-core/src/types.rs` — 1,976 lines
- **Purpose:** Core value types (`Value` enum, `UdtValue`, tombstone metadata, serialization, display, conversions).
- **Why it's large:** `Value` is a 30+ variant union with ~700 lines of impls plus supporting structs.
- **Test code share:** ~25% (from ~line 1,401).
- **Refactor recommendation:** Extract `value_display.rs`, `value_conversion.rs`, `tombstone.rs`, `identifiers.rs`.
- **Severity:** MEDIUM

### `cqlite-flight/src/producer.rs` — 1,928 lines
- **Purpose:** Arrow Flight / Trino producer — merges SSTables into Arrow batches, aggregation pushdown, token filtering.
- **Why it's large:** ~67% inline tests (40+ tests, ~1,293 lines from ~line 636) over merge/aggregation/pushdown logic.
- **Test code share:** ~67%.
- **Refactor recommendation:** Split into `merge.rs`, `aggregation.rs`, `pushdown.rs`; move tests to `tests/`.
- **Severity:** MEDIUM

### `cqlite-core/src/schema/cql_parser.rs` — 1,826 lines
- **Purpose:** Nom-based CQL CREATE TABLE parser (names, columns, keys, types, WITH options, quoted identifiers, clustering order).
- **Why it's large:** ~67% tests (two `#[cfg(test)]` blocks, 50+ edge-case tests) over ~600 lines of combinators + ~130-line builder.
- **Test code share:** ~67% (blocks at ~line 611 and ~line 1,197).
- **Refactor recommendation:** Extract combinators to `parser.rs`, keep builder/entry point; move tests to `tests/`.
- **Severity:** MEDIUM

### `cqlite-core/src/query/select_parser.rs` — 1,783 lines
- **Purpose:** Hand-written CQL SELECT parser — WHERE (all operators, IN, BETWEEN, AND/OR), GROUP BY, ORDER BY, LIMIT/OFFSET, aggregations, WRITETIME/TTL, bind markers, UUID literals.
- **Why it's large:** Tokenizer (~300) + parser (~600) + AST (~50) + ~517 lines of tests, all in one file.
- **Test code share:** ~29% (`mod tests` from ~line 1,267).
- **Refactor recommendation:** Split tokenizer to `lexer.rs`, parser to `parser.rs`, AST to `ast.rs` to enable isolated lexer testing.
- **Severity:** HIGH

### `cqlite-cli/src/repl/engine.rs` — 1,783 lines
- **Purpose:** REPL orchestrator — command parsing, query execution, formatting, schema/data-dir loading, config, status-line, multiline input, session state.
- **Why it's large:** REPL loop (~250) + command handler (~600) + DB rebuild (~200) + helpers (~300), with no inline tests.
- **Test code share:** 0% (validated via CLI integration tests).
- **Refactor recommendation:** Extract `commands.rs`, `config_handler.rs`, `discovery.rs`; keep loop/IO in `engine.rs` to enable unit testing.
- **Severity:** HIGH

---

## Appendix A — All production source files > 500 lines (165)

Files under `*/src/` excluding test crates, sorted by line count.

| Lines | File |
|------:|------|
| 13811 | `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs` |
| 12673 | `cqlite-core/src/storage/write_engine/merge.rs` |
| 11900 | `cqlite-core/src/storage/sstable/writer/data_writer.rs` |
| 4910 | `cqlite-core/src/storage/sstable/bti/parser.rs` |
| 4395 | `cqlite-core/src/storage/write_engine/mod.rs` |
| 4060 | `cqlite-core/src/query/select_executor.rs` |
| 4025 | `cqlite-core/src/storage/sstable/reader/data_access.rs` |
| 3594 | `cqlite-core/src/storage/write_engine/cql_to_mutation.rs` |
| 3108 | `cqlite-core/src/storage/sstable/reader/delta_scan.rs` |
| 3059 | `cqlite-core/src/parser/enhanced_statistics_parser.rs` |
| 2997 | `cqlite-cli/src/commands/mod.rs` |
| 2856 | `cqlite-core/src/parser/types.rs` |
| 2705 | `cqlite-core/src/storage/sstable/writer/mod.rs` |
| 2564 | `cqlite-core/src/storage/sstable/writer/stats_writer.rs` |
| 2423 | `cqlite-cli/src/tui.rs` |
| 2328 | `cqlite-core/src/schema/discovery.rs` |
| 2276 | `cqlite-core/src/storage/sstable/mod.rs` |
| 2185 | `cqlite-core/src/schema/aggregator.rs` |
| 2185 | `cqlite-cli/src/interactive.rs` |
| 2025 | `cqlite-core/src/schema/mod.rs` |
| 1976 | `cqlite-core/src/types.rs` |
| 1928 | `cqlite-flight/src/producer.rs` |
| 1826 | `cqlite-core/src/schema/cql_parser.rs` |
| 1783 | `cqlite-core/src/query/select_parser.rs` |
| 1783 | `cqlite-cli/src/repl/engine.rs` |
| 1740 | `cqlite-core/src/storage/write_engine/mutation.rs` |
| 1691 | `cqlite-cli/src/config.rs` |
| 1682 | `cqlite-core/src/export/arrow_convert.rs` |
| 1658 | `bindings/node/src/database.rs` |
| 1655 | `cqlite-core/src/storage/sstable/verify.rs` |
| 1648 | `cqlite-core/src/schema/registry.rs` |
| 1643 | `cqlite-core/src/storage/sstable/row_cell_state_machine.rs` |
| 1632 | `cqlite-core/src/cql/visitor.rs` |
| 1625 | `cqlite-core/src/export/delta_parquet.rs` |
| 1622 | `cqlite-core/src/storage/write_engine/wal.rs` |
| 1608 | `cqlite-core/src/cql/mutation_parser.rs` |
| 1582 | `cqlite-core/src/storage/sstable/compression.rs` |
| 1579 | `cqlite-core/src/storage/sstable/writer/partitions_writer.rs` |
| 1566 | `cqlite-core/src/query/result.rs` |
| 1536 | `cqlite-core/src/storage/sstable/reader/parsing/value_parsing.rs` |
| 1500 | `cqlite-core/src/storage/sstable/writer/index_writer.rs` |
| 1485 | `cqlite-core/src/parser/statistics.rs` |
| 1436 | `cqlite-core/src/export/delta_schema.rs` |
| 1424 | `cqlite-core/src/storage/serialization/types.rs` |
| 1421 | `cqlite-core/src/storage/sstable/reader/block_io.rs` |
| 1366 | `cqlite-core/src/parser/header.rs` |
| 1295 | `cqlite-cli/src/enhanced_interactive.rs` |
| 1257 | `cqlite-cli/src/main.rs` |
| 1229 | `cqlite-core/src/storage/sstable/reader/parsing/block_entries.rs` |
| 1207 | `cqlite-core/src/storage/sstable/row_cell_state_machine_test.rs` |
| 1207 | `cqlite-core/src/storage/sstable/bti/encoder.rs` |
| 1192 | `cqlite-core/src/parser/vint.rs` |
| 1162 | `cqlite-core/src/storage/write_engine/export.rs` |
| 1160 | `cqlite-core/src/query/engine.rs` |
| 1123 | `tools/sstabledump-validator/src/validator.rs` |
| 1116 | `cqlite-core/src/storage/sstable/version_gate.rs` |
| 1107 | `cqlite-core/src/parser/complex_types.rs` |
| 1103 | `cqlite-core/src/parser/repair_metadata.rs` |
| 1098 | `cqlite-core/src/query/executor.rs` |
| 1084 | `cqlite-core/src/storage/sstable/writer/summary_writer.rs` |
| 1067 | `cqlite-core/src/storage/sstable/reader/header.rs` |
| 1057 | `cqlite-core/src/storage/sstable/s4_verification_test.rs` |
| 1048 | `cqlite-core/src/storage/sstable/reader/mod.rs` |
| 980 | `cqlite-core/src/query/select_optimizer.rs` |
| 965 | `cqlite-core/src/storage/sstable_data_manager.rs` |
| 960 | `cqlite-cli/src/commands/info.rs` |
| 948 | `cqlite-core/src/storage/sstable/index_reader.rs` |
| 948 | `cqlite-core/src/config.rs` |
| 946 | `bindings/python/src/database.rs` |
| 908 | `cqlite-core/src/types/comparator.rs` |
| 895 | `cqlite-core/src/storage/sstable/reader/parsing/key_parsing.rs` |
| 887 | `cqlite-core/src/storage/sstable/reader/tests.rs` |
| 886 | `cqlite-core/src/storage/sstable/tombstone_merger.rs` |
| 862 | `cqlite-flight/src/filter.rs` |
| 857 | `cqlite-core/src/cql/ast.rs` |
| 857 | `cqlite-cli/src/repl/command_parser.rs` |
| 856 | `cqlite-core/src/schema/cql_generator.rs` |
| 855 | `cqlite-core/src/schema/json_exporter.rs` |
| 852 | `cqlite-flight/src/agg.rs` |
| 827 | `cqlite-cli/src/commands/write.rs` |
| 826 | `cqlite-core/src/storage/sstable/header_spec.rs` |
| 816 | `cqlite-core/src/query/planner.rs` |
| 813 | `cqlite-core/src/benchmarks/cassandra5/throughput_benchmarks.rs` |
| 809 | `cqlite-cli/src/repl/completion.rs` |
| 798 | `cqlite-core/src/export/parquet.rs` |
| 797 | `cqlite-core/src/storage/sstable/bulletproof_reader.rs` |
| 790 | `cqlite-core/src/cql/nom_backend.rs` |
| 787 | `tools/sstabledump-validator/src/reconciliation.rs` |
| 775 | `cqlite-core/src/storage/sstable/reader/parsing/mod.rs` |
| 773 | `cqlite-core/src/storage/write_engine/memtable.rs` |
| 773 | `cqlite-core/src/storage/schema_discovery.rs` |
| 771 | `cqlite-cli/src/query_processor.rs` |
| 765 | `cqlite-cli/src/output/json.rs` |
| 761 | `cqlite-core/src/storage/sstable/reader/component_loading.rs` |
| 758 | `cqlite-core/src/query/select_ast.rs` |
| 756 | `tools/sstabledump-validator/src/test_datasets.rs` |
| 753 | `cqlite-core/src/parser/collection_tests.rs` |
| 751 | `cqlite-core/src/storage/sstable/reader/source.rs` |
| 747 | `cqlite-core/src/storage/sstable/reader/partition_lookup.rs` |
| 745 | `cqlite-flight/src/ticket.rs` |
| 734 | `cqlite-core/src/error.rs` |
| 724 | `cqlite-core/src/memory/mod.rs` |
| 720 | `cqlite-cli/src/test_infrastructure/performance.rs` |
| 714 | `cqlite-cli/src/commands/schema.rs` |
| 713 | `cqlite-core/src/storage/sstable/bloom.rs` |
| 688 | `cqlite-cli/src/commands/bench.rs` |
| 685 | `cqlite-core/src/storage/repl_data_api.rs` |
| 684 | `cqlite-core/src/lib.rs` |
| 683 | `cqlite-cli/src/data_parser.rs` |
| 680 | `cqlite-cli/src/select_query_engine.rs` |
| 678 | `cqlite-core/src/schema/parser_tests.rs` |
| 677 | `cqlite-core/src/benchmarks/cassandra5/memory_benchmarks.rs` |
| 674 | `cqlite-core/src/storage/sstable/summary_reader.rs` |
| 673 | `cqlite-core/src/storage/sstable/reader/parsing/comparator_value_parsing.rs` |
| 670 | `cqlite-core/src/parser/collection_benchmarks.rs` |
| 668 | `cqlite-core/src/util/value_fmt.rs` |
| 665 | `cqlite-core/src/storage/mod.rs` |
| 664 | `cqlite-core/src/storage/sstable/writer/compressed_data_writer.rs` |
| 664 | `cqlite-core/src/storage/sstable/reader/compression.rs` |
| 663 | `cqlite-core/src/cql/error.rs` |
| 663 | `cqlite-core/src/benchmarks/cassandra5/zerocopy_benchmarks.rs` |
| 663 | `cqlite-core/src/benchmarks/cassandra5/mod.rs` |
| 660 | `cqlite-core/src/query/parser.rs` |
| 660 | `cqlite-cli/src/repl/history.rs` |
| 659 | `cqlite-core/src/storage/sstable/schema_aware_reader.rs` |
| 656 | `cqlite-core/src/query/select_integration_tests.rs` |
| 642 | `cqlite-cli/src/repl_integration.rs` |
| 634 | `cqlite-core/src/storage/partition_key_codec.rs` |
| 632 | `cqlite-core/src/benchmarks/cassandra5/compression_benchmarks.rs` |
| 631 | `cqlite-core/src/parser/optimized_complex_types.rs` |
| 623 | `cqlite-cli/src/repl/session.rs` |
| 619 | `cqlite-core/src/storage/sstable/writer/filter_writer.rs` |
| 616 | `tools/sstabledump-validator/src/docker.rs` |
| 616 | `cqlite-core/src/storage/sstable/chunk_decompressor.rs` |
| 614 | `tools/sstabledump-validator/src/reporter.rs` |
| 614 | `cqlite-core/src/storage/sstable/bti/node.rs` |
| 612 | `cqlite-cli/src/test_infrastructure/fixtures.rs` |
| 612 | `bindings/python/src/result.rs` |
| 605 | `tools/sstabledump-validator/src/comparator.rs` |
| 602 | `cqlite-core/src/parser/benchmarks.rs` |
| 601 | `cqlite-flight/src/service.rs` |
| 600 | `cqlite-cli/src/cli_types.rs` |
| 594 | `cqlite-cli/tests/compatibility/src/data_generator.rs` |
| 593 | `cqlite-cli/tests/compatibility/src/suite.rs` |
| 591 | `cqlite-core/src/storage/sstable/s3_verification_test.rs` |
| 590 | `cqlite-core/src/query/prepared.rs` |
| 580 | `cqlite-core/src/parser/statistics_test.rs` |
| 578 | `cqlite-cli/src/test_infrastructure/assertions.rs` |
| 575 | `cqlite-core/src/storage/write_engine/merge_policy.rs` |
| 575 | `cqlite-core/src/schema/parser.rs` |
| 567 | `cqlite-cli/src/test_infrastructure/integration.rs` |
| 561 | `bindings/python/src/value.rs` |
| 551 | `cqlite-core/src/storage/sstable/compression_info.rs` |
| 550 | `cqlite-core/src/cql/config.rs` |
| 546 | `cqlite-cli/src/output/csv.rs` |
| 542 | `cqlite-core/src/storage/sstable/format_detector.rs` |
| 540 | `cqlite-core/src/storage/sstable/bti/nodes.rs` |
| 539 | `cqlite-core/src/storage/sstable/oa_format_compliance_test.rs` |
| 534 | `cqlite-core/src/storage/sstable/directory/validation.rs` |
| 533 | `cqlite-core/src/query/m2_select_validator.rs` |
| 531 | `cqlite-core/src/testing/dataset_helpers.rs` |
| 523 | `cqlite-core/src/storage/sstable/directory/tests.rs` |
| 511 | `cqlite-core/src/version_hints.rs` |
| 508 | `cqlite-cli/src/script_executor.rs` |
| 504 | `cqlite-core/src/parser/udt_tests.rs` |

---

## Appendix B — Test & integration files > 500 lines (168)

Files under `tests/` directories, the `tests/` crate, and `*_test*.rs` modules.
Listed separately because large test files are generally lower-priority for refactoring.

| Lines | File |
|------:|------|
| 4835 | `cqlite-cli/tests/parquet_writer_tests.rs` |
| 3821 | `cqlite-core/tests/sstableloader_integration.rs` |
| 1747 | `tests/src/integration_e2e.rs` |
| 1746 | `cqlite-core/tests/issue_819_differential_compaction.rs` |
| 1692 | `cqlite-core/tests/scan_delta_parity_test.rs` |
| 1508 | `cqlite-core/tests/write_integration.rs` |
| 1502 | `cqlite-core/tests/issue_1015_dropped_static_parity.rs` |
| 1502 | `cqlite-core/tests/compaction_integration.rs` |
| 1471 | `cqlite-core/tests/sstabledump_parity_summary.rs` |
| 1465 | `cqlite-core/tests/support/canonical_jsonl.rs` |
| 1435 | `cqlite-core/tests/write_read_roundtrip/type_coverage.rs` |
| 1411 | `tests/src/comprehensive_integration_test_suite.rs` |
| 1403 | `cqlite-core/tests/write_engine_integration_test.rs` |
| 1389 | `tests/src/comprehensive_parser_integration_tests.rs` |
| 1344 | `tests/src/cql_integration_tests.rs` |
| 1311 | `tests/src/cql_parser_validation_suite.rs` |
| 1287 | `cqlite-core/tests/issue_1003_schema_evolution_header_parity.rs` |
| 1276 | `tests/sstable_reading/header_parsing_comprehensive_tests.rs` |
| 1269 | `cqlite-core/tests/write_read_roundtrip/edge_cases.rs` |
| 1253 | `tests/src/edge_case_stress_testing.rs` |
| 1243 | `cqlite-cli/tests/delta_roundtrip_tests.rs` |
| 1241 | `tests/src/comprehensive_integration_tests.rs` |
| 1161 | `tests/src/edge_case_sstable_corruption.rs` |
| 1144 | `cqlite-core/tests/v5_compressed_legacy_parity_test.rs` |
| 1140 | `tests/src/complex_type_validation_suite.rs` |
| 1123 | `cqlite-core/tests/sstable_discovery_comprehensive_tests.rs` |
| 1107 | `tests/src/cql_performance_benchmarks.rs` |
| 1104 | `tests/src/real_sstable_test_fixtures.rs` |
| 1102 | `cqlite-core/tests/issue_1011_ttl_local_deletion_parity.rs` |
| 1070 | `cqlite-core/tests/issue_954_clustering_slice_seek.rs` |
| 1053 | `tests/src/edge_case_data_types.rs` |
| 1050 | `cqlite-cli/tests/export_integration_tests.rs` |
| 1027 | `cqlite-core/tests/issue_1010_deletion_markers_parity.rs` |
| 1023 | `cqlite-core/tests/index_db_offset_calculation_tests.rs` |
| 1019 | `tests/src/advanced_edge_case_tests.rs` |
| 1019 | `cqlite-core/tests/type_invariant_tests.rs` |
| 996 | `tests/src/bin/performance_regression_test_runner.rs` |
| 991 | `cqlite-cli/tests/integration_sstable_tests.rs` |
| 980 | `tests/golden_path/benchmarks.rs` |
| 962 | `cqlite-core/tests/sstable_parity_integration_test.rs` |
| 952 | `tests/src/performance_benchmark_runner.rs` |
| 945 | `tests/src/repl_quality_gates.rs` |
| 943 | `tests/validation/test_hardened_validator_parser.rs` |
| 940 | `cqlite-core/tests/issue_955_multi_partition_lookup_parity.rs` |
| 926 | `cqlite-core/tests/issue_694_writetime_ttl_parity.rs` |
| 919 | `tests/src/comprehensive_sstable_test_suite.rs` |
| 912 | `cqlite-cli/tests/comprehensive_test_framework.rs` |
| 902 | `tests/src/parser_validation.rs` |
| 889 | `tests/src/performance_validation_suite.rs` |
| 887 | `tests/src/performance_regression_framework.rs` |
| 880 | `cqlite-core/tests/issue_1074_static_write_parity.rs` |
| 873 | `cqlite-core/tests/issue_655_oa_read_gates.rs` |
| 871 | `cqlite-core/tests/issue_1014_resurrection_safety_parity.rs` |
| 869 | `tests/src/cql_test_data_fixtures.rs` |
| 868 | `cqlite-cli/tests/parquet_dataset_roundtrip_tests.rs` |
| 867 | `tests/src/performance_benchmarks.rs` |
| 867 | `cqlite-cli/tests/one_shot_e2e_tests.rs` |
| 858 | `cqlite-core/tests/issue_961_parameterized_partition_lookup_parity.rs` |
| 836 | `cqlite-core/tests/issue_1006_null_empty_boundary_parity.rs` |
| 831 | `tests/sstable_reading/header_parsing_performance_tests.rs` |
| 827 | `cqlite-core/tests/issue_1013_rt_index_block_parity.rs` |
| 826 | `tests/src/bin/cql_validation_test_runner.rs` |
| 825 | `tests/sstable_reading/header_parsing_unit_tests.rs` |
| 825 | `cqlite-core/tests/issue_1007_complex_type_parity.rs` |
| 824 | `cqlite-cli/tests/output_format_tests.rs` |
| 822 | `cqlite-core/tests/sstabledump_parity_index.rs` |
| 817 | `cqlite-core/tests/static_composite_roundtrip_test.rs` |
| 816 | `cqlite-core/tests/sstable_parity_compression_info_test.rs` |
| 814 | `tests/src/edge_case_runner.rs` |
| 811 | `tests/src/validation_test_runner.rs` |
| 792 | `tests/src/performance_complex_types_benchmark.rs` |
| 790 | `tests/src/repl_integration_tests.rs` |
| 790 | `cqlite-cli/tests/output_determinism_regression_tests.rs` |
| 775 | `cqlite-core/tests/sstable_parity_statistics_db_strict_test.rs` |
| 774 | `cqlite-core/tests/query_correctness_tests.rs` |
| 772 | `tests/src/collection_compatibility_tests.rs` |
| 770 | `cqlite-core/tests/sstable_parity_index_db_test.rs` |
| 768 | `tests/src/compatibility_framework.rs` |
| 760 | `cqlite-cli/tests/unit/enhanced_unit_tests.rs` |
| 759 | `tests/src/comparator_type_tests.rs` |
| 748 | `cqlite-cli/tests/end_to_end_tests.rs` |
| 745 | `tests/sstable_reading/header_parsing_error_handling_tests.rs` |
| 745 | `tests/src/cli_integration_tests.rs` |
| 745 | `cqlite-core/tests/sstable_parity_filter_db_test.rs` |
| 741 | `cqlite-core/tests/issue_1012_skipped_sstable_parity.rs` |
| 733 | `cqlite-core/tests/issue_908_bti_canonical_write.rs` |
| 724 | `cqlite-core/tests/index_db_parsing_regression_tests.rs` |
| 724 | `cqlite-cli/tests/test_runner.rs` |
| 723 | `tests/golden_path_summary_index_integration_tests.rs` |
| 723 | `cqlite-core/tests/schema_aggregator_integration_test.rs` |
| 721 | `tests/golden_path/coverage.rs` |
| 720 | `cqlite-core/tests/issue_1005_collection_serializer_vectors.rs` |
| 717 | `tests/src/real_cassandra_data_validator.rs` |
| 717 | `cqlite-cli/tests/delta_export_tests.rs` |
| 715 | `cqlite-core/tests/issue_1008_counter_final_value_parity.rs` |
| 713 | `cqlite-cli/tests/duckdb_parquet_validation.rs` |
| 696 | `cqlite-cli/tests/error_handling_tests.rs` |
| 695 | `cqlite-cli/tests/table_formatter_integration.rs` |
| 693 | `tests/golden_path_partition_lookup_tests.rs` |
| 690 | `tests/schema_integration_tests.rs` |
| 687 | `tests/golden_path/integration_tests.rs` |
| 681 | `tests/src/real_sstable_compatibility_test.rs` |
| 677 | `cqlite-core/tests/statistics_parser_no_heuristics_tests.rs` |
| 676 | `cqlite-core/tests/sstable_performance_regression_tests.rs` |
| 666 | `cqlite-core/tests/m1_memory_validation.rs` |
| 659 | `cqlite-cli/tests/unit_tests.rs` |
| 657 | `tests/comprehensive_component_integration_tests.rs` |
| 657 | `cqlite-core/tests/issue_998_inline_crc_trailers.rs` |
| 647 | `cqlite-core/tests/sstabledump_parity_data.rs` |
| 645 | `cqlite-core/tests/v5_compressed_legacy_integration_test.rs` |
| 645 | `cqlite-core/tests/sstable_discovery_negative_tests.rs` |
| 645 | `cqlite-core/tests/issue_997_compressioninfo_parity.rs` |
| 641 | `tests/benchmarks/compatibility_testing.rs` |
| 637 | `cqlite-core/tests/sstable_component_discovery_tests.rs` |
| 637 | `cqlite-core/tests/issue_764_explicit_local_deletion_time.rs` |
| 634 | `cqlite-core/tests/issue_1009_canonical_jsonl_comparator.rs` |
| 629 | `tests/benchmarks/performance_suite.rs` |
| 626 | `cqlite-cli/tests/repl_integration_tests.rs` |
| 624 | `tests/security_integration_tests.rs` |
| 623 | `cqlite-core/tests/sstable_parity_compression_info_db_strict_test.rs` |
| 623 | `cqlite-core/tests/issue_832_bti_traversal.rs` |
| 619 | `cqlite-core/tests/issue_824_column_subset_and_filter.rs` |
| 618 | `cqlite-core/tests/sstabledump_parity_statistics.rs` |
| 616 | `tests/src/type_system_tests.rs` |
| 613 | `cqlite-core/tests/write_read_roundtrip.rs` |
| 612 | `cqlite-core/tests/write_read_roundtrip/summary.rs` |
| 607 | `cqlite-core/tests/index_reader_memory_optimization_tests.rs` |
| 598 | `tests/golden_path_scan_operations_tests.rs` |
| 598 | `cqlite-core/tests/regression_guard_tests.rs` |
| 595 | `tests/benchmarks/load_testing.rs` |
| 595 | `cqlite-core/tests/issue_821_writer_byte_invariants.rs` |
| 590 | `tests/golden_path/scenarios.rs` |
| 587 | `tests/golden_path/metrics.rs` |
| 586 | `tests/src/integration_test_harness.rs` |
| 583 | `cqlite-core/tests/sstable_parity_repaired_metadata_test.rs` |
| 578 | `cqlite-cli/tests/test_helpers.rs` |
| 575 | `cqlite-core/tests/issue_548_uuid_where_tests.rs` |
| 573 | `cqlite-core/tests/observability_correctness.rs` |
| 572 | `cqlite-core/tests/issue_1004_primitive_codec_vectors.rs` |
| 569 | `tests/golden_path/validation.rs` |
| 565 | `cqlite-cli/tests/one_shot_integration_test.rs` |
| 564 | `tests/integration/performance_integration.rs` |
| 564 | `cqlite-core/tests/common/sstable_test_utils.rs` |
| 561 | `tests/src/sstable_format_tests.rs` |
| 561 | `tests/src/performance_monitor.rs` |
| 557 | `tests/sstable_reading/performance_tests.rs` |
| 555 | `cqlite-core/tests/write_error_injection.rs` |
| 552 | `tests/golden_path/artifacts.rs` |
| 548 | `tests/helpers/docker.rs` |
| 548 | `cqlite-core/tests/sstable_discovery_unit_tests.rs` |
| 547 | `cqlite-core/tests/issue_911_bti_sstabledump_parity.rs` |
| 544 | `tests/integration/test_sstabledump_parity_integration.rs` |
| 542 | `tests/src/issue_28a_heuristics_removal_tests.rs` |
| 540 | `cqlite-core/tests/reader_compression_tests.rs` |
| 535 | `cqlite-core/tests/execution_path_parity_tests.rs` |
| 527 | `cqlite-core/tests/issue_657_da_foundation.rs` |
| 526 | `cqlite-core/tests/issue_933_range_tombstone_compaction.rs` |
| 526 | `cqlite-cli/tests/select_fallback_tests.rs` |
| 525 | `cqlite-core/tests/test_issue_827_streaming_parity.rs` |
| 525 | `cqlite-core/benches/m1_performance.rs` |
| 524 | `cqlite-core/tests/compact_command.rs` |
| 523 | `tests/golden_path/harness.rs` |
| 520 | `cqlite-core/tests/parser_error_handling_tests.rs` |
| 519 | `tests/fixture_specific_integration_tests.rs` |
| 518 | `cqlite-cli/tests/integration/script_execution_test.rs` |
| 515 | `cqlite-core/tests/issue_962_tombstones_honest_fallback.rs` |
| 512 | `cqlite-core/tests/write_read_roundtrip/data_multi.rs` |
| 509 | `cqlite-core/tests/contract_stability_tests.rs` |

---

## How to regenerate

```bash
# Production source files > 500 lines
find . -type f -name '*.rs' -not -path './target/*' -path '*/src/*' -not -path './tests/*' \
  | xargs wc -l | awk '$2!="total" && $1>500{print $1"\t"$2}' | sort -rn

# All .rs files > 500 lines (incl. tests)
find . -type f -name '*.rs' -not -path './target/*' \
  | xargs wc -l | awk '$2!="total" && $1>500{print $1"\t"$2}' | sort -rn
```
