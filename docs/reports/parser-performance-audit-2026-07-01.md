# CQLite Parser Performance Audit — 2026-07-01

**Goal:** highest-performing reads with even, predictable outcomes under high load.
**Scope:** the parser only — binary format decoding from decompressed bytes to values: VInt/primitive decode, row/cell framing and emit, CQL type deserialization, metadata parsers (Statistics.db, SerializationHeader, header), and the BTI trie decode. This is the declared out-of-scope half of the July read-path audit (`read-path-performance-audit-2026-07-01.md`), which owns I/O, caching, chunk decompression, index navigation mechanics, merge, and the query engine. Border findings filed there (E1 Value=88B, E2 row HashMap consumer side, E3 copy chain, E6 snappy guess, C3–C5 BTI lookup mechanics) are referenced, not re-filed.
**Method:** six parallel read-only specialist audits (decode primitives, row/cell pipeline, type deserialization, metadata parsers, BTI trie, cross-cutting Rust/architecture), every finding carrying `file:line` evidence and caller-traced wired/dead status, plus lead-level cross-verification of the four correctness-class claims directly against source.

---

## Executive summary

**The parser's safety posture is genuinely strong — and its performance posture wastes most of that discipline.** Zero `unsafe`, zero library `unwrap`/`expect`, every slice length-guarded, depth-bounded recursion on the hot path, `Err`-not-panic on truncated input, strong release profile (lto + codegen-units=1). But the same code allocates two throwaway `String`s **per cell** to decide a branch, copy-pastes its inner loops four and five times over, and carries ~2,500 lines of dead parallel parser generations. Six systemic problems, in descending order of importance:

1. **Four correctness landmines, all verified against source.** A performance audit found these because "even, predictable outcomes" includes *correct* outcomes:
   - **VInt length decode is signed where Cassandra writes unsigned.** `parse_vint_length` — the primitive behind length/count fields — routes through the ZigZag `parse_vint` (`vint.rs:803-804` → `vint_fixed.rs:34-40`), silently mis-reading any genuinely-unsigned length whose bits differ under ZigZag while consuming the same byte count. The v5 hot path was migrated to unsigned `parse_vuint` piecemeal after roborev #863 (`complex_column.rs:135`), but ~20 live call sites remain (`key_parsing.rs`, `block_entries.rs`, `comparator_value_parsing.rs`, `row_cell_state_machine.rs`, `header_spec.rs`). Some parse CQLite-written structures where the encoder is also ZigZag (self-consistent); the blast radius must be established by a corpus differential test **before** the fix, not assumed.
   - **A corrupt `Statistics.db` silently poisons every timestamp.** The parser correctly `Err`s; `load_statistics_reader` swallows it into `None` (`component_loading.rs:235-242`) and the reader proceeds with zero EncodingStats baselines and no SerializationHeader columns — silently wrong `WRITETIME()`/TTL/deletion times for the whole SSTable. This is the "default-on-parse-failure" anti-pattern the no-heuristics mandate forbids. **NEEDS-YOU:** hard-fail open vs explicit opt-in degraded mode.
   - **The live block path silently decodes nine scalar types as `Blob`.** `parse_value_with_schema_type` ends in `_ => parse_blob_value` (`value_parsing.rs:480-483`); Float, Double, Timestamp, Varint, Decimal, Duration, Time, Inet, Timeuuid have no arm. Wrong-typed values can pass row-count parity while corrupting data.
   - **BTI "depth" cap is measured in key-path bytes (128).** `traversal.rs:28,139` — a legitimate long `text`/`blob` clustering key spuriously errors as "corrupt trie", and (per §BTI below) that DFS backs every wide-partition clustering read.

2. **Type dispatch is resolved per cell, not per column.** Every cell decode calls `data_type.to_lowercase()` — twice (`cell_value.rs:246`, `udt.rs:1188`) — then walks a ~30-arm string-match ladder that is physically copy-pasted across five v5 files (`"int" =>` appears 5×). The block path is worse: `ComparatorType::from_data_type` per value = a full recursive type-string parse per cell (`value_parsing.rs:446`). A 1M-row × 10-col scan performs ~20M transient type-string allocations producing nothing but a branch target, for a type that is constant per column. The correct pattern already exists in-tree: the once-per-block `RowColumnResolution` hoist (#1046) — it just never absorbed dispatch. **This is the single biggest hot-path lever in the parser.**

3. **The inner loops are duplicated 4–6×.** One row-body decoder (good), but the partition/row emit skeleton exists in four copies (`block_emit.rs` ×2, `block_emit_windowed.rs` ×2, `compaction.rs`) with the subtle #932 tombstone-coexistence decision hand-copied into all four, the static-merge and HashMap→sorted-Map collapse into three. Three near-verbatim live `ComparatorType` value decoders (`value_parsing.rs`, `comparator_value_parsing.rs`, `schema/parser.rs`). Four VInt implementations. Every future tombstone or type fix must land in N places or the paths silently diverge — this is how parity regressions are manufactured.

4. **Per-row constant factors that shouldn't exist.** After each row, boundary detection runs a *full allocating* partition-header try-parse with eager `format!` error strings as a control-flow sentinel (`mod.rs:586-601`, `row_framing.rs:576-628`) — one throwaway key `to_vec` + up to one error-string per row. Rows are built as `HashMap<String,Value>` with per-cell name clones, then **alphabetically re-sorted every row** solely to hide HashMap iteration nondeterminism (`block_emit_windowed.rs:393-429`) — the parser-side producer of read-path E2. Partition keys and `TableId` strings are cloned per row (13 sites). Every text/blob value is copied out of the decompressed chunk (`String::from_utf8(bytes.to_vec())`).

5. **"Built but unwired," third confirmation — now quantified for the parser.** Dead: `optimized_complex_types.rs` (631L), `zero_copy_parser.rs` (309L), the legacy statistics subtree in `statistics.rs` (~550L incl. analyzer), the `vint.rs` dead cluster (~350L), `parse_vint_binary`, and the BTI stateful navigator stack (`bti/parser/reader.rs` ~1000L — **dead and latently broken**: `TrieNavigator::navigate_to_child` adds an absolute offset to a relative base, `node.rs:459-461`). The in-crate `benchmarks`-feature harness (~1.3kL) benchmarks the two dead stacks — a perf number for code no user runs. The ANTLR CQL backend is a pure stub the `ParserFactory` will happily hand out for `UseCase::Development` or `strict_validation=true`, whereupon every parse fails (`antlr_backend.rs:15-19`, `factory.rs:52-81`). Total: ~4,100 lines of dead/misleading parser code.

6. **The testing floor for a parser of untrusted bytes is missing.** No fuzz target anywhere (the safety discipline in #1-good below is *unproven* against adversarial input), proptest in exactly one file, zero struct-size assertions, no per-type decode bench, no alloc-budget gate. The 33-table sstabledump JSONL parity harness is the one real safety net — every refactor below leans on it.

### What is already good (verified; do not churn)

- **Zero `unsafe`, zero library `unwrap`/`expect`/`panic!` in the wired parse paths**; all multi-byte reads length-guarded before indexing (`node_decode.rs`, `vint.rs:685`, `cell_value.rs:263-383`); counts validated before `with_capacity` (`complex_column.rs:209-221`); `try_into` over `as` for lengths; release profile lto+cu=1+panic=abort.
- **The per-block schema-resolution hoist** (`RowColumnResolution::build`, `parsing/mod.rs:145-242`, #1046) — the model the dispatch fix extends.
- **`want_cell_metadata` gating** (`row_data.rs:55-68`): WRITETIME/TTL metadata maps allocated only when queried.
- **One shared row-body decoder** (`parse_row_data_with_offset`) under all four emit paths; `parse_block_emit` correctly delegates to the windowed impl (no drift there).
- **The write-side VInt module is exemplary** (`storage/serialization/vint.rs`): branchless Cassandra length formula, encode-into-caller-buffer, uniform `#[inline]` — it is the model the read side should mirror.
- **Metadata stack is one parser, not two**: `enhanced_statistics_parser` reuses `statistics.rs`'s types/header — no drift risk; adversarial-length allocations bounded fail-closed (`repair_metadata.rs:231-239` with `checked_mul`); corrupt-input `Err` behavior is *tested* for Statistics.db/repair/header.
- **`repair_metadata.rs` is NOT a heuristics violation** (hypothesis disproven): it decodes Cassandra's persisted repair fields and models undecodable fields as `RepairField::Unparsed` rather than fabricating — the discipline the rest of the codebase should copy (see M1).
- **BTI point-walk is termination-safe by construction** (`walk_bti_trie` bounded by encoded-key length, `partitions.rs:394-448`); node decoders bounds-check before slicing; DFS is iterative, not recursive; the #832 Dense sentinel fix holds.
- **Decode-side decimal/varint are overflow-safe** (raw-bytes representation, no `10^scale` materialization — roborev doctrine held); v5 path has a nesting depth guard with adversarial fixtures.
- **Deferred error construction in the value decoders** — corruption `format!` built in `map_err` closures, not on the success path (the boundary-peek, K2, is the exception).

---

## Proposed epics

Sequencing rationale: **H first** — fuzz + benches + parity nets make every later claim demonstrable and every refactor safe; several fixes below are large consolidations that must not land without them. **I** (correctness landmines) ships next and is small. **J** and **K** are the perf epics — J is dispatch + de-duplication (the throughput lever), K is the per-row mechanics. **L** is BTI. **M** is hygiene that makes the rest cheap to land (campsite splits on exactly the files J/K touch).

Every child issue lists its TDD tests — written **first**, and they must fail on current `main` (or be demonstrably un-writable today).

Numbering continues the read-path audit (A–G) to avoid collision: **H–M**.

---

### Epic H — Parser measurement + adversarial safety net  `P0`

The parser consumes untrusted bytes with no fuzz target, no decode benches, no alloc budgets. Land this before (or with) the first refactor.

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| H1 | Fuzz crate for the parser | `fuzz/` (cargo-fuzz) targets: `parse_vint`/`parse_vuint`, primitive+collection value decode, `parse_block_emit` (partition loop), `bti node_decode`/DFS. Contract: arbitrary bytes → `Ok`/`Err`, never panic/hang/OOM (allocation cap via fuzz harness). Seed from `test-data/` | the targets themselves; CI smoke lane (short run per PR, long nightly) | M |
| H2 | Per-type decode benches + alloc-budget lane | criterion: per-CQL-type decode, wide-row all-primitive decode (rows/sec), text-heavy scan; dhat lane: allocs/row and allocs/cell budgets. Gate in `perf-gate.json` (the existing `read/type_heavy` exercises the real v5 path — good — but nothing pins decode-level cost) | budgets fail on main: type-dispatch allocs are O(rows×cols) today | M |
| H3 | Struct-size assertion suite | `#[cfg(test)]` `size_of` pins for `Value` (ties read-path E1/A4), `ComparatorType`, `ParseStep`/scan cursor, BTI `Transition` | fails the moment a hot struct grows; Value assert fails on main (88B) | S |
| H4 | Dispatch + codec lockstep parity tests | (a) v5 string-ladder vs `ComparatorType` enum path: identical `Value`/`Err` for the same bytes across all types (prerequisite to J1/J2); (b) read-decoder vs write-side `serialization/types.rs` type-map lockstep | divergences surface as failures now, not as parity bugs later | S/M |
| H5 | Parser work-counter infra | test-only counters (existing `SCAN_FOR_KEY_CALLS` pattern): `to_lowercase` calls, header try-parses, BTI nodes-visited/pointer-decodes, per-row sort invocations — the assertion currency for every epic below | counters land with first consumer; each asserts a number that is wrong on main | S |

---

### Epic I — Correctness landmines  `P0`

All verified against source at lead level. Small fixes; the work is the verification-first tests.

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| I1 | VInt length decode: unsigned, not ZigZag | `parse_vint_length` → ZigZag `parse_vint` (`vint.rs:803-804`); Cassandra writes lengths with `writeUnsignedVInt`. **Step 1 (verify):** corpus differential test — every length offset in every corpus Data.db/Statistics.db compared between current decode and unsigned decode; classify the ~20 live call sites (Cassandra-written vs CQLite-self-written where ZigZag is self-consistent). **Step 2:** repoint Cassandra-facing sites to the unsigned decoder; document CQLite-internal signed conventions explicitly | unit: `parse_vint_length(&[0x05]) == 5` (today: 2, or Err for odd) fails on main; corpus differential test; 33-table parity green | M |
| I2 | VInt framing bugs: truncation masked, unbounded fallback | `input.len()==1` special case makes `0x80` decode differently by slice framing and returns a wrong value on truncated multi-byte vints (`vint_fixed.rs:43-63`); `parse_zigzag_vint` `0xF0`/`0xFF` arms consume `len()-1` bytes on corrupt leads (`vint.rs:146-179`) | truncated multi-byte vint → `Err` (today: wrong value); same-prefix/different-framing agreement test; `[0xF0, junk…]` must not swallow the buffer | S |
| I3 | Corrupt Statistics.db must not silently zero-base decode | parser `Err` swallowed to `None` at `component_loading.rs:235-242` → zero EncodingStats baselines + no SerializationHeader columns → silently wrong WRITETIME/TTL for the whole SSTable. **DECIDED (owner, 2026-07-01): hard-fail `open`** — propagate the parse `Err`; no silent degraded mode | fixture: real SSTable dir with truncated `-Statistics.db` → `open` must `Err` (today: `Ok`); `WRITETIME()` of a known row must never be silent epoch | M |
| I4 | Silent `_ => Blob` fallback for 9 scalar types | `value_parsing.rs:480-483`: Float, Double, Timestamp, Varint, Decimal, Duration, Time, Inet, Timeuuid decode as raw `Blob` on the block path (`block_entries.rs:306,634` callers). Add the missing arms (reuse `parser/types` scalars); replace the catch-all with explicit `Err(unhandled type)` | per-type value-parity through the block path on a fixture with those columns → typed `Value` not `Blob` (verify reach; fails on main if path is hit); catch-all-removal compile guard | M |
| I5 | BTI DFS cap: bytes ≠ depth; no total-work bound | `DFS_MAX_DEPTH = 128` applied to accumulated key-path *bytes* (`traversal.rs:28,139`) — long text/blob clustering keys spuriously error; and path-length bounds don't cap total nodes visited on adversarial reconverging tries | synthetic Rows.db with a ~200-byte text clustering key → must decode (fails today with "corrupt trie"); adversarial reconverging/cyclic trie → `Err` within timeout | S |
| I6 | No-heuristics violation: hardcoded test-byte + length-prefix guess in Blob decode | `parser/types/mod.rs:182-203`: literal match on the 16-byte `[0x00..0x0F]` *test fixture* pattern + guessed 4-byte BE length prefix, not behind the experimental flag. Cold/public-API stack — policy violation regardless. Delete both; framed bytes are the blob verbatim (as `value_parsing.rs:90` already does) | adversarial test: the `00..0F` pattern as a framed non-blob value gets no special treatment (fails on main); proptest blob roundtrip | S |
| I7 | Small hardening bundle | depth guards for the recursive `ComparatorType` decoders (`value_parsing.rs:491-609`, `comparator_value_parsing.rs` — v5 path has one, these don't: adversarial nested type → stack overflow abort); duration `months/days` `as i32` truncation → `try_from` (`primitives.rs:147-148`); collection `Vec::new()` → clamped `with_capacity(min(count, cap))` (`value_parsing.rs:133,198`); clamp large `with_capacity` near `MAX_FROZEN_COLLECTION_SIZE` by remaining-bytes (`complex_column.rs:249+`) | 12-level nested comparator → `Err` not overflow; >i32 months → `Err`; huge-declared-count/short-buffer → `Err` with bounded peak alloc (dhat) | S/M |

---

### Epic J — One decoder: per-column dispatch + stack consolidation  `P1` (throughput headline)

Resolve type dispatch once per column, then make there be exactly one implementation per CQL type and one VInt decoder. H4's parity tests are the precondition; the 33-table golden harness is the invariant.

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| J1 | Per-column resolved dispatch — kill per-cell `to_lowercase()` and string ladders | extend `RowColumnResolution`/`ColumnToParse` with a precomputed `CellKind` enum (+ `is_complex`); per-cell path dispatches on the enum (jump table), string parse only at bind time. Kills: 2× `to_lowercase` per cell (`cell_value.rs:246`, `udt.rs:1188`), the 5×-copied string ladder (`raw_value.rs:148-210` et al.), `ComparatorType::from_data_type` per value (`value_parsing.rs:446`), per-value `type_name.clone()` (`value_parsing.rs:584`) | H5 counter: `to_lowercase` calls == 0 in the cell loop (≥2/cell on main); H2 dhat: dispatch allocs O(cols) not O(rows×cols); H2 wide-row bench floor; parity harness green | M/L |
| J2 | Collapse the three live `ComparatorType` decoders | `value_parsing.rs` + `comparator_value_parsing.rs` + `schema/parser.rs` are near-verbatim; merge into one module; `partition_key_codec` reuses its scalars; scalar arms route through the one canonical `parser/types` primitive codec (post-I6) | H4 parity tests pin equivalence before merge; malformed-input `Err` identical from all entry points (diverges on main); parity green | M |
| J3 | Delete the dead parser generations | `optimized_complex_types.rs` (631L), `zero_copy_parser.rs` (309L), legacy statistics subtree + `serialize_statistics` + `StatisticsAnalyzer` (~550L), `vint.rs` dead cluster (~350L), `parse_vint_binary`, the in-crate `benchmarks` harness that measures the dead stacks (or re-point it at live code), fix `collection_benchmarks.rs` cfg inconsistency (`mod.rs:122`) | unwired-symbol guard (every non-test parser module has ≥1 non-bench caller — fails on main for these); build + full gate green | S/M |
| J4 | One VInt decoder | canonical `decode_unsigned`/`decode_signed` pair mirroring `serialization/vint.rs` (single `leading_ones` computation, `split_at` not nom `take`, uniform `#[inline]`, no hardcoded single-byte match table, no fixed→zigzag double-decode fallback); delete `vint_fixed.rs`; `from_be_bytes` assembly replacing the lint-suppressed index loops | new `vint_decode` criterion bench + gate (none exists — fails at main's ns/op); proptest roundtrip vs `serialization::vint` encode for all u64/i64; corpus differential vs `parse_vuint` | M |
| J5 | ANTLR stub: remove or fail closed | `create_for_use_case(Development/Interactive)` and `strict_validation=true` hand out a parser whose every method returns "not implemented" (`factory.rs:52-81`, `antlr_backend.rs:15-19`); `is_backend_available` lies. Remove the backend + collapse the factory toward the single nom backend (schema parsing already bypasses it via `schema/cql_parser.rs`) | `create_for_use_case(uc).parse("CREATE TABLE …")` succeeds for all use-cases (fails on main for 2); `is_backend_available` truthful | S/M |

---

### Epic K — Row/cell hot loop mechanics  `P1`

The per-row constant-factor epic; K1 is also the drift-hazard fix.

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| K1 | `PartitionDriver`: one partition/row loop | extract the framing skeleton (header parse → EOP/marker checks → row decode → boundary peek → `ParseStep`/`flush_and_emitted!`) into one driver yielding decoded row tuples; the 5 emit fns (`block_emit.rs:79,291`; `block_emit_windowed.rs:236,794`; `compaction.rs:470`) become thin adapters; static-merge, the #932 tombstone/has_data_cell decision (4 copies today), and map-collapse become shared helpers | #932 coexistence fixture: user-scan and timestamps paths produce identical maps (lockstep guard); 33-table parity + compaction byte-parity green through the refactor | L |
| K2 | Non-allocating partition-boundary peek | per-row `peek_is_partition_header` runs full `parse_partition_header_full`: throwaway key `to_vec` (`row_framing.rs:627`) + eager `format!` `Err` as sentinel (`:584,609,622`) | H5 counter: key-sized allocs per wide partition == 1 not 1+rows (fails on main); parity green | S |
| K3 | Positional row emit — kill per-row HashMap + alphabetical sort | decoder emits ordered `Vec<(name, Value)>`/positional row from `columns_in_order` (deterministic by construction); deletes per-cell `column.name.clone()` (`row_data.rs:514`) and the 3×-copied per-row `sort_by` (`block_emit_windowed.rs:412-429` et al.). **Joint with read-path E2** (this is E2's producer side — one change, both epics) | H5: per-row sort invocations == 0, per-cell name clones == 0 (both nonzero on main); two-scan determinism without sort; parity green | M (L joint with E2) |
| K4 | Stop cloning identity per row | `Arc<RowKey>`/`Arc<TableId>` handles instead of 13 per-row/`format!`-per-partition clone sites (`block_emit_windowed.rs:445,86`; `block_emit.rs:87`) | dhat: partition-key allocs == 1 per partition not per row (fails on main) | S |
| K5 | Zero-copy value extraction | text/blob/varint/decimal copied out of the decompressed chunk per cell (`cell_value.rs:303,375`; `raw_value.rs`; `raw_type_value.rs`). `Bytes`-backed `Value` slicing the chunk (interacts with read-path E1 boxing + E3 window-as-`Bytes` — design together); interim S-win: `str::from_utf8(data)?.to_owned()` avoids the throwaway pre-validation `Vec` | H2 dhat: bytes-copied-into-values ~0 (borrowed/refcount) vs 1×payload today; text-heavy bench; parity green | L |
| K6 | Hardware-sympathy bundle (measure-first) | `simdutf8` for text validation (~2-4× on text-heavy, claim needs H2 bench); `smallvec` for per-row cell vectors; uniform `#[inline]` on retained decode primitives; skip `memchr` (length-prefixed format — no delimiter scans, honestly assessed) | H2 benches gate each claim; dhat allocs/row budget drops | M |

---

### Epic L — BTI parser: floor-walks and zero-alloc descent  `P1`

Deeper than read-path C3–C5 (which own Partitions.db whole-file copy, double walk, dead `bti/nodes.rs`). This epic owns Rows.db and node-decode mechanics.

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| L1 | Rows.db floor-walk — stop enumerating the whole partition | every clustering read DFS-materializes **all** row-index blocks then linearly filters (`rows.rs:337-342,494-536` → `bti.rs:333-388`); Cassandra does an O(key-length) `separatorFloor` trie walk (cited in CQLite's own doc comment, `rows.rs:481`). Add `rows_floor_block()`; walk forward from the floor for ranges; drop the duplicated `resolve_rows_db_entry` (resolved twice per read, result discarded at `bti.rs:341`) | H5: nodes-visited on a 1000-block partition < 40 not ~1000; dhat: allocs per clustering read independent of partition width (scales today); resolve-count == 1 (2 today); criterion clustering point/range benches on `test_da.wide_table` | L |
| L2 | Zero-clone DFS | `dfs_collect_in_order` clones the accumulated key `Vec` per child push and per emit (`traversal.rs:154,162`) = O(N·D) copies; single reusable path buffer with push/truncate, or visitor `FnMut(&[u8], T)` so offset-only callers never allocate keys | counting-allocator: DFS over N=500 trie does O(payloads) allocs not O(N·D); byte-identical output equivalence test | M |
| L3 | O(1) child lookup; parse nodes once | Dense/Sparse descent materializes and decodes the **entire** child table (up to 256 pointer decodes) to follow one byte (`node_decode.rs:157-275`, `partitions.rs:232-253`); payload-bearing nodes parsed twice per DFS visit (`traversal.rs:153,159`); `SizedPointer.size` computed per transition, used only by dead code (`node.rs:104-125`). Borrow-only `find_child_in_raw` (Sparse: binary-search bytes; Dense: index arithmetic) decoding the single matching pointer; single parse per node | H5: pointer decodes through Dense-256 == 1 not 256; parse count per offset == 1 not 2; zero-alloc descent (dhat); byte-parity vs pinned `test_da` offsets + `find_child` equivalence on all ordinals | M |
| L4 | Delete the dead, broken navigator stack | `PartitionsParser`/`RowsParser`/`BtiHeader`/`TrieNavigator` (~1000L, `reader.rs` + `node.rs:437-487`) have zero production callers AND `navigate_to_child` adds an absolute child offset to the current offset (`node.rs:459-461`) — wrong seeks for anyone who wires it later; its tests pass only via a root-payload shortcut | pre-delete regression test documenting the bug (2-level lookup fails today); post-delete: symbols gone, `issue_832`/`issue_909` suites green | M |

---

### Epic M — Metadata honesty + structural hygiene  `P2`

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| M1 | Option-ify fabricated stats placeholders | `enhanced_statistics_parser/mod.rs:225-263` sets `max_timestamp = min_timestamp`, all-zero table/partition/compression stats, `algorithm: "unknown"` — type-indistinguishable from real values; one caller away from an I3-class bug. Convert never-parsed fields to `Option<T>`/remove, mirroring `RepairField::Unparsed` (adjacent: #1352) | asserts `max_timestamp` is `None` not `== min_timestamp` on a real fixture (fails on main) | M |
| M2 | Campsite splits on the files this audit touches | worst offenders (source >800L): `row_framing.rs` 2353 (seams: partition_header/row_meta/clustering/range_marker/tests), `types.rs` 1976, `complex_column.rs` 1984, `repair_metadata.rs` 1832 (+ rename: "repair" reliably false-alarms no-heuristics reviews — it decodes persisted repair *state*), `udt.rs` 1749, `statistics.rs` 1637 (post-J3 deletion largely solves), `value_parsing.rs` 1536, `serialization/types.rs` 1424, `header.rs` 1366, `vint.rs` 1192 (post-J3/J4 solves), `block_entries.rs` 1229, + BTI quartet ~1000 each. Split per #1116/#1135 **as J/K/L touch them**, not as a standalone sweep | existing suites pin behavior across splits; file-size ratchet stops regrowth | M (amortized) |
| M3 | Rename `v5_compressed_legacy` | it is THE live decode engine for BIG **and** BTI (`data_access/mod.rs:332`, `bti.rs:715`, compaction, delta-scan, streaming — nothing dead); the name invites "legacy = ignorable" mistakes. Rename module to `row_decoder` (keep the on-disk `DataFormat::V5CompressedLegacy` label) | pure rename; build + parity green | S |
| M4 | Hygiene bundle | prune stale pre-`na` "Versions 1-3" narrative (`statistics.rs:279-320`); cold-open metadata parse cost into read-path A5's bench (incl. the repeated TOC walk, `enhanced_statistics_parser/mod.rs:187,345`); purge the 171 inert macOS `._*.rs` AppleDouble files under `cqlite-core/src/` (known tar gotcha) | A5 bench entries; `find -name '._*'` clean | S |

---

## NEEDS-YOU (product decisions)

1. **I3 — corrupt `Statistics.db` open semantics: DECIDED 2026-07-01 — hard-fail `open()`.** Matches the no-heuristics posture; a file whose authoritative metadata can't be trusted doesn't serve data.
2. **Joint scheduling with the read-path epics (open):** K3 (positional rows) is the producer side of read-path E2, and K5 (zero-copy values) interlocks with E1/E3 — these should be designed as single changes owned by one epic each, and the two audit programs sequenced together (recommendation: H+A land first as one measurement wave; I is independent and immediate).

## Test-infrastructure summary (the TDD backbone)

New machinery this program stands up, in dependency order: **fuzz crate** (H1) → **per-type decode + alloc-budget benches gated in perf-gate.json** (H2) → **struct-size pins** (H3) → **dispatch/codec parity nets** (H4) → **work counters** (H5). Every epic's child issues assert against these; the 33-table sstabledump JSONL harness and the compaction byte-parity harness remain the end-to-end invariant that must stay green through every refactor. Verification-first rule: I1 (VInt corpus differential) and I4 (blob-fallback reach) ship their *measuring* test before their fix, per the regression-test-verification doctrine.
