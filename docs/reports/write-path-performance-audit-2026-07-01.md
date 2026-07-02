# CQLite Write-Path Performance Audit — 2026-07-01

**Goal:** even, predictable write-side behavior under high load — ingest/flush/compaction that never stalls readers, never spikes memory data-dependently, and produces optimal files for the read path — judged against Rust best practices, LSM engine best practices (group commit, write amplification, backpressure, incremental compaction), and hardware sympathy (sequential I/O, buffer reuse, fsync discipline, allocation behavior).
**Scope:** `cqlite-core/src/storage/write_engine/**` (WAL, memtable, mutation, merge/, compaction, maintenance, cql_to_mutation/, export, merge_policy, reconcile_rules, stats) and `cqlite-core/src/storage/sstable/writer/**` (data_writer/, stats_writer/, partitions_writer, index_writer, summary_writer, filter_writer, compression_info_writer, compressed_data_writer) — ~58k lines incl. tests. **Out of scope:** read path (epics A–G), parser/decode (epics H–M), bindings internals, correctness-vs-Cassandra parity (owned by the July 2026 parity audit, epics #1378–#1381 incl. P0s #1382/#1390/#1391; #1396/#1406/#1388 now carry owner-approved postures). Read-side merge is D3/D4's; this audit owns the write-engine merge used by compaction/flush. `storage/serialization/vint.rs` verified exemplary by the parser audit — untouched here.
**Method:** six parallel read-only specialist audits (a: WAL/memtable/ingest, b: merge/compaction, c: data writers, d: index/metadata writers, e: cql_to_mutation/export, f: cross-cutting wiring/panic/async/bench), every finding carrying `file:line` evidence and rg-traced wired/dead status, plus lead-level cross-verification of all 12 correctness-class or surprising claims directly against source (all confirmed).

---

## Executive summary

The write path is **architecturally sound and unit-correct**: source-level streaming compaction with bounded channels (#827) is real, flush memory is bounded to one partition (#492), Data.db is strictly sequential append, finalize is crash-safe with a TOC publication barrier, bloom/stats/Index.db writers are exemplary single-pass designs, and the entire ~30k-line non-test surface contains exactly **one** library `unwrap()` (1,692 raw grep hits collapse to 1 real violation). The compaction byte-parity harness (#921) and the sstabledump JSONL goldens make every fix below verifiable as byte-identical.

The systemic problems, ranked:

1. **Wiring gaps make documented capabilities silently inert.** Policy-driven STCS compaction is a **silent no-op from every public surface**: `WriteEngine::new` sets `merge_policy: None` (`write_engine/mod.rs:475`), `maintenance_step` early-returns an empty report on `None` (`maintenance.rs:221-228`), and `set_merge_policy` has **zero non-test callers**. The CLAUDE.md-documented `maintenance --budget-ms` CLI command and the Python `maintenance_step` binding run, emit telemetry, and merge nothing, forever. Similarly the entire compressed-write stack (~1,100L) is built-but-unwired (aligned with #1406's approved "wire later" posture), and `WriteAheadLog::rotate()` is dead public API carrying the one library `unwrap()`.
2. **Backpressure degrades to a failure cliff under the bindings.** Sync `write()` only auto-flushes when *no* Tokio runtime is present (`mod.rs:560-563`) — but Node/Python route DML through the sync path inside `spawn_blocking`, where a runtime handle **is** present. Under sustained binding load the memtable grows past the 64MB threshold (warn-spamming per write), hits the 256MB hard limit, and **every write errors** until a manual flush. The bounded-admission design is good; the flush trigger just never fires on the path that matters.
3. **Allocation tax on every row, cell, and merge entry.** Every `MergeEntry` is deep-cloned **twice** on pure-waste paths in the k-way merge (`merge/mod.rs:2141,2177` — `advance()` already returns owned); `serialize_value` allocates a fresh Vec per cell (1 malloc + 2 copies for a 4-byte int); every row builds a throwaway body Vec + a gratuitous size-VInt Vec; the schema-constant column ordering is recomputed up to 3× **per row** with `to_lowercase()` inside the sort comparator (O(R·C log C) String allocs); set/map ordering re-serializes elements per comparison; `CqlType::parse` re-parses static schema type strings per column per statement.
4. **Two data-dependent cliffs: the widest partition owns your memory and your budget.** Compaction materializes one whole partition 4–5× simultaneously (`step()` → Vec → BTreeMap → HashMap → Vec → Vec), so one fat partition blows the 128MB target that per-source streaming otherwise protects. And `--budget-ms` is only checked *between* partitions (`maintenance.rs:307`) — a giant partition overshoots unboundedly, and dropped-column tables run a **full unbudgeted merge pre-pass** (`compaction.rs:149`) before the budget clock starts.
5. **fsync discipline has no group commit.** `BEGIN BATCH` of N statements = N sequential fsyncs (`mod.rs:709-715` + `mod.rs:531-534`); at 1–10ms/fsync that is 0.1–1s per 100-row batch. The 4KB WAL BufWriter never spans records under `SyncEachWrite`. WAL replay materializes the whole log as `Vec<Mutation>` (2× peak memory on recovery); flush deep-copies the entire memtable and walks the BTreeMap 4× (two are pure `.count()`); `finish()` re-reads the whole finished Data.db from disk **twice** for checksums that could be computed incrementally.
6. **Predictability leaks.** STCS bucket selection depends on **HashMap iteration order** (`merge_policy.rs:162,171,261`) — which tier compacts is random per run. `block_on_async` constructs a fresh Tokio runtime (plus a scoped thread) on **every** flush/maintenance call. The write-bench suite is structurally blind to compaction (zero benches on the heaviest CPU in the write path), wide partitions, jumbo values, and same-table overload.

### Correctness landmines (called out separately)

| # | Landmine | Evidence | Status |
|---|----------|----------|--------|
| L1 | **STCS maintenance is a public-surface no-op** — CLI `maintenance` + Python `maintenance_step` merge nothing (L0 grows unbounded) | `mod.rs:475`, `maintenance.rs:221-228`, rg: `set_merge_policy` test-only | CONFIRMED (lead-verified) |
| L2 | **Auto-flush cliff**: binding writes error at the 256MB hard limit instead of flushing (runtime-presence check defeats the trigger on the spawn_blocking path) | `mod.rs:553-563`, `bindings/node/src/database.rs:709-724`, `bindings/python/src/write.rs:188` | CONFIRMED (lead-verified) |
| L3 | **BTI export reports 0 partitions**: `count_index_entries` understands only BIG framing; failure swallowed by `unwrap_or_else` | `export.rs:640,667-728` | CONFIRMED (lead-verified) |
| L4 | The one library `unwrap()`: `wal.rs:886` in (dead) `rotate()` | `wal.rs:884-887`; rotate callers test-only | CONFIRMED |
| L5 | A single jumbo/deeply-nested mutation overshoots the memtable hard limit (admission checks pre-insert; depth-≥32 estimate flat 1024B) | `memtable.rs:180-183`, `mod.rs:522-528` | CONFIRMED |
| L6 | Doc honesty: `gc_before_secs` comment claims purge "carried but unused" — it is wired and load-bearing (#845); `now_secs` is the genuinely dead field (#848 unimplemented); `delta_helpers.rs:249` `unreachable!` in library code (guarded) | `merge/mod.rs:1080-1085,2243`, `delta_helpers.rs:249` | CONFIRMED |

---

## What is already good (verified, keep and protect)

- **Source-level streaming compaction is real** (#754/#827): per-input producer threads, sliding-window decode, bounded `sync_channel(256)` backpressure (`merge/mod.rs:439,468,563`); dhat test pins a 128MiB bound. Per-source content is never fully resident.
- **Flush memory bounded to one partition** (#492): `with_sink` builds each partition, flushes through a 1MiB BufWriter, clears (`data_writer/mod.rs:167-298`). Data.db is strictly append-only — zero seeks.
- **Crash-safe finalize** (`compaction.rs:268-525`): TOC renamed last as publication barrier, inputs deleted only after all renames, rollback on partial rename, buffered inputs so delete can't SIGBUS.
- **Bounded admission**: hard-limit gate rejects rather than OOMs (`mod.rs:522-528`); memtable size estimate counts real blob/text/collection bytes with a recursion depth cap.
- **fsync is real and instrumented**: BufWriter flush + `sync_all()` with a latency histogram on the durable step only (`wal.rs:680-698`); directory metadata synced on create/rotate.
- **`block_on_async` is panic-safe** for both runtime flavors (#587, `merge/mod.rs:356-382`); Node wraps sync fsync-bearing calls in `spawn_blocking`.
- **Overlap-safe tombstone GC** (#921/#935): purge decisions ride the reconcile pass per-cell; no extra scan. Reconcile kernel (#945) is well-decomposed with a documented fixed step order.
- **Index.db streaming/counting modes are O(1 entry)** (#753/#908) with a byte-identity anchor test — the reference design for the BTI trie writer fix. Bloom sizing/allocation correct, up-front, single hash per insert. Statistics accumulate streaming with a bounded 100-bin histogram. CRC/digest hashing streams in 64KiB chunks.
- **Panic hygiene**: 1,692 raw unwrap/panic hits → 1 library violation (L4). No `unsafe` anywhere in scope.
- **Export reuses the real flush** then file-copies (no materialization); BATCH parses once (no per-row re-parse); write-stats are plain fields under `&mut self` — contention-free.
- **AppleDouble `._*.rs` junk files**: gitignored (`.gitignore:19`), 0-length, inert to rustc/fmt/clippy/ratchet.
- **`write/ingest_wal_on` "flakiness" diagnosed and already correctly handled**: it is `advisory` in `perf-gate.json` (cannot fail CI). The noise is inherent — the bench wall-clocks 256 fsyncs, and fsync latency on shared CI storage is unbounded/neighbor-dependent. Bench-design property, not a regression signal. Keep advisory; do not tighten; `ingest_wal_off` is the stable CPU probe.

---

## Proposed epics (N onward; A–G read path, H–M parser are taken)

New test machinery **reuses parser-audit Epic H infrastructure**: the fuzz crate (H1), the alloc-budget dhat lane (H2), struct-size pins (H3), and the work-counter pattern (H5). Where an issue below says "work counter" or "alloc budget," it extends those lanes rather than standing up parallel machinery. The #921 compaction byte-parity harness + 33-table sstabledump JSONL goldens are the end-to-end invariants for every refactor.

### Epic N — Wiring + landmines: make documented write capabilities real  `P0`

The write path's worst problems are not slow code — they are built capabilities that silently don't run.

| # | Issue | Detail | Tests (TDD — fail on main) | Effort |
|---|-------|--------|---------------------------|--------|
| N1 | Wire STCS policy to the public surface (L1) | Install a default `STCSPolicy` in `WriteEngine::new` (or a `WriteConfig`/CLI/binding knob per the NEEDS-YOU decision); kill the silent no-op branch or make "no policy" loud | open engine via public ctor, flush ≥ min_threshold SSTables, `maintenance_step(large)` → assert `rows_merged > 0` + L0 drops (returns 0 today) | S–M |
| N2 | Fix the auto-flush cliff (L2) | Route binding writes through `write_async` (preferred) or drive `flush_internal()` via `block_on_async` when over threshold regardless of runtime presence; rate-limit the over-threshold warn | `#[tokio::test]`, tiny flush threshold, sync `write()` loop past threshold from inside runtime → assert `generation() > 1` (stays 1 today) | S–M |
| N3 | `wal.rs:886` unwrap + `rotate()` disposition (L4) | Replace unwrap with `Result`; decide wire-or-delete for the dead `rotate()` API (recommend delete) | guard test: no `.unwrap()` in wal.rs library region (1 hit today) | S |
| N4 | BTI export partition count (L3) | Derive counts from the read-path Statistics/index readers (or from the writer that just flushed) instead of the hand-rolled BIG-only byte-reader; kills the duplicated parser too | export a flushed `da` table → assert `partition_count > 0` (0 today) | M |
| N5 | Single-mutation admission overshoot (L5) | Check `size + estimate(mutation) > hard_limit` pre-admission; cap max mutation size; bound the depth-capped estimate | insert one mutation with real size ≫ hard_limit but small estimate → assert `Err` (admitted today) | S–M |
| N6 | Doc/code honesty sweep (L6) | Fix `gc_before_secs` stale comment; drop or implement `now_secs` (#848 note); `unreachable!` → `Err` in delta_helpers | in-module test: bad operator returns `Err` not panic; comment/field asserts via review | S |

### Epic O — Write measurement: benches + overload safety net  `P0` (reuses parser Epic H)

The heaviest CPU in the write path (the k-way merge) has zero bench coverage, and no bench exercises wide partitions, jumbo values, or same-table contention.

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| O1 | Compaction/merge bench, gated | criterion: N-generation compaction rows/sec (narrow + wide + tombstone-heavy shapes); add to `perf-gate.json` (strict for CPU shapes) | the bench + threshold; blind spot today | M |
| O2 | Wide-partition + jumbo-value benches | flush + compaction with one fat partition and giant blob/collection cells; pins the L5/Q5 memory cliffs | dhat peak-heap budget fails on main for the wide-partition compaction case | M |
| O3 | Overload/backpressure test | sustained ingest during flush + compaction on the **same** engine/table (read_while_write deliberately isolates engines — blind spot); assert bounded memory + no error-cliff (with N2) | fails on main via L2 error cliff | M |
| O4 | Write-path work counters + alloc budgets | extend H5 counters/H2 dhat lane: `MergeEntry` clones, `serialize_value` allocs, `to_lowercase` calls, murmur3 calls, WAL syncs | each asserts a number that is wrong on main (F-tables below) | S–M |
| O5 | Keep `ingest_wal_on` advisory | diagnosis: fsync-variance-bound by design; document in benches/README so it stops being re-litigated | n/a (doc) | S |

### Epic P — fsync discipline + ingest/flush mechanics  `P1`

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| P1 | Group commit for BATCH | append all batch mutations, `wal.sync()` once (`mod.rs:709-715`); optional general time/size-window group commit later | WAL sync counter: 8-statement batch → assert 1 sync (8 today) | S (batch) / M (general) |
| P2 | WAL append scratch reuse + PK fast path | `bincode::serialize_into` a persistent scratch (`wal.rs:642`); single-component PK returns the serialized Vec directly (`mutation.rs:434-439`) | alloc budget: ≤K allocs per `write()` (4–6 today) | S |
| P3 | Streaming WAL replay | `replay_each(FnMut)` decode-insert-drop + reusable read buffer, replacing `Vec<Mutation>` materialization (`wal.rs:719-810`) — perf sibling of #1390/#1391, not a re-file | dhat: reopen peak < memtable + slack (~2× today) | M |
| P4 | Flush without deep copy | drain memtable by value (`mem::take`) instead of `key.clone()` + `mutations.to_vec()` (`mod.rs:825`); O(1) `partition_count()` replaces two `iter().count()` walks (`mod.rs:788,794`) | dhat: flush peak < memtable×1.5 (~2× today); count-accessor unit test | S+M |
| P5 | Incremental checksums at finish | feed whole-file + chunk-aligned `crc32fast::Hasher`s during streaming; `finish()` stops re-reading Data.db twice (`finish.rs:160,185`) — preserve #1222 trailer semantics exactly | counting reader: finish() does 0 extra full reads (2 today); CRC.db/Digest goldens byte-identical | L |

### Epic Q — Merge/compaction: allocation, evenness, budget honesty  `P1`

All byte-parity-gated by the #921 harness.

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| Q1 | Kill the MergeEntry double clone | `refill_heap`: move from `advance()` instead of peek+clone+advance (`merge/mod.rs:2177`); `step`: read Copy `run_index` before pushing owned entry (`:2141`) | clone counter: ≤N+heap clones for N rows (≥2N today) | S |
| Q2 | Reconcile micro-allocs | `entry()` API in `resolve_cell_winners` (double hash + double key-clone today, `reconcile.rs:185-189`); compute `had_data_before` first then `mem::take` survivors (`reconcile.rs:344-361`) | CellData clone counter | S |
| Q3 | Deterministic STCS selection | buckets → sorted Vec, smallest-eligible-tier-first (Cassandra-like), deterministic fit-search (`merge_policy.rs:162-272`) | two eligible buckets → same (smaller) pick every run (varies today) | S |
| Q4 | Budget honesty | document the real contract (bounds *between* partitions); move the dropped-column survivor pre-pass (`compaction.rs:149`) inside the budget; mid-partition bounding rides on Q5 | wide-partition `maintenance_step(50ms)` overshoot test; dropped-column first-step budget test | M |
| Q5 | Within-partition streaming merge | `step()` yields clustering-group increments instead of materializing whole partitions 4–5× (`mod.rs:2119-2301`); makes peak ≈ max_row×k and enables true mid-partition budget checks | dhat: compaction with one over-budget partition stays <128MiB (fails today); #921 harness as gate | L |
| Q6 | Range-shadowing binary search | coalesced ranges are sorted+disjoint — binary search per CK instead of linear scan (`mod.rs:2914-2934`) | work counter: O(rows+ranges) not O(rows×ranges) | M (low pri) |
| Q7 | Cache the runtime in `block_on_async` | long-lived (or current_thread) runtime on the engine instead of `Runtime::new()` + scoped thread per flush/maintenance call (`merge/mod.rs:367,377`) | counter: `Runtime::new` ≤1 across N flushes; `write/flush` bench delta | M |
| Q8 | Compaction CPU contract | document "CPU-bound; `spawn_blocking` from async" on `compact_sstables`/`maintenance_step` (or provide a `_blocking` wrapper) | doc + doctest | S |

### Epic R — Serializer + conversion allocation discipline  `P1`

All byte-identical; golden equality as the invariant on every issue.

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| R1 | `serialize_value_into(&mut Vec)` | kill the fresh-Vec-per-cell + double copy (`encoding.rs:162-307`, ~8 call sites); thin `serialize_value` wrapper stays | alloc counter: 1-row×N-int-col write allocs ≤K constant (scales with N today) | M |
| R2 | Row scratch buffer; delete `row_size_buf` | encode the size VInt directly into `self.buffer`; reusable `row_scratch` replaces per-row body Vec (`rows.rs:832-847`, static + empty-static paths) | alloc counter: 2/row → ~0/row | S+M |
| R3 | Cache ordered columns + per-column dispatch | compute ordered regular/static column lists once per writer (schema is fixed); precompute `is_complex`/kind enum so `to_lowercase` never runs per row (`rows.rs:1143-1158`, `encoding.rs:9-11`, `schema_helpers.rs:15`) | `to_lowercase`/`is_complex_column` call counter: O(C) not O(R·C·logC) | M |
| R4 | Decorate-sort for set/map ordering | serialize each element once, compare precomputed bytes, reuse for the write — template already in-repo at `udt_canon.rs:366-397` (`collection_order/mod.rs:121`, `complex.rs:563-625`) | serialize-call counter for `set<text>` N=64: ~N not ~N·logN | M |
| R5 | Stop cloning `StatisticsMetadata` per partition | pass the 3 Copy baseline scalars the writer actually reads (`mod.rs:741`) | alloc counter on many-small-partition write | S |
| R6 | Cache parsed `CqlType` + ordered keys on the schema | `CqlType::parse` per column per statement (≥2 `to_lowercase` allocs each) → parse once at schema load (`builders.rs:99-339`, `delta_helpers.rs:232-307`); pre-sort key lists (`schema/mod.rs:671-682`) | parse-call counter: ≤columns for a 100-row batch (100×columns today) | M |

### Epic S — Metadata writers: streaming BTI + single hash  `P1/P2`

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| S1 | Drop interior raw_key retention | only first/last raw keys are used; interior `raw_key: Vec` copies are pure waste (`partitions_writer.rs:112,165,211-218`) | `retained_raw_key_bytes()` ≤ first+last (~1.6MB for 100k keys today) | S |
| S2 | Incremental BTI trie emission | keys are fixed-length + pre-sorted: single sweep with a depth-≤9 pending stack emits identical bytes; removes the nested-BTreeMap tree (`partitions_writer.rs:280-308`) | dhat peak budget over 1M partitions; `da` fixture byte-identity | M |
| S3 | Stream Partitions.db/Rows.db to a sink | mirror `IndexWriter::with_sink`; removes the whole-output Vec + `RowsTrieWriter` finish-queueing (`partitions_writer.rs:224,683-780`, `bti_state.rs:144-152`) | dhat budget; byte-identity | L |
| S4 | One murmur3 per partition key | fold trie-token (h1) + filter-byte (h2) into one `cassandra_murmur3_x64_128` call (`partitions_writer.rs:158-167,273-275`); optionally plumb into bloom (3 hashes → 1) | hash-call counter: N not 2N (3N with bloom) | S (fold) / M (bloom) |
| S5 | `CompressedDataWriter` → sink shape **before** wiring | O(file) `output` Vec (`compressed_data_writer.rs:247,375-390`) must become chunk-to-sink before #1406's "wire later" lands; sequencing note, unwired today | dhat budget vs a sink API (would retain whole file today) | M |
| S6 | Summary last-key clone churn | `last_key = Some(key.clone())` per partition (`summary_writer.rs:205`) → lazy | alloc counter: ≤2 clones for N partitions | S |

### Epic T — Campsite: the 12k merge split + write-path hygiene  `P2` (enabling)

| # | Issue | Detail | Tests (TDD) | Effort |
|---|-------|--------|-------------|--------|
| T1 | Extract merge tests | `merge/mod.rs` lines 3538–12071 (~8.5k) → `merge/tests/*` by `issue_NNN` scenario per #1135 | tests keep passing; ratchet clears | M |
| T2 | Split merge production (~3.5k) into ~9 modules | seams already mapped: `run_reader` / `adapter` / `baselines` / `schema_plan` / `entry_point` / `merger` / `partition` / `ranges` / `emit` (line ranges in auditor report); **#921 byte-parity harness is the acceptance gate**; every module lands <800L | byte-parity harness green per step | M–L |
| T3 | Remaining write-path splits | `writer/mod.rs` 2475 / `write_engine/mod.rs` 2072 / `builders.rs` 1987 / `mutation.rs` 1772 / `wal.rs` 1682 / `partitions_writer` 1579 / `maintenance` 1545 / `index_writer` 1514 / `udt_canon` 1392 / `scenarios_6.rs` 1705 (test) — mostly test-extraction per #1135, then concern splits per #1116 | ratchet + gate | M (spread) |

---

## Priority matrix (deduplicated headline findings)

| Rank | Finding | Epic | Type |
|------|---------|------|------|
| 1 | STCS maintenance silent no-op from every public surface | N1 | wiring landmine |
| 2 | Auto-flush cliff: binding writes error at hard limit under load | N2 | landmine |
| 3 | No compaction bench; no wide/overload coverage | O1–O3 | measurement |
| 4 | MergeEntry cloned 2× per row in the merge core | Q1 | alloc (S-effort, big win) |
| 5 | BATCH = N fsyncs (no group commit) | P1 | fsync discipline |
| 6 | Whole-partition materialization ×4–5 in compaction (memory cliff) | Q5 | predictability |
| 7 | Budget only honest between partitions + unbudgeted pre-pass | Q4/Q5 | predictability |
| 8 | Per-cell/per-row serializer alloc churn (fresh Vec per value, per-row ordering recompute) | R1–R3 | alloc |
| 9 | BTI trie 3×O(partitions) build-then-serialize + 3× murmur3 per key | S1–S4 | memory/CPU |
| 10 | finish() re-reads Data.db 2×; flush deep-copies memtable; replay materializes WAL | P3–P5 | I/O + memory |
| 11 | Nondeterministic STCS bucket pick; per-call Runtime::new | Q3/Q7 | predictability |
| 12 | merge/mod.rs 12,071L | T1/T2 | hygiene (enabling) |

## Product decisions (DECIDED by owner, 2026-07-01)

1. **Default compaction policy (N1): DECIDED — default-on STCS.** Install `STCSPolicy` as the default in `WriteEngine::new`, with a config off-switch. Makes the documented `maintenance` contract real; matches Cassandra semantics.
2. **Backpressure model for binding writes (N2): DECIDED — route Node/Python writes through `write_async`.** Real async flush on the binding path; removes the error cliff without inline-flush latency surprises on the sync path.
3. **`WriteAheadLog::rotate()` disposition (N3): DECIDED — delete the dead API** (and its `unwrap()` with it). It can return via VCS when a rotation feature actually lands.

## Suggested delivery order

N (landmines, mostly S) → O (measurement, so every later fix lands with a red-then-green number) → Q1/P1/P2/R-quick-wins (S-effort alloc/fsync wins) → P3–P5, R1–R4, S1–S4 (M) → Q5 + S3 + P5 (the L-effort streaming refactors, each behind its dhat budget + byte-parity gate) → T (splits, interleavable — T1 any time).

## Test-infrastructure summary (the TDD backbone)

This audit stands up **no parallel machinery**: it extends parser Epic H — work counters (H5 pattern: MergeEntry clones, serialize calls, `to_lowercase` calls, murmur3 calls, WAL syncs, Runtime::new), the dhat alloc-budget lane (H2: flush peak, replay peak, compaction-with-fat-partition peak, BTI build peak), and criterion+perf-gate.json (new: compaction rows/sec, wide-partition, overload; existing `write/*` gates unchanged, `ingest_wal_on` stays advisory by diagnosis). End-to-end invariants for every refactor: the #921 compaction byte-parity harness, the 33-table sstabledump JSONL goldens, and the CRC.db/Digest goldens (#1190/#1017/#1222) for P5. Verification-first rule: N1 (no-op maintenance), N2 (flush cliff), and L5 (admission overshoot) ship their measuring test before their fix, per the regression-test-verification doctrine.
