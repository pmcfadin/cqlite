# Cursor-Compaction Findings — Applicability to CQLite

> **Living document.** Created 2026-06-17. This is a working ledger, not a finished
> spec: update the status columns and add rows as we verify findings against CQLite's
> code, fix divergences, or import format detail into the definitive guide. When you
> change a status, leave the date and a one-line note.
>
> **Scope.** It distills the Apache Cassandra "garbage-free cursor compaction" design
> journal (the sibling docs in this folder) into the subset that matters for CQLite —
> a *Rust reimplementation of Cassandra SSTable reading and writing* — and tracks what
> we should do about each item: fix it in compaction, verify it, or document it in
> `docs/sstables-definitive-guide/`.

---

## Source documents

All paths relative to this folder (`docs/garbage-free-compaction-improvements/`).
These are the upstream Cassandra (Java) design/verification docs Patrick collected.

| Doc | What it is |
|---|---|
| [`cursor-compaction-plan.md`](./cursor-compaction-plan.md) | The full ~2300-line implementation journal: cursor architecture, increments, merge semantics, and the numbered correctness findings (the "#N" references below). |
| [`bti-sstable-specification.md`](./bti-sstable-specification.md) | Byte-exact spec of a BTI sstable (Data.db + Partitions.db/Rows.db), what both compaction paths must produce identically, and where the format's invariants live. The most directly reusable doc for both compaction and the guide. |
| [`quick-perf-wins.md`](./quick-perf-wins.md) | Ten ranked low-risk JVM allocation wins. Mostly Java-specific; included for the allocation-gate methodology and the format details in the writeups. |
| [`improvements.md`](./improvements.md) | Aggressive backlog: cursor read path, off-heap memtable, SIMD/io_uring/GPU. Mostly JVM-specific or research moonshots; useful as directional context. |
| [`tombstone-histogram-spool-proposal.md`](./tombstone-histogram-spool-proposal.md) | Deep dive on `Statistics.db`'s `estimatedTombstoneDropTime` histogram (Ben-Haim/Tom-Tov streaming histogram, bin vs spool) + a lazy-allocation fix. Relevant to our `stats_writer.rs` determinism. |
| [`ffm-memtable-investigation.md`](./ffm-memtable-investigation.md), [`ffm-memtable-offheap-plan.md`](./ffm-memtable-offheap-plan.md) | Off-heap arena memtable on Java FFM. JVM-specific; directional only for CQLite. |
| `jfr-reports/` | JFR allocation profiles backing the spool proposal. Java-only evidence. |
| `*-jira-draft.md`, `test-failure-investigation.md`, `ladder.sh` | Upstream process artifacts. Context only. |

### Provenance & trust caveat (read before importing anything into the public guide)

These docs describe an **internal, not-yet-upstreamed Cassandra branch**
(`cursor-compaction-completion`). The *on-disk format facts* are trustworthy — they are
verified byte-for-byte by Cassandra's differential harness — but:

- Cite **Cassandra source** as the authority (`UnfilteredSerializer`, `Cell.Serializer`,
  `SerializationHeader`, `Columns`, `BtiFormat.md`, `ByteComparable.md`), **not** this branch.
- Treat the numbered "findings" as *implementation gotchas*, not as the spec itself.
- The format claims are independently checkable against CQLite's own reader — verify, then
  document with our own citation. This keeps the guide honest about
  [the no-heuristics / authoritative-metadata mandate](https://pmcfadin.github.io/cqlite/agents-developing/no-heuristics/).

---

## Part 1 — Findings applicable to CQLite compaction

CQLite's compaction lives in `cqlite-core/src/storage/write_engine/` (M5.2, behind the
`write-support` feature):
- `merge.rs` — binary-heap k-way merge + per-clustering reconcile (`reconcile_cluster`).
- `merge_policy.rs` — STCS sstable selection.

This is architecturally Cassandra's *iterator path* (materialized objects), so the same
byte-level invariants apply. Status legend:

- **DIVERGENT** — CQLite's behavior is confirmed different from Cassandra; potential bug.
- **VERIFY** — plausibly applies; needs a code/behavior check.
- **HOLDS** — checked, CQLite already does the right thing.
- **WRITER** — only relevant once CQLite writes this byte sequence (some already do).
- **N/A** — not reachable in CQLite yet (e.g. counters, BTI output, MVs).

> **Fidelity bar decided 2026-06-19 (#818):** three-tier model —
> Tier 1 *Cassandra-readable* (validity; **shipped to live cluster**, so a real node's load
> path must accept it), Tier 2 *resolution-equivalent* (same survivors/values → same query
> answers), Tier 3 *byte-identical serialization is a NON-GOAL*. See
> [`compaction-fidelity-bar-decision.md`](./compaction-fidelity-bar-decision.md) for the
> per-finding fix-required vs document-only verdicts. The **Verdict (#818)** column below
> carries the #818 disposition for each finding: the *intended* ruling (`FIX` / `DOCUMENT` /
> `DEFER` / `N/A`) and, where the epic's verification work has since SETTLED that disposition,
> the resolved outcome (`HOLDS` = no fix needed, `FIXED` = the FIX has landed). **Status** is
> the independent verification state (`VERIFY` / `HOLDS` / `DIVERGENT` / …).

| # | Cassandra finding (source) | CQLite relevance | Status | Verdict (#818) |
|---|---|---|---|---|
| #4 / #21 | **Cell tie-break at equal timestamp**: `Cells.resolveRegular` keeps the cell whose **raw value bytes** are strictly greater (unsigned lexicographic compare on the *raw* value, skipping the vint length prefix), **not** file/run order. | `reconcile_cluster` (`merge.rs`) keeps **first-seen (lower `run_index` = newer file)** on a full tie (same ts, both live). Two live cells, same timestamp, different values → CQLite and Cassandra can pick different winners. | **DIVERGENT** — confirmed by reading + **verified by test** `test_real_merger_value_tiebreak_diverges_from_cassandra` (`merge.rs`, 2026-06-17): CQLite keeps `"apple"` (newer file), Cassandra would keep `"banana"` (greater raw bytes). | **FIX** (Tier 2) — ruled 2026-06-19; match Cassandra's greater-raw-value-wins. Convergence test replaces the divergence test when the fix lands. |
| #13 / #3 | **Tombstone beats expiring at equal timestamp**; `IS_EXPIRING` strictly means `ttl != NO_TTL` (mutually exclusive with `IS_DELETED`); don't emit `IS_DELETED|IS_EXPIRING` or a wasted TTL byte. | CQLite handles equal-ts cell-tombstone-beats-live (`is_cell_tombstone` in `reconcile_cluster`, `merge.rs` / Issue #498). Need to confirm the *expiring-vs-tombstone* ordering and the strict flag semantics on the **write** side. | **HOLDS** (2026-06-19, #822) — `reconcile_cluster` keeps a cell tombstone over an expiring (TTL) cell at equal ts regardless of run_index; writer keeps `CELL_IS_DELETED` / `CELL_IS_EXPIRING` mutually exclusive with no wasted TTL byte (`data_writer.rs:2404`/`:2535`). Tests `issue_822_merge_ordering_semantics::issue_13_*`, `issue_3_*`. (Note: the reader does not surface per-cell TTL into the merge — `value_to_row_data` sets `ttl: None` — so at merge level "tombstone beats expiring" is structurally "tombstone beats live"; strict flag exclusivity is enforced/asserted on the writer side.) | **HOLDS** — no fix needed. |
| #2 | **`previousUnfilteredSize`** is written to disk but skipped by readers; must equal the byte distance from the previous unfiltered including its own vint length; static rows hardcode 0 and don't advance the chain. Caught as 2601/5100 divergent bytes. | Applies to CQLite's `data_writer.rs` whenever it writes rows. If we write `0` or miscount, our Data.db diverges. | **DIVERGENT → FIXED** (2026-06-19, #821) — non-static rows were already correct (first row = header size; subsequent = previous row's full size). Static rows were wrong on both counts: they wrote the header size as their own `prev_size` (must be 0) and advanced the chain as the "previous unfiltered" (next row got the static row's size alone instead of header+static = its in-partition offset). Fixed in `data_writer.rs` (`write_partition` + `write_partition_with_index_blocks`). Anchored to real Cassandra `test_basic.uncompressed_table` (first row prev_size=30=header) and `static_columns_table` (static=0, regular=46=header30+static16). Test `issue_821_writer_byte_invariants.rs`. | **FIXED** — was Tier-3 best-effort; fixed cheaply (matches Cassandra), readers still skip it so no read-path change. |
| #10 | **DESC clustering ordering**: empty clustering values sort *after* valued ones; comparison must route empty-vs-valued through `type.isReversed()`. | `ClusteringKey::compare` (schema-aware) must honor reversed columns for empty/valued ordering. | **HOLDS** (2026-06-19, #822) — `ClusteringKey::compare` (`mutation.rs:377`) reverses `compare_values` for DESC columns; an empty clustering value (`Text("")`) correctly sorts after valued ones under DESC, both directly and in the live `merge_partition_rows` sort. Tests `issue_822_merge_ordering_semantics::issue_10_*`. (Caveat: verified for text/blob where empty bytes is the natural minimum; not exercised for a type whose minimum differs from the empty-byte sentinel.) | **HOLDS** — no fix needed. |
| #14 / #17 | **Complex-column (multi-cell) deletion**: shadow cells using the **un-purged** deletion value, output the **purged** value; active row/range deletion *strictly* supersedes an equal-timestamp complex deletion (drop on equality). | Reachable once CQLite merges multi-cell collections/UDTs in compaction. Confirm `reconcile_cluster` handles complex deletions at all. | **N/A-yet (not representable)** (2026-06-19, #823) — no per-column complex-deletion representation: `RowData::Tombstone` is whole-row only; a `CellTombstone` is per-cell keyed on column name. The row-vs-cell equality rule that *does* exist HOLDS (an equal-ts row deletion supersedes a cell tombstone via the `cell.timestamp > row_del` drop-on-equality filter, `merge.rs:1337`). gc_grace purge is not applied in-merge (`gc_before_secs`/`now_secs` carried but dead), so shadow-before-purge has no purge step yet. Test `issue_823_complex_column_merge::row_deletion_supersedes_equal_ts_cell_tombstone`. | **FIX** (Tier 2) — blocked: needs per-path complex-deletion model first. |
| #18 | **Complex-column merge is path-ordered** using the column's *path comparator* — UDT field paths use **signed** `ShortType`, list paths use TimeUUID, map paths use the key type — **not** plain unsigned bytes. | Same multi-cell merge path as #14. Easy to get wrong by comparing raw bytes. | **N/A-yet (not representable)** (2026-06-19, #823) — `CellData` (`merge.rs:188`) has no `cell_path`/collection-key; `reconcile_cluster` keys winners on column name only and collections/UDTs enter the merge as one nested `Value` per column (`value_to_row_data`, `merge.rs:720`). Multi-cell elements sharing a column name collide (whole-column collapse), so per-path ordering (signed ShortType / TimeUUID / map-key comparator) is unreachable. Tests `issue_823_complex_column_merge::{multicell_collection_collapses_whole_column_not_per_path,nonfrozen_udt_collapses_whole_column_not_per_field}`. | **FIX pairing** (Tier 2) — blocked: needs a per-path cell model in `CellData` first. |
| #16 | **Index-block / partition offsets must be 64-bit** (`long`), not `int` — an `int` wraps negative past 2 GiB and corrupts every block offset. | CQLite should use `u64`/`i64` for in-partition and data-file offsets throughout the writer. | **HOLDS** (2026-06-19, #821) — every in-partition/data-file offset is already `u64`/`i64` (`DataWriter` positions, `IndexWriter` via `encode_unsigned(u64)`, `PromotedIndexBlock.offset/width: u64`, `header_length: u64`, `PartitionsTrieWriter.data_offset: u64` with an `i64::MAX` guard). Remaining `i32`/`u32` are Cassandra-mandated *format* fields, not data-file offsets (Index.db promoted-offset array, Summary.db sample `int[]`, DeletionTime `localDeletionTime`). Test `issue_821_writer_byte_invariants.rs` round-trips a >2 GiB offset through three encoders. | **HOLDS** — no fix needed (Tier 1 already satisfied). |
| #12 | **Column subset encoding** (sparse rows): `<64`-column superset → single unsigned-vint bitmap (set bit = *missing* column, value 0 reserved → use `HAS_ALL_COLUMNS`); `≥64` → "large subset" with an exact mode boundary and tail-column inclusion. | Applies to CQLite read *and* write of sparse rows. Read path likely exercised; write-side boundary needs care. | **DIVERGENT** (2026-06-19, #824) — the **writer** mode selection is correct (`<64` → single missing-bitmap VInt; `≥64` → large-subset count+index per Cassandra `Columns.serializeSubset`), but the **reader** (`row_decoder::parse_row_metadata`) reads the subset as a single VInt `u64` bitmap with NO large-subset branch, so it mis-parses any `≥64`-column **row that clears `HAS_ALL_COLUMNS`** (i.e. omits ≥1 column and thus carries a subset field); fully-populated rows in such a table carry no subset field and are unaffected (decode-critical for the sparse rows). Byte-level tests at 63/64/65 cols incl. tail inclusion in `issue_824_column_subset_and_filter.rs`. | **FIX correctness incl. 64-col boundary** (Tier 1/2) — reader large-subset decode branch is the follow-up fix. |
| #5 / #6 | **BTI block boundaries**: the tail block's width includes the end-of-partition marker byte; promote to a row index only when a partition spans **>1** block (count blocks *including* the tail); single-block → `trieRoot = -1`, partition entry points direct-to-data. | Only if CQLite emits **BTI** output during compaction. CQLite currently reads BTI (guide ch.17); write path is BIG-format. | **N/A-yet (BTI write)** | **DEFER** (overlaps epic #762). |
| #22 | Header `hasStatic` and column superset come from the **merged input sstable headers**, not current schema (a dropped static column still has static rows on disk). Dropped columns filtered per-source against per-column drop horizons. | CQLite is schema-driven; must read static-ness / column sets from the sstable `SerializationHeader`, not the supplied schema, when merging. | **DIVERGENT** (2026-06-19, #822) — the compaction writer derives `hasStatic` and the column set from the supplied `TableSchema` (`data_writer.rs:299`/`:492`); no `SerializationHeader` is read anywhere under `storage/write_engine/`. A dropped static column whose static rows still exist on disk would not be re-emitted. Pinned by `issue_822_merge_ordering_semantics::issue_22_static_decision_is_schema_driven_not_header_driven_divergent`. | **FIX** (Tier 2) — confirmed divergent; fix is follow-up writer work (read headers, not schema). |
| #23 | `Filter.db` may be `AlwaysPresentFilter` when `bloom_filter_fp_chance = 1.0` — guard the cast. | CQLite bloom handling on read; writer must reproduce. | **HOLDS (read, scan + get)** (2026-06-19, #824) — an absent/always-present `Filter.db` is read without crash and returns correct rows via BOTH `SSTableManager::scan` and point-lookup `SSTableManager::get` (`component_loading.rs` returns `Ok(None)`; bloom gates correctly skip on `None`). The verification surfaced and FIXED a real reader bug: `scan_for_key` did not seek the file handle to the data-section start before chunk-stitching, so a `get()` after a `scan()` stitched zero chunks and returned `None` for an existing partition (independent of `Filter.db`; the absent-filter case merely removed the bloom short-circuit that hid it). Positive tests `issue_824_column_subset_and_filter::{absent_filter_db_reads_without_crash, point_lookup_without_filter_db_returns_row}`. Write side cannot emit AlwaysPresentFilter (always writes `Filter.db` at fp 0.01; `FilterWriter::new` rejects `fp_chance == 1.0`). | **HOLDS (read)** — write-side AlwaysPresent reproduction optional. |
| #24 | TTL-expiry → tombstone conversion uses `nowInSec`, **overridden to `gcBefore` for Accord-enabled tables**. | Accord is out of scope for CQLite; note the general `nowInSec`/`gcBefore` purge rule. | **N/A (Accord) / context** | **N/A** |
| #25 | RT-marker / complex-deletion **body-size vint** corrupts for far-future deletions via a long-vs-`(int)` cast (upstream iterator has the *same* latent bug). | CQLite writer must size marker bodies in the long domain. | **WRITER / VERIFY** | **FIX** (Tier 1) |
| #26 | Counter merge: `CounterContext` shard merge, CASSANDRA-7346 tombstone supremacy, `Flag.LOCAL` clear, tombstone-value tie-break. | CQLite does not merge counters in compaction yet. | **N/A-yet (counters)** | **N/A-yet** |

> The plan reports **22 findings total**; not all are enumerated in the docs read here
> (some are referenced only by count). When working from `cursor-compaction-plan.md`
> directly, add any newly-surfaced finding as a row above with its CQLite assessment.

---

## Part 2 — Architecture & methodology lessons

These are design takeaways, independent of any single finding.

1. **Adopt a differential compaction gate for write output.** CQLite validates *reads* against
   `sstabledump`, but compaction *write* output has no differential gate — which is exactly
   where every Part 1 finding bites. The model: compact identical inputs with real Cassandra
   and with CQLite. Per the #818 fidelity bar, the **gate** is **logical merge equivalence**
   (Tier 2: identical surviving cell/value/tombstone tuples) **plus real-node load-path
   validity** (Tier 1, shipped-to-cluster). A raw-byte `diff` of `Data.db`/`Statistics.db` is
   retained as an **optional debug-only diagnostic** (Tier 3 byte-identity is a non-goal), not a
   pass/fail gate. Add a **two-generation** check (re-compact CQLite's own output) — the docs
   note write-side issues that only the *next* merge surfaces (#2).

2. **Cursor-native is the right Rust end-state.** Cassandra's lever — *flat bytes + reusable
   flyweights beat materialized objects* — is the natural Rust idiom (`&[u8]`, `slice::cmp`,
   no GC). `merge.rs` already separates heap-routing order from winner selection and uses peek
   buffers, so it is halfway there. CQLite can target the cursor model directly rather than
   retracing Cassandra's iterator→cursor migration, and it fits the <128 MB memory budget.

3. **Allocation gates are less load-bearing for us** (no GC), but the *determinism* discipline
   transfers: `Statistics.db` must be byte-stable for a given input
   ([`tombstone-histogram-spool-proposal.md`](./tombstone-histogram-spool-proposal.md),
   Option A is byte-identical; C/D change output bytes). Keep that property in `stats_writer.rs`.

4. **The findings are a free QA spec.** They are precisely the edge cases a k-way merge hits.
   Treat Part 1 as the regression checklist for M5.2 compaction.

---

## Part 3 — How this feeds `docs/sstables-definitive-guide/`

`bti-sstable-specification.md` is the standout for guide enrichment — byte-exact, with
gotchas annotated. Mapping to existing chapters (verify against CQLite's reader first, cite
Cassandra source, per the provenance caveat):

| Guide chapter | What to pull |
|---|---|
| `05-data-db-format.md` | BTI-spec §2: flags-byte + extended-flags tables, row body layout, `previousUnfilteredSize`, column-subset `<64`/`≥64` boundary, cell flags, complex-column layout, RT bound-vs-boundary. |
| `11-merging-tombstones-and-shadowing.md` | BTI-spec §5.2 + findings: cell reconciliation, raw-vs-wire bytes (#4/#21), tombstone>expiring (#13), complex-deletion shadow-before-purge (#14/#17), purge/GC, strict liveness. |
| `17-bti-formats.md` | BTI-spec §3: `Partitions.db`/`Rows.db` trie layout, byte-comparable keys, `TrieIndexEntry` serialization, separators, single-block `trieRoot=-1`, partition-index "last three longs" header. |
| `08-statistics-db.md` | Spool proposal: `estimatedTombstoneDropTime` histogram structure and consumers. |
| `15-compaction-strategies.md` | "Two pipelines, **resolution-equivalent** output" framing (per #818: gate on logical merge equivalence + real-node load-path validity; byte-identity is a Tier-3 non-goal, used only as a diagnostic); adaptive index granularity (#7) and clustering front-coding (#5 in `improvements.md`) as forward-looking sidebars. |
| `appendix-b-encodings-cheat-sheet.md` | Cell/unfiltered flag bits, subset bitmap, delta-encoding bases (`minTimestamp`/`minLocalDeletionTime`/`minTTL`). |
| `appendix-f-known-limitations.md` | The latent long-vs-`(int)` size-vint bug (#25), `AlwaysPresentFilter` cast (#23). |

### Import status (Epic #817 / #825 — VERIFY-THEN-IMPORT applied 2026-06-19)

- **ch.05 (Data.db): DONE** — `previousUnfilteredSize` (writer + static exception),
  64-bit offsets vs narrow format fields (#16), column-subset (set bit = MISSING,
  value 0 reserved, `<64`/`≥64` decode-critical boundary + reader gap noted) (#12),
  `IS_EXPIRING` strict & mutually exclusive with `IS_DELETED` (#13/#3).
- **ch.11 (Merging/shadowing): DONE** — tombstone-beats-live at equal ts (matches);
  #4/#21 raw-vs-wire value tie-break documented as a CQLite divergence (keeps
  first-seen); #14/#17/#18 whole-column-vs-per-path complex merge limitation; #10
  DESC empty-vs-valued clustering ordering.
- **ch.17 (BTI): DONE** — reader-verified footer root, position-sign convention,
  `Rows.db` `TrieIndexEntry` resolution, backward distances; BTI-write out of scope
  (epic #762). Single-block `trieRoot=-1` NOT imported (not verifiable in CQLite's
  reader).
- **ch.08 (Statistics.db): DONE (structure/serialization)** — `estimatedTombstoneDropTime`
  histogram structure, algorithm, and nb serialization documented (verified in `stats_writer.rs`).
  Full byte-stability/determinism is **not** claimed verified: serialization is order-stable for a
  given final bin set, but above-cap streaming merges are insertion-order-dependent — this remains
  an open determinism question (see "Further research questions" #8).
- **appendix-b: DONE** — `prev_size` table, 64-bit-offset vs narrow-field table,
  column-subset mode-boundary table, `IS_EXPIRING`/`IS_DELETED` strictness; corrected
  a stale u8 partition-key-length claim to u16.
- **appendix-f: DONE** — reader `≥64` large-subset gap (#12), whole-column complex
  merge (#14/#17/#18), #4/#21 tie-break divergence (ruled FIX, follow-up), latent
  RT/complex-deletion size-vint width (#25), AlwaysPresentFilter handled (scan + get,
  after the get-after-scan reader fix) (#23).

> Every imported claim is grounded in CQLite's own reader/writer source or a cited
> Cassandra class (`UnfilteredSerializer`, `Cell.Serializer`, `Columns.serializeSubset`,
> `Cells.resolveRegular`, `BtiFormat.md`, `ByteComparable.md`), never the internal branch docs.

---

## Part 4 — Recommended next actions (snapshot — keep current)

1. **[compaction] Reconcile finding #4/#21** — divergence is **verified** by
   `test_real_merger_value_tiebreak_diverges_from_cassandra` (`merge.rs`). **Decision made
   2026-06-19 (#818): FIX** — `reconcile_cluster` must match Cassandra's "larger raw value
   wins" (Tier 2, resolution-equivalence). When the fix lands, that test flips to a convergence
   test. #820 characterizes the tie-break across non-text types. *(Verified 2026-06-17; ruled
   FIX 2026-06-19.)*
2. **[compaction] Differential compaction gate — IMPLEMENTED (#819).**
   `cqlite-core/tests/issue_819_differential_compaction.rs` + playbook
   [`test-data/differential-compaction-harness.md`]. **Pass criterion (#818):** logical merge
   equivalence (Tier 2) + real-node load-path validity (Tier 1, shipped-to-cluster); raw-byte
   per-component diff is a debug-only secondary signal (Tier 3). Default run does a CQLite
   two-generation self-consistency check + Tier-1 validity + an independent-path fixture
   comparison; the live-Cassandra comparison is env-gated (`CQLITE_DIFFERENTIAL_CASSANDRA=1`,
   operator-supplied reference dir). **The harness surfaced two real, currently-open
   compaction-fidelity defects** (pinned as runnable `#[ignore]`d reproductions): (a) a
   clustering-table row tombstone is mis-decoded after compaction (resurfaces as a
   partition-level tombstone with the wrong deletion timestamp + framing corruption — the #2
   "only the next merge observes it" class); (b) a row carrying a cell tombstone has its live
   sibling cells rewritten with the row's max timestamp instead of the original write timestamp.
   Both are follow-up writer/reader fixes. *(Done — #819.)*
3. **[guide] Verify-then-import — DONE (#825).** ch.05/08/11/17 + appendices B/F enriched
   with verified detail (see Part 3 import-status). Each claim grounded in CQLite's reader/
   writer or a cited Cassandra class. *(Done — #825.)*

### Epic #817 status (2026-06-19)

All child issues complete: **#818** (fidelity bar), **#819** (differential harness),
**#820** (tie-break type coverage, prior), **#821** (writer byte-invariants; #2 fixed),
**#822** (merge ordering/semantics), **#823** (complex-column merge), **#824** (column subset
and AlwaysPresentFilter; get-after-scan reader bug fixed), **#825** (guide enrichment).
Follow-up (out of this epic's verify-and-document scope, tracked via the verdicts above):
implement the #4/#21 tie-break FIX, the #22 header-driven static decision, the #12 reader
`≥64` large-subset decode branch, the two harness-surfaced compaction defects (#819 a/b), and
the #14/#17/#18 per-path complex-column model.

---

## Further research questions (for new agents)

Pick one, investigate against CQLite's actual code/behavior, then update Part 1/Part 4 above
with findings. Prefer reproducing each as a test before claiming a status change (see
[regression-test verification doctrine](https://pmcfadin.github.io/cqlite/agents-developing/)).

> **Items 1–7 were investigated and resolved in Epic #817** (verified, with the verdicts
> recorded in Part 1). They remain listed for provenance, each marked ✅ RESOLVED with the
> remaining *implementation* follow-up (the epic's scope was verify-and-document, not fix).
> Items 8–10 are still open.

1. **Tie-break divergence (#4/#21).** ✅ RESOLVED (verified DIVERGENT; #818 ruled FIX).
   `test_real_merger_value_tiebreak_diverges_from_cassandra` reproduces it. **Follow-up:**
   (a) confirm on real corpora via the #819 harness; (b) non-text type coverage — see #820;
   (c) implement the fix and flip the reproduction test to a convergence test.
2. **`previousUnfilteredSize` (#2).** ✅ RESOLVED (#821) — non-static rows correct; static-row
   bug (header size + chain advance) FIXED in `data_writer.rs`. Byte-level test
   `issue_821_writer_byte_invariants.rs`.
3. **DESC empty-vs-valued ordering (#10).** ✅ RESOLVED (#822) — HOLDS; `ClusteringKey::compare`
   reverses for DESC so empty sorts after valued. Test `issue_822_merge_ordering_semantics`.
4. **Header-driven static/columns (#22).** ✅ RESOLVED (#822) — DIVERGENT; CQLite derives
   `hasStatic`/columns from the supplied schema, not input `SerializationHeader`s. **Follow-up:**
   read static-ness from input headers.
5. **Differential gate design (#819).** ✅ RESOLVED (#819) — implemented at
   `cqlite-core/tests/issue_819_differential_compaction.rs` + playbook
   `test-data/differential-compaction-harness.md` (Tier-2 logical equivalence + Tier-1
   load-path validity; Tier-3 byte diff debug-only).
6. **Complex-column merge (#14/#17/#18).** ✅ RESOLVED (#823) — `reconcile_cluster` collapses
   multi-cell collections/UDTs whole-column (no `cell_path` in `CellData`), so per-path merge is
   N/A-yet. **Follow-up:** add a per-path cell model.
7. **64-bit offsets (#16).** ✅ RESOLVED (#821) — HOLDS; all in-partition/data-file offsets are
   `u64`/`i64`. Test `issue_821_writer_byte_invariants.rs`.
8. **Statistics.db determinism.** Is CQLite's `estimatedTombstoneDropTime` byte-stable and
   consistent with Cassandra's histogram for the same input? (Structure/serialization documented
   in guide ch.08 via #825; above-cap streaming merges are insertion-order-dependent — open.)
9. **BTI write path (#5/#6).** If/when CQLite emits BTI output, what are the exact block-promotion
   and `trieRoot=-1` rules it must reproduce? Scope from BTI-spec §3. *(Open; overlaps #762.)*
10. **Cursor-native compaction spike.** Sketch what a zero-copy, flyweight-based `merge.rs`
    would look like in Rust (raw clustering/value `&[u8]` windows, `slice::cmp` reconcile).
    What's the realistic speed/memory win over the current heap-of-materialized-`Value` design?
