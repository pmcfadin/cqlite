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
> carries those rulings (`FIX` / `DOCUMENT` / `DEFER` / `N/A`); **Status** records the current
> verification state.

| # | Cassandra finding (source) | CQLite relevance | Status | Verdict (#818) |
|---|---|---|---|---|
| #4 / #21 | **Cell tie-break at equal timestamp**: `Cells.resolveRegular` keeps the cell whose **raw value bytes** are strictly greater (unsigned lexicographic compare on the *raw* value, skipping the vint length prefix), **not** file/run order. | `reconcile_cluster` (`merge.rs`) keeps **first-seen (lower `run_index` = newer file)** on a full tie (same ts, both live). Two live cells, same timestamp, different values → CQLite and Cassandra can pick different winners. | **DIVERGENT** — confirmed by reading + **verified by test** `test_real_merger_value_tiebreak_diverges_from_cassandra` (`merge.rs`, 2026-06-17): CQLite keeps `"apple"` (newer file), Cassandra would keep `"banana"` (greater raw bytes). | **FIX** (Tier 2) — ruled 2026-06-19; match Cassandra's greater-raw-value-wins. Convergence test replaces the divergence test when the fix lands. |
| #13 / #3 | **Tombstone beats expiring at equal timestamp**; `IS_EXPIRING` strictly means `ttl != NO_TTL` (mutually exclusive with `IS_DELETED`); don't emit `IS_DELETED|IS_EXPIRING` or a wasted TTL byte. | CQLite handles equal-ts cell-tombstone-beats-live (`is_cell_tombstone` in `reconcile_cluster`, `merge.rs` / Issue #498). Need to confirm the *expiring-vs-tombstone* ordering and the strict flag semantics on the **write** side. | **VERIFY** | **FIX** (Tier 2) |
| #2 | **`previousUnfilteredSize`** is written to disk but skipped by readers; must equal the byte distance from the previous unfiltered including its own vint length; static rows hardcode 0 and don't advance the chain. Caught as 2601/5100 divergent bytes. | Applies to CQLite's `data_writer.rs` whenever it writes rows. If we write `0` or miscount, our Data.db diverges. | **WRITER / VERIFY** | **DOCUMENT / best-effort** (Tier 3) — promote to FIX only if a real node validates it on load. |
| #10 | **DESC clustering ordering**: empty clustering values sort *after* valued ones; comparison must route empty-vs-valued through `type.isReversed()`. | `ClusteringKey::compare` (schema-aware) must honor reversed columns for empty/valued ordering. | **VERIFY** | **FIX** (Tier 1) |
| #14 / #17 | **Complex-column (multi-cell) deletion**: shadow cells using the **un-purged** deletion value, output the **purged** value; active row/range deletion *strictly* supersedes an equal-timestamp complex deletion (drop on equality). | Reachable once CQLite merges multi-cell collections/UDTs in compaction. Confirm `reconcile_cluster` handles complex deletions at all. | **VERIFY / N/A-yet** | **FIX** (Tier 2) |
| #18 | **Complex-column merge is path-ordered** using the column's *path comparator* — UDT field paths use **signed** `ShortType`, list paths use TimeUUID, map paths use the key type — **not** plain unsigned bytes. | Same multi-cell merge path as #14. Easy to get wrong by comparing raw bytes. | **VERIFY / N/A-yet** | **FIX pairing** (Tier 2); output order bytes Tier-3. |
| #16 | **Index-block / partition offsets must be 64-bit** (`long`), not `int` — an `int` wraps negative past 2 GiB and corrupts every block offset. | CQLite should use `u64`/`i64` for in-partition and data-file offsets throughout the writer. | **VERIFY** | **FIX** (Tier 1) |
| #12 | **Column subset encoding** (sparse rows): `<64`-column superset → single unsigned-vint bitmap (set bit = *missing* column, value 0 reserved → use `HAS_ALL_COLUMNS`); `≥64` → "large subset" with an exact mode boundary and tail-column inclusion. | Applies to CQLite read *and* write of sparse rows. Read path likely exercised; write-side boundary needs care. | **VERIFY** | **FIX correctness incl. 64-col boundary** (Tier 1/2) — mode selection is decode-critical, not Tier-3. |
| #5 / #6 | **BTI block boundaries**: the tail block's width includes the end-of-partition marker byte; promote to a row index only when a partition spans **>1** block (count blocks *including* the tail); single-block → `trieRoot = -1`, partition entry points direct-to-data. | Only if CQLite emits **BTI** output during compaction. CQLite currently reads BTI (guide ch.17); write path is BIG-format. | **N/A-yet (BTI write)** | **DEFER** (overlaps epic #762). |
| #22 | Header `hasStatic` and column superset come from the **merged input sstable headers**, not current schema (a dropped static column still has static rows on disk). Dropped columns filtered per-source against per-column drop horizons. | CQLite is schema-driven; must read static-ness / column sets from the sstable `SerializationHeader`, not the supplied schema, when merging. | **VERIFY** | **FIX** (Tier 2) |
| #23 | `Filter.db` may be `AlwaysPresentFilter` when `bloom_filter_fp_chance = 1.0` — guard the cast. | CQLite bloom handling on read; writer must reproduce. | **VERIFY** | **FIX** (Tier 1) |
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

---

## Part 4 — Recommended next actions (snapshot — keep current)

1. **[compaction] Reconcile finding #4/#21** — divergence is **verified** by
   `test_real_merger_value_tiebreak_diverges_from_cassandra` (`merge.rs`). **Decision made
   2026-06-19 (#818): FIX** — `reconcile_cluster` must match Cassandra's "larger raw value
   wins" (Tier 2, resolution-equivalence). When the fix lands, that test flips to a convergence
   test. #820 characterizes the tie-break across non-text types. *(Verified 2026-06-17; ruled
   FIX 2026-06-19.)*
2. **[compaction] Stand up a differential compaction gate** against real Cassandra output,
   using `bti-sstable-specification.md` as the contract. **Pass criterion (#818):** logical
   merge equivalence (Tier 2) + real-node load-path validity (Tier 1, shipped-to-cluster);
   raw-byte component diff is a debug-only secondary signal (Tier 3). *(Open — #819.)*
3. **[guide] Verify-then-import** the Data.db (ch.05) and merge/tombstone (ch.11) detail. *(Open.)*

---

## Further research questions (for new agents)

Pick one, investigate against CQLite's actual code/behavior, then update Part 1/Part 4 above
with findings. Prefer reproducing each as a test before claiming a status change (see
[regression-test verification doctrine](https://pmcfadin.github.io/cqlite/agents-developing/)).

1. **Tie-break divergence (#4/#21).** ✅ *Reproduced in a unit test*
   (`test_real_merger_value_tiebreak_diverges_from_cassandra`, `merge.rs`) — CQLite's
   first-seen rule picks a different winner than Cassandra's raw-value-bytes rule.
   **Decided (#818): FIX** — match Cassandra's greater-raw-value-wins (Tier 2). **Remaining
   work:** (a) confirm it fires on *real corpora* via the #819 harness, not just the unit
   fixture; (b) characterize across non-text types where "raw value bytes" ≠ UTF-8 (`int`,
   `decimal`, `blob`, `uuid`) — see #820; (c) implement the fix and flip the reproduction test
   to a convergence test.
2. **`previousUnfilteredSize` (#2).** What does `data_writer.rs` currently write for this
   field? Is it counted correctly (including its own vint length; 0 for static rows)? Add a
   byte-level assertion.
3. **DESC empty-vs-valued ordering (#10).** Does `ClusteringKey::compare` order empty
   clustering values *after* valued ones for reversed columns? Construct a reversed-clustering
   table and check.
4. **Header-driven static/columns (#22).** Does CQLite's compaction read `hasStatic` and the
   column superset from the sstable `SerializationHeader`, or from the supplied schema? Test
   with a dropped static column whose rows still exist on disk.
5. **Differential gate design (#819).** What's the minimal harness to compact the same inputs
   through Cassandra and CQLite and compare per the #818 bar — **gate on logical merge
   equivalence (Tier 2) + real-node load-path validity (Tier 1)**, with a raw-byte
   `Data.db`/`Statistics.db` diff kept as a **debug-only** secondary signal (Tier 3
   byte-identity is a non-goal)? Where does it live relative to the existing `sstabledump`
   parity tooling and `test-data/`?
6. **Complex-column merge (#14/#17/#18).** Does `reconcile_cluster` merge multi-cell
   collections/UDTs at all today, or only collapse whole rows? If it does, are path ordering
   (signed UDT field index) and shadow-before-purge honored?
7. **64-bit offsets (#16).** Audit the writer for any `i32`/`u32` offset that should be 64-bit
   for >2 GiB partitions.
8. **Statistics.db determinism.** Is CQLite's `estimatedTombstoneDropTime` (if written)
   byte-stable and consistent with Cassandra's histogram for the same input? Does CQLite even
   populate it, and do downstream consumers (tombstone-compaction heuristics) rely on it?
9. **BTI write path (#5/#6).** If/when CQLite emits BTI output, what are the exact block-promotion
   and `trieRoot=-1` rules it must reproduce? Scope from BTI-spec §3.
10. **Cursor-native compaction spike.** Sketch what a zero-copy, flyweight-based `merge.rs`
    would look like in Rust (raw clustering/value `&[u8]` windows, `slice::cmp` reconcile).
    What's the realistic speed/memory win over the current heap-of-materialized-`Value` design?
