# find-key-probe — issue #4205 (epic #4192, DIAGNOSE slice, forensics F4)

**Milestone:** 0.19. **Priority:** P2. **Routing:** design-driven (OpenSpec + Seam 1) — the probe
result vocabulary, the CommitLog writetime decode extension, and the CLI report shape all have real
latitude. Correctness is oracle-bound: every "hit"/"absent" verdict is checked against the committed
`*-Data.db.jsonl` sstabledump goldens (issue #1742 physical-dump parity), and every CommitLog claim
against `cassandra-5.0.8`'s `Mutation`/`UnfilteredRowIteratorSerializer`/`EncodingStats` wire format,
never against CQLite's own prior behavior (#3041/#3042).

## Why

Forensics slice F4 (`docs/architecture/forensics-surface-2026-09.md` §6): "Where did my write go?" —
which generations hold a key, and whether it is sitting unflushed in a CommitLog segment. F1
(`cqlite explain`, issue #4193, branch `issue-4193-forensics-explain`, **not yet approved/merged** —
still at Seam-1 spec-review) reports a coarse `hit | absent | scanned` per generation and explicitly
defers the finer split to this issue (`forensics-explain` proposal.md: "Distinguishing
`bloom-negative` from `index-miss` for a generation that does not hold the key (F4 `find`)"). This
change delivers that finer split as its own verb, `cqlite find`, plus the CommitLog half F1 never
touches at all.

## What already exists (read directly, before designing)

- **The probe primitives are real and already load-bearing** (`merge/point_read.rs`,
  `reader/data_access/point_compaction.rs`): `SSTableReader::might_contain_partition` — for BIG,
  consults the actual `Filter.db` bloom filter when present (`partition_lookup.rs:505-509`); for
  **BTI, IS the trie descent itself** (`partition_lookup.rs:469-501`; `lookup_partition_via_bti_trie` at `partition_lookup.rs:192`)
  — there is no separate bloom consultation on the BTI read path (confirmed: "BTI is immune... bloom
  bypassed for the trie", matching #4195's independent finding on the same code this same day).
  `read_single_partition_for_compaction`'s three-exit contract
  (`point_compaction.rs:34-60,219-325`): `DefinitelyAbsent` (an authoritative prune — bloom negative
  OR BTI trie definitive miss, collapsed into ONE variant today), `IndexUnavailable` (no usable
  index, an inconclusive BIG Index.db "miss" per #1572, or a corrupt/unreadable index — ALWAYS
  degrades to a full scan, never trusted as absent), or `Rows` (a real seek, including the
  authoritative-empty case of a BTI prefix-collision).
- **`find`'s job is exactly to un-collapse `DefinitelyAbsent`** into `bloom-negative` (BIG, bloom
  said no) vs `index-miss` (BTI trie definitive miss; also — per this issue's own AC2 — a BIG
  Index.db lookup that structurally found no entry, reported as a diagnostic fact even where the
  PRODUCTION read path's #1572 caution treats that same "no entry" as inconclusive and re-scans
  regardless. `find` is read-only diagnostics, not the read path: it can name what each layer said
  AND still fall through to a confirmatory scan when no layer gave a load-bearing answer.
- **On a hit**, the same primitive that resolves an offset (`lookup_partition_with_index` / the BTI
  trie) also resolves the partition's authoritative end bound
  (`successor_partition_offset`) and decodes it via `seek_partition_compaction_rows` into
  `CompactionRow`s (`row_timestamp: i64` per row, plus a `CompactionRowData::PartitionMarker`
  variant for a whole-partition tombstone with its own deletion timestamp) — everything `find`'s
  "max writetime" and "partition deletion" fields need is already decoded, just not yet surfaced
  through a diagnostic-shaped API.
- **The CommitLog reader (#2389) decodes structure but DISCARDS every timestamp it reads.** A direct
  read of `cqlite-core/src/storage/commitlog/mutation.rs` found: `decode_partition_update` reads
  `EncodingStats.minTimestamp` (`let _min_timestamp = c.uvint()?;`) and discards it; `decode_rows`
  reads the row-level timestamp delta (`ROW_HAS_TIMESTAMP`) and discards it
  (`let _ts_delta = c.uvint()...`); `read_cell` reads each cell's own delta (when not
  `CELL_USE_ROW_TIMESTAMP`) and discards it too. **No writetime is available anywhere in the current
  public `Mutation`/`PartitionUpdate`/`DecodedRow`/`DecodedCell` types.** This change must add it —
  see design.md §D4 for the reconstruction (`writetime = min_timestamp + delta`, the delta convention
  the existing code already assumes by naming and reading these fields in this order, re-confirmed
  against `cassandra-5.0.8` in task 0).
- **`decode_rows` only models UNCLUSTERED tables** (`if !schema.clustering.is_empty() { return
  Err(Unsupported); }`) and only simple scalar columns (no collections/UDTs/tuples). The one
  committed CommitLog fixture (`commitlog-ground-truth.json`, keyspace `commitlog_test`, table
  `users`, `PRIMARY KEY (id)`, columns `name text, age int`) happens to already be exactly this
  supported shape — this change's AC3 fixture work does not need to lift the clustering restriction,
  but a clustered-table mutation's writetime is correctly `unmeasured`, never guessed (see Non-goals).
- **No byte position is exposed today.** `MutationIter` yields `Result<Mutation>` with no offset;
  `FrameWalker` tracks `cursor`/`marker_pos` internally (`frame.rs:54-70`) but `FrameStep::Record`
  carries only the record body slice. A small, additive extension (surface the cursor position at
  the moment a `Record` is yielded) gives `find --commitlog` the "position" field the issue asks for.
- **No matching SSTable fixture exists for `commitlog_test.users`** — only the CommitLog segment +
  its ground-truth JSON are committed. AC3 ("the committed CommitLog fixture ... where one mutation
  is unflushed") needs a companion SSTable set flushed from a SUBSET of the ground truth's inserts,
  committed alongside — new fixture-generation work, not a re-use of an existing pair.

## What changes

**Library (`cqlite-core`).** A new, standalone diagnostic module exposes:

```
pub enum GenerationProbe {
    Hit(HitDetail),
    BloomNegative,
    IndexMiss,
    Scanned(ScanOutcome),
}
pub enum ScanOutcome { Hit(HitDetail), Miss }
pub struct HitDetail {
    pub data_offset: Option<u64>,    // None only for a fail-safe-scan hit that reported no boundary
    pub byte_length: ByteLength,
    pub max_writetime: i64,
    pub has_partition_deletion: bool,
    pub coverage: CellCoverage,      // AC change pending owner approval (2026-11-16): proves "flushed"
}
pub enum ByteLength {
    Known(u64),                      // successor_start - start
    ToEof,                           // last partition, unbounded
    Unmeasured(&'static str),        // fail-safe-scan hit: no end bound resolved; never a guess
}
pub struct CellCoverage {            // what a generation's decoded rows prove present for ONE partition
    pub cell_timestamps: BTreeMap<String, i64>,       // per simple column: max cell timestamp
    pub cell_values: BTreeMap<String, Option<Vec<u8>>>, // state at that max: Some(bytes)=live non-expiring; None=else
    pub liveness_timestamp: Option<i64>,
    pub liveness_plain: bool,                          // every marker at the max is non-expiring
    pub deletion_timestamp: Option<i64>,               // max partition/row deletion
}
```

These are SHIPPED deviations from the first draft, additive and per the #4159 doctrine (an unmeasurable value is
named, never guessed): `data_offset: Option<u64>` and `ByteLength::Unmeasured`. `coverage` is the AC change below.

Built as its OWN type (not #4193's still-unmerged `ProbeOutcome`/`TraceSink`) so this change does not
depend on an unapproved spec landing first — see "Design decision" below for the sequencing question
this raises for the owner. `CommitLogReader`/`mutation.rs` gain: `PartitionUpdate.min_timestamp: Option<i64>`
(promoted, no longer discarded; `None` for an empty-partition carrier with no `EncodingStats`, never `0`, with
`undecoded_cause` naming why rows were not decoded), `DecodedRow.writetime: Option<i64>`,
`DecodedCell.writetime: Option<i64>` (own delta, or inherits the row's when
`CELL_USE_ROW_TIMESTAMP`). `mutations()` KEEPS its API (`Result<Mutation>`); a new
`mutations_with_positions()` yields `(Option<u64>, Result<Mutation>)`, `None` meaning a framing error. The
`minTimestamp` decode is `uvint (as i64).wrapping_add(TIMESTAMP_EPOCH)` per cassandra-5.0.8
`EncodingStats.Serializer` (design D4).

**AC change pending owner approval (2026-11-16): stricter flushed proof.** `unflushed: no` no longer means "the
mutation's writetime is at or below the generation max". It requires every cell, liveness marker and deletion
of the mutation to be PROVEN present in the SSTables (a strictly newer cell, a covering deletion, or an equal
timestamp on an exact state match per `Cells.resolveRegular`); otherwise `unflushed: unmeasured(<cause>)` naming
the unproven cell. See spec F4 / design D5.

**CLI (`cqlite-cli`).** New verb `cqlite find <table-dir> <partition-key> [--commitlog <dir>] [--out
text|json]`, resolving the partition key with the same literal parser `query`/`explain` use. Per
generation: the `GenerationProbe` outcome, rendered with the vocabulary above. With `--commitlog`:
every mutation (across every segment in the dir) whose partition key matches, reporting `segment`,
`position`, `writetime` (or `unmeasured: <cause>` for a clustered/complex-column mutation this
decoder does not model), and `unflushed: yes|no|unmeasured(<cause>)` — `no` only when every cell, marker and
deletion is proven present in the SSTables (AC change above), `unmeasured` whenever the mutation's writetime or
its coverage proof is incomplete, never guessed. Exit `2` also covers a body-decode error (scanning continues
past it), a `SegmentTruncated` torn tail, and a table/commitlog directory `read_dir` failure. The partition-key
argument is split by `split_key_literals` (nested tuple/collection/map/UDT literals stay whole; design D6).

## Design decision — a standalone enum now, not built on #4193's unmerged one

Recommended (not decided unilaterally — see "Open product decisions" below): build `GenerationProbe`
as its own type in this change rather than waiting on #4193 (`ProbeOutcome`/`TraceSink`, branch
`issue-4193-forensics-explain`, still Seam-1 spec-review, unapproved). The issue's own text treats
this as optional ("if #4193 lands first, explain gains the split for free via the shared enum") — it
does not require #4205 to wait. Building independently now means duplicated vocabulary strings for a
window (until whichever change lands second unifies them, a small follow-up), but unblocks this
change immediately rather than making its Seam-1 approval, implementation, and merge depend on
another still-unapproved change's fate. The alternative — sequence #4205 strictly after #4193 merges,
sharing one enum from day one — avoids the duplication window entirely at the cost of blocking on
someone else's Seam-1 approval, review rounds, and merge. Both are sound; this is a real sequencing
call, surfaced to the owner rather than picked here.

## What this change must establish

1. **Every probe verdict is checked against the physical-dump JSONL goldens** (issue #1742) — a
   `hit` fires in exactly the generations whose golden contains the key, `bloom-negative`/`index-miss`
   elsewhere, both formats.
2. **An absent bloom filter never yields `unmeasured`** — `find` falls through to the index layer and
   reports `index-miss` (per the issue's own AC2), never treating a missing optional component as an
   unmeasurable dead end the way, e.g., #4195's audit invariants correctly do for THEIR different
   question (whether components agree) — `find`'s question ("does this generation hold the key") has
   an index-layer answer even without a bloom filter.
3. **CommitLog writetime reconstruction is verified against `cassandra-5.0.8`** source before being
   trusted (`EncodingStats`/`UnfilteredSerializer`/`LivenessInfo.Serializer`'s delta-timestamp
   convention) — not merely inferred from the existing (currently-discarding) CQLite code's variable
   names, which is evidence of intent, never format authority (#3041).
4. **A clustered-table or complex-column CommitLog mutation reports `writetime: unmeasured(<cause>)`**,
   never a fabricated or zero value, and never silently omitted from the segment's mutation list —
   the mutation still counts as "touching the key" (partition-key match is unaffected by the
   clustering restriction), it simply cannot report a writetime (#4159 class, matching this epic's
   "unmeasured never 0" mandate verbatim).
5. **`unflushed` is a strict, propagating PROOF** (AC change pending owner approval, 2026-11-16) — `no` requires
   every cell, marker and deletion of the mutation proven present in the SSTables; an unmeasured writetime, an
   unprovable cell (an equal timestamp without an exact state match included), or an unreadable generation
   yields `unflushed: unmeasured(<cause>)`, never a guessed yes/no.
6. **Wiring evidence from the binary**, both SSTable formats, with and without `--commitlog`.

## Non-goals

- Reading cell VALUES (`read-sstable`/`query` already do this) — `find` reports presence, location,
  and timing metadata only, never a cell's decoded value.
- Replaying CommitLog mutations into SSTables (a write operation; out of scope for this read-only
  DIAGNOSE slice, and for the whole forensics surface per its own design constraints).
- Lifting the CommitLog decoder's clustering / complex-column restriction — this change reports
  `unmeasured` for those mutations rather than extending the decoder to model them; that lift is a
  separate, larger follow-up (the decoder's own module doc already frames it that way for clustering).
- Extending `might_contain_partition`'s BIG Index.db #1572 "inconclusive → must scan" caution — that
  guards the PRODUCTION read path's correctness and is unchanged; `find` reads the same primitive's
  answer for DIAGNOSTIC reporting without weakening the production path's own conservatism.
- Sharing #4193's `ProbeOutcome`/`TraceSink` vocabulary in this change — see the design decision above
  and the open product decision below.

## Impact statements (openspec rules)

- **No-heuristics (#28):** every classification derives from the format's own authoritative structures
  (`Filter.db`, `Index.db`, the BTI trie, `Data.db`'s decoded partition, the CommitLog's decoded
  mutation) — no byte-pattern inference anywhere.
- **Uncompressed-write claim boundary (#1406):** not implicated — entirely read-only.
- **Cassandra 5.0 only:** `na`/`nb` BIG and `da` BTI SSTables; the CommitLog reader is already gated
  to the Cassandra 5.0 CommitLog version (`CommitLogVersionGates`).
- **Public surfaces:** `cqlite-core` gains a new module (`GenerationProbe`/`HitDetail`/`ByteLength`/
  `ScanOutcome`) and additive fields on `PartitionUpdate`/`DecodedRow`/`DecodedCell`, plus
  `MutationIter`'s yielded-item shape changes (position added) — the one call site this touches
  (`cqlite-cli/src/commands/read_commitlog.rs`) is updated in the same change. CLI gains one new verb,
  `find`. Python/Node untouched.
- **<128 MB:** one generation's `Index.db`/BTI-index/bloom resident at a time for the probe; on a hit,
  one partition's decoded rows (the same bound `read_single_partition_for_compaction` already has).
  `--commitlog` holds one segment (bounded `MAX_SEGMENT_BYTES = 128 MiB`) at a time, matching the
  existing reader's documented posture.
- **Gate:** `cqlite-core/tests/issue_4205_find_corpus.rs` and the CLI test named in `cli-tests`
  (#3522) — confirm auto-coverage by the existing globs (matching #4194/#4195's confirmed pattern).
