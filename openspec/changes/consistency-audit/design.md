# Design — consistency-audit (issue #4195)

## D0. Shape: `AuditSummary` is a closed 8-row checklist, additive to `VerifyReport`

```
pub struct VerifyReport {
    // ...existing fields, unchanged...
    pub audit: Option<AuditSummary>,   // NEW — Some only when mode == VerifyMode::Audit
}

pub struct AuditSummary {
    pub rows: [AuditRow; 8],           // always exactly 8 — one per named invariant, never omitted
    pub deep: bool,                    // whether --deep ran (gates row 8's Skip vs a real attempt)
}

pub struct AuditRow {
    pub invariant: &'static str,       // stable id, e.g. "index-data-identity" (never renumbered)
    pub verdict: AuditVerdict,
}

pub enum AuditVerdict {
    Pass,
    Fail,                              // a matching VerifyFinding is ALSO pushed to `findings`
    Skip(String),                      // cause, e.g. "no CompressionInfo: uncompressed",
                                        // "boundary source unreadable", "requires --deep"
}
```

Rationale for a fixed-size, always-8 checklist rather than a `Vec` that only contains rows which ran:
mirrors the sweep's `totals.<severity>` affirmative-zero convention (#4194 design.md §S4.1) — a
reader must be able to tell "this invariant does not apply here" (`Skip`) from "this invariant was
silently never reported" (a missing row), and a fixed array makes the latter a compile-time
impossibility rather than a runtime hope.

**Invariant id → format applicability** (fixed at compile time, not reader-dependent):

| id | Invariant | Applies to |
|---|---|---|
| `index-data-identity` | (1) Index.db entry position → Data.db header key match | BIG only |
| `summary-index-correlation` | (2) Summary.db sample → Index.db position/key match | BIG only (BTI has no Summary.db) |
| `bloom-no-false-negative` | (3) Filter.db has no false negative | BIG and BTI |
| `bti-trie-bounds` | (4) BTI trie leaf reachability + payload/root bounds | BTI only |
| `toc-exact-listing` | (5) TOC.txt lists exactly the files on disk | BIG and BTI |
| `digest-matches-data` | (6) Digest.crc32 == CRC32(Data.db) | BIG and BTI |
| `compression-coverage` | (7) CompressionInfo chunk table covers exactly Data.db | BIG and BTI (compressed only) |
| `statistics-vs-scan` | (8) Statistics.db vs a real scan | BIG and BTI, `--deep` only |

A row whose invariant does not apply to the SSTable's format/state is `Skip` naming why (e.g.
`bti-trie-bounds` on a BIG SSTable: `Skip("not applicable: BIG format")`; `compression-coverage` on
an uncompressed SSTable: `Skip("no CompressionInfo: uncompressed")`) — never omitted from the 8-row
array, and never silently `Pass` for something that did not run.

## D1. Invariants 5 and 6 are ALREADY IMPLEMENTED — audit reuses them verbatim

`check_toc_and_presence` (TOC exact listing, both directions) and `check_digest` (CRC32 match) are
correct today and require **no new checking logic** — `Audit` mode calls them exactly as `Quick`/
`Full` already do (they already run unconditionally in `verify_components`, outside the mode match)
and maps their existing pass/fail state onto `AuditRow`s 5 and 6. This is the one part of the change
that is pure wiring: read whether a `MissingComponent`/`DigestMismatch` finding was pushed for the
relevant component, and set `Pass`/`Fail` accordingly.

## D2. Invariant 1 (BIG): Index.db position → Data.db partition-header key match

**The one invariant with a real runtime-bound constraint** (proposal.md establishment 5): audit
without `--deep` must read only the `Data.db` **partition headers** `Index.db` names, never full
partitions. This makes invariant 1 structurally different from #4194's own BTI identity check
(`bti_partition_identity_mismatch`), which gets its Data.db-side key set from a FULL row scan
(`full_row_scan_partitions`) — reusing that here would violate the runtime bound.

```
check_index_data_identity(components, index_entries) -> AuditVerdict:
   Index.db itself unhealthy (IndexEntryCorrupt already found)?
       -> Skip("boundary source unreadable")
   for each (key, data_offset) in index_entries (BIG, via a re-walk that also captures each
      entry's OWN Index.db byte offset — needed again by D3):
       read ONLY the partition header at Data.db[data_offset..] — the existing point-read /
       header-decode path (cqlite-core/src/storage/sstable/reader/parsing/row_decoder/
       partition_driver/header_arm.rs and the point-read call sites in data_access/) already
       does exactly this for a point lookup: seek to data_offset, decompress only the covering
       chunk(s) if compressed, decode the partition-header key WITHOUT decoding any row —
       never a whole-partition read.
       header key == index entry's key? no -> Fail (IndexPositionKeyMismatch, names offset + both keys)
   all matched -> Pass
```

- **No new boundary-source primitive for the KEY comparison** — `IndexReader::get_partition_entries`
  already yields (key, data_offset) pairs (confirmed present, #4194 design.md §D1). The genuinely new
  primitive is the **header-only** read at an arbitrary `Data.db` offset; task 0 must re-confirm the
  exact existing entry point (candidates: something in `reader/parsing/row_decoder/partition_driver/
  header_arm.rs`, or a helper already used by `reader/data_access/point_compaction.rs`) and its
  compressed-input behavior (chunk lookup via `CompressionInfo::chunk_for_offset`, matching #4194's
  own D1 chunk-translation logic) — this activation located the module but did not trace the exact
  call signature; that is task 0's job, not assumed here.
- Half-open extents are not needed here (unlike #4194's location work) — this check does a POINT
  lookup at each declared offset, not a range intersection.

## D3. Invariant 2 (BIG): Summary.db sample → Index.db position/key match

```
check_summary_index_correlation(components) -> AuditVerdict:
   Summary.db absent -> Skip("no Summary.db")
   Index.db unhealthy -> Skip("boundary source unreadable")
   for each SummaryEntry { partition_key, index_position } in SummaryReader::get_entries():
       # index_position is a BYTE OFFSET INTO Index.db (confirmed:
       # summary_reader/mod.rs:91, "Position in Index.db file (byte offset)")
       look up the Index.db entry whose OWN byte offset == index_position, from the same
       (offset, key) capture D2's re-walk produces
       found and key == partition_key? no -> Fail (SummarySampleMismatch, names the sample
       index and both keys)
   all matched -> Pass
```

- **`Summary.db`'s recorded position is an `Index.db` byte offset, not a `Data.db` offset** —
  confirmed directly from `summary_reader/mod.rs` doc comments and `SummaryEntry.index_position`'s
  own doc ("Position in Index.db file"). This is a different axis from D2's Data.db-header check:
  invariant 2 never touches `Data.db` at all, so it is unconditionally within the no-full-partition
  runtime bound.
- D2 and D3 share ONE re-walk of `Index.db` that captures `(byte_offset_in_index_db, key,
  data_offset_in_data_db)` per entry — a superset of what `check_big_index`'s existing structural walk
  computes but currently discards. Implementation should decide (task 0) whether to extend
  `check_big_index` to return this vector or perform a second pass in the new module; either is
  correct, the requirement is that `Index.db` is walked authoritatively once per verify_components
  call, not re-parsed ad hoc per sample (`Summary.db` can have hundreds of samples).

## D4. Invariant 3: bloom false-negative check, extended to BTI

`check_filter_false_negatives` (issue #1398) already does the right thing for BIG: probe `Filter.db`
against every key in the authoritative present-key set (`Index.db`). The audit extension is
mechanical: run the SAME probe logic for BTI, sourcing the present-key set from `bti_leaves`'s
resolved raw keys (D1's — #4194's — `BtiResolvedLeaf.inline_raw_key`/Data.db-scan resolution) instead
of `Index.db` entries. Confirmed BTI SSTables DO carry `Filter.db` (`da-2-bti-Filter.db` present in
the `bti_partitions_footer_flip` fixture directory) — the existing "BTI is immune" comment in
`verify.rs` is about the READ path bypassing the bloom for point lookups, not about the file's
existence, so auditing it is meaningful (a stale/corrupt BTI `Filter.db` that a future read path
change might consult would otherwise go undetected).

- If resolving `bti_leaves` requires a boundary-source-healthy `Partitions.db`/`Rows.db` (it does —
  D1/#4194's own fail-closed rule), an unhealthy BTI trie makes this row `Skip("boundary source
  unreadable")`, matching this design's general boundary-source-poisons-the-row rule (§D8 below).

## D5. Invariant 4 (BTI): the one new sub-check — `DataOffset` leaf bounds

`check_bti_structure` + `bti_partition_identity_mismatch` already cover reachability (the trie walk
IS the reachability proof — a leaf not walked from the root is not in `partitions`, full stop) and
payload-identity (issue #1103, cross-checked against a Data.db scan). The one gap: a `DataOffset`
leaf's `data_position` is never checked to be `< Data.db`'s length BEFORE a scan runs — today an
out-of-bounds `DataOffset` is only caught if/when a FULL scan actually tries to read there. Add, in
`check_bti_structure` itself (cheap: `Data.db`'s length is already known from a `stat`, no read), a
bounds check for every `DataOffset` leaf: `data_position < data_len` else a NEW finding
(`BtiDataOffsetOutOfBounds`, names the leaf's prefix and the out-of-bounds position). `RowsOffset`
leaves are unaffected (already resolved via `Rows.db`, whose own bounds are already checked). This
folds into the AUDIT row `bti-trie-bounds` alongside the existing reachability/identity checks
(already covered, wired as-is, same pattern as D1).

## D6. Invariant 7: CompressionInfo chunk-table exact coverage

`CompressionInfo { chunk_length: u32, data_length: u64, chunk_offsets: Vec<u64> }` (confirmed fields,
`compression_info.rs:70/76/83`). "Covers exactly Data.db's length" is a closed arithmetic check, no
new I/O beyond what `check_compression_info` already reads:

```
expected_chunk_count = ceil(data_length / chunk_length as u64)   # data_length: LOGICAL uncompressed size
chunk_offsets.len() == expected_chunk_count?
   no -> Fail (CompressionCoverageGap: "declared N chunks, data_length implies M")
   yes -> the LAST chunk's physical range end (chunk_offsets.last() + its compressed size, i.e. the
          next boundary or Data.db's actual on-disk length for the final chunk) must equal Data.db's
          real file size (already read for the existing bounds check) -> mismatch is the same Fail
```

Uncompressed SSTables: `Skip("no CompressionInfo: uncompressed")` — matches the existing
`check_compression_info` early-return semantics (`Ok(None)`).

## D7. Invariant 8 (`--deep` only): Statistics.db vs an actual full scan

The only invariant needing a full decode pass. `check_statistics` today only validates that
`Statistics.db` parses (header sanity + `StatisticsReader::open`); comparing its DECLARED
`min_timestamp`/`max_timestamp`/`min_local_deletion_time`/`max_local_deletion_time`/`partition_count`
(confirmed fields on `StatisticsMetadata`, `writer/stats_writer/mod.rs` tests) against OBSERVED
values needs a scan that tracks running extrema while decoding — CQLite already computes exactly
these four running values on the WRITE side, during flush/compaction merge (`storage/write_engine/
merge/mod.rs`, `merge/fully_expired.rs`), to WRITE `Statistics.db` in the first place. Task 0 must
confirm whether that running-stats accumulator is (a) already a reusable, standalone type callable
read-only over an existing reader's decoded stream, or (b) merge-path-coupled and needs a purpose
extraction. Either way, the observed-vs-declared comparison itself is only meaningful when at least
`partition_count` is available cheaply (`distinct_partition_keys_with_positions().len()`, already
computed by the existing FULL-mode scan) — that half is definitely in scope; the timestamp/LDT
extrema comparison is the change's single largest implementation unknown and is where a scope cut (a
documented DECLARED GAP, #4194-style, for the timestamp/LDT half specifically — landing
`partition_count` cross-check alone) is the fallback if task 0 finds the accumulator is not cheaply
reusable within this change's size budget.

- **Not a #3042 round-trip test**: this reuses CQLite's stats-COMPUTATION logic read-only to check an
  EXISTING (possibly Cassandra-written) `Statistics.db` against the SAME `Data.db` it ships with —
  it is not writing new data and comparing CQLite's write against CQLite's read of that same write.
  A Cassandra-written `Statistics.db` compared against CQLite's own independent scan-and-compute is a
  real cross-implementation check, not a symmetric blind spot.

## D8. Fail-closed: a damaged boundary source Skips its dependent rows, mirroring #4194 §D2

Every row's `Skip("boundary source unreadable")` cause fires whenever the invariant's OWN
authoritative source is itself the corrupt component (matching #4194's location-poisoning rule,
reused verbatim as vocabulary — an operator who has read one SSTable-tool report already knows this
string): `index-data-identity`/`summary-index-correlation` skip if `Index.db` is corrupt;
`bti-trie-bounds`/`bloom-no-false-negative` (BTI) skip if the BTI trie is corrupt;
`bloom-no-false-negative` (BIG) skips if `Index.db` is corrupt (matches the EXISTING
`check_filter_false_negatives` comment: "If Index.db is absent/corrupt the present-key set is
unavailable, so nothing is probed here").

## D9. Vacuous-audit exit contract

`cqlite verify --mode audit` exit code: `2` if ANY row is `Fail`; `2` if EVERY row is `Skip` (naming
`audit: 0 invariants MEASURED` in both text and JSON — affirmative-zero doctrine, matching the CLAUDE.md
mandate that an unmeasured value is never indistinguishable from a clean one); `0` otherwise (a mix of
`Pass` and `Skip` with zero `Fail` is a genuine pass — e.g. an uncompressed SSTable correctly
`Skip`-ping `compression-coverage` is not a failure). This is the SAME `VerifyReport::is_ok()`-driven
exit-code call site `cqlite-cli/src/commands/verify.rs` already has (line 43-47); `is_ok()` gains one
new condition (`audit.is_some() && (any Fail || all Skip)`).

## D10. Report/CLI shape — additive, campsite-respecting

- `cqlite-core/src/storage/sstable/verify_audit.rs` (new file, `mod verify_audit;` next to `pub mod
  verify;`, matching #4194's `verify_location.rs` precedent) — `AuditSummary`/`AuditRow`/
  `AuditVerdict`, the 8 check functions (D1–D7), and the shared `Index.db` re-walk (D2/D3). Split into
  a `verify_audit/` directory (mirroring `summary_reader/`'s `mod.rs` + `interval.rs` pattern) if a
  single file would exceed the ~800-line campsite target — task 0 sizes this once premises are
  confirmed.
- `verify.rs` itself: add `pub audit: Option<AuditSummary>` to `VerifyReport`, dispatch to
  `verify_audit::run(...)` when `mode == VerifyMode::Audit`, add 5 new `VerifyErrorClass` variants.
  Same file-size discipline as #4194 (`CQLITE_ALLOW_FILE_GROWTH=1` + epic #1116 note if unavoidable).
- `cqlite-cli/src/cli_types.rs`: `VerifyModeArg` gains `Audit`; the `Verify` subcommand gains `#[arg(long)]
  deep: bool` with `long_about` stating it is only meaningful with `--mode audit` and is a usage error
  otherwise (fail-closed, matching the codebase's existing usage-error conventions, e.g. #4194 sweep's
  S2.2).
- `cqlite-cli/src/commands/verify.rs` (112 lines, well under threshold): extend `print_text`/
  `print_json` to render the 8-row checklist when `audit.is_some()`, and reject `--deep` without
  `--mode audit` before calling into `verify_sstable` (exit `1`, names the requirement).

## D11. No header hunting (mirrors #4194 §D6/§L3)

Every invariant check reads only decoded, authoritative structures — `Index.db`/`Summary.db` entries,
the BTI trie's resolved leaves, `CompressionInfo.db`'s chunk table, and (invariant 1 only) a
header-only decode at an INDEX-NAMED offset — never a byte-pattern search over `Data.db`. A
`scripts/tests/test_verify_audit_no_resync_scan.sh` (registered in `tooling-tests`, same pattern as
#4194's `test_verify_location_no_resync_scan.sh`) greps `verify_audit.rs` (or the `verify_audit/`
directory) for `memchr`, `windows(`, `find(|b|`, `position(|b|` outside `#[cfg(test)]` and FAILs
naming the line if any is found.

## D12. New corruption fixtures needed

None of the existing `test_comp_corrupt` fixtures exercise a FAIL on invariants 1, 2, 4 (the new
`DataOffset` bounds sub-check), 7, or 8 — each needs a byte-mutated fixture whose mutation is
INVISIBLE to every EXISTING check (so it reaches the new audit code at all) but visible to the new
one. Extend `test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml` via
`generate-corruption-corpus.sh` (matching its existing per-fixture schema: `mutation_type`,
`byte_offset`, before/after hex + sha256, `expected_error_class`, a captured `cassandra_verdict` where
`sstableverify --extended` also has an opinion):

| New fixture | Mutation | Invariant exercised |
|---|---|---|
| `index_entry_offset_swap` | Swap two adjacent BIG `Index.db` entries' `data_offset` VInts (each remains a structurally valid entry — `check_big_index` stays green) | 1 (Fail) |
| `summary_sample_position_corrupt` | Corrupt one `Summary.db` sample's recorded `index_position` to point at a DIFFERENT (still in-bounds) `Index.db` entry | 2 (Fail) |
| `filter_db_bit_flip_bti` | BTI analogue of the existing BIG-only `filter_db_bit_flip` — one bit 1→0 in a BTI `Filter.db` | 3 (Fail, BTI) |
| `bti_data_offset_out_of_bounds` | Rewrite one `DataOffset` leaf's payload in `Partitions.db` to a position past `Data.db`'s end | 4 new sub-check (Fail) |
| `compression_info_short_coverage` | Drop the LAST entry from `CompressionInfo.db`'s `chunk_offsets` (declared count falls short of `ceil(data_length/chunk_length)`) | 7 (Fail) |
| `statistics_timestamp_mismatch` | Corrupt `Statistics.db`'s `min_timestamp` to a plausible-but-wrong value (header + parse stay valid) | 8 (Fail, `--deep`) |

Every fixture's clean source, mutation byte offset, and before/after sha256 are committed in the
manifest (never mutated at test time); `.db` binaries stay gitignored per the corpus convention.
Whether a compressed BTI fixture exists in the committed/fetched corpus for the `compression-coverage`
BTI case is a task-0 premise — if absent, that ONE combination (BTI + compression-coverage) is a
declared gap (BIG's compressed fixtures already cover the invariant; extending to a BTI-compressed
fixture is then a follow-up, not fabricated here).

## D13. `sstableverify --extended` parity (issue #1236 pattern)

Cassandra's `sstableverify --extended` checks digest (invariant 6, already parity-tracked in the
existing manifest) and per-chunk CRC (adjacent to, not identical to, invariant 7's coverage check —
Cassandra's tool re-validates chunk CRCs, it does not assert chunk-COUNT coverage against
`data_length` the way this change's new check does). Cassandra has no equivalent to invariants 1, 2,
4's new bounds check, or 8 — those are CQLite-only detections (matching the existing precedent of
`FilterFalseNegative`, itself a CQLite-only detection per issue #1398's own doc comment). Each new
fixture's manifest entry records `cassandra_verdict`/`verdict_parity` as `n/a: no Cassandra-side
check for this invariant` where no Cassandra check exists, and the real captured verdict (via
`test-data/scripts/capture-cassandra-verify-verdicts.sh`) where one does (fixtures touching digest/
chunk-CRC-adjacent invariants).
