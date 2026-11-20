# Design — find-key-probe (issue #4205)

## D1. Per-generation classification — un-collapsing `DefinitelyAbsent`

`find` does NOT call `read_single_partition_for_compaction` directly (its three-exit contract
collapses bloom-negative and BTI index-miss into one `DefinitelyAbsent`, and treats a BIG Index.db
"miss" as inconclusive-therefore-scan rather than a reportable `index-miss`). Instead, `find`
composes the SAME lower primitives directly, preserving which layer answered:

```
probe_generation(reader, key) -> GenerationProbe:
   BIG:
     reader.bloom_filter.is_some()?
        yes -> bloom.might_contain(key)? no -> BloomNegative
        (yes, or no filter) -> continue
     reader.has_partition_index()? no -> Scanned(fail_safe_scan(reader, key))
     reader.lookup_partition_with_index(key):
        Ok(Some((offset, _))) -> resolve end_bound, decode -> Hit(detail) | (decode disagrees with
                                   key, e.g. a stale/corrupt entry -> Scanned(fail_safe_scan(...)))
        Ok(None)              -> IndexMiss                      # AC2: no bloom, index says no
        Err(_)                -> Scanned(fail_safe_scan(reader, key))   # corrupt/unreadable index
   BTI:
     reader.lookup_partition_via_bti_trie(key):
        Ok(Some(offset)) -> resolve end_bound, decode:
           decoded key == target key -> Hit(detail)
           decoded key != target key (prefix collision, empty Rows) -> IndexMiss   # D1.1
        Ok(None) -> IndexMiss                                    # trie definitive miss
        Err(_)   -> Scanned(fail_safe_scan(reader, key))          # corrupt/unreadable trie
```

- **D1.1 — a BTI prefix-collision is `IndexMiss`, not `Scanned`.** `read_single_partition_for_compaction`
  step 5's doc (`point_compaction.rs:304-308`) is explicit: a BTI trie resolves by PREFIX, so a decoded
  key that turns out to be a DIFFERENT partition is a genuine, authoritative collision — the trie
  answered, it just didn't point at the target. This is index-authoritative absence with no scan
  needed, so `find` reports it as `IndexMiss`, distinguished in its detail text from a definitive
  `Ok(None)` trie miss only for an operator's benefit (both are the SAME top-level verdict).
- **BIG's "index says no" is reported even though production treats it as inconclusive (#1572).**
  `find` is diagnostics: it names what the Index.db lookup itself returned. If a caller wants the
  PRODUCTION guarantee (never trust a bare Index.db miss), that is what `Scanned` is for on every
  path where the index could not be trusted at all (absent, corrupt, unreadable) — `IndexMiss` here
  means "the index was consulted and, taken at face value, said no", which is exactly what an
  operator debugging "where did my write go" wants named, not silently re-scanned into a different
  answer. This is a deliberate, narrow divergence from `read_single_partition_for_compaction`'s own
  posture, scoped to this diagnostic surface only (impact statement 2 in proposal.md).
- **`fail_safe_scan`**: a full, key-filtered scan of the generation's `Data.db`, reusing the SAME
  scan primitive the production `IndexUnavailable` fallback already drives (confirm the exact reused
  entry point in task 0 — the full-scan-filtered-to-keys machinery already exists somewhere in the
  merge/scan path; this must not become a second scan implementation).

## D2. `HitDetail` — reusing what the seek already decoded

`seek_partition_compaction_rows` (already called by step 5 of `read_single_partition_for_compaction`)
returns `Vec<CompactionRow>` for the target partition. `HitDetail` is built from it with NO additional
I/O:

- `data_offset` = the resolved `offset` (already computed in D1).
- `byte_length` = `Known(end_bound - offset)` when `end_bound: Some(_)`, else `ToEof` (the last
  partition in the generation — `successor_partition_offset` returns `None`, matching its own
  documented contract).
- `max_writetime` = `max` over every `CompactionRow.row_timestamp` in the decoded set, ALSO
  considering a `CompactionRowData::PartitionMarker`-variant row's own deletion timestamp (a
  whole-partition tombstone's `markedForDeleteAt` can be the partition's true max timestamp even when
  every live cell is older) — task 0 confirms the exact field name on that variant (proposal.md's
  research located the variant but not its precise field list).
- `has_partition_deletion` = `true` iff any decoded `CompactionRow` is the partition-delete variant.
- **Deviations from the first draft (additive, #4159 doctrine; now normative in spec F2):**
  `data_offset: Option<u64>` (a fail-safe-scan hit may report no partition boundary), and
  `ByteLength::Unmeasured(&'static str)` as a third variant (a fail-safe-scan hit resolves no end bound, and a
  guess is forbidden). Both render as `unmeasured(<cause>)` in text and `{"unmeasured": …}` / `null` in JSON.
- **`coverage: CellCoverage` (AC change approved by the owner on 2026-11-16 — see D5).** The same decode also
  fills a `CellCoverage` per hit, kept just fine enough to PROVE a CommitLog mutation's cells are in the SSTables:
  - `cell_timestamps: BTreeMap<column, i64>` — max cell timestamp per simple column (a cell tombstone's included);
  - `cell_values: BTreeMap<column, Option<Vec<u8>>>` — the state of the cell at that max timestamp: `Some(serialized
    value bytes)` for a live, non-expiring cell, `None` for a tombstone, an expiring cell, a value the schema
    cannot serialize, or a tie at the max between rows with different states;
  - `liveness_timestamp: Option<i64>` and `liveness_plain: bool` — max row-liveness-marker timestamp, and whether
    every marker at it is non-expiring;
  - `deletion_timestamp: Option<i64>` — max partition deletion / row tombstone / row deletion timestamp (a range
    marker covers only a clustering range and is never a proof).
  Per-column maxima are a sound proof only for a table WITHOUT clustering columns (one row per partition), the
  only shape the CommitLog decoder models. Value bytes are produced with the write-path `TypeSerializer`; without
  the `write-support` feature `cell_value` is `None`, so an equal timestamp is never proven equal (fail closed).

## D3. CommitLog: exposing the byte position (`frame.rs`, small additive change)

`FrameWalker` already tracks `cursor: usize` (position of the next record) before it validates and
returns a record. Change `FrameStep::Record(&'a [u8])` to `FrameStep::Record { offset: usize, body:
&'a [u8] }`, captured from `self.cursor` (or the record's own start, whichever `next_frame`'s internal
bookkeeping makes correct — task 0 pins the exact field) at the point the step is constructed. This is
the record's byte offset WITHIN THE SEGMENT FILE (not within a section) — matching what an operator
would use to `dd`/`hexdump` the raw bytes if they needed to. **As built:** `CommitLogReader::mutations()` keeps its API
(`MutationIter`, item `Result<Mutation>`; existing call sites such as `read_commitlog.rs` are unchanged). The new
`CommitLogReader::mutations_with_positions()` returns a `PositionedMutationIter` yielding
`(Option<u64>, Result<Mutation>)`: `Some(offset)` is the record's byte offset in the segment file; `None` means a
FRAMING error (no record offset exists; the iterator ends). A body-decode error carries `Some(offset)` and the
iterator CONTINUES (the frame walker has already advanced past the CRC-valid record), so later matching mutations
and a torn tail are still reported. `truncated()` is available on both iterators.

## D4. CommitLog: reconstructing writetime (the change's largest single risk)

Cassandra's `EncodingStats` (read, currently discarded, as `_min_timestamp`/`_min_ldt`/`_min_ttl`)
exists PRECISELY so per-cell/per-row timestamps can be delta-encoded against a shared base — this is
why `decode_rows`'s row loop and `read_cell` each read a `_ts_delta` uvint only when the value is NOT
inherited from the row (`ROW_HAS_TIMESTAMP`/`CELL_USE_ROW_TIMESTAMP`). The reconstruction this change
adds:

```
min_timestamp    = (uvint read from EncodingStats) as i64  .wrapping_add(TIMESTAMP_EPOCH)   # Java long wrap-around
actual_timestamp = min_timestamp + ts_delta      # uvint delta is >= 0 by construction
```

**Verified against `cassandra-5.0.8` (task 0.4; design CORRECTED from the first draft, which omitted the epoch).**
`EncodingStats.Serializer.serialize` writes `out.writeUnsignedVInt(stats.minTimestamp - TIMESTAMP_EPOCH)` (line
274) and `deserialize` reads `in.readUnsignedVInt() + TIMESTAMP_EPOCH` (line 288), with
`TIMESTAMP_EPOCH = 2015-09-22T00:00:00Z = 1442880000000000` microseconds
(`src/java/org/apache/cassandra/db/rows/EncodingStats.java`). Both are Java `long` arithmetic: the unsigned vint is
reinterpreted as a signed 64-bit value and the add WRAPS, so a `minTimestamp` before the epoch (a pre-2015
timestamp) is written as a wrapped huge unsigned value and must read back to the original negative-delta value —
CQLite decodes `(raw as i64).wrapping_add(TIMESTAMP_EPOCH_MICROS)` and never rejects a pre-epoch timestamp as
out of range. A row/cell timestamp is `minTimestamp + uvint delta` (`SerializationHeader.readTimestamp`). Verified
against the committed Cassandra-written `commitlog_find` fixture (explicit `USING TIMESTAMP`, exact equality) and
a pre-epoch unit test.

- `PartitionUpdate.min_timestamp: Option<i64>` — promoted (no longer `_min_timestamp`). DEVIATION (additive): `None`
  for an empty-partition carrier that has no `EncodingStats` (never `0`), with `PartitionUpdate.undecoded_cause:
  Option<&'static str>` naming WHY rows were not decoded (so `writetime` can say `unmeasured(<cause>)`). `DecodedRow`
  also gains `deletion_timestamp` and `liveness_expiring`.
- `DecodedRow.writetime: Option<i64>` — `Some(min_timestamp + ts_delta)` when `ROW_HAS_TIMESTAMP`;
  `None` when the row has no row-level timestamp (Cassandra rows are not required to carry one — a
  row with only cell-level liveness has no row liveness marker).
- `DecodedCell.writetime: Option<i64>` — `Some(min_timestamp + ts_delta)` when the cell has its own
  delta; `Some(row.writetime)` (inherited) when `CELL_USE_ROW_TIMESTAMP` and the row itself has one;
  `None` otherwise (row has no timestamp to inherit either — a genuinely timestamp-less cell, which
  should not normally occur for a live write but is reported honestly rather than assumed).
- **A row/cell deletion's own timestamp** (`ROW_HAS_DELETION`'s `_mfda`, `read_cell`'s deletion
  `_ldt`) is read but its RECONSTRUCTED value is a smaller, separate addition — `find`'s own
  `writetime` field for a mutation is the MAX writetime it contributes, which for a delete-only
  mutation is the deletion's `mfda`, not a live cell's timestamp (there may be none). Task 2 computes
  "the mutation's writetime" as `max(row.writetime, every cell.writetime, the row/cell deletion mfda
  when present)` over the whole `PartitionUpdate` — a single closed rule, not per-call-site logic.
- **Partition-level deletion mutations remain OUT of scope for writetime** — `decode_partition_update`
  bails (`rows_decoded = false`) the instant `has_partition_deletion` is set, before reading
  `EncodingStats` at all (the partition-deletion body has a DIFFERENT wire shape this decoder does not
  model). `find --commitlog` still reports the mutation touches the key (structural partition-key
  match is unaffected) with `has_partition_deletion: true` and
  `writetime: unmeasured("partition-deletion timestamp not decoded")` — never a guess, matching the
  epic's #4159 doctrine verbatim. Lifting this is a follow-up, not attempted here.
- **Clustered-table / complex-column mutations**: `decode_rows` already bails to `rows_decoded = false`
  for these (unchanged by this design). `find --commitlog` reports
  `writetime: unmeasured("<clustered table | complex column> not decoded")`, naming which.

## D5. `unflushed` — a fail-closed PROOF, not a guess

**AC change approved by the owner on 2026-11-16: stricter flushed proof.** The first draft decided `no` by
`mutation_writetime <= max_generation_writetime`. That is unsound: a later write of column `a` says nothing
about an earlier-timestamped write of column `b`, and an equal timestamp can be a different cell (roborev job
299 M1). `no` is now a PROOF obligation.

```
unflushed(mutation_writetime, needs, generations) =
   mutation_writetime is Unmeasured(cause)            -> Unmeasured(cause)
   max_hit = max over Hit / Scanned(Hit) generations of HitDetail.max_writetime
   max_hit exists AND mutation_writetime <= max_hit:
        needs is Err(cause)                           -> Unmeasured("mutation coverage not proven by SSTable cells: <cause>")
        first_unproven(needs) is Some(n)              -> Unmeasured("mutation coverage not proven by SSTable cells: <describe n>")
        otherwise (every need proven)                 -> No
   some generation Unreadable (could not be probed)   -> Unmeasured("generation <name> could not be probed")   # may hold a newer write
   otherwise (above every hit, or no hit at all)      -> Yes
```

`needs = mutation_needs(update)` lists every cell (`Need::Cell { column, timestamp, value }`), liveness marker
(`Need::Liveness { timestamp, expiring }`) and deletion (`Need::Deletion(ts)`) of the mutation; it is
`Err(cause)` for a shape the proof does not model (undecoded rows, a clustered row, a cell with no timestamp).

A need is PROVEN by ONE Hit (or `Scanned(Hit)`) generation's `CellCoverage` holding:

1. a STRICTLY newer cell of the same column / a strictly newer liveness marker; or
2. a COVERING deletion: `coverage.deletion_timestamp >= need.timestamp`; or
3. an EQUAL timestamp ONLY on an exact state match: for a cell, `need.value` is `Some(bytes)` and
   `coverage.cell_values[column] == Some(bytes)` (both live, non-expiring, identical serialized bytes); for a
   marker, `coverage.liveness_plain && !need.expiring`.

Why equal needs an exact state match: at equal timestamps Cassandra reconciles by tombstone-vs-live, then
expiring state, local deletion time, TTL and finally value bytes — `Cells.resolveRegular`
(`src/java/org/apache/cassandra/db/rows/Cells.java`) and `LivenessInfo.supersedes`
(`src/java/org/apache/cassandra/db/LivenessInfo.java`), both read at `cassandra-5.0.8`. So an equal timestamp
alone proves nothing, and a tombstone, an expiring cell, or a value this schema cannot serialize
(`cell_values == None`; also every cell when `write-support` is not compiled in) is never proven equal: the
verdict degrades to `Unmeasured` naming the column and timestamp, never to a guessed `no`.

`Yes` needs only the newest write to be above everything any generation holds (its own cell then cannot be in any
SSTable); an unreadable generation might hold a newer write, so it blocks a `Yes` but never a proven `No`.
A key absent from every generation with every generation readable is `Yes` unconditionally.

## D6. CLI shape

```
cqlite [--schema … --data-dir …|--dataset …] find <table-dir> <partition-key> [--commitlog <dir>] [--out text|json]
```

- `<partition-key>`: same CQL-literal parser `query`/`explain` use for `WHERE pk = …` — no new
  grammar (matches #4193 design.md §D4's own convention for the sibling verb).
- Per-generation rendering: `<basename>: hit  offset=<N> len=<N|to-eof>  max_writetime=<ts>
  partition_deletion=<yes/no>` / `<basename>: bloom-negative` / `<basename>: index-miss` /
  `<basename>: scanned  result=<hit|miss>`.
- `--commitlog <dir>`: every `*.log` segment in the dir, one line per matching mutation:
  `<segment>@<position>: writetime=<ts|unmeasured(<cause>)>  unflushed=<yes|no|unmeasured(<cause>)>`.
  A segment that cannot be opened (corrupt descriptor, unsupported version, compressed/encrypted) is
  its OWN line naming the cause, never a silent skip (matches the epic's fail-closed mandate).
- `json`: `{ "generations": [{"basename","probe", ...HitDetail fields when probe=="hit"|"scanned-hit"}],
  "commitlog": [{"segment","position","writetime","unflushed"}] | null }`.
- Exit codes: `0` — reported (including "0 generations hold this key", matching #4193's own exit-0
  absence convention, R5 in its spec); `1` — usage / table or schema not resolvable; `2` — anything could not be
  read in full, never a shorter report (#4159): a generation that could not be opened/probed; a segment that
  could not be opened; a CommitLog record whose BODY failed to decode (the walk continues past it, so later
  mutations and a torn tail are still reported); a `SegmentTruncated` torn tail (mutations in the torn bytes
  cannot be seen); a mutation touching the key that declares updates the decoder cannot walk past; and a table
  directory or `--commitlog` directory whose `read_dir` fails (named, `cannot list …`).
- CommitLog text line types (`CommitEntry`): `Mutation` -> `  <segment>@<position>: writetime=…  unflushed=…`;
  `SegmentFailure` -> `  <segment>: UNREADABLE  <cause>`, where a body-decode error's cause reads `record at
  position N could not be read: <error>` (`record at framing error could not be read: <error>` when
  `mutations_with_positions` yielded `None`); `SegmentTruncated` -> `  <segment>: torn tail -- mutations in the
  torn bytes cannot be seen`. JSON: `{"segment","unreadable"}` / `{"segment","truncated":true}`.
- **Key literals — `split_key_literals`.** `<partition-key>` carries one CQL literal per partition-key column.
  `split_key_literals` splits it at commas at nesting depth zero and outside quoted text: `(`/`[`/`{` nest (tuple,
  list/set, map and UDT literals), `'...'` (with `''` escaped) and `"..."` text is opaque. So
  `{street: 'a,b', zip: 1}, [(1,2),(3,4)]` is two literals. Each part is parsed by the SAME SELECT literal parser
  `query` uses (against a FIXED synthetic column name, so a quoted/reserved real column name cannot break the
  parse) and encoded with `encode_partition_key_columns` against the schema column by position; a count
  mismatch is a usage error (exit 1).

## D7. Campsite

`point_read.rs`, `point_compaction.rs`, and `mutation.rs` are all read, none rewritten wholesale —
`find`'s probe logic lives in a NEW file (`cqlite-core/src/storage/sstable/find_probe.rs` or under
`reader/data_access/find.rs`, task 0 picks the more idiomatic location given the existing
`data_access/` module layout) that CALLS the existing primitives; `mutation.rs`'s additive fields are
small, in-place diffs (new struct fields + the delta-reconstruction math at existing read sites), not
a rewrite. Re-measure `mutation.rs` (779 lines) and `point_compaction.rs`/`point_read.rs`'s current
sizes at task-start time against the ~800-line campsite target before assuming no split is needed.

## D8. No header hunting

Every classification and every writetime comes from a decoded, authoritative structure (`Filter.db`,
`Index.db`/BTI trie entries, `Data.db`'s partition header + cells via the existing seek path, the
CommitLog's own `EncodingStats`/delta fields) — never a byte-pattern scan. `find`'s fail-safe scan
(D1) is a FULL, schema-driven decode of `Data.db` filtered to the key, the same mechanism the
production read path already uses on an untrusted index, not a resync/pattern search.
