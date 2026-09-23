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
  step 5's doc (`point_compaction.rs:243-250`) is explicit: a BTI trie resolves by PREFIX, so a decoded
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
- `has_partition_deletion` = `true` iff any decoded `CompactionRow` is the `PartitionMarker` variant.

## D3. CommitLog: exposing the byte position (`frame.rs`, small additive change)

`FrameWalker` already tracks `cursor: usize` (position of the next record) before it validates and
returns a record. Change `FrameStep::Record(&'a [u8])` to `FrameStep::Record { offset: usize, body:
&'a [u8] }`, captured from `self.cursor` (or the record's own start, whichever `next_frame`'s internal
bookkeeping makes correct — task 0 pins the exact field) at the point the step is constructed. This is
the record's byte offset WITHIN THE SEGMENT FILE (not within a section) — matching what an operator
would use to `dd`/`hexdump` the raw bytes if they needed to. `MutationIter::next()` threads this
through: yields `(position: u64, Result<Mutation>)` instead of bare `Result<Mutation>`. The ONE
existing call site, `cqlite-cli/src/commands/read_commitlog.rs`, is updated in this same change
(destructure and ignore the position, or render it — task decides which reads better for that
command; either is a compatible, in-scope edit).

## D4. CommitLog: reconstructing writetime (the change's largest single risk)

Cassandra's `EncodingStats` (read, currently discarded, as `_min_timestamp`/`_min_ldt`/`_min_ttl`)
exists PRECISELY so per-cell/per-row timestamps can be delta-encoded against a shared base — this is
why `decode_rows`'s row loop and `read_cell` each read a `_ts_delta` uvint only when the value is NOT
inherited from the row (`ROW_HAS_TIMESTAMP`/`CELL_USE_ROW_TIMESTAMP`). The reconstruction this change
adds:

```
actual_timestamp = min_timestamp + ts_delta      # uvint delta is always >= 0 by construction
```

**This convention is assumed by the EXISTING code's shape (it reads `min_timestamp` first, then a
delta per row/cell) but was never exercised, because every value was discarded before this change.**
Task 0 MUST verify the exact delta semantics against `cassandra-5.0.8`
(`org/apache/cassandra/db/rows/EncodingStats.java`,
`org/apache/cassandra/db/rows/UnfilteredSerializer.java`'s `serialize`/`deserialize` for
`LivenessInfo`/`Cell`) before this change trusts it — CQLite's own prior (never-executed) code shape
is evidence of intent, never format authority (#3041). If the delta convention differs from the above
(e.g. a different base, or zig-zag rather than a plain non-negative offset), task 0 corrects this
design before task 2 implements it.

- `PartitionUpdate.min_timestamp: i64` — promoted (no longer `_min_timestamp`).
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

## D5. `unflushed` — a fail-closed comparison, not a guess

```
unflushed(mutation_writetime, max_generation_writetime) =
   mutation_writetime is Unmeasured(cause)         -> Unmeasured(cause)
   max_generation_writetime is Unmeasured(cause)   -> Unmeasured(cause)     # every generation itself unmeasured, or key absent everywhere and no generation could even be probed
   key absent from every generation (no Hit anywhere) -> Yes (nothing flushed at all; trivially true)
   mutation_writetime > max_generation_writetime   -> Yes
   otherwise                                       -> No
```

`max_generation_writetime` is the max of every generation's `HitDetail.max_writetime` where that
generation reported `Hit` — `None`/`Unmeasured` only when the key was found nowhere as a `Hit` at all
AND at least one generation could not be conclusively probed (a `Scanned` outcome that itself failed,
or an unreadable generation) — a clean "absent everywhere, every generation readable" case yields
`Yes` unconditionally (anything in the CommitLog is definitionally unflushed if nothing flushed it).

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
  absence convention, R5 in its spec); `1` — usage / table or schema not resolvable; `2` — a
  generation or CommitLog segment could not be read at all (named), never a shorter report (#4159).

## D7. Campsite

`point_read.rs`, `point_compaction.rs`, and `mutation.rs` are all read, none rewritten wholesale —
`find`'s probe logic lives in a NEW file (`cqlite-core/src/storage/sstable/find_probe.rs` or under
`reader/data_access/find.rs`, task 0 picks the more idiomatic location given the existing
`data_access/` module layout) that CALLS the existing primitives; `mutation.rs`'s additive fields are
small, in-place diffs (new struct fields + the delta-reconstruction math at existing read sites), not
a rewrite. Re-measure `mutation.rs` (776 lines) and `point_compaction.rs`/`point_read.rs`'s current
sizes at task-start time against the ~800-line campsite target before assuming no split is needed.

## D8. No header hunting

Every classification and every writetime comes from a decoded, authoritative structure (`Filter.db`,
`Index.db`/BTI trie entries, `Data.db`'s partition header + cells via the existing seek path, the
CommitLog's own `EncodingStats`/delta fields) — never a byte-pattern scan. `find`'s fail-safe scan
(D1) is a FULL, schema-driven decode of `Data.db` filtered to the key, the same mechanism the
production read path already uses on an untrusted index, not a resync/pattern search.
