# Design — snapshot-diff (issue #4201)

## D1. Mechanism — two independent full-compaction trails, then a comparison fold

```
side A: build_traced_merger_with_registry(A's generations, [key], effective schema, UDT registry,
                                          ScanCancel, RecordingSink)
           ──► full-compaction config: with_purge_safe(true),
               with_gc_before_secs(compute_gc_before(schema, now)), with_now_secs(Some(now))
           ──► step() to Complete ──► into_trace_sink() = RecordingSink_A { cells, tombstones }
                                                    (#4193's own mechanism, unmodified; `explain` drives
                                                    exactly this sequence)

side B: identical, over B's own generations           ──► RecordingSink_B { cells, tombstones }

winners_A = { (clustering, column) -> (value, writetime, run_index) : cell.verdict == Winner }
winners_B = same, from RecordingSink_B

cell diff, key = (clustering, column, cell_path), over winners_A ∪ winners_B
(owner Q21=b 2026-11-21: each element of a non-frozen collection is its own cell, as in Cassandra;
 `cell_path` is the element's path bytes, absent for a simple cell or a frozen collection):
   in both, same value bytes + writetime (+ ttl/expiry)      -> equal
   in both, different writetime                              -> newer-in-a | newer-in-b  (greater ts)
   in both, SAME writetime, different value/ttl/tombstone    -> conflict, resolves_to: a|b
        (resolves_to = the side Cassandra keeps: Cells.resolveRegular, cassandra-5.0.8
         db/rows/Cells.java:79-128 — higher ts; equal ts: tombstone or expiring beats live,
         tombstone beats expiring, larger localDeletionTime, lower TTL, then the larger serialized
         value by unsigned-lexicographic ValueAccessor.compare; compared on bytes, never on
         formatted strings)
   in A only                                                 -> only-a
   in B only                                                 -> only-b
   a cell TOMBSTONE on one side at the same key and the SAME writetime as a live cell on the other
   side counts as "in both" (tombstone value, resolves_to the tombstone side by the rule above).

tombstone diff, key = (kind, scope, deletion_time, local_deletion_time), over
   tombstones_A ∪ tombstones_B:
   present in both (same key)            -> already reconciled knowledge; OMITTED (proposal.md
                                             decision 2)
   present in A only                     -> only-a  -> resurrection_risk = check(T, winners_B)
   present in B only                     -> only-b  -> resurrection_risk = check(T, winners_A)

check(T, winners_other):
   shadowed = { c in winners_other : c falls within T's scope AND c.writetime <= T.deletion_time }
   if shadowed is empty:  resurrection_risk = "no"
   else:
      purge_eligible = T.local_deletion_time < (now - gc_grace_seconds)     # strict `<`, #1385
      resurrection_risk = "past-gc-grace" if purge_eligible else "before-gc-grace"
      shadowed_cells = shadowed  (named in the report, not just counted)
```

This is deliberately not a new reconciliation primitive. Both trails are produced by the exact
mechanism #4193 built and #4200 reuses; `diff` owns only the comparison-and-classification fold, one
partition at a time (D3).

## D2. Why this predicate is the correct resurrection-risk rule, cited at the pinned tag

Three facts, all read from `cassandra-5.0.8` (never a CQLite `file:line` — #3041; the exact source
locations the implementation cites are recorded in the module docs and the PR):

1. **A tombstone becomes purge-eligible once its `localDeletionTime` is older than `gcBefore`.**
   `ColumnFamilyStore#gcBefore(long nowInSec)`:
   ```java
   public long gcBefore(long nowInSec)
   {
       return nowInSec - metadata().params.gcGraceSeconds;
   }
   ```
   and the boundary compaction actually applies is a **strict** `<` (matching `DeletionPurger`'s
   `shouldPurge(timestamp, localDeletionTime)` contract, and matching the strict-`<` rule
   `cqlite-core/tests/issue_1385_gc_grace_boundary.rs` already pins for #4193's `purgeable` verdict —
   `diff`'s `before-gc-grace`/`past-gc-grace` boundary is the SAME rule, not a new one).
2. **A cell/row is shadowed by a tombstone when its timestamp is not newer than the tombstone's
   deletion time.** `DeletionPurger`:
   ```java
   public interface DeletionPurger {
       boolean shouldPurge(long timestamp, long localDeletionTime);
       default boolean shouldPurge(DeletionTime dt) {
           return !dt.isLive() && shouldPurge(dt.markedForDeleteAt(), dt.localDeletionTime());
       }
   }
   ```
   — the same `markedForDeleteAt` (writetime) vs. cell-timestamp comparison #4193's reconcile steps
   (`merge/reconcile/steps.rs`, `merge/reconcile/marker.rs`) already implement for a single side's own
   reconciliation (design.md §D1 of the archived `forensics-explain`). `diff` applies the identical comparison across sides: a
   cell in the OTHER side's winner set is shadowed by THIS side's tombstone under the same rule.
3. **This is exactly why Cassandra itself will not purge past that boundary when an older source is
   still outstanding.** `CompactionController#getPurgeEvaluator`:
   ```java
   public LongPredicate getPurgeEvaluator(DecoratedKey key) {
       ...
       for (SSTableReader sstable: filteredSSTables) {
           if (sstable.mayContainAssumingKeyIsInRange(key)) {
               minTimestampSeen = Math.min(minTimestampSeen, sstable.getMinTimestamp());
               hasTimestamp = true;
           }
       }
       ... // memtables checked the same way
       if (!hasTimestamp) return time -> true;
       else { final long finalTimestamp = minTimestampSeen; return time -> time < finalTimestamp; }
   }
   ```
   `getPurgeEvaluator` only ever sees sources on the SAME node (other SSTables + memtables); it
   cannot see a divergent replica. `diff` is the tool that answers the same question — "is there an
   older-timestamped source still holding data this tombstone would shadow" — for the one source
   `getPurgeEvaluator` structurally cannot reach: **the other replica's snapshot.** `before-gc-grace`
   means the tombstone is still physically retained everywhere it exists, so no purge can happen yet;
   `past-gc-grace` means the SIDE HOLDING THE TOMBSTONE is now free to purge it on its own next
   compaction, at which point, if the other side's older data is ever merged back in (repair,
   `sstableloader`, a restored snapshot) without ever having received the tombstone, it resurfaces.
   `diff` cannot see or prevent that compaction — it names the hazard before it happens.

### D2b. Declared scope limit — a liveness-only row (issue #4601)

A row's liveness marker (the `INSERT` primary-key timestamp) is not a cell in the reconciliation
trail. A row tombstone whose only victim is a row that holds ONLY a liveness marker therefore
reports `resurrection_risk: no`. This is a declared limit, stated in `--help` and the cookbook,
tracked by #4601; it is never silently "checked, none found" for such a row (R4.4 pins it).

## D3. Memory bound and streaming (matches #4200 D2's "fold, don't accumulate" posture)

Full-table `diff` enumerates partitions with the structural Data.db walk `rebuild` uses
(`SSTableReader::distinct_partition_keys_with_positions`, decompression-transparent, BIG and BTI alike,
never a byte-pattern scan), read-only, over the UNION of keys present on either side (a key present on only one side is compared against
an empty winner/tombstone set on the other — this is itself a legitimate `only-a`/`only-b` finding at
the partition level, not an error). For each partition: build both sides' traced mergers, fold into the
running `DiffReport` (respecting `max_result_bytes`, the same budget `query`/`explain` honor), and
drop both sides' `RecordingSink` output before the next partition — no whole-table `Vec` of either
side's cells is ever resident. `--partition <key>` skips enumeration entirely and drives both sides
through `build_traced_merger_with_registry` directly (the point-read path), per issue AC 4.

**Effective budget and whole-report rule (R8/R11/R14; C re-check L3).** The effective result budget is
`max_result_bytes` capped at 32 MiB (`MAX_EFFECTIVE_BUDGET`). The cap exists so both trails, the fold and
the report all stay under the 128 MiB memory target (roborev job 393). R14 is a whole-report rule: the
serialized report, metadata included, never exceeds the effective budget. The remaining memory edges
are deferred to follow-up #4613.

## D4. Report shape

```json
{
  "now": "<RFC3339>",
  "table": "<ks.tbl>",
  "gc_grace_seconds": 864000,
  "side_a": { "dir": "<path>", "generations": ["nb-1-big", "nb-2-big"] },
  "side_b": { "dir": "<path>", "generations": ["nb-1-big"] },
  "partitions": [
    {
      "partition_key": "<rendered>",
      "cells": [
        { "clustering": "2", "column": "extra", "status": "only-b", "a": null,
          "b": { "value": "extra_2", "writetime": "2021-01-01T00:00:00Z" } },
        { "clustering": "9", "column": "v", "status": "conflict", "resolves_to": "a",
          "a": { "value": "zebra", "writetime": "1970-01-01T00:00:00.005Z" },
          "b": { "value": "apple", "writetime": "1970-01-01T00:00:00.005Z" } }
      ],
      "tombstones": [
        { "kind": "row", "clustering": "2", "side": "a", "deletion_time": "2021-01-02T00:00:00Z",
          "local_deletion_time": "2026-11-11T00:53:44Z",
          "resurrection_risk": { "status": "yes", "phase": "past-gc-grace",
                                  "shadowed_cells": [{"clustering":"2","column":"extra","side":"b"},
                                                      {"clustering":"2","column":"val","side":"b"}] } }
      ]
    }
  ],
  "summary": { "partitions_compared": 2, "equal": 7, "conflicts": 0, "only_a": 0, "only_b": 9,
               "newer_in_a": 0, "newer_in_b": 0, "tombstones_only_a": 3, "tombstones_only_b": 0,
               "resurrection_risk_before_gc_grace": 0, "resurrection_risk_past_gc_grace": 3 }
}
```

A cell side carries `ttl`/`expires_at` (expiring cell) or `local_deletion_time` (tombstone) when present, so a conflict that differs only in expiry is visible. A collection element carries `cell_path` (hex of the element's path bytes) on its cell entries and on
`shadowed_cells`; `resolves_to` is present only on `conflict`. `text` renders the same data one finding per line, `summary` last. `resurrection_risk.status` is
always present (`"yes"` or `"no"`) on every `only-a`/`only-b` tombstone entry — never omitted, so a
reader can `grep resurrection_risk` and trust silence means "checked, none found," not "not
computed" (the #4159 class: an unmeasured value is never indistinguishable from a measured zero).

## D5. Symmetry (issue AC 3)

`diff(A, B)` and `diff(B, A)` compare the same two trails; swapping the call swaps which physical
directory is labeled `a` vs `b` and nothing else. The fold in D1 is already symmetric in its
definitions (`only-a`/`only-b`, `newer-in-a`/`newer-in-b` are label-relative, not fact-relative), so
the test asserts the mechanical property directly: run both directions, relabel one side's `a`/`b`
tags and swap `newer-in-a`↔`newer-in-b`, and assert the two reports are then equal (`summary` counts
included). This is a property test, not a hand-written mirror fixture, so it holds for any future
fixture without being re-derived per case.

## D6. Fixture (issue AC 1) — `test_tomb/resurrection_gc_positive`, staged as two directories

Already published in `datasets-v4` (Cassandra 5.0.8; the 5.0.2 set this spec was first drafted against
carried different wall-clock deletion times, so every literal below was re-read from the 5.0.8
`*.jsonl` goldens and `nb-2-big-Statistics.db.txt`: "SSTable min/max local deletion time" 1794358424),
2 generations, `gc_grace_seconds = 864000` (`test-data/schemas/tombstone-parity.cql`
table 5). Generation 1 (`nb-1-big`): partition `1` clustering 1–5 all live; partition `2` clustering
1–3 all live. Generation 2 (`nb-2-big`): partition `1` — a ROW tombstone at clustering `2`
(`marked_deleted=2021-01-02T00:00:00Z`, `local_delete_time=2026-11-11T00:53:44Z`) and a CELL
tombstone on column `val` at clustering `3` (same times); partition `2` — a PARTITION tombstone
(same times). Read directly from the committed `*.jsonl` sstabledump goldens next to each
generation's `Data.db` (never re-derived from CQLite output, #3042).

Stage: **A** = a temp copy of the table dir with BOTH generations; **B** = a temp copy with ONLY
`nb-1-big-*`. `gc_before_boundary_epoch = local_delete_time_epoch + gc_grace_seconds =
1794358424 + 864000 = 1795222424` (2026-11-21T00:53:44Z). Per #1385's strict-`<` convention:
`--now 1795222424` (exactly the boundary) ⇒ **not yet** purge-eligible ⇒ `before-gc-grace`;
`--now 1795222425` (boundary + 1s) ⇒ `past-gc-grace`.

Expected `diff(A, B)` at `--now 1795222425`:
- partition `1`, clustering `2`: tombstone `only-a` (row); cells `extra`/`val` `only-b`
  (`extra_2`/`live_2`, both older than the deletion) ⇒ `resurrection_risk: yes, past-gc-grace`.
- partition `1`, clustering `3`, column `val`: tombstone `only-a` (cell); cell `val` `only-b`
  (`live_3`) ⇒ `resurrection_risk: yes, past-gc-grace`. Column `extra` at clustering `3`: **equal**
  on both sides (`extra_3`, untouched) — proves the tool classifies at cell granularity, not row.
- partition `1`, clustering `1`, `4`, `5`: every cell **equal** — proves the report is not vacuously
  "everything differs."
- partition `2`: tombstone `only-a` (partition); all 6 cells (`extra`/`val` × clustering 1–3)
  `only-b` ⇒ `resurrection_risk: yes, past-gc-grace` for every shadowed cell named.

At `--now 1795222424` the same tombstones classify `resurrection_risk: yes, before-gc-grace` (the
shadow relationship is identical — B's data is still older than the deletion — only the gc_grace
phase changes), proving the boundary is exact to the second, matching #1385 and #4193's R2.3.

### D6b. Range-tombstone fixture (R5.1) — `test_explain/trace_decisions`

`test_tomb/wide_range_tombstone` (named in the first draft of R5.1) is a SINGLE-generation table: its
range tombstone and the cells it covers sit in the same `Data.db`, so the "withhold the tombstone's
generation" staging R5.1 describes is impossible there. The committed 2-generation Cassandra-5.0.8
fixture `test-data/datasets/sstables/test_explain/trace_decisions-*` (issue #4193; mutations in
`generation-a.cql`/`generation-b.cql`, pinned `USING TIMESTAMP`) already holds exactly the shape R5.1
needs: partition `3` has A (`nb-1-big`) cells `ck=1 v='range-shadowed'` (ts 1000) and `ck=3
v='outside-range'` (ts 1000), and B (`nb-2-big`) the inclusive range tombstone `[1,2]` at ts 2000
(`local_delete_time` 1794535357, `gc_grace_seconds` 864000 ⇒ boundary 1795399357). Staged as side A =
both generations, side B = `nb-1-big` only, the range tombstone is `only-a`, and the cell
`ck=1 v` is shadowed (1000 `<=` 2000, inside `[1,2]`) while `ck=3 v` is not (outside the span) — a
positive and a negative case in one partition.

## D7. Sizing

One new core module (per-side trail drive is a direct call into #4193's mechanism; the comparison
fold plus the range-tombstone span predicate is the new code, ~350–500 lines), one CLI verb
(~150–200 lines matching `explain`'s shape), tests against the one mandatory fixture plus
a synthetic identical-snapshot case and a truncated-generation failure case (~700–1000 lines, matching
sibling per-scenario density). **Total estimate: ~1.4–2k lines including tests** — within the
single-PR budget the issue's own "Medium" sizing calls for.
