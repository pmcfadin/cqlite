# Proposal: fully-expired-sstable-drop

> Milestone: Cassandra Byte-for-Byte Parity Program (parent epic #1378, from the 2026-07-01 parity audit).
> Issue: #1388. Routing: **design-driven** (a new compaction-planning capability + public API surface,
> not an oracle-driven decode/offset bug fix) → OpenSpec. Owner has already approved the product go/no-go
> (feature is YES). Priority P2 (perf/space optimization). Builds on #1382 (expired data already purges
> through the normal compaction path) — this change makes that purge *cheaper* by dropping whole SSTables
> instead of rewriting them.

## Why

Cassandra's `CompactionController.getFullyExpiredSSTables` computes, before a compaction rewrites
anything, the subset of input SSTables whose data is entirely past `gcBefore` (every cell's
`localDeletionTime < gcBefore`). Those SSTables are **dropped whole** — excluded from the K-way merge and
simply deleted after the output is published — instead of being read, merged, and re-serialized into the
output. This mirrors `TTLExpiryTest`: a table written with only TTL'd data, once every TTL has expired and
the grace period has elapsed, compacts to *nothing* without paying the rewrite cost.

CQLite today has none of this. Grepping `fully_expired` in the write engine finds only a doc comment
(`cqlite-core/src/storage/write_engine/merge/mod.rs:~1092`). After #1382 landed, an all-expired SSTable is
still read and merged; its rows are correctly purged (so the *output* is right), but CQLite pays the full
rewrite cost — reading and decoding every dead cell only to drop it. For a large all-expired SSTable next
to a small live one, a major compaction rewrites the live rows and needlessly streams the entire dead
SSTable through the merger. Dropping it whole is strictly faster and reclaims the space in one unlink.

The correctness subtlety Cassandra guards is **overlap safety**: a fully-expired SSTable may still hold a
*tombstone* (or a deletion) that shadows *older live data* living in an SSTable that is NOT part of this
compaction. Dropping the expired SSTable whole in that case would resurrect the shadowed data on the next
read. Cassandra gates the drop with a max-timestamp overlap check against the non-compacting overlapping
SSTables; this change mirrors that gate using authoritative `Statistics.db` metadata.

## What changes

- Add a **metadata-only fully-expired detection** function to the merge module that, given a candidate
  SSTable's `Statistics.db` and the compaction's `gcBefore` cutoff, decides whether the SSTable is fully
  expired: `maxLocalDeletionTime < gcBefore` (read from `TimestampStatistics.max_deletion_time`). No cell
  scan — authoritative metadata only (no-heuristics mandate).
- Add an **overlap-safety gate** mirroring Cassandra: a candidate that passes the expiry test is only
  eligible to be dropped when its `maxTimestamp` (`TimestampStatistics.max_timestamp`) is strictly less
  than the minimum write timestamp across every *outside* overlapping SSTable (the same
  `EncodingStats.minTimestamp` bound `compute_max_purgeable_timestamp` already reads). If it could shadow
  older data outside the compaction set, it is NOT dropped.
- Add a **drop-set computation** that composes the two: given the compaction input set and the outside
  overlapping set, return the subset to drop whole. Excluded (dropped) SSTables are removed from the
  merger's input list and deleted after the output publishes.
- Wire the drop-set into the compaction surfaces so a dropped SSTable's rows are absent from output AND
  the plan/stats record which SSTables were dropped whole (assertable, not just observable in output).
- Extend the compaction report/stats to expose the dropped-whole set (paths + count) so tests and the CLI
  can assert the plan decision.

## Non-goals

- **NOT changing single-SSTable / normal-path purge from #1382.** Expired cells still purge correctly
  through the merge for any SSTable that is *not* dropped whole; this change only adds a whole-SSTable
  fast path on top of that behavior. Read output for a non-dropped SSTable is unchanged.
- **NOT partial drop.** An SSTable that is only *partially* expired is never dropped; it goes through the
  normal merge exactly as today. Only an SSTable proven fully expired by metadata is eligible.
- **NOT relaxing the overlap-safety contract.** The drop is gated at least as conservatively as
  Cassandra's overlap check. When metadata cannot prove safety (unreadable/absent `Statistics.db` for the
  candidate or any outside SSTable), the SSTable is NOT dropped (conservative default), exactly as
  `compute_max_purgeable_timestamp` already degrades.
- **NOT a new compaction strategy or policy.** This is a planning refinement inside existing major/full
  compaction paths; it does not add STCS/LCS/TWCS selection logic.
- **NOT changing how `gcBefore` is derived.** `gcBefore` continues to come from `compute_gc_before`
  (schema `gc_grace_seconds`, Cassandra's 10-day default on absence) / the CLI `--gc-before` cutoff.
- **NOT applied to purge-unsafe partial background compactions beyond what the max-timestamp gate
  already proves.** A background partial compaction may still drop a fully-expired SSTable, but ONLY when
  the overlap gate proves it shadows nothing outside the set (same conservatism as #935 purging).

## Doctrine impact

- No change to `CLAUDE.md` or the `agents-developing/` site is required: this is a scoped compaction
  capability governed by the existing no-heuristics mandate and the existing supported-format floor. The
  new detection reads only authoritative `Statistics.db` metadata, so it is squarely inside the
  no-heuristics doctrine (cite it in code comments, do not amend it).
- The definitive guide's compaction chapter (`docs/sstables-definitive-guide/chapters/15-compaction-strategies.md`)
  MAY gain a short note that fully-expired SSTables are dropped whole; that documentation edit is in scope
  for the implement phase but changes no doctrine.
