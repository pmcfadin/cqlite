# sstable-scrub — issue #4198 (epic #4192, REPAIR)

**Milestone:** 0.18. **Priority:** P2. **Routing:** design-driven (OpenSpec + Seam 1) — `scrub`
combines an existing behavior (`compact_sstables`/`KWayMerger`, byte-parity-proven since 0.12) with
an existing behavior (#4196 salvage's decode-at-offset loss recording) under a new failure contract
and a new sidecar-output policy that has real latitude (which partitions get separated, what exit
code means what). The bytes it emits are oracle-bound to the existing compaction and salvage paths;
the *behavior it claims to match* is oracle-bound to Cassandra `Scrubber`/`StandaloneScrubber`
source at `cassandra-5.0.8`, read directly for this proposal (never a CQLite `file:line`, #3041).

## Why

`nodetool scrub` needs a running node with the table's schema loaded. An operator with a damaged or
out-of-order table and no cluster has nothing today. #4196 `salvage` (merged 2026-09-19) answers
"give me back whatever one damaged file still has" for ONE generation, verbatim, never merging or
purging. `scrub` answers a different question: "put this table's *generations* back into the shape
compaction would have produced" — out-of-order partitions/rows corrected into a separate output
(Cassandra's own `Scrubber` behavior), corrupted partitions optionally dropped, everything else
untouched. It is the REPAIR-tier sibling epic #4192 names right after #4197 `rebuild`.

## What changes

**Library (`cqlite-core`).** A `scrub` module under `storage/write_engine/`, in new files (file-size
ratchet, #1116), exposing `scrub_table(generations, out_dir, schema, options) -> Result<ScrubReport>`.
It runs every generation of one table through the **same** `KWayMerger`/`compact_sstables` path
`cqlite compact --major` already uses — `purge_safe = options.purge` (default `false`, matching
Cassandra's own scrub, which never purges gc-expired tombstones; `--purge` asserts the operator is
giving scrub every generation, the same assertion `compact --major` already makes) — with one added
behavior the merger does not have today: **partition/row order is checked on the way in**, and an
input whose partitions or rows are NOT already in key order is not silently re-sorted into the
primary output — the out-of-order material is diverted to a separate `<out>/<generation>-outoforder`
SSTable, sorted, mirroring `SortedTableScrubber.writeOutOfOrderPartitions`. Corrupted-partition
handling is NOT a new decode path: `--skip-corrupted` calls #4196's own
`decode_partition_at_offset_for_salvage` + `LossClass` machinery, so a scrub loss and a salvage loss
for the same byte damage are, structurally, the same classification code — R3 makes this an asserted
equality, not a coincidence.

**CLI (`cqlite-cli`).** `cqlite scrub <table-dir> --out <dir> [--now <ts>] [--dry-run]
[--purge] [--skip-corrupted] [--reinsert-overflowed-ttl] [--manifest <path>] [--out-format
text|json]`. `--dry-run` (CQLite-only convention, epic #4192; **Cassandra's scrub has no dry-run
mode at all** — see Non-goals/deviations) computes and reports everything scrub would do — every
generation's partition/row-order verdict, every loss under `--skip-corrupted`, every
overflowed-TTL row `--reinsert-overflowed-ttl` would rewrite — without writing a byte under `--out`.
Exit codes match #4196's convention: `0` clean, `3` output written with something to report
(out-of-order sidecar written, and/or losses, and/or TTL rows rewritten), `2` refused (nothing
written — e.g. `--skip-corrupted` not given and a corrupt partition was found), `1` usage.

## Relationship to salvage (#4196) — how the two verbs do NOT overlap

| | `salvage` | `scrub` |
|---|---|---|
| Input unit | one Data.db / one generation | every generation of one table dir |
| Cross-generation reconciliation | **never** — output count == input count, one-to-one | **always** — the whole table's generations are merged through the compaction path, like `compact --major` |
| Tombstone purging | never (not its job) | governed by `--purge`, same `purge_safe` contract as `compact --major` (default off) |
| Partition/row reordering | not its job — preserves file order | **its job** — out-of-order material is detected and separated (Cassandra `Scrubber` behavior) |
| Corrupted-partition loss | ALWAYS drops (that is the whole point) | drops **only** under `--skip-corrupted`; without it, a corrupted partition is a refusal (see the deviation from Cassandra's actual default below — a NEEDS-YOU) |
| Shared code | `LossClass`, `decode_partition_at_offset_for_salvage`, the boundary-source walk | scrub CALLS salvage's module; salvage does not depend on scrub |

Scrub's `--skip-corrupted` arm is explicitly `salvage`-shaped for the partitions it drops (R3: the
loss SET for a given input, decoded through `--skip-corrupted`, equals what `salvage` would report
for that same input) — the two verbs must never classify the same byte damage two different ways.

## What this change must establish

1. **Healthy input, no `--purge`: byte-identical to `compact_sstables` with `purge_safe=false` over
   the same generations** — same oracle `cqlite compact` already has, no new claim.
2. **Out-of-order input is corrected, not silently accepted or refused**: a fixture with two
   Index/partition-position entries byte-swapped (documented offset mutation) produces a
   MAIN output that is fully ordered and verify-clean, PLUS a `-outoforder` sidecar holding exactly
   the swapped partition, in order — derived from the clean source's Index, never from CQLite's own
   sort behavior.
3. **`--skip-corrupted` loss set is provably identical to salvage's**, on the same corpus fixture,
   asserted by comparing the two verbs' structured loss lists field-for-field, not by re-deriving a
   second expectation.
4. **`--dry-run` writes nothing and reports the same thing the wet run would report**, field for
   field, including the loss set, the out-of-order verdict, and the TTL-rewrite count.
5. **`--reinsert-overflowed-ttl` matches Cassandra's actual, version-gated behavior**, which is NOT
   uniform across formats CQLite supports — see the design's version-gating table. It rewrites
   negative/overflowed local-deletion-time rows only where the source format can even HAVE one.

## Non-goals

- Tombstone purging beyond what `--purge`'s `purge_safe` contract already governs (no scrub-specific
  purge policy; it is `compact --major`'s policy, reused).
- Salvage semantics — #4196 owns partition-loss recording as a concept; scrub only ever CALLS it.
- `nodetool scrub`'s in-place mutation model: Cassandra's `StandaloneScrubber` snapshots the table,
  then obsoletes (deletes) the original SSTables and replaces them with the scrub output, in place.
  CQLite's `scrub` follows epic #4192's binding constraint instead ("no verb modifies its input") —
  it always writes to `--out`, never touches or deletes the input generations, and therefore needs
  no pre-scrub snapshot: the input IS the snapshot, permanently, until the operator deletes it
  themselves. This is a deliberate, doctrine-driven divergence, not an omission.
- Cassandra's `-n/--no-validate` (skip per-cell column-validator checks) and `-m/--manifest-check`
  (repair a Leveled Compaction Strategy manifest, standalone). Neither has a CQLite analogue:
  CQLite's schema-aware row decode IS the validation pass (no separate opt-out is possible without a
  second decode path, which no-heuristics doctrine would not want anyway), and CQLite's offline
  tools do not model a live LCS manifest.
- Counter tables. Cassandra's default (`--skip-corrupted` NOT given) is to **abort scrub entirely on
  the first corrupted partition only for counter tables** (`isCommutative`) — regular tables are
  skip-and-continue BY DEFAULT even without the flag (see the design's parity table; this is the
  single most consequential deviation this proposal surfaces — a NEEDS-YOU). CQLite's write engine
  already refuses to WRITE any `Counter` mutation (`write_engine/mod.rs:423-432`, "Counter columns
  require server-side distributed increment semantics"), so a counter table can be read but never
  round-tripped through the production writer scrub uses — scrub therefore refuses a counter-table
  input outright, unconditionally, citing that existing limitation. This is not a new restriction.
- A `--major`-equivalent "assert this is every generation" flag distinct from `--purge`: scrub always
  receives every generation of a table dir (like `compact`'s `input_dir` convention), so `--purge`
  alone carries that assertion, the same way `compact --major` does.

## Doctrine impact

- **No-heuristics (#28):** out-of-order detection compares decoded keys/clustering values using the
  schema's own comparator, never byte patterns; partition boundaries for `--skip-corrupted` come only
  from the same authoritative Index.db/Partitions.db source #4196 already established.
- **Uncompressed-write claim boundary (#1406):** scrub's output, like salvage's and `compact`'s, is
  UNCOMPRESSED only. `--help` states this. Requesting compressed output is refused
  (`Error::UnsupportedFormat`), not silently downgraded.
- **Cassandra 5.0 only:** `na`+/`nb` BIG, `da` BTI — scrub inherits the compaction path's existing
  version gates; no new gate needed.
- **Oracles (#3042):** every fixture below is Cassandra-written; the out-of-order and
  overflowed-TTL fixtures are documented byte mutations of a Cassandra-written original, with the
  expected outcome derived from the ORIGINAL's decoded bytes (Index positions, `sstabledump`),
  never from what CQLite happens to output today.

## Size

Medium (~1.8–2.4k lines incl. tests) — one PR. **Depends on.** #4196 (merged; shares `LossClass` +
decode-at-offset). **Optional, not blocking.** #4193 (`explain`) for a future `--explain`-flavored
dry-run render; this slice's `--dry-run` report is a plain structured summary, not #4193's decision
trail.
