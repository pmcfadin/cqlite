# sstable-extract-split — issue #4199 (epic #4192)

**Milestone:** 0.18 (owner ruling 2026-09-20: the whole epic #4192 ships in one release).
**Priority:** P2. **Routing:** design-driven (OpenSpec + Seam 1) — a new writing verb whose engine
choice, selection-predicate shapes, and split-boundary policy have real latitude. The bytes it
reads (existing, healthy SSTables) and the bytes the reused writer/merger emit are oracle-bound
(Cassandra source + sstabledump/`SELECT *` parity, the #1918 point-vs-full lane, and #3890's
both-directions column comparison); the CLI surface and split-boundary policy are design decisions.

## Why

An operator who needs ONE partition, a token range, or a named key list out of a table today has no
tool short of standing up a cluster and running `SELECT` — or hand-picking SSTables and hoping the
partition isn't split across generations. And an operator who wants to shrink one oversized
generation (a hot-partition table whose SSTables blew past a target size, or a table being
repartitioned before a restore) has no offline way to divide it. Both are pure compositions of
primitives CQLite already owns and that #4196 (`salvage`) and #4197 (`rebuild`) already established
the conventions for: the point-read reconciling merger (`build_single_partition_merger`, zero
purge), the authoritative partition-boundary walk (`Index.db` entries / the `Partitions.db` trie,
the same walk salvage's `boundaries.rs` and rebuild's byte-extent walk use), the decode-at-offset
primitive salvage already drives (`decode_partition_at_offset_for_salvage`), the production
`SSTableWriter` (uncompressed output, #1406 boundary), and `verify --mode full` as the self-audit
every REPAIR-family writer now runs before declaring success.

## What changes

**Library (`cqlite-core`).** An `extract_split` module under `storage/write_engine/`, split into two
independent entry points that share the boundary-walk and decode-at-offset primitives salvage
already built:

- `extract_partitions(table_dir, selection: Selection, schema, options) -> Result<ExtractReport>`
  where `Selection` is `Key(Vec<u8>) | TokenRange(i64, i64) | KeySet(Vec<Vec<u8>>)`. Default mode
  reconciles the selected partition(s) across every generation via
  `build_single_partition_merger` (no purge — the same point-read semantics `query` already uses,
  #1918-consistent) and writes ONE output generation. `--raw` instead walks each input generation's
  boundary source independently, decodes each selected partition at its offset (no reconciliation,
  no merge), and writes one output generation per input generation that held at least one selected
  partition.
- `split_sstable(input_path, boundary: SplitBoundary, out_dir, schema) -> Result<SplitReport>` where
  `SplitBoundary` is `Parts(u32) | MaxBytes(u64)`. Walks the ONE input generation's boundary source
  sequentially (partitions arrive in ascending Murmur3-token order by construction — the format's
  own invariant, never an assumption this tool adds) and rolls to the next of N output
  `SSTableWriter`s when the running partition count or accumulated Data.db byte span crosses its
  share, so every part's token range is disjoint and ascending by construction, not by a
  post-hoc sort. No reconciliation, no merge, no purge — split divides one generation, it does not
  compact it.

**CLI (`cqlite-cli`).** `cqlite extract <table-dir> (--partition <key> | --token-range <a>,<b> |
--keys-file <f>) --out <dir> [--raw] [--table <name>] [--manifest <path>] [--out-format text|json]`
and `cqlite split <Data.db | table-dir> (--parts <N> | --max-bytes <B>) --out <dir> [--table <name>]
[--manifest <path>] [--out-format text|json]`, both resolving `--schema` the way `salvage`/`rebuild`
do. Both verbs reuse salvage's hardened destructive-path guard (issue #4196 round-23 findings
F1/F4/F5) rather than re-deriving it a third time — promoted out of `commands/salvage/write_guard.rs`
into a shared `commands/write_guard.rs` in this change (task 0.4).

**Manifests.** `extract` and `split` each emit a JSON manifest (also rendered as text) naming the
input(s), the selection/boundary that was applied, the output generation(s) written, and — for
`split` — each part's observed token range and row/partition counts so the disjoint-ascending
property is directly inspectable, not just implicitly true.

## What this change must establish

1. **Extract of a partition reconciled across generations equals `SELECT *` for that key**, under
   both `CQLITE_READ_PATH=point` and `=full` (#1918), columns compared in both directions (#3890) —
   `cqlite-core/tests/issue_4199_extract_parity.rs`.
2. **`--raw` preserves every generation's own bytes verbatim, including tombstones**: each output
   generation's dump for the selected key(s) equals that INPUT generation's own Cassandra-written
   JSONL golden rows for the same key, never CQLite's read-reconciled view.
3. **A key absent from every generation is a NAMED failure, not a silent no-op**: `--keys-file` with
   any key present in none of the input's generations reports that key by name and exits 3 — a
   request for 500 keys that silently recovers 480 is exactly the resurrection-adjacent failure mode
   #4159 named for a different verb.
4. **Split partitions the source exactly once, provably**: `union(parts) == source` by both row
   count and per-partition dump, ranges are disjoint and strictly ascending (asserted, not assumed),
   each part independently passes `verify --mode full`, and each part's Statistics describe only
   that part's own survivors (recomputed from what was actually written, matching salvage's D4
   convention).
5. **Neither verb ever touches its input.** sha256 of every input component is unchanged
   before/after; `--out` must be empty going in (the salvage/rebuild convention).

## Non-goals

- Compressed output (#1406 boundary; both `--help` texts state this).
- Loss-tolerant recovery of a damaged source. `extract`/`split` assume a structurally healthy
  boundary source and a decodable partition; a decode/CRC failure on a requested partition is a
  refusal naming the partition and pointing at `salvage` (#4196) or `rebuild` (#4197) as the remedy,
  never a silently-skipped, partially-successful output the way salvage's own loss manifest is.
  (Open question to the owner below — this is the one place a "skip and report" policy, mirroring
  salvage, was seriously considered and rejected for scope reasons; flagging in case that reasoning
  is wrong for this verb.)
- Loading the output into a live cluster (`sstableloader` is the documented next step, same as
  every other REPAIR-family verb's manifest text says).
- `split --max-bytes` targeting exact byte precision — it rolls over at the first partition boundary
  at or past the threshold, same "structural unit, never mid-partition" discipline #4196's D2 uses.
- A data-dir-wide sweep (#4194) or format conversion (#4202).

## Impact statements (openspec rules)

- **No-heuristics (#28):** partition boundaries and token ordering come only from the authoritative
  Index.db/Partitions.db boundary source, the same one salvage/rebuild already established as
  authoritative; no header scanning, no byte-pattern guessing for either verb.
- **Uncompressed-write boundary (#1406):** both verbs' output is uncompressed; `--help` states it.
- **Cassandra 5.0 only:** BIG `na`/`nb` and BTI `da`; `BigVersionGates`/`BtiVersionGates` already
  reject earlier versions at `SSTableReader::open`, inherited unchanged.
- **Oracles never CQLite alone (#3042):** every acceptance test's expected value is derived from a
  Cassandra-written fixture, a real `sstabledump`/`SELECT` output, or the source's own authoritative
  boundary metadata — never from what CQLite happens to produce today.
- **Public surfaces:** `cqlite-core` gains `pub mod extract_split` (unconditional; pub-surface
  guard); CLI gains two verbs; Python/Node untouched.
- **<128 MB:** one partition resident at a time on read (the existing decode-at-offset / merger
  primitives) and on write (`write_partition`'s existing streaming); `split`'s N concurrently-open
  writers are one `SSTableWriter` each, opened and closed sequentially (never N-at-once), so memory
  stays bounded in N.
- **Gate:** new test targets named in `core-tests`/`write-tests`/`cli-tests` (#3522). Fixtures that
  are gitignored corpus binaries skip-clean when absent, hard-required under
  `CQLITE_REQUIRE_FIXTURES=1` (#1094 doctrine).

## Open product decisions for the owner

See the report below — token-range inclusivity convention, `--keys-file` format for composite keys,
and the "refuse vs. skip-and-report" policy on a damaged partition during extract/split are all
flagged there with a recommendation, not decided here.
