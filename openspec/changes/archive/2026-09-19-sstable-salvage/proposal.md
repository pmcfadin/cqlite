# sstable-salvage — issue #4196 (epic #4192, first slice)

**Milestone:** unmilestoned (owner convention; board `Ready` → claimed). **Priority:** P1.
**Routing:** design-driven (OpenSpec + Seam 1) — a new writing verb with real latitude in its
recovery policy, output shape and failure contract. The bytes it emits are oracle-bound (the
production writer's existing Cassandra parity) and the bytes it reads are oracle-bound (Cassandra
`sstableverify` verdicts and Cassandra-written fixtures).

## Why

When an SSTable is damaged, an operator today has `sstableverify` to tell them it is bad, `nodetool
scrub` which needs a running node and the table's schema loaded, and nothing that says *which*
partitions are gone and hands back the rest. Owner ruling 2026-09-09: this is the thing people need
at 3 a.m. and the first slice of the SSTable tool.

CQLite has every primitive: a Cassandra-parity reader for BIG (`nb`) and BTI (`da`), per-chunk CRC
validation (inline for compressed, `CRC.db` for uncompressed, #1396), Index.db and BTI partition
trie readers that name every partition's data offset, the production `SSTableWriter` (uncompressed
output, byte-parity-proven through compaction), and a corruption corpus with captured Cassandra
verdicts (`test-data/datasets/corruption/test_comp_corrupt`, #1236) plus a real-fixture byte-flip
harness (`cqlite-core/tests/support/corrupt_byte_fixture.rs`, #3782/#3928).

## What changes

**Library (`cqlite-core`).** A `salvage` module under `storage/write_engine/` exposing
`salvage_sstable(input, output_dir, schema, options) -> Result<SalvageReport>`. It enumerates the
partitions of ONE input generation from an **authoritative boundary source** (BIG: `Index.db`
entries; BTI: the `Partitions.db` trie leaves), decodes each partition at its named offset through
the existing compaction-row decoder, and writes every partition that decodes COMPLETELY through the
production writer as a fresh uncompressed generation with all components. A partition that fails —
chunk CRC, decode error, key at the offset not matching the index — is recorded as a **loss** with
its key, offset, chunk range and error class, and skipped. The unit of loss is the partition
(design §D2). The input is never opened for writing.

**CLI (`cqlite-cli`).** `cqlite salvage <Data.db | table-dir> --out <dir> [--manifest <path>]
[--out-format text|json]` with the schema resolved through the same `--schema` global `query` uses.
A table dir salvages every generation separately, one output generation per input generation, never
merged. Exit `0` when nothing was lost, `3` when the output was written with losses, `2` when nothing
could be recovered or no boundary source is readable; `2` leaves no `Data.db` in the output dir.

**Loss manifest.** A JSON document, also rendered as text, that names for the input: the boundary
source used, partitions total / recovered / lost, every loss (key, offset, chunk indices, error
class, message), the component-level verify findings observed on the way in, and — when salvage
refused — the single reason and the remedy (`rebuild` #4197 when only the boundary source is bad).

## What this change must establish

1. **Salvage of a healthy SSTable is a no-purge compaction of it** — byte-identical output to
   `compact_sstables` with purging disabled, and dump-equal to the fixture's Cassandra-written JSONL
   golden. An empty manifest is the affirmative form (`losses: 0 RECOGNISED`).
2. **On a damaged SSTable the survivor set is exactly what the format says survives**: the test
   derives the expected loss set from the healthy source's Index/trie positions and chunk table
   (which partitions' byte ranges intersect the mutated chunk), never from CQLite's behaviour.
3. **Partition atomicity is enforced, not hoped for**: a partition whose LATER rows fail to decode
   is lost entirely, because keeping its earlier rows can resurrect data a later range/row tombstone
   in the same partition deleted (§D2).
4. **Fail closed on the boundary source**: a corrupt Index.db / Partitions.db means "boundaries
   unknown" and salvage refuses with the `rebuild` remedy; it never resynchronises by scanning
   for a plausible header (#3928 measured that inventing partitions).
5. **Wiring evidence from the binary**, on both formats, both compressed and uncompressed input.

## Non-goals

- Merging generations, purging tombstones, or any reconciliation across inputs (that is
  `scrub`, #4198). Salvage preserves every cell and tombstone it can read, verbatim.
- Row-level or cell-level partial recovery of a damaged partition (§D2 explains why not).
- Rebuilding a damaged Index/Partitions component to make salvage possible (#4197 `rebuild`).
- Compressed output (#1406 boundary; `--help` says so).
- Repairing the input in place. Salvage only ever writes to `--out`.
- A data-dir-wide sweep with severities (#4194); salvage takes one table dir or one Data.db.

## Impact statements (openspec rules)

- **No-heuristics (#28):** partition boundaries come only from Index.db / Partitions.db; the key
  decoded at an offset is cross-checked against the boundary source's key; no header hunting.
- **Public surfaces:** `cqlite-core` gains `pub mod salvage` (unconditional; pub-surface guard);
  CLI gains one verb; Python/Node untouched.
- **<128 MB:** one partition resident at a time on the way in (the existing per-partition decoder)
  and on the way out (the writer's `write_partition`); `test_wide_rows` is covered by the
  `memory-budget` lane in R6.
- **Gate:** new test targets must be named in `core-tests` / `write-tests` / `cli-tests` (#3522).
  Corruption `.db` binaries are gitignored — the corpus tests skip-clean when absent, FAIL when
  present-but-wrong, and are hard-required under `CQLITE_REQUIRE_FIXTURES=1` (#1094 doctrine).
