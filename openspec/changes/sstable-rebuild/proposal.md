# sstable-rebuild — issue #4197 (epic #4192)

**Milestone:** unmilestoned (owner convention; board `Ready` → claimed). **Priority:** P1.
**Routing:** design-driven (OpenSpec + Seam 1) — a new writing verb with real latitude in what
"byte-identical" can mean per component and in the `--in-place` safety protocol. The bytes it
reads (an existing, healthy Data.db) and the bytes the reused writers emit are oracle-bound
(Cassandra source + sstabledump goldens); the recovery/refusal POLICY is a design decision.

## Why

`sstable-salvage` (#4196) recovers partitions when Data.db itself is damaged; it is the remedy
when the *primary* data is gone. This issue is the companion for the far more common case: Data.db
is fine, but one or more of its *derived* components — Index.db, Summary.db, Filter.db,
Digest.crc32, TOC.txt, CRC.db, Statistics.db — went missing or corrupt (a botched copy, a crashed
flush that never reached TOC.txt, a truncated transfer). Today an operator has no tool that
regenerates only the missing pieces from the one component that is always authoritative: Data.db.
`nodetool rebuildindex` reindexes secondary indexes, not SSTable components; there is no Cassandra
CLI verb for this at all.

CQLite already owns every writer these components need (`writer/index_writer.rs`,
`summary_writer.rs`, `filter_writer.rs`, `digest_writer.rs`, `toc_writer.rs`, `crc_writer.rs`,
`stats_writer/`, BTI `partitions_writer.rs` + `bti_state.rs`), a byte-parity-proven read path for
both BIG (`nb`) and BTI (`da`), and `verify.rs`'s existing chunk-CRC + Digest checks to confirm a
rebuild's own output. The single new mechanism this issue adds is a **component regenerator that
derives each requested output from the EXISTING, UNCHANGED Data.db** rather than from the
higher-level `SSTableWriter::write_partition` flush path salvage uses — see design.md §D1 for why
those are not interchangeable.

## What changes

**Library (`cqlite-core`).** A `rebuild` module under `storage/write_engine/` exposing
`rebuild_components(data_db_path, schema, requested: &[Component], options) -> Result<RebuildReport>`.
It opens Data.db read-only, walks it once via the same decompression-transparent structural read
the compaction scan already performs, and for each REQUESTED component drives that component's
existing low-level writer directly (not through `SSTableWriter::write_partition`, which would
rewrite Data.db itself). Statistics rebuild is the one component that needs full row/cell decode;
every other requested component needs only partition/row *structure* (keys, offsets, byte extents,
tombstone presence) — see design.md's per-component table for exactly what each needs and from
where.

**CLI (`cqlite-cli`).** `cqlite rebuild <Data.db | table-dir> --components
index,summary,filter,digest,toc,crc,statistics --out <dir> [--in-place]`, schema resolved through
the same `--schema` global `query`/`salvage` use. `--in-place` is accepted but REFUSES with a
named-dependency error until #4195 ships `verify --mode audit` (today's `VerifyMode` is
`Quick`/`Full` only — confirmed in `verify.rs`; there is no `Audit` variant yet). `--out` ships
first and is the only mode this change implements end-to-end.

**Rebuild manifest.** A JSON document, also rendered as text, naming: the input, the requested vs.
actually-regenerated component set, per-component `recovered | recomputed | lost` field
classification (Statistics only — every other component is either fully regenerable or refused
whole), and — when refused — the reason (bad Data.db chunk CRC, mid-partition decode error, or an
unresolvable schema) naming the offset and pointing to `salvage` (#4196) when Data.db itself, not
just a derived component, is what is actually damaged.

## What this change must establish

1. **Byte oracle, per component, with the non-determinism named up front.** For Index.db, Digest,
   TOC, and uncompressed-BIG CRC.db: byte-identical to the Cassandra-written original, because each
   is a deterministic function of bytes already on disk (design.md §D2–D5). For Summary.db and
   Filter.db: byte-identical **only when the table's `min_index_interval` and
   `bloom_filter_fp_chance` are recoverable** — and design.md §D6 reports a genuine finding: CQLite's
   own `SSTableWriter` constructor currently threads `bloom_filter_fp_chance` from the schema's WITH
   clause (so it IS recoverable when `--schema` carries the original table definition) but
   **hardcodes `min_index_interval` to Cassandra's default 128 unconditionally** — there is no
   existing plumbing that threads a non-default value through at all. Rebuild inherits that gap
   until it is closed (task 1.4); until then a non-default `min_index_interval` table's rebuilt
   Summary.db is `recomputed` (default-valued), not byte-identical, and the manifest says so.
2. **Statistics rebuild is opt-in and lossy by declaration.** `StatisticsMetadata`'s aggregate
   fields (min/max timestamp, min/max local-deletion-time, min/max TTL, partition/row/column counts,
   both estimated histograms, first/last key, has-partition-level-deletions) are `recomputed` from a
   full structural scan of Data.db. `repaired_at`, `pending_repair`, `is_transient` are `recovered`
   when the original Statistics.db is readable, else `lost` (default-valued). CQLite's
   `StatisticsMetadata` struct carries no origin-host or compaction-ancestry fields at all — those
   are unconditionally `lost`, confirming the issue's own acceptance criterion rather than
   discovering a new gap.
3. **Refuses on a damaged Data.db, not just a damaged derived component.** A chunk-CRC failure or
   mid-partition decode error while walking Data.db means Data.db itself cannot be trusted as the
   source of truth; rebuild refuses and names `salvage` (#4196) as the remedy — it never emits a
   derived-component set describing data it could not fully read.
4. **`--in-place` is gated on #4195** and, once available, follows the same
   temp-file-then-atomic-rename-per-component protocol salvage's `--out` write path already
   establishes, verified post-write with `verify --mode audit` before any original component is
   replaced.
5. **Wiring evidence from the binary**, on both formats (BIG uncompressed CRC.db rebuild; BTI has no
   CRC.db — inline per-chunk CRC only), both compressed and uncompressed Data.db input for the
   components that apply to each.

## Non-goals

- Rebuilding Data.db itself (impossible — it is the one input this tool trusts; that is what
  `salvage` (#4196) exists for when Data.db is what is damaged).
- CompressionInfo.db regeneration — not in the issue's component list; a compressed input's
  CompressionInfo.db is read, never rewritten.
- Recovering `repaired_at` / `pending_repair` / origin-host / compaction-ancestry when the original
  Statistics.db is unreadable — declared lossy, not a defect to fix later.
- A data-dir-wide sweep (#4194) or format conversion (#4202).
- Anything beyond `--out` until #4195 lands `verify --mode audit`; `--in-place` ships as a
  documented refusal in this change, wired for real once the dependency lands.

## Impact statements (openspec rules)

- **No-heuristics (#28):** every regenerated component is a deterministic function of Data.db bytes
  plus (for Filter/Summary/Statistics-repair fields) authoritative schema/Statistics metadata; no
  byte-pattern guessing, no header hunting (mirrors salvage's R4.3 no-resync-scan discipline).
- **Public surfaces:** `cqlite-core` gains `pub mod rebuild` (unconditional; pub-surface guard); CLI
  gains one verb; Python/Node untouched.
- **<128 MB:** one partition's structural state resident at a time on the read side; each
  low-level component writer already streams (Index.db, CRC.db) or holds one component in memory by
  design (Summary.db, Filter.db, Statistics.db, TOC.txt — all small relative to Data.db).
- **Gate:** new test targets must be named in `core-tests` / `write-tests` / `cli-tests` (#3522).
  Corruption `.db` binaries are gitignored — corpus tests skip-clean when absent, FAIL when
  present-but-wrong, hard-required under `CQLITE_REQUIRE_FIXTURES=1` (#1094 doctrine).
