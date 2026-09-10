# Design — sstable-rebuild (issue #4197)

## D1. Why rebuild is NOT `SSTableWriter::write_partition` replayed against Data.db

`sstable-salvage` (#4196) reuses `SSTableWriter::write_partition` because salvage's job is to
produce a **fresh generation** — a new Data.db plus everything derived from it is fine to emit
together, in lockstep, exactly the way a flush or compaction does.

Rebuild's job is different: Data.db is **healthy and must not change** (byte-for-byte, including
its existing generation number and on-disk path), and only the requested derived components are
regenerated around it. `write_partition` cannot be reused for this because it always creates a NEW
Data.db as a side effect of computing everything else:

```
cqlite-core/src/storage/sstable/writer/mod.rs:690-697
let (data_offset, promoted_blocks, emit_counts) =
    self.data_writer.write_partition_with_index_blocks(&key, &mutations, &schema, ...)?;
...
let entry_info = self.index_writer.add_partition_with_promoted(&key, data_offset, &promoted_blocks)?;
```

`data_offset` and `promoted_blocks` (the Index.db promoted-index / column-index block boundaries)
are **computed as a side effect of serializing rows into the NEW Data.db** — `write_partition_with_index_blocks`
(`writer/data_writer/partition.rs:328`) interleaves "measure how many bytes this row group takes"
with "write those bytes." There is no existing entry point that computes promoted-index block
boundaries against bytes that are *already on disk* at fixed offsets. Two consequences:

- Replaying `write_partition` against a compressed original would silently violate the #1406
  uncompressed-only production-write boundary and — worse — produce a Data.db that is NOT the
  original (different bytes, likely different length), making the "rebuild only touches derived
  components" claim false.
- Even for an uncompressed original, `write_partition` would re-derive `data_offset` from ITS OWN
  write position, not from where the partition ACTUALLY starts in the existing file. Feeding it the
  real offsets is not what the API does — it always advances its own `DataWriter` position.

**Rebuild therefore reads Data.db structurally (key boundaries, row/unfiltered byte extents,
tombstone presence) via the SAME decompression-transparent decode the compaction scan already uses
for logical-offset resolution, and drives each component's low-level writer (`IndexWriter`,
`SummaryWriter`, `FilterWriter`, `DigestWriter`, `TocWriter`, `crc_writer::StreamingCrc`,
`stats_writer`) directly with those extents — never through `SSTableWriter::write_partition`.** This
is new code (new files, per the file-size campsite rule) built on existing read-path primitives, not
a call-through to the existing write path. The one place this genuinely simplifies rather than
complicates: because we are *measuring* already-serialized bytes instead of *producing* them, the
promoted-index block-boundary computation (chunk at `column_index_size_in_kb`, default 4 KiB) needs
only byte-extent tracking per row/unfiltered marker — not full cell-value decode — for every
component except Statistics.

## D2. Per-component rebuild table

| Component | Source writer | What it needs | Byte-equal achievable? | Notes / Cassandra citation |
|---|---|---|---|---|
| **Index.db** (BIG) | `IndexWriter::add_partition_with_promoted` (`index_writer.rs:380`) | Per-partition (key, `data_offset`) in file order + promoted-index blocks for wide partitions (≥ 2 column-index blocks, gated exactly like `write_entry`, `index_writer.rs:405`) | **Yes**, for an uncompressed input where `data_offset` is the physical file offset, and for a compressed input where it is the LOGICAL (decompressed-stream) offset the read path already resolves transparently. Requires a NEW byte-extent-aware partition/row walk (§D1) — not existing today. | Format: `docs/sstables-definitive-guide` Ch.6. Boundary source is the SAME authoritative walk `salvage`'s `boundaries.rs` uses for partition enumeration (task 1.1 reuses it), extended to also track intra-partition row byte extents. |
| **Summary.db** | `SummaryWriter::new(min_index_interval)` / `add_entry` (`summary_writer.rs:158,254`) | Partition keys sampled every `min_index_interval` Index.db entries, plus `min_index_interval`/`sampling_level` header fields | **Conditional.** `sampling_level` for a freshly rebuilt (never-downsampled) Summary.db is always `BASE_SAMPLING_LEVEL` = 128 (`summary_writer.rs:74-78`, independent of `min_index_interval`) — byte-equal for that field always. `min_index_interval` is genuinely non-recoverable today: `SSTableWriter::with_format_and_registry` **hardcodes** `summary_sample_interval = 128` (`mod.rs:520-526`) with no schema or Statistics.db plumbing for a non-default value — confirmed no `min_index_interval`/`max_index_interval` field exists anywhere in `cql_parser.rs`'s WITH-clause option parsing. Cassandra's Summary.db is the ONLY per-sstable place this value is recorded (`summary_writer.rs:33-36` doc comment) — Statistics.db does not carry it. So when Summary.db is itself the damaged/missing component AND the table's real `min_index_interval` was ever set to something other than 128, rebuild cannot recover it without new schema-parsing plumbing (task 1.4); until then it is `recomputed` (Cassandra's own default) and the manifest states this explicitly, never silently. |
| **Filter.db** | `FilterWriter::new(path, expected_keys, fp_chance)` (`filter_writer.rs:103`) | Every partition key + `bloom_filter_fp_chance` | **Yes when `--schema` resolves a schema carrying the original `WITH bloom_filter_fp_chance = …`.** `SSTableWriter::with_format_and_registry` already threads this via `Self::bloom_filter_fp_chance(schema)` reading the WITH-clause option cqlite's CQL parser preserves into `schema.comments` (`mod.rs:507-517`; `cql_parser.rs:1790-1881`) — this is a genuine correction to the issue text's assumption that fp_chance comes from Statistics.db: in CQLite's actual write path it comes from the SCHEMA, and Statistics.db is not consulted at all. Falls back to Cassandra's default 0.01 when the schema lacks the option, classified `recomputed` in that case, `recovered` when the schema states it. `fp_chance == 1.0` means no Filter.db at all (`AlwaysPresentFilter`) — reproduced identically. |
| **Digest.crc32** | `DigestWriter::write_for_file` / `compute_crc32` (`digest_writer.rs:151,202`) | CRC32 (IEEE) of the (unchanged) Data.db bytes | **Always yes.** Pure function of Data.db's existing bytes; no partition walk needed at all. Matches `verify.rs`'s own check (`check 2`, `verify.rs:756-813`), so a rebuilt Digest.crc32 passing `verify --mode full`'s digest check is definitional. |
| **TOC.txt** | `TocWriter::write` (`toc_writer.rs:115`) | The final component set present after rebuild (existing untouched components + regenerated ones); order is documented as not mattering (`toc_writer.rs:60`) | **Always yes** in content (every present component named, self-referential); not meaningfully "byte-identical" to a specific historical TOC.txt since Cassandra's own writer order is not a promised invariant either — the spec asserts set-equality, not byte-equality, for this one component. | TOC.txt is the publication barrier — written LAST, after every other requested component (`--out` mode) or after every atomic rename (`--in-place`, once #4195 lands). |
| **CRC.db** (uncompressed BIG only) | `crc_writer::StreamingCrc`, `CRC_CHUNK_SIZE = 64 * 1024` (`crc_writer.rs:67`) | Chunked recompute over the (unchanged) Data.db bytes at Cassandra's fixed `SequentialWriterOption` buffer-size chunking | **Always yes** for uncompressed BIG input — pure function of existing bytes at a FIXED, non-configurable chunk size (no per-table parameter involved). **N/A for compressed input** (chunk CRC is inline in Data.db itself, not a separate component — `sstable-salvage/design.md` §D1) and **N/A for BTI** (`da` has no separate CRC.db; verified via `verify.rs`'s component classification, `verify.rs:661`). | Cassandra `ChecksumWriter` / `SequentialWriterOption.Builder.bufferSize` default, cited in `crc_writer.rs:23-24,63`. |
| **Statistics.db** | `stats_writer/` (`metadata.rs`, `components.rs`, `serialization_header.rs`) | A FULL structural + cell-level scan of Data.db to recompute aggregates; original Statistics.db (if readable) for the 3 non-recomputable fields | **Declared lossy, per-field (see proposal.md §2).** `recomputed`: min/max timestamp, min/max local-deletion-time, min/max TTL, partition/row/column counts, `estimated_partition_size`/`estimated_cell_count` histograms, first/last key, `has_partition_level_deletions`, tombstone-drop-time histogram — every field `StatisticsMetadata` (`stats_writer/metadata.rs:114-194`) derives purely from folding over decoded mutations, i.e. from Data.db alone. `recovered` (only if original Statistics.db parses): `repaired_at`, `pending_repair`, `is_transient`. `lost` unconditionally: origin host, compaction ancestry — `StatisticsMetadata` has no fields for either, so CQLite's OWN compaction path already drops them; rebuild is not introducing a new gap. | This is the only component needing a genuine per-mutation decode (cell values, not just byte extents) — same decoder salvage already drives (`decode_partition_at_offset_for_salvage` / the compaction-row path), reused here read-only. |
| **BTI Partitions.db / Rows.db** | `PartitionsTrieWriter` / `RowsTrieWriter` (`partitions_writer.rs:131,908`); deferred-payload bookkeeping in `bti_state.rs` | Same partition/row byte-extent walk as BIG Index.db, but keyed to the trie leaf shape (`PartitionTrieEntry`) instead of the flat Index.db record | **Same conditional as BIG Index.db** — byte-identical when the walk resolves the same logical offsets the ORIGINAL BTI write would have produced. `bti_state.rs`'s `PendingBtiPartition`/`PendingRowIndex` deferred-payload pattern (a `RowsOffset` is only known once `Rows.db` itself is serialized) is REUSED as-is; only the SOURCE of `raw_key`/`data_offset`/row-index blocks changes, from "captured while writing Data.db" to "read from the existing Data.db." | `iterate_partitions_in_bti_file` (`bti/parser/traversal.rs:384`) is the existing BTI partition-trie READ side (used to verify/read a trie that already exists); rebuild instead WRITES a new one, so it needs the byte-extent walk over Data.db, not this reader. |

## D3. Failure contract

| Situation | Behaviour | Exit |
|---|---|---|
| every requested component regenerated | requested components written (or `--in-place` renamed once #4195 lands); manifest `refused: null` | 0 |
| Data.db chunk-CRC failure or mid-partition decode error while walking | REFUSE: nothing written; manifest `refused: {reason: "data-corrupt", remedy: "cqlite salvage (#4196)", offset: <n>}` | 2 |
| schema unresolvable, or a requested component that needs schema (Filter, Statistics-if-recovering-schema-derived-fields) has none | usage error | 1 |
| `--in-place` requested before #4195 ships `verify --mode audit` | usage error naming the dependency; nothing written | 1 |
| `--in-place`, `verify --mode audit` on the freshly-rebuilt output set FAILS | REFUSE: originals untouched, no rename performed | 2 |

There is no "partial success" exit code (unlike salvage's exit 3) — rebuild's unit of work is the
WHOLE requested component set for one Data.db; a failure on any requested component refuses the
entire run rather than emitting a partial set that silently omits one (a partially-regenerated
component set is worse than none, since a subsequent tool would trust its presence). The manifest's
`classification` map is where partial *field-level* honesty lives (Statistics.db's
recovered/recomputed/lost), not the process exit code.

## D4. `--in-place` protocol (gated on #4195; documented now, refused until then)

1. Rebuild ALWAYS writes to `<name>.rebuild.tmp` files first, one per requested component, never
   touching the original path directly — identical in spirit to salvage's `--out` write, just
   targeting a temp sibling of the original instead of a separate directory.
2. Once every requested component is written to its `.rebuild.tmp` path, run `verify --mode audit`
   (#4195) against the CANDIDATE set: the freshly-written temp components PLUS the untouched
   originals for everything not requested (Data.db always untouched).
3. Only on a clean audit: atomic rename each `.rebuild.tmp` over its real path, one `rename(2)` per
   component. TOC.txt is renamed LAST (same publication-barrier discipline as a fresh flush).
4. A crash between renames leaves a MIX of old and new components on disk for whichever step didn't
   complete — the same partially-visible-SSTable window `TocWriter`'s own doc comment already
   accepts for a normal flush (`toc_writer.rs:8`: "SSTable is not visible until TOC.txt exists"), so
   a reader gated on TOC.txt presence never observes the intermediate state. Tested with a fault
   injected between renames (mirrors salvage tasks.md's declared-gap note for the equivalent test —
   NOT deferred here; see tasks.md group 4).
5. On ANY audit failure or write error, the `.rebuild.tmp` files are removed and the originals are
   never touched — REFUSE, exit 2.

## D5. Manifest schema (stable; the CLI text form is a rendering of it)

```json
{ "input": "<Data.db path>", "output": "<dir | in-place>", "format": "nb|da",
  "compressed_input": true, "requested": ["index","summary","filter","digest","toc","crc","statistics"],
  "regenerated": ["index","summary","filter","digest","toc","statistics"],
  "skipped_not_applicable": [{"component": "crc", "reason": "compressed input uses inline chunk CRC, no CRC.db"}],
  "classification": {
    "summary": {"min_index_interval": "recomputed", "sampling_level": "recovered"},
    "filter": {"bloom_filter_fp_chance": "recovered"},
    "statistics": {
      "min_timestamp": "recomputed", "max_timestamp": "recomputed",
      "repaired_at": "recovered", "pending_repair": "lost", "is_transient": "recovered",
      "origin_host": "lost", "compaction_ancestry": "lost"
    }
  },
  "refused": null | {"reason": "data-corrupt|schema-unresolvable|in-place-unsupported|audit-failed",
                       "remedy": "…", "offset": 88192 },
  "now": "<RFC3339 of the run>", "cqlite_version": "…" }
```

## D6. Oracles (all Cassandra-written; expectations derived from the format, never from CQLite)

| Case | Fixture | Expected result derived by |
|---|---|---|
| healthy BIG uncompressed, all 7 components deleted then rebuilt | any committed `test_basic` table, uncompressed variant | rebuilt Index.db/Digest/TOC/CRC.db byte-equal to the originals Cassandra wrote; Summary.db/Filter.db byte-equal WHEN the fixture's schema `WITH` clause states `bloom_filter_fp_chance` and used the default `min_index_interval` (both true for every current committed schema — confirmed via `test-data/schemas/*.cql` grep in premise task 0.2) |
| healthy BIG compressed | any committed `test_comp`-style table | Index.db logical offsets recomputed via the decompression-transparent walk byte-equal to the original; CRC.db skipped (`skipped_not_applicable`, inline CRC only) |
| healthy BTI | any committed `test_da` table | Partitions.db/Rows.db byte-equal via the same walk, keyed to the trie leaf shape |
| Data.db chunk-CRC flip | `test_comp_corrupt/data_db_bit_flip` (captured Cassandra verdict `corrupt`) | refused with `data-corrupt`, naming `salvage`, at the exact chunk offset the clean source's `CompressionInfo.db` chunk table identifies as flipped |
| Statistics.db present but original had `repaired_at != 0` | a temp copy with Statistics.db deleted, rebuilt, compared against the CLEAN source's OWN `repaired_at` (never CQLite's default) | `repaired_at` in the rebuilt output equals the clean source's, proving `recovered` classification is not vacuous when the original component happens to still exist as a READABLE input for classification testing (Statistics.db is deleted from the OUTPUT target, but the test independently re-parses the clean source to assert the recovered value is right — not merely present) |
| non-default `min_index_interval` (synthetic — no committed fixture has one) | schema constructed in-test with a non-128 `min_index_interval` comment option | `classification.summary.min_index_interval == "recomputed"`, rebuilt value is Cassandra's default 128, NOT the schema's stated value — proves the documented gap is enforced honestly rather than silently producing a wrong byte-equal claim |

## D7. CLI shape and campsite

`cqlite-cli/src/commands/rebuild.rs`, wired like `salvage.rs`/`read_commitlog.rs`;
`Commands::Rebuild` in `cli_types.rs`. Schema resolution and table-dir generation discovery reuse
the same helpers salvage's CLI half already established (`write.rs::load_compaction_table_schema`,
a format-aware generation walker). New core module is new files only
(`storage/write_engine/rebuild/`: `mod.rs`, `boundaries.rs` — extended byte-extent walk shared in
spirit with, but not literally reusing, salvage's `boundaries.rs` — `components.rs` driving the
per-component low-level writers, `statistics.rs` for the Statistics-only full decode path), so the
file-size ratchet is not engaged by construction; `cli_types.rs` growth is one variant.
