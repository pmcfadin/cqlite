# Design — sstable-salvage (issue #4196)

## D1. The plan is the index: enumerate, decode-at-offset, write-or-record

```
boundary source (BIG Index.db entries | BTI Partitions.db leaves)
      │  (key, data_offset) in file order
      ▼
for each partition:
   decode partition at data_offset via the compaction-row decoder ──► Ok(rows)  ──► cross-check key ──► writer.write_partition
                                                                 └──► Err(e)    ──► Loss{key, offset, chunks, class(e)}  (skip)
      ▼
SalvageReport { boundary_source, totals, losses[], component_findings[], refused: Option<Refusal> }
```

- The boundary source is authoritative (the format's own partition index). `IndexReader::
  get_partition_entries` already yields BIG entries with data positions; BTI needs a full walk of
  the `Partitions.db` trie yielding every leaf's key and data position — premise task 0.3 confirms
  whether that walk exists or is the one new primitive.
- Decode-at-offset reuses the single-partition compaction decode the point-read path uses
  (`SinglePartitionCompaction`), not a fresh parser. Every partition is decoded to `CompactionRow`s
  and converted with the same `merge_entry_to_mutation` path compaction uses, so salvage output for
  a healthy input is a no-purge compaction of that input (R1).
- Chunk pre-flight: before the loop, salvage validates every chunk CRC (inline CRC for compressed
  input; `CRC.db` for uncompressed BIG, #1396) and records the set of bad chunk indices as
  component findings. A partition whose byte range intersects a bad chunk is a loss with class
  `chunk-crc` even if its bytes happened to decode — Cassandra's chunk CRC is the authority on what
  is trustworthy, and a decodable-but-CRC-failed chunk is exactly the #3782 case (a flipped byte
  that still parses).
- Key cross-check: the decoded partition's key must equal the boundary source's key at that
  offset, else loss class `key-mismatch` (the index and the data disagree; neither can be trusted
  for that slot).

## D2. Partition atomicity (the one recovery-policy decision, and why)

A partition whose decode fails at row *k* is lost whole; rows `0..k` are not written. Reason: a
Cassandra partition's later bytes can carry a row deletion, a range tombstone, or a newer cell
version that shadows earlier rows. Emitting the prefix would write the shadowed data WITHOUT its
tombstone — the resurrection bug, manufactured by the recovery tool. #3782 measured a mid-partition
decode failure producing "102 rows where 100 existed, 2 keys lost, 3 fabricated"; the only safe
unit is the one the format frames with a header and an index entry.

**The guarantee holds BY CONSTRUCTION, not by counting** (issue #4196, round 20-21): the
decode-at-offset path's `drive_partition_sliding`
(`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/partition_driver.rs`) buffers a whole
partition's decoded rows locally and forwards them to its caller ONLY on structural completion (the
`END_OF_PARTITION` marker, or a final-chunk truncated-body flush) — a mid-partition `Err` propagates
before that buffer is ever flushed, so the rows it held are simply dropped. Salvage therefore never
SEES a partial row set to accidentally write, let alone writes one. An earlier design iteration had
the manifest report how many rows decoded before the failure; round 20 proved that count is
structurally always `0` (the buffering above means no caller can ever observe otherwise), so round
21 removed the field rather than ship one that never reflected what its doc claimed — see issue
#4218 for reinstating a real count if the partition driver's buffering ever changes to incremental
emission.

### D2.1 The 128 MiB plausible-partition-span ceiling — a size-based refusal on HEALTHY input

**The threshold.** `SALVAGE_MAX_PLAUSIBLE_PARTITION_BYTES = 128 * 1024 * 1024`
(`cqlite-core/src/storage/sstable/reader/data_access/point_compaction.rs`, `write-support`-gated),
matching `compression.rs::MAX_DECOMPRESSED_SIZE`'s established convention and the crate's <128 MB
memory target. It bounds the SPAN a single boundary-source slot may claim, checked by
`exceeds_plausible_partition_span(start, end)` in BOTH arms of
`decode_partition_at_offset_for_salvage` (uncompressed positional read, compressed chunk window)
BEFORE anything is allocated.

**The behaviour, including on healthy input.** A slot whose authoritative span is wider than the
ceiling is REFUSED: `PartitionAtOffsetOutcome::SpanTooWide { span_bytes }` becomes a
`Loss { class: "truncated" }` whose message names the real cause and the actual width
(`… is N bytes wide, exceeding this salvage tool's 128 MiB plausible-partition-span ceiling —
Data.db itself is intact; this is a size-based refusal, not evidence of truncation or corruption`).
The run continues; the generation is still written; the exit code is D3's `3` (output with losses),
or `2` if it was the only slot.

**This is a documented DEVIATION from R1, not an implementation detail.** R1 says salvage of a
healthy SSTable equals a no-purge compaction of it — and for a genuinely healthy partition wider
than 128 MiB that is FALSE: the partition is lost. The trade-off is deliberate and is the same one
D2/D3 make everywhere else — a conservative, NAMED, counted loss beats an unbounded allocation that
starves or OOM-kills the process, on a file this tool exists to recover from. It is stated here
because a silent data-loss policy on healthy input that appears in no spec or design text is
indistinguishable, to a reader, from a bug.

**Why the ceiling is needed at all** (round-15 audit): for the LAST boundary entry (`end_bound:
None`) — including one that is only ARTIFICIALLY last because `Index.db`/`Partitions.db` was
truncated exactly on an entry boundary and therefore parsed cleanly with fewer real entries — `end`
resolves to the WHOLE remaining data section, not one partition's worth. Every other guard bounds
`end` against the REAL FILE SIZE, which such a span satisfies by construction, so it passes all of
them and then allocates the entire remaining multi-GB section before the slot is ever classified.
The check is therefore applied UNCONDITIONALLY to both span-resolution paths, `Some(end_bound)`
included: a corrupted-but-plausible middle boundary can name just as wide a gap, and the risk is the
SPAN's width, not which path produced it.

**Why `LossClass::Truncated` is reused, and why that is not a wording bug.** `LossClass` has no
dedicated variant for a size-based refusal, and `Truncated` is the closest existing class — the
outcome is, ultimately, "this partition was not recovered". But `Truncated`'s own canonical message
("the partition's authoritative byte range extends past `Data.db`'s actual end") is FACTUALLY FALSE
for this case: the file is intact. So the two share a class and NOT a message — the `SpanTooWide`
arm in `salvage/recover_helpers.rs` emits its own text naming the ceiling and the span width, so an
operator of a wide, healthy partition is never told their data is corrupt. Adding a distinct
`LossClass` would change the D5 manifest's public `class` vocabulary; that is deliberately NOT done
here, and the class/message split is the recorded resolution.

## D3. Failure contract

| Situation | Behaviour | Exit |
|---|---|---|
| every partition recovered | output generation + manifest with `losses: 0 RECOGNISED` | 0 |
| some partitions lost | output generation + manifest naming each loss | 3 |
| boundary source unreadable (Index.db / Partitions.db corrupt, missing) | REFUSE: no Data.db written; manifest `refused: {reason: boundary-source-unreadable, remedy: "cqlite rebuild --components index (#4197)"}` | 2 |
| zero partitions decodable | REFUSE: no Data.db; manifest lists every loss | 2 |
| schema unresolvable, output dir not empty, input dir has no Data.db | usage error | 1 |

Never a shorter output presented as success: exit 0 is reachable ONLY with an empty loss list,
and the loss list is affirmative (`0 RECOGNISED`) so an unmeasured run cannot read as clean.
Interrupted runs leave a `.salvage-incomplete` marker in `--out`; the writer's normal finish
(TOC last) means a complete component set implies a completed run.

## D4. Output

Uncompressed, `nb` for BIG input and `da` for BTI input (same family as the source), generation
number = source generation (the output lives in a different directory), all components the writer
already emits (Data, Index/Partitions+Rows, Summary, Filter, Statistics, Digest, CRC for `nb`,
TOC). Statistics are recomputed from what was written (the writer's stats fold) — a salvaged
file's min/max timestamps describe its survivors, not the original. Repaired/pending-repair
state is carried through `set_repair_state` from the source Statistics when readable, else
unrepaired and noted in the manifest.

## D5. Manifest schema (stable; the CLI text form is a rendering of it)

```json
{ "input": "<path>", "output": "<dir>", "format": "nb|da", "compressed_input": true,
  "boundary_source": "index|bti-trie", "generation": 12,
  "partitions": { "total": 1204, "recovered": 1198, "lost": 6 },
  "losses": [ { "key_hex": "…", "key": "<rendered when schema decodes it>", "data_offset": 88192,
                "chunks": [12, 13], "class": "chunk-crc|decode|key-mismatch|truncated",
                "message": "…" } ],
  "component_findings": [ { "class": "<VerifyErrorClass code>", "component": "…", "detail": "…" } ],
  "refused": null | { "reason": "boundary-source-unreadable|nothing-decodable", "remedy": "…" },
  "now": "<RFC3339 of the run>", "cqlite_version": "…" }
```

## D6. Oracles (all Cassandra-written; expectations derived from the format, never from CQLite)

| Case | Fixture | Expected survivor set derived by |
|---|---|---|
| healthy BIG compressed / BTI | any committed `test_basic` / `test_da` table | golden JSONL = output dump; `compact_sstables` no-purge = byte-equal |
| compressed chunk CRC flip | `test_comp_corrupt/data_db_bit_flip` (captured Cassandra verdict `corrupt`) | healthy source's Index positions × CompressionInfo chunk table → partitions intersecting the flipped chunk are the loss set; all others must appear in the output dump |
| uncompressed chunk CRC flip | `test_comp_corrupt/uncompressed_data_bit_flip` (`CRC.db`) | same, with `CRC.db`'s chunk size |
| Data.db truncation | `test_comp_corrupt/data_db_truncation` | partitions whose range extends past the new EOF are lost (`truncated`) |
| decodable-but-corrupt row (CRC recomputed) | `corrupt_byte_fixture.rs` `ClusteringTextLiteral` for BIG and BTI | the partition holding the needle is lost (`decode`), every other partition recovered — the #3782 fixture, now with a recovery assertion instead of a refusal assertion |
| boundary source damaged | `test_comp_corrupt/index_db_bit_flip_big`, `bti_partitions_footer_flip`, `bti_rows_truncation` | refusal with `rebuild` remedy; no Data.db in `--out` |
| both damaged | temp copy: `data_db_bit_flip` + Index.db deleted | refusal names the boundary source (the honest limit of this slice) |

## D7. CLI shape and campsite

`cqlite-cli/src/commands/salvage.rs`, wired like `read_commitlog.rs`; `Commands::Salvage` in
`cli_types.rs`. Output resolution and `--schema` reuse `write.rs`'s compaction helpers (schema
load, generation ordering). New core module is new files only (`write_engine/salvage/`), so the
file-size ratchet is not engaged by construction; `cli_types.rs` growth is one variant.
