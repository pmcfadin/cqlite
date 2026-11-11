# datasets-v4 (`cassandra5-small-full-v4.0.tar.gz`) — Cassandra 5.0.8 move, stage 4 of #4482

v4.0 is a **splice onto v3.6**, like v3.4 -> v3.5 (see `REGEN-NOTES-1935.md`): table directory
names (random UUID suffix) and generation prefixes (`nb-1-big`, ...) are pinned by manifests and
tests, so regenerated tables are renamed onto the v3.6 names. `metadata.yml`, `references.yml`
and all `system*` keyspaces are carried over unchanged. The tool is
`test-data/scripts/splice-datasets-v4.py`.

Bundle: sha256 `bfb07a23356025d0f809e17fc72ff1e57cc69566a7a0deefeb825137a6789b78`, 4,256,695 bytes.

## Provenance per table (the per-table truth is `cassandra_source.carried_over` in the manifest)

| Class | Tables | Bytes | Goldens |
|---|---|---|---|
| **Regenerated on `cassandra:5.0.8`** (committed generator, pinned driver 3.30.1, then spliced) | `test_types` (20), `test_tomb` (all 9, incl. `resurrection_gc_positive`), `test_compactionparity` (2), `test_compactionparityudt` (4), `test_compaction_tombstone_ttl` (3 of 4), `test_signed_coll` (4), `test_nested_udt_keys` (1), `test_comparator_order` (1), `test_comp` (7), `test_writeparity` (3), `test_da.multiclustering_table` | 5.0.8 | 5.0.8 |
| **Corruption corpus** `corruption/test_comp_corrupt` | 12 fixtures | re-derived offline from the new clean `test_comp`/`test_da` sources by `generate-corruption-corpus.sh` | regenerated |
| **Commitlog** `commitlog/` | 3 segments (id 1794289002839, v7) + ground truth | 5.0.8 (absorbed from #4493) | 5.0.8 |
| **Carried over from 5.0.2: bytes kept, goldens re-dumped with 5.0.8 `sstabledump`/`sstablemetadata`** | `test_basic` (7 of 8), `test_collections` (8), `test_timeseries` (9), `test_wide_rows` (8), `test_oa` (6), `test_da.simple_table`, `test_da.collection_table`, `test_da.wide_multiclustering_small` | 5.0.2 | 5.0.8 |
| **Carried over from 5.0.2: verbatim (bytes and goldens)** | the aged TTL seams `test_basic.ttl_test_table`, `test_da.ttl_table`; `test_da.wide_table` (aged, `gen-wide-bti.sh`); `test_deltas.*` (9); `test_compaction_tombstone_ttl.ttl_expired_live`; `test_big.wide_partition` | 5.0.2 | 5.0.2 |
| **Unchanged** | `system*` (ignored by the corpus audit) | 5.0.2 | 5.0.2 |

`test_signed_coll`, `test_nested_udt_keys` and `test_comparator_order` are #4494's output and the
commitlog segments are #4493's: both were absorbed into this change because bundle-resident
fixtures can only change together with the bundle (the warm-cache restore overlays the pinned
bundle on tracked files, #4508). The bundle holds exactly the tracked bytes of those paths.

`cassandra_source.carried_over` declares 13 table entries (`test_basic.*`, `test_collections.*`,
`test_timeseries.*`, `test_wide_rows.*`, `test_oa.*`, `test_deltas.*`, `test_big.*`,
`test_compaction_tombstone_ttl.ttl_expired_live` and the five explicit `test_da` tables).
`evidence.cassandra_version` of a scenario names the release that produced its NON-carried-over
data, or the single carry-over pin when EVERY table it covers is carried over: 89 scenarios
declare 5.0.2 for that reason, all others 5.0.8. The lint prints one summary line (95 scenarios
touch carried-over tables written by another release; `--verbose` lists them) and the corpus
audit rejects an undeclared mix.

Random-data keyspaces (`test_basic`, `test_collections`, `test_timeseries`, `test_wide_rows`,
`test_oa`, `test_da`) are NOT re-cut: the committed `regenerate-datasets.sh` does not reproduce
them (#4482 report 2c; the shape-bearing generator was never committed, #2222).

### Tables carried over beyond the TTL seams, and why (each found by a failing test)

| Table | Pinned by |
|---|---|
| `test_deltas.*` (9 tables) | `issue_992_ttl_tombstone_parity` pins the absolute wall-clock `expires_at` / `local_delete_time` of `ttl_cells`, `cell_/row_/partition_tombstones`. The generator stamps `now()`, so a rebuild changes every pinned value. |
| `test_compaction_tombstone_ttl.ttl_expired_live` | the query-semantics oracle (`pinned_now`) assumes the rows are already expired at the pinned instant; a fresh TTL write is live at `pinned_now`. A second aged TTL seam. |
| `test_big.wide_partition` | `sstable_parity_index_db_test::issue_1309_*` pins Index.db offsets (598601) of random-payload content; the regenerated partition differs by 7 bytes. |

`test_tomb.resurrection_gc_positive` and `test_da.multiclustering_table` were carried over until
#4507 (#4501) made the position-consuming tests convention-independent; they are now regenerated
on 5.0.8, and #4507's tests (`issue_3032_*`, `issue_4222_raw_view_*point_read_test`,
`issue_992_*`, `issue_4196_salvage_*`) pass on the real bytes.

## Golden diff classes against v3.6

* TOC.txt: sorted lexicographically (CASSANDRA-20494, 5.0.6). Regenerated tables only; carried-over
  tables keep their 5.0.2 writer order (the file is written by Cassandra, not by `sstabledump`).
* sstabledump partition `position`: first partition no longer `0`, later ones shift by the header
  length (every JSONL of every re-dumped or regenerated table). Row `position` is unchanged.
* sstabledump JSON dialect: 5.0.8 emits compact `{"a":1}`, 5.0.2 emitted `{"a": 1}`. 13 v3.6
  goldens used the spaced dialect and tests match literals such as `"type": "row"`, so
  `splice-datasets-v4.py --goldens` re-serialises each golden in the dialect that reproduces
  EVERY line of the v3.6 file.
* Statistics.db (+ `.txt`): wall-clock timestamps, host id, commitlog position, token range.
* Wall-clock content in regenerated tables: `tstamp`, `local_delete_time`, `marked_deleted`,
  `expires_at`, timeuuid cell paths, counter shard values (`test_types.ct_*`; replaced wholesale
  with a 5.0.8 run, not masked: no test pinned the old values).
* Everything else (Data.db, Index/Filter/Summary/CompressionInfo, Digest) is byte-identical for
  regenerated tables with no wall-clock content. No structural change was found: row/cell content,
  liveness, deletion shapes and collection paths are equal after masking the classes above.

## Findings

* **F1 (resolved by #4507):** tests treated sstabledump's partition `position` as a partition file
  offset. Under 5.0.8 it is the start of the partition's content (after the header). The raw view
  keeps the TRUE partition start.
* **F2 — `sstabledump` 5.0.8 refuses `oa` sstables under the image's default
  `storage_compatibility_mode: CASSANDRA_4`** ("incompatible sstable version (oa)"); the dump must
  run with the mode set to `NONE`, exactly as the generation phase does.
* **F3 — `bump-dataset-pin.sh` never repointed the tag in the composite restore action**
  (`default: datasets-v3`), so the first tag change (v3 -> v4) left `check-dataset-pin.sh` red.
  Fixed here and checked on a scratch copy.
* **F4 — corpus-audit provenance was one release per corpus.** The weekly lanes record ONE pin and
  92 scenarios cover the broad `test-data/datasets` dir, so ALL scenario evidence moved in this
  change, the manifest primary pin is `cassandra-5.0.8` (5.0.2 stays a declared additional pin),
  the `CQLITE_CASSANDRA_RELEASE=5.0.2` overrides and their guard assertions are gone from
  `exhaustive-regeneration.yml` / `parity-regen-matrix.yml`, and the carry-over declaration above
  keeps the bundle's mixed provenance truthful per table.
* **Corruption verdicts recaptured on 5.0.8** (`capture-cassandra-verify-verdicts.sh`, all 12
  fixtures): every verdict is unchanged (corrupt x10, clean for `filter_db_bit_flip` and
  `toc_missing_component`); the manifest and notes now name 5.0.8.
* The `test_corrupted_file_handling` and `table_snapshot_tests` CLI tests fail identically on the
  v3.6 bundle (pre-existing, not caused by v4).

## Reproduce

```
gh release download datasets-v3 -p cassandra5-small-full-v3.6.tar.gz      # base, sha256 a3448929…
# 1. generators on cassandra:5.0.8 (Docker, one at a time):
#    generate-cql-type-parity generate-tombstone-parity generate-deltas generate-compaction-parity
#    generate-compaction-parity-udt generate-compaction-tombstone-ttl-parity
#    generate-signed-collection-parity generate-nested-udt-keys
#    generate-issue-3790-comparator-ordering generate-compression-parity generate-write-load-parity
#    gen-wide-big gen-multiclustering-bti    (each with --out <dir>; the gen-* scripts take OUT=<dir>)
# 2. re-dump goldens of the kept-bytes tables with 5.0.8 (storage_compatibility_mode: NONE for oa):
#    sstabledump <Data.db> -l  -> <Data.db>.jsonl.raw ; sstablemetadata <Data.db> -> Statistics.db.txt
# 3. splice-datasets-v4.py --base <v3.6 datasets> --gen <each out> --goldens <redump dir> \
#       --preserve KS.TABLE ... --dest <v4 datasets>
#    then overlay the tracked-in-repo directories (commitlog/, test_signed_coll, test_nested_udt_keys,
#    test_comparator_order) from the checkout so the bundle holds EXACTLY the tracked bytes
# 4. generate-corruption-corpus.sh --out <v4 datasets-parent>    (offline)
# 5. tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=2026-11-10 -C <stage> -cf - test-data/datasets | gzip -n -9
# 6. check: every tracked path under test-data/datasets is byte-identical to the extracted bundle
```
