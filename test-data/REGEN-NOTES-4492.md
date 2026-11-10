# datasets-v4 (`cassandra5-small-full-v4.0.tar.gz`) — Cassandra 5.0.8 move, stage 4 of #4482

v4.0 is a **splice onto v3.6**, like v3.4 -> v3.5 (see `REGEN-NOTES-1935.md`): table directory
names (random UUID suffix) and generation prefixes (`nb-1-big`, ...) are pinned by manifests and
tests, so regenerated tables are renamed onto the v3.6 names. `metadata.yml`, `references.yml`,
`commitlog/` and all `system*` keyspaces are carried over unchanged. The tool is
`test-data/scripts/splice-datasets-v4.py`.

## Provenance per keyspace

| Class | Tables | Bytes | Goldens |
|---|---|---|---|
| **Rebuilt on `cassandra:5.0.8`** (committed generator, pinned driver 3.30.1, then spliced) | `test_types` (20), `test_tomb` (8 of 9), `test_compactionparity` (2), `test_compactionparityudt` (4), `test_compaction_tombstone_ttl` (3 of 4), `test_signed_coll` (4), `test_nested_udt_keys` (1), `test_comparator_order` (1), `test_comp` (7), `test_writeparity` (3) | 5.0.8 | 5.0.8 |
| **Corruption corpus** `corruption/test_comp_corrupt` | 12 fixtures | re-derived offline from the new clean `test_comp`/`test_da` sources by `generate-corruption-corpus.sh` | regenerated |
| **Bytes kept from v3.6, goldens re-dumped with 5.0.8 `sstabledump`/`sstablemetadata`** | `test_basic` (7 of 8), `test_collections` (8), `test_timeseries` (9), `test_wide_rows` (8), `test_oa` (6), `test_da` (`simple_table`, `collection_table`, `wide_multiclustering_small`) | 5.0.2 | 5.0.8 |
| **Verbatim from v3.6 (bytes and goldens)** | the two aged TTL seams `test_basic.ttl_test_table`, `test_da.ttl_table`; `test_da.wide_table` (aged, `gen-wide-bti.sh`); plus the pinned tables below | 5.0.2 | 5.0.2 |
| **Unchanged** | `system*`, `commitlog/` (stage 3, #4491, owns the commitlog fixtures) | 5.0.2 | 5.0.2 |

Random-data keyspaces (`test_basic`, `test_collections`, `test_timeseries`, `test_wide_rows`,
`test_oa`, `test_da`) are NOT re-cut: the committed `regenerate-datasets.sh` does not reproduce
them (#4482 report 2c; the shape-bearing generator was never committed, #2222).

### Tables kept verbatim beyond the TTL seams, and why (each found by a failing test)

| Table | Pinned by |
|---|---|
| `test_deltas.*` (9 tables) | `issue_992_ttl_tombstone_parity` pins the absolute wall-clock `expires_at` / `local_delete_time` of `ttl_cells`, `cell_/row_/partition_tombstones`; `issue_992_range_boundary_grammar` reads `partition.position` as a Data.db offset. The generator stamps `now()`, so a rebuild changes every pinned value. |
| `test_compaction_tombstone_ttl.ttl_expired_live` | the query-semantics oracle (`pinned_now`) assumes the rows are already expired at the pinned instant; a fresh TTL write is live at `pinned_now`. A second aged TTL seam. |
| `test_big.wide_partition` | `sstable_parity_index_db_test::issue_1309_*` pins Index.db offsets (598601) of random-payload content; the regenerated partition differs by 7 bytes. |
| `test_tomb.resurrection_gc_positive` | `issue_4222_raw_view_point_read_test` compares the raw view's `position` column with sstabledump's partition `position` (41 vs 59 under 5.0.8). |
| `test_da.multiclustering_table` | `issue_3032_multiclustering_rows_trie_shape` compares the Rows.db entry `data_position` with sstabledump's partition `position` (0 vs 7 under 5.0.8). |

The last two are the **sstabledump `position` convention change** (see F1). They stay at the
5.0.2 convention until the tests are made convention-independent.

## Golden diff classes against v3.6 (file-by-file and JSON-path comparison, masked per class)

* TOC.txt: sorted lexicographically (CASSANDRA-20494, 5.0.6). Rebuilt tables only; kept-bytes
  tables keep their 5.0.2 writer order (the file is written by Cassandra, not by `sstabledump`).
* sstabledump partition `position`: first partition no longer `0`, later ones shift by the header
  length (every JSONL of every re-dumped or rebuilt table). Row `position` is unchanged.
* sstabledump JSON dialect: 5.0.8 emits compact `{"a":1}`, 5.0.2 emitted `{"a": 1}`. 13 v3.6
  goldens used the spaced dialect and tests match literals such as `"type": "row"`, so
  `splice-datasets-v4.py --goldens` re-serialises each golden in the dialect that reproduces
  EVERY line of the v3.6 file.
* Statistics.db (+ `.txt`): wall-clock timestamps, host id, commitlog position, token range.
* Wall-clock content in rebuilt tables: `tstamp`, `local_delete_time`, `marked_deleted`,
  `expires_at`, timeuuid cell paths, counter shard values (`test_types.ct_*`).
* Everything else (Data.db, Index/Filter/Summary/CompressionInfo, Digest) is byte-identical for
  the rebuilt tables with no wall-clock content (51 of 77 generations rebuilt in the first pass).
  No structural change was found: row/cell content, liveness, deletion shapes and collection paths
  are equal after masking the classes above.

## Findings

* **F1 — sstabledump `position` is consumed as a partition file offset by tests the #4485 audit
  missed** (`issue_3032_*`, `issue_4222_raw_view_point_read_test`, `issue_992_*` helpers
  `golden_partition_position`). Under 5.0.8 `position` is the start of the partition's content
  (after the header), so equality with CQLite's own offset fails. Whether the raw view's
  `position` should follow the new sstabledump convention is a product call.
* **F2 — `sstabledump` 5.0.8 refuses `oa` sstables under the image's default
  `storage_compatibility_mode: CASSANDRA_4`** ("incompatible sstable version (oa)"); the dump must
  run with the mode set to `NONE`, exactly as the generation phase does.
* **F3 — `bump-dataset-pin.sh` never repointed the tag in the composite restore action**
  (`default: datasets-v3`), so the first tag change (v3 -> v4) left `check-dataset-pin.sh` red.
  Fixed here and checked on a scratch copy.
* **F4 — corpus-audit provenance is one release per corpus** (`pin_binding_*`): the weekly lanes
  record ONE pin for the whole corpus and 92 scenarios cover the broad `test-data/datasets` dir,
  so ALL scenario evidence moved to 5.0.8 together in this change, the manifest primary pin is now
  `cassandra-5.0.8` (5.0.2 stays a declared additional pin), and the
  `CQLITE_CASSANDRA_RELEASE=5.0.2` overrides were removed from `exhaustive-regeneration.yml` and
  `parity-regen-matrix.yml` with their guard assertions. The bundle itself is mixed-provenance
  (kept-bytes and verbatim tables were written by 5.0.2; the format is identical, see #4482 2b).
* **Corruption verdicts recaptured on 5.0.8** (`capture-cassandra-verify-verdicts.sh`, all 12
  fixtures): every verdict is unchanged (corrupt x10, clean for `filter_db_bit_flip` and
  `toc_missing_component`); the manifest and notes now name 5.0.8.
* **test_types determinism:** counter shard values and `now()` timeuuid paths differ per run, so
  the `ct_*`/timeuuid tables are replaced wholesale with a 5.0.8 run (stated reason), not masked;
  `test_tomb` Data.db deletion-time deltas likewise. No test pinned the old values.
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
#    gen-wide-big      (each with --out <dir>; gen-wide-big takes OUT=<dir>)
# 2. re-dump goldens of the kept-bytes tables with 5.0.8 (storage_compatibility_mode: NONE for oa):
#    sstabledump <Data.db> -l  -> <Data.db>.jsonl.raw ; sstablemetadata <Data.db> -> Statistics.db.txt
# 3. splice-datasets-v4.py --base <v3.6 datasets> --gen <each out> --goldens <redump dir> \
#       --preserve KS.TABLE ... --dest <v4 datasets>
# 4. generate-corruption-corpus.sh --out <v4 datasets-parent>    (offline)
# 5. tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=2026-11-10 -C <stage> -cf - test-data/datasets | gzip -n -9
```
