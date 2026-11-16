# issue_4531 — Cassandra-written BTI (`da`) uuid/timeuuid CLUSTERING fixture

Backs **#4531** (CQLite encodes uuid/timeuuid clustering bounds as raw bytes; Cassandra swizzles).

- Keyspace `test_uuid_bti`: `timeuuid_clustering` (ASC), `timeuuid_clustering_desc` (DESC),
  `uuid_clustering` (mixed version-1 / version-4). Partition 1 = 480 rows (29 row-index blocks at
  `column_index_size: 2KiB`), partition 2 = 8 rows (control). One `da` SSTable per table.
- **Provenance:** `cassandra:5.0.8` (image `sha256:259578bbeac315a7326c2a99e60eab86f3415e9e895207bf2a032af4de8c16b5`),
  BTI selected via `storage_compatibility_mode: NONE` + `sstable.selected_format: bti`, in the
  owner-authorized `wintermute:cassandra` VM by
  `test-data/scripts/generate-issue-4531-uuid-clustering-bti.sh`; `USING TIMESTAMP 1000`, flush + compact,
  `sstabledump -l` golden (`da-2-bti-Data.db.jsonl`) and `sstablemetadata` alongside. Schema:
  `test-data/schemas/issue-4531-uuid-clustering-bti.cql`.
- Checkout-relative on purpose (an sstables root); `*.db` are force-added.
- Consumer: `cqlite-core/tests/issue_4531_uuid_clustering_slice.rs`.
