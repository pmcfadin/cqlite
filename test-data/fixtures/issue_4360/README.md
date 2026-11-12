# issue_4360 — Cassandra-written top-level `varint` fixture

Backs **#4360** (cross-binding canonicalizer vs the CLI's unquoted varint).

- Keyspace `test_varint`: `varint_regular (id int PK, v varint)` and
  `varint_clustered (pk int, ck varint, tag text, PK (pk, ck))`.
- Values: 0, 1, -1, 127, 128, 255, 256, -128, -129, 2^63-1, 2^64, 2^64+1, -(2^64), and a
  40-digit positive and negative; `varint_regular` id 100 is an explicit `v = null`.
- **Provenance:** written by `cassandra:5.0.8` (image `sha256:259578bbeac315a7326c2a99e60eab86f3415e9e895207bf2a032af4de8c16b5`,
  `nodetool version` 5.0.8) in the owner-authorized `wintermute:cassandra` VM, by
  `test-data/scripts/generate-issue-4360-varint-column.sh`; every INSERT `USING TIMESTAMP 1000`,
  one `nodetool flush`, `sstabledump -l` golden (`nb-1-big-Data.db.jsonl`) and `sstablemetadata` alongside.
  Schema: `test-data/schemas/issue-4360-varint-column.cql`.
- The null row is a cell tombstone with a wall-clock `local_delete_time`: never byte-compare
  across regenerations and never assert on it.
- Checkout-relative on purpose (an sstables root: it directly contains the keyspace dir); the
  `*.db` files are force-added (`git add -f`).
- Consumers: `bindings/parity/fixtures.json` (`varint_values`, 3 legs) and
  `cqlite-core/tests/issue_4360_varint_column_oracle.rs`. Two oracle tests are `#[ignore]`d for
  known CQLite defects the fixture exposed: varint CLUSTERING decodes as Blob (class of #4279) and a
  row with only a null cell is not returned by SELECT (class of #2189).
