# issue_4628 fixture: tombstone and data in different generations (field round F8)

Minimal reproduction of #4628. Table `fix4628.t` (RF 1, STCS, auto-compaction off), Cassandra
5.0.8 BTI (`da`), single node (cassandra-0 of the freeside field-round cluster), regenerated
2026-11-26. Keys were chosen with `nodetool getendpoints` to all live on that node.

| gen | content |
|-----|---------|
| `da-1` | data, ts 1000, keys 1,3,4,5,9, rows (c1 0..2, c2 0..1), `v`, `l list<int>`, `m map<int,text>` |
| `da-2` | filler key 6 only: ABSENT for keys 1,3,4,5,9, so it contributes no merge run |
| `da-3` | deletions, ts 2000: key 1 partition delete, key 3 range delete (`c1=1`), key 4 row delete (0,1), key 5 collection overwrite `l=[9]` on (0,0) |

Oracles committed here:
- `live-select-consistency-all.txt`: `SELECT ... writetime(..)` at `CONSISTENCY ALL` (read semantics).
- `sstabledump-da-{1,2,3}-bti.json`: `sstabledump -t` of each generation.
- `schema.cql`: keyspace plus `DESCRIBE TABLE`.

Regenerate (freeside cluster, keyspace `fix4628`, dropped afterwards): create the table with
`compaction={'class':'SizeTieredCompactionStrategy','enabled':'false'}`; INSERT the rows
`USING TIMESTAMP 1000` for keys 1,3,4,5,9 and `nodetool flush`; INSERT key 6 and flush; apply
the four deletions `USING TIMESTAMP 2000` and flush; copy `da-*` out of the table directory.

Consumers: `cqlite-cli/tests/issue_4628_explain_cross_generation.rs` (explain) and
`cqlite-core/tests/issue_4628_cross_generation_select.rs` (SELECT, `--features cli-helpers`).
Files are force-added past the `*.db` ignore rule, like the other committed fixtures.
