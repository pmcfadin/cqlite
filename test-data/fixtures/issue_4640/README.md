# issue_4640 fixture: set/list/map element tombstones and overwrites across generations

Table `fix4640.t` (RF 1, STCS, auto-compaction off), Cassandra 5.0.8 BTI (`da`), cassandra-0 of the
freeside field-round cluster, generated 2026-11-26 (keyspace dropped afterwards). Partition keys 3, 4
and 5 were chosen with `nodetool getendpoints` to all live on that node. Default (server) timestamps,
a flush and a 2 s sleep between generations.

| gen | content |
|-----|---------|
| `da-1` | key 3: c=0 `s={1,2,3} l=[10,20,30] m={1:a,2:b,3:c}`, c=1 `{7,8} [70,80] {7:x,8:y}`; key 4 `{1} [1] {1:only}`; key 5 `{5,6} [5,6] {5:p,6:q}` |
| `da-2` | key 3 c=0: `s-{2}`, `m-{2}`, `l-[20]` (element tombstones), then `s+{4}`, `m+{4:d}`, `l+[40]`; key 4: `s+{9}`; key 5: `DELETE m[6]` |
| `da-3` | key 3 c=0: `m[1]='a2'`, `s+{5}`; key 4: `l=[99]` (whole-collection overwrite); key 3 c=2 new row; key 5: `DELETE s` (whole set) |

Oracles: `live-select-consistency-all.txt` (`SELECT` at `CONSISTENCY ALL`, read semantics),
`sstabledump-da-{1,2,3}-bti.json` (`sstabledump -t`), `schema.cql`.
Consumer: `cqlite-core/tests/issue_4640_collection_assembly.rs`. `*.db` files are force-added.
