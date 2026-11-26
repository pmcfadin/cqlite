# issue_4640b fixture: non-frozen collections of frozen UDTs across generations

Keyspace `fix4640b` (RF 1, STCS, auto-compaction off), Cassandra 5.0.8 BTI (`da`), cassandra-0 of the
freeside field-round cluster, generated 2026-11-26 (keyspace dropped afterwards). `CREATE TYPE addr
(street text, num int)`; table `t (id int, c int, v text, al list<frozen<addr>>, am map<int, frozen<addr>>, ak map<frozen<addr>, int>)`.
Keys 3 and 4 were checked with `nodetool getendpoints` to live on that node. The SSTables were copied from the `dropped-*` snapshot Cassandra takes on `DROP KEYSPACE`. Server timestamps, a flush
and a 2 s sleep between generations.

| gen | content |
|-----|---------|
| `da-1` | key 3 c=0 `al=[a,b] am={1:m1,2:m2} ak={k1:10,k2:20}`, c=1 `[x] {7:m7}`; key 4 `[p] {5:m5} ak={k5:50}` |
| `da-2` | key 3 c=0: `al+[c]`, `DELETE al[0]` (list element tombstone), `DELETE am[1]`, `am[2]=m2b`, `am+{3:m3}`, `ak+{k3:30}`, `DELETE ak[k1]` (UDT-KEYED map entry tombstone) |
| `da-3` | key 3 c=0 `al+[d]`; key 4 `am+{6:m6}`, `ak+{k6:60}`; key 3 c=2 new row |

Oracles: `live-select-consistency-all.txt` (`SELECT id,c,v,al,am,ak` at CL ALL),
`live-select-scalar-consistency-all.txt` (`SELECT id,c,v`, the projection that excludes the UDT collections),
`sstabledump-da-{1,2,3}-bti.json`, `schema.cql`. Consumer: `cqlite-core/tests/issue_4640_udt_collections.rs`.
`*.db` files are force-added.
