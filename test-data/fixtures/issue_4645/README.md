# issue_4645 fixture: collection element tombstones inside ONE Cassandra-written generation

Table `fix4645.t` (RF 1, STCS), Cassandra 5.0.8 BTI (`da`), cassandra-0 of the freeside field-round cluster,
generated 2026-11-26 (keyspace dropped, data dir removed afterwards). Keys 3, 4, 5 reuse the #4640
`getendpoints` choice (all on cassandra-0). Explicit `USING TIMESTAMP`; two flushes, then
`nodetool compact`, so `da-3-bti` is Cassandra's OWN single-generation output.

| key | gen 1 (ts 1000) | gen 2 | what the compacted generation holds |
|-----|-----------------|-------|-------------------------------------|
| 3,c=0 | `s={1,2,3} l=[10,20,30] m={1:a,2:b,3:c}` | ts 2000 `s-{2} m-{2} l-[20]`; ts 3000 `s+{4} m+{4:d} l+[40]` | element tombstones between live elements (list: live, dead, live, live) |
| 3,c=1 | `{7,8} [70,80] {7:x,8:y}` | ts 2000 `m = {9:z}` | complex deletion (whole-map overwrite) plus a live element |
| 4,c=0 | `{1} [1] {1:only}` | ts 2000 `s-{1} m-{1} l-[1]` | every element of every collection deleted (reads null) |
| 5,c=0 | `{5,6} [5,6] {5:p,6:q}` | ts 2000 `DELETE s`, `DELETE m` | whole-collection deletions, list untouched |

Oracles: `live-select-consistency-all.txt` (`SELECT *` at `CONSISTENCY ALL`, read semantics),
`sstabledump-da-3-bti.json` (`sstabledump -t`), `schema.cql`.
Consumer: `cqlite-core/tests/issue_4645_single_generation_collections.rs`. `*.db` files are force-added.
