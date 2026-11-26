# issue_4629 fixture: equal-timestamp TTL ties across generations

Cassandra 5.0.8 (BTI, `da`), written on a freeside cluster (3 nodes, RF 1, node 0 holds every key used),
no compression, auto-compaction off. Every write uses `USING TIMESTAMP 1795600000000000`; the cells differ only
in value, TTL and kind. Table: `schema.cql` (`fix4629.t (id, c, v, l list<int>, m map<int,text>)`).

Generations, oldest first (a `nodetool flush` between each; `cql/*.cql` are the exact scripts, `cql/gen.py` writes them):

| gen | file | content |
| --- | --- | --- |
| 1 | `da-1-bti` | `a.cql`: first write of each case |
| 2 | `da-2-bti` | `b.cql`: second write of each case |
| 3 | `da-3-bti` | `x.cql`: partition 16 only, so partitions 1..13 are ABSENT here |
| 4 | `da-4-bti` | `c.cql`: the expiring write for partition 11 |

Cases (id, three rows each, `c` = 0..2): 1 plain then expiring; 3 expiring then plain (both orders of live vs
expiring); 4 and 5 expiring/expiring with different TTL (`localDeletionTime` decides, both orders); 6 expiring then
cell tombstone (tombstone wins); 11 the same plain cell in gens 1 and 2 plus an expiring one in gen 4; 13 expiring
then plain.

`oracle/live-select-cl-all.txt` is `SELECT id,c,v,WRITETIME(v),TTL(v) FROM fix4629.t` at CONSISTENCY ALL taken
after the flushes; `oracle/sstabledump-da-N-bti.json` is `sstabledump -t` of each generation (5.0.8).

Why generation 3 exists: the field-round bug needs a generation that does NOT hold the key between generations
that do (the traced point-read builder dropped it from the run list and displaced every later run).

## Regenerating

On a Cassandra 5.0.8 cluster (`sstable.selected_format: bti`): create the keyspace and table from `schema.cql`
(RF 1), pick keys that all land on one node (`nodetool getendpoints`; the ids above did on node 0), run
`python3 cql/gen.py <dir>`, then for each of `a b x c` in order: `cqlsh -f <dir>/<name>.cql`, `nodetool flush`.
Copy `da-*` from the table directory, dump with `sstabledump -t`, `SELECT` at CONSISTENCY ALL. The token placement
of the ids is cluster specific; the committed bytes are the authority, not a re-run. Drop the keyspace afterwards.
