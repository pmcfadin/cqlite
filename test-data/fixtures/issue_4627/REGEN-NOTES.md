# issue_4627 -- frozen<tuple<..>> fixture (Cassandra 5.0.8)

Backs `cqlite-cli/tests/issue_4627_frozen_tuple_cassandra_golden.rs` (#4627, field round
0.19 F7). Table `fix4627.t_tuple` (`schema.cql`), 3 partitions / 5 rows, uncompressed.
Columns cover `frozen<tuple<int,text,uuid>>` (flat, null element, all-null),
`frozen<tuple<int,frozen<tuple<text,uuid>>>>` (nested), a UDT with a tuple field,
and `list<frozen<tuple<int,text>>>`.

## Provenance (2026-11-26, Cassandra 5.0.8, `nodetool version` = 5.0.8)

1. On freeside `fr` cassandra-0 (BTI configured), keyspace `fix4627` (RF 1, later altered to
   `NetworkTopologyStrategy dc1:3` so node 0 owns every key), `CREATE TYPE tt`, `CREATE TABLE
   t_tuple ... WITH compression={'enabled':'false'}` (see `schema.cql`), INSERTs of the five
   rows (`INSERT INTO fix4627.t_tuple (id,ck,tp,p,nt,lt) VALUES (1,1,(7,'hello',<uuid>),
   {a:1,b:(2,'x',<uuid>)},(3,('y',<uuid>)),[(1,'a'),(2,'b')])` etc.; the dump below is the
   authority for values).
2. `nodetool flush fix4627; nodetool compact fix4627 t_tuple` -> `da-4-bti-*` (`sstables/`).
3. `sstabledump <dir>/da-4-bti-Data.db > goldens/da-4-bti-Data.db.sstabledump.json`
   (`/opt/cassandra/tools/bin/sstabledump`, Cassandra 5.0.8).
4. BIG copy: in the `tools`/`oracle` container (default format = BIG) with an offline
   `cassandra.yaml` (data dir containing a copy of node-0 `system_schema` + the BTI generation),
   `CASSANDRA_CONF=... /opt/cassandra/bin/sstablescrub fix4627 t_tuple` rewrote it as
   `nb-5-big-*` (`sstables_big/`); `sstabledump` of it is
   `goldens/nb-5-big-Data.db.sstabledump.json` (identical to the BTI golden apart from
   `position`).
5. Keyspace `fix4627` dropped afterwards. Binary components are force-added (`*.db` is
   gitignored).
