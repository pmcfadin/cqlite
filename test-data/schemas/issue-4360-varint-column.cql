-- issue-4360-varint-column.cql — Cassandra 5.0.8 reference schema for the
-- top-level `varint` column fixture (issue #4360).
--
-- WHY: the cross-binding parity harness (bindings/parity) had no fixture with a
-- `varint` column, so the CLI leg's unquoted-integer rendering was pinned by
-- synthetic vectors only. Until this fixture the corpus held varint data only as a
-- UDT field (issue_3722, one positive value) and as empty set members (issue_4106).
--
-- Subject values (the same list populates both tables): 0, 1, -1, the byte-count
-- edges 127/128/255/256/-128/-129 (a varint gains a sign byte at 128 and at -129),
-- 2^63-1, 2^64, 2^64+1, -(2^64), and a 40-digit positive and negative. Table
-- `varint_regular` also carries one row (id 100) whose varint is an explicit CQL null,
-- i.e. a cell tombstone (its `local_delete_time` is a wall clock: never assert on it).
--
-- `varint_clustered` puts the varint in the CLUSTERING key, which exercises the
-- IntegerType comparator order on disk (numeric, not byte-wise) as well as the
-- encoding.
--
-- Generator: test-data/scripts/generate-issue-4360-varint-column.sh
-- Backs: issue #4360.

CREATE KEYSPACE IF NOT EXISTS test_varint
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS test_varint.varint_regular (
    id int PRIMARY KEY,
    v  varint
);

CREATE TABLE IF NOT EXISTS test_varint.varint_clustered (
    pk  int,
    ck  varint,
    tag text,
    PRIMARY KEY (pk, ck)
);
