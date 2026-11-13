-- issue-4524-null-only-row.cql — Cassandra 5.0.8 reference schema for the "row with no live
-- regular cell" fixture (issue #4524).
--
-- A SELECT must return a row that has LIVENESS (primary-key INSERT) even when it has no live
-- regular cell: Cassandra `BTreeRow.hasLiveData` is true when `primaryKeyLivenessInfo` is live,
-- whatever the cells. It must NOT return a row with NO liveness whose every cell is deleted.
-- Physical-dump parity (sstabledump goldens) cannot see this: the dump lists the row either way.
--
-- Generator: test-data/scripts/generate-issue-4524-null-only-row.sh (documents every row's
-- statements and expected SELECT outcome). Backs: issue #4524.

CREATE KEYSPACE IF NOT EXISTS test_nullrow
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS test_nullrow.flat_rows (
    id int PRIMARY KEY,
    v  text
);

CREATE TABLE IF NOT EXISTS test_nullrow.clustered_rows (
    pk int,
    ck int,
    v  text,
    PRIMARY KEY (pk, ck)
);

-- Two SSTables (a flush between the two write phases): the row's liveness and its cells live in
-- DIFFERENT generations, so the k-way merge arm must reconcile them (issue #4524, sibling audit).
CREATE TABLE IF NOT EXISTS test_nullrow.multigen_rows (
    id int PRIMARY KEY,
    v  text
);
