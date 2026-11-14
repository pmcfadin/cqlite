-- issue-4524-rowdel-liveness.cql — Cassandra 5.0.8 reference schema for the "primary-key liveness
-- vs row deletion" fixture (issue #4524, roborev job 307).
--
-- A liveness marker with timestamp <= a row deletion's markedForDeleteAt is deleted
-- (DeletionTime.deletes: ts <= markedForDeleteAt), even when a later cell tombstone sits in the
-- same SSTable. A marker strictly newer than the deletion survives.
--
-- Generator: test-data/scripts/generate-issue-4524-rowdel-liveness.sh (documents every row).

CREATE KEYSPACE IF NOT EXISTS test_rowdel
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS test_rowdel.single_rows (
    id int PRIMARY KEY,
    v  text
);

CREATE TABLE IF NOT EXISTS test_rowdel.multigen_rows (
    id int PRIMARY KEY,
    v  text
);

CREATE TABLE IF NOT EXISTS test_rowdel.clustered_multigen (
    pk int,
    ck int,
    v  text,
    PRIMARY KEY (pk, ck)
);

CREATE TABLE IF NOT EXISTS test_rowdel.clustered_single (
    pk int,
    ck int,
    v  text,
    PRIMARY KEY (pk, ck)
);
