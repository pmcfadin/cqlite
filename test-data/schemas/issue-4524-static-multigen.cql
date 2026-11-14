-- issue-4524-static-multigen.cql — Cassandra 5.0.8 reference schema for the "a live STATIC cell
-- never revives a clustering row" fixture (issue #4524, roborev job 297).
-- Generator: test-data/scripts/generate-issue-4524-static-multigen.sh (documents every row's
-- statements and expected SELECT outcome). Two flushes -> two SSTables (generations).

CREATE KEYSPACE IF NOT EXISTS test_staticrow
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS test_staticrow.static_multigen (
    pk int,
    ck int,
    s  text STATIC,
    v  text,
    PRIMARY KEY (pk, ck)
);
