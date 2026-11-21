-- issue-4201-diff-risk.cql — Cassandra 5.0.8 schema for the `cqlite diff` resurrection-risk fixture
-- (issue #4201). Generator: test-data/scripts/generate-issue-4201-diff-risk.sh
--
-- Two raw generations that are staged as TWO SIDES by the tests: side A = generation 2 only
-- (the tombstones and the "a" cells), side B = generation 1 only. Every write pins its client
-- timestamp (USING TIMESTAMP) so the expected outcomes follow from the mutations and Cassandra's
-- rules (DeletionTime.deletes: cell.timestamp <= markedForDeleteAt), never from CQLite output.
CREATE KEYSPACE IF NOT EXISTS test_diff
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS test_diff.risk_cases (
    id int,
    ck int,
    v  text,
    PRIMARY KEY (id, ck)
) WITH compression = {'enabled': false}
  AND gc_grace_seconds = 864000;
