-- issue-4530-equal-ts-ties.cql — Cassandra 5.0.8 schema for the EQUAL-TIMESTAMP reconcile-tie
-- fixture (issue #4530). Generator: test-data/scripts/generate-issue-4530-equal-ts-ties.sh
--
-- Two SSTable generations written at the SAME client timestamp (USING TIMESTAMP 1000) with
-- conflicting content, then compacted by Cassandra to give the reconcile ORACLE. Cases (pk=1,
-- column v, one clustering key each) — `Cells.resolveRegular` (Cells.java:79-129):
--   ck 1  live vs live, gen1 'apple' / gen2 'zebra'      -> larger value 'zebra'
--   ck 2  live vs live, gen1 'zebra' / gen2 'apple'      -> larger value 'zebra'   (reversed)
--   ck 3  gen1 live 'live' / gen2 expiring 'ttl'         -> expiring wins
--   ck 4  gen1 expiring 'ttl' / gen2 live 'live'         -> expiring wins          (reversed)
--   ck 5  gen1 TTL 300000000 'big' / gen2 TTL 100000000 'small' -> larger LDT (= larger TTL)
--   ck 6  gen1 TTL 100000000 'small' / gen2 TTL 300000000 'big' -> larger LDT      (reversed)
--   ck 7  cell tombstone / cell tombstone (different LDT) -> larger LDT
-- pk=2: row deletion vs row deletion; pk=3: partition deletion vs partition deletion
-- (`DeletionTime.supersedes`, DeletionTime.java:158: equal mfda -> larger LDT).
CREATE KEYSPACE IF NOT EXISTS test_ties
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS test_ties.ties (
    pk int,
    ck int,
    v  text,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};
