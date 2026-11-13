-- issue-4543-percell-cross-gen.cql — Cassandra 5.0.8 schema for the cross-generation PER-CELL
-- timestamp fixture (issue #4543). Generator: test-data/scripts/generate-issue-4543-percell-cross-gen.sh
-- Two generations; the NEWER file holds OLDER per-cell timestamps for some cells (pk=1):
--   ck 1  gen1 a@300 b@100 c@500 / gen2 a@100 b@400   -> a from gen1, b from gen2, c from gen1
--   ck 2  gen1 row delete@200   / gen2 a@150 b@300     -> a shadowed (150<=200), b survives
--   ck 3  gen1 a@100            / gen2 a-tombstone@50  -> a survives (older tombstone loses)
--   ck 4  gen1 row delete@200   / gen2 a@100           -> row invisible
--   ck 5  gen1 row delete@200   / gen2 DELETE a@300     -> NO row (a newer cell TOMBSTONE is not live data; #4545 review)
--   ck 6  gen1 row delete@200   / gen2 INSERT (pk,ck)@300 TTL 5, expired before flush -> NO row (an EXPIRED row marker is not liveness)
-- pk=2 only in gen1, pk=3 only in gen2.
CREATE KEYSPACE IF NOT EXISTS test_percell
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS test_percell.pcell (
    pk int, ck int, a text, b text, c text,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};
