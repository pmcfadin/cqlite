-- Field-repro "tiny" fixture (issue #2289, #2193 pinned check): the exact shape
-- that trips the arrow-java `Failed to read message` decode failure — 3 rows,
-- ONE flush -> ONE `nb-1-big` SSTable, LZ4 compression. Kept in its own
-- keyspace/file so `field-repro.sh` never touches `e2e-data.cql`'s
-- `analytics` keyspace or its flush count.
--
-- NB: CQL/cqlsh comments are `--`/`//`/`/* */` ONLY — `#` is NOT a valid CQL
-- comment leader (unlike this repo's YAML/shell conventions) and made cqlsh
-- abort the whole batch with "Invalid syntax" on a real run of this file
-- (issue #2289 field-repro.sh dry run, 2026-07-09) before this fix.
CREATE KEYSPACE IF NOT EXISTS fieldrepro
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS fieldrepro.tiny (
  id int PRIMARY KEY,
  val text
) WITH compression = {'class': 'LZ4Compressor'};

INSERT INTO fieldrepro.tiny (id, val) VALUES (1, 'a');
INSERT INTO fieldrepro.tiny (id, val) VALUES (2, 'b');
INSERT INTO fieldrepro.tiny (id, val) VALUES (3, 'c');
