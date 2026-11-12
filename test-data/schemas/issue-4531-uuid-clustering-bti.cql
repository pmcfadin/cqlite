-- issue-4531-uuid-clustering-bti.cql — Cassandra 5.0.8 BTI (`da`) schema for uuid/timeuuid
-- CLUSTERING columns (issue #4531).
--
-- WHY: CQLite's OSS50 clustering encoder writes a `uuid`/`timeuuid` as its RAW 16 bytes, while
-- Cassandra's `UUIDType`/`TimeUUIDType.asComparableBytes` swizzle them (version-first /
-- timestamp-reordered). The Rows.db row-index separators of a Cassandra-written table are in
-- the swizzled order, so a slice's physical byte bounds may select the wrong blocks. No
-- datasets-v4 BTI table had a uuid/timeuuid clustering column, so the effect was unmeasured.
--
-- Tables (partition 1 is LARGE: many Rows.db blocks at column_index_size=2KiB; partition 2 small):
--   timeuuid_clustering       ck timeuuid ASC
--   timeuuid_clustering_desc  ck timeuuid DESC  (ReversedType path)
--   uuid_clustering           ck uuid, mixing version-1 and version-4 values
--
-- Generator: test-data/scripts/generate-issue-4531-uuid-clustering-bti.sh
CREATE KEYSPACE IF NOT EXISTS test_uuid_bti
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS test_uuid_bti.timeuuid_clustering (
    pk int, ck timeuuid, payload text, PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS test_uuid_bti.timeuuid_clustering_desc (
    pk int, ck timeuuid, payload text, PRIMARY KEY (pk, ck)
) WITH CLUSTERING ORDER BY (ck DESC) AND compression = {'class': 'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS test_uuid_bti.uuid_clustering (
    pk int, ck uuid, payload text, PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};
