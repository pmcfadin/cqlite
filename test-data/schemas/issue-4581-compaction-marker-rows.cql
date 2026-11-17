-- issue-4581-compaction-marker-rows.cql — Cassandra 5.0.8 reference schema for the "compaction of
-- rows whose only content is the primary-key liveness marker" fixtures (issue #4581).
--
-- Every table is compacted by Cassandra itself (`nodetool compact`); the generator records the
-- sstabledump of the INPUT SSTables and of Cassandra's COMPACTED output. CQLite compacts the same
-- inputs and must reproduce the output (rows, liveness ts/TTL/LDT, deletions).
--
-- Every table is UNCOMPRESSED (`compression = {'enabled': 'false'}`) so CQLite's output (always
-- uncompressed, #1406) can be compared to Cassandra's compacted Data.db BYTE FOR BYTE.
--
-- gc_grace_seconds is part of the scenario: the *_gc tables use 1 second so a major compaction run
-- seconds after the writes is PAST grace; the others keep the default (864000 s) so every expired
-- marker / deletion is WITHIN grace and is kept verbatim.
--
-- Generator: test-data/scripts/generate-issue-4581-compaction-marker-rows.sh (documents every row's
-- statements and expected outcome). Backs: issue #4581.

CREATE KEYSPACE IF NOT EXISTS test_markerrows
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- live marker-only rows, TTL'd-but-live markers, marker older than a cell, equal-ts liveness ties.
CREATE TABLE IF NOT EXISTS test_markerrows.live_marker (
    id int PRIMARY KEY,
    v  text
) WITH compression = {'enabled': 'false'};

-- BYTE-PARITY shape: marker-only rows beside ONE row that writes `v`, so the SSTable header lists
-- `v` (Cassandra's compaction header columns are the union of the INPUT headers), and no TTL / cell
-- tombstone (both have their own, marker-unrelated, encoding-baseline / HAS_ALL_COLUMNS gaps).
CREATE TABLE IF NOT EXISTS test_markerrows.byte_parity (
    id int PRIMARY KEY,
    v  text
) WITH compression = {'enabled': 'false'};

-- expired markers WITHIN gc_grace (kept verbatim), incl. liveness supersedes() tie-breaks.
CREATE TABLE IF NOT EXISTS test_markerrows.ttl_within_grace (
    id int PRIMARY KEY,
    v  text
) WITH compression = {'enabled': 'false'};

-- expired markers PAST gc_grace (purged), a marker that outlives its purged cell tombstone.
CREATE TABLE IF NOT EXISTS test_markerrows.ttl_past_grace (
    id int PRIMARY KEY,
    v  text
) WITH gc_grace_seconds = 1
  AND compression = {'enabled': 'false'};

-- partition deletion vs marker (equal ts, older, newer), deletion kept (within grace).
CREATE TABLE IF NOT EXISTS test_markerrows.partition_del (
    id int PRIMARY KEY,
    v  text
) WITH compression = {'enabled': 'false'};

-- partition deletion over a marker, then gc-purged.
CREATE TABLE IF NOT EXISTS test_markerrows.partition_del_gc (
    id int PRIMARY KEY,
    v  text
) WITH gc_grace_seconds = 1
  AND compression = {'enabled': 'false'};

-- row deletion vs marker on a clustered table (within grace).
CREATE TABLE IF NOT EXISTS test_markerrows.clustered_markers (
    pk int,
    ck int,
    v  text,
    PRIMARY KEY (pk, ck)
) WITH compression = {'enabled': 'false'};

-- row deletion vs marker, then gc-purged; an expired marker past grace on a clustered table.
CREATE TABLE IF NOT EXISTS test_markerrows.clustered_markers_gc (
    pk int,
    ck int,
    v  text,
    PRIMARY KEY (pk, ck)
) WITH gc_grace_seconds = 1
  AND compression = {'enabled': 'false'};

-- UPDATE-created rows carry NO primary-key liveness marker (Cassandra writes `LivenessInfo.EMPTY`,
-- no HAS_TIMESTAMP); compaction must not invent one from the cells (#4581, audit caveat 2).
-- Plain UPDATE, UPDATE ... USING TTL (live and expired within grace), and an INSERT control.
CREATE TABLE IF NOT EXISTS test_markerrows.update_rows (
    id int PRIMARY KEY,
    v  text
) WITH compression = {'enabled': 'false'};

-- UPDATE-created rows past gc_grace: an expired UPDATE ... USING TTL leaves NO ghost row.
CREATE TABLE IF NOT EXISTS test_markerrows.update_rows_gc (
    id int PRIMARY KEY,
    v  text
) WITH gc_grace_seconds = 1
  AND compression = {'enabled': 'false'};

-- A purged marker beside surviving data (#4581, audit caveat 1): INSERT ... USING TTL then a plain
-- UPDATE; compacted past gc_grace the marker is purged (BTreeRow.purge -> EMPTY), the data stays,
-- and the row carries no marker. Its INPUTS also back the public-surface read guard (a clustering-
-- less table with a live marker-only row, an expired marker-only row and an expired marker with
-- live data).
CREATE TABLE IF NOT EXISTS test_markerrows.purged_marker (
    id int PRIMARY KEY,
    v  text
) WITH gc_grace_seconds = 1
  AND compression = {'enabled': 'false'};
