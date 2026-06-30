-- signed-collection-parity.cql — Cassandra 5.0.2 reference schema for the
-- SIGNED numeric collection-element / map-key ordering golden (issue #1295,
-- follow-up to #1275).
--
-- These tables are written BY CASSANDRA (cqlsh + nodetool flush) to produce
-- committed reference SSTables containing collections whose numeric
-- elements/keys MIX NEGATIVE AND POSITIVE values. Cassandra's `SetType` and
-- `MapType` sort by the element/key type's own `AbstractType.compare`, which for
-- the integer marshal types (Int32Type / LongType / ShortType / ByteType) and
-- DecimalType / DoubleType is SIGNED. A collection containing -1 must therefore
-- persist -1 BEFORE 0/1 — the opposite of the raw big-endian two's-complement
-- (unsigned) byte order in which -1 sorts LAST.
--
-- The committed on-disk ordering of these collections is the authoritative
-- ORACLE that issue #1275's previously-DERIVED writer tests are upgraded to
-- assert byte-for-byte against (cqlite-core/.../scenarios_4.rs and the read-path
-- test cqlite-core/tests/issue_1295_signed_collection_order_parity.rs).
--
-- DETERMINISM NOTE: these fixtures assert ELEMENT-ORDER parity, not whole-artifact
-- byte parity, so no USING TIMESTAMP contract is needed — the on-disk
-- collection element/key byte ORDER (and the inner element bytes) is what is
-- compared against the sstabledump golden. Tables are UNCOMPRESSED so Data.db is
-- a direct byte slice.
--
-- Tables (negative + positive numeric collection elements/keys):
--   1. signed_int_collections    — non-frozen set<int> / map<int,text>
--   2. frozen_int_collections    — frozen<set<int>> / frozen<map<int,text>>
--   3. signed_width_collections  — set<bigint> / set<smallint> / set<tinyint>
--                                  (other integer-width signed comparator branches)
--   4. signed_special_collections — set<decimal> (large unscaled) + set<double>
--                                  (NaN / +0.0 / -0.0 comparator branches)

CREATE KEYSPACE IF NOT EXISTS test_signed_coll WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_signed_coll;

-- Non-frozen (multicell) set<int> / map<int,text> with negative elements/keys.
CREATE TABLE IF NOT EXISTS signed_int_collections (
    id   INT PRIMARY KEY,
    s    SET<INT>,
    m    MAP<INT, TEXT>
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Frozen (single-cell) set<int> / map<int,text> with negative elements/keys.
CREATE TABLE IF NOT EXISTS frozen_int_collections (
    id   INT PRIMARY KEY,
    fs   FROZEN<SET<INT>>,
    fm   FROZEN<MAP<INT, TEXT>>
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Other integer-width signed comparator branches.
CREATE TABLE IF NOT EXISTS signed_width_collections (
    id    INT PRIMARY KEY,
    sb    SET<BIGINT>,
    ss    SET<SMALLINT>,
    st    SET<TINYINT>
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- decimal (large unscaled) + double (NaN / signed-zero) comparator branches.
CREATE TABLE IF NOT EXISTS signed_special_collections (
    id    INT PRIMARY KEY,
    sd    SET<DECIMAL>,
    sf    SET<DOUBLE>
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
