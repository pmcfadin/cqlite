-- compaction-parity-udt.cql — Cassandra 5.0.2 reference schema for the UDT /
-- frozen-value COMPACTION byte-parity slice (issue #1020, epic #973).
--
-- These three tables are written BY CASSANDRA (cqlsh + nodetool flush) as TWO
-- overlapping SSTables each, then MAJOR-COMPACTED (`nodetool compact`) into a
-- single output SSTable. CQLite re-produces the same two inputs (via its public
-- WriteEngine API with a UdtRegistry) and runs its own `compact_sstables` over
-- them. The byte-comparison test
-- (cqlite-core/tests/issue_1020_udt_frozen_compaction_byte_parity.rs) diffs the
-- two COMPACTED outputs (Data.db/Index.db/Summary.db/Digest.crc32 byte-for-byte;
-- CRC.db prefix; Statistics.db/Filter.db present-not-diffed) and the secondary
-- sstabledump JSONL golden.
--
-- This is the UDT / frozen-value extension of the foundational live-cell slice
-- (issue #1017 / compaction-parity.cql). EVERY value here is FROZEN, so each
-- complex value serializes as a SINGLE value cell (no multicell cell-paths), the
-- smallest deterministic surface two independent compactors can byte-match for
-- UDTs and frozen collections.
--
-- The DDL + UDT definitions are committed WITH the fixtures (this file) so CQLite
-- cannot pass by rendering a blob fallback: the parity test builds the SAME
-- UdtRegistry from these definitions and FAILS LOUDLY if a UDT cannot be resolved.
--
-- DETERMINISM CONTRACT (so two independent compactors can byte-match):
--   * Every mutation carries an explicit USING TIMESTAMP so the cell writetime
--     (and therefore the EncodingStats.minTimestamp delta baseline) is fixed and
--     identical on both engines. The two fixed writetimes are:
--         T_A = 1000   (older generation), micros
--         T_B = 2000   (newer generation, wins overlaps), micros
--   * Tables are UNCOMPRESSED (no CompressionInfo.db) so Data.db is a direct
--     byte slice with no chunk framing differences.
--   * Partition keys are int so the 4-byte big-endian key bytes and Murmur3
--     token ordering are identical and deterministic on both engines.
--   * No TTL and no DELETE: purge never fires for live cells, so the compaction
--     output is independent of gcBefore / nowInSeconds and reproducible.
--   * autocompaction is disabled and a single explicit major compaction is run,
--     so exactly two inputs merge into exactly one deterministic output.
--
-- Field-ordering / null-field / nested-field / frozen-boundary coverage (AC):
--   * udt_frozen_person  — frozen<person> with a FULLY populated value, a value
--     with a NULL middle field, and a value with an EMPTY-string field, so UDT
--     field ORDER + null-field encoding + frozen value boundaries are exercised.
--   * udt_nested         — frozen<employee> which CONTAINS a frozen<address>, so
--     NESTED UDT field decoding + the inner frozen value boundary are exercised,
--     including an inner-UDT with a null field.
--   * udt_collections    — frozen<list<frozen<person>>> and
--     frozen<map<text, frozen<address>>>: COLLECTIONS CONTAINING UDT VALUES,
--     plus the plain frozen<list<int>> / frozen<map<text,int>> the AC names.

CREATE KEYSPACE IF NOT EXISTS test_compactionparityudt WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_compactionparityudt;

-- ── User-Defined Types ──────────────────────────────────────────────────────
-- person: multi-field UDT exercising field-order + null/empty field encoding.
CREATE TYPE IF NOT EXISTS person (
    first_name text,
    last_name  text,
    age        int
);

-- address: inner UDT used both standalone and nested inside `employee`.
CREATE TYPE IF NOT EXISTS address (
    street text,
    city   text,
    zip    text
);

-- employee: a UDT that CONTAINS a frozen<address> (nested UDT).
CREATE TYPE IF NOT EXISTS employee (
    name  text,
    home  frozen<address>,
    level int
);

-- ── Tables (every complex value FROZEN) ──────────────────────────────────────

-- udt_frozen_person — frozen<person> values: full / null-middle-field / empty.
CREATE TABLE IF NOT EXISTS udt_frozen_person (
    id INT PRIMARY KEY,
    p  frozen<person>
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- udt_nested — frozen<employee> (contains frozen<address>): nested UDT decoding.
CREATE TABLE IF NOT EXISTS udt_nested (
    id INT PRIMARY KEY,
    e  frozen<employee>
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- udt_collections — frozen collections, including collections of UDT values and
-- the plain frozen<list<int>> / frozen<map<text,int>> the AC names.
CREATE TABLE IF NOT EXISTS udt_collections (
    id  INT PRIMARY KEY,
    fl  frozen<list<int>>,
    fm  frozen<map<text,int>>,
    lp  frozen<list<frozen<person>>>,
    ma  frozen<map<text, frozen<address>>>
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
