-- compression-parity.cql — Compression / chunk-format parity SSTable fixtures for CQLite epic #970 (issue #996)
--
-- Generated with Cassandra 5.0.2, storage_compatibility_mode: CASSANDRA_4 (nb BIG format)
--
-- Keyspace: test_comp (isolated from every other corpus so the compression
--           fixtures never collide with test_basic/test_tomb/etc.)
-- SSTable version: nb (BIG format, storage_compatibility_mode: CASSANDRA_4)
--
-- PURPOSE
-- -------
-- One table per compression scenario the epic #970 (compression / checksum
-- parity) program must read byte-for-byte the same as Apache Cassandra. Each
-- table pins its compressor class AND an explicit `chunk_length_in_kb` so the
-- CompressionInfo.db header (algorithm string + chunk_length) is fully
-- deterministic. Data is inserted from a FIXED PRNG seed (see
-- generate-compression-parity.sh) so re-runs produce equivalent logical fixtures.
--
-- DETERMINISM
-- -----------
--   * All writes use explicit `USING TIMESTAMP <micros>` (T_BASE below) so
--     writetime is fixed.
--   * Payload bytes are produced from a fixed seed (SEED=0x00C0FFEE in the
--     generator's PRNG) so compressible text and incompressible blobs are
--     reproducible across runs.
--     T_BASE = 1609459200000000  (2021-01-01T00:00:00Z)
--
-- SCENARIO -> TABLE -> MANIFEST KEY (fixture_matrix.*) -> COMPRESSION -> chunk_kb
-- ----------------------------------------------------------------------------
--   LZ4               -> lz4_table               -> fixture_matrix.lz4                            LZ4Compressor      / 16
--   Snappy            -> snappy_table            -> fixture_matrix.snappy                         SnappyCompressor   / 16
--   Deflate           -> deflate_table           -> fixture_matrix.deflate                        DeflateCompressor  / 16
--   Zstd (no dict)    -> zstd_table              -> fixture_matrix.zstd_no_dictionary             ZstdCompressor     / 16
--   Uncompressed      -> uncompressed_table      -> fixture_matrix.uncompressed_table             {'enabled': false} / n-a
--   Short final chunk -> short_final_chunk       -> fixture_matrix.short_final_chunk              LZ4Compressor      / 4
--   Incompressible    -> incompressible_uncompressed_chunk
--                                                -> fixture_matrix.incompressible_uncompressed_chunk
--                                                                                                 LZ4Compressor      / 4 (min_compress_ratio=1.0)
--
-- NOTES ON THE TWO "PROPERTY" TABLES
-- ----------------------------------
--   short_final_chunk: chunk_length_in_kb = 4 (= 4096 uncompressed bytes/chunk).
--     We write enough highly-compressible text rows that the total uncompressed
--     Data.db payload is NOT a multiple of 4096, so the LAST compressed chunk
--     covers fewer than 4096 uncompressed bytes (a "short final chunk"). The
--     generator verifies this by parsing the chunk-offset table.
--
--   incompressible_uncompressed_chunk: chunk_length_in_kb = 4 AND
--     min_compress_ratio = 1.0. Each row carries a high-entropy (fixed-PRNG)
--     blob. LZ4 cannot shrink random bytes. min_compress_ratio = 1.0 sets
--     maxCompressedLength = chunk_length (4096); CompressedSequentialWriter then
--     stores any chunk whose compressed form is >= 4096 bytes RAW (its
--     uncompressed-chunk fallback). NOTE: with the DEFAULT min_compress_ratio = 0
--     this fallback NEVER fires (maxCompressedLength = Integer.MAX_VALUE = the
--     0x7FFFFFFF you see in other tables' CompressionInfo.db), which is why this
--     table pins min_compress_ratio explicitly. The generator verifies at least
--     one chunk's on-disk compressed length == its raw uncompressed length.

CREATE KEYSPACE IF NOT EXISTS test_comp WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_comp;

-- -----------------------------------------------------------------------
-- Table 1: lz4_table  (fixture_matrix.lz4)
-- LZ4Compressor, chunk_length_in_kb = 16 (default chunk size, explicit).
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lz4_table (
    pk   INT,
    ck   INT,
    body TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 16};

-- -----------------------------------------------------------------------
-- Table 2: snappy_table  (fixture_matrix.snappy)
-- SnappyCompressor, chunk_length_in_kb = 16.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS snappy_table (
    pk   INT,
    ck   INT,
    body TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'SnappyCompressor', 'chunk_length_in_kb': 16};

-- -----------------------------------------------------------------------
-- Table 3: deflate_table  (fixture_matrix.deflate)
-- DeflateCompressor, chunk_length_in_kb = 16.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS deflate_table (
    pk   INT,
    ck   INT,
    body TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'DeflateCompressor', 'chunk_length_in_kb': 16};

-- -----------------------------------------------------------------------
-- Table 4: zstd_table  (fixture_matrix.zstd_no_dictionary)
-- ZstdCompressor, plain, NO dictionary, chunk_length_in_kb = 16.
-- Cassandra's ZstdCompressor does not use a trained dictionary; this is the
-- plain per-chunk Zstd path.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS zstd_table (
    pk   INT,
    ck   INT,
    body TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'ZstdCompressor', 'chunk_length_in_kb': 16};

-- -----------------------------------------------------------------------
-- Table 5: uncompressed_table  (fixture_matrix.uncompressed_table)
-- compression disabled. NO CompressionInfo.db is emitted; instead Cassandra
-- writes a CRC.db component (4-byte chunk size header + per-chunk CRC32).
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS uncompressed_table (
    pk   INT,
    ck   INT,
    body TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'enabled': false};

-- -----------------------------------------------------------------------
-- Table 6: short_final_chunk  (fixture_matrix.short_final_chunk)
-- LZ4Compressor, chunk_length_in_kb = 4. Row volume chosen so the total
-- uncompressed payload is NOT a multiple of 4096 -> last chunk is short.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS short_final_chunk (
    pk   INT,
    ck   INT,
    body TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 4};

-- -----------------------------------------------------------------------
-- Table 7: incompressible_uncompressed_chunk
--          (fixture_matrix.incompressible_uncompressed_chunk)
-- LZ4Compressor, chunk_length_in_kb = 4. High-entropy fixed-PRNG blob payload
-- so LZ4 cannot shrink it.
--
-- min_compress_ratio = 1.0 is REQUIRED to exercise Cassandra's
-- uncompressed-chunk fallback. With the DEFAULT min_compress_ratio = 0,
-- CompressionParams.maxCompressedLength() returns Integer.MAX_VALUE
-- (CompressionInfo.db stores 0x7FFFFFFF), so CompressedSequentialWriter NEVER
-- stores a chunk raw — it keeps the (possibly larger) compressed bytes. Setting
-- min_compress_ratio = 1.0 makes maxCompressedLength = chunk_length / 1.0 =
-- 4096; any chunk whose compressed form is >= 4096 bytes (which random data is)
-- is then written RAW at exactly the uncompressed chunk length. The generator
-- verifies compressed_len == raw_chunk_len for at least one chunk.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS incompressible_uncompressed_chunk (
    pk   INT,
    ck   INT,
    payload BLOB,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 4, 'min_compress_ratio': 1.0};
