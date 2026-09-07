-- issue-4114-vector-float.cql — Cassandra 5.0 reference schema for the
-- `vector<float, n>` READ-COMPLETENESS fixture (issue #4114, epic #3479).
--
-- WHY THIS EXISTS
-- `vector<float, n>` is the Cassandra 5 flagship type and CQLite has no decoder
-- for it, no CQL type variant, and — before this file — no fixture anywhere in
-- the corpus. `git grep -i vector -- test-data/schemas/**` returned exactly one
-- pre-existing hit, `legacy/hardened_validator_test_schema.cql:146`, and that
-- column is `vector_value LIST<DOUBLE>` — a list, not a vector. So nothing
-- exercised the type at all, which is precisely why its failure mode was
-- unknown.
--
-- The first question this fixture exists to answer is NOT "does CQLite decode a
-- vector" (it does not) but "what does CQLite DO with one" — fail closed with a
-- named error, or hand back plausible-looking wrong numbers. Those are a missing
-- feature and a correctness defect respectively, and they carry very different
-- priorities. AC1 of #4114 is that measurement.
--
-- ============================================================================
-- THE ON-DISK ENCODING, DERIVED FROM PINNED CASSANDRA SOURCE
--
-- Read at the tag, never from CQLite's own code (CLAUDE.md format authority):
--   git show cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/VectorType.java
--
-- VectorType's constructor picks its serializer and its fixed length from the
-- ELEMENT type (VectorType.java:94-101):
--
--     this.valueLengthIfFixed = elementType.isValueLengthFixed()
--                             ? elementType.valueLengthIfFixed() * dimension
--                             : super.valueLengthIfFixed();
--     this.serializer = elementType.isValueLengthFixed()
--                     ? new FixedLengthSerializer()
--                     : new VariableLengthSerializer();
--
-- `FloatType.isValueLengthFixed()` is true with `valueLengthIfFixed() == 4`, so
-- for `vector<float, n>`:
--
--   * the serializer is FixedLengthSerializer;
--   * the whole column value is FIXED-LENGTH at exactly `4 * n` bytes;
--   * the elements are packed CONTIGUOUSLY with NO per-element length prefix —
--     FixedLengthSerializer.split() just slices `dimension` successive
--     4-byte windows and calls `checkConsumedFully` (VectorType.java:444-458);
--   * each element is a big-endian IEEE-754 binary32, i.e. FloatType's own
--     encoding.
--
-- So `vector<float, 3>` is 12 bytes, and `vector<float, 384>` is 1536 bytes.
-- It is NOT `LIST<FLOAT>`: a list is a multicell/variable-width shape with
-- per-element framing, and reading one encoding as the other yields garbage.
--
-- This resolves the tension #4114 flags at
-- `cqlite-core/src/parser/repair_clustering.rs:135`, which lists "VectorType"
-- among the VARIABLE-width clustering layouts. Per the source above that is
-- wrong for every fixed-width element type, `float` included; it is right only
-- for a variable-width element type (e.g. `vector<text, n>`, which takes
-- VariableLengthSerializer). One classification cannot cover both.
-- ============================================================================
--
-- ============================================================================
-- COLUMN ORDER IS LOAD-BEARING — the `a_before` / `z_after` sentinels
--
-- Cassandra serializes a row's regular columns in COLUMN-NAME order, so the
-- names here are chosen to pin a known position relative to the vector columns:
--
--     a_before  <  v1  <  v384  <  z_after
--
-- That ordering is what lets the measurement distinguish three outcomes that a
-- single-column fixture would conflate. CQLite's row loop SWALLOWS a failed cell
-- decode — it logs at `debug` and `break`s out of the loop
-- (`cqlite-core/src/storage/sstable/row_decoder/row_data.rs`, CLAUDE.md's fifth
-- blind spot, #3890) — so if the vector kills the decode, every column AFTER it
-- is lost too. `z_after` is the witness for that: it is a plain `text` that can
-- always be decoded on its own, so if it is missing from a returned row the
-- cause is upstream of it. `a_before` is the control that proves the row was
-- reached at all.
--
-- Without those two sentinels, "the vector column is absent" and "the row was
-- truncated at the vector column" look identical, and only the second one also
-- silently loses unrelated user data.
-- ============================================================================
--
-- TWO TABLES, because AC3 requires the type in both shapes and the row-framing
-- paths differ:
--   * vector_pk_only   — partition key only, no clustering column.
--   * vector_clustered — partition key + clustering column.
--
-- DIMENSIONS: n=1 is the degenerate lower bound (4 bytes, a single element, and
-- the shape most likely to be confused with a bare `float`); n=384 is a typical
-- real embedding width (1536 bytes, which also crosses past the small-value
-- shapes a decoder might accidentally special-case); n=3 in the clustered table
-- keeps that table's rows small since its subject is the row shape, not the
-- dimension.
--
-- A NULL VECTOR IS A REQUIRED CASE (AC3). Cassandra does not allow an EMPTY
-- vector — `VectorSerializer.isNull` says so explicitly, "we don't allow empty
-- vectors, so we can just check for null" (VectorType.java:412-417) — so
-- null/unset is the only absent-value shape that exists for this type, and it
-- must be distinguishable from a zero-filled vector.

CREATE KEYSPACE IF NOT EXISTS test_vector
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_vector;

-- Partition-key-only. Regular-column order on disk: a_before, v1, v384, z_after.
CREATE TABLE IF NOT EXISTS vector_pk_only (
    id       int PRIMARY KEY,
    a_before text,             -- control: sorts BEFORE both vector columns
    v1       vector<float, 1>,   -- 4 bytes
    v384     vector<float, 384>, -- 1536 bytes; typical embedding width
    z_after  text              -- witness: sorts AFTER both vector columns
);

-- Partition key + clustering column. Regular-column order: v3, z_after.
CREATE TABLE IF NOT EXISTS vector_clustered (
    pk       int,
    ck       int,
    v3       vector<float, 3>,   -- 12 bytes
    z_after  text,             -- witness: sorts AFTER the vector column
    PRIMARY KEY (pk, ck)
);
