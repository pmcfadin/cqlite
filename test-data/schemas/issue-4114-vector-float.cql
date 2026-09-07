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

-- ============================================================================
-- vector_last — THE SILENT-MIS-DECODE REACHABILITY SUBJECT (issue #4114)
--
-- The two tables above prove a cell-level mis-decode but BOTH of them FAIL
-- CLOSED end-to-end, for two different reasons, and neither reason is a
-- property of the vector type:
--
--   * vector_clustered.v3 = [1.0, ...] starts `3f 80 00 00`. A reader that
--     treats the value as vint-length-prefixed consumes 0x3f == 63 as a length
--     with only ~24 bytes remaining, so the bounds check rejects it => Err.
--   * vector_pk_only.v1 = [1.5] starts `3f c0 00 00`, and there the 63-byte
--     read IS satisfiable (a 1536-byte v384 follows), so v1 silently returns 63
--     wrong bytes with NO error — but the resulting offset desync then lands
--     mid-`v384` and the row dies a few columns later.
--
-- So in both shapes the user sees an error. That makes the defect look like a
-- missing feature. The question that changes the priority is whether a FULLY
-- SILENT WRONG ROW is reachable: emitted to the user, wrong value, exit 0.
--
-- This table is built to answer exactly that, by removing both escape hatches:
--
--   1. The vector is the ONLY regular column, so there is NOTHING after it to
--      desync into. Any bytes the mis-decode leaves unconsumed sit at the end
--      of the row body, where a size-prefixed row body simply ends.
--   2. The FIRST ELEMENT is chosen so its leading big-endian byte is a SMALL,
--      SATISFIABLE vint length:
--        id=1  v3[0] = 0.0                     => 00 00 00 00 => vint 0x00 = 0
--        id=2  v3[0] = 3.851859888774472e-34   => 08 00 00 00 => vint 0x08 = 8
--      (that second literal is exactly 2^-111: exponent field 127-111 == 16 ==
--      0x10, mantissa 0, so the binary32 encoding is precisely 0x08000000 —
--      chosen for its LEADING BYTE, not for its magnitude.)
--      Both lengths are far below the bytes remaining, so the bounds check that
--      saves the other two tables cannot fire.
--
-- The remaining elements are 1.0 and 2.0 (`3f 80 00 00`, `40 00 00 00`) so that
-- the bytes a mis-decode hands back are legible in a hex dump and obviously
-- belong to the LATER elements of this same value.
--
-- There is deliberately NO sentinel column here. A sentinel would sort after
-- the vector and re-introduce escape hatch (1); the partition key in the
-- emitted row is sufficient evidence that the row was reached and returned.
-- ============================================================================
CREATE TABLE IF NOT EXISTS vector_last (
    id       int PRIMARY KEY,
    v3       vector<float, 3>   -- 12 bytes; the ONLY regular column
);

-- ============================================================================
-- vector_exact — THE FULLY-SILENT CASE (issue #4114)
--
-- vector_last (above) FAILS CLOSED, but NOT via the vint bounds check: the
-- measured error is a ROW-BODY UNDER-CONSUMPTION guard. With v3[0]=0.0 the
-- bogus vint 0x00 makes the mis-decode consume 1 byte of a 12-byte value, so
-- 11 bytes of the row body belong to no column and the reader rejects the row
-- against the row header's authoritative row_size.
--
-- That guard is an ACCOUNTING check, so it is defeated by arithmetic rather
-- than by bounds: it cannot fire when the mis-decode consumes EXACTLY the
-- value's width. For a `vector<float,3>` the misreader consumes
-- `1 (the vint byte) + len`, so the row body balances precisely when
-- `len == 11`, i.e. when the first element's leading big-endian byte is 0x0b.
--
-- 2^-105 encodes as exactly 0x0b000000: exponent field 127-105 == 22, and the
-- top 7 bits of 22 are 0b0001011 == 0x0b with a zero mantissa. So
-- v3 = [2^-105, x, y] mis-decodes into an 11-byte blob — the *tail* of its own
-- value — consumes all 12 bytes, balances the row, and is emitted with NO
-- error. Again the literal is chosen for its LEADING BYTE, not its magnitude.
--
-- TWO ROWS WITH DIFFERENT TAILS, because a single row cannot distinguish a
-- data-derived wrong value from a constant. id=1 tails on [1.0, 2.0] and id=2
-- on [4.5, -5.0], so the two wrong blobs differ in exactly the bytes the tails
-- differ in.
--
-- BOTH rows must mis-decode silently for the demonstration to be visible at
-- all: the reader ABORTS the whole scan on the first partition that errors
-- (measured on vector_last: exit 5, zero rows emitted), so a single
-- fail-closed row anywhere in this table would mask every silent row after it.
-- That is also why this is a separate table from vector_last rather than a
-- third row in it.
-- ============================================================================
CREATE TABLE IF NOT EXISTS vector_exact (
    id       int PRIMARY KEY,
    v3       vector<float, 3>   -- 12 bytes; the ONLY regular column
);
