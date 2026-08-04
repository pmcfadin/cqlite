-- CQLite-WRITTEN BTI (`da`) fixture — a PERFORMANCE DRIFT CONTROL ONLY.
-- Issue #3234, owner decision 1C.
--
-- !! NOT A CORRECTNESS ORACLE, AND IT MAY NEVER BE USED AS ONE !!
--
-- The fixture this schema describes is written by cqlite-core's PRODUCTION
-- `SSTableWriter::with_format(.., SSTableFormat::Bti)` and read back by CQLite. Such a
-- corpus is INVARIANT to a uniform framing/serialization error (issue #3042): both
-- halves make the identical mistake, the round-trip closes, and the test stays green
-- while real Cassandra-written data reads wrong. For BTI that is not hypothetical —
-- #3002 (the `Rows.db` row-index root base 2 bytes low, missing
-- `writeWithShortLength`'s 2-byte prefix) hid behind exactly such a symmetric test,
-- masked by a second, compensating encoder defect. Two defects that cancel are
-- undetectable by a symmetric test by construction.
--
-- The BTI correctness oracle is CASSANDRA-WRITTEN bytes:
--   test-data/datasets/sstables/test_da/**  (incl. the committed small golden
--   `wide_multiclustering_small-*`, test-data/schemas/wide-multiclustering-small-bti.cql)
-- or the pinned Cassandra 5.0.8 source. Never this file's fixture.
--
-- WHAT IT IS FOR: read-path PERFORMANCE drift work. It needs no container, no
-- Cassandra and no fetched corpus, and its bytes are REPRODUCIBLE from a recorded
-- seed — so two measurements taken weeks apart provably ran on identical input. The
-- Cassandra-written corpora cannot offer that: Cassandra stamps a wall-clock write
-- timestamp into every row, so even a same-seed regeneration changes the `Data.db`
-- length (measured on the #3234 perf corpus: 19,474,015 B vs 19,474,397 B).
--
-- Byte-identity is VERIFIED, not asserted: the owning test writes the fixture three
-- times and compares every component byte for byte.
--   Test:     cqlite-core/tests/issue_3234_cqlite_written_bti_drift_control.rs
--   Identity: test-data/cqlite-written-bti-drift-control-identity.json
--             (per-component sha256 + byte size + row/partition counts + the seed —
--              its OWN identity, never inherited from the perf corpus's manifest)
--
-- Shape: 4 partitions / 248 rows (one WIDE partition of 200 x ~2 KiB rows, so `Rows.db`
-- is non-empty and the BTI row-index writer is exercised too), UNCOMPRESSED (CQLite's production write surface
-- emits no CompressionInfo.db; configuring compressed production writes is
-- fail-closed per #1406), partitions written in Murmur3 token order. The statement
-- below is asserted to match the test's in-code DDL, so the two cannot drift apart.

CREATE TABLE drift_control.bti_written_small (
    pk int,
    bucket text,
    seq int,
    payload text,
    PRIMARY KEY (pk, bucket, seq)
);
