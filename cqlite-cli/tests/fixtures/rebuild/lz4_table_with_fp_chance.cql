-- Issue #4197 (spec R3.1/R9.1) — `test_comp.lz4_table` as declared in
-- test-data/schemas/compression-parity.cql, PLUS the explicit
-- `bloom_filter_fp_chance` option no committed corpus schema states
-- (premise task 0.2), so `cqlite rebuild --components filter` can classify
-- that field as `recovered` rather than `recomputed`.
--
-- The value is Cassandra's own, not a value chosen to make a test pass: the
-- fixture generation's Cassandra-written `sstablemetadata` sidecar
-- (`nb-1-big-Statistics.db.txt`) records `Bloom Filter FP chance: 0.01`.
CREATE TABLE IF NOT EXISTS lz4_table (
    pk   INT,
    ck   INT,
    body TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 16}
  AND bloom_filter_fp_chance = 0.01;
