-- cql-type-parity.cql — CQL Type & Schema-Evolution parity SSTable fixtures for CQLite epic #971
--
-- Generated with Cassandra 5.0.2, storage_compatibility_mode: CASSANDRA_4 (nb BIG format)
--
-- Keyspace: test_types (isolated from every other test corpus so they never collide)
-- SSTable version: nb (BIG format, storage_compatibility_mode: CASSANDRA_4)
--
-- All NON-COUNTER writes use explicit `USING TIMESTAMP <micros>` so writetime is
-- 100% deterministic and reproducible. COUNTER writes do NOT (and CANNOT) carry
-- USING TIMESTAMP — Cassandra rejects it on counter UPDATEs; counter cells carry a
-- driver/coordinator-derived clock instead. This is documented per-table below.
--
-- FIXED TIMESTAMP SCHEME (documented in generate-cql-type-parity.sh too)
-- -----------------------------------------------------------------------
--   BASE_EPOCH_SECONDS = 1609459200   (2021-01-01T00:00:00Z)
--   BASE_TS_MICROS     = 1609459200000000
--
--   Per-generation write timestamps (micros) — strictly increasing so newer
--   writes/drops always shadow older live data:
--     T_GEN1 = 1609459200000000  (2021-01-01T00:00:00Z)  base
--     T_GEN2 = 1609545600000000  (2021-01-02T00:00:00Z)  base + 1 day
--     T_GEN3 = 1609632000000000  (2021-01-03T00:00:00Z)  base + 2 days
--
-- =====================================================================
-- ALTER TYPE SUBSTITUTION DECISIONS (Cassandra 5.0.2)
-- =====================================================================
-- Apache Cassandra 5.0 effectively DISABLES `ALTER TABLE ... ALTER <col> TYPE ...`.
-- The historically-allowed "compatible widening" set (e.g. int->varint, the
-- text<->varchar alias, or any value-compatible change) was progressively
-- restricted and the statement throws "Altering of types is not allowed"
-- (CASSANDRA-12443 / 5.0 hardening). Therefore we CANNOT model an on-disk
-- SerializationHeader type-map divergence via ALTER ... TYPE.
--
-- SUBSTITUTION: every "altered column" concern is instead modelled as an
-- ADD-column / DROP-column header divergence between generations, which DOES
-- produce a gen-1 SerializationHeader whose declared column set differs from the
-- gen-2 schema — byte-for-byte readable and exactly the parity surface #1003
-- needs. Per-table on-disk header difference is documented inline below.
--
-- =====================================================================
-- TABLE -> ISSUE / MANIFEST-ID -> GENERATIONS
-- =====================================================================
--  GROUP A — Schema evolution / SerializationHeader (#1003)
--   1. se_no_schema_change            #1003  control, no ALTER                 2 gens
--   2. se_altered_column_type         #1003  ADD col in gen-2 (ALTER-TYPE sub) 2 gens
--   3. se_dropped_column_same_type    #1003  DROP text col between flushes     2 gens
--   4. se_altered_then_dropped_column #1003  ADD (g2) then DROP (g3) a col      3 gens
--   5. se_static_regular_kind_mismatch#1003  static col recorded in header     1 gen
--   6. se_frozen_multicell_collection #1003  frozen vs multicell list flag     1 gen
--
--  GROUP B — Null vs empty & length boundaries (#1006)
--   7. nb_null_empty_text_blob        #1006  absent/NULL/''/0x with neighbors  1 gen
--   8. nb_absent_vs_null_regular      #1006  absent vs deleted vs empty rows   1 gen
--   9. nb_empty_collections           #1006  empty multicell vs frozen empty   1 gen
--  10. nb_length_prefix_edges         #1006  len 0,1,127,128,...,16384 edges   1 gen
--
--  GROUP C — UDT / tuple / frozen / nested (#1007)
--  11. cx_tuple_field_order           #1007  tuple<int,text,boolean> + null    1 gen
--  12. cx_udt_field_order_null_empty  #1007  frozen UDT null/''/full           1 gen
--  13. cx_frozen_udt_value            #1007  frozen<udt> values                1 gen
--  14. cx_nested_frozen_collections   #1007  map<text,frozen<list>>, etc.      1 gen
--  15. cx_multicell_udt_collection    #1007  multicell UDT + multicell coll.   1 gen
--  16. cx_legacy_dropped_tuple_udt    #1007  DROP a tuple/UDT col between gens  2 gens
--
--  GROUP D — Counters (#1008)  [nightly_docker]
--  17. ct_single_sstable             #1008  one counter, increments           1 gen
--  18. ct_multi_sstable_merge        #1008  inc gen-1, inc/dec gen-2           2 gens
--  19. ct_deleted_counter_shadowing  #1008  inc, flush, DELETE, flush          2 gens
--  20. ct_compacted_final_value      #1008  multi-gen + nodetool compact       2 gens + compacted
--
-- Backs: epic #971 (issues #1003, #1006, #1007, #1008)

CREATE KEYSPACE IF NOT EXISTS test_types WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_types;

-- =====================================================================
-- User-Defined Types (Group C)
-- =====================================================================

-- Multi-field UDT exercising field-order + null/empty field encoding.
CREATE TYPE IF NOT EXISTS person_type (
    first_name text,
    last_name  text,
    age        int,
    active     boolean
);

-- Nested UDT (a UDT that contains another frozen UDT) for cx_frozen_udt_value.
CREATE TYPE IF NOT EXISTS address_type (
    street text,
    city   text,
    zip    text
);

CREATE TYPE IF NOT EXISTS employee_type (
    name      text,
    home      frozen<address_type>,
    title     text,
    level     int
);

-- =====================================================================
-- GROUP A — Schema evolution / SerializationHeader (#1003)
-- =====================================================================

-- 1. se_no_schema_change — CONTROL. Two flushes, NO ALTER. Both generations'
--    SerializationHeaders declare the identical column set {v}.
CREATE TABLE IF NOT EXISTS se_no_schema_change (
    pk int,
    ck int,
    v  text,
    PRIMARY KEY (pk, ck)
);

-- 2. se_altered_column_type — ALTER-TYPE SUBSTITUTION (see header). Cassandra 5.0.2
--    forbids ALTER ... TYPE, so we instead ADD `added_col bigint` in gen-2.
--    On-disk header difference:
--      gen-1 header declares regular columns {orig_col}  (no added_col)
--      gen-2 header declares regular columns {orig_col, added_col}
--    => the gen-1 SerializationHeader type-map differs from the gen-2 schema,
--       which is the readable divergence #1003 targets.
CREATE TABLE IF NOT EXISTS se_altered_column_type (
    pk int,
    ck int,
    orig_col text,
    PRIMARY KEY (pk, ck)
);

-- 3. se_dropped_column_same_type — gen-1 has `dropme text`; it is DROPped between
--    flushes; gen-2 writes only the remaining columns.
--    On-disk header difference:
--      gen-1 header still DECLARES dropme (type text) — Cassandra keeps dropped
--        columns in the header of pre-drop SSTables.
--      gen-2 header does NOT declare dropme.
CREATE TABLE IF NOT EXISTS se_dropped_column_same_type (
    pk int,
    ck int,
    keepme text,
    dropme text,
    PRIMARY KEY (pk, ck)
);

-- 4. se_altered_then_dropped_column — 3 flushes.
--    gen-1: base columns only.
--    (ALTER ADD evolve_col text) gen-2: writes evolve_col.
--    (ALTER DROP evolve_col)     gen-3: writes base columns only.
--    On-disk header difference across the THREE generations:
--      gen-1 header: {base_col}            (no evolve_col yet)
--      gen-2 header: {base_col, evolve_col}
--      gen-3 header: {base_col} + dropped-column record for evolve_col
CREATE TABLE IF NOT EXISTS se_altered_then_dropped_column (
    pk int,
    ck int,
    base_col text,
    PRIMARY KEY (pk, ck)
);

-- 5. se_static_regular_kind_mismatch — a STATIC column lives beside a regular
--    column. The SerializationHeader records `stat_col` with the STATIC column
--    kind (distinct from regular `row_col`). Single flush.
CREATE TABLE IF NOT EXISTS se_static_regular_kind_mismatch (
    pk int,
    ck int,
    stat_col text STATIC,
    row_col  text,
    PRIMARY KEY (pk, ck)
);

-- 6. se_frozen_multicell_collection_mismatch — two list<text> columns: one
--    multicell (`ml`) and one frozen (`fl`). The SerializationHeader records the
--    frozen flag difference: `fl` serialized as a single frozen cell, `ml` as a
--    multicell collection with a complex-deletion + cell-path layout. Single flush.
CREATE TABLE IF NOT EXISTS se_frozen_multicell_collection_mismatch (
    pk int,
    ck int,
    ml list<text>,
    fl frozen<list<text>>,
    PRIMARY KEY (pk, ck)
);

-- =====================================================================
-- GROUP B — Null vs empty & length boundaries (#1006)
-- =====================================================================

-- 7. nb_null_empty_text_blob — neighbors BEFORE and AFTER the boundary columns so
--    offset tracing can prove each boundary value consumes the right byte range.
--    Rows distinguish: absent column, explicit NULL (write-then-DELETE cell),
--    empty string '', empty blob 0x, and non-empty values.
CREATE TABLE IF NOT EXISTS nb_null_empty_text_blob (
    pk int,
    ck int,
    before_col  text,
    target_text text,
    target_blob blob,
    after_col   text,
    PRIMARY KEY (pk, ck)
);

-- 8. nb_absent_vs_null_regular — at least 3 distinct rows: a regular column
--    never written (absent), explicitly deleted (tombstone/null), and
--    written-empty.
CREATE TABLE IF NOT EXISTS nb_absent_vs_null_regular (
    pk int,
    ck int,
    anchor text,
    reg    text,
    PRIMARY KEY (pk, ck)
);

-- 9. nb_empty_collections — empty + non-empty multicell collections (note: an
--    empty MULTICELL collection is stored as ABSENT by Cassandra) alongside
--    FROZEN empty collections (which DO persist as an empty-but-present value).
CREATE TABLE IF NOT EXISTS nb_empty_collections (
    pk int,
    ck int,
    ml list<int>,
    ms set<text>,
    mm map<text,int>,
    fl frozen<list<int>>,
    fs frozen<set<text>>,
    fm frozen<map<text,int>>,
    PRIMARY KEY (pk, ck)
);

-- 10. nb_length_prefix_edges — text/blob values of length 0,1,127,128,255,256,
--     16383,16384 with non-empty neighbors before/after so each value's
--     length-prefix + payload byte range is independently verifiable.
CREATE TABLE IF NOT EXISTS nb_length_prefix_edges (
    pk int,
    ck int,
    before_col text,
    edge_text  text,
    edge_blob  blob,
    after_col  text,
    PRIMARY KEY (pk, ck)
);

-- =====================================================================
-- GROUP C — UDT / tuple / frozen / nested (#1007)
-- =====================================================================

-- 11. cx_tuple_field_order — tuple<int,text,boolean> including a null middle field.
--     (tuples are always frozen in Cassandra.)
CREATE TABLE IF NOT EXISTS cx_tuple_field_order (
    pk int,
    ck int,
    t  tuple<int, text, boolean>,
    PRIMARY KEY (pk, ck)
);

-- 12. cx_udt_field_order_null_empty — frozen person_type rows exercising a null
--     field, an empty-string field, and a fully-populated value.
CREATE TABLE IF NOT EXISTS cx_udt_field_order_null_empty (
    pk int,
    ck int,
    p  frozen<person_type>,
    PRIMARY KEY (pk, ck)
);

-- 13. cx_frozen_udt_value — frozen<employee_type> (a nested-UDT value: contains a
--     frozen<address_type>).
CREATE TABLE IF NOT EXISTS cx_frozen_udt_value (
    pk int,
    ck int,
    e  frozen<employee_type>,
    PRIMARY KEY (pk, ck)
);

-- 14. cx_nested_frozen_collections — collection-of-collection nesting.
CREATE TABLE IF NOT EXISTS cx_nested_frozen_collections (
    pk int,
    ck int,
    m_list_vals map<text, frozen<list<int>>>,
    l_set_vals  list<frozen<set<text>>>,
    s_map_vals  set<frozen<map<text,int>>>,
    PRIMARY KEY (pk, ck)
);

-- 15. cx_multicell_udt_collection_paths — a NON-frozen (multicell) UDT column and
--     a multicell collection, so cell-path encoding is exercised on both.
CREATE TABLE IF NOT EXISTS cx_multicell_udt_collection_paths (
    pk int,
    ck int,
    mp person_type,        -- NON-frozen UDT => multicell, one cell per field w/ cell-path
    ml list<text>,         -- multicell list => one cell per element w/ cell-path
    PRIMARY KEY (pk, ck)
);

-- 16. cx_legacy_dropped_tuple_udt — gen-1 writes a tuple column AND a frozen UDT
--     column; both are DROPped between flushes; gen-2 writes only the survivor.
--     gen-1 header carries the dropped complex (tuple + UDT) columns.
CREATE TABLE IF NOT EXISTS cx_legacy_dropped_tuple_udt (
    pk int,
    ck int,
    survivor text,
    drop_tuple tuple<int, text>,
    drop_udt frozen<person_type>,
    PRIMARY KEY (pk, ck)
);

-- =====================================================================
-- GROUP D — Counters (#1008) [nightly_docker]
-- =====================================================================
-- Counters CANNOT be mixed with non-counter columns, so each counter table is a
-- dedicated pk + counter-column-only table. Counter UPDATEs CANNOT carry
-- USING TIMESTAMP (Cassandra rejects it); the counter shard clock is
-- coordinator-derived. EXPECTED final values per pk are documented in
-- generate-cql-type-parity.sh and emitted to *.counter-select.txt sidecars.

-- 17. ct_single_sstable — one counter, several increments, single flush.
CREATE TABLE IF NOT EXISTS ct_single_sstable (
    pk int PRIMARY KEY,
    c  counter
);

-- 18. ct_multi_sstable_merge — increments in gen-1, then increments AND decrements
--     in gen-2 (2 generations to merge).
CREATE TABLE IF NOT EXISTS ct_multi_sstable_merge (
    pk int PRIMARY KEY,
    c  counter
);

-- 19. ct_deleted_counter_shadowing — increment, flush, DELETE the counter cell/row,
--     flush (the delete shadows the older increments).
CREATE TABLE IF NOT EXISTS ct_deleted_counter_shadowing (
    pk int PRIMARY KEY,
    c  counter
);

-- 20. ct_compacted_final_value — like ct_multi_sstable_merge but ALSO compacted via
--     `nodetool compact` after the second flush, so a compacted SSTable exists.
CREATE TABLE IF NOT EXISTS ct_compacted_final_value (
    pk int PRIMARY KEY,
    c  counter
);
