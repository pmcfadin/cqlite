-- issue-3747-empty-map-key.cql — Cassandra 5.0 reference schema for the
-- EMPTY MULTICELL MAP KEY fixture (issue #3747).
--
-- WHY THIS EXISTS
-- CQLite's multicell map decoder guards its key decode on a NON-EMPTY cell path
-- (cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column.rs,
-- the `if !cell.path_bytes.is_empty()` at the map branch). A map entry whose key
-- is the EMPTY value therefore yields `decoded_key == None`, and the entry is not
-- merely left undecoded — it never reaches the reconstructed `Value::Map` at all.
-- A `SELECT` returns a map SHORT ONE ENTRY, with no error and no warning.
--
-- NO fixture in the corpus had an empty map key before this one, which is
-- precisely why the defect survived: the guard is only reachable with data no
-- test subject carried. Confirmed by census — zero goldens under test-data/**
-- contain an empty `"key"`, and no committed .cql writes one.
--
-- WHY AN EMPTY KEY IS LEGAL DATA AND NOT CORRUPTION
-- Read at the pinned tag (`git show cassandra-5.0.8:...`), which is the format
-- authority here — never CQLite's own source (CLAUDE.md, "Format authority"):
--   * `CollectionSerializer.readNonNullValue` rejects only a NULL (-1) length
--     with "Null value read when not allowed". A ZERO length is accepted.
--   * `UTF8Type` / `AsciiType` / `BytesType` are variable-width and validate any
--     byte sequence, INCLUDING none.
-- So Cassandra writes, stores and returns such an entry, and `sstabledump` shows
-- it. Empty is a distinct value from null. That is the whole claim this fixture
-- exists to make EMPIRICALLY, because the issue could only make it from source.
--
-- WHY IT IS CASSANDRA-WRITTEN AND NOT A CQLITE ROUND-TRIP
-- A CQLite-written + CQLite-read subject is INVARIANT to a uniform error on both
-- sides (CLAUDE.md, #3042): CQLite's own writer would have to choose how to
-- encode an empty key, and if it chose the same wrong thing the reader does, the
-- round-trip would close and stay green while real Cassandra data still read
-- wrong. Only Cassandra-written bytes can settle what the on-disk encoding IS.
--
-- ============================================================================
-- WHAT EACH COLUMN IS FOR — the four are NOT interchangeable
--
--   m_text   map<text,int>   — AC2. The headline subject. Key `''` must decode
--                              to `Value::Text("")`.
--   m_blob   map<blob,int>   — AC3. A SECOND variable-width family, reached
--                              through a DIFFERENT arm of `parse_cell_path_key`
--                              (the blob fallback, not the utf8 arm). Key `0x`
--                              must decode to `Value::Blob(b"")`. Without this
--                              column the fixture would only prove the utf8 arm.
--   m_ascii  map<ascii,int>  — the third variable-width family member, and the
--                              one that pins the fix is a property of the GUARD
--                              rather than of one type name: `ascii` shares the
--                              utf8 arm but is a distinct declared type.
--   m_frozen frozen<map<text,int>>
--                            — THE CONTROL, and it is load-bearing. A frozen map
--                              is a COMPLETELY DIFFERENT ENCODING: one inline
--                              length-prefixed blob, decoded by
--                              `frozen.rs::parse_frozen_map_value_raw`, which
--                              reads an i32 key length, rejects only NEGATIVE,
--                              and pushes every entry unconditionally. It is
--                              MEASURED to already handle an empty key today.
--                              It is here so the fixture DISCRIMINATES the two
--                              encodings: a change that "fixes" empty keys by
--                              breaking the frozen path reds on this column.
--                              It must never be described as a second instance
--                              of the defect — it is the regression control.
--
-- NOT COVERED, AND DELIBERATELY SO: `inet`. `InetAddressSerializer.validate`
-- early-returns on an empty buffer, so an empty inet key is representable ON
-- DISK — but CQL has NO literal that expresses one, so a CASSANDRA-WRITTEN
-- fixture cannot carry it. Claiming inet coverage from a fixture that cannot
-- contain it would be a false claim; the empty-inet path stays unproven here and
-- is called out rather than quietly implied.
-- ============================================================================
--
-- ROWS (see the generator for the inserted values):
--   id 1 — THE SUBJECT. Every map column carries an empty key ALONGSIDE a
--          non-empty one. The non-empty sibling is what makes the failure
--          legible: pre-fix the map is short exactly one entry rather than
--          absent, so "entry dropped" is distinguishable from "column missing".
--   id 2 — THE CONTRAST. No empty key anywhere. Pins that the fix changes
--          nothing for ordinary maps; this row must decode identically before
--          and after.
--
-- Every INSERT pins `USING TIMESTAMP 1000`, which stabilises `liveness_info` and
-- the collection tombstones' `marked_deleted`. It does NOT make the golden
-- byte-reproducible: an INSERT of a whole NON-FROZEN collection REPLACES it, so
-- Cassandra emits a collection tombstone per multicell column whose
-- `local_delete_time` is a wall clock no CQL clause can pin — MEASURED on the
-- first generation, and stated here because an earlier draft claimed the
-- opposite. Never byte-compare this golden across regenerations, and never
-- assert on `local_delete_time`.
--
-- Generator: test-data/scripts/generate-issue-3747-empty-map-key.sh
-- Backs: issue #3747.

CREATE KEYSPACE IF NOT EXISTS test_empty_map_key
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS test_empty_map_key.empty_key_map (
    id       int PRIMARY KEY,
    m_text   map<text, int>,
    m_blob   map<blob, int>,
    m_ascii  map<ascii, int>,
    m_frozen frozen<map<text, int>>
);
