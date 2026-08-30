-- nested-udt-keys.cql — Cassandra 5.0.2 reference schema for the
-- NESTED-UDT-INSIDE-A-HASHABLE-POSITION read-fidelity slice (issue #3500).
--
-- WHY THIS FIXTURE EXISTS
-- The Python binding projects a CQL `set` to a Python `set` and a CQL `map` to a
-- Python `dict`, so every set ELEMENT and every map KEY has to be reduced to a
-- HASHABLE Python object. That reduction (bindings/python/src/value.rs
-- `value_to_hashable_key` / `contains_udt`) was not TOTAL over the
-- `cqlite_core::Value` tree: a UDT reached through a tuple or through a nested
-- collection fell through to the generic converter and raised
--   TypeError: unhashable type: 'dict'   (UDT reached via a tuple)
--   TypeError: unhashable type: 'list'   (UDT reached via a nested set/list)
-- No committed fixture in this repository declared any of those shapes, so the
-- defect was UNREACHABLE from the corpus. This schema declares them.
--
-- The four shapes (issue #3500's table), one column each:
--   s_tuple_udt — set<frozen<tuple<frozen<key_part>, int>>>
--                 UDT reached through a TUPLE inside a set element.
--   s_set_udt   — set<frozen<set<frozen<key_part>>>>
--                 UDT reached through a NESTED SET inside a set element.
--   m_tuple_udt — map<frozen<tuple<frozen<key_part>, int>>, int>
--                 the same tuple-borne UDT in the MAP-KEY position.
--   s_list_udt  — set<frozen<list<frozen<key_part>>>>
--                 UDT reached through a NESTED LIST inside a set element.
--                 This one is the CONTROL: it did not raise before the fix, so a
--                 regression here would show a fix that changed working behaviour.
--
-- Plus three FROZEN-OUTER columns so the SAME nesting is also covered when the
-- whole collection arrives as a SINGLE value cell rather than as multicell
-- cell-paths (the two decode paths are different code):
--   f_set_tuple_udt  — frozen<set<frozen<tuple<frozen<key_part>, int>>>>
--   f_map_tuple_udt  — frozen<map<frozen<tuple<frozen<key_part>, int>>, int>>
--   f_map_set_udt    — frozen<map<frozen<set<frozen<key_part>>>, int>>
--
-- THE TWO FROZEN MAPS ARE THE ONLY COLUMNS THAT REACH THE HASHABLE PROJECTION'S
-- NEW ARMS, AND THAT IS WHY THEY EXIST (issue #3500).
-- The four issue-named columns above do NOT. Two independent routes divert them:
--   * A `set` whose elements contain a UDT takes `set_to_py`'s LIST fallback
--     (bindings/python/src/value.rs), which converts elements with `value_to_py`
--     and never calls `value_to_hashable_key` at all. Making `contains_udt`
--     total — issue #3500's own AC1 — makes that true of EVERY UDT-bearing set
--     column, including the four above.
--   * A MULTICELL map's keys arrive from `parse_cell_path_key`, a scalar-only
--     allowlist whose catch-all yields `Value::Blob`, so `m_tuple_udt`'s keys
--     are opaque bytes rather than a structured tuple (a cqlite-core gap,
--     filed as #3612).
-- A FROZEN map takes neither route. It arrives as one value cell and
-- `parse_frozen_map_value` -> `read_frozen_element` -> `parse_value_from_raw_bytes`
-- decodes each KEY STRUCTURALLY, so `map_to_py` hands a real `Value::Tuple` /
-- `Value::Set` (with a real `Value::Udt` inside) to `value_to_hashable_key`.
-- f_map_tuple_udt therefore drives its `Tuple` arm and f_map_set_udt its `Set`
-- arm, end to end, from Cassandra-written bytes. Without these two columns both
-- arms could regress with zero test failures.
--
-- The four issue-named columns are deliberately NON-frozen at the top level, so
-- each set element / map key is written as a CELL PATH — which is the shape a
-- real `SELECT` returns and therefore the shape the hashable projection sees.
--
-- COVERAGE NOTES
--   * Multiple distinct elements per collection, inserted OUT of sorted order,
--     so element ordering + uniqueness are exercised by Cassandra's own writer.
--   * A row whose nested UDTs carry a NULL FIELD (`rank:null`, `label:null`).
--     In the FROZEN-MAP columns that field is what reaches
--     `value_to_hashable_key`'s Udt-arm `None => py.None()` branch — the only
--     place in this repository that branch is exercised. (The set columns'
--     null fields travel `udt_to_py`'s own `None` branch instead, a different
--     function, so they do not cover it.)
--   * An EMPTY-string UDT field, distinct from null.
--
-- DETERMINISM / SIMPLICITY CONTRACT
--   * Tables are UNCOMPRESSED (no CompressionInfo.db): Data.db is a direct byte
--     slice, so the sstabledump JSONL golden and any hex inspection line up with
--     the committed bytes. Compression is not the property under test.
--   * int partition key: 4-byte big-endian key bytes, deterministic Murmur3 order.
--   * No USING TIMESTAMP / TTL / DELETE: this is a READ-fidelity fixture, not a
--     compaction byte-parity fixture, so writetimes are not pinned and the
--     oracle is the sstabledump JSONL golden + a real `SELECT`.
--   * ONE `nodetool flush` => exactly ONE SSTable generation per table.
--
-- Generated by: test-data/scripts/generate-nested-udt-keys.sh
-- Backs: issue #3500.

CREATE KEYSPACE IF NOT EXISTS test_nested_udt_keys WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_nested_udt_keys;

-- ── User-Defined Type ───────────────────────────────────────────────────────
-- key_part: deliberately SMALL (two scalar fields). The property under test is
-- the NESTING depth a UDT can be reached at, not UDT width — width is already
-- covered by test-data/schemas/compaction-parity-udt.cql.
CREATE TYPE IF NOT EXISTS key_part (
    label text,
    rank  int
);

-- ── Table ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS nested_udt_keys (
    id              int PRIMARY KEY,
    s_tuple_udt     set<frozen<tuple<frozen<key_part>, int>>>,
    s_set_udt       set<frozen<set<frozen<key_part>>>>,
    m_tuple_udt     map<frozen<tuple<frozen<key_part>, int>>, int>,
    s_list_udt      set<frozen<list<frozen<key_part>>>>,
    f_set_tuple_udt frozen<set<frozen<tuple<frozen<key_part>, int>>>>,
    -- Accepted verbatim by Cassandra 5.0.2 (probed before committing): a
    -- collection/tuple used as a map key must be frozen, and both nestings
    -- below satisfy that.
    f_map_tuple_udt frozen<map<frozen<tuple<frozen<key_part>, int>>, int>>,
    f_map_set_udt   frozen<map<frozen<set<frozen<key_part>>>, int>>
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
