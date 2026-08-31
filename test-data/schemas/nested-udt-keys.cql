-- nested-udt-keys.cql — Cassandra 5.0.2 reference schema for the
-- NESTED-UDT-INSIDE-A-HASHABLE-POSITION read-fidelity slice (issue #3500).
--
-- WHY THIS FIXTURE EXISTS
-- The Python binding projects a CQL `set` to a Python `set` and a CQL `map` to a
-- Python `dict`, so every set ELEMENT and every map KEY has to be reduced to a
-- HASHABLE Python object. That reduction (bindings/python/src/value_hashable.rs
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
-- Plus ONE column whose map KEY is a tuple carrying a NESTED LIST:
--   f_map_tuple_list_udt
--     frozen<map<frozen<tuple<frozen<list<frozen<key_part>>>, int>>, int>>
-- It exists because f_map_tuple_udt's key CANNOT distinguish the hashable
-- projection's `Tuple` arm from a delegation to the ordinary one — since #3504
-- both routes answer with the same object for a scalar-field UDT. Nesting a
-- `list` under the tuple makes the two routes DIVERGE, so the arm's recursion
-- becomes observable. The full argument is stated in exactly ONE place and is
-- NOT restated here: the docstring of
-- bindings/python/tests/test_nested_udt_hashable.py's
-- TestFrozenMapWithTupleBorneNestedListUdtKey (issue #3500, roborev job 275).
--
-- Plus TWO columns that put a UDT inside a frozen MAP that is itself a SET
-- ELEMENT, which is the only shape that exercises `contains_udt`'s MAP arm
-- (roborev job 245, F3):
--   s_map_udt_key — set<frozen<map<frozen<key_part>, int>>>
--                   UDT in the map KEY only  -> covers `contains_udt(k)`.
--   s_map_udt_val — set<frozen<map<int, frozen<key_part>>>>
--                   UDT in the map VALUE only -> covers `contains_udt(v)`.
-- TWO NARROW COLUMNS, NOT ONE WIDE ONE, AND THAT IS THE POINT. The arm reads
-- `pairs.iter().any(|(k, v)| contains_udt(k) || contains_udt(v))`, and `||`
-- SHORT-CIRCUITS: a single column with a UDT in both halves stays green if the
-- key half is correct and the value half regressed (the key half answers first
-- and `v` is never consulted), and its failure would not say WHICH half broke.
-- One column per half makes each half independently falsifiable.
--
-- Neither is a crash case post-fix: `value_to_hashable_key` is TOTAL, so a
-- regressed half projects the element HASHABLY and the column silently comes
-- back as a `frozenset` of tuples where #804 requires a `list` of `dict`s — a
-- SHAPE regression with no exception. That is exactly the "an arm covered by
-- nothing" class this fixture exists to close, so it is closed the same way:
-- with real Cassandra-written bytes.
--
-- THE TWO FROZEN MAPS ARE THE ONLY COLUMNS THAT REACH THE HASHABLE PROJECTION'S
-- NEW ARMS, AND THAT IS WHY THEY EXIST (issue #3500).
-- f_map_tuple_udt drives `value_to_hashable_key`'s `Tuple` arm and f_map_set_udt
-- its `Set` arm, end to end, from Cassandra-written bytes; without these two
-- columns both arms could regress with zero test failures. That is the reason
-- the two columns are in this table.
--
-- The ROUTING that establishes it — which columns reach `value_to_hashable_key`
-- and on which arm, per column, for all nine — is stated in exactly ONE place
-- and is NOT restated here: the ROUTING section of
-- bindings/python/src/value_hashable.rs. This header asserts nothing about it.
-- (The version that lived here said a UDT-bearing `set` column "never calls
-- `value_to_hashable_key` at all", which is false — s_map_udt_key /
-- s_map_udt_val reach it through their INNER frozen map's keys, and
-- m_tuple_udt's keys reach it as a `Value::Blob`. Five of the nine columns
-- reach the function; only these two reach the NEW arms. Scope such a claim to
-- the ARM, never to the function.)
--
-- The four issue-named columns are deliberately NON-frozen at the top level, so
-- each set element / map key is written as a CELL PATH — which is the shape a
-- real `SELECT` returns and therefore the shape the hashable projection sees.
--
-- COVERAGE NOTES
--   * Multiple distinct elements per collection, inserted OUT of sorted order,
--     so element ordering + uniqueness are exercised by Cassandra's own writer.
--   * A row whose nested UDTs carry a NULL FIELD (`rank:null`, `label:null`),
--     in a set column and in the frozen-map columns both. Which of those
--     null fields reaches which `None` branch is part of the ROUTING statement
--     named above, and is not restated here.
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
-- Every column type below was probed against Cassandra 5.0.2 before being
-- committed and was accepted VERBATIM (`DESCRIBE TABLE` echoes each declaration
-- unchanged): a collection or tuple used as a map key must be frozen, and so
-- must a map used as a set element — every nesting below satisfies both.
--
-- One measured cqlsh DISPLAY quirk, recorded so it is not mistaken for a
-- rejection: a `set<frozen<map<frozen<key_part>, …>>>` element whose UDT map key
-- has NULL fields makes the python driver's SortedSet formatter raise
-- `Failed to format value … '<' not supported between instances of 'str' and
-- 'NoneType'` while printing. The INSERT and the SELECT both still exit 0 and
-- the data is stored correctly; it is a client-side formatting failure only.
-- s_map_udt_key therefore carries an EMPTY-STRING label rather than a null one,
-- and null-field coverage for the new pair lives in s_map_udt_val's map VALUES
-- (where the formatter is fine). Null UDT fields in a hashable position are
-- already covered by m_tuple_udt / f_map_tuple_udt / f_map_set_udt.
--
-- NO `--` COMMENT MAY APPEAR BETWEEN `CREATE TABLE (` AND ITS CLOSING `)`.
-- CQLite's nom schema parser (cqlite-core/src/schema/cql_parser.rs) strips
-- comments only BETWEEN statements, not inside one, so a comment in the column
-- list makes the whole CREATE TABLE unparseable. The loader then reports
-- `loaded 0 schemas` and the reader silently falls back to the Statistics.db
-- marshal types: the partition key decodes to None and every frozen-outer
-- column decodes to an opaque Blob — i.e. a green-looking schema file that
-- takes the entire read path down. Measured on this file (issue #3500); keep
-- per-column rationale in this header instead.
CREATE TABLE IF NOT EXISTS nested_udt_keys (
    id              int PRIMARY KEY,
    s_tuple_udt     set<frozen<tuple<frozen<key_part>, int>>>,
    s_set_udt       set<frozen<set<frozen<key_part>>>>,
    m_tuple_udt     map<frozen<tuple<frozen<key_part>, int>>, int>,
    s_list_udt      set<frozen<list<frozen<key_part>>>>,
    f_set_tuple_udt frozen<set<frozen<tuple<frozen<key_part>, int>>>>,
    f_map_tuple_udt frozen<map<frozen<tuple<frozen<key_part>, int>>, int>>,
    f_map_set_udt   frozen<map<frozen<set<frozen<key_part>>>, int>>,
    f_map_tuple_list_udt frozen<map<frozen<tuple<frozen<list<frozen<key_part>>>, int>>, int>>,
    s_map_udt_key   set<frozen<map<frozen<key_part>, int>>>,
    s_map_udt_val   set<frozen<map<int, frozen<key_part>>>>
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
