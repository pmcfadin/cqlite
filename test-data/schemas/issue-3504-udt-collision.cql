-- issue-3504-udt-collision.cql — Cassandra 5.0 reference schema for the UDT
-- FIELD-NAME / TYPE-IDENTITY COLLISION fixture (issue #3504).
--
-- WHY THIS EXISTS
-- The Python and Node bindings used to render a UDT as ONE FLAT namespace
-- holding both the injected type identity (`_type` / `_keyspace`) and the UDT's
-- own declared fields. A UDT that DECLARES a field literally named `_type` or
-- `_keyspace` therefore silently overwrote the injected marker, and the type
-- identity was unrecoverable. No pre-existing corpus fixture declares such a
-- field, so the defect had no test subject at all.
--
-- This schema declares one. The field names REQUIRE quoting: an unquoted CQL
-- identifier must start with a letter, so `"_type"` / `"_keyspace"` are only
-- expressible as quoted identifiers (CQLite's `parse_create_type` accepts
-- them — `identifier` in cqlite-core/src/schema/cql_parser.rs handles the
-- quoted form).
--
-- WRITTEN BY CASSANDRA, NOT BY CQLITE. The fixture is produced by
-- test-data/scripts/generate-issue-3504-udt-collision.sh (cqlsh INSERT +
-- `nodetool flush` inside a cassandra:5.0.2 container). A CQLite-written
-- fixture would not do: it additionally proves the DECODER can produce such a
-- UDT at all, and CQLite's write path cannot emit a UDT literal anyway.
--
-- The SSTable components are committed CHECKOUT-RELATIVE under
-- test-data/fixtures/issue_3504/ — NOT under test-data/datasets/sstables/ —
-- because both binding test suites resolve the dataset corpus from
-- CQLITE_DATASETS_ROOT and never fall back to the checkout, so a corpus-rooted
-- fixture is invisible on every gate run. A checkout-relative path cannot be
-- hidden by an env var.
--
-- Types:
--   * collide      — declares BOTH colliding field names plus a normal field.
--   * collide_twin — the SAME three field names as `collide` under a DIFFERENT
--                    type name, so two values with IDENTICAL fields must stay
--                    DISTINCT once type identity moves out of the field
--                    namespace (the hashable-projection property, site 4).
--   * plain        — no colliding field name: the contrast case, where reading
--                    `_type` out of the field namespace must FAIL.
--
-- Table `udt_collide` columns:
--   * c  frozen<collide>            — site 3 (the rendered UDT value).
--   * p  frozen<plain>              — the non-colliding contrast.
--   * cm map<frozen<collide>, int>  — site 4: a UDT used as a MAP KEY, which
--                                     Python must project to a hashable value.
--   * tm map<frozen<collide_twin>, int>
--                                   — same-fields/different-type key, for the
--                                     distinctness property above.
--
-- Deliberately tiny and uncompressed (no CompressionInfo.db): the fixture is a
-- committed binary, and the property under test is above decode.

CREATE KEYSPACE IF NOT EXISTS test_udt_collision WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_udt_collision;

-- ── User-Defined Types ──────────────────────────────────────────────────────

-- collide: the subject. Both field names collide with the metadata keys the
-- bindings used to inject into the same namespace as the fields.
CREATE TYPE IF NOT EXISTS collide (
    "_type"     text,
    "_keyspace" text,
    real_field  int
);

-- collide_twin: identical FIELD SHAPE, different TYPE NAME. Two values with the
-- same field values must not compare or hash equal.
CREATE TYPE IF NOT EXISTS collide_twin (
    "_type"     text,
    "_keyspace" text,
    real_field  int
);

-- plain: no colliding field name — the contrast case.
CREATE TYPE IF NOT EXISTS plain (
    label      text,
    real_field int
);

-- ── Table ───────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS udt_collide (
    id INT PRIMARY KEY,
    c  frozen<collide>,
    p  frozen<plain>,
    cm map<frozen<collide>, int>,
    tm map<frozen<collide_twin>, int>
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
