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
-- identifier must start with a letter, so `"_type"` / `"_keyspace"` /
-- `"__proto__"` are only expressible as quoted identifiers (CQLite's
-- `parse_create_type` accepts them — `identifier` in
-- cqlite-core/src/schema/cql_parser.rs handles the quoted form).
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
--   * collide      — declares the two colliding field names, the JavaScript
--                    prototype-accessor name `"__proto__"`, and a normal field.
--   * collide_twin — the SAME four field names as `collide` under a DIFFERENT
--                    type name, so two values with IDENTICAL fields must stay
--                    DISTINCT once type identity moves out of the field
--                    namespace (the hashable-projection property, site 4).
--   * plain        — no colliding field name: the contrast case, where reading
--                    `_type` out of the field namespace must FAIL.
--   * unhashable_fields
--                  — a UDT with a `map` field. Since #3631 that field DECODES to
--                    a Python `dict`, so the UDT itself is not hashable: the
--                    subject for the projection-totality boundary (second table
--                    below), which #3631 moved again — see the `stn` note there.
--
-- Table `udt_collide` columns:
--   * c  frozen<collide>            — site 3 (the rendered UDT value).
--   * p  frozen<plain>              — the non-colliding contrast.
--   * cm map<frozen<collide>, int>  — a UDT used as the KEY of a NON-FROZEN
--                                     (multicell) map, so the key lives in the
--                                     CELL PATH. It USED to decode as
--                                     `Value::Blob`: `parse_cell_path_key`
--                                     matched a closed set of PRIMITIVE
--                                     cell-path types and fell back to Blob for
--                                     a frozen UDT. FIXED by #3612 — the site
--                                     (now row_decoder/complex_column/cell_path_key.rs)
--                                     delegates to the structural decoder, so
--                                     this column decodes to `Value::Udt` and
--                                     DOES reach the Python hashable
--                                     projection's Udt arm, identically to the
--                                     frozen `fcm` below.
--   * tm map<frozen<collide_twin>, int>
--                                   — the same, one type over.
--   * fcm frozen<map<frozen<collide>, int>>
--                                   — SITE 4's ACTUAL SUBJECT. A frozen map is a
--                                     SINGLE value cell decoded by
--                                     `parse_map_with_types`, which resolves the
--                                     key type through the UdtRegistry, so the key
--                                     really is a `Value::Udt` and really does
--                                     reach the projection.
--   * ftm frozen<map<frozen<collide_twin>, int>>
--                                   — same field NAMES and VALUES as `fcm`'s key
--                                     under a DIFFERENT type name: two projected
--                                     keys that must stay DISTINCT once type
--                                     identity is out of the field namespace.
--   * fs  frozen<set<frozen<collide>>>
--                                   — the set path into the same projection
--                                     (`set_to_py`), which shares
--                                     `value_to_hashable_key` with map keys.
--
-- Deliberately tiny and uncompressed (no CompressionInfo.db): the fixture is a
-- committed binary, and the property under test is above decode. Every INSERT
-- carries an explicit USING TIMESTAMP 1000, so the committed sstabledump JSONL
-- golden is reproducible across regenerations instead of carrying a wall clock
-- (the non-frozen map columns' collection tombstone still records a
-- nowInSeconds local_delete_time, which no CQL clause pins).

CREATE KEYSPACE IF NOT EXISTS test_udt_collision WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_udt_collision;

-- ── User-Defined Types ──────────────────────────────────────────────────────

-- collide: the subject. `"_type"`/`"_keyspace"` collide with the metadata keys
-- the bindings used to inject into the same namespace as the fields.
--
-- `"__proto__"` is the SAME CLASS one layer down, in JavaScript's own object
-- model rather than in ours: an ordinary property assignment on a plain object
-- invokes `Object.prototype`'s inherited `__proto__` ACCESSOR instead of
-- creating a field, so a string value is silently discarded and a null value
-- REPLACES the object's prototype. A field named `__proto__` is legal CQL via a
-- quoted identifier exactly as `"_type"` is, so a fix that gave the fields their
-- own namespace while still building it by assignment would have closed the
-- reported instance and left the class open. `fields` is therefore built with a
-- NULL PROTOTYPE, and this field is what proves it (roborev R1-1 on #3504).
CREATE TYPE IF NOT EXISTS collide (
    "_type"     text,
    "_keyspace" text,
    "__proto__" text,
    real_field  int
);

-- collide_twin: identical FIELD SHAPE, different TYPE NAME. Two values with the
-- same field values must not compare or hash equal. The field list MUST stay
-- identical to `collide`'s — including `"__proto__"` — because the
-- same-fields/different-type distinctness test asserts equal field mappings as
-- its own precondition, so dropping a field here would silently weaken what
-- that test proves rather than fail it.
CREATE TYPE IF NOT EXISTS collide_twin (
    "_type"     text,
    "_keyspace" text,
    "__proto__" text,
    real_field  int
);

-- plain: no colliding field name — the contrast case.
CREATE TYPE IF NOT EXISTS plain (
    label      text,
    real_field int
);

-- unhashable_fields: a UDT declaring a `map`-typed field. It exists for the
-- SECOND table below, as the subject that turned a prediction into a
-- MEASUREMENT — TWICE, in opposite directions, which is why the note is this
-- long.
--
-- Under #3504 it projected successfully, and the recorded reason was that a
-- collection field inside a frozen UDT decoded to `Value::Blob`, so the field
-- arrived as hashable `bytes` (roborev R1-2). #3631 CLOSED that decode gap: `m`
-- now decodes to `{'a': 1}` from its declared `frozen<map<text, int>>`.
--
-- #3631 predicted that would make the shape unprojectable again. MEASURED: it
-- does NOT. `stn` still projects, because #3500's container rule renders a
-- UDT-bearing set as a Python `list`, and a `list` hashes nothing — so the
-- original "projects only because of the blob" attribution was itself wrong by
-- the time #3631 tested it. What DID change is that `Udt.__hash__`'s `TypeError`
-- is now reachable from decoded data at all. Full table:
-- test-data/fixtures/issue_3504/README.md, "Table 2".
CREATE TYPE IF NOT EXISTS unhashable_fields (
    label text,
    m     frozen<map<text, int>>
);

-- ── Table ───────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS udt_collide (
    id  INT PRIMARY KEY,
    c   frozen<collide>,
    p   frozen<plain>,
    cm  map<frozen<collide>, int>,
    tm  map<frozen<collide_twin>, int>,
    fcm frozen<map<frozen<collide>, int>>,
    ftm frozen<map<frozen<collide_twin>, int>>,
    fs  frozen<set<frozen<collide>>>
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- ── Second table: hashable-projection totality (roborev R1-2) ───────────────
--
-- HISTORICAL BOUNDARY — SUPERSEDED BY #3500, kept because the columns still exist.
--
-- As WRITTEN, this section described the pre-#3500 state: `value_to_hashable_key`
-- had arms for `List`, `Map`, `Frozen` and `Udt` while `Tuple` and `Set` had NONE
-- and fell through to `value_to_py`, so a UDT reached through that fallback became
-- an unhashable `dict` and the read raised `TypeError` (issue #3500). #3504 made a
-- UDT a HASHABLE `cqlite.Udt`, which incidentally resolved the sub-family whose
-- field values are themselves hashable and left the rest failing.
--
-- THAT IS NO LONGER THE IMPLEMENTATION. #3500 made both `value_to_hashable_key`
-- and `contains_udt` TOTAL over `cqlite_core::Value`: every variant is named, there
-- is no wildcard arm, and the property is pinned by three
-- `#[deny(clippy::wildcard_enum_match_arm)]` attributes in
-- `bindings/python/src/value_hashable.rs`. Verified in that file: `Tuple` and `Set`
-- now HAVE arms, and `contains_udt` traverses tuples
-- (`Value::List(items) | Value::Set(items) | Value::Tuple(items)`).
--
-- So the two mechanisms the per-column notes below reason FROM are gone. The
-- outcomes are NOT re-stated here, because the `udt_hashable_shapes` binaries are
-- not in this corpus and could not be measured while making this correction —
-- asserting a new outcome from the code alone is what produced the stale text in
-- the first place. Treat each note below as a RECORD OF THE PRE-#3500 MEASUREMENT,
-- to be re-measured against the current implementation when the fixture is
-- available. The live, measured coverage for these shapes is the #3500 suite
-- (`bindings/python/tests/test_nested_udt_hashable.py`) against
-- `test_nested_udt_keys.nested_udt_keys`, whose `s_set_udt` column is this
-- section's `ssu` shape.
--
-- PRE-#3500 MEASUREMENT, one column per side of the then-boundary:
--
--   * stu frozen<set<frozen<tuple<frozen<collide>, int>>>>
--                    — NOW SUCCEEDS. The set has no UDT element at top level
--                      (`contains_udt` does not look inside a tuple), so it takes
--                      the `frozenset` branch, and hashing the tuple hashes the
--                      `Udt`, whose fields are all text/int.
--   * mtu frozen<map<frozen<tuple<frozen<collide>, int>>, int>>
--                    — NOW SUCCEEDS, same projection reached as a MAP KEY.
--   * ssu frozen<set<frozen<set<frozen<collide>>>>>
--                    — STILL FAILS, and NOT for the old reason: the INNER set
--                      has a UDT element, so `set_to_py` returns a Python `list`
--                      for CLI parity (#804), and a list is unhashable in the
--                      outer set. Unchanged by #3504.
--   * stn frozen<set<frozen<tuple<frozen<unhashable_fields>, int>>>>
--                    — MEASURED (pre-#3500): also SUCCEEDS, contradicting the
--                      obvious prediction. The reason RECORDED at the time was
--                      that CQLite decoded a collection field inside a FROZEN UDT
--                      as `Value::Blob`, so the field arrived as hashable
--                      `bytes`.
--
--                      SUPERSEDED BY #3631, which closed that decode gap — `m`
--                      is now `{'a': 1}` — and by the re-measurement that came
--                      with it: the column STILL succeeds, so the recorded
--                      attribution above was already wrong. #3500's container
--                      rule (a UDT-bearing set renders as a Python `list`) is
--                      what makes it succeed, and a `list` hashes nothing. The
--                      Python suite now pins the decoded `dict` AND the fact
--                      that `hash()` of the element or of the UDT raises, which
--                      before #3631 no decoder path could reach.
--
-- Each shape lives in its OWN ROW (`stu`+`mtu` in row 1, `ssu` in row 2, `stn` in
-- row 3): the `TypeError` is raised while CONVERTING a row, so a row carrying
-- both a projectable and an unprojectable cell would make the projectable one
-- unobservable — which is also why the Python suite reads this table by primary
-- key rather than scanning it.
CREATE TABLE IF NOT EXISTS udt_hashable_shapes (
    id  INT PRIMARY KEY,
    stu frozen<set<frozen<tuple<frozen<collide>, int>>>>,
    mtu frozen<map<frozen<tuple<frozen<collide>, int>>, int>>,
    ssu frozen<set<frozen<set<frozen<collide>>>>>,
    stn frozen<set<frozen<tuple<frozen<unhashable_fields>, int>>>>
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
