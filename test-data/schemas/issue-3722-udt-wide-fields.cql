-- issue-3722-udt-wide-fields.cql — Cassandra 5.0 reference schema for the UDT
-- FIELD-TYPE COVERAGE fixture (issue #3722).
--
-- WHY THIS EXISTS
-- UDT *field* values of many CQL types decode as an opaque `Value::Blob`. Two
-- SEPARATE shared UDT-field decoders live in
-- cqlite-core/src/storage/sstable/reader/parsing/row_decoder/udt.rs —
-- `parse_udt_field_value` and `parse_simple_udt_field_value` — with DIVERGENT arm
-- sets, and BOTH end in `_ => Value::Blob`. So a field whose type either decoder
-- does not name silently becomes bytes.
--
-- The defect is currently UNREACHABLE FROM THE CORPUS. A census of every
-- `CREATE TYPE` in test-data/schemas/** (7 files: `key_part`, `collide`,
-- `collide_twin`, `plain`, `unhashable_fields`, `person`/`address`/`employee`,
-- `person_type`/`address_type`/`employee_type`) found NOT ONE declaring
-- smallint / tinyint / decimal / varint / time / timeuuid / duration, and only
-- `unhashable_fields` declares a collection field. Exactly the position #3500 and
-- #3504 were in before their fixtures existed. This schema is the missing subject.
--
-- WRITTEN BY CASSANDRA, NOT BY CQLITE. Produced by
-- test-data/scripts/generate-issue-3722-udt-wide-fields.sh (cqlsh INSERT +
-- `nodetool flush` inside a cassandra:5.0.2 container). Mandated by CLAUDE.md:
-- for an ON-DISK decode property the oracle must be Cassandra-written bytes. A
-- CQLite-write/CQLite-read round trip is INVARIANT to a uniform decode error —
-- both sides would make the identical mistake and the test would stay green —
-- and CQLite's write path cannot emit a UDT literal anyway.
--
-- OUTPUT LOCATION IS LOAD-BEARING. The SSTable components are committed
-- CHECKOUT-RELATIVE under test-data/fixtures/issue_3722/ and NOT under
-- test-data/datasets/sstables/: consumers resolve the corpus from
-- CQLITE_DATASETS_ROOT, which every gate run sets, so a corpus-rooted fixture is
-- INVISIBLE on every gate run. A checkout-relative path cannot be hidden by an
-- env var. Precedent: test-data/fixtures/issue_3504/.
--
-- ── MEASURED REFUSAL — DO NOT RETRY ─────────────────────────────────────────
-- Issue #3722's AC1 names `counter` among the affected field types. `counter` is
-- REFUSED as a UDT field by Cassandra 5.0.2, verbatim:
--
--   InvalidRequest: code=2200 [Invalid query] message="A user type cannot contain counters"
--
-- So that AC is unsatisfiable by construction and no fixture can carry it; the
-- `counter` arm is pinned at the `CqlType` level elsewhere instead. Do not spend
-- another probe container discovering this again.
--
-- ── FIELD-BY-FIELD RATIONALE (kept HERE, in the header) ─────────────────────
-- NOTE ON COMMENT PLACEMENT, which is measured and load-bearing (#3500): NO `--`
-- comment may appear BETWEEN `CREATE TABLE (` and its closing `)`. CQLite's nom
-- schema parser strips comments only BETWEEN statements; a comment INSIDE one
-- makes the whole `CREATE TABLE` unparseable, the loader reports `loaded 0
-- schemas`, and the reader silently falls back to Statistics.db marshal types —
-- a green-looking schema file that takes the whole read path down. That is why
-- per-column rationale lives in this header and not beside the columns.
--
-- Type `wide` — 16 fields, one per affected decode route, plus two CONTROLS:
--   s  smallint            — the two-byte signed route. Value -300 = 0xfed4: a
--                            wrong-WIDTH read cannot produce -300, so a wrong
--                            decode is VISIBLE, not merely absent.
--   t  tinyint             — one-byte signed. Value -1 = 0xff.
--   d  decimal             — scale + unscaled BigInt (123.45 = scale 2 /
--                            unscaled 12345).
--   vi varint              — arbitrary-precision integer, chosen to EXCEED i64
--                            (90071992547409910000) so an i64 shortcut overflows
--                            rather than silently working.
--   tm time                — nanoseconds since midnight, 8 bytes.
--   tu timeuuid            — 16 bytes, version-1 layout, distinct from `uuid`.
--   du duration            — three vint components; the value 2mo3d4h5m has ALL
--                            THREE non-zero, so dropping any component shows.
--   dt date               — days since epoch, unsigned-biased 4 bytes.
--   ip inet                — 4- or 16-byte address.
--   fl frozen<list<int>>   — collection field: list. Values [1,-2,3] carry a
--                            negative element so element sign survives.
--   fs frozen<set<text>>   — collection field: set.
--   fm frozen<map<text,int>>
--                          — collection field: map, with a negative value.
--   tp frozen<tuple<int,text>>
--                          — tuple field.
--   nu frozen<inner_u>     — NESTED UDT field: a UDT field whose own type is a
--                            UDT, which is the recursion route.
--   bl blob                — *** CONTROL 1 ***: the ONE field that MUST STILL
--                            decode to `Value::Blob`. A fix that blanket-stops
--                            emitting Blob (e.g. by making the fallback an error
--                            or a string) is caught HERE.
--   i  int                 — *** CONTROL 2 ***: already decodes correctly today.
--                            A regression in the working path is caught here.
--
-- Type `inner_u (a int, b text)` — the nested-UDT field's type. Deliberately
-- two already-working scalar types: it is the NESTING that is under test, not
-- the inner field types.
--
-- Table `udt_wide_fields` columns — the five container routes named by #3722's
-- AC3. Two are near-siblings (`fmw` vs `mw`, `fsw` vs `sw`) and BOTH halves stay,
-- because the frozen and multicell paths are DIFFERENT CODE: a frozen collection
-- is one value cell parsed by the collection parser, while a multicell
-- collection delivers the UDT as a CELL-PATH, parsed elsewhere.
--   id  int PRIMARY KEY
--   w   frozen<wide>                    — top-level frozen UDT: the direct route
--                                         into the field decoders.
--   mw  map<frozen<wide>, int>          — MULTICELL map: the UDT arrives as a
--                                         CELL-PATH KEY.
--   fmw frozen<map<frozen<wide>, int>>  — FROZEN map key: a single value cell,
--                                         a different parser from `mw`.
--   fsw frozen<set<frozen<wide>>>       — frozen set ELEMENT.
--   sw  set<frozen<wide>>               — MULTICELL set: the UDT arrives as a
--                                         cell-path element.
--
-- Rows (see the generator's insert_rows() for the literals):
--   id 1 — every column populated with the full 16-field value.
--   id 2 — NULL UDT FIELDS: `w` with several fields null (at least one scalar and
--          one collection) and the rest populated, proving the absent-field
--          encoding is orthogonal to field-type decoding.
--   id 3 — a SECOND, DISTINCT `wide` value in mw/fmw/fsw/sw with different scalar
--          values, so multi-element ordering and uniqueness are exercised by
--          Cassandra's OWN writer. Inserted OUT of sorted order.
--   Separate rows where a decode failure in one column could mask another.
--
-- Deliberately tiny and UNCOMPRESSED (no CompressionInfo.db) with
-- SizeTieredCompactionStrategy: the fixture is a committed binary and the
-- property under test is above decode. Every INSERT carries an explicit
-- USING TIMESTAMP 1000, so the committed sstabledump JSONL golden is reproducible
-- across regenerations instead of carrying a wall clock. NOT fully deterministic:
-- a MULTICELL collection INSERT (`mw`, `sw`) also emits a collection tombstone
-- whose `local_delete_time` comes from nowInSeconds, which NO CQL clause pins.

CREATE KEYSPACE IF NOT EXISTS test_udt_wide_fields WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_udt_wide_fields;

-- ── User-Defined Types ──────────────────────────────────────────────────────

-- inner_u: the nested-UDT field's type. See the header.
CREATE TYPE IF NOT EXISTS inner_u (
    a int,
    b text
);

-- wide: one field per affected decode route, plus the two controls `bl` (must
-- STAY Blob) and `i` (must stay correct). Field ORDER is the declaration order
-- and is what the SerializationHeader's UserType(...) marshal string records;
-- do not reorder without regenerating the fixture. Accepted VERBATIM by
-- Cassandra 5.0.2 (`DESCRIBE TYPE` echoes it unchanged).
CREATE TYPE IF NOT EXISTS wide (
    s  smallint,
    t  tinyint,
    d  decimal,
    vi varint,
    tm time,
    tu timeuuid,
    du duration,
    dt date,
    ip inet,
    fl frozen<list<int>>,
    fs frozen<set<text>>,
    fm frozen<map<text, int>>,
    tp frozen<tuple<int, text>>,
    nu frozen<inner_u>,
    bl blob,
    i  int
);

-- ── Table ───────────────────────────────────────────────────────────────────
-- NO `--` comment may appear inside the parentheses below. See the header.

CREATE TABLE IF NOT EXISTS udt_wide_fields (
    id  INT PRIMARY KEY,
    w   frozen<wide>,
    mw  map<frozen<wide>, int>,
    fmw frozen<map<frozen<wide>, int>>,
    fsw frozen<set<frozen<wide>>>,
    sw  set<frozen<wide>>
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
