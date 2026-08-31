-- issue-3630-row-collision.cql — Cassandra 5.0 reference schema for the ROW-LEVEL
-- Object.prototype COLLISION fixture (issue #3630).
--
-- WHY THIS EXISTS
-- The Node binding builds a result row by writing each column name onto a plain
-- JavaScript object with an ordinary property assignment. A property assignment
-- is a JavaScript `[[Set]]`, which CONSULTS THE PROTOTYPE CHAIN — so a column
-- name that matches an inherited accessor reaches that accessor's SETTER instead
-- of creating an own property. The column is then silently lost, with no error
-- and no diagnostic anywhere. #3504 fixed exactly this mechanism for the UDT
-- FIELD bag and deliberately scoped the ROW and JSON-object paths out, because a
-- row is a documented plain-object surface and the remedy there is a design
-- call. #3630 is that call, and this schema is its subject.
--
-- No pre-existing corpus fixture declares a row column with such a name, so the
-- defect had no row-level test subject at all.
--
-- WHY THE IDENTIFIERS ARE QUOTED
-- An unquoted CQL identifier must start with a letter, so `"__proto__"` is only
-- expressible as a quoted identifier. `"constructor"`, `"toString"` and
-- `"prototype"` are quoted for consistency and to preserve their exact case —
-- CQLite's `identifier` rule in cqlite-core/src/schema/cql_parser.rs handles the
-- quoted form. A quoted identifier (or a `SELECT ... AS` alias) is all a user
-- needs to reach this defect, which is what makes these names ordinary rather
-- than exotic.
--
-- WHAT EACH COLLISION COLUMN EXERCISES — THE FOUR ARE NOT INTERCHANGEABLE, AND
-- THIS IS THE MOST IMPORTANT PARAGRAPH IN THIS FILE. Three distinct mechanisms
-- are represented, and conflating them would make the fixture claim more than it
-- proves:
--
--   * "__proto__"   — THE ONLY INHERITED ACCESSOR on Object.prototype, and the
--                     only column here that is intercepted at all. An ordinary
--                     `[[Set]]` invokes its setter rather than creating a
--                     property, which produces TWO different failure modes
--                     depending on the VALUE:
--                       - a STRING value is silently DISCARDED (absent from
--                         Object.keys, not an own property, and reading the name
--                         back yields Object.prototype);
--                       - a NULL (or object) value is the one case the accessor
--                         honours, so it REPLACES the object's prototype — the
--                         returned row silently becomes a null-prototype object.
--                     Both are the defect; a fix must close both.
--
--   * "constructor" — INHERITED WRITABLE DATA properties, NOT accessors. A
--   * "toString"      `[[Set]]` of these DOES create an own property, so they
--                     behave CORRECTLY even before the fix. They are included
--                     for two load-bearing reasons anyway. First, they are the
--                     AC7 DISCRIMINATOR: a fix that special-cases the literal
--                     string `__proto__` passes every `__proto__` case above and
--                     is indistinguishable from a real fix until something asks
--                     about a DIFFERENT inherited name — which is the difference
--                     between removing the shared channel and picking a rarer
--                     delimiter (#3312's umbrella lesson, applied one layer down
--                     into JavaScript's own object model). Second, they are the
--                     regression control: they must keep working, so they prove
--                     the new write mechanism did not break the ordinary path.
--
--   * "prototype"   — NOT A PROPERTY OF Object.prototype AT ALL; `prototype`
--                     lives on FUNCTIONS. This column therefore exercises NO new
--                     interception mechanism and is expected to behave exactly
--                     like any other column both BEFORE and AFTER the fix. It is
--                     present only because the ruling on this issue is to
--                     enumerate the class rather than special-case one name, and
--                     a reader checking the class wants the adjacent name tested
--                     rather than assumed. It is NOT a second accessor case and
--                     must never be described as one — a false claim in a
--                     committed fixture comment is worse than an omitted column,
--                     because it is what stops the next person looking.
--
-- WRITTEN BY CASSANDRA, NOT BY CQLITE. The fixture is produced by
-- test-data/scripts/generate-issue-3630-row-collision.sh (cqlsh INSERT +
-- `nodetool flush` inside a cassandra:5.0.2 container). A CQLite-written fixture
-- would not do: Cassandra-written bytes additionally prove the DECODER can carry
-- such a column name at all, which is a separate claim from the binding's
-- rendering and the one a CQLite-written subject cannot make.
--
-- COMMITTED CHECKOUT-RELATIVE, under test-data/fixtures/issue_3630/ and NOT
-- under test-data/datasets/sstables/. Both binding test suites resolve the
-- dataset corpus from CQLITE_DATASETS_ROOT and never fall back to the checkout,
-- so a corpus-rooted fixture is INVISIBLE on exactly the runs that must see it
-- (every gate run exports that variable). A checkout-relative path cannot be
-- hidden by an environment variable. Same reasoning, verbatim, as
-- test-data/schemas/issue-3504-udt-collision.cql.
--
-- DELIBERATELY TINY, UNCOMPRESSED AND TIME-PINNED. No CompressionInfo.db: the
-- fixture is a committed binary and the property under test is ABOVE decode.
-- Every INSERT carries an explicit USING TIMESTAMP 1000 so the committed
-- sstabledump JSONL golden is reproducible across regenerations instead of
-- carrying a wall clock.
--
-- THE THREE ROWS (one per case the spec's first requirement names):
--   1. id=1 — all four collision columns non-null (strings), real_col set.
--             The string-valued `__proto__` case: the column must SURVIVE as an
--             own enumerable data property.
--   2. id=2 — "__proto__" NULL, the other three set. The prototype-replacement
--             case: the row's prototype must be UNCHANGED.
--   3. id=3 — none of the collision columns set, real_col only. The contrast
--             case, which pins that the fix is a property of the CONSTRUCTION
--             and not of the data.

CREATE KEYSPACE IF NOT EXISTS test_row_collision WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_row_collision;

CREATE TABLE IF NOT EXISTS row_collide (
    id            INT PRIMARY KEY,
    "__proto__"   text,
    "constructor" text,
    "toString"    text,
    "prototype"   text,
    real_col      int
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
