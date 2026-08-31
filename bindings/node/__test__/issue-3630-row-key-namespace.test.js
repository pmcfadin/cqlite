/**
 * A ROW COLUMN named `__proto__` is a COLUMN, not a prototype write (issue #3630).
 *
 * Spec: `openspec/changes/node-binding-drops-column/specs/binding-row-key-namespace/spec.md`.
 *
 * `row_to_object` builds each result row by writing the column name onto a plain
 * JavaScript object. An ordinary property assignment is a `[[Set]]`, which
 * CONSULTS THE PROTOTYPE CHAIN — so a column name matching an inherited accessor
 * reaches that accessor's SETTER instead of creating an own property. There is
 * exactly one such accessor on `Object.prototype`: `__proto__`. A string value is
 * then silently DISCARDED (the column vanishes with no error anywhere) and a null
 * value REPLACES the object's prototype.
 *
 * #3504 closed this mechanism for the UDT FIELD bag with `Object.create(null)`
 * and deliberately scoped the ROW path out, because a row is a DOCUMENTED
 * plain-object surface (`lib/index.d.ts`: `interface Row { [column: string]: Value }`)
 * whose consumers call `row.hasOwnProperty(...)`, spread it, and pass it to code
 * expecting a normal prototype. The adopted remedy here is therefore different:
 * rows KEEP `Object.prototype` and every column is defined as an OWN property
 * without ever performing a `[[Set]]`.
 *
 * WHY FOUR COLLISION COLUMNS, EXERCISING THREE MECHANISMS. The fixture's schema
 * states this at length; the short form, because it decides what each test proves:
 *   * `"__proto__"`   — the ONLY inherited ACCESSOR. The actual defect.
 *   * `"constructor"` — inherited WRITABLE DATA properties, not accessors, so a
 *   * `"toString"`      `[[Set]]` of these ALREADY works. They are the class
 *                       discriminator (a fix special-casing the literal string
 *                       `__proto__` passes every `__proto__` case and fails to
 *                       generalise) AND the regression control for the new write
 *                       mechanism.
 *   * `"prototype"`   — NOT on `Object.prototype` at all; it lives on functions.
 *                       Exercises NO interception and is expected to behave like
 *                       any ordinary column before and after. Present because the
 *                       ruling is to enumerate the class, never to special-case a
 *                       name. It is NOT a second accessor case.
 *
 * AN OBJECT LITERAL CANNOT EXPRESS THIS SUITE'S EXPECTATIONS — the same trap
 * `issue-3504-udt-field-namespace.test.js` documents. In a literal,
 * `{ __proto__: v }` (quoted or not) is the SPECIAL prototype-setting form, not a
 * property definition: for a string value it creates NO own property, so a
 * literal-based expectation would silently drop the very column under test and
 * then pass against output that had also dropped it. `Object.fromEntries` uses
 * `CreateDataProperty`, which defines an own property for any name. Hence the
 * `expectedRow` helper rather than inline literals.
 *
 * THE SUBJECT IS CASSANDRA-5.0.2-WRITTEN — `test-data/fixtures/issue_3630/`, from
 * `test-data/scripts/generate-issue-3630-row-collision.sh`. Not CQLite-written: a
 * Cassandra-written fixture additionally proves the DECODER can carry such a
 * column name at all, and a CQLite-written + CQLite-read subject is invariant to a
 * uniform error on both sides (#3042).
 *
 * WHAT THE JSONL GOLDEN IS *NOT* AN ORACLE FOR: physical-dump parity enumerates
 * on-disk cells and is completely blind to this defect — the cells are all present
 * on disk either way. The oracle is the spec's required shape, asserted at the
 * binding surface below.
 *
 * WHY THIS FILE RESOLVES ITS OWN PATHS: `global.testPaths.SSTABLES_DIR` is an
 * EITHER/OR on `CQLITE_DATASETS_ROOT` (`setup.js`) — unset it falls back to the
 * checkout, but when the variable IS set, which every gate run does, the checkout
 * copy is never consulted, so a fixture reached through it would be INVISIBLE
 * exactly where it has to run. Every path here is git-committed source, so absence
 * is a BROKEN CHECKOUT and must FAIL, never skip.
 */

const fs = require('fs');
const path = require('path');
const { Database } = require('../lib/index.js');

const FIXTURE_ROOT = path.join(
  global.testPaths.PROJECT_ROOT,
  'test-data',
  'fixtures',
  'issue_3630'
);
const SCHEMA = path.join(global.testPaths.SCHEMAS_DIR, 'issue-3630-row-collision.cql');
const QUERY = 'SELECT * FROM test_row_collision.row_collide';

/** The accessor column — the only one of the four that is actually intercepted. */
const ACCESSOR_COL = '__proto__';
/** Inherited writable DATA properties: already work unfixed; the class discriminator. */
const INHERITED_DATA_COLS = ['constructor', 'toString'];
/** Not on Object.prototype at all: exercises no interception. */
const NON_INHERITED_COL = 'prototype';
const ALL_COLLISION_COLS = [ACCESSOR_COL, ...INHERITED_DATA_COLS, NON_INHERITED_COL];

/**
 * Fail closed, naming the missing artifact, on a checkout lacking the fixture.
 *
 * The table directory is GLOBBED: a regeneration mints a new UUID, so a hardcoded
 * path would rot the first time the fixture is rebuilt.
 */
function assertFixturePresent() {
  if (!fs.existsSync(SCHEMA)) {
    throw new Error(`committed schema missing: ${SCHEMA}`);
  }
  const keyspaceDir = path.join(FIXTURE_ROOT, 'test_row_collision');
  const tables = fs.existsSync(keyspaceDir)
    ? fs.readdirSync(keyspaceDir).filter((name) => name.startsWith('row_collide-'))
    : [];
  if (tables.length !== 1) {
    throw new Error(
      `expected exactly one row_collide-* table dir under ${keyspaceDir}, got ${JSON.stringify(tables)}`
    );
  }
  const dataFiles = fs
    .readdirSync(path.join(keyspaceDir, tables[0]))
    .filter((n) => n.endsWith('-Data.db'));
  if (dataFiles.length !== 1) {
    throw new Error(
      `expected exactly one *-Data.db in ${tables[0]}, got ${JSON.stringify(dataFiles)}`
    );
  }
}

/**
 * Realm-safe "this object's prototype was not replaced" assertion.
 *
 * `Object.getPrototypeOf(row) === Object.prototype` DOES NOT WORK HERE and its
 * failure is not a defect: jest runs this file in a sandboxed VM context whose
 * `Object` intrinsic is a DIFFERENT object from the one the addon reaches through
 * `env.get_global()`, so the identity comparison is false even for a perfectly
 * healthy plain object. Measured: it failed on rows 2 and 3 — rows with no
 * colliding cell at all — with jest reporting "Expected: {} Received: serializes
 * to the same string", the signature of two distinct-but-identical prototypes.
 * (`types.test.js` documents the same realm split for `instanceof Map`.)
 *
 * The property that actually matters is what the defect does: assigning null to
 * `__proto__` REPLACES the prototype with null. So assert (a) the prototype is
 * not null, and (b) it is the SAME object for every row — self-consistency, which
 * is realm-independent and still catches a single row being mutated by its data.
 */
function expectPrototypeIntact(row, reference) {
  const proto = Object.getPrototypeOf(row);
  expect(proto).not.toBeNull();
  expect(proto).toBe(reference);
}

/** Build an expectation object safely — see the header on why not a literal. */
function expectedRow(entries) {
  return Object.fromEntries(entries);
}

/**
 * Assert `name` is an own ENUMERABLE DATA property of `obj`, and return its value.
 *
 * This is the property the whole change is about, so it is asserted structurally
 * (via the descriptor) rather than by reading the value — `obj[name]` would
 * happily return an INHERITED value and pass.
 */
function ownDataProperty(obj, name) {
  const desc = Object.getOwnPropertyDescriptor(obj, name);
  expect(desc).toBeDefined();
  expect(desc).toEqual({
    value: desc && desc.value,
    writable: true,
    enumerable: true,
    configurable: true,
  });
  expect(Object.hasOwn(obj, name)).toBe(true);
  return desc.value;
}

describe('row column / Object.prototype collision (issue #3630)', () => {
  let db = null;
  let rows = null;
  let columns = null;

  beforeAll(async () => {
    assertFixturePresent();
    db = await Database.open(FIXTURE_ROOT, { schema: SCHEMA });
    const result = await db.executeNative(QUERY);
    columns = result.columns.map((c) => c.name);
    rows = new Map(result.rows.map((row) => [row.id, row]));
    // Three rows by construction. A partial read is a decode regression, not a
    // reason to assert less.
    expect([...rows.keys()].sort()).toEqual([1, 2, 3]);
  });

  afterAll(async () => {
    if (db) {
      await db.close();
      db = null;
    }
  });

  // ==========================================================================
  // The defect: the accessor column
  // ==========================================================================

  test('a STRING-valued column named `__proto__` survives as an own data property', () => {
    // BEFORE THE FIX, measured on this exact row: the cell was absent from
    // `Object.keys(row)`, was not an own property, and `row.__proto__` read back
    // `Object.prototype` — the column was silently LOST with no error.
    const row = rows.get(1);

    expect(ownDataProperty(row, ACCESSOR_COL)).toBe('user-supplied-proto');
    expect(Object.keys(row)).toContain(ACCESSOR_COL);
    expect(Object.entries(row)).toContainEqual([ACCESSOR_COL, 'user-supplied-proto']);
    // Survives a JSON round-trip, i.e. it really is an enumerable own property
    // and not an accessor that happens to read back.
    expect(JSON.parse(JSON.stringify(row))[ACCESSOR_COL]).toBe('user-supplied-proto');
  });

  test('the row keeps Object.prototype — the ADOPTED contract, not a null-proto bag', () => {
    // The row contract deliberately DIFFERS from #3504's UDT field bag: rows are
    // a documented plain-object surface, so they keep their prototype and the
    // columns are DEFINED rather than assigned. Asserted for every row, including
    // the one carrying a null-valued accessor column, because the property must
    // be one of the CONSTRUCTION and not of the data.
    // The reference is the prototype of a row with NO collision column, i.e. one
    // no data could have mutated. Every other row must share that exact object.
    const reference = Object.getPrototypeOf(rows.get(3));
    expect(reference).not.toBeNull();
    for (const id of [1, 2, 3]) {
      expectPrototypeIntact(rows.get(id), reference);
    }
  });

  test('the documented plain-object affordances still work', () => {
    const row = rows.get(1);
    // These are exactly what `Object.create(null)` would have broken on every row
    // of every query, and why remedy 2 was rejected for rows.
    expect(row.hasOwnProperty(ACCESSOR_COL)).toBe(true);
    expect(typeof row.toString).toBe('string'); // shadowed BY THE COLUMN, see below
    // NOT `row instanceof Object` — same realm split as the prototype identity
    // above (the addon's Object is a different intrinsic), so that would fail on
    // a healthy row. `Object.prototype.toString` reads the internal slot instead.
    expect(Object.prototype.toString.call(row)).toBe('[object Object]');
    expect({ ...row }[ACCESSOR_COL]).toBe('user-supplied-proto');
  });

  // ==========================================================================
  // The CLASS, not the name — asserted over every column the row carries
  // ==========================================================================

  test('EVERY DECLARED column of the fully-populated row is an own data property', () => {
    // A guard that matches a NAME cannot see a PROPERTY, so this case names no
    // column — it derives its subject from the AUTHORITATIVE column list, which
    // means a future inherited name added to `Object.prototype` is covered here
    // without editing this file.
    //
    // IT MUST ITERATE `columns`, NOT `Object.keys(row)`, AND THAT IS THE WHOLE
    // POINT. The first draft iterated `Object.keys(row)` and PASSED against
    // UNFIXED CODE — vacuously, because the lost `__proto__` column is precisely
    // the one absent from `Object.keys`, so the loop could only ever inspect the
    // columns that had already survived. A test whose subject set is the output
    // it is checking cannot see an omission from that output. Row 1 has every
    // cell populated by construction, so every DECLARED column must arrive.
    const row = rows.get(1);
    expect(columns.length).toBeGreaterThan(0);
    const missing = columns.filter((name) => !Object.hasOwn(row, name));
    expect(missing).toEqual([]);
    for (const name of columns) {
      expect({ name, ok: ownDataProperty(row, name) !== undefined })
        .toEqual({ name, ok: true });
    }
  });

  test('a SECOND inherited name behaves correctly by the same mechanism', () => {
    // The class discriminator (AC7): a fix special-casing the literal string
    // `__proto__` passes every case above and fails HERE. `constructor` and
    // `toString` are inherited WRITABLE DATA properties, so a `[[Set]]` of them
    // already created an own property — which makes them the regression control
    // too: they must keep working under the new write mechanism.
    const row = rows.get(1);
    expect(ownDataProperty(row, 'constructor')).toBe('user-supplied-constructor');
    expect(ownDataProperty(row, 'toString')).toBe('user-supplied-tostring');
    // The column SHADOWS the inherited member rather than being shadowed BY it.
    expect(row.constructor).toBe('user-supplied-constructor');
    expect(row.toString).toBe('user-supplied-tostring');
  });

  test('a name that is NOT on Object.prototype is an ordinary column', () => {
    // `prototype` lives on FUNCTIONS, not on Object.prototype, so this exercises
    // no interception and must be indistinguishable from `real_col`. Asserted so
    // the class claim is measured across all four names rather than assumed for
    // the two that are interesting.
    const row = rows.get(1);
    expect(ownDataProperty(row, NON_INHERITED_COL)).toBe('user-supplied-prototype');
    expect(ownDataProperty(row, 'real_col')).toBe(42);
  });

  // ==========================================================================
  // Row 2 — a column DECLARED in metadata with NO value
  // ==========================================================================

  test('a valueless column is SKIPPED, not null-filled, and does not touch the prototype', () => {
    // MEASURED on the generated golden: row 2's explicit CQL NULL is a CELL
    // TOMBSTONE with no value cell (see test-data/fixtures/issue_3630/README.md).
    // So this row is NOT the prototype-replacement case it was first drafted as —
    // with no entry in the decoded value map there is no assignment to intercept.
    // Its real role is the #1446 contract this change must preserve: a metadata
    // column with no matching value is SKIPPED rather than emitted as null.
    // (The prototype-replacement mode is covered at the Rust level, over a value
    // map explicitly holding `Value::Null`.)
    const row = rows.get(2);

    expect(columns).toContain(ACCESSOR_COL); // declared in metadata...
    expect(Object.hasOwn(row, ACCESSOR_COL)).toBe(false); // ...and correctly absent
    expect(Object.keys(row)).not.toContain(ACCESSOR_COL);
    // No phantom null, and — the point of the row — no prototype write either.
    expectPrototypeIntact(row, Object.getPrototypeOf(rows.get(3)));

    // The other three columns on the same row are unaffected.
    expect(ownDataProperty(row, 'constructor')).toBe('user-supplied-constructor-2');
    expect(ownDataProperty(row, 'toString')).toBe('user-supplied-tostring-2');
    expect(ownDataProperty(row, NON_INHERITED_COL)).toBe('user-supplied-prototype-2');
  });

  test('row 3 is the contrast case — no collision column present at all', () => {
    const row = rows.get(3);
    expect(Object.keys(row).sort()).toEqual(['id', 'real_col']);
    for (const col of ALL_COLLISION_COLS) {
      expect(Object.hasOwn(row, col)).toBe(false);
    }
    expect(Object.getPrototypeOf(row)).not.toBeNull();
  });

  // ==========================================================================
  // The ALIAS route — the issue's own "minimal repro needs no fixture" claim,
  // and the only route that reaches the NULL-VALUED case
  // ==========================================================================

  describe('reached by a SELECT alias rather than a declared column', () => {
    // MEASURED (task 1.1): `SELECT id AS __proto__` IS supported, so the issue's
    // claim that a minimal repro needs no fixture at all is TRUE. Recorded here
    // as a SECOND subject, never a substitute for the Cassandra-written fixture —
    // the fixture is what proves the DECODER can carry such a column name, which
    // an alias applied to an ordinary column does not.
    //
    // ALSO MEASURED: a QUOTED alias is a parse error ("Expected alias name after
    // AS"), so these queries must use the unquoted form. That is a CQL-parser gap
    // orthogonal to this issue; it is recorded, not worked around.

    test('an aliased column named `__proto__` is an own data property', async () => {
      const result = await db.executeNative(
        'SELECT id AS __proto__ FROM test_row_collision.row_collide'
      );
      expect(result.columns.map((c) => c.name)).toEqual([ACCESSOR_COL]);
      for (const row of result.rows) {
        expect(ownDataProperty(row, ACCESSOR_COL)).toEqual(expect.any(Number));
        expect(Object.keys(row)).toEqual([ACCESSOR_COL]);
        expect(Object.getPrototypeOf(row)).not.toBeNull();
      }
    });

    test('a NULL-valued column named `__proto__` does NOT replace the prototype', async () => {
      // THIS IS THE SECOND, HARSHER FAILURE MODE, and this query is the ONLY
      // route to it that exists today. Assigning a STRING to `__proto__` is a
      // silent no-op; assigning NULL is the one case the inherited accessor
      // HONOURS, so an unfixed `[[Set]]` REPLACES the row's prototype with null
      // and the row silently stops being a normal object.
      //
      // The fixture CANNOT reach this (measured): an explicit CQL null is a cell
      // TOMBSTONE, so the column arrives with no value at all, `row_to_object`
      // skips it, and there is no assignment to intercept — see
      // test-data/fixtures/issue_3630/README.md. And it cannot be reached from a
      // Rust unit test either: a real `ConvCtx` needs a live napi `Env`, which
      // `value_tests.rs` records as unavailable there.
      //
      // `TTL(col)` is the route because its evaluation is NOT YET WIRED (#692) so
      // it returns `Value::Null` — a genuine null VALUE present in the row's
      // value map, which is exactly the input required. If #692 lands and TTL
      // starts returning a real integer, THIS TEST WILL SILENTLY STOP COVERING
      // THE NULL CASE: the `toBeNull()` assertion below is what fails loudly then,
      // and the remedy is to find another null-valued route rather than to relax
      // it.
      const result = await db.executeNative(
        'SELECT TTL(real_col) AS __proto__ FROM test_row_collision.row_collide'
      );
      expect(result.rows.length).toBeGreaterThan(0);
      for (const row of result.rows) {
        // The value really is null — see the #692 note above.
        expect(ownDataProperty(row, ACCESSOR_COL)).toBeNull();
        expect(Object.keys(row)).toContain(ACCESSOR_COL);
        // ...and the prototype was NOT replaced by it.
        expect(Object.getPrototypeOf(row)).not.toBeNull();
        expect(Object.prototype.toString.call(row)).toBe('[object Object]');
      }
    });
  });

  // ==========================================================================
  // #1446's ordering contract must survive
  // ==========================================================================

  test('Object.keys(row) preserves authoritative SELECT column order', () => {
    // #1446: property insertion order equals the column order, so `Object.keys`
    // matches `columns.map(c => c.name)` — not hash order. Under a
    // define-properties mechanism the descriptors must be applied in that same
    // order. Compared against the columns actually PRESENT in the row, because a
    // valueless column is legitimately skipped (see row 2 above).
    for (const [, row] of rows) {
      const present = columns.filter((name) => Object.hasOwn(row, name));
      expect(Object.keys(row)).toEqual(present);
    }
  });

  test('the full expected shape of row 1, built without an object literal', () => {
    // Whole-row equality, as a SET-and-VALUE assertion rather than a count: a
    // count states only "N of something" and cannot see a column that was lost
    // while another took its place, which is this defect's entire shape.
    expect({ ...rows.get(1) }).toEqual(
      expectedRow([
        ['id', 1],
        [ACCESSOR_COL, 'user-supplied-proto'],
        ['constructor', 'user-supplied-constructor'],
        ['toString', 'user-supplied-tostring'],
        [NON_INHERITED_COL, 'user-supplied-prototype'],
        ['real_col', 42],
      ])
    );
  });
});
