/**
 * A UDT field named `_type`/`_keyspace` displaces nothing (issue #3504).
 *
 * Spec: `openspec/changes/udt-field-namespace/specs/udt-type-identity/spec.md`.
 *
 * `udt_to_object` used to set `_type` and `_keyspace` on the result object and
 * then set every field name on the SAME object, so a UDT that DECLARES a field
 * named `_type` or `_keyspace` — legal CQL via a quoted identifier — silently
 * OVERWROTE the marker and the type name became unrecoverable. The shape is now
 * `{ typeName, keyspace, fields }`: identity at the top level, declared fields in
 * a namespace of their own.
 *
 * THE SAME CLASS ONE LAYER DOWN (roborev R1-1): giving the fields their own
 * object is not sufficient while it is a PLAIN object, because an ordinary
 * property assignment consults the prototype chain — so a field named
 * `__proto__` (just as legal, and just as quoted) reached
 * `Object.prototype`'s inherited accessor instead of becoming a field. The
 * fixture's `collide`/`collide_twin` types now declare one, and `fields` is
 * built with a NULL PROTOTYPE. Measured before the fix, on these exact rows:
 * row 1's string-valued `__proto__` VANISHED (absent from `Object.keys`, not an
 * own property, reading back as `Object.prototype`) and row 3's null-valued one
 * REPLACED the field bag's prototype with `null`.
 *
 * THE SUBJECT IS CASSANDRA-WRITTEN. `test-data/fixtures/issue_3504/` comes from
 * `test-data/scripts/generate-issue-3504-udt-collision.sh` (cassandra:5.0.2
 * container), not from CQLite's write path — which additionally proves the
 * DECODER can produce such a UDT at all.
 *
 * WHAT THE JSONL GOLDEN IS *NOT* AN ORACLE FOR: for this input `sstabledump`'s
 * flat `{"_type": "user-supplied-type", ...}` is textually identical to the OLD
 * buggy injection, so physical-dump parity is blind to this defect. The oracle is
 * the spec's required shape, asserted at the binding surface below.
 *
 * WHY THIS FILE RESOLVES ITS OWN PATHS: `global.testPaths.SSTABLES_DIR` is an
 * EITHER/OR on `CQLITE_DATASETS_ROOT` (`setup.js:23`) — unset, it DOES fall back
 * to the checkout's `test-data/datasets`. But when the variable IS set, which
 * every gate run does, the checkout copy is never consulted, so a fixture
 * reached through it would be INVISIBLE exactly where it has to run. The fixture is committed checkout-relative and resolved from
 * `PROJECT_ROOT` with no environment variable, so it also does NOT depend on
 * `DATASETS_AVAILABLE` and must never be skipped: every path here is
 * git-committed source, so absence is a broken checkout, not a skippable
 * condition.
 */

const fs = require('fs');
const path = require('path');
const { Database } = require('../lib/index.js');

const FIXTURE_ROOT = path.join(
  global.testPaths.PROJECT_ROOT,
  'test-data',
  'fixtures',
  'issue_3504'
);
const SCHEMA = path.join(global.testPaths.SCHEMAS_DIR, 'issue-3504-udt-collision.cql');
const PARITY_FACTS = path.join(FIXTURE_ROOT, 'binding-parity-facts.json');
const QUERY = 'SELECT * FROM test_udt_collision.udt_collide';

/**
 * Fail closed, naming the missing artifact, on a checkout that lacks the fixture.
 *
 * The table directory is GLOBBED: a regeneration mints a new UUID, so a hardcoded
 * path would rot the first time the fixture is rebuilt.
 */
function assertFixturePresent() {
  if (!fs.existsSync(SCHEMA)) {
    throw new Error(`committed schema missing: ${SCHEMA}`);
  }
  if (!fs.existsSync(PARITY_FACTS)) {
    throw new Error(`committed parity reference missing: ${PARITY_FACTS}`);
  }
  const keyspaceDir = path.join(FIXTURE_ROOT, 'test_udt_collision');
  const tables = fs.existsSync(keyspaceDir)
    ? fs.readdirSync(keyspaceDir).filter((name) => name.startsWith('udt_collide-'))
    : [];
  if (tables.length !== 1) {
    throw new Error(
      `expected exactly one udt_collide-* table dir under ${keyspaceDir}, got ${JSON.stringify(tables)}`
    );
  }
  const dataDb = fs
    .readdirSync(path.join(keyspaceDir, tables[0]))
    .filter((name) => name.endsWith('-Data.db'));
  if (dataDb.length === 0) {
    throw new Error(
      `no *-Data.db under ${path.join(keyspaceDir, tables[0])} — the binaries are ` +
        'gitignored and must be force-added (`git add -f`); see ' +
        'test-data/fixtures/issue_3504/README.md'
    );
  }
}

/** The language-neutral fact triple for a rendered UDT (the reference's shape). */
function facts(udt) {
  return { typeName: udt.typeName, keyspace: udt.keyspace, fields: udt.fields };
}

/**
 * Build an expected field bag from `[name, value]` pairs.
 *
 * AN OBJECT LITERAL CANNOT EXPRESS THIS SUITE'S EXPECTATIONS. In a literal,
 * `{ __proto__: v }` — quoted or not — is the SPECIAL prototype-setting form,
 * not a property definition: for a string value it creates NO own property at
 * all, so a literal-based expectation would silently drop the very field under
 * test and then pass against output that had also dropped it. `Object.fromEntries`
 * uses `CreateDataProperty`, which defines an own property for any name. (This
 * is the same control/data collision the production fix removes, met again in
 * the test's own syntax — hence a helper rather than a comment at each site.)
 */
function fieldsOf(entries) {
  return Object.fromEntries(entries);
}

/** Own-property-safe read of `name` from a field bag with a null prototype. */
function ownField(fields, name) {
  expect(Object.prototype.hasOwnProperty.call(fields, name)).toBe(true);
  return fields[name];
}

/**
 * Cross-realm `Map` test, matching `types.test.js`'s helper.
 *
 * `instanceof Map` is FALSE here even for a genuine map: jest runs this file in a
 * sandboxed VM context whose `Map` intrinsic is a different object from the one
 * the addon reaches through `env.get_global()`. `Object.prototype.toString` reads
 * the internal slot instead, so it is realm-independent.
 */
const isMap = (value) =>
  value !== null &&
  value !== undefined &&
  typeof value === 'object' &&
  Object.prototype.toString.call(value) === '[object Map]';

/** The single entry of a one-entry JS `Map`, as `[key, value]`. */
function soleEntry(map) {
  expect(isMap(map)).toBe(true);
  expect(map.size).toBe(1);
  return [...map.entries()][0];
}

describe('UDT field-name / type-identity collision (issue #3504)', () => {
  let db = null;
  let rows = null;

  beforeAll(async () => {
    assertFixturePresent();
    db = await Database.open(FIXTURE_ROOT, { schema: SCHEMA });
    const result = await db.executeNative(QUERY);
    rows = new Map(result.rows.map((row) => [row.id, row]));
    // Three rows by construction; a partial read is a decode regression, not a
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
  // Site 3 — the rendered UDT
  // ==========================================================================

  test('a colliding UDT yields BOTH the field value and the type identity', () => {
    const udt = rows.get(1).c;
    expect(typeof udt).toBe('object');

    // Identity, in a namespace no field name can address.
    expect(udt.typeName).toBe('collide');
    expect(udt.keyspace).toBe('test_udt_collision');

    // ...and the declared fields, all four, unmodified.
    expect(udt.fields).toEqual(
      fieldsOf([
        ['_type', 'user-supplied-type'],
        ['_keyspace', 'user-supplied-keyspace'],
        ['__proto__', 'user-supplied-proto'],
        ['real_field', 42],
      ])
    );
    // The exact field-NAME SET, not a count: a count says only "four of
    // something" and cannot see a lost field masked by an injected one, which is
    // exactly how the `__proto__` loss survived a `toHaveLength(3)` assertion.
    expect(Object.keys(udt.fields).sort()).toEqual([
      '__proto__',
      '_keyspace',
      '_type',
      'real_field',
    ]);
  });

  test('the field namespace is `fields` ALONE — Object.keys holds no field name', () => {
    const udt = rows.get(1).c;
    // The exact top-level surface. Asserted as an EQUALITY rather than as
    // absence probes, so a future re-flattening of fields beside `typeName`
    // (the defect, reintroduced) reds here immediately.
    expect(Object.keys(udt).sort()).toEqual(['fields', 'keyspace', 'typeName']);
    for (const name of Object.keys(udt.fields)) {
      expect(Object.prototype.hasOwnProperty.call(udt, name)).toBe(false);
    }
  });

  test('the removed markers are no longer readable from the object', () => {
    // The REMOVED SHARED CHANNEL, asserted as removed: on `main` both of these
    // returned strings — for the colliding UDT, the user's own field values.
    for (const udt of [rows.get(1).c, rows.get(1).p]) {
      expect(udt._type).toBeUndefined();
      expect(udt._keyspace).toBeUndefined();
      expect(udt.typeName).toBeTruthy();
    }
  });

  test('a non-colliding UDT keeps working through field access', () => {
    const udt = rows.get(1).p;
    expect(udt.typeName).toBe('plain');
    expect(udt.keyspace).toBe('test_udt_collision');
    expect(udt.fields.label).toBe('no-colliding-field');
    expect(udt.fields.real_field).toBe(7);
    expect(udt.fields).toEqual({ label: 'no-colliding-field', real_field: 7 });
    expect(Object.keys(udt.fields).sort()).toEqual(['label', 'real_field']);
  });

  test('a NULL colliding field does not null the type name', () => {
    // A distinct failure mode from the string case: under the old code the
    // injected type name was overwritten by whatever the field held, so a null
    // `_type` field made the type name `null` rather than merely wrong.
    const udt = rows.get(3).c;
    expect(udt.typeName).toBe('collide');
    expect(udt.keyspace).toBe('test_udt_collision');
    expect(udt.fields._type).toBeNull();
    expect(Object.prototype.hasOwnProperty.call(udt.fields, '_type')).toBe(true);
    expect(udt.fields).toEqual(
      fieldsOf([
        ['_type', null],
        ['_keyspace', 'keyspace-field-only'],
        ['__proto__', null],
        ['real_field', 0],
      ])
    );
  });

  // ==========================================================================
  // R1-1 — a field name cannot reach JavaScript's own object model
  // ==========================================================================

  test('a UDT field named `__proto__` is a FIELD, not a prototype write', () => {
    // MEASURED BEFORE THE FIX, on this exact row: `Object.keys(fields)` was
    // ["_type","_keyspace","real_field"], `hasOwnProperty('__proto__')` was
    // false, and `fields.__proto__` read back `Object.prototype` — the declared
    // field was silently GONE, because `[[Set]]` had called the inherited
    // accessor instead of defining a property.
    const fields = rows.get(1).c.fields;
    expect(ownField(fields, '__proto__')).toBe('user-supplied-proto');
    expect(Object.keys(fields)).toContain('__proto__');
    // An own DATA property, not an accessor: the descriptor is the only thing
    // that distinguishes "defined the field" from "wrote through a setter that
    // happened to store the value somewhere".
    expect(Object.getOwnPropertyDescriptor(fields, '__proto__')).toEqual({
      value: 'user-supplied-proto',
      writable: true,
      enumerable: true,
      configurable: true,
    });
    // ...and it survives the ordinary ways a caller consumes a field bag.
    expect(JSON.parse(JSON.stringify(fields)).__proto__).toBe('user-supplied-proto');
    expect(Object.entries(fields)).toContainEqual(['__proto__', 'user-supplied-proto']);
  });

  test('a NULL `__proto__` field does not replace the field bag prototype', () => {
    // The harsher half of the hazard, and a DIFFERENT failure mode from the
    // string case: `obj.__proto__ = null` REPLACES the object's prototype, so
    // before the fix row 3's field bag came back with its prototype mutated by
    // data AND the field missing. Measured then: prototype `null` for row 3,
    // `Object.prototype` for row 1 — i.e. the shape of the object depended on a
    // field VALUE.
    const fields = rows.get(3).c.fields;
    expect(ownField(fields, '__proto__')).toBeNull();
    expect(Object.keys(fields)).toContain('__proto__');
  });

  test('every UDT field bag has a null prototype, by construction not by data', () => {
    // The property that makes the fix a CLASS fix rather than a `__proto__`
    // special case: the bag inherits NOTHING, so no field name — not
    // `__proto__`, not `constructor`, not a name a future JavaScript adds to
    // `Object.prototype` — can reach an inherited accessor. Asserted across a
    // UDT that declares `__proto__` (rows 1/3), one that does not (`p`), and a
    // UDT in key and element position, so it is visibly independent of the data.
    const bags = [
      rows.get(1).c.fields,
      rows.get(1).p.fields,
      rows.get(2).p.fields,
      rows.get(3).c.fields,
      soleEntry(rows.get(1).fcm)[0].fields,
      soleEntry(rows.get(1).ftm)[0].fields,
      [...rows.get(1).fs][0].fields,
    ];
    for (const fields of bags) {
      expect(Object.getPrototypeOf(fields)).toBeNull();
      // Nothing is inherited, so an absence probe on the bag is exactly an
      // absence: on a plain object `fields.constructor` is truthy and
      // `'toString' in fields` is true, both of which read as fields that do not
      // exist.
      expect(fields.constructor).toBeUndefined();
      expect('toString' in fields).toBe(false);
    }
    // The OUTER object is developer-keyed (`typeName`/`keyspace`/`fields` are
    // chosen here, never by data), so it deliberately keeps a normal prototype —
    // stated as an assertion so the asymmetry is intentional and visible.
    // Compared BEHAVIOURALLY, not by identity against this realm's
    // `Object.prototype`: jest runs each file in a sandboxed VM context whose
    // intrinsics are different objects from the ones the addon reaches through
    // `env.get_global()` (the same realm split the `isMap` helper above exists
    // for), so an identity check would fail on correct output.
    const outer = rows.get(1).c;
    expect(Object.getPrototypeOf(outer)).not.toBeNull();
    expect(typeof outer.hasOwnProperty).toBe('function');
  });

  // ==========================================================================
  // UDTs in key / element position
  // ==========================================================================

  test('a frozen map keyed by a colliding UDT keeps the key identity', () => {
    // Node needs no hashable projection (the Python site 4): `map_to_js_map`
    // builds a real JS `Map` keyed by the object itself. The property asserted
    // here is the same one — identity recoverable from the KEY without reading
    // its field namespace.
    const [key, value] = soleEntry(rows.get(1).fcm);
    expect(value).toBe(3);
    expect(key.typeName).toBe('collide');
    expect(key.keyspace).toBe('test_udt_collision');
    expect(key.fields).toEqual(
      fieldsOf([
        ['_type', 'key-type-marker'],
        ['_keyspace', 'key-keyspace-marker'],
        ['__proto__', 'key-proto-marker'],
        ['real_field', 100],
      ])
    );
  });

  test('same fields, different UDT types stay distinguishable as map keys', () => {
    const [collideKey] = soleEntry(rows.get(1).fcm);
    const [twinKey] = soleEntry(rows.get(1).ftm);
    expect(collideKey.fields).toEqual(twinKey.fields);
    expect(collideKey.typeName).toBe('collide');
    expect(twinKey.typeName).toBe('collide_twin');
    expect(collideKey.typeName).not.toBe(twinKey.typeName);
  });

  test('frozen set of UDTs keeps each element identity', () => {
    const members = [...rows.get(1).fs];
    expect(members).toHaveLength(1);
    expect(members[0].typeName).toBe('collide');
    expect(members[0].fields).toEqual(
      fieldsOf([
        ['_type', 'set-member-type'],
        ['_keyspace', 'set-member-keyspace'],
        ['__proto__', 'set-member-proto'],
        ['real_field', 200],
      ])
    );
  });

  test('FIXED (#3612): a non-frozen map keyed by a UDT decodes structurally', () => {
    // This test used to pin the DEFECT. A NON-frozen `map<frozen<udt>, int>` is
    // multicell, so its key lives in the CELL PATH, and `parse_cell_path_key`
    // (cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column/cell_path_key.rs)
    // used to match a closed set of PRIMITIVE cell-path types and fall back to
    // `Value::Blob` for a frozen UDT — so `cm`/`tm` keys arrived as a Buffer. It
    // now delegates to the structural decoder.
    //
    // Asserted AGAINST the frozen control rather than against literals: `cm` and
    // `fcm` are the two legal spellings of the same map and the fixture stores
    // the same key in both, so the strongest statement is that they agree.
    // Mirrors 'a frozen map keyed by a colliding UDT keeps the key identity'.
    const expectedFields = fieldsOf([
      ['_type', 'key-type-marker'],
      ['_keyspace', 'key-keyspace-marker'],
      ['__proto__', 'key-proto-marker'],
      ['real_field', 100],
    ]);
    for (const [column, control, typeName] of [
      ['cm', 'fcm', 'collide'],
      ['tm', 'ftm', 'collide_twin'],
    ]) {
      const [key, value] = soleEntry(rows.get(1)[column]);
      expect(Buffer.isBuffer(key)).toBe(false);
      expect(key.typeName).toBe(typeName);
      expect(key.keyspace).toBe('test_udt_collision');
      expect(key.fields).toEqual(expectedFields);
      expect(value).toBe(column === 'cm' ? 1 : 2);
      // The parity statement: the multicell and frozen spellings of one map
      // present the same key, so a caller cannot tell them apart.
      const [controlKey] = soleEntry(rows.get(1)[control]);
      expect(key.typeName).toBe(controlKey.typeName);
      expect(key.keyspace).toBe(controlKey.keyspace);
      expect(key.fields).toEqual(controlKey.fields);
    }
  });

  // ==========================================================================
  // Cross-binding parity (AC3)
  // ==========================================================================

  test('binding facts match the committed cross-binding reference', () => {
    // Compared as DATA, never by host type: this suite and
    // `bindings/python/tests/test_issue_3504_udt_field_namespace.py` each derive
    // the same fact set from their OWN binding output and assert equality against
    // ONE committed file, so neither can drift without reddening. The Python
    // binding spells `typeName` as `type_name` (PyO3 exposes snake_case, napi-rs
    // camelCases); the SEMANTICS, which is what AC3 constrains, are identical.
    const reference = JSON.parse(fs.readFileSync(PARITY_FACTS, 'utf8'));
    const expected = reference.udts;

    const observed = {
      'row1.c': facts(rows.get(1).c),
      'row1.p': facts(rows.get(1).p),
      'row1.fcm_key': facts(soleEntry(rows.get(1).fcm)[0]),
      'row1.ftm_key': facts(soleEntry(rows.get(1).ftm)[0]),
      'row1.fs_0': facts([...rows.get(1).fs][0]),
      'row2.p': facts(rows.get(2).p),
      'row3.c': facts(rows.get(3).c),
    };

    // Both directions: a reference entry with no observed counterpart is as much
    // a drift as the reverse, and comparing only the intersection would let
    // either side quietly shrink.
    expect(Object.keys(observed).sort()).toEqual(Object.keys(expected).sort());
    expect(observed).toEqual(expected);

    expect(soleEntry(rows.get(1).fcm)[1]).toBe(reference.map_values['row1.fcm_value']);
    expect(soleEntry(rows.get(1).ftm)[1]).toBe(reference.map_values['row1.ftm_value']);

    // Non-vacuity: the reference must actually carry the colliding subjects, or
    // an emptied/renamed file would let this pass having compared nothing. Both
    // collision classes are named: the `_type` field and the `__proto__` field.
    // `JSON.parse` defines `__proto__` as an ORDINARY OWN PROPERTY (it never
    // invokes a setter), so the reference really does carry the field and
    // `toEqual` above really does compare it.
    expect(expected['row1.c'].fields._type).toBe('user-supplied-type');
    expect(ownField(expected['row1.c'].fields, '__proto__')).toBe('user-supplied-proto');
    expect(expected['row1.c'].typeName).toBe('collide');
  });
});
