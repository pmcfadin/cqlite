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
 * WHY THIS FILE RESOLVES ITS OWN PATHS: `global.testPaths.SSTABLES_DIR` derives
 * from `CQLITE_DATASETS_ROOT` and never falls back to the checkout, so a fixture
 * reached through it is INVISIBLE on any box with that variable set — i.e. on
 * every gate run. The fixture is committed checkout-relative and resolved from
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

    // ...and the declared fields, all three, unmodified.
    expect(udt.fields).toEqual({
      _type: 'user-supplied-type',
      _keyspace: 'user-supplied-keyspace',
      real_field: 42,
    });
    expect(Object.keys(udt.fields)).toHaveLength(3);
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
    expect(udt.fields).toEqual({
      _type: null,
      _keyspace: 'keyspace-field-only',
      real_field: 0,
    });
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
    expect(key.fields).toEqual({
      _type: 'key-type-marker',
      _keyspace: 'key-keyspace-marker',
      real_field: 100,
    });
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
    expect(members[0].fields).toEqual({
      _type: 'set-member-type',
      _keyspace: 'set-member-keyspace',
      real_field: 200,
    });
  });

  test('RECORDED GAP: a non-frozen map keyed by a UDT decodes to a Buffer key', () => {
    // Decode-level, out of #3504's scope. A NON-frozen `map<frozen<udt>, int>`
    // is multicell, so its key lives in the CELL PATH, and `parse_cell_path_key`
    // (cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column.rs)
    // matches a closed set of PRIMITIVE cell-path types and falls back to
    // `Value::Blob` for a frozen UDT. Pinned as characterization, not as a
    // desirable shape: `cm` is the spelling a user would most naturally write,
    // and without this a reader would reasonably assume it covers the key path.
    // Details: test-data/fixtures/issue_3504/README.md.
    for (const column of ['cm', 'tm']) {
      const [key] = soleEntry(rows.get(1)[column]);
      expect(Buffer.isBuffer(key)).toBe(true);
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

    // Non-vacuity: the reference must actually carry the colliding subject, or an
    // emptied/renamed file would let this pass having compared nothing.
    expect(expected['row1.c'].fields._type).toBe('user-supplied-type');
    expect(expected['row1.c'].typeName).toBe('collide');
  });
});
