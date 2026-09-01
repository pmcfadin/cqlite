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

    // Every entry is DERIVED from this binding's own output; nothing here is a
    // literal. `cm`/`tm` (MULTICELL: the key lives in the CELL PATH) sit beside
    // `fcm`/`ftm` (FROZEN: a single value cell) because those are two different
    // decoders in cqlite-core and only the frozen one used to reach a UDT at all
    // (#3612). Carrying both makes this case a parity control in TWO directions
    // at once: cross-BINDING, as every entry here is, and cross-DECODE-PATH.
    const observed = {
      'row1.c': facts(rows.get(1).c),
      'row1.p': facts(rows.get(1).p),
      'row1.cm_key': facts(soleEntry(rows.get(1).cm)[0]),
      'row1.tm_key': facts(soleEntry(rows.get(1).tm)[0]),
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

    // The map VALUES, one per map column, also derived from this binding.
    const mapValues = reference.map_values;
    expect(soleEntry(rows.get(1).cm)[1]).toBe(mapValues['row1.cm_value']);
    expect(soleEntry(rows.get(1).tm)[1]).toBe(mapValues['row1.tm_value']);
    expect(soleEntry(rows.get(1).fcm)[1]).toBe(mapValues['row1.fcm_value']);
    expect(soleEntry(rows.get(1).ftm)[1]).toBe(mapValues['row1.ftm_value']);
    // ...and those four values must be PAIRWISE DISTINCT in the reference, which
    // is what makes the four assertions above discriminating. The four map
    // columns hold the SAME key by construction, so a case that read the wrong
    // column's cell -- exactly the confusion a multicell/frozen pair invites --
    // would pass unnoticed against equal values.
    const declaredMapValues = [
      mapValues['row1.cm_value'],
      mapValues['row1.tm_value'],
      mapValues['row1.fcm_value'],
      mapValues['row1.ftm_value'],
    ];
    expect(new Set(declaredMapValues).size).toBe(declaredMapValues.length);

    // Non-vacuity: the reference must actually carry the colliding subjects, or
    // an emptied/renamed file would let this pass having compared nothing. Both
    // collision classes are named: the `_type` field and the `__proto__` field.
    // `JSON.parse` defines `__proto__` as an ORDINARY OWN PROPERTY (it never
    // invokes a setter), so the reference really does carry the field and
    // `toEqual` above really does compare it.
    expect(expected['row1.c'].fields._type).toBe('user-supplied-type');
    expect(ownField(expected['row1.c'].fields, '__proto__')).toBe('user-supplied-proto');
    expect(expected['row1.c'].typeName).toBe('collide');
    // ...and the reference states the CROSS-DECODE-PATH identity in its own
    // right: the multicell key facts EQUAL the frozen ones, which is #3612's
    // property (a caller cannot tell the two spellings of one map apart). Stated
    // here so the committed FILE remains a valid control on its own -- the
    // per-binding case that measures this within one binding lives above, and
    // this file is what compares the two bindings.
    expect(expected['row1.cm_key']).toEqual(expected['row1.fcm_key']);
    expect(expected['row1.tm_key']).toEqual(expected['row1.ftm_key']);
  });

  // ==========================================================================
  // AC5 — the committed fixture resolves CHECKOUT-RELATIVE, never through
  // CQLITE_DATASETS_ROOT (#3131/#3148; issue #3724 AC5)
  // ==========================================================================

  test('the fixture and the parity reference resolve checkout-relative, not via the datasets-root env var', () => {
    // The file docstring and the reference's `note_on_paths` DOCUMENT this
    // contract; nothing ASSERTED it. `assertFixturePresent` cannot: it checks
    // existence at the ALREADY-RESOLVED path, so it would pass unchanged if
    // resolution became env-routed and the env root happened to hold the file.
    // MEASURED: with `FIXTURE_ROOT` re-anchored on the env-routed
    // `TEST_DATA_ROOT` and `CQLITE_DATASETS_ROOT` pointed at a symlink to the
    // checkout's `test-data`, all 13 other tests here — the guard and the
    // cross-binding parity case included — stay GREEN and only this one reds.
    const { spawnSync } = require('child_process');
    const os = require('os');

    // The AMBIENT value, appended to every failure below. Half 1's discriminating
    // power depends on it: SET (as every gate run has it) and an env-routed
    // resolution reds on the equality alone; UNSET (the usual local run) and only
    // Half 2 can see it. A maintainer reading a failure needs to know which run
    // they are looking at, and neither state is the "right" one to run under.
    const ambientNote =
      `ambient CQLITE_DATASETS_ROOT: ` + // CONTROL: names the variable under test, diagnostic label only
      `${process.env.CQLITE_DATASETS_ROOT === undefined ? '(unset)' : process.env.CQLITE_DATASETS_ROOT}`; // CONTROL: the AMBIENT read, diagnostic only
    // jest matchers take no message argument, so the note is added by RETHROWING:
    // the matcher's own diff is preserved and the ambient value is appended to it.
    const withAmbient = (assertions) => {
      try {
        assertions();
      } catch (err) {
        err.message = `${err.message}\n${ambientNote}`;
        throw err;
      }
    };

    // Half 1 — AFFIRMATIVE EQUALITY against a `__dirname`-derived repo root. A
    // "the env value is not a prefix" check would go vacuous whenever the
    // variable is unset or coincidentally equals the checkout: a pass derived
    // from the absence of a bad signal.
    const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');
    const expectedRoot = path.join(REPO_ROOT, 'test-data', 'fixtures', 'issue_3504');
    const expectedFacts = path.join(expectedRoot, 'binding-parity-facts.json');
    withAmbient(() => {
      expect(FIXTURE_ROOT).toBe(expectedRoot);
      expect(PARITY_FACTS).toBe(expectedFacts);
    });
    // SCHEMA is pinned to the RESOLVED schemas dir, not to the checkout:
    // `CQLITE_SCHEMAS_ROOT` legitimately relocates that directory
    // (`setup.js:67-102`, the gate-validated #3148 contract), so pinning it to
    // the checkout would red a correct out-of-tree run. Only the FIXTURE corpus
    // and the parity facts are in AC5's scope.
    const expectedSchema = path.join(
      global.testPaths.SCHEMAS_DIR,
      'issue-3504-udt-collision.cql'
    );
    withAmbient(() => expect(SCHEMA).toBe(expectedSchema));

    // Half 2 — BEHAVIOURAL INVARIANCE, MEASURED IN A CHILD PROCESS.
    //
    // Module-level constants freeze at load, so reloading a NEIGHBOURING module
    // in-process cannot observe a resolution that reads the variable DIRECTLY
    // at load time: such a check stays green whenever the variable happened to
    // be unset when this file was first imported. Jest forbids re-requiring a
    // test file mid-run (its `describe` would re-register), so the probe is a
    // fresh `node` that stubs `describe` to a no-op, requires `setup.js` and
    // then THIS file, and reads the constants back off `module.exports`.
    //
    // It asserts a PAIR, because the invariant alone would be satisfiable by an
    // environment the child never saw:
    //   * the POSITIVE CONTROL — the child echoes the perturbed value back, and
    //     `SSTABLES_DIR`, whose documented contract IS to follow the variable
    //     (`setup.js:23-25`), HAS moved onto the bogus root;
    //   * the INVARIANT — the fixture root and parity-facts path are unmoved and
    //     checkout-derived, and the fixture still reads its three rows.
    //
    // Nothing in the parent process is mutated: no env var, no global, no module
    // registry. The pollution risk is removed rather than managed.
    const bogus = fs.mkdtempSync(path.join(os.tmpdir(), 'cqlite-3724-no-corpus-'));
    const outPath = path.join(
      fs.mkdtempSync(path.join(os.tmpdir(), 'cqlite-3724-probe-')),
      'probe.json'
    );
    try {
      expect(fs.readdirSync(bogus)).toEqual([]);

      const childEnv = { ...process.env, CQLITE_DATASETS_ROOT: bogus }; // CONTROL: the perturbation itself
      // Cleared for the CHILD only: `setup.js:246-251` makes a corpus-less root
      // a hard throw under either strict-fixture flag (as the gate's
      // node-bindings lane sets them), which would leave the probe unable to run
      // rather than able to measure.
      delete childEnv.CQLITE_REQUIRE_FIXTURES;
      delete childEnv.CQLITE_PARITY_REQUIRE_DATASETS;

      const probe = spawnSync(
        process.execPath,
        ['-e', RESOLUTION_PROBE, __dirname, __filename, outPath],
        { env: childEnv, encoding: 'utf8', timeout: PROBE_TIMEOUT_MS }
      );

      // AFFIRMATIVE COMPLETION ASSERT, before a single byte of the payload is
      // read. A timed-out, unspawnable or dead probe must fail NAMING that, and
      // must never fall through into comparing absent output against an
      // expected path: that either misleads (a "path mismatch" for a hang) or,
      // with no payload written at all, risks a comparison that passes having
      // measured nothing.
      const failure = probeCompletionFailure(probe, PROBE_TIMEOUT_MS, outPath);
      if (failure !== null) {
        throw new Error(failure);
      }
      const payload = JSON.parse(fs.readFileSync(outPath, 'utf8'));

      withAmbient(() => {
        // POSITIVE CONTROL — the perturbation really was in effect, and a
        // constant whose contract is to follow the variable really did move onto
        // it.
        expect(payload.envSeen).toBe(bogus);
        expect(payload.controlEnvRouted).toBe(path.join(bogus, 'sstables'));
        expect(payload.controlEnvRouted).not.toBe(global.testPaths.SSTABLES_DIR); // CONTROL

        // THE INVARIANT — unmoved, checkout-derived, and BOTH artifacts read.
        expect(payload.projectRoot).toBe(REPO_ROOT);
        expect(payload.fixtureRoot).toBe(expectedRoot);
        expect(payload.parityFacts).toBe(expectedFacts);
        // The schemas dir must not move either: it follows CQLITE_SCHEMAS_ROOT,
        // which the probe leaves exactly as this process has it.
        expect(payload.schemasDir).toBe(global.testPaths.SCHEMAS_DIR);
        expect(payload.schema).toBe(expectedSchema);
        // A read failure is REPORTED without a cause being claimed: the
        // `beforeAll` guard has already ruled out a broken checkout, but a
        // decoder regression or a lost force-added binary would look identical
        // here, and naming one would assert what this test has not established.
        expect(payload.readError).toBeNull();
        expect(payload.rowIds).toEqual([1, 2, 3]);
        // AC5 covers the parity-facts FILE as much as the corpus, so the probe
        // OPENED it rather than only recording its path. Non-vacuity: an emptied
        // or renamed reference would otherwise let the path equality above stand
        // in for a file nobody opened. Asserted NON-ZERO rather than at an exact
        // count, which is #3724's own subject to widen.
        expect(payload.parityFactsError).toBeNull();
        expect(payload.parityFactsUdts).toBeGreaterThan(0);
      });
    } finally {
      fs.rmSync(bogus, { recursive: true, force: true });
      fs.rmSync(path.dirname(outPath), { recursive: true, force: true });
    }
  });

  test('this file names no env-routed corpus constant outside its positive control', () => {
    // THE CLASS THIS CLOSES, WHICH THE ENVIRONMENT CASES ONLY SAMPLE. The test
    // above pins the CONSTANTS this module resolves today. A future test added to
    // this file that builds its OWN path from `setup.js`'s env-routed corpus
    // constants is invisible to it: that path would resolve through
    // `CQLITE_DATASETS_ROOT` while every assertion above stayed green, because
    // those assertions are about `FIXTURE_ROOT`/`PARITY_FACTS` and not about the
    // file's other consumers. This repository has ALREADY paid for exactly that
    // defect, in this very directory — `setup.js`'s round-10 note records
    // `write.test.js` and `write-smoke.test.js` building the schemas path
    // themselves and BYPASSING the resolver, so the variable was honoured by part
    // of the suite and ignored by the rest.
    //
    // Answered from THIS FILE'S OWN SOURCE. The needles are SPLIT, and here that
    // split is the MECHANISM rather than belt-and-braces: unlike the Python
    // sibling, which tokenizes and so can ignore strings by type, this scan is
    // textual, so an unsplit literal would match its own source and the test
    // could never pass.
    //
    // Two line classes are exempt, both deliberately:
    //   * a PURE COMMENT line (trimmed, starts with `//`, `*` or `/*`) — this
    //     file discusses the env-routed constants at length in prose, and prose
    //     is not a consumer;
    //   * a line carrying the `CONTROL` marker — the positive control legitimately
    //     READS the env-routed constant, in the test above and in the probe
    //     source. Requiring it to SAY so is what stops the exemption silently
    //     growing into a consumer.
    //
    // A SECOND NEEDLE, closing the cheap half of what this guard used to merely
    // declare (roborev #3724 round 4): the env VARIABLE NAME itself. A future test
    // that skips the corpus constants and reads `process.env.<var>` DIRECTLY names
    // none of the constants above, so the first needle cannot see it — but such a
    // read must contain the variable's name as a literal, and a literal in this
    // file's source plainly IS checkable. Same two exemptions, same count assert.
    //
    // DECLARED RESIDUAL, and it is what keeps this guard honest — the half a
    // source scan genuinely cannot reach is an INDIRECT read: a helper that
    // returns the value, a COMPUTED or concatenated variable name
    // (`process.env[someVar]`), or an alias bound to `process.env` names neither a
    // corpus constant nor the literal, so neither needle fires. That half stays
    // the child-process probe's job — it measures the RESOLVED paths under a
    // perturbed environment, whatever route a consumer took to read it, so the two
    // are complementary rather than overlapping and neither alone closes the class.
    // Deliberately NOT met with an AST or dataflow "environment read" detector: a
    // recogniser over author-controlled code accumulates false PASSes, and a guard
    // with known false PASSes is worse than no guard at all.
    const forbidden = ['SSTABLES' + '_DIR', 'TEST_DATA' + '_ROOT', 'DATASETS' + '_AVAILABLE'];
    const envVar = 'CQLITE_' + 'DATASETS_ROOT';
    const needles = [...forbidden, envVar];
    const source = fs.readFileSync(__filename, 'utf8');

    const offenders = [];
    source.split('\n').forEach((line, index) => {
      const trimmed = line.trim();
      if (trimmed.startsWith('//') || trimmed.startsWith('*') || trimmed.startsWith('/*')) {
        return;
      }
      if (line.includes('CONTROL')) {
        return;
      }
      for (const name of needles) {
        if (line.includes(name)) {
          offenders.push(`${index + 1}: ${trimmed}`);
          return;
        }
      }
    });

    expect(offenders).toEqual([]);

    // NON-VACUITY, and it is load-bearing: every needle is split, so a typo in a
    // split would silently make the scan look for a name that occurs nowhere and
    // the test would pass having checked nothing. The two known control lines
    // must therefore be FOUND — by the same `includes` the scan uses.
    const controlLinesFor = (names) =>
      source
        .split('\n')
        .filter((line) => line.includes('CONTROL') && names.some((n) => line.includes(n)));
    expect(controlLinesFor(forbidden).length).toBe(2);
    // The env-var needle's own control set, counted SEPARATELY: folding the two
    // into one total would let a typo in either split hide behind the other's
    // matches. MEASURED at 4 -- the ambient diagnostic's label and read, the
    // perturbation, and the probe's echo of it.
    expect(controlLinesFor([envVar]).length).toBe(4);
  });
});

// The child bound for the AC5 probe below. MEASURED, not picked: jest's resolved
// `testTimeout` here is 30000ms — `globalConfig.testTimeout` from
// `jest.config.js`, with BOTH project entries null, read via
// `npx jest --showConfig` on jest 29.7.0 — and this file sets no per-test
// override. `spawnSync` BLOCKS the event loop, so jest physically cannot
// interrupt it while it runs; the bound must therefore stay strictly BELOW
// jest's, so the call always returns first and jest's deadline remains the outer
// enforcing authority instead of being silently unenforceable. This is half of
// it, and ~300x the probe's measured warm runtime (~50ms), so a loaded box
// cannot flake on it. Do NOT raise jest's timeout to accommodate this bound —
// that inverts the fix.
const PROBE_TIMEOUT_MS = 15000;

/**
 * Why the probe did not complete, as a message naming the cause — or `null` when
 * it completed and left a payload.
 *
 * The states are the ones `spawnSync` actually reports, measured on this node
 * rather than assumed: a timeout is `error.code === 'ETIMEDOUT'` with
 * `status === null` and `signal === 'SIGTERM'`; an ordinary failure is a numeric
 * non-zero `status` with no `error`; and an exit-0 child that wrote nothing is
 * `status === 0`, which only the payload check below can catch.
 *
 * @param {import('child_process').SpawnSyncReturns<string>} result
 * @param {number} timeoutMs
 * @param {string} outPath
 * @returns {string|null}
 */
function probeCompletionFailure(result, timeoutMs, outPath) {
  const detail =
    `--- child stdout ---\n${result.stdout === undefined ? '(none)' : result.stdout}\n` +
    `--- child stderr ---\n${result.stderr === undefined ? '(none)' : result.stderr}`;
  if (result.error && result.error.code === 'ETIMEDOUT') {
    return (
      `resolution probe TIMED OUT after ${timeoutMs}ms (spawnSync ETIMEDOUT, killed with ` +
      `${String(result.signal)}). It normally completes in well under a second, so this is a ` +
      `hang, not a slow box — nothing about path resolution was measured.\n${detail}`
    );
  }
  if (result.error) {
    return (
      `resolution probe could not be spawned: ` +
      `${result.error.code || result.error.message} (bound ${timeoutMs}ms)\n${detail}`
    );
  }
  if (result.signal !== null && result.signal !== undefined) {
    return (
      `resolution probe was KILLED by ${String(result.signal)} (bound ${timeoutMs}ms)\n${detail}`
    );
  }
  if (typeof result.status !== 'number') {
    return (
      `resolution probe did not exit normally (status=${String(result.status)}, ` +
      `signal=${String(result.signal)}, bound ${timeoutMs}ms)\n${detail}`
    );
  }
  if (result.status !== 0) {
    return `resolution probe exited ${result.status} (bound ${timeoutMs}ms)\n${detail}`;
  }
  if (!fs.existsSync(outPath)) {
    return (
      `resolution probe exited 0 but wrote no payload to ${outPath} — nothing was ` +
      `measured, so no path comparison below would mean anything\n${detail}`
    );
  }
  return null;
}

// The child-process probe for the AC5 behavioural half above. Run as
// `node -e <this> <testsDir> <thisFile> <outPath>` with a perturbed
// `CQLITE_DATASETS_ROOT`, it re-resolves `setup.js` AND this module from scratch
// and records BOTH the paths this suite resolves and a path that legitimately
// DOES follow that variable, so the parent can prove the perturbation was in
// effect. `describe` is the only jest global it stubs: this file registers its
// suite at module scope, and with the callback never invoked no other one is
// reached.
const RESOLUTION_PROBE = `
global.describe = () => {};
const fsProbe = require('fs');
const pathProbe = require('path');
const [testsDir, modulePath, outPath] = process.argv.slice(1);
require(pathProbe.join(testsDir, 'setup.js'));
const mod = require(modulePath);
const payload = {
  // The POSITIVE CONTROL pair: what the child actually saw, and a constant
  // whose documented contract IS to follow it.
  envSeen: process.env.CQLITE_DATASETS_ROOT, // CONTROL: the probe echoes the perturbed value back
  controlEnvRouted: global.testPaths.SSTABLES_DIR, // CONTROL: env-routed BY CONTRACT, never a consumer
  // The INVARIANT: this suite's own resolved constants.
  projectRoot: global.testPaths.PROJECT_ROOT,
  schemasDir: global.testPaths.SCHEMAS_DIR,
  fixtureRoot: mod.FIXTURE_ROOT,
  parityFacts: mod.PARITY_FACTS,
  schema: mod.SCHEMA,
  rowIds: null,
  readError: null,
  parityFactsUdts: null,
  parityFactsError: null,
};

// AC5 covers the parity-facts FILE as much as the corpus, so the probe OPENS it
// rather than only recording its path — otherwise that half is path-equality
// only while the corpus half is path-equality PLUS a read-back. Synchronous, so
// it is recorded before the payload is written below.
try {
  payload.parityFactsUdts = Object.keys(
    JSON.parse(fsProbe.readFileSync(mod.PARITY_FACTS, 'utf8')).udts
  ).length;
} catch (err) {
  payload.parityFactsError = String((err && err.stack) || err);
}
// The read is attempted AFTER the paths are recorded, and its failure is
// REPORTED rather than thrown: an env-routed resolution makes the open fail, and
// the parent must be able to name the path mismatch that caused it instead of
// reporting only a dead child.
(async () => {
  try {
    const { Database } = require(pathProbe.join(testsDir, '..', 'lib', 'index.js'));
    const db = await Database.open(mod.FIXTURE_ROOT, { schema: mod.SCHEMA });
    try {
      const result = await db.executeNative(mod.QUERY);
      payload.rowIds = result.rows.map((row) => row.id).sort();
    } finally {
      await db.close();
    }
  } catch (err) {
    payload.readError = String((err && err.stack) || err);
  }
  fsProbe.writeFileSync(outPath, JSON.stringify(payload));
})();
`;

// Exported for that probe, and ONLY for it. Reading the constants off
// `module.exports` is what makes the probe measure THIS file's resolution rather
// than a re-derivation of it — a re-derivation would assert nothing about the
// constants the suite actually uses, which is precisely the vacuity this
// replaced. Harmless to jest: a test file may carry exports.
module.exports = { FIXTURE_ROOT, SCHEMA, PARITY_FACTS, QUERY };
