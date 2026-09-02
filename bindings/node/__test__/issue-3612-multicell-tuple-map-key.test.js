/**
 * Issue #3612, AC 4 — THE NODE LEG for `m_tuple_udt`.
 *
 * AC 4 names one subject and four surfaces: a MULTICELL map whose key is a
 * `tuple<frozen<key_part>, int>` must read as a STRUCTURED TUPLE KEY through the
 * Rust core, the CLI, and BOTH bindings. Core, Python and the CLI are covered
 * elsewhere; this file is the Node half. The subject is
 * `test_nested_udt_keys.nested_udt_keys`'s
 * `m_tuple_udt map<frozen<tuple<frozen<key_part>, int>>, int>`, whose keys live in
 * the cell PATH because the map is non-frozen.
 *
 * WHAT IS DISCRIMINATING HERE. The addon renders `Value::Tuple` as an ARRAY,
 * `Value::Udt` as `{ typeName, keyspace, fields }` and `Value::Map` as a JS `Map`
 * (`src/value.rs`). Before #3612 this cell-path site returned `Value::Blob` for
 * every COMPOSITE key, which the addon renders as a Buffer — so
 * `Array.isArray(key)` is a TYPE discrimination that reds on the old behaviour,
 * not a formatting nicety.
 *
 * A JS `Map` keys by OBJECT IDENTITY, so two structurally-equal keys stay two
 * entries; the entry COUNT is therefore also meaningful here and is asserted
 * (a key collapse would show as a short map).
 *
 * ORACLE: the committed `sstabledump` golden (`*-Data.db.jsonl`), parsed at run
 * time — never what the binding emits (doctrine #3042). sstabledump renders a
 * composite cell path as a nested join: the tuple's components with `:`, the
 * inner UDT's fields with an escaped `\:`, and `\@` for a NULL field, so
 * `charlie\:3:8` is `tuple(key_part{label: "charlie", rank: 3}, 8)`. The parser
 * below asserts its own arity, so a change in sstabledump's escaping reds here
 * instead of silently mis-parsing.
 *
 * WHY THIS FILE RESOLVES ITS OWN CORPUS PATH. `global.testPaths.SSTABLES_DIR` is
 * an EITHER/OR on `CQLITE_DATASETS_ROOT` (`setup.js`), and the fleet-local root
 * that variable usually names is MEASURED not to carry `test_nested_udt_keys`
 * (issue #3220) — so reaching the fixture through it would yield ZERO rows,
 * silently, exactly where this has to run. The fixture is git-tracked under the
 * checkout, so it is resolved from `PROJECT_ROOT` with no environment variable
 * and NO SKIP PATH: absence is a broken checkout, not a skippable condition, and
 * this suite does not consult `DATASETS_AVAILABLE`. (Same reasoning, same shape,
 * as `issue-3504-udt-field-namespace.test.js`.)
 *
 * The SCHEMA is read through `global.testPaths.SCHEMAS_DIR`, which is the suite's
 * mirror of the gate's #3148 schemas-root contract — committed source, validated
 * by the gate, with an out-of-tree override the gate stamps.
 */

const fs = require('fs');
const path = require('path');
const { Database } = require('../lib/index.js');

const KEYSPACE = 'test_nested_udt_keys';
const TABLE = 'nested_udt_keys';
const COLUMN = 'm_tuple_udt';
const QUERY = `SELECT id, ${COLUMN} FROM ${KEYSPACE}.${TABLE}`;

/** The CHECKOUT's corpus root — deliberately not `SSTABLES_DIR`; see the header. */
const SSTABLES_ROOT = path.join(
  global.testPaths.PROJECT_ROOT,
  'test-data',
  'datasets',
  'sstables'
);
const SCHEMA = path.join(global.testPaths.SCHEMAS_DIR, 'nested-udt-keys.cql');

/**
 * The committed table directory, GLOBBED: a regeneration mints a fresh table
 * UUID, so a hardcoded path would rot. Fails closed, naming the artifact.
 */
function fixtureTableDir() {
  if (!fs.existsSync(SCHEMA)) {
    throw new Error(`committed schema missing: ${SCHEMA}`);
  }
  const keyspaceDir = path.join(SSTABLES_ROOT, KEYSPACE);
  const tables = fs.existsSync(keyspaceDir)
    ? fs.readdirSync(keyspaceDir).filter((name) => name.startsWith(`${TABLE}-`))
    : [];
  if (tables.length !== 1) {
    throw new Error(
      `expected exactly one ${TABLE}-* dir under ${keyspaceDir}, got ${JSON.stringify(tables)} — ` +
        `${KEYSPACE} is git-tracked, so this is a broken checkout, not a skip`
    );
  }
  const tableDir = path.join(keyspaceDir, tables[0]);
  const dataDb = fs.readdirSync(tableDir).filter((name) => name.endsWith('-Data.db'));
  if (dataDb.length === 0) {
    throw new Error(
      `no *-Data.db under ${tableDir} — the binaries are force-added; without them ` +
        'this suite would pass on zero rows'
    );
  }
  return tableDir;
}

/**
 * Decode one sstabledump composite cell path into `[label, rank, second]`, with
 * `null` for a NULL UDT field. `rank` and `second` are returned as NUMBERS so the
 * comparison is against the binding's own value domain.
 */
function parseGoldenPath(cellPath) {
  // Split on UNESCAPED ':' only; keep both bytes of an escape so `\:` cannot end
  // a component and `\@` survives for the null test below.
  const parts = [];
  let cur = '';
  for (let i = 0; i < cellPath.length; i += 1) {
    const c = cellPath[i];
    if (c === '\\') {
      cur += c;
      i += 1;
      if (i < cellPath.length) cur += cellPath[i];
    } else if (c === ':') {
      parts.push(cur);
      cur = '';
    } else {
      cur += c;
    }
  }
  parts.push(cur);
  if (parts.length !== 2) {
    throw new Error(
      `golden tuple path ${JSON.stringify(cellPath)} must render exactly 2 tuple components`
    );
  }
  const fields = parts[0].split('\\:');
  if (fields.length !== 2) {
    throw new Error(
      `golden UDT component ${JSON.stringify(parts[0])} must render exactly 2 fields`
    );
  }
  const unnull = (s) => (s === '\\@' ? null : s);
  const label = unnull(fields[0]);
  const rankText = unnull(fields[1]);
  const asInt = (text, what) => {
    if (!/^-?\d+$/.test(text)) {
      throw new Error(`golden ${what} ${JSON.stringify(text)} is not an int`);
    }
    return Number(text);
  };
  return [
    label,
    rankText === null ? null : asInt(rankText, 'rank'),
    asInt(parts[1], 'tuple component 2'),
  ];
}

/** `partition key -> the golden's m_tuple_udt keys`, as sorted comparison text. */
function goldenKeysByPk(tableDir) {
  const jsonl = fs.readdirSync(tableDir).filter((n) => n.endsWith('-Data.db.jsonl'));
  if (jsonl.length !== 1) {
    throw new Error(`expected one committed golden under ${tableDir}, got ${JSON.stringify(jsonl)}`);
  }
  const raw = fs.readFileSync(path.join(tableDir, jsonl[0]), 'utf8');
  const byPk = new Map();
  for (const line of raw.split('\n')) {
    if (!line.trim()) continue;
    const doc = JSON.parse(line);
    const pk = String(doc.partition.key[0]);
    for (const row of doc.rows || []) {
      for (const cell of row.cells || []) {
        if (cell.name !== COLUMN) continue;
        if (!cell.path || typeof cell.path[0] !== 'string') continue;
        if (!byPk.has(pk)) byPk.set(pk, []);
        byPk.get(pk).push(JSON.stringify(parseGoldenPath(cell.path[0])));
      }
    }
  }
  if (byPk.size === 0) {
    throw new Error(`the golden must carry ${COLUMN} entries; an empty expectation is vacuous`);
  }
  return byPk;
}

/**
 * Cross-realm `Map` test, matching the rest of this suite: `instanceof Map` is
 * FALSE here even for a genuine map, because jest runs this file in a sandboxed
 * VM context whose `Map` intrinsic differs from the one the addon reaches.
 */
const isMap = (value) =>
  value !== null &&
  value !== undefined &&
  typeof value === 'object' &&
  Object.prototype.toString.call(value) === '[object Map]';

/**
 * The AC-4 structural assertion for ONE rendered key, reduced to the golden's
 * comparison text. Everything asserted here reds on the pre-#3612 Buffer.
 */
function structuredKey(key) {
  expect(Array.isArray(key)).toBe(true); // a Buffer (the old opaque Blob) is not an Array
  expect(key).toHaveLength(2);
  const [udt, second] = key;
  expect(typeof udt).toBe('object');
  expect(udt).not.toBeNull();
  expect(udt.typeName).toBe('key_part');
  expect(udt.keyspace).toBe(KEYSPACE);
  // The field namespace: exactly the two declared fields, nothing injected
  // (issue #3504's shape, which this key inherits by going through the same
  // renderer). Sorted — emitted ORDER is #3504's subject, not AC 4's.
  expect(Object.keys(udt.fields).sort()).toEqual(['label', 'rank']);
  for (const name of ['label', 'rank']) {
    expect(Object.prototype.hasOwnProperty.call(udt.fields, name)).toBe(true);
  }
  const label = udt.fields.label;
  const rank = udt.fields.rank;
  expect(label === null || typeof label === 'string').toBe(true);
  expect(rank === null || typeof rank === 'number').toBe(true);
  expect(typeof second).toBe('number');
  return JSON.stringify([label, rank, second]);
}

describe('multicell tuple-borne map key reads structurally (issue #3612, AC 4)', () => {
  let db = null;
  let rowsById = null;
  let expected = null;

  beforeAll(async () => {
    expected = goldenKeysByPk(fixtureTableDir());
    db = await Database.open(SSTABLES_ROOT, { schema: SCHEMA });
    const result = await db.executeNative(QUERY);
    expect(Array.isArray(result.rows)).toBe(true);
    // Zero rows from a PRESENT committed fixture is a decode failure, never a skip.
    expect(result.rows.length).toBeGreaterThan(0);
    rowsById = new Map(result.rows.map((row) => [row.id, row]));
  });

  afterAll(async () => {
    if (db) {
      await db.close();
      db = null;
    }
  });

  test('every golden partition is present in the read', () => {
    for (const pk of expected.keys()) {
      expect(rowsById.has(Number(pk))).toBe(true);
    }
  });

  test(`${COLUMN} keys are structured tuples matching the sstabledump golden`, () => {
    let checked = 0;
    for (const [pk, want] of expected.entries()) {
      const row = rowsById.get(Number(pk));
      expect(row).toBeDefined();
      const map = row[COLUMN];
      expect(isMap(map)).toBe(true);
      // A JS Map keys by object identity, so structurally-equal keys stay
      // distinct: a short map here is a key COLLAPSE, not a duplicate.
      expect(map.size).toBe(want.length);
      const got = [...map.keys()].map((key) => structuredKey(key));
      expect(got.sort()).toEqual([...want].sort());
      checked += 1;
    }
    expect(checked).toBe(expected.size);
  });
});
