/**
 * Node leg of the 3-way cross-binding parity harness (issue #1455).
 *
 * Runs each fixture's SELECT through the napi binding, canonicalizes every row
 * with `canonical.mjs`, and writes `out/node.<fixture>.json`.
 *
 * Runnable standalone:
 *
 *     node bindings/parity/driver.mjs --out-dir bindings/parity/out
 *
 * Exit status: 0 on success; non-zero when the datasets are present but a
 * query throws or returns ZERO rows. A 0-row pass over a present corpus is the
 * exact false-green this repository forbids, so it is an ERROR, never a skip.
 */

import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import fs from 'node:fs';
import path from 'node:path';

import { canonRowNode, typesFromColumns } from './canonical.mjs';

const require = createRequire(import.meta.url);

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '..', '..');
const FIXTURES_PATH = path.join(HERE, 'fixtures.json');
const DEFAULT_OUT_DIR = path.join(HERE, 'out');

// The binding is loaded by PATH (`../node/lib/index.js`), never by package
// name: the repo's Node tests do the same, because the package name resolves
// only from a published install.
const BINDING_PATH = path.join(REPO_ROOT, 'bindings', 'node', 'lib', 'index.js');

export function resolveDatasetsRoot(explicit) {
  const raw = explicit || process.env.CQLITE_DATASETS_ROOT;
  const root = raw ? path.resolve(raw) : path.join(REPO_ROOT, 'test-data', 'datasets');
  const candidate = path.join(root, 'sstables');
  return fs.existsSync(candidate) ? candidate : root;
}

export function loadFixtureFile(p = FIXTURES_PATH) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

export function loadFixtures(p = FIXTURES_PATH) {
  return loadFixtureFile(p).fixtures;
}

/** The fixture set must not be able to shrink to nothing and stay green (B2). */
export function checkFixtureFloor(data = loadFixtureFile()) {
  const floor = data.floor;
  if (!floor || typeof floor !== 'object') {
    return ['fixtures.json has no `floor` block — the case floor cannot be checked'];
  }
  const failures = [];
  // PRESENCE first (issue #1455, F3): a `|| []` here would let an absent
  // section inherit the permissive branch.
  for (const key of ['min_fixtures', 'required_names']) {
    if (!(key in floor)) failures.push(`fixtures.json floor block is missing \`${key}\``);
  }
  if (!('fixtures' in data)) failures.push('fixtures.json is missing the `fixtures` section');
  if (failures.length) return failures;
  const fixtures = data.fixtures;
  for (const fixture of fixtures) {
    const label = fixture.name || '<unnamed>';
    for (const key of ['name', 'keyspace', 'table', 'schema', 'query', 'columns',
      'known_divergence']) {
      if (!(key in fixture)) failures.push(`fixture '${label}' is missing \`${key}\``);
    }
    if (!fixture.columns || !Object.keys(fixture.columns).length) {
      failures.push(`fixture '${label}' declares no columns`);
    }
  }
  if (failures.length) return failures;
  const names = fixtures.map((f) => f.name);
  if (fixtures.length < floor.min_fixtures) {
    failures.push(`fixture floor: ${fixtures.length} < ${floor.min_fixtures} — fixtures were REMOVED`);
  }
  const missing = floor.required_names.filter((n) => !names.includes(n));
  if (missing.length) failures.push(`required fixture(s) absent: ${JSON.stringify(missing)}`);
  if (new Set(names).size !== names.length) failures.push('duplicate fixture names');
  return failures;
}

/**
 * Column name -> parsed CQL type.
 *
 * Delegates to `typesFromColumns` so there is ONE builder: it returns a
 * null-prototype object because `__proto__` is a legal CQL column name
 * (issue #1455, F1), and the `rows` section of `canonical-vectors.json` pins
 * that builder in BOTH languages.
 */
export function fixtureTypes(fixture) {
  return typesFromColumns(fixture.columns);
}

export async function runFixture(fixture, datasets) {
  const { Database } = require(BINDING_PATH);
  const schema = path.join(REPO_ROOT, fixture.schema);
  if (!fs.existsSync(schema)) throw new Error(`schema not found: ${schema}`);
  const types = fixtureTypes(fixture);
  const db = await Database.open(datasets, { schema });
  let rows;
  const observedSet = new Set();
  try {
    const result = await db.executeNative(fixture.query);
    rows = result.rows.map((row) => {
      // UNION over every row, never the last row's keys (issue #1455, B3).
      // This leg is the one that SKIPS a metadata column with no value
      // (bindings/node/src/row.rs:123-138), so a per-row assignment here would
      // be last-row-wins over a genuinely varying key set.
      Object.keys(row).forEach((k) => observedSet.add(k));
      return canonRowNode(row, types);
    });
  } finally {
    await db.close();
  }
  const observed = [...observedSet].sort();
  if (!rows.length) {
    throw new Error(
      `fixture '${fixture.name}' returned 0 rows from ${datasets} (query: ${fixture.query})`,
    );
  }
  return {
    fixture: fixture.name,
    leg: 'node',
    query: fixture.query,
    observed_columns: observed,
    rows,
  };
}

function parseArgs(argv) {
  const out = { outDir: DEFAULT_OUT_DIR, datasetsRoot: null, fixtures: [] };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--out-dir') { out.outDir = argv[++i]; } else if (a === '--datasets-root') {
      out.datasetsRoot = argv[++i];
    } else if (a === '--fixture') { out.fixtures.push(argv[++i]); } else {
      throw new Error(`unrecognized argument: ${a}`);
    }
  }
  return out;
}

/**
 * Stable JSON for the artifact: object keys are written in the order
 * `JSON.stringify` produces for a fresh object, which for canonical rows is
 * the DECLARED column order. The comparator reads the parsed JSON, never the
 * text, so byte-identical artifacts are a convenience, not a contract.
 */
function writeArtifact(target, payload) {
  fs.writeFileSync(target, `${JSON.stringify(payload, null, 1)}\n`, 'utf8');
}

async function main() {
  let args;
  try {
    args = parseArgs(process.argv.slice(2));
  } catch (e) {
    process.stderr.write(`${e.message}\n`);
    return 2;
  }
  const datasets = resolveDatasetsRoot(args.datasetsRoot);
  fs.mkdirSync(args.outDir, { recursive: true });

  const data = loadFixtureFile();
  const floorFailures = checkFixtureFloor(data);
  if (floorFailures.length) {
    for (const line of floorFailures) process.stderr.write(`FAIL ${line}\n`);
    return 2;
  }
  let fixtures = data.fixtures;
  if (args.fixtures.length) {
    const wanted = new Set(args.fixtures);
    fixtures = fixtures.filter((f) => wanted.has(f.name));
    const found = new Set(fixtures.map((f) => f.name));
    const missing = [...wanted].filter((n) => !found.has(n));
    if (missing.length) {
      process.stderr.write(`unknown fixture(s): ${JSON.stringify(missing)}\n`);
      return 2;
    }
  }
  if (!fixtures.length) {
    process.stderr.write('no fixtures selected\n');
    return 2;
  }

  let failures = 0;
  for (const fixture of fixtures) {
    try {
      // Sequential on purpose: one Database handle at a time keeps the memory
      // profile flat and makes a failure attributable to ONE fixture.
      // eslint-disable-next-line no-await-in-loop
      const payload = await runFixture(fixture, datasets);
      const target = path.join(args.outDir, `node.${fixture.name}.json`);
      writeArtifact(target, payload);
      process.stdout.write(`OK   ${fixture.name}: ${payload.rows.length} rows -> ${target}\n`);
    } catch (e) {
      process.stderr.write(`FAIL ${fixture.name}: ${e && e.message ? e.message : e}\n`);
      failures += 1;
    }
  }
  return failures ? 1 : 0;
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url))) {
  main().then((code) => { process.exitCode = code; }).catch((e) => {
    process.stderr.write(`FATAL: ${e && e.stack ? e.stack : e}\n`);
    process.exitCode = 1;
  });
}
