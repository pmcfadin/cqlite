/**
 * Differential pin for the JS canonicalizer (issue #1455).
 *
 * Twin of `vectors.py`: materializes every vector in `canonical-vectors.json`
 * into the NODE binding's native value shape and checks it against the
 * vector's expected canonical output; it also drives the `errors` table (each
 * case must RAISE) and enforces the shared `floor` block. Both halves read the
 * SAME file, which is the only reason the two independent canonicalizers are
 * KNOWN to agree.
 *
 * Note: this file `JSON.parse`s the vector table, so a `cli` field holding an
 * integer above 2^53 is read lossily here. That is harmless and deliberate --
 * the JS runner only ever consumes the `node` and `canonical` fields, and
 * every above-safe integer is expressed there as a {"$":"bigint"} tag or a
 * decimal STRING, never as a JSON number.
 *
 * Runnable standalone:
 *
 *     node bindings/parity/vectors.mjs
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  CanonicalError, canonNode, canonRowNode, canonicalEqual, parseType, shapeTag,
  typesFromColumns,
} from './canonical.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
export const VECTORS_PATH = path.join(HERE, 'canonical-vectors.json');

/** The leg this runner owns; the python and cli legs are vectors.py's job. */
export const LEGS = ['node'];

export function loadAll(p = VECTORS_PATH) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

export function loadVectors(p = VECTORS_PATH) {
  return loadAll(p).vectors;
}

/** Turn a vector's leg spec into a napi-binding-shaped native value. */
export function materializeNode(spec) {
  if (spec === null || typeof spec === 'boolean' || typeof spec === 'number'
      || typeof spec === 'string') {
    return spec;
  }
  if (Array.isArray(spec)) return spec.map(materializeNode);
  if (typeof spec !== 'object' || !('$' in spec)) {
    throw new Error(`untagged vector spec: ${JSON.stringify(spec)}`);
  }
  switch (spec.$) {
    // The Node binding surfaces uuid/decimal/inet as plain strings.
    case 'uuid': case 'decimal': case 'inet': return spec.v;
    // A PRESENT property holding `undefined` (issue #1455, F6). Node-only by
    // construction: JSON has no `undefined`, and Python has no analogue -- an
    // absent Python key is simply absent. Only refusal cases use it.
    case 'undefined': return undefined;
    case 'bytes': return Buffer.from(spec.hex, 'hex');
    case 'datetime': return new Date(spec.ms);
    case 'date': return new Date(`${spec.v}T00:00:00.000Z`);
    case 'bigint': return BigInt(spec.v);
    case 'duration': return {
      months: spec.months, days: spec.days, nanos: BigInt(spec.nanos),
    };
    // Node-only, REFUSAL cases only (issue #1455, F5): each field is
    // materialized individually, so a case can plant the wrong JS type on ONE
    // of them -- a bare JSON number stays a number, a {"$":"bigint"} tag
    // becomes a BigInt. The plain `duration` tag above always builds the
    // CORRECT shape, which is what the value vectors use.
    case 'duration_raw': {
      // Every key except `$` is materialized individually, so a case can plant
      // both a wrong FIELD TYPE (F5) and an EXTRA key (R6).
      const out = {};
      for (const [k, v] of Object.entries(spec)) {
        if (k !== '$') out[k] = materializeNode(v);
      }
      return out;
    }
    // R5: a bare Uint8Array where the binding always produces a Buffer.
    case 'uint8array': return Uint8Array.from(Buffer.from(spec.hex, 'hex'));
    // A tuple is indistinguishable from a list on this leg (DECLARED GAP).
    case 'list': case 'tuple': return spec.items.map(materializeNode);
    case 'set': return new Set(spec.items.map(materializeNode));
    case 'map': return new Map(spec.entries.map(([k, v]) => [materializeNode(k), materializeNode(v)]));
    case 'bytearray': case 'memoryview': case 'mutable_set': case 'duck_duration':
      throw new Error(`the \`${spec.$}\` vector tag is python-only`);
    default: throw new Error(`unknown vector tag: ${spec.$}`);
  }
}

// ---------------------------------------------------------------------------
// Value vectors
// ---------------------------------------------------------------------------

/**
 * Returns `{ failures, counts }`. `counts` is per (vector, leg) PAIR, not per
 * vector, so a vector failing on more than one leg cannot under-count (N7).
 */
export function checkVectors(vectors = loadVectors()) {
  const failures = [];
  const counts = {
    vectors: 0, checks: 0, ok: 0, failed: 0, skipped: 0,
  };
  for (const vec of vectors) {
    counts.vectors += 1;
    counts.checks += 1;
    if (!('node' in vec)) {
      // PRESENCE is required (issue #1455, F3): an absent leg key is a NAMED
      // refusal, never a silent skip that shrinks the subject set.
      failures.push(`${vec.name}/node: leg key 'node' is ABSENT from the vector`);
      counts.failed += 1;
      continue;
    }
    const expected = vec.canonical;
    let actual;
    try {
      // parseType is INSIDE the try (N6): a malformed `type` is THIS vector's
      // failure, never an abort that leaves every later vector unmeasured.
      const t = parseType(vec.type);
      actual = canonNode(materializeNode(vec.node), t);
    } catch (e) {
      failures.push(`${vec.name}/node: raised ${e && e.message ? e.message : e}`);
      counts.failed += 1;
      continue;
    }
    if (canonicalEqual(actual, expected)) {
      counts.ok += 1;
      continue;
    }
    failures.push(
      `${vec.name}/node: expected ${JSON.stringify(expected)} (${shapeTag(expected)}), `
      + `got ${JSON.stringify(actual)} (${shapeTag(actual)})`,
    );
    counts.failed += 1;
  }
  return { failures, counts };
}

// ---------------------------------------------------------------------------
// Row cases -- the ROW-BUILDING path (issue #1455, F1)
// ---------------------------------------------------------------------------

/**
 * A row object with the spec's keys VERBATIM.
 *
 * NULL-PROTOTYPE, like every column-name-keyed object in this harness: with an
 * ordinary `{}` the case's own `__proto__` key would set the prototype instead
 * of becoming an own property, and the case would then be testing nothing --
 * a fixture that quietly stops exercising its own hazard.
 */
export function materializeNodeRow(spec) {
  const row = Object.create(null);
  for (const [name, value] of Object.entries(spec)) row[name] = materializeNode(value);
  return row;
}

/**
 * Drive `typesFromColumns` + `canonRowNode` for whole ROWS.
 *
 * The value vectors never build a column-name-keyed object, so they cannot
 * reach this hazard: `__proto__` is a legal quoted CQL identifier and, on an
 * ordinary object, assigning it runs `Object.prototype`'s inherited setter --
 * no own property, no error, the column simply gone from the canonical row.
 */
export function checkRows(rows = loadAll().rows) {
  const failures = [];
  const counts = {
    rows: 0, checks: 0, ok: 0, failed: 0, skipped: 0,
  };
  for (const c of rows) {
    counts.rows += 1;
    counts.checks += 1;
    if (!('node' in c)) {
      // Same rule as the value vectors (F3).
      failures.push(`${c.name}/node: leg key 'node' is ABSENT from the row case`);
      counts.failed += 1;
      continue;
    }
    const expected = c.canonical;
    let actual;
    try {
      actual = canonRowNode(materializeNodeRow(c.node), typesFromColumns(c.columns));
    } catch (e) {
      failures.push(`${c.name}/node: raised ${e && e.message ? e.message : e}`);
      counts.failed += 1;
      continue;
    }
    const present = new Set(Object.keys(actual));
    const missing = Object.keys(c.columns).filter((col) => !present.has(col));
    if (missing.length) {
      failures.push(
        `${c.name}/node: canonical row LOST column(s) ${JSON.stringify(missing)} `
        + `(got keys ${JSON.stringify(Object.keys(actual).sort())})`,
      );
      counts.failed += 1;
      continue;
    }
    if (canonicalEqual(actual, expected)) { counts.ok += 1; continue; }
    failures.push(
      `${c.name}/node: expected ${JSON.stringify(expected)} (${shapeTag(expected)}), `
      + `got ${JSON.stringify(actual)} (${shapeTag(actual)})`,
    );
    counts.failed += 1;
  }
  return { failures, counts };
}

// ---------------------------------------------------------------------------
// Refusal cases (issue #1455, N3)
// ---------------------------------------------------------------------------

function expectRaise(fn, expect) {
  let result;
  try {
    result = fn();
  } catch (e) {
    if (!(e instanceof CanonicalError)) {
      return [false, `raised ${e && e.name ? e.name : 'Error'} (expected CanonicalError): ${e && e.message}`];
    }
    if (String(e.message).toLowerCase().includes(expect.toLowerCase())) return [true, ''];
    return [false, `raised CanonicalError but message lacks "${expect}": ${e.message}`];
  }
  return [false, `did NOT raise; returned ${JSON.stringify(result)}`];
}

export function checkErrors(errors = loadAll().errors) {
  const failures = [];
  const counts = {
    cases: 0, checks: 0, ok: 0, failed: 0, otherLeg: 0,
  };
  const record = (label, ok, detail) => {
    if (ok) counts.ok += 1;
    else { counts.failed += 1; failures.push(`${label}: ${detail}`); }
  };
  for (const c of errors) {
    counts.cases += 1;
    if (c.stage === 'parse_type') {
      counts.checks += 1;
      const [ok, detail] = expectRaise(() => parseType(c.type), c.expect);
      record(`${c.name}/parse_type`, ok, detail);
      continue;
    }
    for (const [leg, spec] of Object.entries(c.legs)) {
      if (!LEGS.includes(leg)) {
        // Owned by the OTHER runner (vectors.py). Counted, not dropped.
        counts.otherLeg += 1;
        continue;
      }
      counts.checks += 1;
      const [ok, detail] = expectRaise(
        () => canonNode(materializeNode(spec), parseType(c.type)),
        c.expect,
      );
      record(`${c.name}/${leg}`, ok, detail);
    }
  }
  return { failures, counts };
}

// ---------------------------------------------------------------------------
// Case floor (issue #1455, B2)
// ---------------------------------------------------------------------------

export const REQUIRED_FLOOR_KEYS = [
  'min_vectors', 'min_errors', 'min_rows', 'required_row_names', 'required_error_names',
  'required_kinds', 'require_nested_container', 'require_null_canonical',
];
export const ALL_LEGS = ['python', 'node', 'cli'];

/**
 * Every case must CARRY every field the runners read (issue #1455, F3).
 *
 * The class this closes: a `|| []` / `?? 0` / `.get(k, default)` read lets an
 * ABSENT field inherit the permissive branch, so a deleted leg, section or
 * floor key silently shrinks the subject set while both runners report green.
 * Every read in the checks is now a direct index; this is what turns the
 * resulting `undefined` into a message naming the case and the field.
 *
 * Twin of `check_schema` in vectors.py — deliberately the SAME required set,
 * asserted over the SAME file, so neither runner can drift into accepting a
 * shape the other rejects.
 */
export function checkSchema(data = loadAll()) {
  const failures = [];
  for (const section of ['floor', 'vectors', 'rows', 'errors']) {
    if (!(section in data)) failures.push(`canonical-vectors.json is missing the \`${section}\` section`);
  }
  if (failures.length) return failures;
  for (const key of REQUIRED_FLOOR_KEYS) {
    if (!(key in data.floor)) failures.push(`floor block is missing \`${key}\``);
  }
  for (const vec of data.vectors) {
    const label = vec.name || '<unnamed>';
    for (const key of ['name', 'type', 'canonical', ...ALL_LEGS]) {
      if (!(key in vec)) failures.push(`vector '${label}' is missing \`${key}\``);
    }
  }
  for (const c of data.rows) {
    const label = c.name || '<unnamed>';
    for (const key of ['name', 'columns', 'canonical', ...ALL_LEGS]) {
      if (!(key in c)) failures.push(`row case '${label}' is missing \`${key}\``);
    }
  }
  for (const c of data.errors) {
    const label = c.name || '<unnamed>';
    for (const key of ['name', 'stage', 'expect', 'type']) {
      if (!(key in c)) failures.push(`error case '${label}' is missing \`${key}\``);
    }
    if (c.stage === 'canonicalize') {
      if (!c.legs || typeof c.legs !== 'object' || !Object.keys(c.legs).length) {
        failures.push(
          `error case '${label}' is stage=canonicalize but carries no non-empty \`legs\` `
          + '(it would verify NOTHING and still count as a case)',
        );
      } else {
        const unknown = Object.keys(c.legs).filter((leg) => !ALL_LEGS.includes(leg));
        if (unknown.length) failures.push(`error case '${label}' names unknown leg(s) ${JSON.stringify(unknown)}`);
      }
    } else if (c.stage !== 'parse_type') {
      failures.push(`error case '${label}' has unknown stage ${JSON.stringify(c.stage)}`);
    }
  }
  return failures;
}

export function collectKinds(typeText) {
  const walk = (t) => [t.kind, ...t.args.flatMap(walk)];
  return walk(parseType(typeText));
}

/** The subject set must not be able to shrink to nothing and stay green. */
export function checkFloor(data = loadAll()) {
  const floor = data.floor;
  if (!floor || typeof floor !== 'object') {
    return ['canonical-vectors.json has no `floor` block — the case floor cannot be checked'];
  }
  // The SCHEMA must hold before any floor arithmetic: every read below is a
  // direct index precisely so an absent key cannot inherit a permissive
  // default (issue #1455, F3).
  const schemaFailures = checkSchema(data);
  if (schemaFailures.length) return schemaFailures;
  const failures = [];
  const vectors = data.vectors;
  const errors = data.errors;
  if (vectors.length < floor.min_vectors) {
    failures.push(`vector floor: ${vectors.length} < ${floor.min_vectors} — vectors were REMOVED`);
  }
  if (errors.length < floor.min_errors) {
    failures.push(`error-case floor: ${errors.length} < ${floor.min_errors} — cases were REMOVED`);
  }
  const rows = data.rows;
  if (rows.length < floor.min_rows) {
    failures.push(`row-case floor: ${rows.length} < ${floor.min_rows} — row cases were REMOVED`);
  }
  const rowNames = rows.map((r) => r.name);
  const missingRows = floor.required_row_names.filter((n) => !rowNames.includes(n));
  if (missingRows.length) failures.push(`required row case(s) absent: ${JSON.stringify(missingRows)}`);
  const errorNames = errors.map((c) => c.name);
  const missingErrors = floor.required_error_names.filter((n) => !errorNames.includes(n));
  if (missingErrors.length) {
    failures.push(`required strictness refusal case(s) absent: ${JSON.stringify(missingErrors)}`);
  }
  const names = vectors.map((v) => v.name);
  if (new Set(names).size !== names.length) failures.push('duplicate vector names');

  const seen = new Set();
  let nested = false;
  for (const vec of vectors) {
    let kinds;
    try {
      kinds = collectKinds(vec.type);
    } catch (e) {
      failures.push(`${vec.name}: unparseable type "${vec.type}": ${e.message}`);
      continue;
    }
    kinds.forEach((k) => seen.add(k));
    const containers = kinds.filter((k) => ['list', 'set', 'map', 'tuple'].includes(k));
    if (containers.length >= 2) nested = true;
  }
  const missing = floor.required_kinds.filter((k) => !seen.has(k));
  if (missing.length) failures.push(`no vector covers CQL kind(s): ${JSON.stringify(missing)}`);
  if (floor.require_nested_container && !nested) {
    failures.push('no vector nests a container inside a container');
  }
  if (floor.require_null_canonical && !vectors.some((v) => v.canonical === null)) {
    failures.push('no vector canonicalizes to null');
  }
  return failures;
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/**
 * Emit `[{name, canonical}]` for every vector THROUGH `JSON.stringify`.
 *
 * This is the only way the JS/Python pin can see the SERIALIZATION boundary
 * (issue #1455, B4): `checkVectors` compares an IN-MEMORY value, so it cannot
 * observe that `JSON.stringify({h: 1.0})` emits `{"h":1}` and that `json.load`
 * therefore hands Python an `int` where the python and cli legs hold a
 * `float`. The Python side re-reads this file and compares.
 */
export function emitCanonical(target, data = loadAll()) {
  const out = {
    vectors: data.vectors.map((vec) => ({
      name: vec.name,
      canonical: canonNode(materializeNode(vec.node), parseType(vec.type)),
    })),
    // Rows are emitted too, so the `__proto__` cases cross the SERIALIZATION
    // boundary as well as the in-memory one: a null-prototype object with an
    // own `__proto__` key serializes to `{"__proto__": ...}`, which json.loads
    // reads back as an own key. If either half of that ever stops holding, the
    // Python round-trip check is what says so.
    rows: data.rows.filter((c) => 'node' in c).map((c) => ({
      name: c.name,
      canonical: canonRowNode(materializeNodeRow(c.node), typesFromColumns(c.columns)),
    })),
  };
  fs.writeFileSync(target, `${JSON.stringify(out, null, 1)}\n`, 'utf8');
  return out.vectors.length + out.rows.length;
}

function main() {
  const emitAt = process.argv.indexOf('--emit');
  if (emitAt !== -1) {
    const target = process.argv[emitAt + 1];
    if (!target) {
      process.stderr.write('--emit requires a path\n');
      return 2;
    }
    const n = emitCanonical(target);
    process.stdout.write(`emitted ${n} canonical values -> ${target}\n`);
    return 0;
  }
  const data = loadAll();
  const schemaFailures = checkSchema(data);
  if (schemaFailures.length) {
    // Fail BEFORE the sweeps: every reader below indexes directly, so a
    // malformed file would throw rather than report (issue #1455, F3).
    for (const line of schemaFailures) process.stderr.write(`FAIL ${line}\n`);
    process.stdout.write(`schema: FAILED (${schemaFailures.length} RECOGNISED)\n`);
    return 1;
  }
  const floorFailures = checkFloor(data);
  const vec = checkVectors(data.vectors);
  const rows = checkRows(data.rows);
  const err = checkErrors(data.errors);
  for (const line of [...floorFailures, ...vec.failures, ...rows.failures, ...err.failures]) {
    process.stderr.write(`FAIL ${line}\n`);
  }
  // Counts are reported AFFIRMATIVELY -- "0 RECOGNISED", never a bare 0.
  process.stdout.write(
    `schema: OK (${data.vectors.length} vectors, ${data.rows.length} rows, `
    + `${data.errors.length} error cases, all required fields present)\n`,
  );
  process.stdout.write(
    `vectors: ${vec.counts.ok}/${vec.counts.checks} leg-checks OK over `
    + `${vec.counts.vectors} vectors (${vec.counts.skipped} RECOGNISED leg-skips) `
    + `[legs: ${LEGS.join(', ')}]\n`,
  );
  process.stdout.write(
    `refusals: ${err.counts.ok}/${err.counts.checks} leg-checks OK over ${err.counts.cases} cases `
    + `(${err.counts.otherLeg} RECOGNISED leg-checks owned by vectors.py)\n`,
  );
  process.stdout.write(
    `rows: ${rows.counts.ok}/${rows.counts.checks} leg-checks OK over `
    + `${rows.counts.rows} row cases (${rows.counts.skipped} RECOGNISED leg-skips)\n`,
  );
  process.stdout.write(`floor: ${floorFailures.length ? 'FAILED' : 'OK'}\n`);
  return (floorFailures.length || vec.failures.length || rows.failures.length
    || err.failures.length) ? 1 : 0;
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url))) {
  process.exitCode = main();
}
