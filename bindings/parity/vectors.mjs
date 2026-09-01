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
  CanonicalError, canonNode, canonicalEqual, parseType, shapeTag,
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
    case 'bytes': return Buffer.from(spec.hex, 'hex');
    case 'datetime': return new Date(spec.ms);
    case 'date': return new Date(`${spec.v}T00:00:00.000Z`);
    case 'bigint': return BigInt(spec.v);
    case 'duration': return {
      months: spec.months, days: spec.days, nanos: BigInt(spec.nanos),
    };
    // A tuple is indistinguishable from a list on this leg (DECLARED GAP).
    case 'list': case 'tuple': return spec.items.map(materializeNode);
    case 'set': return new Set(spec.items.map(materializeNode));
    case 'map': return new Map(spec.entries.map(([k, v]) => [materializeNode(k), materializeNode(v)]));
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

export function checkErrors(errors = loadAll().errors || []) {
  const failures = [];
  const counts = {
    cases: 0, checks: 0, ok: 0, failed: 0,
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
    for (const [leg, spec] of Object.entries(c.legs || {})) {
      if (!LEGS.includes(leg)) continue;
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
  const failures = [];
  const vectors = data.vectors || [];
  const errors = data.errors || [];
  if (vectors.length < floor.min_vectors) {
    failures.push(`vector floor: ${vectors.length} < ${floor.min_vectors} — vectors were REMOVED`);
  }
  if (errors.length < floor.min_errors) {
    failures.push(`error-case floor: ${errors.length} < ${floor.min_errors} — cases were REMOVED`);
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

function main() {
  const data = loadAll();
  const floorFailures = checkFloor(data);
  const vec = checkVectors(data.vectors);
  const err = checkErrors(data.errors || []);
  for (const line of [...floorFailures, ...vec.failures, ...err.failures]) {
    process.stderr.write(`FAIL ${line}\n`);
  }
  process.stdout.write(
    `vectors: ${vec.counts.ok}/${vec.counts.checks} leg-checks OK over `
    + `${vec.counts.vectors} vectors (${vec.counts.skipped} leg-checks skipped) `
    + `[legs: ${LEGS.join(', ')}]\n`,
  );
  process.stdout.write(
    `refusals: ${err.counts.ok}/${err.counts.checks} leg-checks OK over ${err.counts.cases} cases\n`,
  );
  process.stdout.write(`floor: ${floorFailures.length ? 'FAILED' : 'OK'}\n`);
  return (floorFailures.length || vec.failures.length || err.failures.length) ? 1 : 0;
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url))) {
  process.exitCode = main();
}
