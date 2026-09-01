/**
 * Differential pin for the JS canonicalizer (issue #1455).
 *
 * Twin of `vectors.py`: materializes every vector in `canonical-vectors.json`
 * into the NODE binding's native value shape and checks it against the
 * vector's expected canonical output. Both halves must agree with the SAME
 * expected value, which is the only reason the two independent canonicalizers
 * are KNOWN to agree.
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

import { canonNode, parseType } from './canonical.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
export const VECTORS_PATH = path.join(HERE, 'canonical-vectors.json');

export function loadVectors(p = VECTORS_PATH) {
  return JSON.parse(fs.readFileSync(p, 'utf8')).vectors;
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

/**
 * Type-tagged shape, so `1` and `"1"` (or `1` and `true`) never compare equal
 * by accident -- the integer rule's whole point is WHICH JSON type a value
 * lands in.
 */
export function typedShape(v) {
  if (v === null) return 'null';
  if (typeof v === 'boolean') return 'bool';
  if (typeof v === 'number') return 'number';
  if (typeof v === 'string') return 'str';
  if (Array.isArray(v)) return `[${v.map(typedShape).join(',')}]`;
  if (typeof v === 'object') {
    return `{${Object.keys(v).sort().map((k) => `${k}:${typedShape(v[k])}`).join(',')}}`;
  }
  return typeof v;
}

export function deepEqual(a, b) {
  if (a === null || b === null) return a === b;
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
    return a.every((x, i) => deepEqual(x, b[i]));
  }
  if (typeof a === 'object' || typeof b === 'object') {
    if (typeof a !== 'object' || typeof b !== 'object') return false;
    const ka = Object.keys(a).sort();
    const kb = Object.keys(b).sort();
    if (ka.length !== kb.length || ka.some((k, i) => k !== kb[i])) return false;
    return ka.every((k) => deepEqual(a[k], b[k]));
  }
  return a === b;
}

/** Return a list of human-readable failures; empty means the JS leg agrees. */
export function checkVectors(vectors = loadVectors()) {
  const failures = [];
  for (const vec of vectors) {
    const t = parseType(vec.type);
    const expected = vec.canonical;
    let actual;
    try {
      actual = canonNode(materializeNode(vec.node), t);
    } catch (e) {
      failures.push(`${vec.name}/node: raised ${e && e.message ? e.message : e}`);
      continue;
    }
    if (!deepEqual(actual, expected) || typedShape(actual) !== typedShape(expected)) {
      failures.push(
        `${vec.name}/node: expected ${JSON.stringify(expected)} (${typedShape(expected)}), `
        + `got ${JSON.stringify(actual)} (${typedShape(actual)})`,
      );
    }
  }
  return failures;
}

function main() {
  const vectors = loadVectors();
  const failures = checkVectors(vectors);
  for (const line of failures) process.stderr.write(`FAIL ${line}\n`);
  process.stdout.write(`${vectors.length - failures.length}/${vectors.length} vectors OK (node leg)\n`);
  return failures.length ? 1 : 0;
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url))) {
  process.exitCode = main();
}
