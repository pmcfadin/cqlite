/**
 * Canonical JSON form for 3-way cross-binding parity (issue #1455) -- JS half.
 *
 * This is the twin of `canonical.py`. The two are INDEPENDENT implementations
 * of one written specification (`bindings/parity/README.md`) and are only
 * KNOWN to agree because `canonical-vectors.json` pins both against the same
 * expected output. Two implementations that merely look similar are not known
 * to agree; do not rely on care.
 *
 * Canonicalization is TYPE-DRIVEN from the DECLARED CQL type in
 * `fixtures.json`, never inferred from the value's runtime shape -- the
 * no-heuristics mandate (issue #28) applied to the harness itself.
 */

export class CanonicalError extends Error {}

/**
 * Integers outside JavaScript's exact-integer range canonicalize to a DECIMAL
 * STRING rather than a JSON number. Identical rule in canonical.py.
 */
export const JS_SAFE_INT_MAX = 9007199254740991n; // 2**53 - 1

/** Same bound as canonical.py's DECIMAL_PLAIN_MAX_CHARS. */
export const DECIMAL_PLAIN_MAX_CHARS = 4096;

const INT_KINDS = new Set(['tinyint', 'smallint', 'int', 'bigint', 'counter', 'varint']);
const TEXT_KINDS = new Set(['text', 'ascii', 'varchar']);
const FLOAT_KINDS = new Set(['float', 'double']);
const UUID_KINDS = new Set(['uuid', 'timeuuid']);
const OTHER_SCALARS = new Set([
  'boolean', 'blob', 'timestamp', 'date', 'time', 'duration', 'decimal', 'inet',
]);

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const DECIMAL_RE = /^(-?)(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/;

// ---------------------------------------------------------------------------
// CQL type strings
// ---------------------------------------------------------------------------

function tokenizeType(text) {
  const tokens = [];
  let current = '';
  for (const ch of text) {
    if (ch === '<' || ch === '>' || ch === ',') {
      if (current.trim()) tokens.push(current.trim());
      current = '';
      tokens.push(ch);
    } else {
      current += ch;
    }
  }
  if (current.trim()) tokens.push(current.trim());
  return tokens;
}

function parseTokens(tokens, i) {
  if (i >= tokens.length) throw new CanonicalError('truncated CQL type string');
  const name = tokens[i].toLowerCase();
  if (name === '<' || name === '>' || name === ',') {
    throw new CanonicalError(`unexpected token '${name}' where a type name was expected`);
  }
  i += 1;
  const args = [];
  if (i < tokens.length && tokens[i] === '<') {
    i += 1;
    for (;;) {
      const [node, next] = parseTokens(tokens, i);
      args.push(node);
      i = next;
      if (i >= tokens.length) throw new CanonicalError("unbalanced '<' in CQL type string");
      if (tokens[i] === ',') { i += 1; continue; }
      if (tokens[i] === '>') { i += 1; break; }
      throw new CanonicalError(`unexpected token '${tokens[i]}' in CQL type string`);
    }
  }
  if (name === 'frozen') {
    if (args.length !== 1) throw new CanonicalError('frozen<> takes exactly one argument');
    return [args[0], i];
  }
  return [{ kind: name, args }, i];
}

function validateArity(t) {
  if (t.kind === 'list' || t.kind === 'set') {
    if (t.args.length !== 1) throw new CanonicalError(`${t.kind}<> takes exactly one argument`);
  } else if (t.kind === 'map') {
    if (t.args.length !== 2) throw new CanonicalError('map<> takes exactly two arguments');
  } else if (t.kind === 'tuple') {
    if (t.args.length === 0) throw new CanonicalError('tuple<> takes at least one argument');
  } else if (t.args.length) {
    throw new CanonicalError(`type '${t.kind}' does not take type arguments`);
  } else if (!INT_KINDS.has(t.kind) && !TEXT_KINDS.has(t.kind)
             && !FLOAT_KINDS.has(t.kind) && !UUID_KINDS.has(t.kind)
             && !OTHER_SCALARS.has(t.kind)) {
    // DECLARED GAP: UDTs need their declared field types; inferring them from
    // the value would be the heuristic issue #28 forbids. Refuse loudly.
    throw new CanonicalError(
      `unsupported CQL type '${t.kind}' in the parity harness `
      + '(UDTs and unlisted scalars are a declared gap -- see bindings/parity/README.md)',
    );
  }
  for (const a of t.args) validateArity(a);
}

/** Parse a declared CQL type string into a type tree. `frozen<X>` is transparent. */
export function parseType(text) {
  const tokens = tokenizeType(text);
  if (!tokens.length) throw new CanonicalError('empty CQL type string');
  const [node, i] = parseTokens(tokens, 0);
  if (i !== tokens.length) throw new CanonicalError(`trailing tokens in CQL type string: ${text}`);
  validateArity(node);
  return node;
}

/**
 * Column name -> parsed CQL type. NULL-PROTOTYPE by necessity (issue #1455, F1).
 *
 * ONE builder for the row path, shared by `driver.mjs`'s `fixtureTypes` and by
 * the `rows` section of `canonical-vectors.json`, so the twin of
 * `types_from_columns` in canonical.py is pinned rather than assumed.
 *
 * `__proto__` is a legal CQL column name (quoted identifier; see
 * `test-data/schemas/issue-3630-row-collision.cql`). On an ordinary object
 * `types['__proto__'] = <CqlType>` runs `Object.prototype`'s inherited SETTER
 * and REPLACES the prototype -- no own property, no error -- so the column
 * would vanish from `Object.entries(types)` and never be canonicalized.
 */
export function typesFromColumns(columns) {
  const types = Object.create(null);
  for (const [name, text] of Object.entries(columns)) types[name] = parseType(text);
  return types;
}

export function renderType(t) {
  if (!t.args.length) return t.kind;
  return `${t.kind}<${t.args.map(renderType).join(', ')}>`;
}

// ---------------------------------------------------------------------------
// Total order over canonical values
// ---------------------------------------------------------------------------

function rank(v) {
  if (v === null) return 0;
  if (typeof v === 'boolean') return 1;
  if (typeof v === 'number') return 2;
  if (typeof v === 'string') return 3;
  if (Array.isArray(v)) return 4;
  if (typeof v === 'object') return 5;
  throw new CanonicalError(`not a canonical value: ${typeof v}`);
}

function cmp(a, b) {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

/**
 * Compare two strings by Unicode CODE POINT.
 *
 * JS `<` compares UTF-16 CODE UNITS, which orders astral characters (U+10000+)
 * BEFORE U+E000..U+FFFF; Python's `<` compares code points. Without this the
 * two canonicalizers would sort some sets differently -- a divergence that
 * would look like a binding bug.
 */
function compareStringsByCodePoint(a, b) {
  const ai = a[Symbol.iterator]();
  const bi = b[Symbol.iterator]();
  for (;;) {
    const x = ai.next();
    const y = bi.next();
    if (x.done && y.done) return 0;
    if (x.done) return -1;
    if (y.done) return 1;
    const cx = x.value.codePointAt(0);
    const cy = y.value.codePointAt(0);
    if (cx !== cy) return cx < cy ? -1 : 1;
  }
}

/** Total order over CANONICAL values, identical to canonical_compare() in Python. */
export function canonicalCompare(a, b) {
  const ra = rank(a);
  const rb = rank(b);
  if (ra !== rb) return cmp(ra, rb);
  if (ra === 0) return 0;
  if (ra === 1) return cmp(a ? 1 : 0, b ? 1 : 0);
  if (ra === 2) return cmp(a, b);
  if (ra === 3) return compareStringsByCodePoint(a, b);
  if (ra === 4) {
    const n = Math.min(a.length, b.length);
    for (let i = 0; i < n; i += 1) {
      const c = canonicalCompare(a[i], b[i]);
      if (c) return c;
    }
    return cmp(a.length, b.length);
  }
  const ka = Object.keys(a).slice().sort(compareStringsByCodePoint);
  const kb = Object.keys(b).slice().sort(compareStringsByCodePoint);
  const c = canonicalCompare(ka, kb);
  if (c) return c;
  for (const k of ka) {
    const cv = canonicalCompare(a[k], b[k]);
    if (cv) return cv;
  }
  return 0;
}

// ---------------------------------------------------------------------------
// Shared scalar canonicalizers
// ---------------------------------------------------------------------------

export function canonInt(n) {
  let big;
  if (typeof n === 'bigint') {
    big = n;
  } else if (typeof n === 'number') {
    if (!Number.isInteger(n)) throw new CanonicalError(`expected an integer, got ${n}`);
    big = BigInt(n);
  } else {
    throw new CanonicalError(`expected an integer, got ${typeof n}`);
  }
  const abs = big < 0n ? -big : big;
  return abs <= JS_SAFE_INT_MAX ? Number(big) : big.toString();
}

export function canonHex(bytes) {
  let out = '0x';
  for (const b of bytes) out += b.toString(16).padStart(2, '0');
  return out;
}

export function canonUuidStr(s) {
  const low = String(s).toLowerCase();
  if (!UUID_RE.test(low)) throw new CanonicalError(`not a hyphenated UUID: ${s}`);
  return low;
}

/** Canonical plain-decimal text, identical to normalize_decimal_string() in Python. */
export function normalizeDecimalString(s) {
  const text = String(s).trim();
  const m = DECIMAL_RE.exec(text);
  if (!m) throw new CanonicalError(`not a decimal string: ${s}`);
  const sign = m[1];
  const intPart = m[2];
  const fracPart = m[3] || '';
  const exp = m[4] === undefined ? 0 : parseInt(m[4], 10);
  let digits = intPart + fracPart;
  let point = intPart.length + exp;
  if (Math.abs(point) > DECIMAL_PLAIN_MAX_CHARS || digits.length > DECIMAL_PLAIN_MAX_CHARS) {
    const stripped = digits.replace(/^0+/, '') || '0';
    const scale = fracPart.length - exp;
    const body = `${sign}${stripped}`;
    return scale === 0 ? body : `${body}e${-scale}`;
  }
  if (point <= 0) {
    digits = '0'.repeat(1 - point) + digits;
    point = 1;
  } else if (point > digits.length) {
    digits += '0'.repeat(point - digits.length);
  }
  const whole = digits.slice(0, point).replace(/^0+/, '') || '0';
  const frac = digits.slice(point);
  const out = frac ? `${whole}.${frac}` : whole;
  return `${sign}${out}`;
}

/**
 * Type-tagged shape of a CANONICAL value -- ONE definition, every caller.
 *
 * There is no int/float distinction in JS, and there must not be one in the
 * Python twin either: `JSON.stringify({h: 1.0})` emits `{"h":1}`, so an
 * INTEGRAL double read back by `json.load` is a Python `int` while the python
 * and cli legs still hold a `float`. Tagging them differently would red this
 * lane on correct input (issue #1455, B4). `bool` is checked FIRST, so the
 * property the tag enforces -- number vs string vs bool vs null -- is intact.
 */
export function shapeTag(v) {
  if (v === null || v === undefined) return 'null';
  if (typeof v === 'boolean') return 'bool';
  if (typeof v === 'number') return 'number';
  if (typeof v === 'string') return 'str';
  if (Array.isArray(v)) return `[${v.map(shapeTag).join(',')}]`;
  if (typeof v === 'object') {
    return `{${Object.keys(v).sort().map((k) => `${k}:${shapeTag(v[k])}`).join(',')}}`;
  }
  return typeof v;
}

/** Deep equality over canonical values (shape tag compared separately). */
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

/** Canonical equality that ALSO compares the shape tag. */
export function canonicalEqual(a, b) {
  return deepEqual(a, b) && shapeTag(a) === shapeTag(b);
}

function canonDuration(months, days, nanos) {
  return { months: canonInt(months), days: canonInt(days), nanos: canonInt(nanos) };
}

// ---------------------------------------------------------------------------
// Node adapter
// ---------------------------------------------------------------------------

function isBytes(v) {
  return v instanceof Uint8Array || (typeof Buffer !== 'undefined' && Buffer.isBuffer(v));
}

const nodeAdapter = {
  name: 'node',
  asSeq(value) {
    if (Array.isArray(value)) return value;
    if (value instanceof Set) return Array.from(value);
    throw new CanonicalError(`expected an Array/Set, got ${Object.prototype.toString.call(value)}`);
  },
  asMap(value) {
    if (value instanceof Map) return Array.from(value.entries());
    throw new CanonicalError(`expected a Map, got ${Object.prototype.toString.call(value)}`);
  },
  scalar(value, kind) {
    if (kind === 'boolean') {
      if (typeof value !== 'boolean') throw new CanonicalError(`boolean column got ${typeof value}`);
      return value;
    }
    if (INT_KINDS.has(kind)) return canonInt(value);
    if (FLOAT_KINDS.has(kind)) {
      if (typeof value !== 'number') throw new CanonicalError(`${kind} column got ${typeof value}`);
      return value;
    }
    if (TEXT_KINDS.has(kind)) {
      if (typeof value !== 'string') throw new CanonicalError(`${kind} column got ${typeof value}`);
      return value;
    }
    if (kind === 'blob') {
      if (!isBytes(value)) throw new CanonicalError(`blob column got ${typeof value}`);
      return canonHex(value);
    }
    if (UUID_KINDS.has(kind)) {
      if (typeof value !== 'string') throw new CanonicalError(`${kind} column got ${typeof value}`);
      return canonUuidStr(value);
    }
    if (kind === 'timestamp') {
      if (!(value instanceof Date)) throw new CanonicalError('timestamp column is not a Date');
      const ms = value.getTime();
      if (!Number.isFinite(ms)) throw new CanonicalError('timestamp column is an Invalid Date');
      return canonInt(ms);
    }
    if (kind === 'date') {
      if (!(value instanceof Date)) throw new CanonicalError('date column is not a Date');
      const iso = value.toISOString();
      return iso.slice(0, iso.indexOf('T'));
    }
    if (kind === 'time') return canonInt(value);
    if (kind === 'duration') {
      if (value === null || typeof value !== 'object'
          || !('months' in value) || !('days' in value) || !('nanos' in value)) {
        throw new CanonicalError('duration column is not {months, days, nanos}');
      }
      return canonDuration(value.months, value.days, value.nanos);
    }
    if (kind === 'decimal') {
      if (typeof value !== 'string') throw new CanonicalError(`decimal column got ${typeof value}`);
      return normalizeDecimalString(value);
    }
    if (kind === 'inet') {
      if (typeof value !== 'string') throw new CanonicalError(`inet column got ${typeof value}`);
      return value;
    }
    throw new CanonicalError(`unsupported scalar kind '${kind}'`);
  },
};

// ---------------------------------------------------------------------------
// Type-driven walk
// ---------------------------------------------------------------------------

function canon(value, t, ad) {
  if (value === null || value === undefined) return null;
  const { kind } = t;
  if (kind === 'list') return ad.asSeq(value).map((x) => canon(x, t.args[0], ad));
  if (kind === 'set') {
    const items = ad.asSeq(value).map((x) => canon(x, t.args[0], ad));
    items.sort(canonicalCompare);
    return items;
  }
  if (kind === 'map') {
    const entries = ad.asMap(value).map(([k, v]) => [canon(k, t.args[0], ad), canon(v, t.args[1], ad)]);
    entries.sort((x, y) => canonicalCompare(x[0], y[0]));
    return entries;
  }
  if (kind === 'tuple') {
    // DECLARED GAP (README): a tuple canonicalizes to a PLAIN array, because
    // neither the Node binding nor the CLI can distinguish tuple from list.
    const items = ad.asSeq(value);
    if (items.length !== t.args.length) {
      throw new CanonicalError(
        `tuple arity mismatch: declared ${t.args.length}, value has ${items.length}`,
      );
    }
    return items.map((x, i) => canon(x, t.args[i], ad));
  }
  return ad.scalar(value, kind);
}

export function canonNode(value, t) {
  return canon(value, t, nodeAdapter);
}

/**
 * Canonicalize one Node row object against `types` (name -> CqlType).
 *
 * The result is a NULL-PROTOTYPE object, and that is load-bearing, not style
 * (issue #1455, F1). `__proto__` is a legal CQL column name -- expressible as
 * the quoted identifier `"__proto__"`, and this repository already has a
 * fixture schema for it (`test-data/schemas/issue-3630-row-collision.cql`) --
 * and on an ORDINARY object `out['__proto__'] = v` runs the inherited SETTER
 * on `Object.prototype` instead of creating an own property. It throws
 * nothing: the column simply VANISHES from `Object.keys(out)` and from the
 * emitted JSON, so this harness would report agreement about a column it had
 * silently dropped. The Node binding itself already defends the same way
 * (`bindings/node/src/value.rs` uses `Object.create(null)` for UDT fields and
 * JSON objects); the harness now follows suit.
 *
 * The READ side was already safe -- `hasOwnProperty.call` plus a `row[name]`
 * that resolves an OWN `__proto__` data property ahead of the prototype
 * accessor -- but it is only safe as long as it stays written that way.
 */
export function canonRowNode(row, types) {
  const out = Object.create(null);
  for (const [name, t] of Object.entries(types)) {
    const raw = Object.prototype.hasOwnProperty.call(row, name) ? row[name] : null;
    try {
      out[name] = canon(raw, t, nodeAdapter);
    } catch (e) {
      throw new CanonicalError(`[node] column '${name}' (${renderType(t)}): ${e.message}`);
    }
  }
  return out;
}

export { nodeAdapter };
