/**
 * Parity test utilities for validating Node.js bindings against sstabledump JSONL files.
 *
 * Issue #307: sstabledump Parity Tests
 *
 * Adapted from Python bindings (bindings/python/tests/test_parity.py) patterns:
 * - JSONL file discovery and parsing
 * - Type normalization for comparison
 * - Value equality with tolerance for floats and dates
 */

const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

// =============================================================================
// Caches for Performance (matching Python's @lru_cache pattern)
// =============================================================================

const MAX_CACHE_ENTRIES = 64; // Match Python's lru_cache maxsize
const rowCountCache = new Map();
const partitionsCache = new Map();

/**
 * Evict oldest entry from a Map if it exceeds the max size.
 *
 * @param {Map} cache - The cache Map to check
 */
function evictIfNeeded(cache) {
  if (cache.size >= MAX_CACHE_ENTRIES) {
    const firstKey = cache.keys().next().value;
    cache.delete(firstKey);
  }
}

// =============================================================================
// JSONL File Discovery
// =============================================================================

/**
 * The canonical shape of a Cassandra table directory: `<table>-<32 hex uuid>`.
 *
 * Hoisted here (issue #3493) because the two golden lookups below now apply it.
 * They previously selected a directory with a bare `startsWith(`${table}-`)`,
 * which accepts committed siblings this regex rejects — `<table>-wip`,
 * `<table>-extra-<uuid>` — so Jest could select a directory that
 * `check-dataset-manifest.sh` (which requires the UUID shape) never validated,
 * and the manifest would report the corpus complete while Jest read a broken
 * golden out of the sibling. Consumer and validator now share ONE rule.
 *
 * Measured across both corpus roots when this was tightened: the only non-UUID
 * table directory anywhere is `system/_paxos_repair_state`, which no lookup can
 * select (it has no `<table>-` prefix), so this narrows nothing that resolves today.
 */
const TABLE_DIR_RE = /^(.+)-[0-9a-f]{32}$/;

/**
 * True if `entryName` is the canonical table directory FOR EXACTLY `table`.
 *
 * Equality on the captured table name, not a prefix test: `startsWith` treats
 * `orders-extra-<uuid>` as a directory of table `orders`, while TABLE_DIR_RE
 * parses its table name as `orders-extra`. This mirrors the manifest's
 * `${base#"${table}-"}` + 32-hex-suffix test exactly.
 *
 * @param {string} entryName - Directory basename
 * @param {string} table - Logical table name
 * @returns {boolean}
 */
function isTableDirFor(entryName, table) {
  const m = TABLE_DIR_RE.exec(entryName);
  return m !== null && m[1] === table;
}

/**
 * Find the JSONL reference file for a given keyspace and table.
 * Tables have hash-suffixed directories: {table}-{hash}/nb-1-big-Data.db.jsonl
 *
 * Restricted to the COMMITTED corpus at TABLE granularity (#1319): an untracked
 * WIP `<table>-<uuid>/` dir reusing an existing committed table's logical name
 * is SKIPPED so the lookup never resolves a WIP golden in place of the
 * committed one. Falls back (git unavailable) to treating all discovered dirs
 * as committed, matching isCommittedTableDir().
 *
 * @param {string} keyspace - Keyspace name (e.g., "test_basic")
 * @param {string} table - Table name (e.g., "simple_table")
 * @returns {string|null} - Path to JSONL file or null if not found
 */
function findJsonlFile(keyspace, table) {
  const keyspaceDir = path.join(global.testPaths.SSTABLES_DIR, keyspace);

  if (!fs.existsSync(keyspaceDir)) {
    return null;
  }

  const entries = fs.readdirSync(keyspaceDir, { withFileTypes: true });

  for (const entry of entries) {
    if (
      entry.isDirectory() &&
      isTableDirFor(entry.name, table) &&
      isCommittedTableDir(keyspace, entry.name)
    ) {
      const jsonlFile = path.join(keyspaceDir, entry.name, 'nb-1-big-Data.db.jsonl');
      if (fs.existsSync(jsonlFile)) {
        return jsonlFile;
      }
    }
  }

  return null;
}

/**
 * Find the JSONL reference file for an oa-format table (Issue #656 VG4).
 * oa tables use oa-N-big-Data.db.jsonl naming instead of nb-1-big-Data.db.jsonl.
 *
 * Restricted to the COMMITTED corpus at TABLE granularity (#1319): an untracked
 * WIP `<table>-<uuid>/` dir reusing a committed table's logical name is SKIPPED
 * so the lookup never resolves a WIP golden.
 *
 * @param {string} keyspace - Keyspace name (e.g., "test_oa")
 * @param {string} table - Table name (e.g., "simple_table")
 * @returns {string|null} - Path to JSONL file or null if not found
 */
function findOaJsonlFile(keyspace, table) {
  const keyspaceDir = path.join(global.testPaths.SSTABLES_DIR, keyspace);

  if (!fs.existsSync(keyspaceDir)) {
    return null;
  }

  const entries = fs.readdirSync(keyspaceDir, { withFileTypes: true });

  for (const entry of entries) {
    if (
      entry.isDirectory() &&
      isTableDirFor(entry.name, table) &&
      isCommittedTableDir(keyspace, entry.name)
    ) {
      const tableDir = path.join(keyspaceDir, entry.name);
      // oa tables use oa-N-big-Data.db.jsonl naming
      const dirEntries = fs.readdirSync(tableDir);
      for (const fname of dirEntries) {
        if (/^oa-\d+-big-Data\.db\.jsonl$/.test(fname)) {
          const jsonlFile = path.join(tableDir, fname);
          if (fs.existsSync(jsonlFile)) {
            return jsonlFile;
          }
        }
      }
    }
  }

  return null;
}

// =============================================================================
// JSONL Parsing
// =============================================================================

/**
 * Parse a golden `liveness_info.expires_at` / cell `expires_at` ISO-8601 stamp
 * (e.g. "2025-10-07T01:12:06Z") to a millisecond epoch. Mirrors Python's
 * `_parse_golden_expires_at` (bindings/python/tests/test_parity.py). Returns
 * `null` when the value is missing or unparseable, so callers can treat "no
 * per-cell expiry" and "unparseable" identically to the Python helper.
 *
 * @param {*} expiresAt - ISO-8601 timestamp string (or anything else)
 * @returns {number|null} - Epoch milliseconds, or null if missing/unparseable
 */
function parseGoldenExpiresAt(expiresAt) {
  if (typeof expiresAt !== 'string') return null;
  const ms = new Date(expiresAt).getTime();
  return Number.isNaN(ms) ? null : ms;
}

/**
 * Count golden rows that are LIVE under Cassandra `SELECT` semantics NOW.
 * Results are cached for performance.
 *
 * This MUST stay in lockstep with the reader and the Python parity harness.
 * Issue #1741 / #1742: the read path applies partition/range-tombstone shadowing
 * and WALL-CLOCK TTL expiry (matching a Cassandra `SELECT`), so a TTL-expired row
 * is correctly HIDDEN from query results. The physical sstabledump golden does
 * NOT apply wall-clock TTL — it lists every on-disk row — so a raw physical count
 * over-counts once TTLs elapse. This function derives the expected LIVE count from
 * the same authoritative metadata the reader uses.
 *
 * Ported faithfully from Python `count_live_rows_in_jsonl`
 * (bindings/python/tests/test_parity.py); keep the two harnesses in lockstep on
 * any future reader-semantics change. Exclusions applied, in order:
 *   1. range-tombstone markers (`type != "row"`);
 *   2. row tombstones (`deletion_info` with no surviving cells);
 *   3. a row whose row-level `liveness_info` carries a `ttl` whose `expires_at`
 *      is at/before now, UNLESS a non-deleted cell keeps it alive — mirroring the
 *      reader's has_live_forever_data_cell aggregate (row_data.rs). A cell keeps
 *      the row visible when it is either:
 *        - explicitly still-live: it carries its own `expires_at` in the future; or
 *        - live-forever: it carries NO `expires_at` AND an own `tstamp` (written by
 *          a separate non-TTL mutation). A cell with neither shares the row
 *          liveness (USE_ROW_TTL) and expires with it. A collection `path` alone is
 *          NOT a live-forever signal (the golden cannot distinguish an inherited
 *          `default_time_to_live` element from a live-forever one, so keying off it
 *          would diverge from the reader — see the Python docstring for app_metrics).
 *
 * For a table with NO TTL metadata this is identical to the plain tombstone-only
 * count (nothing extra excluded), so non-TTL tables keep asserting exact physical
 * parity. `now` is captured once per call via the wall clock (same basis as the
 * reader's `new Date()`); the fixtures' expiries are far from the present, so no
 * one-second-boundary race is possible.
 *
 * @param {string} jsonlPath - Path to JSONL file
 * @returns {number} - Wall-clock-live row count
 */
function countRowsInJsonl(jsonlPath) {
  if (rowCountCache.has(jsonlPath)) {
    return rowCountCache.get(jsonlPath);
  }

  const content = fs.readFileSync(jsonlPath, 'utf8');
  const lines = content.split('\n');
  const now = Date.now(); // wall clock, same basis as the reader (new Date())
  let totalRows = 0;

  for (let lineNum = 0; lineNum < lines.length; lineNum++) {
    const line = lines[lineNum];
    if (!line.trim()) continue;

    try {
      const partition = JSON.parse(line);
      const rows = partition.rows || [];

      for (const row of rows) {
        // Exclude range_tombstone_bound entries and row-level tombstones (rows
        // with deletion_info but no cells). CQLite suppresses deleted rows from
        // query results (VG6, Issue #672).
        if (row.type !== 'row') continue;
        const cells = row.cells || [];
        if (row.deletion_info && cells.length === 0) continue;

        // TTL-aware liveness (Issue #1741, ported from Python
        // count_live_rows_in_jsonl). A row whose row-liveness TTL has elapsed
        // survives only if some non-deleted cell keeps it alive.
        const liveness = row.liveness_info || {};
        const rowExpiresAt = parseGoldenExpiresAt(liveness.expires_at);
        if (liveness.ttl && rowExpiresAt !== null && rowExpiresAt <= now) {
          let cellStillLive = false;
          for (const cell of cells) {
            // A cell/collection tombstone is not live data.
            if (cell.deletion_info) continue;
            const cellExp = parseGoldenExpiresAt(cell.expires_at);
            if (cellExp !== null) {
              // Explicit per-cell TTL: live iff still in the future.
              if (cellExp > now) {
                cellStillLive = true;
                break;
              }
              continue;
            }
            // No per-cell expiry -> live-forever ONLY if written by a separate
            // non-TTL mutation, which the golden marks with an own `tstamp`
            // distinct from the row liveness. A cell with neither `expires_at`
            // nor own `tstamp` inherits the elapsed row TTL (USE_ROW_TTL) and
            // does NOT keep the row alive.
            if (cell.tstamp != null) {
              cellStillLive = true;
              break;
            }
          }
          if (!cellStillLive) continue; // Every cell shadowed/expired: SELECT hides the row.
        }

        totalRows++;
      }
    } catch (error) {
      throw new Error(
        `Failed to parse JSONL at ${jsonlPath}:${lineNum + 1}: ${error.message}`
      );
    }
  }

  evictIfNeeded(rowCountCache);
  rowCountCache.set(jsonlPath, totalRows);
  return totalRows;
}

/**
 * Load all partitions from a JSONL file.
 * Results are cached for performance.
 *
 * @param {string} jsonlPath - Path to JSONL file
 * @returns {Array<Object>} - Array of partition objects
 */
function loadJsonlPartitions(jsonlPath) {
  if (partitionsCache.has(jsonlPath)) {
    return partitionsCache.get(jsonlPath);
  }

  const content = fs.readFileSync(jsonlPath, 'utf8');
  const lines = content.split('\n');
  const partitions = [];

  for (let lineNum = 0; lineNum < lines.length; lineNum++) {
    const line = lines[lineNum];
    if (!line.trim()) continue;

    try {
      partitions.push(JSON.parse(line));
    } catch (error) {
      throw new Error(
        `Failed to parse JSONL at ${jsonlPath}:${lineNum + 1}: ${error.message}`
      );
    }
  }

  evictIfNeeded(partitionsCache);
  partitionsCache.set(jsonlPath, partitions);
  return partitions;
}

/**
 * Extract all rows from JSONL partitions as a flat array with cells mapped to columns.
 *
 * @param {Array<Object>} partitions - Array of partition objects from loadJsonlPartitions
 * @returns {Array<Object>} - Array of row objects with column names as keys
 */
function extractRowsFromPartitions(partitions) {
  const rows = [];

  for (const partition of partitions) {
    const partitionKey = partition.partition?.key || [];

    for (const row of partition.rows || []) {
      if (row.type !== 'row') continue;

      const rowObj = {};

      // Add partition key values (assuming single partition key column for now)
      // The actual column name would need schema info, but for comparison we use 'id'
      if (partitionKey.length === 1) {
        rowObj._partition_key = partitionKey[0];
      } else if (partitionKey.length > 1) {
        rowObj._partition_key = partitionKey;
      }

      // Add clustering columns if present
      if (row.clustering) {
        rowObj._clustering = row.clustering;
      }

      // Add cell values
      for (const cell of row.cells || []) {
        if (cell.name && !('deletion_info' in cell) && !('path' in cell)) {
          rowObj[cell.name] = cell.value;
        }
      }

      rows.push(rowObj);
    }
  }

  return rows;
}

// =============================================================================
// Hex Encoding Utilities (Issue #343)
// =============================================================================

// Varint hex pattern: "0x{hex}" (without decimal: prefix)
const VARINT_HEX_PATTERN = /^0x[0-9a-f]+$/i;

// Decimal hex pattern: "decimal:{scale}:0x{hex}"
const DECIMAL_HEX_PATTERN = /^decimal:(\d+):0x([0-9a-f]+)$/i;

/**
 * Parse a varint hex string to BigInt.
 * Format: "0x{hex}" where hex is two's complement big-endian.
 *
 * @param {string} hexStr - Hex string like "0x7f" or "0xff"
 * @returns {bigint} - The parsed BigInt value
 */
function parseVarintHex(hexStr) {
  if (!VARINT_HEX_PATTERN.test(hexStr)) {
    throw new Error(`Invalid varint hex format: ${hexStr}`);
  }

  const hex = hexStr.slice(2); // Remove '0x'
  if (hex.length === 0) {
    return 0n;
  }

  const bytes = Buffer.from(hex, 'hex');

  // Check sign from high bit
  const isNegative = (bytes[0] & 0x80) !== 0;

  let value = 0n;
  for (const byte of bytes) {
    value = (value << 8n) | BigInt(byte);
  }

  // Sign extend if negative
  if (isNegative) {
    const signBits = ~((1n << BigInt(bytes.length * 8)) - 1n);
    value |= signBits;
  }

  return value;
}

/**
 * Parse a decimal hex string to a human-readable decimal string.
 * Format: "decimal:{scale}:0x{hex}"
 *
 * @param {string} decimalHex - Decimal hex string like "decimal:2:0x7b"
 * @returns {string} - Human-readable decimal like "1.23"
 */
function parseDecimalHex(decimalHex) {
  const match = DECIMAL_HEX_PATTERN.exec(decimalHex);
  if (!match) {
    throw new Error(`Invalid decimal hex format: ${decimalHex}`);
  }

  const scale = parseInt(match[1], 10);
  const hex = match[2];

  if (hex.length === 0) {
    return '0';
  }

  const bytes = Buffer.from(hex, 'hex');

  // Parse two's complement
  const isNegative = (bytes[0] & 0x80) !== 0;
  let value = 0n;
  for (const byte of bytes) {
    value = (value << 8n) | BigInt(byte);
  }
  if (isNegative) {
    const signBits = ~((1n << BigInt(bytes.length * 8)) - 1n);
    value |= signBits;
  }

  // Apply scale
  const absValue = value < 0n ? -value : value;
  const sign = value < 0n ? '-' : '';
  const digits = absValue.toString();

  if (scale === 0) {
    return sign + digits;
  } else if (scale > 0) {
    if (digits.length <= scale) {
      return sign + '0.' + '0'.repeat(scale - digits.length) + digits;
    } else {
      const splitPoint = digits.length - scale;
      return sign + digits.slice(0, splitPoint) + '.' + digits.slice(splitPoint);
    }
  } else {
    // Negative scale means multiply by 10^|scale|
    return sign + digits + 'e' + (-scale);
  }
}

/**
 * Check if a string is a varint hex encoding from execute().
 * Note: This only matches varint format, not general hex blobs.
 *
 * @param {string} value - String to check
 * @returns {boolean} - True if it's a varint hex string
 */
function isVarintHex(value) {
  return typeof value === 'string' &&
    VARINT_HEX_PATTERN.test(value) &&
    !value.includes(':');
}

/**
 * Check if a string is a decimal hex encoding from execute().
 *
 * @param {string} value - String to check
 * @returns {boolean} - True if it's a decimal hex string
 */
function isDecimalHex(value) {
  return typeof value === 'string' && DECIMAL_HEX_PATTERN.test(value);
}

// =============================================================================
// Type Normalization (matching Python's normalize_jsonl_value)
// =============================================================================

// UUID pattern: 8-4-4-4-12 hex chars
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// Timestamp patterns
const TIMESTAMP_PATTERN1 = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+Z?$/;
const TIMESTAMP_PATTERN2 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z?$/;

// Date-only pattern
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

// Time pattern
const TIME_PATTERN = /^\d{2}:\d{2}:\d{2}\.\d+$/;

// Duration pattern (e.g., "1mo2d3h4m5s6ns")
const DURATION_PATTERN = /^(\d+mo)?(\d+d)?(\d+h)?(\d+m)?(\d+s)?(\d+ns)?$/;

/**
 * Normalize a JSONL value to a comparable JavaScript type.
 * Handles UUIDs, timestamps, dates, hex blobs, and nested structures.
 *
 * @param {any} value - Value from JSONL file
 * @param {string} [columnName] - Optional column name for context
 * @returns {any} - Normalized value
 */
function normalizeJsonlValue(value, columnName) {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === 'boolean' || typeof value === 'number') {
    return value;
  }

  if (typeof value === 'string') {
    // Hex blob (0x...)
    if (value.startsWith('0x')) {
      return Buffer.from(value.slice(2), 'hex');
    }

    // UUID - keep as string for comparison
    if (UUID_PATTERN.test(value)) {
      return value.toLowerCase();
    }

    // Timestamp
    if (TIMESTAMP_PATTERN1.test(value) || TIMESTAMP_PATTERN2.test(value)) {
      // Normalize to ISO format and parse
      let normalized = value.replace(' ', 'T');
      if (!normalized.endsWith('Z')) {
        normalized += 'Z';
      }
      return new Date(normalized);
    }

    // Date only (keep as string for comparison)
    if (DATE_PATTERN.test(value)) {
      return value;
    }

    // Time (keep as string for comparison)
    if (TIME_PATTERN.test(value)) {
      return value;
    }

    // Duration (keep as string for comparison)
    if (DURATION_PATTERN.test(value)) {
      return value;
    }

    return value;
  }

  if (Array.isArray(value)) {
    return value.map((v) => normalizeJsonlValue(v, columnName));
  }

  if (typeof value === 'object') {
    const result = {};
    for (const [k, v] of Object.entries(value)) {
      result[k] = normalizeJsonlValue(v, k);
    }
    return result;
  }

  return value;
}

// =============================================================================
// Value Comparison (matching Python's values_equal)
// =============================================================================

/**
 * Compare two values with type-aware equality.
 * Handles:
 * - Null values
 * - Float comparison with tolerance
 * - Date comparison with 1ms tolerance
 * - Buffer/byte array comparison
 * - Set comparison (order-independent)
 * - Map comparison
 * - Recursive structure comparison
 *
 * @param {any} actual - Actual value from Node.js bindings
 * @param {any} expected - Expected value from JSONL (normalized)
 * @returns {boolean} - True if values are equal
 */
function valuesEqual(actual, expected) {
  // Null handling
  if (actual === null && expected === null) return true;
  if (actual === undefined && expected === null) return true;
  if (actual === null && expected === undefined) return true;
  if (actual === null || actual === undefined) return expected === null || expected === undefined;
  if (expected === null || expected === undefined) return false;

  // Buffer/Uint8Array comparison
  if (Buffer.isBuffer(actual) && Buffer.isBuffer(expected)) {
    return actual.equals(expected);
  }
  if (Buffer.isBuffer(actual) && typeof expected === 'string' && expected.startsWith('0x')) {
    return actual.equals(Buffer.from(expected.slice(2), 'hex'));
  }

  // BigInt comparison (issue #3505).
  //
  // These arms are already EXACT and deliberately stay that way: unlike Python's
  // `values_equal`, which coerced an int/float pair through `float()` and so
  // rounded the exact side down to the lossy side (the #3505 mask), Node never
  // coerces a bigint to a double here.  Do NOT "align" this with Python by
  // adding a tolerance -- the alignment goes the other way.
  //
  // The hardening #3505 DID need: `BigInt(x)` THROWS `RangeError` for a
  // non-integer / NaN / Infinity `number`, which crashed the harness instead of
  // reporting a mismatch.  A number that is not an integer can never equal a
  // bigint, so that case is `false`.
  //
  // The Node CEILING here is the ORACLE, not the binding, and it is documented
  // rather than coerced away: `JSON.parse` reads a golden's
  // `18446744073709551615` into an f64 (`18446744073709552000`) and the digits
  // are gone before `valuesEqual` is ever called.  JS has no lossless integer
  // JSON parse without a custom reviver, so a Node-side parity comparison above
  // 2**53 is limited by the harness's own JSON reader -- not by this function
  // and not by the binding, which returns an exact `BigInt`.
  if (typeof actual === 'bigint' && typeof expected === 'bigint') {
    return actual === expected;
  }
  if (typeof actual === 'bigint' && typeof expected === 'number') {
    if (!Number.isInteger(expected)) return false;
    return actual === BigInt(expected);
  }
  if (typeof actual === 'number' && typeof expected === 'bigint') {
    if (!Number.isInteger(actual)) return false;
    return BigInt(actual) === expected;
  }

  // Float comparison with tolerance
  if (typeof actual === 'number' && typeof expected === 'number') {
    if (actual === expected) return true;
    if (Number.isNaN(actual) && Number.isNaN(expected)) return true;
    // The tolerance formula below DEGENERATES on an infinite operand (issue
    // #3505): `Math.abs(actual - expected)` is `Infinity` and so is
    // `relTol * Math.max(|actual|, |expected|)`, leaving
    // `Infinity <= Infinity` -- which is `true`.  So every finite value
    // compared equal to infinity, and `+Infinity` compared equal to
    // `-Infinity`.  CQL `float`/`double` columns can legitimately hold
    // `Infinity`, so that masked a real mismatch.
    //
    // This MUST sit after the `actual === expected` branch above: two genuine
    // equal infinities ARE equal in IEEE-754 and that case is already answered
    // there.  By here the operands differ, and a differing pair with an
    // infinite member can never be within any finite tolerance.  (A NaN
    // operand is also non-finite; NaN-vs-NaN is answered above and
    // NaN-vs-anything-else correctly falls to `false` either way.)
    if (!Number.isFinite(actual) || !Number.isFinite(expected)) return false;

    const relTol = 1e-6;
    const absTol = 1e-9;
    return Math.abs(actual - expected) <= Math.max(
      relTol * Math.max(Math.abs(actual), Math.abs(expected)),
      absTol
    );
  }

  // Date comparison with 1ms tolerance (inclusive)
  if (actual instanceof Date && expected instanceof Date) {
    const diff = Math.abs(actual.getTime() - expected.getTime());
    return diff <= 1;
  }
  if (actual instanceof Date && typeof expected === 'string') {
    // Compare Date to timestamp string
    const expectedDate = new Date(expected.replace(' ', 'T'));
    if (!isNaN(expectedDate.getTime())) {
      const diff = Math.abs(actual.getTime() - expectedDate.getTime());
      return diff < 1;
    }
  }

  // Set comparison (order-independent)
  if (actual instanceof Set && (expected instanceof Set || Array.isArray(expected))) {
    const expectedSet = expected instanceof Set ? expected : new Set(expected);
    if (actual.size !== expectedSet.size) return false;
    for (const item of actual) {
      let found = false;
      for (const expItem of expectedSet) {
        if (valuesEqual(item, expItem)) {
          found = true;
          break;
        }
      }
      if (!found) return false;
    }
    return true;
  }

  // Map comparison
  if (actual instanceof Map && (expected instanceof Map || typeof expected === 'object')) {
    const expectedMap = expected instanceof Map ? expected : new Map(Object.entries(expected));
    if (actual.size !== expectedMap.size) return false;
    for (const [key, val] of actual) {
      if (!expectedMap.has(key)) return false;
      if (!valuesEqual(val, expectedMap.get(key))) return false;
    }
    return true;
  }

  // Array comparison
  if (Array.isArray(actual) && Array.isArray(expected)) {
    if (actual.length !== expected.length) return false;
    return actual.every((a, i) => valuesEqual(a, expected[i]));
  }

  // Object comparison
  if (typeof actual === 'object' && typeof expected === 'object' &&
      !Array.isArray(actual) && !Array.isArray(expected) &&
      !(actual instanceof Date) && !(expected instanceof Date) &&
      !(actual instanceof Set) && !(expected instanceof Set) &&
      !(actual instanceof Map) && !(expected instanceof Map)) {
    const actualKeys = Object.keys(actual).sort();
    const expectedKeys = Object.keys(expected).sort();
    if (actualKeys.length !== expectedKeys.length) return false;
    if (!actualKeys.every((k, i) => k === expectedKeys[i])) return false;
    return actualKeys.every((k) => valuesEqual(actual[k], expected[k]));
  }

  // String comparison (case-sensitive)
  if (typeof actual === 'string' && typeof expected === 'string') {
    // UUID comparison (case-insensitive)
    if (UUID_PATTERN.test(actual) && UUID_PATTERN.test(expected)) {
      return actual.toLowerCase() === expected.toLowerCase();
    }
    return actual === expected;
  }

  // Default strict equality
  return actual === expected;
}

/**
 * Format a value difference for error messages.
 *
 * @param {string} field - Field name
 * @param {any} actual - Actual value
 * @param {any} expected - Expected value
 * @returns {string} - Formatted difference message
 */
function formatDifference(field, actual, expected) {
  const actualStr = formatValue(actual);
  const expectedStr = formatValue(expected);
  return `${field}: got ${actualStr}, expected ${expectedStr}`;
}

/**
 * Format a value for display in error messages.
 *
 * @param {any} value - Value to format
 * @returns {string} - Formatted string
 */
function formatValue(value) {
  if (value === null || value === undefined) {
    return 'null';
  }
  if (Buffer.isBuffer(value)) {
    return `Buffer(${value.length} bytes)`;
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (value instanceof Set) {
    return `Set(${value.size})`;
  }
  if (value instanceof Map) {
    return `Map(${value.size})`;
  }
  if (typeof value === 'object') {
    return JSON.stringify(value).slice(0, 100);
  }
  return String(value);
}

// =============================================================================
// Test Table Definitions
// =============================================================================

// =============================================================================
// Dynamic corpus enumeration (Issue #1229)
//
// The table set is DISCOVERED by walking the committed corpus
// (sstables/<keyspace>/<table>-<uuid>/), NOT hand-typed. Based on directory
// structure (committed), independent of Data.db presence. The skip-set +
// rationale lives in test-data/corpus-coverage-policy.md and is mirrored in
// bindings/python/tests/corpus.py.
// =============================================================================

// (declaration hoisted above findJsonlFile — see the top-of-file definition)


/**
 * All `system*` keyspaces (system, system_auth, system_schema,
 * system_distributed, system_traces, system_views, ...) are Cassandra-internal
 * metadata, not user-data read-parity targets, and are excluded by PREFIX so
 * any future `system*` keyspace shipped in a dataset subset is auto-excluded
 * (#1229). See test-data/corpus-coverage-policy.md.
 */
function isSystemKeyspace(keyspace) {
  return keyspace.startsWith('system');
}

/**
 * Keyspaces intentionally excluded from the read-parity corpus by EXACT name
 * (reasons in policy doc). `system*` keyspaces are excluded separately by
 * prefix via isSystemKeyspace() — do not enumerate them here.
 */
const SKIP_KEYSPACES = {
  test_writeparity: 'write byte-parity fixtures (dedicated Rust parity tests)',
  test_compactionparity: 'compaction byte-parity fixtures (differential-compaction harness)',
  test_compactionparityudt: 'compaction-parity UDT fixtures (compaction harness; may be local-only)',
  test_signed_coll: 'signed set/map element-order byte-parity fixtures (dedicated Rust parity test issue_1295_*)',
  test_compaction_tombstone_ttl: 'tombstone/TTL compaction byte-parity fixtures (dedicated Rust parity test issue_1387_*)',
  test_comparator_order: 'inet/time multicell-collection element/key ORDERING fixture (dedicated Rust ordering test issue_3790_*)',
};

/**
 * Keyspaces discovered + listed in-scope but not executed through the
 * comprehensive row-count corpus. This set MUST be identical across
 * smoke-test-all-tables.sh, corpus.py (SKIP_PENDING_KEYSPACES), and
 * corpus-coverage-policy.md.
 */
const SKIP_PENDING_KEYSPACES = {
  test_deltas: 'binaries not in published dataset asset yet (issue #701)',
  test_tomb:
    'tombstone parity fixtures with valid zero-live-row partitions; validated by dedicated Rust tombstone/TTL parity tests, not the comprehensive row-count corpus',
  test_types:
    'CQL-type/schema-evolution parity fixtures with valid zero-live-row cases (deleted-counter shadowing); validated by dedicated Rust CQL-type parity tests, not the comprehensive row-count corpus',
};

/**
 * Explicit in-scope read-parity corpus (the documented list in
 * test-data/corpus-coverage-policy.md). AUTHORITATIVE classified set used by
 * unclassifiedKeyspaces() — NOT "everything not skipped", so a newly-committed
 * keyspace that nobody added here trips the integrity guard. Mirrors
 * corpus.py IN_SCOPE_KEYSPACES (includes the skip-pending keyspaces).
 */
const IN_SCOPE_KEYSPACES = {
  test_basic: 'simple-types read-parity corpus',
  test_collections: 'list/set/map read-parity corpus',
  test_timeseries: 'time-series read-parity corpus',
  test_wide_rows: 'wide-partition read-parity corpus',
  test_oa: 'Cassandra 5.0 oa-format read-parity corpus (#656)',
  test_da: 'BTI (da-format) read-parity corpus',
  test_big: 'large/wide-partition read-parity corpus',
  test_comp: 'compression read-parity corpus',
  test_tomb: 'tombstone read-parity corpus',
  test_types: 'extended CQL-type read-parity corpus',
  test_deltas: 'CDC-delta read-parity corpus (skip-pending, #701)',
  test_nested_udt_keys:
    'nested-UDT-in-a-hashable-position read-fidelity corpus (#3500): a UDT reached through a tuple or a nested collection inside a set element / map key. ENFORCED (not a skip): every partition has live rows',
};

/** Keyspaces this Node suite can EXECUTE queries against (have a schema map). */
const EXECUTABLE_KEYSPACES = ['test_basic', 'test_collections', 'test_timeseries', 'test_wide_rows'];

/** Discover all keyspace directory names under the sstables dir. */
function discoverKeyspaces() {
  const dir = global.testPaths.SSTABLES_DIR;
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir, { withFileTypes: true })
    .filter((e) => e.isDirectory() && !e.name.startsWith('.'))
    .map((e) => e.name)
    .sort();
}

/**
 * Discover table names (UUID suffix stripped) for one keyspace.
 *
 * Filtered to the COMMITTED corpus at TABLE granularity (#1319): an untracked
 * WIP `<table>-<uuid>/` dir (no git-tracked file at all) under an
 * already-tracked keyspace is IGNORED, not enumerated into ALL_TABLES/OA_TABLES.
 */
function discoverTables(keyspace) {
  const dir = path.join(global.testPaths.SSTABLES_DIR, keyspace);
  if (!fs.existsSync(dir)) return [];
  const tables = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const m = TABLE_DIR_RE.exec(entry.name);
    if (m && isCommittedTableDir(keyspace, entry.name)) tables.push(m[1]);
  }
  return tables.sort();
}

let _trackedTableDirsCache = null;

// The committed corpus is owned by THIS source tree (the repo that contains
// this harness + the corpus-coverage policy), NOT by whatever checkout
// CQLITE_DATASETS_ROOT points at. A concurrent session can commit WIP fixtures
// into a *different* checkout's index (e.g. the main repo the datasets root
// points at) while this branch has not adopted them yet; the classification
// guard must reflect what THIS branch considers committed. parity-utils.js
// lives at <repo>/bindings/node/__test__/parity-utils.js.
const SOURCE_TREE_SSTABLES = path.resolve(
  __dirname,
  '..',
  '..',
  '..',
  'test-data',
  'datasets',
  'sstables',
);

/**
 * `"<keyspace>/<table-dir>"` for each dir owning ANY git-tracked file (#1319/#1312).
 *
 * The classification/enforcement set is the COMMITTED corpus, NOT raw live-disk
 * enumeration: a table DIRECTORY counts as "committed" iff git tracks AT LEAST
 * ONE file under `<keyspace>/<table>-<uuid>/` — Data.db, TOC, Statistics, a
 * JSONL golden, ANYTHING. This deliberately does NOT require a tracked
 * `*-Data.db.jsonl` golden: a newly-committed table dir that ships SSTable
 * metadata but is (regressionly) MISSING its JSONL golden must still count as
 * committed so the coverage check can surface the missing golden and FAIL
 * LOUDLY (#1229), rather than be silently omitted as "uncommitted". The
 * separate golden-presence check (findJsonlFile / coverage tests) enforces that.
 *
 * This still ignores untracked WIP fixtures a concurrent session may have
 * dropped into the live CQLITE_DATASETS_ROOT — at either keyspace granularity
 * (a whole new keyspace, e.g. `test_signed_coll`, ZERO tracked files) OR table
 * granularity (a new untracked `<table>-<uuid>/` dir under an already-tracked
 * keyspace) — so neither gets enforced.
 *
 * Tracked-ness is measured against THIS source tree's
 * `test-data/datasets/sstables` (the repo that owns this harness + the policy),
 * NOT the live SSTABLES_DIR — the live datasets root may be a *different*
 * checkout whose index already contains a concurrent session's WIP. Single
 * `git ls-files` call (no pathspec — ALL tracked files), parsed into
 * `keyspace/table-dir` (first two segments).
 *
 * @returns {Set<string>} tracked `keyspace/table-dir` (empty if git unavailable)
 */
function gitTrackedTableDirs() {
  if (_trackedTableDirsCache) return _trackedTableDirsCache;
  const out = new Set();
  try {
    const stdout = execFileSync(
      'git',
      ['-C', SOURCE_TREE_SSTABLES, 'ls-files', '-z'],
      { encoding: 'buffer' },
    );
    for (const raw of stdout.toString('utf8').split('\0')) {
      if (!raw) continue;
      const parts = raw.split('/');
      if (parts.length >= 3 && parts[0] && parts[1]) out.add(`${parts[0]}/${parts[1]}`);
    }
  } catch (_err) {
    // git unavailable / not a work tree: fall back (empty => treat all as committed).
  }
  _trackedTableDirsCache = out;
  return out;
}

/**
 * Keyspaces with at least one git-tracked file under a table dir (#1319/#1312).
 *
 * Derived from the table-granular tracked set (gitTrackedTableDirs): a keyspace
 * is committed iff it owns at least one tracked table dir. Empty when git is
 * unavailable (callers then fall back to treating all as committed).
 *
 * @returns {Set<string>} tracked keyspace names (empty if git unavailable)
 */
function gitTrackedKeyspaces() {
  const out = new Set();
  for (const td of gitTrackedTableDirs()) out.add(td.split('/', 1)[0]);
  return out;
}

/**
 * True if `<keyspace>/<tableDirName>` owns ANY git-tracked file (#1319/#1312).
 *
 * "Committed" is decoupled from "has a JSONL golden": a committed dir missing
 * its golden must remain DISCOVERABLE so the coverage check fails loudly on the
 * missing golden (#1229), not be silently dropped here.
 *
 * Graceful fallback: if git reports NO tracked files (git unavailable / not a
 * work tree), every discovered table dir is treated as committed so the guard
 * is not silently neutered.
 *
 * @returns {boolean}
 */
function isCommittedTableDir(keyspace, tableDirName) {
  const tracked = gitTrackedTableDirs();
  if (tracked.size === 0) return true;
  return tracked.has(`${keyspace}/${tableDirName}`);
}

/**
 * Discovered keyspaces restricted to the COMMITTED (git-tracked) corpus (#1319).
 *
 * Untracked-on-disk WIP keyspaces are excluded — neither enforced nor flagged.
 * Untracked table dirs UNDER a tracked keyspace are filtered at table
 * granularity by discoverTables(). Graceful fallback: if git reports NO tracked
 * files (git unavailable / not a work tree), every discovered keyspace is
 * treated as committed so the guard is not silently neutered. In CI and local
 * dev `.git` is present.
 *
 * @returns {string[]} committed keyspace names
 */
function committedKeyspaces() {
  const discovered = discoverKeyspaces();
  const tracked = gitTrackedKeyspaces();
  if (tracked.size === 0) return discovered;
  return discovered.filter((k) => tracked.has(k));
}

/** In-scope keyspaces = committed minus the documented skip-set + system* (#1319). */
function inScopeKeyspaces() {
  return committedKeyspaces().filter((k) => !(k in SKIP_KEYSPACES) && !isSystemKeyspace(k));
}

/**
 * Discovered keyspaces classified into NONE of the explicit buckets.
 *
 * A keyspace is "classified" only if it appears in one of the explicit,
 * hand-maintained sets: IN_SCOPE_KEYSPACES (read-parity corpus, incl.
 * skip-pending) or SKIP_KEYSPACES (intentionally excluded). This is
 * deliberately NOT "discovered minus skip-set" (which can never be
 * unclassified by construction — the tautology #1229 exists to kill). A
 * newly-committed keyspace nobody added to either explicit set is returned
 * here so the classification test reds the suite instead of absorbing it.
 *
 * The guard enumerates the COMMITTED corpus (git-tracked goldens), NOT raw
 * live-disk enumeration (#1319): an untracked WIP keyspace a concurrent session
 * dropped into CQLITE_DATASETS_ROOT (e.g. `test_signed_coll`, goldens not yet
 * committed) is IGNORED. A genuinely-committed unclassified keyspace still reds.
 */
function unclassifiedKeyspaces() {
  const classified = new Set([
    ...Object.keys(IN_SCOPE_KEYSPACES),
    ...Object.keys(SKIP_KEYSPACES),
    ...Object.keys(SKIP_PENDING_KEYSPACES),
  ]);
  // system* keyspaces are classified by prefix (Cassandra-internal metadata),
  // not enumerated in any explicit set.
  return committedKeyspaces().filter((k) => !classified.has(k) && !isSystemKeyspace(k));
}

/**
 * Executable test tables organized by keyspace — DISCOVERED dynamically.
 * (The Node suite only runs queries against EXECUTABLE_KEYSPACES.)
 */
const ALL_TABLES = Object.fromEntries(
  EXECUTABLE_KEYSPACES.map((ks) => [ks, discoverTables(ks)]),
);

/**
 * oa-format test tables — discovered dynamically (Issue #656 VG4 / #1229).
 */
const OA_TABLES = discoverTables('test_oa');

/**
 * Known issues from Python parity tests that may also affect Node.js.
 * These tables have core library issues (not binding issues).
 */
const KNOWN_ISSUES = {
  'test_basic.static_columns_table': 'Static column duplication (200 vs 100 rows)',
  'test_collections.typed_collections_table': 'V5CompressedLegacy cell extraction failure',
  'test_collections.frozen_collections_table': 'Null byte parsing error in frozen collection data',
};

/**
 * Check if a table has a known issue.
 *
 * @param {string} keyspace - Keyspace name
 * @param {string} table - Table name
 * @returns {string|null} - Issue description or null
 */
function getKnownIssue(keyspace, table) {
  return KNOWN_ISSUES[`${keyspace}.${table}`] || null;
}

// =============================================================================
// Exports
// =============================================================================

module.exports = {
  // JSONL utilities
  isTableDirFor,
  findJsonlFile,
  findOaJsonlFile,
  countRowsInJsonl,
  loadJsonlPartitions,
  extractRowsFromPartitions,

  // Type normalization
  normalizeJsonlValue,

  // Value comparison
  valuesEqual,
  formatDifference,
  formatValue,

  // Hex encoding utilities (Issue #343)
  parseVarintHex,
  parseDecimalHex,
  isVarintHex,
  isDecimalHex,
  VARINT_HEX_PATTERN,
  DECIMAL_HEX_PATTERN,

  // Test tables (dynamically discovered — Issue #1229)
  ALL_TABLES,
  OA_TABLES,
  KNOWN_ISSUES,
  getKnownIssue,

  // Corpus enumeration (Issue #1229)
  SKIP_KEYSPACES,
  EXECUTABLE_KEYSPACES,
  isSystemKeyspace,
  discoverKeyspaces,
  gitTrackedTableDirs,
  gitTrackedKeyspaces,
  isCommittedTableDir,
  committedKeyspaces,
  discoverTables,
  inScopeKeyspaces,
  unclassifiedKeyspaces,
};
