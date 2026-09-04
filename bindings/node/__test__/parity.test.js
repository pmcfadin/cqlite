/**
 * sstabledump Parity Tests for Node.js bindings.
 *
 * Issue #307: Validate Node.js binding output against sstabledump JSONL reference files.
 *
 * Test Tiers:
 * - Tier 1: Row count parity for the dynamically-discovered executable corpus
 * - Tier 2: Value parity for representative tables
 *
 * Issue #1229: the table set is enumerated from the committed corpus (no
 * hand-typed allowlist, no tautological count assertions). Skip-set + rationale
 * in test-data/corpus-coverage-policy.md.
 *
 * Adapted from Python bindings (bindings/python/tests/test_parity.py) patterns.
 */

const { Database } = require('../lib/index.js');
const { assertDatasetsAvailable } = require('./helpers.js');
const {
  findJsonlFile,
  findOaJsonlFile,
  countRowsInJsonl,
  loadJsonlPartitions,
  extractRowsFromPartitions,
  normalizeJsonlValue,
  valuesEqual,
  formatDifference,
  ALL_TABLES,
  OA_TABLES,
  getKnownIssue,
  inScopeKeyspaces,
  unclassifiedKeyspaces,
  isSystemKeyspace,
  SKIP_KEYSPACES,
} = require('./parity-utils.js');

// =============================================================================
// Guarded test.each registration (Issue #1229 round-2)
//
// Under Jest 29 an empty `test.each([])` is a COLLECTION-TIME error, which
// bypasses the intended graceful dataset-absent skip. `guardedTestEach` guards
// the dynamic registration:
//   * cases present            -> normal `test.each(cases)(...)`
//   * cases empty + no datasets -> a single `test.skip` placeholder
//   * cases empty + datasets present -> a single FAILING test (the disk-derived
//     enumeration is unexpectedly empty — a real bug, not a skip)
// This mirrors the Python fail-vs-skip distinction.
// =============================================================================

const fsForGuard = require('fs');

function datasetsPresent() {
  return fsForGuard.existsSync(global.testPaths.SSTABLES_DIR);
}

function guardedTestEach(label, cases, name, fn) {
  if (Array.isArray(cases) && cases.length > 0) {
    test.each(cases)(name, fn);
    return;
  }
  if (datasetsPresent()) {
    // Datasets are on disk but enumeration produced nothing: fail loudly.
    test(`${label} enumeration is non-empty`, () => {
      throw new Error(
        `${label}: datasets are present but the disk-derived corpus is empty; ` +
          'dynamic enumeration is broken (#1229)'
      );
    });
  } else {
    test.skip(`${label} (datasets absent — skipped)`, () => {});
  }
}

// =============================================================================
// Schema Mapping for Keyspaces
// =============================================================================

const KEYSPACE_SCHEMAS = {
  test_basic: global.testPaths.SCHEMA_BASIC_TYPES,
  test_collections: global.testPaths.SCHEMA_COLLECTIONS,
  test_timeseries: global.testPaths.SCHEMA_TIME_SERIES,
  test_wide_rows: global.testPaths.SCHEMA_WIDE_ROWS,
};

// =============================================================================
// Database Instances (module-scoped for performance)
// =============================================================================

let databases = {};

beforeAll(async () => {
  assertDatasetsAvailable();

  // Open databases for each keyspace
  for (const [keyspace, schemaPath] of Object.entries(KEYSPACE_SCHEMAS)) {
    databases[keyspace] = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: schemaPath,
    });
  }
});

afterAll(async () => {
  // Close all databases
  for (const db of Object.values(databases)) {
    if (db) {
      await db.close();
    }
  }
});

// =============================================================================
// Tier 1: Row Count Parity Tests
// =============================================================================

describe('Tier 1: Row Count Parity (Issue #307)', () => {
  describe('test_basic keyspace', () => {
    guardedTestEach('test_basic', ALL_TABLES.test_basic, '%s row count matches JSONL', async (table) => {
      const jsonlPath = findJsonlFile('test_basic', table);
      if (!jsonlPath) {
        console.log(`  Skipping ${table}: JSONL file not found`);
        return;
      }

      const knownIssue = getKnownIssue('test_basic', table);
      if (knownIssue) {
        console.log(`  Known issue for ${table}: ${knownIssue}`);
      }

      const expectedCount = countRowsInJsonl(jsonlPath);
      const db = databases.test_basic;
      const result = await db.execute(`SELECT * FROM test_basic.${table}`);

      if (knownIssue) {
        // Log actual vs expected but don't fail
        console.log(`  ${table}: got ${result.rowCount} rows, expected ${expectedCount} (known issue)`);
      } else {
        expect(result.rowCount).toBe(expectedCount);
        console.log(`  ${table}: ${result.rowCount} rows (match)`);
      }
    });
  });

  describe('test_collections keyspace', () => {
    guardedTestEach('test_collections', ALL_TABLES.test_collections, '%s row count matches JSONL', async (table) => {
      const jsonlPath = findJsonlFile('test_collections', table);
      if (!jsonlPath) {
        console.log(`  Skipping ${table}: JSONL file not found`);
        return;
      }

      const knownIssue = getKnownIssue('test_collections', table);
      if (knownIssue) {
        console.log(`  Known issue for ${table}: ${knownIssue}`);
      }

      const expectedCount = countRowsInJsonl(jsonlPath);
      const db = databases.test_collections;

      try {
        const result = await db.execute(`SELECT * FROM test_collections.${table}`);

        if (knownIssue) {
          console.log(`  ${table}: got ${result.rowCount} rows, expected ${expectedCount} (known issue)`);
        } else {
          expect(result.rowCount).toBe(expectedCount);
          console.log(`  ${table}: ${result.rowCount} rows (match)`);
        }
      } catch (error) {
        if (knownIssue) {
          console.log(`  ${table}: query failed with known issue - ${error.message.slice(0, 50)}`);
        } else {
          throw error;
        }
      }
    });
  });

  describe('test_timeseries keyspace', () => {
    guardedTestEach('test_timeseries', ALL_TABLES.test_timeseries, '%s row count matches JSONL', async (table) => {
      const jsonlPath = findJsonlFile('test_timeseries', table);
      if (!jsonlPath) {
        console.log(`  Skipping ${table}: JSONL file not found`);
        return;
      }

      const knownIssue = getKnownIssue('test_timeseries', table);
      if (knownIssue) {
        console.log(`  Known issue for ${table}: ${knownIssue}`);
      }

      const expectedCount = countRowsInJsonl(jsonlPath);
      const db = databases.test_timeseries;
      const result = await db.execute(`SELECT * FROM test_timeseries.${table}`);

      if (knownIssue) {
        console.log(`  ${table}: got ${result.rowCount} rows, expected ${expectedCount} (known issue)`);
      } else {
        expect(result.rowCount).toBe(expectedCount);
        console.log(`  ${table}: ${result.rowCount} rows (match)`);
      }
    });
  });

  describe('test_wide_rows keyspace', () => {
    guardedTestEach('test_wide_rows', ALL_TABLES.test_wide_rows, '%s row count matches JSONL', async (table) => {
      const jsonlPath = findJsonlFile('test_wide_rows', table);
      if (!jsonlPath) {
        console.log(`  Skipping ${table}: JSONL file not found`);
        return;
      }

      const knownIssue = getKnownIssue('test_wide_rows', table);
      if (knownIssue) {
        console.log(`  Known issue for ${table}: ${knownIssue}`);
      }

      const expectedCount = countRowsInJsonl(jsonlPath);
      const db = databases.test_wide_rows;
      const result = await db.execute(`SELECT * FROM test_wide_rows.${table}`);

      if (knownIssue) {
        console.log(`  ${table}: got ${result.rowCount} rows, expected ${expectedCount} (known issue)`);
      } else {
        expect(result.rowCount).toBe(expectedCount);
        console.log(`  ${table}: ${result.rowCount} rows (match)`);
      }
    });
  });
});

// =============================================================================
// Tier 2: Column and Type Validation Tests (Representative Tables)
// =============================================================================

describe('Tier 2: Column and Type Validation (Issue #307)', () => {
  /**
   * Helper to validate that a table returns expected columns with valid values.
   * This validates structure rather than exact value matching (which requires
   * complex partition key mapping).
   *
   * @param {string} keyspace - Keyspace name
   * @param {string} table - Table name
   * @param {string[]} expectedColumns - Expected column names
   */
  async function validateTableColumns(keyspace, table, expectedColumns) {
    const db = databases[keyspace];
    const result = await db.execute(`SELECT * FROM ${keyspace}.${table} LIMIT 10`);

    expect(result.rowCount).toBeGreaterThan(0);

    // Check that expected columns exist in results
    const firstRow = result.rows[0];
    const actualColumns = Object.keys(firstRow);

    const missingColumns = expectedColumns.filter((col) => !actualColumns.includes(col));
    if (missingColumns.length > 0) {
      console.log(`  ${table}: Missing columns: ${missingColumns.join(', ')}`);
    }

    // Validate that we have data for each expected column in at least one row
    let columnsWithData = new Set();
    for (const row of result.rows) {
      for (const col of expectedColumns) {
        if (row[col] !== null && row[col] !== undefined) {
          columnsWithData.add(col);
        }
      }
    }

    console.log(`  ${table}: ${columnsWithData.size}/${expectedColumns.length} columns have data`);
    expect(columnsWithData.size).toBeGreaterThan(0);

    // Log sample values for debugging
    const sampleRow = result.rows[0];
    const samples = expectedColumns.slice(0, 5).map((col) => {
      const val = sampleRow[col];
      if (val === null || val === undefined) return `${col}=null`;
      if (Buffer.isBuffer(val)) return `${col}=Buffer(${val.length})`;
      if (val instanceof Date) return `${col}=Date`;
      if (typeof val === 'object') return `${col}=Object`;
      return `${col}=${String(val).slice(0, 20)}`;
    });
    console.log(`    Sample: ${samples.join(', ')}`);
  }

  /**
   * Validate JSONL cells match the columns returned by query.
   *
   * @param {string} keyspace - Keyspace name
   * @param {string} table - Table name
   */
  async function validateJsonlCellCoverage(keyspace, table) {
    const jsonlPath = findJsonlFile(keyspace, table);
    if (!jsonlPath) {
      console.log(`  Skipping ${table}: JSONL file not found`);
      return;
    }

    const partitions = loadJsonlPartitions(jsonlPath);
    const db = databases[keyspace];
    const result = await db.execute(`SELECT * FROM ${keyspace}.${table} LIMIT 10`);

    // Get cell names from JSONL
    const jsonlCellNames = new Set();
    for (const partition of partitions.slice(0, 10)) {
      for (const row of partition.rows || []) {
        if (row.type !== 'row') continue;
        for (const cell of row.cells || []) {
          if (cell.name && !('deletion_info' in cell)) {
            jsonlCellNames.add(cell.name);
          }
        }
      }
    }

    // Get column names from query result
    const queryColumns = new Set(Object.keys(result.rows[0] || {}));

    // Check coverage
    const cellsNotInQuery = [...jsonlCellNames].filter((c) => !queryColumns.has(c));
    const columnsNotInJsonl = [...queryColumns].filter((c) => !jsonlCellNames.has(c));

    console.log(`  ${table}: JSONL cells: ${jsonlCellNames.size}, Query columns: ${queryColumns.size}`);

    if (cellsNotInQuery.length > 0) {
      console.log(`    JSONL cells not in query: ${cellsNotInQuery.join(', ')}`);
    }
    if (columnsNotInJsonl.length > 0) {
      // This is expected - partition/clustering keys are in query but not in JSONL cells
      console.log(`    Query columns not in JSONL cells (likely keys): ${columnsNotInJsonl.join(', ')}`);
    }

    // At least some overlap should exist
    const overlap = [...jsonlCellNames].filter((c) => queryColumns.has(c));
    expect(overlap.length).toBeGreaterThan(0);
  }

  test('test_basic.simple_table has expected columns', async () => {
    // simple_table has comprehensive type coverage
    const expectedColumns = ['id', 'active', 'age', 'name', 'email', 'created'];
    await validateTableColumns('test_basic', 'simple_table', expectedColumns);
    await validateJsonlCellCoverage('test_basic', 'simple_table');
  });

  test('test_collections.collection_table has expected columns', async () => {
    // collection_table tests list, set, map types
    const expectedColumns = ['id', 'tags', 'scores', 'metadata_map'];
    await validateTableColumns('test_collections', 'collection_table', expectedColumns);
    await validateJsonlCellCoverage('test_collections', 'collection_table');
  });

  test('test_timeseries.sensor_data has expected columns', async () => {
    // sensor_data tests time-series patterns with timestamps
    const expectedColumns = ['sensor_id', 'timestamp', 'temperature', 'humidity', 'status'];
    await validateTableColumns('test_timeseries', 'sensor_data', expectedColumns);
    await validateJsonlCellCoverage('test_timeseries', 'sensor_data');
  });

  test('test_wide_rows.wide_partition_table has expected columns', async () => {
    // wide_partition_table tests wide partitions with many clustering columns
    const expectedColumns = ['partition_id', 'clustering_col1', 'data_column'];
    await validateTableColumns('test_wide_rows', 'wide_partition_table', expectedColumns);
    await validateJsonlCellCoverage('test_wide_rows', 'wide_partition_table');
  });
});

// =============================================================================
// Summary Test
// =============================================================================

describe('Parity Summary (Issue #307 / dynamic enumeration #1229)', () => {
  test('Every discovered executable table has a JSONL reference file', () => {
    let missing = [];
    let found = 0;
    let total = 0;

    for (const [keyspace, tables] of Object.entries(ALL_TABLES)) {
      for (const table of tables) {
        total++;
        const jsonlPath = findJsonlFile(keyspace, table);
        if (jsonlPath) {
          found++;
        } else {
          missing.push(`${keyspace}.${table}`);
        }
      }
    }

    if (total === 0) {
      // No corpus discovered (CI without fetched datasets) — nothing to assert.
      console.log('  No executable tables discovered; skipping JSONL coverage check');
      return;
    }

    if (missing.length > 0) {
      console.log(`  Missing JSONL files: ${missing.join(', ')}`);
    }

    console.log(`  Found ${found}/${total} JSONL reference files (discovered, not hard-coded)`);
    // Every discovered executable table must have a golden — not a literal count.
    expect(found).toBe(total);
  });

  test('Every committed keyspace is classified (in-scope or documented skip-set)', () => {
    if (inScopeKeyspaces().length === 0) {
      console.log('  No keyspaces discovered; skipping classification check');
      return;
    }
    const unclassified = unclassifiedKeyspaces();
    if (unclassified.length > 0) {
      console.log(`  Unclassified keyspaces: ${unclassified.join(', ')}`);
    }
    // A newly-committed keyspace that nobody classified reds the suite instead
    // of being silently uncovered (replaces the old tautological toBe(33)).
    expect(unclassified).toEqual([]);
  });

  test('All system* keyspaces are excluded by prefix (not enumerated)', () => {
    // The prefix rule must cover every system* keyspace, including ones a
    // dataset subset ships beyond the hard-named three (#1229).
    for (const ks of [
      'system',
      'system_auth',
      'system_schema',
      'system_distributed',
      'system_traces',
      'system_views',
      'system_anything_future',
    ]) {
      expect(isSystemKeyspace(ks)).toBe(true);
    }
    // system* must NOT be enumerated in the exact-name skip-set (prefix-only).
    for (const k of Object.keys(SKIP_KEYSPACES)) {
      expect(isSystemKeyspace(k)).toBe(false);
    }
    // A non-system test keyspace is not caught by the prefix.
    expect(isSystemKeyspace('test_brandnew')).toBe(false);
  });

  test('A genuinely-unknown keyspace still trips the classification guard', () => {
    // The guard must NOT be neutered: a fake, never-classified keyspace name is
    // neither in any explicit bucket nor a system* keyspace, so the
    // classification logic must treat it as unclassified.
    const fake = 'test_brandnew';
    const classified =
      fake in SKIP_KEYSPACES || isSystemKeyspace(fake);
    expect(classified).toBe(false);
  });
});

// =============================================================================
// VG4 (Issue #656): OA Format Parity Tests
// =============================================================================

/**
 * Locate an OA JSONL golden, failing LOUDLY under strict fixtures (issue #3493).
 *
 * A missing golden used to be a bare `return`, so under CQLITE_REQUIRE_FIXTURES=1 -- which
 * the FULL gate now sets -- the OA parity assertions could all vanish and the gate still
 * report success (roborev round 16). OA tables are excluded from the ALL_TABLES
 * golden-coverage assertion, so nothing else would have noticed.
 *
 * Lenient runs keep the graceful skip: the goldens ship with the fetched corpus, so their
 * absence is a legitimate state when fixtures were never fetched.
 *
 * @param {string} keyspace
 * @param {string} table
 * @returns {string|null} Path to the golden, or null on a lenient run without one.
 * @throws {Error} When strict fixtures are required and the golden is absent.
 */
function requireOaGolden(keyspace, table) {
  const jsonlPath = findOaJsonlFile(keyspace, table);
  if (jsonlPath) return jsonlPath;
  if (global.REQUIRE_FIXTURES) {
    throw new Error(
      `No JSONL golden for ${keyspace}.${table}. CQLITE_REQUIRE_FIXTURES=1, so this is a ` +
      'failure rather than a skip: OA tables are outside the ALL_TABLES golden-coverage ' +
      'assertion, so a silent skip here drops the parity check with nothing objecting ' +
      '(issue #3493).'
    );
  }
  console.log(`  Skipping ${keyspace}.${table}: JSONL file not found`);
  return null;
}

/**
 * Resolve the OA schema, failing LOUDLY when it is absent (issue #3493).
 *
 * The schemas are COMMITTED SOURCE -- `test-data/schemas`, resolved checkout-relative or
 * from an absolute `CQLITE_SCHEMAS_ROOT` -- so their absence means a broken checkout or a
 * mis-pointed root, never a legitimate environment. A bare `return` here used to make
 * every OA assertion vanish behind `dbOa === null` under a green suite.
 *
 * Shared by BOTH OA `beforeAll` blocks, and called BEFORE oaBinariesPresent(): one
 * guards committed source (an error), the other guards fetched fixtures (a real skip),
 * and putting the skip first hid the error on every run without OA binaries.
 *
 * @returns {string} Absolute path to the OA schema.
 * @throws {Error} When the committed schema is not present.
 */
function requireOaSchema() {
  const schemaPath = global.testPaths.SCHEMA_OA_TEST;
  if (!require('fs').existsSync(schemaPath)) {
    throw new Error(
      `OA schema not found at ${schemaPath}. It is committed source, so this means a ` +
      'broken checkout or a wrong CQLITE_SCHEMAS_ROOT - failing loudly rather than ' +
      'silently skipping every OA assertion (issue #3493).'
    );
  }
  return schemaPath;
}

/**
 * Helper: check whether oa Data.db binary files are present.
 * Returns true when at least one oa-format Data.db exists in test_oa/.
 * Graceful-skip: if only JSONL goldens are present (no binaries), tests skip.
 */
function oaBinariesPresent() {
  const oaDir = require('path').join(global.testPaths.SSTABLES_DIR, 'test_oa');
  if (!require('fs').existsSync(oaDir)) return false;
  const entries = require('fs').readdirSync(oaDir, { withFileTypes: true });
  for (const entry of entries) {
    if (entry.isDirectory()) {
      const tableDir = require('path').join(oaDir, entry.name);
      const files = require('fs').readdirSync(tableDir);
      if (files.some((f) => /^oa-\d+-big-Data\.db$/.test(f))) {
        return true;
      }
    }
  }
  return false;
}

// VG6 (Issue #672): All 6 oa tables now pass row-count parity.
// The range-tombstone-marker skip function was fixed to correctly parse the
// u16 cluster_count + marker_body_size fields per the Cassandra serializer
// (ClusteringBoundOrBoundary.java:105, UnfilteredSerializer.java:291).
// countRowsInJsonl now excludes row-level tombstones (matching CQLite's behaviour
// of suppressing deleted rows from query results).

describe('VG6: OA Format Parity — Row Count (Issue #672)', () => {
  let dbOa = null;

  beforeAll(async () => {
    assertDatasetsAvailable();

    // Issue #3493: the COMMITTED-SOURCE check runs FIRST, before the fetched-fixture
    // one. The ORDER is the whole point (roborev round 13): with oaBinariesPresent()
    // first, a missing committed schema stayed silently hidden on any run without OA
    // binaries -- most partial-corpus `npm test` runs -- so the loud error added
    // earlier could not fire in the very situation it was written for.
    const schemaPath = requireOaSchema();

    if (!oaBinariesPresent()) {
      // FETCHED fixtures: a genuine skip -- but NOT under strict mode (issue #3493,
      // roborev round 15). CQLITE_REQUIRE_FIXTURES=1 means "a dataset test that does not
      // run is a failure", which is exactly what the full gate now sets. Skipping here
      // under strict let a corpus pass check-dataset-manifest.sh -- which accepts any
      // regular `*-Data.db` -- and then skip EVERY OA assertion beneath a green full
      // gate, because oaBinariesPresent() requires the narrower `oa-<n>-big-Data.db`.
      // The two predicates disagree by design, so the strict run must say so out loud.
      if (global.REQUIRE_FIXTURES) {
        throw new Error(
          'OA binaries not found under ' +
          require('path').join(global.testPaths.SSTABLES_DIR, 'test_oa') +
          ' (expected a file matching oa-<n>-big-Data.db). CQLITE_REQUIRE_FIXTURES=1, so ' +
          'this is a failure rather than a skip: a strict run that silently drops every ' +
          'OA assertion is the vacuous pass strict mode exists to prevent (issue #3493).'
        );
      }
      // Lenient runs still skip; individual tests skip via dbOa === null.
      return;
    }

    dbOa = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: schemaPath,
    });
  });

  afterAll(async () => {
    if (dbOa) {
      await dbOa.close();
      dbOa = null;
    }
  });

  // All 6 oa tables now enforce row count parity (VG6, Issue #672)
  guardedTestEach('test_oa', OA_TABLES, 'test_oa.%s row count matches JSONL golden', async (table) => {
    if (!dbOa) {
      console.log(`  Skipping test_oa.${table}: oa binaries absent (run fetch-datasets.sh)`);
      return; // graceful skip (no assert failures)
    }

    const jsonlPath = requireOaGolden('test_oa', table);
    if (!jsonlPath) return;   // lenient run: graceful skip

    const expectedCount = countRowsInJsonl(jsonlPath);
    const result = await dbOa.execute(`SELECT * FROM test_oa.${table}`);

    expect(result.rowCount).toBe(expectedCount);
    console.log(`  test_oa.${table}: ${result.rowCount} rows (match)`);
  });
});

describe('VG4: OA Format Parity — Value Spot Check (Issue #656)', () => {
  let dbOa = null;

  beforeAll(async () => {
    assertDatasetsAvailable();

    // Issue #3493: the COMMITTED-SOURCE check runs FIRST, before the fetched-fixture
    // one. The ORDER is the whole point (roborev round 13): with oaBinariesPresent()
    // first, a missing committed schema stayed silently hidden on any run without OA
    // binaries -- most partial-corpus `npm test` runs -- so the loud error added
    // earlier could not fire in the very situation it was written for.
    const schemaPath = requireOaSchema();

    if (!oaBinariesPresent()) {
      // FETCHED fixtures: a genuine skip -- but NOT under strict mode (issue #3493,
      // roborev round 15). CQLITE_REQUIRE_FIXTURES=1 means "a dataset test that does not
      // run is a failure", which is exactly what the full gate now sets. Skipping here
      // under strict let a corpus pass check-dataset-manifest.sh -- which accepts any
      // regular `*-Data.db` -- and then skip EVERY OA assertion beneath a green full
      // gate, because oaBinariesPresent() requires the narrower `oa-<n>-big-Data.db`.
      // The two predicates disagree by design, so the strict run must say so out loud.
      if (global.REQUIRE_FIXTURES) {
        throw new Error(
          'OA binaries not found under ' +
          require('path').join(global.testPaths.SSTABLES_DIR, 'test_oa') +
          ' (expected a file matching oa-<n>-big-Data.db). CQLITE_REQUIRE_FIXTURES=1, so ' +
          'this is a failure rather than a skip: a strict run that silently drops every ' +
          'OA assertion is the vacuous pass strict mode exists to prevent (issue #3493).'
        );
      }
      // Lenient runs still skip; individual tests skip via dbOa === null.
      return;
    }

    dbOa = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: schemaPath,
    });
  });

  afterAll(async () => {
    if (dbOa) {
      await dbOa.close();
      dbOa = null;
    }
  });

  // Tier 2: Value-level parity for test_oa.udt_table.
  // udt_table is chosen because it has complex UDT fields and correct row count.
  // simple_table has a timestamp overflow in the binding layer (year 73326 error).
  test('test_oa.udt_table cell values match JSONL golden', async () => {
    if (!dbOa) {
      console.log('  Skipping: oa binaries absent (run fetch-datasets.sh)');
      return;
    }

    const jsonlPath = requireOaGolden('test_oa', 'udt_table');
    if (!jsonlPath) {
      return;
    }

    const partitions = loadJsonlPartitions(jsonlPath);
    expect(partitions.length).toBeGreaterThan(0);

    const result = await dbOa.execute('SELECT * FROM test_oa.udt_table');
    expect(result.rowCount).toBeGreaterThan(0);

    // Build lookup by partition key (UUID string)
    const actualByKey = new Map();
    for (const row of result.rows) {
      const key = row.id;
      if (key != null) {
        actualByKey.set(String(key), row);
      }
    }

    let validated = 0;
    for (const partition of partitions) {
      const partitionKey = partition.partition.key[0];
      const rows = partition.rows || [];

      for (const rowData of rows) {
        if (rowData.type !== 'row') continue;

        const cells = rowData.cells || [];
        const actualRow = actualByKey.get(String(partitionKey));
        if (!actualRow) continue;

        for (const cell of cells) {
          const cellName = cell.name;
          if (!cellName || cell.deletion_info || cell.path) continue;

          // UDT address field: validate presence, not exact comparison
          if (typeof cell.value === 'object' && cell.value !== null && cell.value.street) {
            const actualUdt = actualRow[cellName];
            expect(actualUdt).not.toBeNull();
            validated++;
            continue;
          }

          const expected = normalizeJsonlValue(cell.value, cellName);
          const actual = actualRow[cellName];

          expect(valuesEqual(actual, expected)).toBe(true);
          validated++;
        }
      }
    }

    expect(validated).toBeGreaterThan(0);
    console.log(`  test_oa.udt_table: validated ${validated} cell values`);
  });
});
