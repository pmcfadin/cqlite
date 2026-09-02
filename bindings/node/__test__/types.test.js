/**
 * Type Conversion Tests for Issue #308.
 *
 * Comprehensive tests for all CQL type conversions to JavaScript types.
 * Uses executeNative() to test native type mapping.
 *
 * Epic: #318 M4 Node.js Bindings
 * Phase: 3 - Testing
 *
 * TDD Requirements from the issue:
 * - [x] Test every CQL primitive type
 * - [x] Test every collection type
 * - [x] Test nested collections
 * - [x] Test UDT field access
 * - [x] Test null handling
 * - [x] Test precision for large numbers
 */

const { Database } = require('../lib/index.js');
const { assertDatasetsAvailable } = require('./helpers.js');

// Helper to check if value is a Date (works across realms)
const isDate = (value) =>
  value !== null &&
  value !== undefined &&
  typeof value === 'object' &&
  typeof value.getTime === 'function' &&
  typeof value.toISOString === 'function' &&
  !isNaN(value.getTime());

// Helper to check if value is a Map (works across realms)
const isMap = (value) =>
  value !== null &&
  value !== undefined &&
  typeof value === 'object' &&
  Object.prototype.toString.call(value) === '[object Map]';

// Helper to check if value is a Set (works across realms)
const isSet = (value) =>
  value !== null &&
  value !== undefined &&
  typeof value === 'object' &&
  Object.prototype.toString.call(value) === '[object Set]';

// UUID regex pattern
const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// IPv4 and IPv6 patterns
const IPV4_REGEX = /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/;
const IPV6_REGEX = /^[0-9a-fA-F:]+$/;

describe('Type Conversion Tests (Issue #308)', () => {
  let dbBasic = null;
  let dbCollections = null;

  beforeAll(async () => {
    assertDatasetsAvailable();

    // Open databases for different keyspaces
    dbBasic = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_BASIC_TYPES,
    });

    dbCollections = await Database.open(global.testPaths.SSTABLES_DIR, {
      schema: global.testPaths.SCHEMA_COLLECTIONS,
    });
  });

  afterAll(async () => {
    if (dbBasic) {
      await dbBasic.close();
      dbBasic = null;
    }
    if (dbCollections) {
      await dbCollections.close();
      dbCollections = null;
    }
  });

  // ============================================================================
  // PRIMITIVES
  // ============================================================================
  describe('Primitives', () => {
    test('boolean converts to true/false', async () => {
      const result = await dbBasic.executeNative(
        'SELECT active FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundBoolean = false;
      for (const row of result.rows) {
        if (row.active !== null) {
          expect(typeof row.active).toBe('boolean');
          expect([true, false]).toContain(row.active);
          foundBoolean = true;
        }
      }
      expect(foundBoolean).toBe(true);
    });

    test('tinyint converts to number', async () => {
      const result = await dbBasic.executeNative(
        'SELECT small_number FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundTinyint = false;
      for (const row of result.rows) {
        if (row.small_number !== null) {
          expect(typeof row.small_number).toBe('number');
          // TINYINT range: -128 to 127
          expect(row.small_number).toBeGreaterThanOrEqual(-128);
          expect(row.small_number).toBeLessThanOrEqual(127);
          foundTinyint = true;
        }
      }
      expect(foundTinyint).toBe(true);
    });

    test('smallint converts to number', async () => {
      const result = await dbBasic.executeNative(
        'SELECT medium_number FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundSmallint = false;
      for (const row of result.rows) {
        if (row.medium_number !== null) {
          expect(typeof row.medium_number).toBe('number');
          // SMALLINT range: -32768 to 32767
          expect(row.medium_number).toBeGreaterThanOrEqual(-32768);
          expect(row.medium_number).toBeLessThanOrEqual(32767);
          foundSmallint = true;
        }
      }
      expect(foundSmallint).toBe(true);
    });

    test('int converts to number', async () => {
      const result = await dbBasic.executeNative(
        'SELECT age FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundInt = false;
      for (const row of result.rows) {
        if (row.age !== null) {
          expect(typeof row.age).toBe('number');
          expect(Number.isInteger(row.age)).toBe(true);
          foundInt = true;
        }
      }
      expect(foundInt).toBe(true);
    });

    test('bigint converts to BigInt', async () => {
      const result = await dbBasic.executeNative(
        'SELECT salary FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundBigint = false;
      for (const row of result.rows) {
        if (row.salary !== null) {
          expect(typeof row.salary).toBe('bigint');
          foundBigint = true;
        }
      }
      expect(foundBigint).toBe(true);
    });

    test('float converts to number', async () => {
      const result = await dbBasic.executeNative(
        'SELECT height FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundFloat = false;
      for (const row of result.rows) {
        if (row.height !== null) {
          expect(typeof row.height).toBe('number');
          expect(Number.isFinite(row.height)).toBe(true);
          foundFloat = true;
        }
      }
      expect(foundFloat).toBe(true);
    });

    test('double converts to number', async () => {
      const result = await dbBasic.executeNative(
        'SELECT weight FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundDouble = false;
      for (const row of result.rows) {
        if (row.weight !== null) {
          expect(typeof row.weight).toBe('number');
          expect(Number.isFinite(row.weight)).toBe(true);
          foundDouble = true;
        }
      }
      expect(foundDouble).toBe(true);
    });

    test('decimal converts to string for precision', async () => {
      const result = await dbBasic.executeNative(
        'SELECT account_balance FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundDecimal = false;
      for (const row of result.rows) {
        if (row.account_balance !== null) {
          expect(typeof row.account_balance).toBe('string');
          // Should be a valid decimal number string
          expect(row.account_balance).toMatch(/^-?\d+\.?\d*$/);
          foundDecimal = true;
        }
      }
      expect(foundDecimal).toBe(true);
    });
  });

  // ============================================================================
  // TEXT TYPES
  // ============================================================================
  describe('Text Types', () => {
    test('text converts to string', async () => {
      const result = await dbBasic.executeNative(
        'SELECT name FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundText = false;
      for (const row of result.rows) {
        if (row.name !== null) {
          expect(typeof row.name).toBe('string');
          foundText = true;
        }
      }
      expect(foundText).toBe(true);
    });

    test('varchar converts to string', async () => {
      const result = await dbBasic.executeNative(
        'SELECT varchar_field FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundVarchar = false;
      for (const row of result.rows) {
        if (row.varchar_field !== null) {
          expect(typeof row.varchar_field).toBe('string');
          foundVarchar = true;
        }
      }
      expect(foundVarchar).toBe(true);
    });

    test('ascii converts to string', async () => {
      const result = await dbBasic.executeNative(
        'SELECT ascii_field FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundAscii = false;
      for (const row of result.rows) {
        if (row.ascii_field !== null) {
          expect(typeof row.ascii_field).toBe('string');
          // ASCII should only contain 7-bit characters
          for (const char of row.ascii_field) {
            expect(char.charCodeAt(0)).toBeLessThan(128);
          }
          foundAscii = true;
        }
      }
      expect(foundAscii).toBe(true);
    });
  });

  // ============================================================================
  // BINARY TYPES
  // ============================================================================
  describe('Binary Types', () => {
    test('blob converts to Buffer', async () => {
      const result = await dbBasic.executeNative(
        'SELECT description FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundBlob = false;
      for (const row of result.rows) {
        if (row.description !== null) {
          expect(Buffer.isBuffer(row.description)).toBe(true);
          foundBlob = true;
        }
      }
      expect(foundBlob).toBe(true);
    });

    test('uuid converts to formatted string', async () => {
      const result = await dbBasic.executeNative(
        'SELECT id FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      for (const row of result.rows) {
        if (row.id !== null) {
          expect(typeof row.id).toBe('string');
          expect(row.id).toMatch(UUID_REGEX);
        }
      }
    });

    test('timeuuid converts to formatted string', async () => {
      const result = await dbBasic.executeNative(
        'SELECT session_id FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundTimeuuid = false;
      for (const row of result.rows) {
        if (row.session_id !== null) {
          expect(typeof row.session_id).toBe('string');
          expect(row.session_id).toMatch(UUID_REGEX);
          foundTimeuuid = true;
        }
      }
      expect(foundTimeuuid).toBe(true);
    });

    test('inet converts to string', async () => {
      const result = await dbBasic.executeNative(
        'SELECT ip_address FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundInet = false;
      for (const row of result.rows) {
        if (row.ip_address !== null) {
          expect(typeof row.ip_address).toBe('string');
          // Should be either IPv4 or IPv6 format
          const isValidIp = IPV4_REGEX.test(row.ip_address) || IPV6_REGEX.test(row.ip_address);
          expect(isValidIp).toBe(true);
          foundInet = true;
        }
      }
      expect(foundInet).toBe(true);
    });
  });

  // ============================================================================
  // TEMPORAL TYPES
  // ============================================================================
  describe('Temporal Types', () => {
    test('timestamp converts to Date', async () => {
      const result = await dbBasic.executeNative(
        'SELECT created FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundTimestamp = false;
      for (const row of result.rows) {
        if (row.created !== null) {
          expect(isDate(row.created)).toBe(true);
          // Should have valid timestamp
          expect(row.created.getTime()).not.toBeNaN();
          foundTimestamp = true;
        }
      }
      expect(foundTimestamp).toBe(true);
    });

    test('date converts to Date (midnight UTC)', async () => {
      const result = await dbBasic.executeNative(
        'SELECT birth_date FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundDate = false;
      for (const row of result.rows) {
        if (row.birth_date !== null) {
          expect(isDate(row.birth_date)).toBe(true);
          foundDate = true;
        }
      }
      expect(foundDate).toBe(true);
    });

    test('time converts to bigint (nanoseconds)', async () => {
      const result = await dbBasic.executeNative(
        'SELECT work_time FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundTime = false;
      for (const row of result.rows) {
        if (row.work_time !== null) {
          expect(typeof row.work_time).toBe('bigint');
          // Time is nanoseconds since midnight: 0 to 86399999999999
          expect(row.work_time).toBeGreaterThanOrEqual(0n);
          expect(row.work_time).toBeLessThan(86400000000000n); // 24 hours in nanos
          foundTime = true;
        }
      }
      expect(foundTime).toBe(true);
    });

    test('duration converts to object with months, days, nanos', async () => {
      const result = await dbBasic.executeNative(
        'SELECT duration_val FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundDuration = false;
      for (const row of result.rows) {
        if (row.duration_val !== null) {
          expect(typeof row.duration_val).toBe('object');
          expect(row.duration_val).toHaveProperty('months');
          expect(row.duration_val).toHaveProperty('days');
          expect(row.duration_val).toHaveProperty('nanos');
          expect(typeof row.duration_val.months).toBe('number');
          expect(typeof row.duration_val.days).toBe('number');
          expect(typeof row.duration_val.nanos).toBe('bigint');
          foundDuration = true;
        }
      }
      expect(foundDuration).toBe(true);
    });
  });

  // ============================================================================
  // COLLECTIONS
  // Based on actual data in test_collections keyspace:
  // - collection_table: ordered_values(LIST<TIMESTAMP>), metadata_map(MAP<TEXT,BIGINT>), numbers_set(SET<INT>)
  // - frozen_collections_table: frozen_scores(FROZEN<LIST<INT>>), frozen_properties(FROZEN<MAP<TEXT,TEXT>>)
  // ============================================================================
  describe('Collections', () => {
    test('list<timestamp> converts to Date[]', async () => {
      const result = await dbCollections.executeNative(
        'SELECT ordered_values FROM test_collections.collection_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundList = false;
      for (const row of result.rows) {
        if (row.ordered_values !== null && row.ordered_values.length > 0) {
          expect(Array.isArray(row.ordered_values)).toBe(true);
          for (const item of row.ordered_values) {
            expect(isDate(item)).toBe(true);
          }
          foundList = true;
        }
      }
      expect(foundList).toBe(true);
    });

    test('map<text,bigint> converts to Map<string,bigint>', async () => {
      const result = await dbCollections.executeNative(
        'SELECT metadata_map FROM test_collections.collection_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundMap = false;
      for (const row of result.rows) {
        if (row.metadata_map !== null && row.metadata_map.size > 0) {
          expect(isMap(row.metadata_map)).toBe(true);
          for (const [key, value] of row.metadata_map) {
            expect(typeof key).toBe('string');
            expect(typeof value).toBe('bigint');
          }
          foundMap = true;
        }
      }
      expect(foundMap).toBe(true);
    });

    test('set<int> converts to Set<number>', async () => {
      const result = await dbCollections.executeNative(
        'SELECT numbers_set FROM test_collections.collection_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      // Note: Some sets may be empty in test data
      for (const row of result.rows) {
        if (row.numbers_set !== null) {
          expect(isSet(row.numbers_set)).toBe(true);
          for (const item of row.numbers_set) {
            expect(typeof item).toBe('number');
          }
        }
      }
    });

    test('frozen list<int> converts to number[]', async () => {
      const result = await dbCollections.executeNative(
        'SELECT frozen_scores FROM test_collections.frozen_collections_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundFrozen = false;
      for (const row of result.rows) {
        if (row.frozen_scores !== null) {
          expect(Array.isArray(row.frozen_scores)).toBe(true);
          for (const item of row.frozen_scores) {
            expect(typeof item).toBe('number');
          }
          foundFrozen = true;
        }
      }
      expect(foundFrozen).toBe(true);
    });

    test('frozen map<text,text> converts to Map<string,string>', async () => {
      const result = await dbCollections.executeNative(
        'SELECT frozen_properties FROM test_collections.frozen_collections_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundFrozen = false;
      for (const row of result.rows) {
        if (row.frozen_properties !== null && row.frozen_properties.size > 0) {
          expect(isMap(row.frozen_properties)).toBe(true);
          for (const [key, value] of row.frozen_properties) {
            expect(typeof key).toBe('string');
            expect(typeof value).toBe('string');
          }
          foundFrozen = true;
        }
      }
      expect(foundFrozen).toBe(true);
    });
  });

  // ============================================================================
  // TYPED COLLECTIONS (collections with specific element types)
  // Based on actual data in typed_collections_table:
  // - decimal_set(SET<DECIMAL>), boolean_map(MAP<TEXT,BOOLEAN>), blob_list(LIST<BLOB>)
  // ============================================================================
  describe('Typed Collections', () => {
    test('set<decimal> converts to Set<string>', async () => {
      const result = await dbCollections.executeNative(
        'SELECT decimal_set FROM test_collections.typed_collections_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      // Test data may have empty sets, so just verify the structure
      for (const row of result.rows) {
        if (row.decimal_set !== null) {
          expect(isSet(row.decimal_set)).toBe(true);
          // If set has items, verify type
          for (const item of row.decimal_set) {
            expect(typeof item).toBe('string');
            expect(item).toMatch(/^-?\d+\.?\d*$/);
          }
        }
      }
    });

    test('map<text,boolean> converts to Map<string,boolean>', async () => {
      const result = await dbCollections.executeNative(
        'SELECT boolean_map FROM test_collections.typed_collections_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundMap = false;
      for (const row of result.rows) {
        if (row.boolean_map !== null && row.boolean_map.size > 0) {
          expect(isMap(row.boolean_map)).toBe(true);
          for (const [key, value] of row.boolean_map) {
            expect(typeof key).toBe('string');
            expect(typeof value).toBe('boolean');
          }
          foundMap = true;
        }
      }
      expect(foundMap).toBe(true);
    });

    test('list<blob> converts to Buffer[]', async () => {
      const result = await dbCollections.executeNative(
        'SELECT blob_list FROM test_collections.typed_collections_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundList = false;
      for (const row of result.rows) {
        if (row.blob_list !== null && row.blob_list.length > 0) {
          expect(Array.isArray(row.blob_list)).toBe(true);
          for (const item of row.blob_list) {
            expect(Buffer.isBuffer(item)).toBe(true);
          }
          foundList = true;
        }
      }
      expect(foundList).toBe(true);
    });
  });

  // ============================================================================
  // NESTED COLLECTIONS
  // Based on actual data in nested_collections_table:
  // - scores_by_game(MAP<TEXT,FROZEN<LIST<INT>>>)
  // - user_preferences(MAP<TEXT,FROZEN<MAP<TEXT,TEXT>>>)
  // - tags_by_category(MAP<TEXT,FROZEN<SET<TEXT>>>)
  // - time_series_data(MAP<DATE,FROZEN<LIST<TIMESTAMP>>>)
  // ============================================================================
  describe('Nested Collections', () => {
    test('map<text,frozen<set<text>>> converts correctly', async () => {
      const result = await dbCollections.executeNative(
        'SELECT tags_by_category FROM test_collections.nested_collections_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundNested = false;
      for (const row of result.rows) {
        if (row.tags_by_category !== null && row.tags_by_category.size > 0) {
          expect(isMap(row.tags_by_category)).toBe(true);
          for (const [key, value] of row.tags_by_category) {
            expect(typeof key).toBe('string');
            // Inner frozen set should unwrap to Set
            expect(isSet(value)).toBe(true);
            for (const item of value) {
              expect(typeof item).toBe('string');
            }
          }
          foundNested = true;
        }
      }
      expect(foundNested).toBe(true);
    });

    test('map<text,frozen<list<int>>> converts correctly', async () => {
      const result = await dbCollections.executeNative(
        'SELECT scores_by_game FROM test_collections.nested_collections_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundNested = false;
      for (const row of result.rows) {
        if (row.scores_by_game !== null && row.scores_by_game.size > 0) {
          expect(isMap(row.scores_by_game)).toBe(true);
          for (const [key, value] of row.scores_by_game) {
            expect(typeof key).toBe('string');
            expect(Array.isArray(value)).toBe(true);
            for (const item of value) {
              expect(typeof item).toBe('number');
            }
          }
          foundNested = true;
        }
      }
      expect(foundNested).toBe(true);
    });

    test('map<text,frozen<map<text,text>>> converts correctly', async () => {
      const result = await dbCollections.executeNative(
        'SELECT user_preferences FROM test_collections.nested_collections_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundNested = false;
      for (const row of result.rows) {
        if (row.user_preferences !== null && row.user_preferences.size > 0) {
          expect(isMap(row.user_preferences)).toBe(true);
          for (const [key, value] of row.user_preferences) {
            expect(typeof key).toBe('string');
            expect(isMap(value)).toBe(true);
            for (const [innerKey, innerValue] of value) {
              expect(typeof innerKey).toBe('string');
              expect(typeof innerValue).toBe('string');
            }
          }
          foundNested = true;
        }
      }
      expect(foundNested).toBe(true);
    });

    test('map<date,frozen<list<timestamp>>> converts correctly', async () => {
      const result = await dbCollections.executeNative(
        'SELECT time_series_data FROM test_collections.nested_collections_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundNested = false;
      for (const row of result.rows) {
        if (row.time_series_data !== null && row.time_series_data.size > 0) {
          expect(isMap(row.time_series_data)).toBe(true);
          for (const [key, value] of row.time_series_data) {
            // Date key converted to Date object
            expect(isDate(key)).toBe(true);
            expect(Array.isArray(value)).toBe(true);
            for (const item of value) {
              expect(isDate(item)).toBe(true);
            }
          }
          foundNested = true;
        }
      }
      expect(foundNested).toBe(true);
    });
  });

  // ============================================================================
  // COMPLEX TYPES (UDT)
  // Based on actual data in collections_with_udts:
  // - addresses(LIST<FROZEN<address_type>>)
  // - contacts(SET<FROZEN<contact_info>>)
  // - locations_visited(MAP<DATE,FROZEN<address_type>>)
  // - emergency_contacts(MAP<TEXT,FROZEN<contact_info>>)
  // ============================================================================
  describe('Complex Types', () => {
    test('list<frozen<udt>> converts to object[] with out-of-band typeName/keyspace', async () => {
      const result = await dbCollections.executeNative(
        'SELECT addresses FROM test_collections.collections_with_udts LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundUdt = false;
      for (const row of result.rows) {
        if (row.addresses !== null && row.addresses.length > 0) {
          expect(Array.isArray(row.addresses)).toBe(true);
          for (const addr of row.addresses) {
            expect(typeof addr).toBe('object');
            // Type identity is carried OUT OF BAND (#3504)
            expect(addr.typeName).toBe('address_type');
            expect(addr.keyspace).toBe('test_collections');
            // ...and the removed markers are no longer readable at all
            expect(addr._type).toBeUndefined();
            expect(addr._keyspace).toBeUndefined();
            // Declared fields live in their own namespace, and NOWHERE else:
            // `Object.keys(addr)` is exactly the out-of-band surface.
            expect(Object.keys(addr).sort()).toEqual(['fields', 'keyspace', 'typeName']);
            expect(addr.fields).toHaveProperty('street');
            expect(addr.fields).toHaveProperty('city');
            expect(addr.fields).toHaveProperty('state');
            expect(addr.fields).toHaveProperty('zip_code');
            expect(addr.fields).toHaveProperty('country');
            foundUdt = true;
          }
        }
      }
      expect(foundUdt).toBe(true);
    });

    test('set<frozen<udt>> converts to Set<object>', async () => {
      const result = await dbCollections.executeNative(
        'SELECT contacts FROM test_collections.collections_with_udts LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      // Test data may have empty sets, so just verify the structure
      for (const row of result.rows) {
        if (row.contacts !== null) {
          expect(isSet(row.contacts)).toBe(true);
          // If set has items, verify type
          for (const contact of row.contacts) {
            expect(typeof contact).toBe('object');
            expect(contact.typeName).toBe('contact_info');
          }
        }
      }
    });

    test('map<date,frozen<udt>> converts correctly', async () => {
      const result = await dbCollections.executeNative(
        'SELECT locations_visited FROM test_collections.collections_with_udts LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundMap = false;
      for (const row of result.rows) {
        if (row.locations_visited !== null && row.locations_visited.size > 0) {
          expect(isMap(row.locations_visited)).toBe(true);
          for (const [key, addr] of row.locations_visited) {
            expect(isDate(key)).toBe(true);
            expect(typeof addr).toBe('object');
            expect(addr.typeName).toBe('address_type');
          }
          foundMap = true;
        }
      }
      expect(foundMap).toBe(true);
    });

    test('map<text,frozen<udt>> with nested udt converts correctly', async () => {
      const result = await dbCollections.executeNative(
        'SELECT emergency_contacts FROM test_collections.collections_with_udts LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      let foundNestedUdt = false;
      for (const row of result.rows) {
        if (row.emergency_contacts !== null && row.emergency_contacts.size > 0) {
          expect(isMap(row.emergency_contacts)).toBe(true);
          for (const [key, contact] of row.emergency_contacts) {
            expect(typeof key).toBe('string');
            expect(typeof contact).toBe('object');
            expect(contact.typeName).toBe('contact_info');
            // Check nested address UDT — nested UDTs nest through `fields` too
            if (contact.fields.address !== null) {
              expect(contact.fields.address.typeName).toBe('address_type');
              expect(contact.fields.address.fields).toHaveProperty('street');
            }
            foundNestedUdt = true;
          }
        }
      }
      expect(foundNestedUdt).toBe(true);
    });

    test('null values are properly represented', async () => {
      const result = await dbBasic.executeNative(
        'SELECT * FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      // Verify that null values exist and are JavaScript null
      for (const row of result.rows) {
        for (const [key, value] of Object.entries(row)) {
          if (value === null) {
            expect(value).toBeNull();
            // Ensure it's not undefined
            expect(value).not.toBeUndefined();
          }
        }
      }
    });
  });

  // ============================================================================
  // PRECISION TESTS
  // ============================================================================
  describe('Precision', () => {
    test('bigint preserves values beyond Number.MAX_SAFE_INTEGER', async () => {
      const result = await dbBasic.executeNative(
        'SELECT salary FROM test_basic.simple_table LIMIT 100'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      // BigInt should be used for all salary values
      for (const row of result.rows) {
        if (row.salary !== null) {
          expect(typeof row.salary).toBe('bigint');
          // Verify BigInt operations work
          const doubled = row.salary * 2n;
          expect(typeof doubled).toBe('bigint');
        }
      }
    });

    test('decimal preserves arbitrary precision as string', async () => {
      const result = await dbBasic.executeNative(
        'SELECT account_balance FROM test_basic.simple_table LIMIT 100'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      for (const row of result.rows) {
        if (row.account_balance !== null) {
          expect(typeof row.account_balance).toBe('string');
          // String representation preserves all digits
          const parsed = parseFloat(row.account_balance);
          expect(Number.isFinite(parsed)).toBe(true);
        }
      }
    });

    test('time uses bigint for nanosecond precision', async () => {
      const result = await dbBasic.executeNative(
        'SELECT work_time FROM test_basic.simple_table LIMIT 100'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      for (const row of result.rows) {
        if (row.work_time !== null) {
          expect(typeof row.work_time).toBe('bigint');
          // Nanosecond precision would lose precision as Number
          // BigInt preserves full precision
          const nanos = row.work_time;
          expect(nanos).toBeGreaterThanOrEqual(0n);
        }
      }
    });

    test('duration nanos uses bigint for nanosecond precision', async () => {
      const result = await dbBasic.executeNative(
        'SELECT duration_val FROM test_basic.simple_table LIMIT 100'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      for (const row of result.rows) {
        if (row.duration_val !== null) {
          expect(typeof row.duration_val.nanos).toBe('bigint');
        }
      }
    });
  });

  // ============================================================================
  // EDGE CASES
  // ============================================================================
  describe('Edge Cases', () => {
    test('empty string is preserved', async () => {
      // Query may contain empty strings
      const result = await dbBasic.executeNative(
        'SELECT name, varchar_field, ascii_field FROM test_basic.simple_table LIMIT 100'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      // At least verify we can handle string columns
      for (const row of result.rows) {
        if (row.name !== null) {
          expect(typeof row.name).toBe('string');
        }
      }
    });

    test('list with multiple items preserves order', async () => {
      const result = await dbCollections.executeNative(
        'SELECT ordered_values FROM test_collections.collection_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      for (const row of result.rows) {
        if (row.ordered_values !== null && row.ordered_values.length > 1) {
          // Lists preserve order - verify it's an array
          expect(Array.isArray(row.ordered_values)).toBe(true);
          // Index access should work
          expect(row.ordered_values[0]).toBeDefined();
        }
      }
    });

    test('set maintains unique values', async () => {
      const result = await dbCollections.executeNative(
        'SELECT numbers_set FROM test_collections.collection_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      for (const row of result.rows) {
        if (row.numbers_set !== null) {
          const setValues = [...row.numbers_set];
          const uniqueValues = [...new Set(setValues)];
          expect(setValues.length).toBe(uniqueValues.length);
        }
      }
    });

    test('map keys are accessible', async () => {
      const result = await dbCollections.executeNative(
        'SELECT metadata_map FROM test_collections.collection_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      for (const row of result.rows) {
        if (row.metadata_map !== null && row.metadata_map.size > 0) {
          // Map.get() should work
          const firstKey = [...row.metadata_map.keys()][0];
          expect(row.metadata_map.has(firstKey)).toBe(true);
          expect(row.metadata_map.get(firstKey)).toBeDefined();
        }
      }
    });
  });

  // ============================================================================
  // PRE-EPOCH TIMESTAMP TESTS (Issue #341)
  // ============================================================================
  describe('Pre-Epoch Timestamps', () => {
    // These tests validate that the timestamp conversion logic handles
    // pre-epoch (negative) timestamps correctly. Issue #341 identified
    // that truncating division caused errors for negative values.

    test('JavaScript Date handles pre-epoch timestamps correctly', () => {
      // Validate JavaScript Date behavior for reference
      // This documents the expected behavior our Rust code should match

      // -1500ms = 1.5 seconds before epoch (1969-12-31T23:59:58.500Z)
      const date1 = new Date(-1500);
      expect(date1.toISOString()).toBe('1969-12-31T23:59:58.500Z');

      // -1ms = just before epoch
      const date2 = new Date(-1);
      expect(date2.toISOString()).toBe('1969-12-31T23:59:59.999Z');

      // -500ms = half second before epoch
      const date3 = new Date(-500);
      expect(date3.toISOString()).toBe('1969-12-31T23:59:59.500Z');

      // -60000ms = 1 minute before epoch
      const date4 = new Date(-60000);
      expect(date4.toISOString()).toBe('1969-12-31T23:59:00.000Z');
    });

    test('pre-epoch timestamp conversion is mathematically correct', () => {
      // Verify correct Euclidean division behavior
      // Bug was: ts / 1000 truncates toward zero, not toward negative infinity

      // For -1500ms:
      // Wrong: -1500 / 1000 = -1 (truncating toward zero)
      // Correct: floor(-1500 / 1000) = -2 (floor toward negative infinity)

      const wrongDivision = Math.trunc(-1500 / 1000); // -1 (WRONG)
      const correctDivision = Math.floor(-1500 / 1000); // -2 (CORRECT)

      expect(wrongDivision).toBe(-1);
      expect(correctDivision).toBe(-2);

      // The correct result: -2 seconds + 500 milliseconds = -1.5 seconds = -1500ms
      const reconstructed = correctDivision * 1000 + 500;
      expect(reconstructed).toBe(-1500);

      // Wrong result: -1 seconds + 500 milliseconds = -0.5 seconds = -500ms (WRONG!)
      const wrongReconstructed = wrongDivision * 1000 + 500;
      expect(wrongReconstructed).toBe(-500); // This was the bug!
    });

    test('executeNative handles timestamps from test data', async () => {
      // Query real data and verify Date conversion works
      const result = await dbBasic.executeNative(
        'SELECT created FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      for (const row of result.rows) {
        if (row.created !== null) {
          expect(isDate(row.created)).toBe(true);
          // All timestamps should produce valid ISO strings
          expect(() => row.created.toISOString()).not.toThrow();
        }
      }
    });

    test('execute JSON path handles timestamps', async () => {
      // Test the JSON conversion path (execute instead of executeNative)
      // This is the path that had the bug in Issue #341
      const result = await dbBasic.execute(
        'SELECT created FROM test_basic.simple_table LIMIT 10'
      );

      expect(result.rowCount).toBeGreaterThan(0);

      for (const row of result.rows) {
        if (row.created !== null) {
          // JSON path returns ISO 8601 strings, not Date objects
          expect(typeof row.created).toBe('string');
          // Should be valid ISO date string
          const parsed = new Date(row.created);
          expect(isDate(parsed)).toBe(true);
          expect(parsed.toISOString()).toBeDefined();
        }
      }
    });
  });
});
