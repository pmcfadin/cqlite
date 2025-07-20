/**
 * CQLite Basic Tests
 * Testing the revolutionary SSTable querying functionality
 */

const { SSTableReader, Utils, CQLiteError, SchemaError, QueryError } = require('../lib/index.js');
const path = require('path');
const fs = require('fs').promises;

describe('CQLite Basic Functionality', () => {
  let testDataDir;
  let mockSSTablePath;
  let mockSchemaPath;

  beforeAll(async () => {
    // Setup test environment
    testDataDir = path.join(__dirname, 'test-data');
    await fs.mkdir(testDataDir, { recursive: true });

    mockSSTablePath = path.join(testDataDir, 'test-users-Data.db');
    mockSchemaPath = path.join(testDataDir, 'test-users_schema.json');

    // Create mock schema file
    const mockSchema = {
      table: 'users',
      columns: [
        { name: 'user_id', type: 'uuid', primaryKey: true },
        { name: 'name', type: 'text' },
        { name: 'email', type: 'text' },
        { name: 'age', type: 'int' },
        { name: 'active', type: 'boolean' },
        { name: 'created_date', type: 'timestamp', clusteringKey: true }
      ]
    };

    await fs.writeFile(mockSchemaPath, JSON.stringify(mockSchema, null, 2));

    // Create a minimal mock SSTable file (would normally be created by Cassandra)
    await fs.writeFile(mockSSTablePath, 'MOCK_SSTABLE_DATA');
  });

  afterAll(async () => {
    // Cleanup test files
    try {
      await fs.rm(testDataDir, { recursive: true, force: true });
    } catch (error) {
      // Ignore cleanup errors
    }
  });

  describe('SSTableReader Construction', () => {
    test('should create SSTableReader with valid parameters', () => {
      expect(() => {
        new SSTableReader(mockSSTablePath, { schema: mockSchemaPath });
      }).not.toThrow();
    });

    test('should throw error for non-existent SSTable file', () => {
      expect(() => {
        new SSTableReader('/non/existent/file.db', { schema: mockSchemaPath });
      }).toThrow();
    });

    test('should throw error for non-existent schema file', () => {
      expect(() => {
        new SSTableReader(mockSSTablePath, { schema: '/non/existent/schema.json' });
      }).toThrow();
    });

    test('should accept all configuration options', () => {
      const options = {
        schema: mockSchemaPath,
        compression: 'lz4',
        cacheSize: 128,
        enableBloomFilter: true
      };

      expect(() => {
        new SSTableReader(mockSSTablePath, options);
      }).not.toThrow();
    });
  });

  describe('Query Validation', () => {
    test('should validate correct SELECT queries', () => {
      const validQueries = [
        'SELECT * FROM users',
        'SELECT name, email FROM users WHERE age > 25',
        'SELECT COUNT(*) FROM users WHERE active = true',
        'SELECT name FROM users ORDER BY created_date DESC LIMIT 10'
      ];

      validQueries.forEach(query => {
        expect(() => Utils.validateQuery(query)).not.toThrow();
      });
    });

    test('should reject invalid queries', () => {
      const invalidQueries = [
        'INSERT INTO users VALUES (1, \'test\')',
        'UPDATE users SET name = \'test\'',
        'DELETE FROM users WHERE id = 1',
        'CREATE TABLE test (id int)',
        'INVALID SQL SYNTAX'
      ];

      invalidQueries.forEach(query => {
        expect(() => Utils.validateQuery(query)).toThrow();
      });
    });

    test('should validate complex SELECT queries', () => {
      const complexQueries = [
        'SELECT u.name, u.email FROM users u WHERE u.age BETWEEN 25 AND 45',
        'SELECT department, COUNT(*) as count FROM users GROUP BY department',
        'SELECT * FROM users WHERE name LIKE \'John%\' AND active = true'
      ];

      complexQueries.forEach(query => {
        expect(() => Utils.validateQuery(query)).not.toThrow();
      });
    });
  });

  describe('Schema Parsing', () => {
    test('should parse schema file correctly', async () => {
      const schema = await Utils.parseSchema(mockSchemaPath);
      
      expect(schema.table).toBe('users');
      expect(schema.columns).toHaveLength(6);
      
      const userIdColumn = schema.columns.find(col => col.name === 'user_id');
      expect(userIdColumn).toBeDefined();
      expect(userIdColumn.type).toBe('uuid');
      expect(userIdColumn.primaryKey).toBe(true);
      
      const createdDateColumn = schema.columns.find(col => col.name === 'created_date');
      expect(createdDateColumn).toBeDefined();
      expect(createdDateColumn.clusteringKey).toBe(true);
    });

    test('should throw error for invalid schema file', async () => {
      const invalidSchemaPath = path.join(testDataDir, 'invalid_schema.json');
      await fs.writeFile(invalidSchemaPath, 'INVALID JSON');

      await expect(Utils.parseSchema(invalidSchemaPath)).rejects.toThrow();
    });
  });

  describe('Error Handling', () => {
    test('should create CQLiteError with correct properties', () => {
      const error = new CQLiteError('Test message', 'TEST_CODE', { detail: 'test' });
      
      expect(error.message).toBe('Test message');
      expect(error.code).toBe('TEST_CODE');
      expect(error.details).toEqual({ detail: 'test' });
      expect(error).toBeInstanceOf(Error);
      expect(error.name).toBe('CQLiteError');
    });

    test('should create SchemaError as subclass of CQLiteError', () => {
      const error = new SchemaError('Schema test message', { column: 'test' });
      
      expect(error.message).toBe('Schema test message');
      expect(error.code).toBe('SCHEMA_ERROR');
      expect(error.details).toEqual({ column: 'test' });
      expect(error).toBeInstanceOf(CQLiteError);
      expect(error.name).toBe('SchemaError');
    });

    test('should create QueryError as subclass of CQLiteError', () => {
      const error = new QueryError('Query test message', { sql: 'SELECT * FROM test' });
      
      expect(error.message).toBe('Query test message');
      expect(error.code).toBe('QUERY_ERROR');
      expect(error.details).toEqual({ sql: 'SELECT * FROM test' });
      expect(error).toBeInstanceOf(CQLiteError);
      expect(error.name).toBe('QueryError');
    });
  });

  describe('Utility Functions', () => {
    test('should convert CQL values correctly', () => {
      expect(Utils.convertCQLValue('123', 'bigint')).toBe(BigInt(123));
      expect(Utils.convertCQLValue('2023-01-01T00:00:00Z', 'timestamp')).toBeInstanceOf(Date);
      expect(Utils.convertCQLValue(['a', 'b', 'c'], 'list')).toEqual(['a', 'b', 'c']);
      expect(Utils.convertCQLValue(['a', 'b', 'c'], 'set')).toEqual(['a', 'b', 'c']);
      expect(Utils.convertCQLValue({ a: 1, b: 2 }, 'map')).toBeInstanceOf(Map);
      expect(Utils.convertCQLValue('regular text', 'text')).toBe('regular text');
    });

    test('should create schema object correctly', () => {
      const columns = [
        { name: 'id', type: 'uuid', primaryKey: true },
        { name: 'name', type: 'text' }
      ];
      
      const schema = Utils.createSchema('test_table', columns);
      
      expect(schema.table).toBe('test_table');
      expect(schema.columns).toHaveLength(2);
      expect(schema.columns[0].primaryKey).toBe(true);
      expect(schema.columns[1].primaryKey).toBe(false);
    });

    test('should validate SSTable file format', async () => {
      // Test with valid SSTable file
      await expect(Utils.validateSSTableFile(mockSSTablePath)).resolves.toBe(true);
      
      // Test with non-existent file
      await expect(Utils.validateSSTableFile('/non/existent/file.db')).rejects.toThrow();
      
      // Test with directory instead of file
      await expect(Utils.validateSSTableFile(testDataDir)).rejects.toThrow();
    });
  });

  describe('Convenience Functions', () => {
    // Note: These tests would require actual SSTable files to work properly
    // In a real implementation, you'd mock the native module responses

    test('should provide quickQuery function', () => {
      expect(typeof require('../lib/index.js').quickQuery).toBe('function');
    });

    test('should provide batchQuery function', () => {
      expect(typeof require('../lib/index.js').batchQuery).toBe('function');
    });

    test('should provide createTypedReader function', () => {
      expect(typeof require('../lib/index.js').createTypedReader).toBe('function');
    });
  });

  describe('Module Exports', () => {
    test('should export all required classes and functions', () => {
      const exports = require('../lib/index.js');
      
      expect(exports.SSTableReader).toBeDefined();
      expect(exports.CQLiteError).toBeDefined();
      expect(exports.SchemaError).toBeDefined();
      expect(exports.QueryError).toBeDefined();
      expect(exports.Utils).toBeDefined();
      expect(exports.validateQuery).toBeDefined();
      expect(exports.parseSchema).toBeDefined();
      expect(exports.createTypedReader).toBeDefined();
      expect(exports.quickQuery).toBeDefined();
      expect(exports.batchQuery).toBeDefined();
    });

    test('should have proper class hierarchies', () => {
      expect(new SchemaError('test')).toBeInstanceOf(CQLiteError);
      expect(new QueryError('test')).toBeInstanceOf(CQLiteError);
      expect(new CQLiteError('test', 'TEST')).toBeInstanceOf(Error);
    });
  });

  describe('Performance Considerations', () => {
    test('should handle large query strings efficiently', () => {
      const largeQuery = 'SELECT * FROM users WHERE ' + 
        Array.from({ length: 100 }, (_, i) => `field${i} = 'value${i}'`).join(' OR ');
      
      // Should not throw for large but valid queries
      expect(() => Utils.validateQuery(largeQuery)).not.toThrow();
    });

    test('should validate reasonable column lists', () => {
      const manyColumns = Array.from({ length: 50 }, (_, i) => `col${i}`).join(', ');
      const query = `SELECT ${manyColumns} FROM users`;
      
      expect(() => Utils.validateQuery(query)).not.toThrow();
    });
  });
});

// Integration tests (would require actual SSTable files)
describe('CQLite Integration Tests', () => {
  // These tests would run against real SSTable files in a CI environment
  
  test.skip('should query actual SSTable file', async () => {
    // This test would run with actual Cassandra-generated SSTable files
    // Skip for now as it requires test data setup
  });

  test.skip('should handle streaming on large SSTable', async () => {
    // This test would verify streaming performance on large files
    // Skip for now as it requires large test SSTable files
  });

  test.skip('should benchmark query performance', async () => {
    // This test would measure and verify query performance metrics
    // Skip for now as it requires performance baselines
  });
});

// Mock native module for testing
jest.mock('../cqlite.node', () => ({
  SSTableReader: class MockSSTableReader {
    constructor(path, options) {
      if (!require('fs').existsSync(path)) {
        throw new Error(`SSTable file not found: ${path}`);
      }
      if (!require('fs').existsSync(options.schema)) {
        throw new Error(`Schema file not found: ${options.schema}`);
      }
    }

    async query(sql, options) {
      // Mock query response
      return {
        rows: [
          { user_id: '123e4567-e89b-12d3-a456-426614174000', name: 'John Doe', email: 'john@example.com', age: 30 },
          { user_id: '123e4567-e89b-12d3-a456-426614174001', name: 'Jane Smith', email: 'jane@example.com', age: 25 }
        ],
        rowCount: 2,
        executionTime: 15,
        stats: { blocksRead: 1, cacheHits: 0, cacheMisses: 1 }
      };
    }

    async getSchema() {
      return {
        table: 'users',
        columns: [
          { name: 'user_id', type: 'uuid', primaryKey: true },
          { name: 'name', type: 'text' },
          { name: 'email', type: 'text' },
          { name: 'age', type: 'int' }
        ]
      };
    }

    async getStats() {
      return {
        fileSize: 1024000,
        estimatedRows: 1000,
        compression: 'lz4',
        bloomFilterPresent: true
      };
    }

    async close() {
      // Mock close
    }

    queryStreamInternal(sql, callback) {
      // Mock streaming
      setTimeout(() => {
        callback({ name: 'John', age: 30 });
        callback({ name: 'Jane', age: 25 });
        callback({}); // End of stream
      }, 10);
    }
  },
  
  validateQuery: (sql) => {
    if (!sql.trim().toUpperCase().startsWith('SELECT')) {
      throw new Error('Only SELECT queries are supported');
    }
    return true;
  },
  
  parseSchema: async (path) => {
    const fs = require('fs').promises;
    const content = await fs.readFile(path, 'utf8');
    return JSON.parse(content);
  }
}), { virtual: true });