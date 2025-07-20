/**
 * Jest test setup for CQLite
 */

// Extend Jest matchers
expect.extend({
  toBeSSTableResult(received) {
    const pass = (
      received &&
      typeof received === 'object' &&
      Array.isArray(received.rows) &&
      typeof received.rowCount === 'number' &&
      typeof received.executionTime === 'number'
    );

    if (pass) {
      return {
        message: () => `expected ${received} not to be a valid SSTable result`,
        pass: true,
      };
    } else {
      return {
        message: () => `expected ${received} to be a valid SSTable result with rows, rowCount, and executionTime`,
        pass: false,
      };
    }
  },

  toBeValidCQLiteError(received) {
    const pass = (
      received &&
      received instanceof Error &&
      typeof received.code === 'string' &&
      received.name.includes('Error')
    );

    if (pass) {
      return {
        message: () => `expected ${received} not to be a valid CQLite error`,
        pass: true,
      };
    } else {
      return {
        message: () => `expected ${received} to be a valid CQLite error with code and name`,
        pass: false,
      };
    }
  }
});

// Global test utilities
global.testUtils = {
  createMockSSTablePath: () => '/mock/sstable-Data.db',
  createMockSchemaPath: () => '/mock/schema.json',
  
  // Common test queries
  queries: {
    simple: 'SELECT * FROM users',
    withWhere: 'SELECT name, email FROM users WHERE age > 25',
    withLimit: 'SELECT * FROM users LIMIT 10',
    withOrderBy: 'SELECT name, age FROM users ORDER BY age DESC',
    aggregation: 'SELECT COUNT(*) FROM users',
    groupBy: 'SELECT department, COUNT(*) FROM users GROUP BY department',
    complex: 'SELECT name, age FROM users WHERE age BETWEEN 25 AND 45 AND department IN (\'Engineering\', \'Product\') ORDER BY age DESC LIMIT 5'
  },

  // Mock data generators
  generateMockUser: (id = 1) => ({
    user_id: `123e4567-e89b-12d3-a456-42661417400${id}`,
    name: `User ${id}`,
    email: `user${id}@example.com`,
    age: 20 + (id * 5),
    department: id % 2 === 0 ? 'Engineering' : 'Product',
    active: true,
    created_date: new Date().toISOString()
  }),

  generateMockSchema: () => ({
    table: 'users',
    columns: [
      { name: 'user_id', type: 'uuid', primaryKey: true },
      { name: 'name', type: 'text' },
      { name: 'email', type: 'text' },
      { name: 'age', type: 'int' },
      { name: 'department', type: 'text' },
      { name: 'active', type: 'boolean' },
      { name: 'created_date', type: 'timestamp', clusteringKey: true }
    ]
  })
};

// Console override for cleaner test output
const originalConsoleLog = console.log;
const originalConsoleError = console.error;

console.log = (...args) => {
  if (!process.env.VERBOSE_TESTS) {
    return;
  }
  originalConsoleLog(...args);
};

console.error = (...args) => {
  if (!process.env.VERBOSE_TESTS) {
    return;
  }
  originalConsoleError(...args);
};

// Cleanup after tests
afterAll(async () => {
  // Restore console
  console.log = originalConsoleLog;
  console.error = originalConsoleError;
  
  // Cleanup any test files
  const fs = require('fs').promises;
  const path = require('path');
  
  try {
    const testDataDir = path.join(__dirname, 'test-data');
    await fs.rm(testDataDir, { recursive: true, force: true });
  } catch (error) {
    // Ignore cleanup errors
  }
});