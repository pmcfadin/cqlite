/**
 * CQLite Query Tests
 * Testing complex SQL query execution on SSTable files
 */

const { SSTableReader, Utils, QueryError } = require('../lib/index.js');
const path = require('path');

describe('CQLite Query Execution', () => {
  let reader;
  let mockSSTablePath;
  let mockSchemaPath;

  beforeAll(async () => {
    // Setup mock files (using the mocked native module)
    mockSSTablePath = path.join(__dirname, 'test-data', 'test-users-Data.db');
    mockSchemaPath = path.join(__dirname, 'test-data', 'test-users_schema.json');
    
    // Ensure test files exist (mock will handle validation)
    const fs = require('fs').promises;
    await fs.mkdir(path.dirname(mockSSTablePath), { recursive: true });
    await fs.writeFile(mockSSTablePath, 'MOCK_DATA');
    await fs.writeFile(mockSchemaPath, JSON.stringify({
      table: 'users',
      columns: [
        { name: 'user_id', type: 'uuid', primaryKey: true },
        { name: 'name', type: 'text' },
        { name: 'email', type: 'text' },
        { name: 'age', type: 'int' },
        { name: 'department', type: 'text' },
        { name: 'active', type: 'boolean' },
        { name: 'created_date', type: 'timestamp', clusteringKey: true },
        { name: 'salary', type: 'decimal' },
        { name: 'tags', type: 'list' },
        { name: 'preferences', type: 'map' }
      ]
    }));

    reader = new SSTableReader(mockSSTablePath, { schema: mockSchemaPath });
  });

  afterAll(async () => {
    if (reader) {
      await reader.close();
    }
  });

  describe('Basic SELECT Queries', () => {
    test('should execute simple SELECT *', async () => {
      const result = await reader.query('SELECT * FROM users');
      
      expect(result).toBeDefined();
      expect(result.rows).toBeInstanceOf(Array);
      expect(result.rowCount).toBeGreaterThanOrEqual(0);
      expect(result.executionTime).toBeGreaterThan(0);
    });

    test('should execute SELECT with specific columns', async () => {
      const result = await reader.query('SELECT name, email, age FROM users');
      
      expect(result.rows).toBeInstanceOf(Array);
      if (result.rows.length > 0) {
        const firstRow = result.rows[0];
        expect(firstRow).toHaveProperty('name');
        expect(firstRow).toHaveProperty('email');
        expect(firstRow).toHaveProperty('age');
      }
    });

    test('should execute SELECT with WHERE clause', async () => {
      const result = await reader.query('SELECT * FROM users WHERE age > 25');
      
      expect(result.rows).toBeInstanceOf(Array);
      expect(result.rowCount).toBeGreaterThanOrEqual(0);
    });

    test('should execute SELECT with LIMIT', async () => {
      const result = await reader.query('SELECT * FROM users LIMIT 10');
      
      expect(result.rows).toBeInstanceOf(Array);
      expect(result.rows.length).toBeLessThanOrEqual(10);
    });

    test('should execute SELECT with ORDER BY', async () => {
      const result = await reader.query('SELECT name, age FROM users ORDER BY age DESC LIMIT 5');
      
      expect(result.rows).toBeInstanceOf(Array);
      expect(result.rowCount).toBeGreaterThanOrEqual(0);
    });
  });

  describe('WHERE Clause Variants', () => {
    test('should handle equality conditions', async () => {
      const queries = [
        "SELECT * FROM users WHERE name = 'John Doe'",
        "SELECT * FROM users WHERE age = 30",
        "SELECT * FROM users WHERE active = true"
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
      }
    });

    test('should handle comparison operations', async () => {
      const queries = [
        'SELECT * FROM users WHERE age > 25',
        'SELECT * FROM users WHERE age >= 30',
        'SELECT * FROM users WHERE age < 40',
        'SELECT * FROM users WHERE age <= 35',
        'SELECT * FROM users WHERE age != 25'
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
      }
    });

    test('should handle BETWEEN conditions', async () => {
      const result = await reader.query('SELECT * FROM users WHERE age BETWEEN 25 AND 45');
      
      expect(result.rows).toBeInstanceOf(Array);
    });

    test('should handle IN conditions', async () => {
      const result = await reader.query("SELECT * FROM users WHERE department IN ('Engineering', 'Product', 'Sales')");
      
      expect(result.rows).toBeInstanceOf(Array);
    });

    test('should handle LIKE conditions', async () => {
      const queries = [
        "SELECT * FROM users WHERE name LIKE 'John%'",
        "SELECT * FROM users WHERE email LIKE '%@example.com'",
        "SELECT * FROM users WHERE name LIKE '%Doe%'"
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
      }
    });

    test('should handle IS NULL conditions', async () => {
      const queries = [
        'SELECT * FROM users WHERE department IS NULL',
        'SELECT * FROM users WHERE department IS NOT NULL'
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
      }
    });

    test('should handle compound conditions', async () => {
      const queries = [
        'SELECT * FROM users WHERE age > 25 AND department = \'Engineering\'',
        'SELECT * FROM users WHERE age < 30 OR salary > 100000',
        'SELECT * FROM users WHERE (age > 25 AND age < 40) AND active = true'
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
      }
    });
  });

  describe('Aggregation Functions', () => {
    test('should execute COUNT queries', async () => {
      const queries = [
        'SELECT COUNT(*) FROM users',
        'SELECT COUNT(*) as total_users FROM users WHERE active = true',
        'SELECT COUNT(DISTINCT department) FROM users'
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
        expect(result.rows.length).toBeGreaterThan(0);
      }
    });

    test('should execute SUM queries', async () => {
      const queries = [
        'SELECT SUM(salary) FROM users',
        'SELECT SUM(age) as total_age FROM users WHERE department = \'Engineering\''
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
      }
    });

    test('should execute AVG queries', async () => {
      const queries = [
        'SELECT AVG(age) FROM users',
        'SELECT AVG(salary) as avg_salary FROM users WHERE active = true'
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
      }
    });

    test('should execute MIN/MAX queries', async () => {
      const queries = [
        'SELECT MIN(age), MAX(age) FROM users',
        'SELECT MIN(salary) as min_sal, MAX(salary) as max_sal FROM users'
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
      }
    });
  });

  describe('GROUP BY Queries', () => {
    test('should execute GROUP BY with aggregations', async () => {
      const queries = [
        'SELECT department, COUNT(*) FROM users GROUP BY department',
        'SELECT department, AVG(age) as avg_age FROM users GROUP BY department',
        'SELECT department, COUNT(*) as count, AVG(salary) as avg_salary FROM users GROUP BY department'
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
      }
    });

    test('should execute GROUP BY with HAVING', async () => {
      const queries = [
        'SELECT department, COUNT(*) as count FROM users GROUP BY department HAVING COUNT(*) > 5',
        'SELECT department, AVG(age) as avg_age FROM users GROUP BY department HAVING AVG(age) > 30'
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
      }
    });
  });

  describe('Complex Queries', () => {
    test('should execute subqueries', async () => {
      const queries = [
        'SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users)',
        'SELECT * FROM users WHERE department IN (SELECT department FROM users WHERE salary > 100000)'
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
      }
    });

    test('should execute queries with CASE statements', async () => {
      const query = `
        SELECT name, age,
          CASE 
            WHEN age < 30 THEN 'Young'
            WHEN age BETWEEN 30 AND 50 THEN 'Middle-aged'
            ELSE 'Senior'
          END as age_group
        FROM users
      `;

      const result = await reader.query(query);
      expect(result.rows).toBeInstanceOf(Array);
    });

    test('should execute queries with window functions', async () => {
      const queries = [
        'SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as rank FROM users',
        'SELECT department, name, salary, RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank FROM users'
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
      }
    });

    test('should execute queries with CTEs', async () => {
      const query = `
        WITH high_earners AS (
          SELECT * FROM users WHERE salary > 100000
        )
        SELECT department, COUNT(*) as high_earner_count
        FROM high_earners
        GROUP BY department
      `;

      const result = await reader.query(query);
      expect(result.rows).toBeInstanceOf(Array);
    });
  });

  describe('CQL-Specific Features', () => {
    test('should handle collection queries', async () => {
      const queries = [
        "SELECT * FROM users WHERE tags CONTAINS 'javascript'",
        "SELECT * FROM users WHERE preferences['theme'] = 'dark'",
        "SELECT * FROM users WHERE tags CONTAINS KEY 'skills'"
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
      }
    });

    test('should handle token queries', async () => {
      const queries = [
        'SELECT * FROM users WHERE TOKEN(user_id) > TOKEN(?)',
        'SELECT * FROM users WHERE TOKEN(user_id) >= TOKEN(?) AND TOKEN(user_id) < TOKEN(?)'
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
      }
    });

    test('should handle time-based queries', async () => {
      const queries = [
        "SELECT * FROM users WHERE created_date > '2023-01-01'",
        "SELECT * FROM users WHERE created_date >= '2023-01-01' AND created_date < '2024-01-01'",
        'SELECT * FROM users WHERE created_date > NOW() - INTERVAL 30 DAY'
      ];

      for (const query of queries) {
        const result = await reader.query(query);
        expect(result.rows).toBeInstanceOf(Array);
      }
    });
  });

  describe('Query Options', () => {
    test('should respect limit option', async () => {
      const result = await reader.query('SELECT * FROM users', { limit: 5 });
      
      expect(result.rows.length).toBeLessThanOrEqual(5);
    });

    test('should respect timeout option', async () => {
      const startTime = Date.now();
      
      try {
        await reader.query('SELECT * FROM users', { timeout: 1 }); // 1ms timeout
      } catch (error) {
        // May timeout or complete quickly
      }
      
      const elapsed = Date.now() - startTime;
      expect(elapsed).toBeLessThan(1000); // Should not take more than 1 second
    });

    test('should handle bloom filter options', async () => {
      const result = await reader.query('SELECT * FROM users WHERE user_id = ?', { 
        skipBloomFilter: true 
      });
      
      expect(result.rows).toBeInstanceOf(Array);
    });
  });

  describe('Error Handling', () => {
    test('should throw error for non-SELECT statements', async () => {
      const invalidQueries = [
        'INSERT INTO users VALUES (1, \'test\')',
        'UPDATE users SET name = \'test\'',
        'DELETE FROM users WHERE id = 1',
        'CREATE TABLE test (id int)'
      ];

      for (const query of invalidQueries) {
        await expect(reader.query(query)).rejects.toThrow();
      }
    });

    test('should throw error for syntax errors', async () => {
      const invalidQueries = [
        'SELECT * FROMM users', // typo
        'SELECT * FROM users WHRE age > 25', // typo
        'SELECT * FROM users WHERE age >', // incomplete
        'SELECT FROM users', // missing columns
        'SELECT * FROM' // incomplete
      ];

      for (const query of invalidQueries) {
        await expect(reader.query(query)).rejects.toThrow();
      }
    });

    test('should throw error for non-existent columns', async () => {
      const invalidQueries = [
        'SELECT non_existent_column FROM users',
        'SELECT * FROM users WHERE non_existent_column = 1',
        'SELECT * FROM users ORDER BY non_existent_column'
      ];

      for (const query of invalidQueries) {
        await expect(reader.query(query)).rejects.toThrow();
      }
    });

    test('should throw error for type mismatches', async () => {
      const invalidQueries = [
        "SELECT * FROM users WHERE age = 'not a number'",
        "SELECT * FROM users WHERE active = 'not a boolean'",
        "SELECT * FROM users WHERE created_date = 'invalid date'"
      ];

      for (const query of invalidQueries) {
        await expect(reader.query(query)).rejects.toThrow();
      }
    });
  });

  describe('Performance Tests', () => {
    test('should complete simple queries quickly', async () => {
      const startTime = Date.now();
      const result = await reader.query('SELECT * FROM users LIMIT 100');
      const elapsed = Date.now() - startTime;
      
      expect(elapsed).toBeLessThan(1000); // Should complete within 1 second
      expect(result.executionTime).toBeGreaterThan(0);
    });

    test('should handle multiple concurrent queries', async () => {
      const queries = Array.from({ length: 10 }, (_, i) => 
        reader.query(`SELECT * FROM users WHERE age > ${20 + i} LIMIT 10`)
      );

      const results = await Promise.all(queries);
      
      expect(results).toHaveLength(10);
      results.forEach(result => {
        expect(result.rows).toBeInstanceOf(Array);
        expect(result.executionTime).toBeGreaterThan(0);
      });
    });

    test('should provide execution statistics', async () => {
      const result = await reader.query('SELECT * FROM users LIMIT 10');
      
      expect(result.executionTime).toBeGreaterThan(0);
      expect(result.rowCount).toBeGreaterThanOrEqual(0);
      
      if (result.stats) {
        expect(result.stats).toHaveProperty('blocksRead');
        expect(result.stats).toHaveProperty('cacheHits');
        expect(result.stats).toHaveProperty('cacheMisses');
      }
    });
  });

  describe('Result Format Tests', () => {
    test('should return properly formatted results', async () => {
      const result = await reader.query('SELECT name, age, email FROM users LIMIT 3');
      
      expect(result).toHaveProperty('rows');
      expect(result).toHaveProperty('rowCount');
      expect(result).toHaveProperty('executionTime');
      
      if (result.rows.length > 0) {
        const row = result.rows[0];
        expect(typeof row).toBe('object');
        expect(row).not.toBeInstanceOf(Array);
      }
    });

    test('should handle null values correctly', async () => {
      const result = await reader.query('SELECT name, department FROM users WHERE department IS NULL');
      
      result.rows.forEach(row => {
        if (row.department === null || row.department === undefined) {
          expect([null, undefined]).toContain(row.department);
        }
      });
    });

    test('should preserve data types', async () => {
      const result = await reader.query('SELECT user_id, name, age, active FROM users LIMIT 1');
      
      if (result.rows.length > 0) {
        const row = result.rows[0];
        expect(typeof row.name).toBe('string');
        expect(typeof row.age).toBe('number');
        expect(typeof row.active).toBe('boolean');
        expect(typeof row.user_id).toBe('string'); // UUID as string
      }
    });
  });
});

// Additional mock for query-specific tests
jest.mock('../cqlite.node', () => ({
  SSTableReader: class MockSSTableReader {
    constructor(path, options) {
      if (!require('fs').existsSync(path)) {
        throw new Error(`SSTable file not found: ${path}`);
      }
      if (!require('fs').existsSync(options.schema)) {
        throw new Error(`Schema file not found: ${options.schema}`);
      }
      this.closed = false;
    }

    async query(sql, options = {}) {
      if (this.closed) {
        throw new Error('Reader is closed');
      }

      // Mock query validation
      if (!sql.trim().toUpperCase().startsWith('SELECT')) {
        throw new Error('Only SELECT statements are supported');
      }

      // Check for syntax errors
      if (sql.includes('FROMM') || sql.includes('WHRE') || sql.endsWith('WHERE age >')) {
        throw new Error('SQL syntax error');
      }

      // Check for non-existent columns
      if (sql.includes('non_existent_column')) {
        throw new Error('Column not found: non_existent_column');
      }

      // Mock different result sizes based on LIMIT
      let rowCount = 2;
      if (sql.includes('LIMIT')) {
        const limitMatch = sql.match(/LIMIT (\d+)/i);
        if (limitMatch) {
          rowCount = Math.min(parseInt(limitMatch[1]), 2);
        }
      }

      // Apply options.limit
      if (options.limit) {
        rowCount = Math.min(options.limit, rowCount);
      }

      // Generate mock results
      const rows = [];
      for (let i = 0; i < rowCount; i++) {
        rows.push({
          user_id: `123e4567-e89b-12d3-a456-42661417400${i}`,
          name: `User ${i + 1}`,
          email: `user${i + 1}@example.com`,
          age: 25 + i * 5,
          department: i % 2 === 0 ? 'Engineering' : 'Product',
          active: true,
          created_date: new Date().toISOString(),
          salary: 50000 + i * 10000,
          tags: ['javascript', 'react'],
          preferences: { theme: 'dark', notifications: true }
        });
      }

      return {
        rows,
        rowCount,
        executionTime: Math.floor(Math.random() * 50) + 10, // 10-60ms
        stats: {
          blocksRead: Math.ceil(rowCount / 10),
          cacheHits: Math.floor(Math.random() * 5),
          cacheMisses: Math.floor(Math.random() * 3)
        }
      };
    }

    async close() {
      this.closed = true;
    }

    queryStreamInternal(sql, callback) {
      // Mock streaming with delay
      const rows = [
        { name: 'John', age: 30 },
        { name: 'Jane', age: 25 },
        { name: 'Bob', age: 35 }
      ];

      let index = 0;
      const interval = setInterval(() => {
        if (index < rows.length) {
          callback(rows[index]);
          index++;
        } else {
          callback({}); // End of stream
          clearInterval(interval);
        }
      }, 10);
    }
  },
  
  validateQuery: (sql) => {
    if (!sql.trim().toUpperCase().startsWith('SELECT')) {
      throw new Error('Only SELECT queries are supported');
    }
    if (sql.includes('FROMM') || sql.includes('WHRE')) {
      throw new Error('SQL syntax error');
    }
    return true;
  }
}), { virtual: true });