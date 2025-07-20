#!/usr/bin/env ts-node

/**
 * CQLite TypeScript Example
 * Demonstrates type-safe SSTable querying with full TypeScript support
 */

import { SSTableReader, createTypedReader, Utils, QueryResult, SSTableOptions } from '../index';
import * as path from 'path';

// Define types for our data
interface User {
  user_id: string;
  name: string;
  email: string;
  age: number;
  department: string;
  active: boolean;
  created_date: Date;
  preferences?: {
    theme: string;
    notifications: boolean;
  };
  skills?: string[];
}

interface UserStats {
  department: string;
  count: number;
  avg_age: number;
  active_users: number;
}

async function typescriptExample(): Promise<void> {
  console.log('🔷 CQLite TypeScript Example');
  console.log('============================\n');

  try {
    const sstablePath = path.join(__dirname, '..', 'test-data', 'users-big-Data.db');
    const schemaPath = path.join(__dirname, '..', 'test-data', 'users_schema.json');

    console.log('📁 Opening SSTable with TypeScript type safety...');

    // Option 1: Standard reader with manual type assertions
    const options: SSTableOptions = {
      schema: schemaPath,
      compression: 'lz4',
      cacheSize: 64,
      enableBloomFilter: true
    };

    const reader = new SSTableReader(sstablePath, options);

    // Get schema information
    const schema = await reader.getSchema();
    console.log(`\n📋 Schema for table '${schema.table}':`);
    schema.columns.forEach(col => {
      const markers: string[] = [];
      if (col.primaryKey) markers.push('PK');
      if (col.clusteringKey) markers.push('CK');
      console.log(`  - ${col.name}: ${col.type}${markers.length ? ' [' + markers.join(', ') + ']' : ''}`);
    });

    // Type-safe querying
    console.log('\n🔍 Type-safe Queries:');
    console.log('=====================');

    // Query 1: Basic type-safe query
    console.log('\n1️⃣ Basic type-safe query:');
    const result1: QueryResult = await reader.query(
      'SELECT name, email, age, department FROM users WHERE age > 25 LIMIT 10'
    );

    // Process results with type safety
    const users: User[] = result1.rows.map(row => ({
      user_id: row.user_id as string,
      name: row.name as string,
      email: row.email as string,
      age: row.age as number,
      department: row.department as string,
      active: row.active as boolean,
      created_date: new Date(row.created_date as string),
      skills: row.skills as string[] || []
    }));

    console.log(`Found ${users.length} users:`);
    users.slice(0, 3).forEach((user, i) => {
      console.log(`  ${i + 1}. ${user.name} (${user.department}) - Age: ${user.age}`);
    });

    // Option 2: Typed reader with full type safety
    console.log('\n2️⃣ Fully typed reader:');
    const typedReader = createTypedReader<User>(sstablePath, options);

    const typedResult = await typedReader.query(
      'SELECT * FROM users WHERE department = \'Engineering\' LIMIT 5'
    );

    console.log('\nEngineering team members:');
    typedResult.rows.forEach((user: User, i: number) => {
      console.log(`  ${i + 1}. ${user.name} - ${user.email}`);
      if (user.skills && user.skills.length > 0) {
        console.log(`     Skills: ${user.skills.join(', ')}`);
      }
    });

    // Query 3: Async iteration with types
    console.log('\n3️⃣ Async iteration with TypeScript:');
    const stream = typedReader.queryStream('SELECT name, age, department FROM users WHERE active = true LIMIT 20');
    
    let departmentCounts: Record<string, number> = {};
    let totalAge = 0;
    let userCount = 0;

    for await (const user of stream) {
      // TypeScript knows user is of type User
      departmentCounts[user.department] = (departmentCounts[user.department] || 0) + 1;
      totalAge += user.age;
      userCount++;
    }

    const avgAge = userCount > 0 ? (totalAge / userCount).toFixed(1) : 0;
    console.log(`Average age of active users: ${avgAge}`);
    console.log('Department distribution:');
    Object.entries(departmentCounts)
      .sort(([,a], [,b]) => b - a)
      .forEach(([dept, count]) => {
        console.log(`  ${dept}: ${count} users`);
      });

    // Query 4: Complex aggregation with type safety
    console.log('\n4️⃣ Complex aggregation:');
    
    interface DepartmentStats {
      department: string;
      user_count: number;
      avg_age: number;
      active_percentage: number;
    }

    // Simulate aggregation query (would be actual SQL in real implementation)
    const deptStatsQuery = `
      SELECT 
        department,
        COUNT(*) as user_count,
        AVG(age) as avg_age,
        (COUNT(CASE WHEN active = true THEN 1 END) * 100.0 / COUNT(*)) as active_percentage
      FROM users 
      GROUP BY department 
      ORDER BY user_count DESC
    `;

    const statsResult = await reader.query(deptStatsQuery);
    const departmentStats: DepartmentStats[] = statsResult.rows.map(row => ({
      department: row.department as string,
      user_count: row.user_count as number,
      avg_age: row.avg_age as number,
      active_percentage: row.active_percentage as number
    }));

    console.log('Department Statistics:');
    departmentStats.forEach(stats => {
      console.log(`  ${stats.department}:`);
      console.log(`    Users: ${stats.user_count}`);
      console.log(`    Avg Age: ${stats.avg_age.toFixed(1)} years`);
      console.log(`    Active: ${stats.active_percentage.toFixed(1)}%`);
    });

    // Query 5: Working with complex types
    console.log('\n5️⃣ Complex CQL types:');
    
    interface UserWithComplexTypes {
      user_id: string;
      name: string;
      preferences: Map<string, any>;
      skills: Set<string>;
      project_history: Array<{
        project: string;
        role: string;
        duration: number;
      }>;
    }

    const complexQuery = 'SELECT user_id, name, preferences, skills, project_history FROM users WHERE user_id IN (?, ?, ?) LIMIT 3';
    const complexResult = await reader.query(complexQuery);

    complexResult.rows.forEach((row, i) => {
      console.log(`\n  User ${i + 1}: ${row.name}`);
      
      // Handle Map type
      if (row.preferences && typeof row.preferences === 'object') {
        const prefs = new Map(Object.entries(row.preferences));
        console.log(`    Preferences: ${Array.from(prefs.entries()).map(([k, v]) => `${k}=${v}`).join(', ')}`);
      }
      
      // Handle Set type
      if (Array.isArray(row.skills)) {
        const skills = new Set(row.skills);
        console.log(`    Skills: ${Array.from(skills).join(', ')}`);
      }
      
      // Handle List of complex objects
      if (Array.isArray(row.project_history)) {
        console.log(`    Projects: ${row.project_history.length} entries`);
        row.project_history.slice(0, 2).forEach((project: any) => {
          console.log(`      - ${project.project} (${project.role})`);
        });
      }
    });

    // Performance monitoring with types
    console.log('\n6️⃣ Performance monitoring:');
    
    interface PerformanceMetrics {
      query: string;
      executionTime: number;
      rowsReturned: number;
      throughput: number; // rows per second
    }

    const performanceTests: Array<{ name: string; query: string }> = [
      { name: 'Simple WHERE', query: 'SELECT * FROM users WHERE age > 30 LIMIT 100' },
      { name: 'Complex WHERE', query: 'SELECT * FROM users WHERE age BETWEEN 25 AND 45 AND department IN (\'Engineering\', \'Product\') LIMIT 50' },
      { name: 'ORDER BY', query: 'SELECT name, age FROM users ORDER BY age DESC LIMIT 25' },
      { name: 'Aggregation', query: 'SELECT department, COUNT(*) FROM users GROUP BY department' }
    ];

    const metrics: PerformanceMetrics[] = [];

    for (const test of performanceTests) {
      const startTime = Date.now();
      const result = await reader.query(test.query);
      const endTime = Date.now();
      
      const metric: PerformanceMetrics = {
        query: test.name,
        executionTime: result.executionTime,
        rowsReturned: result.rowCount,
        throughput: result.rowCount / (result.executionTime / 1000)
      };
      
      metrics.push(metric);
      console.log(`  ${test.name}: ${metric.rowsReturned} rows in ${metric.executionTime}ms (${metric.throughput.toFixed(0)} rows/sec)`);
    }

    // Error handling with typed errors
    console.log('\n7️⃣ Type-safe error handling:');
    
    try {
      await reader.query('INVALID SQL QUERY');
    } catch (error) {
      if (error instanceof Error) {
        console.log(`  Caught error: ${error.message}`);
        console.log(`  Error type: ${error.constructor.name}`);
      }
    }

    // Utility functions with TypeScript
    console.log('\n8️⃣ Utility functions:');
    
    // Validate query syntax
    const testQueries = [
      'SELECT * FROM users',
      'SELECT name, age FROM users WHERE age > 25',
      'INVALID QUERY SYNTAX'
    ];

    for (const query of testQueries) {
      try {
        const isValid = Utils.validateQuery(query);
        console.log(`  ✅ "${query.substring(0, 30)}..." is valid: ${isValid}`);
      } catch (error) {
        console.log(`  ❌ "${query.substring(0, 30)}..." is invalid: ${error instanceof Error ? error.message : 'Unknown error'}`);
      }
    }

    // Clean up
    await reader.close();
    await typedReader.close();
    
    console.log('\n✅ TypeScript example completed successfully!');
    console.log('🎯 Key Benefits:');
    console.log('   - Full type safety for query results');
    console.log('   - IntelliSense support in IDEs');
    console.log('   - Compile-time error checking');
    console.log('   - Better refactoring support');
    console.log('   - Self-documenting code');

  } catch (error) {
    console.error('❌ Error:', error instanceof Error ? error.message : 'Unknown error');
    
    if (error instanceof Error && 'code' in error) {
      console.error('Error code:', (error as any).code);
    }
    
    console.log('\n💡 TypeScript Tips:');
    console.log('   - Define interfaces for your table schemas');
    console.log('   - Use createTypedReader for full type safety');
    console.log('   - Handle CQL complex types (Map, Set, List) appropriately');
    console.log('   - Use proper error handling with typed catch blocks');
  }
}

// Advanced TypeScript patterns
namespace CQLiteAdvanced {
  // Generic query builder
  export class TypedQueryBuilder<T> {
    private tableName: string;
    private reader: ReturnType<typeof createTypedReader<T>>;

    constructor(tableName: string, reader: ReturnType<typeof createTypedReader<T>>) {
      this.tableName = tableName;
      this.reader = reader;
    }

    select<K extends keyof T>(...columns: K[]): QueryChain<Pick<T, K>> {
      return new QueryChain(this.reader, `SELECT ${columns.join(', ')} FROM ${this.tableName}`);
    }

    selectAll(): QueryChain<T> {
      return new QueryChain(this.reader, `SELECT * FROM ${this.tableName}`);
    }
  }

  export class QueryChain<T> {
    private reader: any;
    private query: string;

    constructor(reader: any, query: string) {
      this.reader = reader;
      this.query = query;
    }

    where(condition: string): QueryChain<T> {
      this.query += ` WHERE ${condition}`;
      return this;
    }

    orderBy(column: keyof T, direction: 'ASC' | 'DESC' = 'ASC'): QueryChain<T> {
      this.query += ` ORDER BY ${String(column)} ${direction}`;
      return this;
    }

    limit(count: number): QueryChain<T> {
      this.query += ` LIMIT ${count}`;
      return this;
    }

    async execute(): Promise<T[]> {
      const result = await this.reader.query(this.query);
      return result.rows;
    }

    stream(): NodeJS.ReadableStream & { [Symbol.asyncIterator](): AsyncIterableIterator<T> } {
      return this.reader.queryStream(this.query);
    }
  }
}

if (require.main === module) {
  typescriptExample().catch(error => {
    console.error('Failed to run TypeScript example:', error.message);
    process.exit(1);
  });
}

export { typescriptExample, CQLiteAdvanced };