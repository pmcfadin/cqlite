/**
 * Performance Example
 *
 * Demonstrates memory-optimized usage patterns for @cqlite/node:
 * - Efficient query patterns
 * - Memory management
 * - Connection pooling patterns
 * - Parallel query execution
 *
 * Run: npx ts-node examples/performance.ts
 */

import { Database, StreamingConfig } from '@cqlite/node';

async function main() {
  const dataDir = process.env.CQLITE_DATA_DIR || 'path/to/sstables';
  const schemaPath = process.env.CQLITE_SCHEMA || 'path/to/schema.cql';

  console.log('=== Performance Patterns ===\n');

  // =============================================
  // Pattern 1: Reuse database connections
  // =============================================
  console.log('--- Pattern 1: Connection Reuse ---');

  const db = await Database.open(dataDir, { schema: schemaPath });
  const startTime = Date.now();

  try {
    // Good: Reuse the same connection for multiple queries
    for (let i = 0; i < 5; i++) {
      await db.execute('SELECT * FROM test_basic.simple_table LIMIT 10');
    }

    console.log(`5 queries with connection reuse: ${Date.now() - startTime}ms`);
  } finally {
    await db.close();
  }

  // =============================================
  // Pattern 2: Project only needed columns
  // =============================================
  console.log('\n--- Pattern 2: Column Projection ---');

  const db2 = await Database.open(dataDir, { schema: schemaPath });

  try {
    // Less efficient: SELECT *
    const t1 = Date.now();
    await db2.execute('SELECT * FROM test_basic.simple_table LIMIT 100');
    console.log(`SELECT * : ${Date.now() - t1}ms`);

    // More efficient: Only select needed columns
    const t2 = Date.now();
    await db2.execute('SELECT id, name FROM test_basic.simple_table LIMIT 100');
    console.log(`SELECT id, name: ${Date.now() - t2}ms`);
  } finally {
    await db2.close();
  }

  // =============================================
  // Pattern 3: Use LIMIT appropriately
  // =============================================
  console.log('\n--- Pattern 3: Appropriate LIMIT ---');

  const db3 = await Database.open(dataDir, { schema: schemaPath });

  try {
    // Avoid fetching more than needed
    const small = await db3.execute('SELECT * FROM test_basic.simple_table LIMIT 10');
    console.log(`LIMIT 10: ${small.rowCount} rows, ${small.executionTimeMs}ms`);

    const medium = await db3.execute('SELECT * FROM test_basic.simple_table LIMIT 100');
    console.log(`LIMIT 100: ${medium.rowCount} rows, ${medium.executionTimeMs}ms`);
  } finally {
    await db3.close();
  }

  // =============================================
  // Pattern 4: execute() vs executeNative()
  // =============================================
  console.log('\n--- Pattern 4: execute() vs executeNative() ---');

  const db4 = await Database.open(dataDir, { schema: schemaPath });

  try {
    // execute(): Faster for simple serialization needs
    const t1 = Date.now();
    const jsonResult = await db4.execute('SELECT * FROM test_basic.simple_table LIMIT 50');
    console.log(`execute() (JSON-serializable): ${Date.now() - t1}ms`);

    // executeNative(): Use when you need native types (BigInt, Date, Set, Map)
    const t2 = Date.now();
    const nativeResult = await db4.executeNative('SELECT * FROM test_basic.simple_table LIMIT 50');
    console.log(`executeNative() (native types): ${Date.now() - t2}ms`);

    // Recommendation:
    // - Use execute() for JSON responses (APIs, logging)
    // - Use executeNative() for data processing (calculations, date math)
  } finally {
    await db4.close();
  }

  // =============================================
  // Pattern 5: Memory-conscious configuration
  // =============================================
  console.log('\n--- Pattern 5: StreamingConfig for Memory ---');

  // For memory-constrained environments
  const memoryOptimizedConfig: StreamingConfig = {
    bufferSize: 256,    // Smaller buffer
    chunkSize: 1000,    // Smaller chunks
  };
  console.log('Memory-optimized config:', memoryOptimizedConfig);

  // For high-throughput environments
  const performanceConfig: StreamingConfig = {
    bufferSize: 4096,   // Larger buffer
    chunkSize: 50000,   // Larger chunks
  };
  console.log('Performance-optimized config:', performanceConfig);

  // Calculate approximate memory usage
  const estimateMemoryMB = (config: StreamingConfig): number => {
    // Rough estimate: 1KB per row average
    return ((config.bufferSize || 1024) + (config.chunkSize || 10000)) / 1024;
  };

  console.log(`Memory-optimized estimate: ~${estimateMemoryMB(memoryOptimizedConfig)}MB`);
  console.log(`Performance-optimized estimate: ~${estimateMemoryMB(performanceConfig)}MB`);

  // =============================================
  // Pattern 6: Parallel queries (with caution)
  // =============================================
  console.log('\n--- Pattern 6: Parallel Queries ---');

  const db6 = await Database.open(dataDir, { schema: schemaPath });

  try {
    const tables = [
      'test_basic.simple_table',
      'test_basic.simple_table',
      'test_basic.simple_table',
    ];

    // Sequential execution
    const seqStart = Date.now();
    for (const table of tables) {
      await db6.execute(`SELECT * FROM ${table} LIMIT 10`);
    }
    console.log(`Sequential (3 queries): ${Date.now() - seqStart}ms`);

    // Parallel execution
    const parStart = Date.now();
    await Promise.all(
      tables.map(table => db6.execute(`SELECT * FROM ${table} LIMIT 10`))
    );
    console.log(`Parallel (3 queries): ${Date.now() - parStart}ms`);

    // Note: Parallel queries share the same database handle.
    // For CPU-bound workloads, consider worker_threads with separate connections.
  } finally {
    await db6.close();
  }

  // =============================================
  // Pattern 7: Proper cleanup
  // =============================================
  console.log('\n--- Pattern 7: Proper Cleanup ---');

  async function processData(): Promise<void> {
    const db = await Database.open(dataDir, { schema: schemaPath });

    try {
      const result = await db.execute('SELECT * FROM test_basic.simple_table LIMIT 5');
      console.log(`Processed ${result.rowCount} rows`);
    } finally {
      // Always close in finally block
      await db.close();
    }
  }

  await processData();
  console.log('Database properly closed');

  // =============================================
  // Summary
  // =============================================
  console.log('\n=== Performance Summary ===');
  console.log('1. Reuse database connections (avoid open/close per query)');
  console.log('2. Project only needed columns (avoid SELECT *)');
  console.log('3. Use appropriate LIMIT (fetch only what you need)');
  console.log('4. Use execute() for JSON, executeNative() for processing');
  console.log('5. Configure StreamingConfig for your memory constraints');
  console.log('6. Parallel queries can help, but measure first');
  console.log('7. Always close database connections in finally blocks');
}

main().catch(console.error);
