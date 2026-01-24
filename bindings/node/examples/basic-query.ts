/**
 * Basic Query Example
 *
 * Demonstrates simple SELECT queries with @cqlite/node.
 *
 * Run: npx ts-node examples/basic-query.ts
 */

import { Database } from '@cqlite/node';

async function main() {
  // Configuration - adjust paths for your environment
  const dataDir = process.env.CQLITE_DATA_DIR || 'path/to/sstables';
  const schemaPath = process.env.CQLITE_SCHEMA || 'path/to/schema.cql';

  console.log('Opening database...');
  const db = await Database.open(dataDir, { schema: schemaPath });

  try {
    // Simple SELECT query
    console.log('\n--- Simple SELECT ---');
    const result = await db.execute('SELECT * FROM test_basic.simple_table LIMIT 5');

    console.log(`Returned ${result.rowCount} rows in ${result.executionTimeMs}ms`);

    // Iterate over rows
    for (const row of result.rows) {
      console.log(row);
    }

    // Access column metadata
    console.log('\n--- Column Metadata ---');
    for (const col of result.columns) {
      console.log(`  ${col.name}: ${col.dataType} (position: ${col.position})`);
    }

    // SELECT with specific columns
    console.log('\n--- SELECT Specific Columns ---');
    const projection = await db.execute(
      'SELECT id, name FROM test_basic.simple_table LIMIT 3'
    );

    for (const row of projection.rows) {
      console.log(`  id: ${row.id}, name: ${row.name}`);
    }

    // Query with WHERE clause (partition key)
    console.log('\n--- Query with WHERE ---');
    const filtered = await db.execute(
      "SELECT * FROM test_basic.simple_table WHERE id = 'user-1'"
    );

    console.log(`Found ${filtered.rowCount} rows matching filter`);

    // Get database statistics
    console.log('\n--- Database Stats ---');
    const stats = await db.getStats();
    console.log(`SSTable files: ${stats.totalSstables}`);
    console.log(`Total rows: ${stats.totalRows}`);
    console.log(`Memory used: ${stats.memoryUsedBytes} bytes`);

  } finally {
    // Always close the database
    await db.close();
    console.log('\nDatabase closed.');
  }
}

main().catch(console.error);
