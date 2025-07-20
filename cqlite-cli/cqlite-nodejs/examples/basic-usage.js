#!/usr/bin/env node

/**
 * CQLite Basic Usage Example
 * The FIRST EVER NodeJS package for direct SSTable SELECT queries!
 */

const { SSTableReader, Utils } = require('../lib/index.js');
const path = require('path');

async function basicExample() {
  console.log('🚀 CQLite NodeJS - First Ever Direct SSTable Querying!');
  console.log('==================================================\n');

  try {
    // Example SSTable file and schema (you'll need to provide these)
    const sstablePath = path.join(__dirname, '..', 'test-data', 'users-big-Data.db');
    const schemaPath = path.join(__dirname, '..', 'test-data', 'users_schema.json');

    console.log('📁 Opening SSTable:', sstablePath);
    console.log('📋 Using schema:', schemaPath);

    // Create SSTable reader
    const reader = new SSTableReader(sstablePath, {
      schema: schemaPath,
      compression: 'lz4',
      cacheSize: 64, // 64MB cache
      enableBloomFilter: true
    });

    console.log('\n✅ SSTable reader created successfully!');

    // Get schema information
    console.log('\n📊 Schema Information:');
    const schema = await reader.getSchema();
    console.log('Table:', schema.table);
    console.log('Columns:');
    schema.columns.forEach(col => {
      const markers = [];
      if (col.primaryKey) markers.push('PK');
      if (col.clusteringKey) markers.push('CK');
      console.log(`  - ${col.name} (${col.type})${markers.length ? ' [' + markers.join(', ') + ']' : ''}`);
    });

    // Get SSTable statistics
    console.log('\n📈 SSTable Statistics:');
    const stats = await reader.getStats();
    console.log(`File size: ${(stats.fileSize / 1024 / 1024).toFixed(2)} MB`);
    console.log(`Estimated rows: ${stats.estimatedRows.toLocaleString()}`);
    console.log(`Compression: ${stats.compression}`);
    console.log(`Bloom filter: ${stats.bloomFilterPresent ? 'Yes' : 'No'}`);

    // Execute SELECT queries - THE REVOLUTIONARY FEATURE!
    console.log('\n🔍 Executing SELECT Queries:');
    console.log('==============================');

    // Query 1: Simple SELECT with WHERE clause
    console.log('\n1️⃣ Simple SELECT with WHERE:');
    const query1 = 'SELECT name, email, age FROM users WHERE age > 25 LIMIT 10';
    console.log('Query:', query1);
    
    const result1 = await reader.query(query1);
    console.log(`Results: ${result1.rowCount} rows in ${result1.executionTime}ms`);
    
    if (result1.rows.length > 0) {
      console.log('Sample rows:');
      result1.rows.slice(0, 3).forEach((row, i) => {
        console.log(`  ${i + 1}. ${row.name} (${row.email}) - Age: ${row.age}`);
      });
    }

    // Query 2: COUNT aggregation
    console.log('\n2️⃣ COUNT Query:');
    const query2 = 'SELECT COUNT(*) as total_users FROM users WHERE active = true';
    console.log('Query:', query2);
    
    const result2 = await reader.query(query2);
    console.log(`Total active users: ${result2.rows[0]?.total_users || 0}`);

    // Query 3: Complex WHERE with multiple conditions
    console.log('\n3️⃣ Complex WHERE clause:');
    const query3 = 'SELECT name, email, department FROM users WHERE age BETWEEN 25 AND 45 AND department IN (\'Engineering\', \'Product\') LIMIT 5';
    console.log('Query:', query3);
    
    const result3 = await reader.query(query3);
    console.log(`Results: ${result3.rowCount} rows`);
    result3.rows.forEach((row, i) => {
      console.log(`  ${i + 1}. ${row.name} - ${row.department}`);
    });

    // Query 4: Projection with specific columns
    console.log('\n4️⃣ Column projection:');
    const query4 = 'SELECT user_id, name, created_date FROM users ORDER BY created_date DESC LIMIT 5';
    console.log('Query:', query4);
    
    const result4 = await reader.query(query4);
    console.log('Latest users:');
    result4.rows.forEach((row, i) => {
      console.log(`  ${i + 1}. ${row.name} (ID: ${row.user_id}) - ${row.created_date}`);
    });

    console.log('\n🎯 Performance Summary:');
    console.log('======================');
    const totalTime = result1.executionTime + result2.executionTime + result3.executionTime + result4.executionTime;
    const totalRows = result1.rowCount + result2.rowCount + result3.rowCount + result4.rowCount;
    console.log(`Total queries: 4`);
    console.log(`Total rows returned: ${totalRows}`);
    console.log(`Total execution time: ${totalTime}ms`);
    console.log(`Average query time: ${(totalTime / 4).toFixed(2)}ms`);

    // Close the reader
    await reader.close();
    console.log('\n✅ SSTable reader closed successfully');

  } catch (error) {
    console.error('❌ Error:', error.message);
    console.error('Details:', error.details || 'No additional details');
    
    if (error.code) {
      console.error('Error code:', error.code);
    }
  }
}

// Utility function to create test data if needed
async function createTestData() {
  const fs = require('fs').promises;
  const testDataDir = path.join(__dirname, '..', 'test-data');
  
  try {
    await fs.mkdir(testDataDir, { recursive: true });
    
    // Create example schema file
    const schema = {
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
    };
    
    await fs.writeFile(
      path.join(testDataDir, 'users_schema.json'),
      JSON.stringify(schema, null, 2)
    );
    
    console.log('📝 Test schema created at:', path.join(testDataDir, 'users_schema.json'));
    console.log('📋 You can now create a test SSTable file with Cassandra and place it at:');
    console.log('   ', path.join(testDataDir, 'users-big-Data.db'));
    
  } catch (error) {
    console.warn('Warning: Could not create test data directory:', error.message);
  }
}

// Check if this script is being run directly
if (require.main === module) {
  // First, create test data structure
  createTestData().then(() => {
    // Then run the example (will fail gracefully if no SSTable file exists)
    return basicExample();
  }).catch(error => {
    console.error('Failed to run example:', error.message);
    process.exit(1);
  });
}

module.exports = { basicExample, createTestData };