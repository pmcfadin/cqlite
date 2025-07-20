#!/usr/bin/env node

/**
 * CQLite Streaming Example
 * Demonstrates high-performance streaming of large SSTable results
 */

const { SSTableReader } = require('../lib/index.js');
const path = require('path');

async function streamingExample() {
  console.log('🌊 CQLite Streaming Query Example');
  console.log('==================================\n');

  try {
    const sstablePath = path.join(__dirname, '..', 'test-data', 'large_table-Data.db');
    const schemaPath = path.join(__dirname, '..', 'test-data', 'large_table_schema.json');

    console.log('📁 Opening large SSTable for streaming...');
    
    const reader = new SSTableReader(sstablePath, {
      schema: schemaPath,
      compression: 'lz4',
      cacheSize: 128, // Larger cache for streaming
      enableBloomFilter: true
    });

    // Example 1: Basic streaming
    console.log('\n1️⃣ Basic Streaming Query:');
    console.log('SELECT * FROM large_table WHERE status = \'active\'');
    
    let rowCount = 0;
    const startTime = Date.now();
    
    const stream = reader.queryStream('SELECT * FROM large_table WHERE status = \'active\'');
    
    stream.on('data', (row) => {
      rowCount++;
      
      // Process each row as it arrives
      if (rowCount % 1000 === 0) {
        const elapsed = Date.now() - startTime;
        const rate = (rowCount / elapsed * 1000).toFixed(0);
        console.log(`  Processed ${rowCount.toLocaleString()} rows (${rate} rows/sec)`);
      }
      
      // Example row processing
      if (row.priority === 'high') {
        console.log(`  🔥 High priority item: ${row.id} - ${row.description}`);
      }
    });
    
    stream.on('end', () => {
      const totalTime = Date.now() - startTime;
      const rate = (rowCount / totalTime * 1000).toFixed(0);
      console.log(`✅ Streaming complete: ${rowCount.toLocaleString()} rows in ${totalTime}ms (${rate} rows/sec)`);
    });
    
    stream.on('error', (error) => {
      console.error('❌ Streaming error:', error.message);
    });

    // Wait for stream to complete
    await new Promise((resolve, reject) => {
      stream.on('end', resolve);
      stream.on('error', reject);
    });

    // Example 2: Async Iterator (Modern JavaScript)
    console.log('\n2️⃣ Async Iterator Streaming:');
    console.log('SELECT id, name, score FROM large_table WHERE score > 80 ORDER BY score DESC');
    
    let topScores = [];
    let processedCount = 0;
    
    const asyncStream = reader.queryStream('SELECT id, name, score FROM large_table WHERE score > 80 ORDER BY score DESC');
    
    try {
      for await (const row of asyncStream) {
        processedCount++;
        
        // Collect top 10 scores
        if (topScores.length < 10) {
          topScores.push(row);
        }
        
        // Stop after processing 5000 rows for demo
        if (processedCount >= 5000) {
          break;
        }
      }
      
      console.log(`\n🏆 Top ${topScores.length} Scores:`);
      topScores.forEach((row, i) => {
        console.log(`  ${i + 1}. ${row.name}: ${row.score} (ID: ${row.id})`);
      });
      
    } catch (error) {
      console.error('❌ Async iteration error:', error.message);
    }

    // Example 3: Streaming with backpressure handling
    console.log('\n3️⃣ Streaming with Backpressure Control:');
    
    const { pipeline } = require('stream');
    const { Transform } = require('stream');
    
    // Create processing pipeline
    const processor = new Transform({
      objectMode: true,
      transform(row, encoding, callback) {
        // Simulate processing time
        setTimeout(() => {
          // Transform the data
          const processed = {
            ...row,
            processed_at: new Date().toISOString(),
            computed_field: row.value1 + row.value2
          };
          
          callback(null, processed);
        }, 1); // 1ms processing delay per row
      }
    });
    
    const writer = new Transform({
      objectMode: true,
      transform(row, encoding, callback) {
        // Write to console or database
        if (Math.random() < 0.01) { // Sample 1% for demo
          console.log(`  Processed: ${row.id} at ${row.processed_at}`);
        }
        callback();
      }
    });
    
    const sourceStream = reader.queryStream('SELECT * FROM large_table LIMIT 10000');
    
    await new Promise((resolve, reject) => {
      pipeline(
        sourceStream,
        processor,
        writer,
        (error) => {
          if (error) {
            console.error('❌ Pipeline error:', error.message);
            reject(error);
          } else {
            console.log('✅ Pipeline processing complete');
            resolve();
          }
        }
      );
    });

    // Example 4: Memory-efficient aggregation
    console.log('\n4️⃣ Memory-Efficient Aggregation:');
    
    let aggregation = {
      totalRows: 0,
      sumScore: 0,
      minScore: Infinity,
      maxScore: -Infinity,
      scoreHistogram: {}
    };
    
    const aggStream = reader.queryStream('SELECT score FROM large_table WHERE score IS NOT NULL');
    
    for await (const row of aggStream) {
      aggregation.totalRows++;
      aggregation.sumScore += row.score;
      aggregation.minScore = Math.min(aggregation.minScore, row.score);
      aggregation.maxScore = Math.max(aggregation.maxScore, row.score);
      
      // Histogram buckets
      const bucket = Math.floor(row.score / 10) * 10;
      aggregation.scoreHistogram[bucket] = (aggregation.scoreHistogram[bucket] || 0) + 1;
      
      // Process in chunks to avoid memory issues
      if (aggregation.totalRows >= 100000) break;
    }
    
    const avgScore = aggregation.sumScore / aggregation.totalRows;
    
    console.log('📊 Aggregation Results:');
    console.log(`  Total rows: ${aggregation.totalRows.toLocaleString()}`);
    console.log(`  Average score: ${avgScore.toFixed(2)}`);
    console.log(`  Min score: ${aggregation.minScore}`);
    console.log(`  Max score: ${aggregation.maxScore}`);
    console.log('  Score distribution:');
    
    Object.entries(aggregation.scoreHistogram)
      .sort(([a], [b]) => Number(a) - Number(b))
      .slice(0, 10) // Show top 10 buckets
      .forEach(([bucket, count]) => {
        console.log(`    ${bucket}-${Number(bucket) + 9}: ${count.toLocaleString()} rows`);
      });

    await reader.close();
    console.log('\n✅ All streaming examples completed successfully!');

  } catch (error) {
    console.error('❌ Error:', error.message);
    if (error.code === 'FILE_ERROR') {
      console.log('\n💡 Tip: Create a large SSTable file to test streaming:');
      console.log('   - Use Cassandra to create a table with millions of rows');
      console.log('   - Export the SSTable files to test-data/ directory');
      console.log('   - Update the schema file accordingly');
    }
  }
}

// Performance monitoring utility
class StreamPerformanceMonitor {
  constructor(name) {
    this.name = name;
    this.startTime = Date.now();
    this.rowCount = 0;
    this.lastReport = Date.now();
    this.reportInterval = 5000; // Report every 5 seconds
  }
  
  recordRow() {
    this.rowCount++;
    
    const now = Date.now();
    if (now - this.lastReport >= this.reportInterval) {
      this.report();
      this.lastReport = now;
    }
  }
  
  report() {
    const elapsed = Date.now() - this.startTime;
    const rate = (this.rowCount / elapsed * 1000).toFixed(0);
    console.log(`[${this.name}] ${this.rowCount.toLocaleString()} rows processed at ${rate} rows/sec`);
  }
  
  final() {
    const elapsed = Date.now() - this.startTime;
    const rate = (this.rowCount / elapsed * 1000).toFixed(0);
    console.log(`[${this.name}] FINAL: ${this.rowCount.toLocaleString()} rows in ${elapsed}ms (${rate} rows/sec)`);
  }
}

if (require.main === module) {
  streamingExample().catch(error => {
    console.error('Failed to run streaming example:', error.message);
    process.exit(1);
  });
}

module.exports = { streamingExample, StreamPerformanceMonitor };