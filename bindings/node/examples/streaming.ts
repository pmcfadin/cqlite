/**
 * Streaming Example
 *
 * Demonstrates memory-efficient streaming for large result sets.
 *
 * Note: Full AsyncIterator streaming (Issue #305) is planned for future release.
 * This example shows current best practices for handling large datasets.
 *
 * Run: npx ts-node examples/streaming.ts
 */

import { Database, StreamingConfig } from '@cqlite/node';

async function main() {
  const dataDir = process.env.CQLITE_DATA_DIR || 'path/to/sstables';
  const schemaPath = process.env.CQLITE_SCHEMA || 'path/to/schema.cql';

  const db = await Database.open(dataDir, { schema: schemaPath });

  try {
    // =============================================
    // Current approach: Batch processing with LIMIT
    // =============================================
    console.log('=== Batch Processing Large Tables ===\n');

    // Note: CQL does not support OFFSET. For pagination, use:
    // - Token-based pagination with WHERE token(pk) > token(last_pk)
    // - Or process all rows in a single query with LIMIT

    const batchSize = 100;
    let totalProcessed = 0;

    console.log(`Processing with LIMIT ${batchSize}...`);

    // Single batch example - in production, use token-based pagination
    const result = await db.execute(
      `SELECT * FROM test_basic.simple_table LIMIT ${batchSize}`
    );

    // Process the batch
    for (const row of result.rows) {
      totalProcessed++;
      // Process row...
    }

    console.log(`  Processed ${totalProcessed} rows`);
    console.log(`  Query time: ${result.executionTimeMs}ms`);

    // For larger datasets, use token-based pagination:
    // let lastToken = null;
    // while (hasMore) {
    //   const query = lastToken
    //     ? `SELECT * FROM table WHERE token(pk) > token('${lastPk}') LIMIT ${batchSize}`
    //     : `SELECT * FROM table LIMIT ${batchSize}`;
    //   const result = await db.execute(query);
    //   // ... process rows, track lastPk ...
    // }

    console.log(`\nTotal rows processed: ${totalProcessed}`);

    // =============================================
    // StreamingConfig for memory control
    // =============================================
    console.log('\n=== StreamingConfig Options ===\n');

    // StreamingConfig is available for future streaming API
    const config: StreamingConfig = {
      bufferSize: 512,    // Rows to buffer in memory
      chunkSize: 1000,    // Rows per chunk
    };

    console.log(`StreamingConfig:`);
    console.log(`  bufferSize: ${config.bufferSize} rows`);
    console.log(`  chunkSize: ${config.chunkSize} rows`);
    console.log(`  Estimated memory: ${estimateMemory(config)} MB`);

    // =============================================
    // Progress tracking pattern
    // =============================================
    console.log('\n=== Progress Tracking Pattern ===\n');

    await processWithProgress(db, 'test_basic.simple_table', (progress) => {
      console.log(`  Progress: ${progress.processed} rows (${progress.percent.toFixed(1)}%)`);
    });

    // =============================================
    // Memory-efficient aggregation
    // =============================================
    console.log('\n=== Memory-Efficient Aggregation ===\n');

    const stats = await aggregateInBatches(db, 'test_basic.simple_table', 500);
    console.log(`Aggregation results:`);
    console.log(`  Total rows: ${stats.count}`);
    console.log(`  Non-null values: ${stats.nonNullCount}`);

  } finally {
    await db.close();
  }
}

/**
 * Estimate memory usage for StreamingConfig
 */
function estimateMemory(config: StreamingConfig): number {
  // Rough estimate: assume 1KB per row on average
  const rowSizeKb = 1;
  const totalRows = (config.bufferSize || 1024) + (config.chunkSize || 10000);
  return (totalRows * rowSizeKb) / 1024; // Convert to MB
}

/**
 * Process a table with progress callback
 */
async function processWithProgress(
  db: InstanceType<typeof Database>,
  table: string,
  onProgress: (progress: { processed: number; total: number; percent: number }) => void
): Promise<void> {
  // First, get total count (for progress calculation)
  // Note: In production, you might cache or estimate this
  const countResult = await db.execute(`SELECT * FROM ${table} LIMIT 10000`);
  const estimatedTotal = countResult.rowCount;

  const batchSize = 100;
  let processed = 0;

  const result = await db.execute(`SELECT * FROM ${table} LIMIT ${batchSize}`);

  for (const row of result.rows) {
    processed++;

    // Report progress every 10 rows
    if (processed % 10 === 0 || processed === result.rowCount) {
      onProgress({
        processed,
        total: estimatedTotal,
        percent: (processed / estimatedTotal) * 100,
      });
    }
  }
}

/**
 * Aggregate data in batches to avoid memory spikes
 */
async function aggregateInBatches(
  db: InstanceType<typeof Database>,
  table: string,
  batchSize: number
): Promise<{ count: number; nonNullCount: number }> {
  let count = 0;
  let nonNullCount = 0;

  const result = await db.execute(`SELECT * FROM ${table} LIMIT ${batchSize}`);

  for (const row of result.rows) {
    count++;

    // Count non-null values in each row
    for (const key of Object.keys(row)) {
      if (row[key] !== null) {
        nonNullCount++;
      }
    }
  }

  return { count, nonNullCount };
}

main().catch(console.error);
