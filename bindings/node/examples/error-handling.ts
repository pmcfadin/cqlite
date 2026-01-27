/**
 * Error Handling Example
 *
 * Demonstrates error handling patterns with @cqlite/node, including:
 * - Error codes and categories
 * - Recovery strategies
 * - Common error scenarios
 *
 * Run: npx ts-node examples/error-handling.ts
 */

import { Database } from '@cqlite/node';

// CqliteError interface for TypeScript
interface CqliteError extends Error {
  code: string;
  category: string;
  isRecoverable: boolean;
}

function isCqliteError(e: unknown): e is CqliteError {
  return e instanceof Error && 'code' in e && 'category' in e;
}

async function demonstrateErrorHandling() {
  console.log('=== Error Handling Examples ===\n');

  // =============================================
  // Example 1: Invalid data directory
  // =============================================
  console.log('--- Example 1: Invalid Data Directory ---');

  try {
    await Database.open('/nonexistent/path/to/data');
  } catch (e) {
    if (isCqliteError(e)) {
      console.log(`Error Code: ${e.code}`);           // 'IO'
      console.log(`Category: ${e.category}`);          // 'System'
      console.log(`Recoverable: ${e.isRecoverable}`);  // true
      console.log(`Message: ${e.message}`);

      // IO errors are often recoverable (file might become available)
      if (e.isRecoverable) {
        console.log('  -> This error may be retried');
      }
    }
  }

  // =============================================
  // Example 2: Invalid SQL syntax
  // =============================================
  console.log('\n--- Example 2: Invalid SQL Syntax ---');

  const dataDir = process.env.CQLITE_DATA_DIR || 'path/to/sstables';
  const schemaPath = process.env.CQLITE_SCHEMA || 'path/to/schema.cql';

  let db: InstanceType<typeof Database> | null = null;

  try {
    db = await Database.open(dataDir, { schema: schemaPath });

    // Intentionally malformed SQL
    await db.execute('SELEC * FORM table');
  } catch (e) {
    if (isCqliteError(e)) {
      console.log(`Error Code: ${e.code}`);           // 'PARSE' or 'QUERY'
      console.log(`Category: ${e.category}`);          // 'Query'
      console.log(`Recoverable: ${e.isRecoverable}`);  // false
      console.log(`Message: ${e.message}`);

      // Parse errors are not recoverable without fixing the query
      if (!e.isRecoverable) {
        console.log('  -> Fix the query syntax and retry');
      }
    }
  }

  // =============================================
  // Example 3: Table not found
  // =============================================
  console.log('\n--- Example 3: Table Not Found ---');

  try {
    if (db) {
      await db.execute('SELECT * FROM nonexistent_keyspace.nonexistent_table');
    }
  } catch (e) {
    if (isCqliteError(e)) {
      console.log(`Error Code: ${e.code}`);           // 'NOT_FOUND' or 'SCHEMA'
      console.log(`Category: ${e.category}`);
      console.log(`Message: ${e.message}`);
    }
  }

  // =============================================
  // Example 4: Operations on closed database
  // =============================================
  console.log('\n--- Example 4: Operations on Closed Database ---');

  try {
    if (db) {
      await db.close();

      // Attempting to query after close
      await db.execute('SELECT * FROM test_basic.simple_table');
    }
  } catch (e) {
    if (isCqliteError(e)) {
      console.log(`Error Code: ${e.code}`);           // 'INVALID_INPUT'
      console.log(`Category: ${e.category}`);          // 'Logic'
      console.log(`Message: ${e.message}`);
    }
  }

  // =============================================
  // Example 5: Comprehensive error handler
  // =============================================
  console.log('\n--- Example 5: Comprehensive Error Handler ---');

  async function executeWithErrorHandling(
    db: InstanceType<typeof Database>,
    query: string
  ): Promise<void> {
    try {
      const result = await db.execute(query);
      console.log(`Query successful: ${result.rowCount} rows`);
    } catch (e) {
      if (!isCqliteError(e)) {
        // Unknown error type
        console.error('Unknown error:', e);
        throw e;
      }

      // Handle based on error code
      switch (e.code) {
        case 'IO':
          console.log('I/O error - check file permissions and paths');
          if (e.isRecoverable) {
            console.log('  Suggestion: Retry after checking data directory');
          }
          break;

        case 'SCHEMA':
          console.log('Schema error - verify schema file matches SSTable data');
          break;

        case 'QUERY':
          console.log('Query execution error');
          break;

        case 'PARSE':
          console.log('Query syntax error - check CQL syntax');
          break;

        case 'NOT_FOUND':
          console.log('Resource not found - verify table/keyspace exists');
          break;

        case 'INVALID_INPUT':
          console.log('Invalid operation - check database state');
          break;

        default:
          console.log(`Unhandled error code: ${e.code}`);
      }

      // Handle based on category
      switch (e.category) {
        case 'System':
          console.log('  Category: System-level issue (I/O, memory, etc.)');
          break;
        case 'Schema':
          console.log('  Category: Schema-related issue');
          break;
        case 'Query':
          console.log('  Category: Query-related issue');
          break;
        case 'Logic':
          console.log('  Category: Application logic issue');
          break;
      }
    }
  }

  // Test the comprehensive handler
  console.log('\nTesting comprehensive error handler:');
  const testDb = await Database.open(dataDir, { schema: schemaPath });
  try {
    await executeWithErrorHandling(testDb, 'SELECT * FROM test_basic.simple_table LIMIT 1');
    await executeWithErrorHandling(testDb, 'INVALID QUERY');
    await executeWithErrorHandling(testDb, 'SELECT * FROM no.such_table');
  } finally {
    await testDb.close();
  }
}

// =============================================
// Retry pattern for recoverable errors
// =============================================
async function withRetry<T>(
  operation: () => Promise<T>,
  maxRetries: number = 3,
  delayMs: number = 1000
): Promise<T> {
  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (e) {
      if (isCqliteError(e) && e.isRecoverable) {
        console.log(`Attempt ${attempt} failed (recoverable), retrying...`);
        lastError = e;
        await new Promise(resolve => setTimeout(resolve, delayMs));
      } else {
        // Non-recoverable error, don't retry
        throw e;
      }
    }
  }

  throw lastError || new Error('Max retries exceeded');
}

async function main() {
  await demonstrateErrorHandling();

  console.log('\n=== Retry Pattern Example ===\n');
  console.log('(Retry pattern defined but not demonstrated to avoid slowdown)');
}

main().catch(console.error);
