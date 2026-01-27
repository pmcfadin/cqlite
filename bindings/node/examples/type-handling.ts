/**
 * Type Handling Example
 *
 * Demonstrates working with CQL types in @cqlite/node, including:
 * - Primitive types (int, bigint, float, boolean, text)
 * - Temporal types (timestamp, date, time, duration)
 * - Binary types (blob)
 * - Collection types (list, set, map)
 * - User-defined types (UDTs)
 *
 * Run: npx ts-node examples/type-handling.ts
 */

import { Database } from '@cqlite/node';

async function main() {
  const dataDir = process.env.CQLITE_DATA_DIR || 'path/to/sstables';
  const schemaPath = process.env.CQLITE_SCHEMA || 'path/to/schema.cql';

  const db = await Database.open(dataDir, { schema: schemaPath });

  try {
    // =============================================
    // Primitive Types
    // =============================================
    console.log('=== Primitive Types ===\n');

    const primitives = await db.executeNative(
      'SELECT * FROM test_basic.simple_table LIMIT 1'
    );

    for (const row of primitives.rows) {
      // Text types -> string
      if (row.name !== null) {
        console.log(`name (text): ${row.name} [${typeof row.name}]`);
      }

      // Integer types -> number
      if (row.age !== null) {
        console.log(`age (int): ${row.age} [${typeof row.age}]`);
      }

      // BigInt types -> bigint (with executeNative)
      if (row.balance !== null && typeof row.balance === 'bigint') {
        console.log(`balance (bigint): ${row.balance} [${typeof row.balance}]`);
      }

      // Boolean types -> boolean
      if (row.active !== null) {
        console.log(`active (boolean): ${row.active} [${typeof row.active}]`);
      }

      // Float/double types -> number
      if (row.score !== null) {
        console.log(`score (double): ${row.score} [${typeof row.score}]`);
      }
    }

    // =============================================
    // Temporal Types
    // =============================================
    console.log('\n=== Temporal Types ===\n');

    // simple_table has timestamp (created) and duration (duration_val) columns
    const temporal = await db.executeNative(
      'SELECT created, duration_val FROM test_basic.simple_table LIMIT 1'
    );

    for (const row of temporal.rows) {
      // Timestamp -> Date object
      if (row.created !== null) {
        const created = row.created as Date;
        console.log(`created (timestamp): ${created.toISOString()}`);
        console.log(`  Year: ${created.getFullYear()}`);
        console.log(`  Month: ${created.getMonth() + 1}`);
      }

      // Duration -> object with months, days, nanos
      if (row.duration_val !== null) {
        const dur = row.duration_val as { months: number; days: number; nanos: bigint };
        console.log(`duration_val: ${dur.months}mo ${dur.days}d ${dur.nanos}ns`);
      }
    }

    // =============================================
    // Binary Types
    // =============================================
    console.log('\n=== Binary Types ===\n');

    // large_blob_table has blob_data column
    const binary = await db.executeNative(
      'SELECT blob_data FROM test_wide_rows.large_blob_table LIMIT 1'
    );

    for (const row of binary.rows) {
      if (row.blob_data !== null) {
        const buf = row.blob_data as Buffer;
        console.log(`blob_data: ${buf.length} bytes`);
        console.log(`  First 10 bytes: ${buf.slice(0, 10).toString('hex')}`);
        console.log(`  Is Buffer: ${Buffer.isBuffer(buf)}`);
      }
    }

    // =============================================
    // Collection Types
    // =============================================
    console.log('\n=== Collection Types ===\n');

    const collections = await db.executeNative(
      'SELECT * FROM test_collections.typed_collections_table LIMIT 1'
    );

    for (const row of collections.rows) {
      // List -> Array
      if (row.tags !== null && Array.isArray(row.tags)) {
        console.log(`tags (list): [${(row.tags as string[]).join(', ')}]`);
        console.log(`  Is Array: ${Array.isArray(row.tags)}`);
      }

      // Set -> Set (with executeNative)
      if (row.categories !== null) {
        const set = row.categories as Set<string>;
        if (set instanceof Set || Object.prototype.toString.call(set) === '[object Set]') {
          console.log(`categories (set): ${[...set].join(', ')}`);
          console.log(`  Size: ${set.size}`);
        }
      }

      // Map -> Map (with executeNative)
      if (row.metadata !== null) {
        const map = row.metadata as Map<string, string>;
        if (map instanceof Map || Object.prototype.toString.call(map) === '[object Map]') {
          console.log(`metadata (map):`);
          for (const [key, value] of map) {
            console.log(`  ${key}: ${value}`);
          }
        }
      }
    }

    // =============================================
    // UUID Types
    // =============================================
    console.log('\n=== UUID Types ===\n');

    // simple_table has id (uuid) and session_id (timeuuid) columns
    const uuids = await db.execute(
      'SELECT id, session_id FROM test_basic.simple_table LIMIT 1'
    );

    for (const row of uuids.rows) {
      // UUID -> string (lowercase formatted)
      if (row.id !== null) {
        console.log(`id (uuid): ${row.id}`);
        // UUIDs are returned as formatted strings
        const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
        console.log(`  Valid UUID format: ${uuidPattern.test(row.id as string)}`);
      }
      if (row.session_id !== null) {
        console.log(`session_id (timeuuid): ${row.session_id}`);
      }
    }

    // =============================================
    // User-Defined Types (UDTs)
    // =============================================
    console.log('\n=== User-Defined Types ===\n');

    // collections_with_udts has UDT columns
    const udts = await db.execute(
      'SELECT * FROM test_collections.collections_with_udts LIMIT 1'
    );

    for (const row of udts.rows) {
      // UDTs are returned as objects with _type and _keyspace metadata
      const keys = Object.keys(row);
      console.log(`Row has ${keys.length} columns`);

      // Check each column for UDT structure
      for (const key of keys) {
        const value = row[key];
        if (value && typeof value === 'object' && '_type' in value) {
          const udt = value as { _type: string; _keyspace: string; [key: string]: unknown };
          console.log(`${key} (UDT):`);
          console.log(`  Type: ${udt._type}`);
          console.log(`  Keyspace: ${udt._keyspace}`);
          // Print UDT fields
          for (const field of Object.keys(udt)) {
            if (!field.startsWith('_')) {
              console.log(`  ${field}: ${udt[field]}`);
            }
          }
        }
      }
    }

    // =============================================
    // execute() vs executeNative() comparison
    // =============================================
    console.log('\n=== execute() vs executeNative() ===\n');

    // execute() returns JSON-serializable values
    const jsonResult = await db.execute(
      'SELECT balance, created FROM test_basic.simple_table LIMIT 1'
    );
    console.log('execute() result:');
    for (const row of jsonResult.rows) {
      console.log(`  balance: ${row.balance} [${typeof row.balance}]`);
      console.log(`  created: ${row.created} [${typeof row.created}]`);
    }

    // executeNative() returns native JS types
    const nativeResult = await db.executeNative(
      'SELECT balance, created FROM test_basic.simple_table LIMIT 1'
    );
    console.log('\nexecuteNative() result:');
    for (const row of nativeResult.rows) {
      console.log(`  balance: ${row.balance} [${typeof row.balance}]`);
      console.log(`  created: ${row.created} [${typeof row.created}]`);
    }

  } finally {
    await db.close();
  }
}

main().catch(console.error);
