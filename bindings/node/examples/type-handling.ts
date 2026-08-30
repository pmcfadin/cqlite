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

    // WHICH CALL, AND WHY IT IS NOT `execute()`: only the NATIVE path produces
    // the UDT object. `execute()` shapes rows through `value_to_json`
    // (`src/database/json_value.rs`, `Value::Udt`), which emits a BARE object of
    // the declared fields — no `typeName`, no `keyspace`, no `fields` wrapper —
    // because JSON output mirrors the CLI's. So the structural check below can
    // never fire on an `execute()` row and the #3504 shape is not demonstrable
    // there. (The pre-#3504 version of this example had the same defect one shape
    // earlier: it sniffed `_type` on an `execute()` row, which `value_to_json`
    // does not emit either — so it printed nothing then too.)
    const udts = await db.executeNative(
      'SELECT * FROM test_collections.collections_with_udts LIMIT 1'
    );

    // A UDT is `{ typeName, keyspace, fields }` — the type identity is carried
    // OUT OF BAND and the declared fields live in `fields` alone (issue #3504).
    // Before that change `_type`/`_keyspace` were set on the same object as the
    // field names, so a UDT declaring a field of either name silently overwrote
    // the marker.
    interface UdtValueShape {
      typeName: string;
      keyspace: string;
      fields: Record<string, unknown>;
    }

    // Recognise a UDT by its STRUCTURE, not by sniffing for a marker key the data
    // itself could supply. Note the LIMIT of this check: three co-occurring keys
    // is a narrower SNIFF than Python's `isinstance(v, cqlite.Udt)`, not an
    // authoritative type test — a JSON object cell can carry the same keys
    // (`docs/development/M4_spec.md` §5.3, instance b-5).
    const isUdt = (value: unknown): value is UdtValueShape =>
      typeof value === 'object' &&
      value !== null &&
      'typeName' in value &&
      'keyspace' in value &&
      'fields' in value;

    // In THIS table every UDT is nested inside a collection
    // (`LIST<FROZEN<address_type>>`, `SET<FROZEN<contact_info>>`,
    // `MAP<TEXT, FROZEN<contact_info>>`) — measured: it has no top-level UDT
    // column at all, so checking only the cell itself prints nothing. Hence the
    // descent through arrays, `Set`s and `Map` VALUES, and through UDT fields
    // (`contact_info.address` is itself a UDT).
    function* udtsIn(value: unknown): Generator<UdtValueShape> {
      if (isUdt(value)) {
        yield value;
        for (const field of Object.values(value.fields)) yield* udtsIn(field);
      } else if (Array.isArray(value)) {
        for (const item of value) yield* udtsIn(item);
      } else if (value instanceof Set) {
        for (const item of value) yield* udtsIn(item);
      } else if (value instanceof Map) {
        for (const item of value.values()) yield* udtsIn(item);
      }
    }

    for (const row of udts.rows) {
      const columns = Object.keys(row);
      console.log(`Row has ${columns.length} columns`);

      for (const column of columns) {
        for (const udt of udtsIn(row[column])) {
          console.log(`${column} -> UDT:`);
          console.log(`  Type: ${udt.typeName}`);
          console.log(`  Keyspace: ${udt.keyspace}`);
          // Print UDT fields. No name filtering is needed any more: `fields`
          // holds declared fields and nothing else, so a field genuinely named
          // `_type` prints like any other.
          for (const [field, fieldValue] of Object.entries(udt.fields)) {
            console.log(`  ${field}: ${JSON.stringify(fieldValue)}`);
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
