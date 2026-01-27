# CQLite CQL Feature Parity Checklist (Node.js)

Track coverage between Cassandra CQL features and Node.js bindings.

## Parity Verification Workflow

1. **Identify CQL feature** from Cassandra documentation
2. **Check Rust implementation** - does `cqlite` core support it?
3. **Check Node.js binding** - is it exposed via napi-rs?
4. **Add test** - JavaScript/TypeScript test exercising the feature
5. **Update checklist** - mark status below

## Statement Types

| Statement | Rust Core | Node Binding | Tests | Notes |
|-----------|:---------:|:------------:|:-----:|-------|
| SELECT | ⬜ | ⬜ | ⬜ | |
| INSERT | ⬜ | ⬜ | ⬜ | |
| UPDATE | ⬜ | ⬜ | ⬜ | |
| DELETE | ⬜ | ⬜ | ⬜ | |
| BATCH | ⬜ | ⬜ | ⬜ | |
| CREATE KEYSPACE | ⬜ | ⬜ | ⬜ | |
| CREATE TABLE | ⬜ | ⬜ | ⬜ | |
| CREATE INDEX | ⬜ | ⬜ | ⬜ | |
| CREATE TYPE | ⬜ | ⬜ | ⬜ | UDT |
| CREATE FUNCTION | ⬜ | ⬜ | ⬜ | UDF |
| CREATE AGGREGATE | ⬜ | ⬜ | ⬜ | UDA |
| ALTER KEYSPACE | ⬜ | ⬜ | ⬜ | |
| ALTER TABLE | ⬜ | ⬜ | ⬜ | |
| DROP * | ⬜ | ⬜ | ⬜ | |
| TRUNCATE | ⬜ | ⬜ | ⬜ | |
| USE | ⬜ | ⬜ | ⬜ | |

## Data Types

| Type | Rust Core | Node Binding | JS Type | Notes |
|------|:---------:|:------------:|---------|-------|
| ascii | ⬜ | ⬜ | `string` | |
| bigint | ⬜ | ⬜ | `bigint` | |
| blob | ⬜ | ⬜ | `Buffer` | |
| boolean | ⬜ | ⬜ | `boolean` | |
| counter | ⬜ | ⬜ | `bigint` | |
| date | ⬜ | ⬜ | `Date` or string | |
| decimal | ⬜ | ⬜ | `string` or BigDecimal | No native JS decimal |
| double | ⬜ | ⬜ | `number` | |
| duration | ⬜ | ⬜ | `object` | Cassandra 3.10+ |
| float | ⬜ | ⬜ | `number` | |
| inet | ⬜ | ⬜ | `string` | |
| int | ⬜ | ⬜ | `number` | |
| smallint | ⬜ | ⬜ | `number` | |
| text | ⬜ | ⬜ | `string` | |
| time | ⬜ | ⬜ | `bigint` (nanos) | |
| timestamp | ⬜ | ⬜ | `Date` | |
| timeuuid | ⬜ | ⬜ | `string` | |
| tinyint | ⬜ | ⬜ | `number` | |
| uuid | ⬜ | ⬜ | `string` | |
| varchar | ⬜ | ⬜ | `string` | |
| varint | ⬜ | ⬜ | `bigint` | |

## Collection Types

| Type | Rust Core | Node Binding | JS Type | Notes |
|------|:---------:|:------------:|---------|-------|
| list<T> | ⬜ | ⬜ | `Array<T>` | |
| set<T> | ⬜ | ⬜ | `Set<T>` or `Array<T>` | |
| map<K,V> | ⬜ | ⬜ | `Map<K,V>` or `Object` | |
| frozen<T> | ⬜ | ⬜ | same as T | |
| tuple<...> | ⬜ | ⬜ | `Array` | |

## Special Features

| Feature | Rust Core | Node Binding | Tests | Notes |
|---------|:---------:|:------------:|:-----:|-------|
| User-Defined Types (UDT) | ⬜ | ⬜ | ⬜ | |
| Secondary Indexes | ⬜ | ⬜ | ⬜ | |
| Materialized Views | ⬜ | ⬜ | ⬜ | |
| ALLOW FILTERING | ⬜ | ⬜ | ⬜ | |
| LIMIT | ⬜ | ⬜ | ⬜ | |
| ORDER BY | ⬜ | ⬜ | ⬜ | |
| GROUP BY | ⬜ | ⬜ | ⬜ | Cassandra 4.0+ |
| TTL | ⬜ | ⬜ | ⬜ | |
| WRITETIME | ⬜ | ⬜ | ⬜ | |
| IF NOT EXISTS | ⬜ | ⬜ | ⬜ | LWT |
| IF EXISTS | ⬜ | ⬜ | ⬜ | LWT |
| IF conditions | ⬜ | ⬜ | ⬜ | LWT |
| JSON support | ⬜ | ⬜ | ⬜ | INSERT/SELECT JSON |
| DISTINCT | ⬜ | ⬜ | ⬜ | |
| PER PARTITION LIMIT | ⬜ | ⬜ | ⬜ | |
| Token function | ⬜ | ⬜ | ⬜ | |
| Aggregate functions | ⬜ | ⬜ | ⬜ | COUNT, SUM, etc. |

## Cassandra 5.0 Features

| Feature | Rust Core | Node Binding | Tests | Notes |
|---------|:---------:|:------------:|:-----:|-------|
| Vector type | ⬜ | ⬜ | ⬜ | vector<float, N> |
| SAI indexes | ⬜ | ⬜ | ⬜ | Storage-Attached Indexes |
| VECTOR ANN queries | ⬜ | ⬜ | ⬜ | ANN (Approximate Nearest Neighbor) |

## SSTable Parsing (if applicable)

| Feature | Rust Core | Node Binding | Tests | Notes |
|---------|:---------:|:------------:|:-----:|-------|
| Read SSTable metadata | ⬜ | ⬜ | ⬜ | |
| Parse Data.db | ⬜ | ⬜ | ⬜ | |
| Parse Index.db | ⬜ | ⬜ | ⬜ | |
| Parse Filter.db | ⬜ | ⬜ | ⬜ | Bloom filter |
| Parse Statistics.db | ⬜ | ⬜ | ⬜ | |
| Compression support | ⬜ | ⬜ | ⬜ | LZ4, Snappy, etc. |
| SSTable format mc | ⬜ | ⬜ | ⬜ | Cassandra 3.x |
| SSTable format nb | ⬜ | ⬜ | ⬜ | Cassandra 4.x |
| SSTable format nc | ⬜ | ⬜ | ⬜ | Cassandra 5.x |

## Legend

- ⬜ Not started
- 🔄 In progress  
- ✅ Complete
- ❌ Not planned / Out of scope
- ⚠️ Partial support

## Adding New Features

When implementing a new CQL feature:

```rust
// 1. Add to Rust core (src/parser.rs or similar)
pub enum StatementType {
    Select,
    Insert,
    NewFeature,  // Add variant
}

// 2. Add Node.js binding (src/node/types.rs)
#[napi]
impl Statement {
    #[napi(getter)]
    pub fn is_new_feature(&self) -> bool {
        matches!(self.inner.statement_type(), StatementType::NewFeature)
    }
}

// 3. Add test (__test__/new_feature.spec.mjs)
test('new feature support', (t) => {
    const stmt = parse('NEW FEATURE SYNTAX');
    t.true(stmt.isNewFeature);
});

// 4. Update this checklist
```

## TypeScript Type Parity

Ensure TypeScript definitions match all exposed functionality:

```typescript
// index.d.ts should include:
export interface Statement {
    readonly queryType: string;
    readonly keyspace: string | null;
    readonly table: string | null;
    // ... all properties
}

export function parse(cql: string): Statement;
export function parseAll(cql: string): Statement[];
// ... all functions
```

## Parity Test Pattern

```javascript
// __test__/parity.spec.mjs
import test from 'ava';
import { parse } from '../index.js';

const PARITY_TESTS = [
    ['SELECT * FROM users', { queryType: 'select', table: 'users' }],
    ['INSERT INTO users (id) VALUES (1)', { queryType: 'insert', table: 'users' }],
    // Add more as features are implemented
];

for (const [cql, expected] of PARITY_TESTS) {
    test(`parity: ${cql}`, (t) => {
        const stmt = parse(cql);
        for (const [attr, value] of Object.entries(expected)) {
            t.is(stmt[attr], value, `${attr} mismatch for: ${cql}`);
        }
    });
}
```

## Cross-Platform Parity with Python Bindings

If maintaining both Python and Node.js bindings, ensure consistent behavior:

| Feature | Python | Node.js | Notes |
|---------|:------:|:-------:|-------|
| Function naming | snake_case | camelCase | Convention difference |
| Error types | Custom exceptions | Error with message | |
| Async | async/await | Promise | Same semantics |
| Buffers | bytes | Buffer | Same data |
| BigInt | int | bigint | Same range |

Test both bindings against same CQL inputs to verify identical parsing results.
