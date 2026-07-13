# Testing Strategies

## Testing Layers

```
┌─────────────────────────────────────┐
│  Node.js Integration Tests (ava)    │  ← Test the API users actually use
├─────────────────────────────────────┤
│  Rust Unit Tests (cargo test)       │  ← Test core logic
└─────────────────────────────────────┘
```

## Rust Unit Tests

Test core Rust logic independently of napi bindings:

```rust
// src/parser.rs
pub fn parse(cql: &str) -> Result<Statement, ParseError> {
    // Core parsing logic
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_select() {
        let stmt = parse("SELECT * FROM users").unwrap();
        assert_eq!(stmt.query_type(), QueryType::Select);
    }
    
    #[test]
    fn test_parse_invalid() {
        let err = parse("NOT VALID CQL").unwrap_err();
        assert!(err.to_string().contains("unexpected"));
    }
}
```

```bash
cargo test
cargo test --release
```

## Node.js Integration Tests with AVA

### Setup

```json
// package.json
{
  "devDependencies": {
    "ava": "^6.0.0"
  },
  "ava": {
    "timeout": "3m",
    "extensions": ["mjs"],
    "files": ["__test__/**/*.spec.mjs"]
  }
}
```

### Basic Tests

```javascript
// __test__/parser.spec.mjs
import test from 'ava';
import { parse, Statement, ParseError } from '../index.js';

test('parse SELECT statement', (t) => {
    const stmt = parse('SELECT * FROM users');
    t.is(stmt.queryType, 'select');
});

test('parse with keyspace', (t) => {
    const stmt = parse('SELECT * FROM ks.users');
    t.is(stmt.keyspace, 'ks');
    t.is(stmt.table, 'users');
});

test('parse invalid CQL throws ParseError', (t) => {
    const error = t.throws(() => {
        parse('NOT VALID CQL');
    });
    t.true(error.message.includes('unexpected'));
});

test('Statement is instance of class', (t) => {
    const stmt = parse('SELECT * FROM users');
    t.true(stmt instanceof Statement);
});
```

### Async Tests

```javascript
// __test__/async.spec.mjs
import test from 'ava';
import { readAndParse, Database } from '../index.js';
import { writeFile, unlink } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';

test('readAndParse reads file and parses CQL', async (t) => {
    const tmpFile = join(tmpdir(), 'test.cql');
    await writeFile(tmpFile, 'SELECT * FROM users;');
    
    try {
        const statements = await readAndParse(tmpFile);
        t.is(statements.length, 1);
        t.is(statements[0].queryType, 'select');
    } finally {
        await unlink(tmpFile);
    }
});

test('async errors become rejected promises', async (t) => {
    await t.throwsAsync(
        () => readAndParse('/nonexistent/file.cql'),
        { message: /Failed to read/ }
    );
});
```

### Type Conversion Tests

```javascript
// __test__/types.spec.mjs
import test from 'ava';
import { processBuffer, getSchema, executeQuery } from '../index.js';

test('Buffer roundtrip', (t) => {
    const input = Buffer.from([0x00, 0x01, 0x02, 0xff]);
    const output = processBuffer(input);
    t.true(Buffer.isBuffer(output));
    t.deepEqual(output, input);
});

test('returns nested objects', (t) => {
    const schema = getSchema();
    t.is(typeof schema.name, 'string');
    t.true(Array.isArray(schema.columns));
    t.is(typeof schema.columns[0].name, 'string');
});

test('accepts options object', (t) => {
    const result = executeQuery('SELECT * FROM users', {
        keyspace: 'test',
        limit: 100
    });
    t.truthy(result);
});

test('optional fields can be null', (t) => {
    const stmt = parse('SELECT * FROM users');
    t.is(stmt.keyspace, null);
});
```

### Error Handling Tests

```javascript
// __test__/errors.spec.mjs
import test from 'ava';
import { parse, ParseError } from '../index.js';

test('errors have descriptive messages', (t) => {
    const error = t.throws(() => parse('SELEC * FROM users'));
    t.true(error.message.length > 10);
});

test('errors preserve context', (t) => {
    const error = t.throws(() => parse('SELECT * FROM'));
    // Should indicate where parsing failed
    t.regex(error.message, /line|position|unexpected/i);
});
```

## Benchmarking

```javascript
// __test__/benchmark.spec.mjs
import test from 'ava';
import { parse } from '../index.js';

test('parse performance', (t) => {
    const iterations = 10000;
    const query = 'SELECT col1, col2 FROM keyspace.table WHERE pk = ? LIMIT 100';
    
    const start = performance.now();
    for (let i = 0; i < iterations; i++) {
        parse(query);
    }
    const duration = performance.now() - start;
    
    const opsPerSecond = (iterations / duration) * 1000;
    t.log(`${opsPerSecond.toFixed(0)} ops/sec`);
    t.true(opsPerSecond > 1000, 'Should parse > 1000 ops/sec');
});
```

Or use a dedicated benchmark framework:

```javascript
// bench/parse.mjs
import { Bench } from 'tinybench';
import { parse } from '../index.js';

const bench = new Bench({ time: 1000 });

bench
    .add('parse simple', () => parse('SELECT * FROM users'))
    .add('parse complex', () => parse(`
        SELECT col1, col2, col3 
        FROM keyspace.table 
        WHERE pk = ? AND ck > ? 
        LIMIT 1000
    `));

await bench.run();
console.table(bench.table());
```

## TypeScript Tests

If using TypeScript:

```typescript
// __test__/types.spec.ts
import test from 'ava';
import { parse, Statement, QueryType } from '../index';

test('TypeScript types work', (t) => {
    const stmt: Statement = parse('SELECT * FROM users');
    const type: string = stmt.queryType;
    t.is(type, 'select');
});

test('optional properties are typed correctly', (t) => {
    const stmt = parse('SELECT * FROM users');
    // TypeScript knows keyspace is string | null
    const ks: string | null = stmt.keyspace;
    t.is(ks, null);
});
```

```json
// tsconfig.json for tests
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true
  }
}
```

## Test Configuration

```json
// package.json
{
  "ava": {
    "timeout": "3m",
    "concurrency": 5,
    "failFast": true,
    "verbose": true,
    "nodeArguments": ["--experimental-vm-modules"]
  },
  "scripts": {
    "test": "ava",
    "test:watch": "ava --watch",
    "test:coverage": "c8 ava"
  }
}
```

## CI Test Matrix

```yaml
# .github/workflows/test.yml
name: Test
on: [push, pull_request]

jobs:
  test:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
        node: [18, 20, 22]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: ${{ matrix.node }}
      - uses: dtolnay/rust-toolchain@stable
      - run: npm ci
      - run: npm run build
      - run: npm test
      
  rust-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo test
```

## Testing Checklist

- [ ] Rust unit tests for core logic
- [ ] Node.js tests for API surface
- [ ] Type conversion tests (all supported types)
- [ ] Error handling tests (all error conditions)
- [ ] Async function tests
- [ ] Edge cases (empty input, large input, unicode)
- [ ] TypeScript type definitions work
- [ ] Benchmark critical paths
- [ ] Test on all supported Node.js versions
- [ ] Test on all target platforms
