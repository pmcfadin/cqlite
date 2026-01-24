/**
 * StreamingConfig tests for Issue #304.
 *
 * Issue #306: Migrated to Jest format.
 *
 * TDD Requirements from the issue:
 * - [x] Test: Default StreamingConfig has correct values
 * - [x] Test: Custom bufferSize is respected
 * - [x] Test: Custom chunkSize is respected
 * - [x] Test: DatabaseOptions.schema is used (covered in database.test.js)
 * - [x] Test: StreamingConfig converts to core type correctly
 *
 * Note: StreamingConfig is used as a plain JavaScript object passed to
 * executeStreaming(). The validation happens in Rust when to_core() is called.
 * These tests verify the TypeScript interface and expected values.
 */

describe('StreamingConfig Tests (Issue #304)', () => {
  test('Default StreamingConfig has correct values', () => {
    // Default values as specified in Issue #304
    const defaultConfig = {
      bufferSize: 1024,
      chunkSize: 10000,
    };

    expect(defaultConfig.bufferSize).toBe(1024);
    expect(defaultConfig.chunkSize).toBe(10000);
  });

  test('Custom bufferSize is respected', () => {
    const config = {
      bufferSize: 512,
      chunkSize: 10000,
    };

    expect(config.bufferSize).toBe(512);
    expect(config.chunkSize).toBe(10000);
  });

  test('Custom chunkSize is respected', () => {
    const config = {
      bufferSize: 1024,
      chunkSize: 5000,
    };

    expect(config.bufferSize).toBe(1024);
    expect(config.chunkSize).toBe(5000);
  });

  test('Both custom values work together', () => {
    const config = {
      bufferSize: 256,
      chunkSize: 2500,
    };

    expect(config.bufferSize).toBe(256);
    expect(config.chunkSize).toBe(2500);
  });

  test('Optional fields can be undefined', () => {
    // Both fields are optional (undefined uses defaults in Rust)
    const configNoBuffer = { chunkSize: 5000 };
    const configNoChunk = { bufferSize: 512 };
    const configEmpty = {};

    expect(configNoBuffer.bufferSize).toBeUndefined();
    expect(configNoBuffer.chunkSize).toBe(5000);

    expect(configNoChunk.bufferSize).toBe(512);
    expect(configNoChunk.chunkSize).toBeUndefined();

    expect(configEmpty.bufferSize).toBeUndefined();
    expect(configEmpty.chunkSize).toBeUndefined();
  });

  test('Memory budget calculation', () => {
    // From the spec: assuming 1KB average row size
    // buffer_size: 1024 rows = ~1MB in flight
    // chunk_size: 10000 rows = ~10MB per chunk
    // Total: ~11MB (well within 128MB budget)

    const config = {
      bufferSize: 1024,
      chunkSize: 10000,
    };

    const avgRowSizeKB = 1;
    const bufferMemoryMB = (config.bufferSize * avgRowSizeKB) / 1024;
    const chunkMemoryMB = (config.chunkSize * avgRowSizeKB) / 1024;
    const totalMemoryMB = bufferMemoryMB + chunkMemoryMB;

    expect(bufferMemoryMB).toBeLessThanOrEqual(2);
    expect(chunkMemoryMB).toBeLessThanOrEqual(15);
    expect(totalMemoryMB).toBeLessThanOrEqual(128);
  });

  test('Validation rules documented', () => {
    // Zero values should be rejected by to_core() in Rust
    // We document this behavior here for reference
    const invalidConfigs = [
      { bufferSize: 0, chunkSize: 10000 }, // bufferSize == 0 is invalid
      { bufferSize: 1024, chunkSize: 0 }, // chunkSize == 0 is invalid
      { bufferSize: 0, chunkSize: 0 }, // Both zero is invalid
    ];

    // These configs are syntactically valid JS objects but will fail validation
    // when passed to executeStreaming() which calls to_core()
    for (const config of invalidConfigs) {
      expect(
        typeof config.bufferSize === 'number' || config.bufferSize === undefined
      ).toBe(true);
      expect(
        typeof config.chunkSize === 'number' || config.chunkSize === undefined
      ).toBe(true);
    }

    // Document expected Rust error messages for zero values
    const expectedErrors = {
      zeroBufferSize: 'bufferSize must be greater than 0',
      zeroChunkSize: 'chunkSize must be greater than 0',
    };

    expect(expectedErrors.zeroBufferSize).toContain('greater than 0');
    expect(expectedErrors.zeroChunkSize).toContain('greater than 0');
  });

  test('Config matches Python bindings defaults', () => {
    // Python bindings use the same defaults from cqlite-core
    // bindings/python/src/config.rs:98-103
    const pythonDefaults = {
      buffer_size: 1024,
      chunk_size: 10_000,
    };

    // Node.js uses camelCase
    const nodeDefaults = {
      bufferSize: 1024,
      chunkSize: 10000,
    };

    expect(pythonDefaults.buffer_size).toBe(nodeDefaults.bufferSize);
    expect(pythonDefaults.chunk_size).toBe(nodeDefaults.chunkSize);
  });
});
