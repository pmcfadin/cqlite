/**
 * Smoke tests: verify native module loads without error.
 *
 * Issue #306: Migrated to Jest format.
 */

const cqlite = require('../index.js');

describe('Smoke Tests', () => {
  test('Module loads', () => {
    expect(cqlite).toBeDefined();
  });

  test('version() returns semver string', () => {
    expect(typeof cqlite.version).toBe('function');
    const ver = cqlite.version();
    expect(typeof ver).toBe('string');
    expect(ver).toMatch(/^\d+\.\d+\.\d+/);
  });

  test('Database class exists with expected methods', () => {
    expect(typeof cqlite.Database).toBe('function');
    expect(typeof cqlite.Database.open).toBe('function');
  });

  test('Module version is accessible', () => {
    const ver = cqlite.version();
    console.log(`    cqlite-node version: ${ver}`);
    expect(ver.length).toBeGreaterThan(0);
  });
});
