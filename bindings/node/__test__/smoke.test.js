// Smoke test: verify native module loads without error
const assert = require('assert');

try {
  const cqlite = require('../index.js');

  // Test 1: Module loads
  assert(cqlite, 'Module should load');

  // Test 2: Version function exists and returns string
  assert(typeof cqlite.version === 'function', 'version() should be a function');
  const ver = cqlite.version();
  assert(typeof ver === 'string', 'version() should return a string');
  assert(ver.match(/^\d+\.\d+\.\d+/), 'version() should return semver');

  // Test 3: Database class exists
  assert(typeof cqlite.Database === 'function', 'Database should be a class');

  // Test 4: Database.open() throws with Phase 2 placeholder message
  try {
    cqlite.Database.open('/fake/path');
    assert.fail('Database.open() should throw');
  } catch (err) {
    assert(
      err.message.includes('Not yet implemented - Phase 2'),
      'Should throw Phase 2 placeholder error'
    );
  }

  console.log('All smoke tests passed');
  console.log(`  cqlite-node version: ${ver}`);
  process.exit(0);
} catch (err) {
  console.error('Smoke test failed:', err.message);
  process.exit(1);
}
