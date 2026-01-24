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

  // Test 3: Database class exists with expected methods
  assert(typeof cqlite.Database === 'function', 'Database should be a class');
  assert(typeof cqlite.Database.open === 'function', 'Database.open should be a function');

  // Test 4: QueryResult and DatabaseStats types are exported
  // (These are object types, exported via napi as part of the module)
  // Note: napi-rs exports object types implicitly, no explicit check needed

  console.log('All smoke tests passed');
  console.log(`  cqlite-node version: ${ver}`);
  process.exit(0);
} catch (err) {
  console.error('Smoke test failed:', err.message);
  console.error(err.stack);
  process.exit(1);
}
