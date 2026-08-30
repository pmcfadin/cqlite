/**
 * Jest configuration for CQLite Node.js bindings.
 *
 * Issue #306: Jest Test Infrastructure Setup
 * Epic #318: M4 Node.js Bindings
 *
 * Issue #1465: leak detection is SCOPED, not global. The suite is split into two
 * `projects` that share one base config:
 *   * `default` — every test file EXCEPT the leak lane, with exactly the
 *     behaviour it had before (same environment, setup file and 30s timeout), so
 *     the existing suite is unaffected.
 *   * `leaks`   — `__test__/leak-paths.test.js` only, with
 *     `detectOpenHandles: true` (an abandoned iterator whose `return()`/`close()`
 *     never ran leaves a handle behind, which is exactly the signal that lane
 *     wants) and a longer timeout for its multi-pass measurement.
 * `detectLeaks: true` is enabled for that project as well — measured GREEN and
 * stable against this native module — but NO correctness weight is placed on it
 * (it watches jest's TestEnvironment instance, which test code cannot reach, so
 * its liveness here is undemonstrable; see the header of
 * `__test__/leak-paths.test.js`). The load-bearing guard is the measured
 * heap+external budget asserted in that file.
 *
 * @type {import('jest').Config}
 */
const LEAK_TEST = '<rootDir>/__test__/leak-paths.test.js';

// Shared by both projects: whatever the single-project config used to set.
const baseProject = {
  // Use Node.js environment for native module testing
  testEnvironment: 'node',
  // Run setup file before tests
  setupFilesAfterEnv: ['<rootDir>/__test__/setup.js'],
};

module.exports = {
  projects: [
    {
      ...baseProject,
      displayName: 'default',
      // Match test files in __test__ directory
      testMatch: ['**/__test__/**/*.test.js'],
      // The leak lane runs in its own project (below) with its own options.
      testPathIgnorePatterns: ['/node_modules/', '/__test__/leak-paths\\.test\\.js$'],
    },
    {
      ...baseProject,
      displayName: 'leaks',
      testMatch: [LEAK_TEST],
      // Abandoned iterators are exactly the shape that leaves a handle behind.
      detectOpenHandles: true,
      detectLeaks: true,
    },
  ],

  // 30 second timeout for database operations. Set at the ROOT: verified to
  // propagate into every project (a 7s test passes, so jest's 5s default is not
  // in force), and project-level `testTimeout` trips a jest 29 config-validation
  // warning. The two multi-pass leak budgets carry their own per-test timeout.
  testTimeout: 30000,

  // Coverage configuration
  coverageDirectory: 'coverage',
  coverageThreshold: {
    global: {
      lines: 80,
      branches: 65,  // Lower for thin JS wrapper layer (most logic in Rust)
      functions: 80,
      statements: 80,
    },
  },
  collectCoverageFrom: [
    'lib/**/*.js',
    '!lib/**/*.d.ts',
  ],

  // Verbose output for CI visibility
  verbose: true,
};
