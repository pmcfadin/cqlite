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
 * `detectLeaks` is deliberately NOT enabled: the napi addon is process-global by
 * construction (N-API keeps the addon + a lazily-initialised Tokio runtime alive
 * for the process), so module-registry leak detection reports a leak for ANY
 * file touching the addon — a measured false positive against the property under
 * test. The leak lane asserts a measured heap+external budget instead; the full
 * rationale is in the header of `__test__/leak-paths.test.js`.
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
      // 30 second timeout for database operations
      testTimeout: 30000,
    },
    {
      ...baseProject,
      displayName: 'leaks',
      testMatch: [LEAK_TEST],
      // Abandoned iterators are exactly the shape that leaves a handle behind.
      detectOpenHandles: true,
      // The budgets measure 9 passes x 300 iterations over a wide table.
      testTimeout: 120000,
    },
  ],

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
