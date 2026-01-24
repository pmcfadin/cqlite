/**
 * Jest configuration for CQLite Node.js bindings.
 *
 * Issue #306: Jest Test Infrastructure Setup
 * Epic #318: M4 Node.js Bindings
 *
 * @type {import('jest').Config}
 */
module.exports = {
  // Use Node.js environment for native module testing
  testEnvironment: 'node',

  // Match test files in __test__ directory
  testMatch: ['**/__test__/**/*.test.js'],

  // 30 second timeout for database operations
  testTimeout: 30000,

  // Run setup file before tests
  setupFilesAfterEnv: ['<rootDir>/__test__/setup.js'],

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
