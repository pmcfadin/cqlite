/**
 * Jest configuration for CQLite Node.js bindings.
 *
 * Issue #306: Jest Test Infrastructure Setup
 * Epic #318: M4 Node.js Bindings
 *
 * Issue #1465: the leak lane is a SEPARATE PROJECT, and its leak DETECTORS are
 * passed on a dedicated invocation — not declared here. The suite is split into
 * two `projects` that share one base config:
 *   * `default` — every test file EXCEPT the leak lane, with exactly the
 *     behaviour it had before (same environment, setup file, and the root
 *     `testTimeout` below).
 *   * `leaks`   — `__test__/leak-paths.test.js` only, so it can be selected on
 *     its own (`--selectProjects leaks`, used by `npm run test:leaks`) and so a
 *     future lane-only option has somewhere to live.
 *
 * WHY NO `detectOpenHandles`/`detectLeaks` KEY IN THE `leaks` ENTRY (measured on
 * jest 29.7.0, not assumed):
 *   * `detectOpenHandles` is read from the GLOBAL config only
 *     (`@jest/core/build/runJest.js:322` for handle collection,
 *     `testSchedulerHelper.js:29` for the runInBand implication). Declaring it in
 *     a `projects[]` entry resolves to `projectConfig.detectOpenHandles = true`
 *     and `globalConfig.detectOpenHandles = false`, i.e. it does NOTHING —
 *     verified by introspecting `readConfigs()` on this very file. It is not
 *     passed at the invocation either: it prints a report and exits 0 (no
 *     enforcement), its baseline on this lane is one `CustomGC` handle from
 *     loading the napi addon, and an outstanding handle makes jest HANG rather
 *     than fail, which in a gate component is worse than no signal. See the
 *     header of `__test__/leak-paths.test.js` for the four measurements. It stays
 *     available to a human as the separate `npm run test:leaks:handles` script,
 *     which no lane invokes.
 *   * `detectLeaks` is deliberately absent. (It IS honoured per-project —
 *     `jest-runner/build/runTest.js:261` reads `projectConfig.detectLeaks` and
 *     `@jest/core/build/TestScheduler.js:138` surfaces the verdict — so it did
 *     run here; it is removed because of what it MEASURES, not because it was
 *     dead.) jest-leak-detector watches the jest `TestEnvironment` INSTANCE, so
 *     it answers "was the whole environment collected after this FILE finished",
 *     never "does each iteration of this loop retain memory" — the property this
 *     issue is about. Being both blind to that property and able to red for
 *     unrelated environment retention on any jest/Node bump, it is a guard that
 *     could only ever fail wrongly. Issue #1465 step 3 authorises the documented
 *     budget fallback, which is what `__test__/leak-paths.test.js` asserts.
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
      // No leak-detector keys here. `detectOpenHandles` would be inert (it is
      // read from the GLOBAL config only) and is not wanted on a lane anyway;
      // `detectLeaks` WOULD take effect per-project, and is deliberately not
      // used because it measures the wrong thing. Both are explained in the
      // header above.
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
