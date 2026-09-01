/**
 * Jest configuration for CQLite Node.js bindings.
 *
 * Issue #306: Jest Test Infrastructure Setup
 * Epic #318: M4 Node.js Bindings
 *
 * Issue #1465: the leak lane is a SEPARATE PROJECT, and NO jest leak detector is
 * enabled for it — not here, and not at any lane's invocation (the reasons are
 * below). The suite is split into two `projects` that share one base config:
 *   * `default` — every test file EXCEPT the leak lane. Unchanged behaviour for
 *     the 27 pre-existing files: same environment, same setup file, same
 *     `testMatch`, and the root `testTimeout` below; the only addition is the
 *     `testPathIgnorePatterns` entry that hands the leak file to the other
 *     project (jest's own default for that option is `['/node_modules/']`, which
 *     is preserved).
 *   * `leaks`   — `__test__/leak-paths.test.js` only, so it can be selected on its
 *     own (`--selectProjects leaks`, used by `npm run test:leaks`) and so a future
 *     lane-only option has somewhere to live. NOTE (round 9): a bare `npm test` runs
 *     BOTH projects, so the gate's whole-suite run already executes the leak file
 *     exactly once. The gate therefore does NOT invoke `test:leaks`; it affirms the
 *     budget tests by name from the whole-suite `--json` report. Keep that in mind
 *     before adding a second lane that selects this project: two executions where
 *     only one is affirmed is what the recomposition removed.
 *
 *     NO FILE COUNT IS QUOTED (issue #3772). This note used to end "measured via
 *     `jest --listTests`: 28 files, no duplicates", repeating a number
 *     `scripts/agent-gate.sh` carried in four more places; the suite grew and every
 *     copy went stale, so the sentence asserted a false measurement in the one place
 *     a reader checks this composition against. Nothing here needs the count: the
 *     `node-bindings` gate component DERIVES it every run from two independent
 *     oracles and prints `suite set RECONCILED: N`.
 *
 *     The "no duplicates" half was worse than stale — it named an oracle that cannot
 *     see the violation. Measured on jest 29.7.0 (#3772), two projects both matching
 *     one file: `jest --listTests` prints it ONCE while the run reports
 *     `Test Suites: 2 passed, 2 total`. Deleting the `testPathIgnorePatterns` entry
 *     below leaves this package's `--listTests` output unchanged, too. What actually
 *     catches a double execution is the gate comparing jest's reported suite TOTAL
 *     against the DEDUPLICATED disk inventory, plus the leak affirmation's refusal on
 *     two suites at the leak path. So: if you change the projects' `testMatch` or the
 *     ignore list, do not reason about it from `--listTests` — run the suite and read
 *     the total. If you need today's file count, run `--listTests`; do not write the
 *     answer down here.
 *
 * WHY NO `detectOpenHandles`/`detectLeaks` ANYWHERE (measured on jest 29.7.0, not
 * assumed):
 *   * `detectOpenHandles` is read from the GLOBAL config only
 *     (`runJest` in @jest/core reads `globalConfig.detectOpenHandles` to decide
 *     whether to collect handles at all, and `shouldRunInBand` in @jest/core's
 *     testSchedulerHelper reads it for the run-in-band implication). Declaring it in
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
 *     `runTestInternal` in jest-runner's runTest reads `projectConfig.detectLeaks`
 *     and @jest/core's TestScheduler surfaces the `testResult.leaks` verdict — so it did
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

// The two project-level options both projects need. The rest of the old
// single-project config did not move here because it is either per-project
// (`testMatch`) or global (`testTimeout`, coverage) — see below.
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
