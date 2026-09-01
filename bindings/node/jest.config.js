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
 *     exactly once — measured via `jest --listTests`: 28 files, no duplicates. The
 *     gate therefore does NOT invoke `test:leaks`; it affirms the budget tests by
 *     name from the whole-suite `--json` report. Keep that in mind before adding a
 *     second lane that selects this project: two executions where only one is
 *     affirmed is what the recomposition removed.
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

// PLATFORM-DECLARED SUITE EXCLUSIONS (issue #3640, debt tracked in #1979).
// `node-ci.yml`'s `test` job became a REQUIRED GATING TIER on all three
// platforms, and two suites fail DETERMINISTICALLY on windows-latest for
// Windows filesystem reasons in the test harness (measured on run 33488084650,
// 2026-09-01: `refresh.test.js` — a deleted generation is still mmap'd so
// `readersRemoved` is 0 not 1; `execute-deprecation.test.js` — an `fs.rmSync`
// teardown raises ENOTEMPTY). Gating Windows with those two included would red
// EVERY mandating pull request, so the workflow names them explicitly here.
//
// It is an ADDITIVE list, never a replacement: a CLI
// `--testPathIgnorePatterns` would OVERRIDE the project patterns below and so
// silently un-ignore `/node_modules/` and the leak lane. Setting the variable
// with no usable pattern THROWS — a declared exclusion that excludes nothing is
// a lie about what ran — and every applied pattern is printed, because a lane
// that omits coverage silently is indistinguishable from one that covers it.
// Removing the workflow's use of this variable is #1979's completion criterion.
const EXTRA_IGNORE_ENV = 'CQLITE_JEST_IGNORE_SUITES';

function declaredExtraIgnorePatterns() {
  const raw = process.env[EXTRA_IGNORE_ENV];
  if (raw === undefined) return [];

  const patterns = raw
    .split(',')
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
  if (patterns.length === 0) {
    throw new Error(
      `${EXTRA_IGNORE_ENV} is set to ${JSON.stringify(raw)}, which names no test-path pattern. ` +
        'Unset it or give it a comma-separated list of patterns (issue #3640).',
    );
  }
  // eslint-disable-next-line no-console
  console.error(
    `jest.config.js: ${EXTRA_IGNORE_ENV} excludes ${patterns.length} test path pattern(s) ` +
      `from this run: ${patterns.join(', ')} (issue #1979)`,
  );
  return patterns;
}

const EXTRA_IGNORE = declaredExtraIgnorePatterns();

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
      testPathIgnorePatterns: [
        '/node_modules/',
        '/__test__/leak-paths\\.test\\.js$',
        ...EXTRA_IGNORE,
      ],
    },
    {
      ...baseProject,
      displayName: 'leaks',
      testMatch: [LEAK_TEST],
      // The declared exclusions apply here too, so one variable governs the
      // whole `npm test` run rather than half of it.
      testPathIgnorePatterns: ['/node_modules/', ...EXTRA_IGNORE],
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
