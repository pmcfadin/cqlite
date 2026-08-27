/**
 * Tests for the dataset-availability guard itself (issue #1458).
 *
 * The guard IS the fail-loudly mechanism for missing SSTable fixtures, so it
 * needs its own coverage: a present-but-EMPTY sstables directory (the exact
 * shape of the original #773 failure) must FAIL the suite under strict mode,
 * never silently `describe.skip`.
 *
 * These tests drive the REAL `setup.js` in a child `jest` process — they never
 * reimplement the directory walk — because a test that reimplements the logic
 * it asserts on is invariant to the bug (#3042).
 */
const { spawnSync } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const SETUP_JS = path.join(__dirname, 'setup.js');
const PACKAGE_DIR = path.dirname(__dirname);

/**
 * Locate the jest CLI entry point. jest's own module resolver refuses
 * `require.resolve('jest/bin/jest.js')` (package `exports` map), so resolve the
 * conventional install locations directly -- the same path package.json's
 * `test` script uses. Missing => throw (fail loudly), never skip.
 *
 * @returns {string}
 */
function resolveJestBin() {
  const candidates = [
    path.join(PACKAGE_DIR, 'node_modules', 'jest', 'bin', 'jest.js'),
    path.join(PACKAGE_DIR, '..', '..', 'node_modules', 'jest', 'bin', 'jest.js'),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }
  throw new Error(`jest CLI not found; looked in:\n  ${candidates.join('\n  ')}`);
}

/**
 * Run a child jest process whose ONLY setup file is the real setup.js, against
 * a datasets root that exists but holds zero *-Data.db files.
 *
 * @param {{strict: boolean}} opts
 * @returns {{status: number, stdout: string, stderr: string}}
 */
function runChildJest({ strict }) {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cqlite-dataset-guard-'));
  // Present-but-empty corpus: sstables/test_basic/ exists, no *-Data.db anywhere.
  fs.mkdirSync(path.join(tmp, 'datasets', 'sstables', 'test_basic'), { recursive: true });
  fs.writeFileSync(
    path.join(tmp, 'noop.test.js'),
    "test('noop', () => { expect(global.DATASETS_AVAILABLE).toBe(false); });\n"
  );

  const config = JSON.stringify({
    testEnvironment: 'node',
    rootDir: tmp,
    testMatch: ['**/*.test.js'],
    setupFilesAfterEnv: [SETUP_JS],
  });

  const env = { ...process.env, CQLITE_DATASETS_ROOT: path.join(tmp, 'datasets') };
  delete env.CQLITE_REQUIRE_FIXTURES;
  delete env.CQLITE_PARITY_REQUIRE_DATASETS;
  // Jest sets these for its own workers; leaking them into a nested run confuses it.
  delete env.JEST_WORKER_ID;
  if (strict) env.CQLITE_REQUIRE_FIXTURES = '1';

  const result = spawnSync(process.execPath, [resolveJestBin(), '--config', config, '--ci'], {
    env,
    encoding: 'utf8',
    cwd: tmp,
  });
  return { status: result.status, stdout: result.stdout || '', stderr: result.stderr || '' };
}

describe('dataset guard (issue #1458)', () => {
  test('strict mode fails the suite on a present-but-empty datasets dir', () => {
    const { status, stdout, stderr } = runChildJest({ strict: true });
    const output = `${stdout}${stderr}`;
    expect(status).not.toBe(0);
    expect(output).toMatch(/-Data\.db/);
  }, 120000);

  test('non-strict mode still passes (skips) on a present-but-empty datasets dir', () => {
    const { status, stdout, stderr } = runChildJest({ strict: false });
    if (status !== 0) {
      throw new Error(`expected exit 0, got ${status}\n${stdout}${stderr}`);
    }
    expect(status).toBe(0);
  }, 120000);
});
