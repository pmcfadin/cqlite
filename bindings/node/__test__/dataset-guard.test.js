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
const { execFileSync, spawnSync } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const SETUP_JS = path.join(__dirname, 'setup.js');
// Upper bound on a nested jest run (see the spawnSync call for why it matters).
const CHILD_TIMEOUT_MS = 90000;
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
 * Corpus shapes a child jest run can be pointed at. Each shape owns both the
 * on-disk layout AND the child test body, because the body decides what a
 * non-zero exit is allowed to mean.
 *
 * @type {Record<string, {build: (sstables: string) => void, childTest: string}>}
 */
const CORPUS_SHAPES = {
  // Present-but-empty: sstables/test_basic/ exists, no *-Data.db anywhere.
  // The body asserts the guard's own verdict, so the non-strict control proves
  // the run both survived AND saw an unavailable corpus.
  empty: {
    build: (sstables) => {
      fs.mkdirSync(path.join(sstables, 'test_basic'), { recursive: true });
    },
    childTest: "test('noop', () => { expect(global.DATASETS_AVAILABLE).toBe(false); });\n",
  },
  // test_basic/ present but EMPTY, while a DIFFERENT keyspace holds a
  // *-Data.db. A corpus-WIDE content check that dropped the test_basic
  // requirement reports this as available and then enables the
  // test_basic-dependent suites, which cannot possibly pass (issue #1458).
  //
  // The body here is deliberately assertion-FREE: the guard throwing must be
  // the only possible source of a non-zero exit, otherwise a failing child
  // assertion would green this test against the very bug it pins.
  otherKeyspaceOnly: {
    build: (sstables) => {
      fs.mkdirSync(path.join(sstables, 'test_basic'), { recursive: true });
      fs.mkdirSync(path.join(sstables, 'test_collections'), { recursive: true });
      fs.writeFileSync(path.join(sstables, 'test_collections', 'nb-1-big-Data.db'), '');
    },
    childTest: "test('noop', () => {});\n",
  },
  // A NON-REGULAR entry named `*-Data.db` inside test_basic: a FIFO (falling
  // back to a directory where mkfifo is unavailable). Name-only matching
  // reports this as a corpus and enables every dataset-dependent suite, which
  // then fails on the first read -- the guard must require a REGULAR FILE,
  // exactly as Python's `Path.is_file()` filter does (issue #1458).
  //
  // Body is deliberately assertion-FREE (see otherKeyspaceOnly): the guard
  // throwing must be the only possible source of a non-zero exit.
  nonRegularDataDb: {
    build: (sstables) => {
      const testBasic = path.join(sstables, 'test_basic');
      fs.mkdirSync(testBasic, { recursive: true });
      const fifo = path.join(testBasic, 'nb-1-big-Data.db');
      try {
        execFileSync('mkfifo', [fifo]);
      } catch (err) {
        // No mkfifo (non-POSIX host): a DIRECTORY named *-Data.db is the other
        // non-regular shape the is_file() filter must reject.
        fs.mkdirSync(fifo, { recursive: true });
      }
    },
    childTest: "test('noop', () => {});\n",
  },
  // POSITIVE control for the other half of the regular-file rule: a *-Data.db
  // that is a SYMLINK to a real regular file still counts, because Python's
  // `Path.is_file()` follows links. Pins rule 2 so a future tightening of the
  // non-regular rejection above cannot silently drop symlinked fixtures.
  symlinkedDataFile: {
    build: (sstables) => {
      const testBasic = path.join(sstables, 'test_basic');
      fs.mkdirSync(testBasic, { recursive: true });
      const payload = path.join(sstables, '..', 'payload.bin');
      fs.writeFileSync(payload, 'sstable bytes');
      fs.symlinkSync(payload, path.join(testBasic, 'nb-1-big-Data.db'), 'file');
    },
    childTest: "test('noop', () => { expect(global.DATASETS_AVAILABLE).toBe(true); });\n",
  },
  // A self-referential symlink inside test_basic. Symlinked DIRECTORIES are not
  // traversed (matching Python's recursive glob), which is what makes a cycle
  // structurally unreachable; this case pins that observable contract -- the
  // link is skipped, the corpus reports unavailable, and setup never crashes in
  // NON-strict mode (issue #1458).
  //
  // HONESTY NOTE: this case is a behavioral CONTROL, not a red-then-green
  // regression pin -- no shipped version of this walk crashed here. Measured on
  // Linux, the earlier symlink-following walk did not blow the stack either:
  // the kernel's 40-level symlink cap makes statSync() fail ELOOP at recursion
  // depth ~81, which its broken-symlink `continue` swallowed. Now that
  // directory links are skipped outright there is no cycle machinery left to
  // exercise, so what this case guards is the contract, on every platform.
  symlinkCycle: {
    build: (sstables) => {
      const testBasic = path.join(sstables, 'test_basic');
      fs.mkdirSync(testBasic, { recursive: true });
      fs.symlinkSync(sstables, path.join(testBasic, 'loop'), 'dir');
    },
    childTest: "test('noop', () => { expect(global.DATASETS_AVAILABLE).toBe(false); });\n",
  },
};

/**
 * Run a child jest process whose ONLY setup file is the real setup.js, against
 * a synthesized datasets root of the requested shape.
 *
 * @param {{strict: boolean, corpus?: keyof typeof CORPUS_SHAPES}} opts
 * @returns {{status: number, stdout: string, stderr: string}}
 */
function runChildJest({ strict, corpus = 'empty' }) {
  const shape = CORPUS_SHAPES[corpus];
  if (!shape) throw new Error(`unknown corpus shape: ${corpus}`);
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cqlite-dataset-guard-'));
  shape.build(path.join(tmp, 'datasets', 'sstables'));
  fs.writeFileSync(path.join(tmp, 'noop.test.js'), shape.childTest);

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
    // A finite bound is load-bearing, not hygiene: jest's own per-test timeout
    // cannot interrupt a SYNCHRONOUS spawnSync, so without this a wedged child
    // stalls the whole test job indefinitely. Kept below the 120s per-test
    // timeout so a hang surfaces as the explicit throw below.
    timeout: CHILD_TIMEOUT_MS,
    killSignal: 'SIGKILL',
  });

  // A timed-out or unstartable child yields status === null, and the strict
  // cases assert `status !== 0` -- so an unreported hang would MASQUERADE as
  // the strict-mode failure they are trying to prove. Fail loudly instead.
  if (result.error) {
    throw new Error(
      `child jest did not complete (corpus=${corpus} strict=${strict}): ${result.error.message}`
    );
  }
  if (result.status === null) {
    throw new Error(
      `child jest was killed without an exit status (corpus=${corpus} strict=${strict}; ` +
      `signal=${result.signal}) -- treated as a harness failure, never as a guard verdict`
    );
  }
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

  // Regression: content-awareness was ADDED to the test_basic requirement, not
  // swapped for it. A *-Data.db under some OTHER keyspace does not make the
  // test_basic-dependent suites runnable, so it must not report available.
  test('strict mode fails when test_basic is empty but another keyspace has data', () => {
    const { status, stdout, stderr } = runChildJest({
      strict: true,
      corpus: 'otherKeyspaceOnly',
    });
    const output = `${stdout}${stderr}`;
    if (status === 0) {
      throw new Error(`expected non-zero exit, got 0\n${output}`);
    }
    expect(output).toMatch(/test_basic/);
    expect(output).toMatch(/-Data\.db/);
  }, 120000);

  // Symlinked directories are skipped, so a link pointing at an ancestor is
  // never entered: the walk terminates quietly (exit 0, corpus unavailable)
  // rather than crashing setup.
  test('a self-referential symlink does not crash the walk (non-strict)', () => {
    const { status, stdout, stderr } = runChildJest({ strict: false, corpus: 'symlinkCycle' });
    const output = `${stdout}${stderr}`;
    if (status !== 0) {
      throw new Error(`expected exit 0, got ${status}\n${output}`);
    }
    expect(output).not.toMatch(/Maximum call stack/);
  }, 120000);

  // Regression (roborev round 3): name-only matching accepted a FIFO/directory
  // named *-Data.db as a fixture. Only a REGULAR FILE is a corpus, matching
  // Python's `Path.is_file()` filter.
  test('strict mode fails when the only *-Data.db entry is not a regular file', () => {
    const { status, stdout, stderr } = runChildJest({
      strict: true,
      corpus: 'nonRegularDataDb',
    });
    const output = `${stdout}${stderr}`;
    if (status === 0) {
      throw new Error(`expected non-zero exit, got 0\n${output}`);
    }
    expect(output).toMatch(/test_basic/);
    expect(output).toMatch(/-Data\.db/);
  }, 120000);

  // Positive control for the other half of the same rule: `is_file()` follows
  // symlinks, so a *-Data.db symlinked to a real regular file still counts.
  test('a *-Data.db symlinked to a regular file still counts as available', () => {
    const { status, stdout, stderr } = runChildJest({
      strict: false,
      corpus: 'symlinkedDataFile',
    });
    const output = `${stdout}${stderr}`;
    if (status !== 0) {
      throw new Error(`expected exit 0, got ${status}\n${output}`);
    }
    expect(status).toBe(0);
  }, 120000);
});
