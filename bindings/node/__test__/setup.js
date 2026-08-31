/**
 * Jest setup file for CQLite Node.js bindings tests.
 *
 * Issue #306: Centralized test configuration matching Python conftest.py patterns.
 *
 * This module provides:
 * - Centralized path constants
 * - Dataset availability detection
 * - Slow test handling (RUN_SLOW_TESTS environment variable)
 */
const path = require('path');
const fs = require('fs');

// =============================================================================
// Path Constants (matching Python conftest.py pattern)
// =============================================================================

const TESTS_DIR = __dirname;
const BINDINGS_DIR = path.dirname(TESTS_DIR);
const PROJECT_ROOT = path.resolve(BINDINGS_DIR, '..', '..');

// Support CQLITE_DATASETS_ROOT environment variable (used in CI)
const TEST_DATA_ROOT = process.env.CQLITE_DATASETS_ROOT ||
  path.join(PROJECT_ROOT, 'test-data', 'datasets');
const SSTABLES_DIR = path.join(TEST_DATA_ROOT, 'sstables');
// Issue #3493: honour CQLITE_SCHEMAS_ROOT. The agent gate's #3148 preflight resolves a
// schemas root -- an out-of-tree override when set and usable, else the checkout's --
// validates it, and STAMPS it in the SUMMARY. Ignoring the variable here meant a run
// could certify one schemas root while this suite read another, once the gate started
// running the whole suite.
//
// This MIRRORS the gate's `_gate_schemas_root` / Rust `resolve_schemas_root` rather than
// inventing a third contract (roborev round 8, Medium -- the first cut diverged on two
// of the three cases and would have failed ordinary `npm test` runs that the other
// runners resolve fine):
//   * blank / whitespace-only  -> treated as UNSET (the gate's override_present test is
//                                 `*[![:space:]]*`, i.e. presence requires a non-space
//                                 character), so an exported-but-empty value is not an
//                                 attempt to override and must not be an error;
//   * relative                 -> REJECTED, loudly. This is the one case that must fail
//                                 rather than fall back: the gate resolves it against the
//                                 repo root while each runner resolves against its own
//                                 cwd, so it would silently mean two different roots;
//   * absolute, not a directory-> fall back to the checkout, exactly as
//                                 `_gate_schemas_root`'s `[ -d ... ]` guard does. A stale
//                                 exported path is not a request to fail every run.
// BLANKNESS IS THE UNICODE WHITE_SPACE PROPERTY -- the same set all three resolvers use
// (roborev #3493 rounds 35-46).
//
// Six rules were tried here and five were wrong. The first four were each off by one
// character (`trim()` on U+FEFF, `\p{White_Space}` measured against a locale-specific
// gate on U+0085, ASCII-only on U+2003, the glibc set on U+2007). The fifth was wrong in
// KIND: I mirrored a MEASUREMENT of the gate's `[[:space:]]`, which is locale-sensitive --
// with a lone U+2003 the gate answered "present" under LC_ALL=C and "blank" under UTF-8,
// so no fixed mirror could be right in both.
//
// Round 45 pinned both sides to ASCII, which made gate and Node deterministic but moved
// the gate AWAY from Rust's `trim()` in the common UTF-8 case -- trading one divergence
// for another. The end state is to pick ONE contract and implement it explicitly
// everywhere: `char::is_whitespace` IS the Unicode White_Space property, so Rust already
// has it, the gate now strips exactly that set by byte substitution (locale-independent),
// and this is the same property natively. Agreement is by construction, not by
// measurement.
//
// U+FEFF is NOT White_Space (Unicode removed it), so a BOM-only value stays present in
// all three -- which is why `String.trim()`, that strips it, was wrong at the start.
const SCHEMAS_ROOT_RAW = process.env.CQLITE_SCHEMAS_ROOT;
const SCHEMAS_ROOT_OVERRIDE =
  SCHEMAS_ROOT_RAW !== undefined &&
  !/^\p{White_Space}*$/u.test(SCHEMAS_ROOT_RAW)
    ? SCHEMAS_ROOT_RAW
    : undefined;
// Control characters are rejected BEFORE the absolute test, in that order, because the
// gate rejects them first too and reports that as the reason (roborev round 12, Low --
// the first cut accepted an absolute path carrying one, so the two "mirrors" diverged
// on a case the gate has an explicit rule for).
//
// `\p{Cc}` -- the full Unicode Cc category -- rather than a hand-written C0+DEL range
// (roborev round 13, Low). That range omitted the C1 block U+0080-U+009F, and BOTH other
// resolvers reject it: `fixture_roots.rs` uses `char::is_control` (which IS Cc), and the
// gate's `[[:cntrl:]]` matches C1 under this locale -- verified, not assumed, with
// U+0085. So the hand-written range made JavaScript the one outlier of three, in a
// mirror whose entire value is that it does not diverge. Naming the category instead of
// enumerating a range is also what stops the next Unicode-shaped gap.
if (SCHEMAS_ROOT_OVERRIDE !== undefined && /\p{Cc}/u.test(SCHEMAS_ROOT_OVERRIDE)) {
  throw new Error(
    'CQLITE_SCHEMAS_ROOT must not contain control characters (newline/CR/tab), got ' +
    JSON.stringify(SCHEMAS_ROOT_OVERRIDE)
  );
}
if (SCHEMAS_ROOT_OVERRIDE !== undefined && !path.isAbsolute(SCHEMAS_ROOT_OVERRIDE)) {
  throw new Error(
    `CQLITE_SCHEMAS_ROOT must be an absolute path, got '${SCHEMAS_ROOT_OVERRIDE}' ` +
    '(the gate resolves it against the repo root while each runner resolves it against ' +
    'its own cwd, so a relative value would silently mean two different roots)'
  );
}
const SCHEMAS_DIR =
  SCHEMAS_ROOT_OVERRIDE !== undefined && fs.existsSync(SCHEMAS_ROOT_OVERRIDE) &&
  fs.statSync(SCHEMAS_ROOT_OVERRIDE).isDirectory()
    ? SCHEMAS_ROOT_OVERRIDE
    : path.join(PROJECT_ROOT, 'test-data', 'schemas');

// Schema file paths for different test keyspaces
const SCHEMA_BASIC_TYPES = path.join(SCHEMAS_DIR, 'basic-types.cql');
const SCHEMA_COLLECTIONS = path.join(SCHEMAS_DIR, 'collections.cql');
const SCHEMA_TIME_SERIES = path.join(SCHEMAS_DIR, 'time-series.cql');
const SCHEMA_WIDE_ROWS = path.join(SCHEMAS_DIR, 'wide-rows.cql');
// Issue #656 (VG4): oa test schema for test_oa keyspace
const SCHEMA_OA_TEST = path.join(SCHEMAS_DIR, 'oa-test.cql');
// Issue #3493 round 10: write.test.js and write-smoke.test.js used to build this path
// themselves from `__dirname/../../../test-data/schemas`, which BYPASSES the resolver
// above -- so CQLITE_SCHEMAS_ROOT was honoured by some of the suite and ignored by the
// rest, and the gate could certify an out-of-tree root that parts of Jest never read.
// Every schema the suite needs must come from here.
const SCHEMA_WRITE_TEST = path.join(SCHEMAS_DIR, 'write-test.cql');

// =============================================================================
// Dataset Availability Detection
// =============================================================================

/**
 * Recursively check whether `dir` contains at least one `*-Data.db` SSTable
 * binary FILE (issue #1458).
 *
 * Directory existence alone is NOT evidence of a corpus: a present-but-EMPTY
 * sstables dir is the exact shape of the original #773 failure and used to
 * count as "available", false-greening a broken fixture setup.
 *
 * Three traversal rules, DELIBERATELY IDENTICAL to those of the Python guard's
 * `_data_db_files()` in bindings/python/tests/conftest.py, which gets them for
 * free from a recursive `Path.glob` of `*-Data.db` filtered by `Path.is_file()`:
 *   1. a symlinked DIRECTORY is NOT traversed (recursion enters real dirs only);
 *   2. a symlinked FILE whose target is a regular file IS counted (`is_file()`,
 *      like `statSync`, follows the link);
 *   3. a FIFO, socket, device node or directory named `*-Data.db` is NOT
 *      counted -- only a regular file qualifies.
 *
 * The two implementations are ONE contract written twice: whoever changes the
 * rules here must change `_data_db_files()` too, and vice versa.
 *
 * Consequence, recorded honestly: a symlinked KEYSPACE DIRECTORY is unsupported
 * in BOTH languages. No corpus uses that layout (measured: zero symlinks under
 * the fleet corpus and under the checkout's test-data/datasets), and rule 1
 * fails in the SAFE direction -- such a root reports zero fixtures, so strict
 * mode throws the existing loud "0 *-Data.db files" error rather than silently
 * skipping.
 *
 * Rule 1 also makes symlink CYCLES structurally unreachable (only a followed
 * directory link could close one), so this walk needs no realpath/visited-set
 * cycle machinery.
 *
 * @param {string} dir
 * @returns {boolean}
 */
function hasDataDbFile(dir) {
  let entries;
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch (err) {
    // Unreadable/absent dir contributes no fixtures.
    return false;
  }
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      // Rule 1: withFileTypes never follows symlinks, so isDirectory() is true
      // for REAL directories only and a symlinked dir is skipped here.
      if (hasDataDbFile(full)) return true;
    } else if (entry.name.endsWith('-Data.db')) {
      // Rule 3: a regular file qualifies immediately.
      if (entry.isFile()) return true;
      if (entry.isSymbolicLink()) {
        // Rule 2: follow the link -- it counts iff the target is a regular
        // file. A broken link (or one to a FIFO/socket/dir) contributes none.
        try {
          if (fs.statSync(full).isFile()) return true;
        } catch (err) {
          continue;
        }
      }
    }
  }
  return false;
}

// Issue #1458: content-aware AND still test_basic-scoped.
//
// BOTH halves are load-bearing, and content-awareness was ADDED to the original
// `existsSync(SSTABLES_DIR/test_basic)` requirement -- never swapped for it. A
// corpus-WIDE `hasDataDbFile(SSTABLES_DIR)` accepts a root holding only, say,
// test_collections/ and then enables EVERY dataset-dependent suite including the
// test_basic ones that cannot possibly pass: a net WEAKENING of this guard.
// Asking for content INSIDE test_basic implies the dir exists AND that the
// corpus is non-empty, so this is strictly stronger than either half alone.
// Do not "simplify" it back to a corpus-wide check.
const TEST_BASIC_DIR = path.join(SSTABLES_DIR, 'test_basic');
const DATASETS_AVAILABLE = fs.existsSync(SSTABLES_DIR) && hasDataDbFile(TEST_BASIC_DIR);

// Strict fixture mode (issue #1230/#1458). Mirrors the Python
// _require_fixtures_strict() helper: same two env var names, and the same
// accepted truthy spellings ('1', 'true'). No other names are recognised.
const REQUIRE_FIXTURES = ['1', 'true'].includes(process.env.CQLITE_REQUIRE_FIXTURES) ||
  ['1', 'true'].includes(process.env.CQLITE_PARITY_REQUIRE_DATASETS);

// =============================================================================
// Slow Test Handling (matching Python conftest.py RUN_SLOW_TESTS pattern)
// =============================================================================

const SHOULD_RUN_SLOW_TESTS = process.env.RUN_SLOW_TESTS === '1';

// =============================================================================
// Export to global scope for test files
// =============================================================================

global.testPaths = {
  PROJECT_ROOT,
  TEST_DATA_ROOT,
  SSTABLES_DIR,
  SCHEMAS_DIR,
  SCHEMA_BASIC_TYPES,
  SCHEMA_COLLECTIONS,
  SCHEMA_TIME_SERIES,
  SCHEMA_WIDE_ROWS,
  SCHEMA_OA_TEST,
  SCHEMA_WRITE_TEST,
};

global.DATASETS_AVAILABLE = DATASETS_AVAILABLE;
global.REQUIRE_FIXTURES = REQUIRE_FIXTURES;
global.SHOULD_RUN_SLOW_TESTS = SHOULD_RUN_SLOW_TESTS;

// Log test configuration (only in verbose mode)
if (process.env.DEBUG_TESTS) {
  console.log('Test Configuration:');
  console.log(`  PROJECT_ROOT: ${PROJECT_ROOT}`);
  console.log(`  SSTABLES_DIR: ${SSTABLES_DIR}`);
  console.log(`  DATASETS_AVAILABLE: ${DATASETS_AVAILABLE}`);
  console.log(`  REQUIRE_FIXTURES: ${REQUIRE_FIXTURES}`);
  console.log(`  SHOULD_RUN_SLOW_TESTS: ${SHOULD_RUN_SLOW_TESTS}`);
}

// Issue #1458: under strict mode a missing corpus is a hard failure of the
// WHOLE suite -- throwing here beats leaving DATASETS_AVAILABLE=false for
// describe.skip, which would report a green run over zero real assertions.
if (REQUIRE_FIXTURES && !DATASETS_AVAILABLE) {
  throw new Error(
    `No SSTable fixtures found: ${TEST_BASIC_DIR} is absent or contains 0 *-Data.db files ` +
    '(CQLITE_REQUIRE_FIXTURES=1 — fetch with bash test-data/scripts/fetch-datasets.sh)'
  );
}
