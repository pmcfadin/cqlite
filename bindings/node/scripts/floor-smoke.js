/**
 * Floor smoke for the CQLite Node.js bindings (issue #1459).
 *
 * `package.json` advertises `engines.node: "^18.17.0 || >= 20.3.0"`, but CI only
 * ever ran a recent Node 20 — so the advertised boundaries were never executed.
 * The range is DISCONTINUOUS because `Cargo.toml` enables napi9, and Node-API 9
 * ships in 18.17.0+ and 20.3.0+ but NEVER in 19.x or 20.0-20.2. Two earlier
 * spellings were both FALSE claims: `">= 18"` (18.0.0-18.16.x cannot load the
 * module at all) and `">= 18.17.0"` (which still swept in 19.x and 20.0-20.2).
 * See issue #1459. This script is what each leg of the `smoke-floor` matrix in
 * `.github/workflows/node-ci.yml` runs on its exact boundary version against
 * the *already-built* prebuilt `.node` artifact. It deliberately does NOT
 * rebuild: we ship that prebuilt binary, and a napi-rs module built once must
 * load across every Node major we claim (Node-API is ABI-stable). If it does
 * not, the claim is false and this job is the thing that says so.
 *
 * Three checks, in order of increasing strength:
 *
 *   1. The interpreter really is the EXACT version we meant to test.
 *      `setup-node` silently leaving the runner's default Node in place would
 *      otherwise make this whole job a second, redundant Node-20 run that
 *      reports a floor it never touched. `CQLITE_FLOOR_EXPECT_NODE_VERSION` is
 *      a MANDATORY input: an unset value FAILS rather than skipping the check,
 *      so the guarantee is unconditional — matching the Python leg, which
 *      asserts its interpreter unconditionally in the workflow itself.
 *   2. The native module loads and `version()` returns a semver string.
 *      Catches an ABI/loader break on this major.
 *   3. One real query against the canonical corpus. ZERO rows is a FAILURE
 *      (exit 1), not a pass — a silently-empty result is exactly the shape of
 *      the bug this is meant to catch (see CLAUDE.md, "never let a
 *      dataset-dependent test pass on an empty dataset").
 *
 * Fixture handling is TWO-MODE, because "the fixtures were missing" and "the
 * query ran and was fine" must never reach the same verdict:
 *
 *   - `CQLITE_FLOOR_STRICT_FIXTURES=1` (set by CI, which restores the corpus
 *     first): absent corpus/schema is a FAILURE. Otherwise a broken restore
 *     step would let this job report a green floor having never executed the
 *     query it exists to guarantee — the permissive-branch shape CLAUDE.md
 *     forbids ("a positive verdict requires an affirmative measurement").
 *   - unset (local invocation): absent fixtures SKIP check 3 loudly and visibly
 *     with a `::warning::` naming the path, and checks 1-2 still govern the exit
 *     status, so a developer without the corpus can still smoke the wheel.
 */

'use strict';

const path = require('path');
const fs = require('fs');

const SCRIPTS_DIR = __dirname;
const BINDINGS_NODE_DIR = path.dirname(SCRIPTS_DIR);
const PROJECT_ROOT = path.resolve(BINDINGS_NODE_DIR, '..', '..');

// Mirrors `__test__/setup.js`: CQLITE_DATASETS_ROOT names the corpus root (the
// parent of `sstables/`).
const TEST_DATA_ROOT =
  process.env.CQLITE_DATASETS_ROOT || path.join(PROJECT_ROOT, 'test-data', 'datasets');
const SSTABLES_DIR = path.join(TEST_DATA_ROOT, 'sstables');
// The CQL schemas are committed source resolved checkout-relative (issue
// #3148); CQLITE_SCHEMAS_ROOT is an optional out-of-tree override.
const SCHEMAS_DIR =
  process.env.CQLITE_SCHEMAS_ROOT || path.join(PROJECT_ROOT, 'test-data', 'schemas');
const SCHEMA_BASIC_TYPES = path.join(SCHEMAS_DIR, 'basic-types.cql');

const SMOKE_KEYSPACE_TABLE = 'test_basic.simple_table';
const SMOKE_QUERY = `SELECT * FROM ${SMOKE_KEYSPACE_TABLE} LIMIT 1`;

/**
 * Check 1: assert we run on the EXACT Node version this job claims to test.
 *
 * `CQLITE_FLOOR_EXPECT_NODE_VERSION` is MANDATORY, and its absence FAILS. A
 * positive verdict requires an affirmative measurement (CLAUDE.md): if the
 * variable is unset we do not know which interpreter this is, and "we could not
 * tell" must never take the permissive branch. Concretely — for an issue whose
 * whole subject is "CI claimed a floor it never tested", a copied job or a
 * typo'd variable name would otherwise make this script report green having
 * never checked the interpreter at all.
 */
function checkNodeVersion() {
  const expected = process.env.CQLITE_FLOOR_EXPECT_NODE_VERSION;
  const actual = process.versions.node;
  if (!expected) {
    console.error(
      '::error::floor smoke: CQLITE_FLOOR_EXPECT_NODE_VERSION is unset, so the ' +
        `running interpreter (${process.version}) could not be checked against ` +
        'the version this job claims to test. Set it in the workflow step.'
    );
    return false;
  }

  // TWO modes, chosen by the shape of the expected value. Both are assertions —
  // neither is a permissive fallback, because an unmeasured interpreter is the
  // whole defect this script exists to prevent.
  //
  //   'X.Y.Z' (a BOUNDARY of engines.node) -> EXACT match. `napi9` becomes
  //     available AT a patch boundary (18.17.0, 20.3.0), so for those legs
  //     "some 18.x ran" would not prove the boundary; setup-node resolving to
  //     18.20.x must FAIL rather than report the boundary as tested.
  //   'X' (the current maintained MAJOR, whose newest patch is what we want)
  //     -> major match, because there is no specific patch being claimed.
  const exact = expected.includes('.');
  const actualMajor = actual.split('.')[0];
  if (exact ? actual !== expected : actualMajor !== expected) {
    console.error(
      `::error::floor smoke: expected Node ${expected} ` +
        `(${exact ? 'an advertised engines.node boundary, matched exactly' : 'the current maintained major'}) ` +
        `but this process is ${process.version}. The version was NOT tested.`
    );
    return false;
  }
  console.log(
    `node version OK: ${process.version} matches expected ${expected} ` +
      `(${exact ? 'exact boundary' : 'major'})`
  );
  return true;
}

/** Check 2: the prebuilt native module loads and reports a version. */
function checkLoad() {
  // Required lazily so check 1 can report a wrong-interpreter failure even if
  // the module cannot load on it at all.
  const cqlite = require(path.join(BINDINGS_NODE_DIR, 'index.js'));
  const reported = cqlite.version();
  if (typeof reported !== 'string' || !/^\d+\.\d+\.\d+/.test(reported)) {
    console.error(
      `::error::floor smoke: version() returned ${JSON.stringify(reported)}, ` +
        'which is not a semver string.'
    );
    return null;
  }
  console.log(`load OK: cqlite-node ${reported} loaded on ${process.version}`);
  return cqlite;
}

/** Check 3: one real query. Zero rows fails; an absent corpus skips loudly. */
async function checkRealQuery(cqlite) {
  const strict = process.env.CQLITE_FLOOR_STRICT_FIXTURES === '1';
  const missingFixture = (what, where, remedy) => {
    if (strict) {
      console.error(
        `::error::floor smoke: ${what} at ${where}, and ` +
          'CQLITE_FLOOR_STRICT_FIXTURES=1 — the real-query check could not run, ' +
          'so this job cannot certify the floor it claims to test. ' +
          `${remedy}`
      );
      return false;
    }
    console.log(
      `::warning::floor smoke: SKIPPING the real-query check — ${what} at ` +
        `${where}. The version and load checks still ran. ${remedy}`
    );
    return true;
  };

  if (!fs.existsSync(SSTABLES_DIR)) {
    return missingFixture(
      'no corpus directory',
      SSTABLES_DIR,
      'Set CQLITE_DATASETS_ROOT or run test-data/scripts/fetch-datasets.sh.'
    );
  }
  if (!fs.existsSync(SCHEMA_BASIC_TYPES)) {
    return missingFixture(
      'no schema file',
      SCHEMA_BASIC_TYPES,
      'The CQL schemas are committed source; check CQLITE_SCHEMAS_ROOT.'
    );
  }

  console.log(`query check: ${SMOKE_QUERY} (corpus ${SSTABLES_DIR})`);
  const db = await cqlite.Database.open(SSTABLES_DIR, { schema: SCHEMA_BASIC_TYPES });
  let rowCount;
  try {
    const result = await db.execute(SMOKE_QUERY);
    rowCount = result.rows.length;
  } finally {
    await db.close();
  }

  if (rowCount < 1) {
    console.error(
      `::error::floor smoke: ${SMOKE_QUERY} returned 0 rows on ${process.version}. ` +
        'The corpus is present, so an empty result is a real failure, not a skip.'
    );
    return false;
  }
  console.log(`query OK: ${rowCount} row(s) from ${SMOKE_KEYSPACE_TABLE}`);
  return true;
}

async function main() {
  console.log(`cqlite floor smoke on ${process.version}`);
  let ok = checkNodeVersion();
  const cqlite = checkLoad();
  if (cqlite === null) {
    return 1;
  }
  // Run the query check even when an earlier check failed: two findings in one
  // CI log beat one.
  ok = (await checkRealQuery(cqlite)) && ok;
  if (!ok) {
    return 1;
  }
  console.log('floor smoke PASSED');
  return 0;
}

main().then(
  (code) => process.exit(code),
  (err) => {
    console.error(`::error::floor smoke threw: ${err && err.stack ? err.stack : err}`);
    process.exit(1);
  }
);
