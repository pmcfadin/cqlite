/**
 * Tests for the dataset ASSERTION helper itself (issue #3641).
 *
 * `assertDatasetsAvailable()` was called `skipIfNoDatasets()` for its whole life
 * while throwing on every call. The behaviour was never in doubt — #1458 wants a
 * missing corpus to fail loudly — but nothing ASSERTED it: the contract lived in
 * a doc comment, and a doc comment does not travel to the 14 call sites. So the
 * misnomer was free to be believed, and it was: the agent gate's `node-bindings`
 * component had to be designed around a reader's expectation that a "skip" helper
 * makes a reduced-coverage run available over an absent corpus, when in fact
 * every dataset-gated suite THROWS.
 *
 * These tests pin the three properties the new name claims, so that reverting to
 * a skip — option 2 of #3641, which is rejected unless #1458 is re-argued — is a
 * RED test rather than a rename nobody notices.
 *
 * The corpus is irrelevant here: these drive `global.DATASETS_AVAILABLE` directly,
 * so this file is deliberately NOT dataset-gated and runs on an empty root.
 */
const fs = require('fs');
const path = require('path');

const helpers = require('./helpers.js');
const { assertDatasetsAvailable } = helpers;

describe('assertDatasetsAvailable() (issue #3641)', () => {
  let savedAvailable;
  let savedRequireFixtures;

  beforeEach(() => {
    savedAvailable = global.DATASETS_AVAILABLE;
    savedRequireFixtures = global.REQUIRE_FIXTURES;
  });

  afterEach(() => {
    global.DATASETS_AVAILABLE = savedAvailable;
    global.REQUIRE_FIXTURES = savedRequireFixtures;
  });

  test('THROWS when the corpus is unavailable — it does not skip', () => {
    global.DATASETS_AVAILABLE = false;
    expect(() => assertDatasetsAvailable()).toThrow(/Test data not available/);
  });

  test('the throw names both remedies (env var and fetch script)', () => {
    global.DATASETS_AVAILABLE = false;
    // A loud failure whose message does not say what to DO is only half of
    // #1458's intent; every one of the 14 suites surfaces this string.
    expect(() => assertDatasetsAvailable()).toThrow(/CQLITE_DATASETS_ROOT/);
    expect(() => assertDatasetsAvailable()).toThrow(/fetch-datasets\.sh/);
  });

  test('returns quietly when the corpus is available', () => {
    global.DATASETS_AVAILABLE = true;
    expect(() => assertDatasetsAvailable()).not.toThrow();
    expect(assertDatasetsAvailable()).toBeUndefined();
  });

  // The reason the helper is NOT called `requireDatasets()`: `require` is this
  // suite's strict-mode vocabulary (CQLITE_REQUIRE_FIXTURES /
  // CQLITE_PARITY_REQUIRE_DATASETS -> global.REQUIRE_FIXTURES in setup.js), and
  // this assertion is independent of it. `scripts/agent-gate.sh` states that
  // independence as load-bearing — it is why the `node-bindings` component must
  // SKIP wholesale rather than run leniently under
  // AGENT_GATE_ALLOW_MISSING_FIXTURES=1 — so it is asserted here in BOTH
  // directions rather than left as a claim in a shell comment.
  test('is independent of strict mode: throws with REQUIRE_FIXTURES off', () => {
    global.DATASETS_AVAILABLE = false;
    global.REQUIRE_FIXTURES = false;
    expect(() => assertDatasetsAvailable()).toThrow(/Test data not available/);
  });

  test('is independent of strict mode: silent with REQUIRE_FIXTURES on', () => {
    global.DATASETS_AVAILABLE = true;
    global.REQUIRE_FIXTURES = true;
    expect(() => assertDatasetsAvailable()).not.toThrow();
  });

  test('exposes no skip-named entry point', () => {
    // Reintroducing the old name as an alias would restore the misnomer this
    // issue removed while leaving every call site green.
    expect(Object.keys(helpers)).toContain('assertDatasetsAvailable');
    expect(Object.keys(helpers)).not.toContain('skipIfNoDatasets');
  });
});

describe('the dataset assertion is not duplicated (issue #3641)', () => {
  // Three suites (conversion-budget, leak-paths, and event-loop-latency via a
  // local `requireData()`) used to carry a verbatim copy of the helper's body,
  // which is why the "14 dataset-gated suites" doctrine count did not match the
  // 11 files that actually called it. They now call the helper. A fourth copy
  // would silently re-open that gap, so it is a RED here.
  test('no test file inlines the helper error message', () => {
    const dir = __dirname;
    const offenders = fs
      .readdirSync(dir)
      .filter((name) => name.endsWith('.test.js'))
      .filter((name) => {
        const body = fs.readFileSync(path.join(dir, name), 'utf8');
        // Outside this file the literal only ever appears as a COPY of the
        // helper's message; this file quotes it in the assertions above, which
        // is why it excludes itself.
        return name !== 'helpers.test.js' && body.includes('Test data not available');
      });
    expect(offenders).toEqual([]);
  });
});
