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

// The files allowed to contain the helper's error-message literal, each with the
// reason it is allowed (issue #3772). Add an entry ONLY when a suite quotes the
// message in order to ASSERT ON IT -- never to silence a fresh inline copy of the
// helper's body, which is the single thing this guard exists to catch. State which
// of the two it is in the reason, so the next reader can tell a legitimate quoter
// from a waived offender without re-deriving it.
//
// A named map rather than the inline `rel !== 'helpers.test.js'` self-exclusion it
// replaces: a guard with no exemption path reds on correct input the first time a
// suite legitimately needs the string, and a guard that reds on correct input is
// the guard agents learn to waive (CLAUDE.md). `dataset-guard.test.js` is the
// anticipated case -- it spawns child jest processes and asserts on their output,
// so it may one day need the literal -- and is DELIBERATELY NOT pre-added: it does
// not contain the string today (verified), and an entry that excuses a file needing
// no excuse only widens the hole.
// The needle is ASSEMBLED FROM FRAGMENTS rather than written out, so that this
// file -- the one file on the allow-list -- does not contain the literal merely by
// virtue of the guard that scans for it (roborev job 85). With the literal spelled
// out here, the "is this exemption still necessary" check below was
// SELF-SATISFYING: the guard's own `.includes(...)`/`.toContain(...)` kept the
// string present, so the check passed even if the assertDatasetsAvailable()
// assertions that actually justify the exemption were deleted. A check that cannot
// fail measures nothing -- the exact defect class issue #3772 exists to remove, and
// this is the third time in that issue it was reintroduced by its own fix.
//
// Assembled this way, the ONLY occurrences of the literal in this file are the
// `toThrow(/.../)` assertions at the top. Delete those and the allow-list entry
// stops being necessary and the check below FAILS, which is the property wanted.
const MESSAGE_LITERAL = ['Test data', 'not available'].join(' ');

const MESSAGE_LITERAL_ALLOWED = new Map([
  [
    'helpers.test.js',
    'quotes the message to assert on it in the assertDatasetsAvailable() tests above',
  ],
]);

describe('the helper error message is not inlined by another suite (issue #3641)', () => {
  // Three suites (conversion-budget, leak-paths, and event-loop-latency via a
  // local `requireData()`) used to carry a verbatim copy of the helper's body,
  // which is why the "14 dataset-gated suites" doctrine count did not match the
  // 11 files that actually called it. They now call the helper. A fourth copy
  // would silently re-open that gap, so it is a RED here.
  //
  // DECLARED GAP, and the reason this describe is titled after its MECHANISM
  // rather than after "is not duplicated" (issue #3772): the check matches the
  // EXACT error-message literal, so a fourth copy carrying a REWORDED message
  // evades it entirely. The property actually enforced is "no other suite
  // inlines this literal", which is strictly weaker than "the assertion is not
  // duplicated" -- and the block used to claim the stronger one. Nothing here
  // detects a semantic duplicate; that would need a shape/AST check, and a title
  // asserting more than its test delivers is the defect #3641 itself was about.
  test('no test file inlines the helper error message', () => {
    // RECURSIVE, matching jest's configured scope (`__test__/**/*.test.js`).
    // A flat readdir scans only this directory, so a nested suite could inline
    // the message and escape the guard entirely (roborev job 71). There are no
    // nested suites today, which is exactly why the scope is pinned now rather
    // than after one appears. `node_modules` is skipped for the same reason
    // jest ignores it.
    const walk = (dir) =>
      fs.readdirSync(dir, { withFileTypes: true }).flatMap((ent) => {
        if (ent.name === 'node_modules') return [];
        const full = path.join(dir, ent.name);
        if (ent.isDirectory()) return walk(full);
        return ent.name.endsWith('.test.js') ? [full] : [];
      });
    const offenders = walk(__dirname)
      // Normalised to forward slashes so the allow-list keys are
      // platform-independent (roborev job 86). path.relative() yields
      // `sub\\file.test.js` on Windows, so a NESTED allow-list entry written
      // `sub/file.test.js` would match on POSIX and silently miss there --
      // turning an exemption into an offender on one platform only. Latent
      // today (the sole key has no separator, and no CI lane runs Windows),
      // but package.json ships x86_64-pc-windows-msvc, and the extension path
      // is exactly what this allow-list exists to invite.
      .map((full) => path.relative(__dirname, full).split(path.sep).join('/'))
      // The exemption is FILE-level, and that is a DECLARED GAP rather than an
      // oversight (roborev jobs 101 and 103). Read this before "tightening" it.
      //
      // job 101 was right that the contract ("permits a file to ASSERT on the
      // message") is stronger than a file-level exemption delivers: an inline
      // copy of the helper's body elsewhere in an allow-listed file is not
      // seen. The obvious fix -- exempt only occurrences that LOOK like
      // assertions -- was implemented, and job 103 then showed it reds on
      // CORRECT input. Both halves of that are MEASURED in this suite, not
      // supposed:
      //   * multiline assertions are real here: `.toThrow(` with its argument
      //     on the NEXT line appears at leak-paths.test.js:766 and in 4 more
      //     places, so a same-line shape test misses them;
      //   * the anticipated exemption case, dataset-guard.test.js, asserts on
      //     child-process output with `toMatch` (107 `toMatch` / 70 `toContain`
      //     / 39 `toThrow` across the suite), so a toThrow-only test would
      //     report the very file this allow-list exists to serve.
      // Widening the matcher set fixes neither the multiline half nor the next
      // shape: it is a recogniser over source text, and those do not close
      // (CLAUDE.md: remove the channel, do not pick a rarer delimiter; and a
      // guard that reds on correct input is the guard agents learn to waive).
      // An expected-occurrence COUNT was rejected too -- a curated number goes
      // red the moment someone adds a legitimate third assertion, the same trap
      // as the hard-coded suite count this issue began with.
      //
      // So the CONTRACT is narrowed to what the mechanism delivers, and the
      // residual is stated: within an allow-listed file this guard does not
      // distinguish an assertion from a copy. That residual is bounded by the
      // allow-list being one deliberate, reasoned entry long -- adding to it is
      // a reviewed act, which is the actual control here. Closing it properly
      // needs a syntax-aware check, which is far beyond this issue's subject.
      .filter((rel) => !MESSAGE_LITERAL_ALLOWED.has(rel))
      // Reported as file:line even though the exemption is file-level, because
      // a location is a better diagnostic than a bare filename.
      .flatMap((rel) => {
        const lines = fs
          .readFileSync(path.join(__dirname, rel), 'utf8')
          .split('\n');
        return lines
          .map((line, i) => ({ line, no: i + 1 }))
          .filter(({ line }) => line.includes(MESSAGE_LITERAL))
          .map(({ no }) => `${rel}:${no}`);
      });
    expect(offenders).toEqual([]);
  });

  // The allow-list is CURATED -- it encodes a human judgement that cannot be
  // derived -- so the one thing that CAN be checked is that it stays TRUTHFUL
  // (issue #3772). Without this, an entry outlives the file it excused, or the
  // file stops quoting the literal, and the list quietly becomes a place where
  // excuses accumulate: a stale curated claim, which is the same decay this
  // issue removed from the gate census.
  test('every allow-list entry is still necessary and still explained', () => {
    for (const [rel, reason] of MESSAGE_LITERAL_ALLOWED) {
      // `rel` is '/'-separated by construction (see the scan above); path.join
      // accepts that on every platform, so no denormalisation is needed here.
      const full = path.join(__dirname, rel);
      expect(fs.existsSync(full)).toBe(true);
      // If an allowed file no longer contains the literal, the entry is dead
      // and must be REMOVED -- keeping it would excuse a future inline copy.
      expect(fs.readFileSync(full, 'utf8')).toContain(MESSAGE_LITERAL);
      expect(reason.trim().length).toBeGreaterThan(0);
    }
  });
});
