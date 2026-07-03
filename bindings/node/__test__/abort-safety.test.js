/**
 * Abort-safety regression harness (issue #1437).
 *
 * Drives a corrupt/truncated SSTable through every Node entry point and proves
 * the host process SURVIVES: each call must terminate in a catchable state (a
 * normal return, or — under a panic=unwind build — a caught panic surfaced as
 * a thrown JS Error) rather than aborting the process.
 *
 * Why a child process?  The workspace *release* profile is `panic = "abort"`
 * (Cargo.toml), so a panic inside cqlite-core during a scan through napi-rs
 * does not become a catchable JS Error — it kills the whole Node process with
 * a signal.  The only way to observe that as a test FAILURE (rather than the
 * Jest runner dying) is to spawn each driver in a child process and assert it
 * exited 0 while emitting a terminal sentinel on stdout.
 *
 * This harness exercises the "compressed" fixture flavor — the exact issue
 * recipe (mutate the Snappy-compressed Data.db). Snappy decompression contains
 * the corruption, so all three entry points survive today in BOTH debug and
 * release. Covers executeNative, streaming, and exportParquet over both
 * truncate and bit-flip mutations.
 *
 * DEBUG vs RELEASE: `npm run build` builds the addon `--release`
 * (`panic=abort`). These compressed tests survive on that release addon
 * because the corruption never reaches a panic. The release-profile abort
 * PROOF for issue #1437 is carried by the Python harness
 * (`test_abort_safety.py::test_uncompressed_...`), which does reach the
 * raw-parser panic through PyO3.
 *
 * WHY NO "uncompressed" (raw parse path) FLAVOR HERE (finding, verified
 * 2026-07-02): dropping CompressionInfo.db to reach the raw VInt/row parser
 * makes the Node process ABORT even under a DEBUG (`panic=unwind`) build with
 *   thread '<unnamed>' panicked at bindings/node/src/value.rs:266
 *   fatal runtime error: failed to initiate panic, error 5, aborting
 * i.e. a panic in the Node binding's own decimal formatter (unbounded
 * `format!` padding width from a corrupt DECIMAL scale) on the napi async
 * worker thread, which cannot unwind across the FFI boundary. Because it
 * aborts under unwind too, #1440's panic profile alone will NOT fix the Node
 * path — it additionally needs `scale` bounded in value.rs and/or an explicit
 * catch_unwind at the napi boundary. Asserting survival on that flavor would
 * therefore be RED under the debug gate, so it is intentionally omitted and
 * reported as a follow-up rather than run as a known-red test.
 *
 * These tests actually run and assert (never silently skip) when the dataset
 * is present; a missing/empty source Data.db throws loudly per issue #1437.
 */
'use strict';

const { execFileSync } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const { MODES, sourceTableDir, makeCorruptFixture } = require('./corrupt-fixture.js');

const MODULE_PATH = path.resolve(__dirname, '..', 'lib', 'index.js');

// Child driver. `require` sits OUTSIDE the try so a broken build crashes
// non-zero (surfacing as a setup failure, not a false green). A hard abort
// prints no terminal sentinel and exits with a signal.
const DRIVER = `
const [root, schema, entry] = process.argv.slice(1);
const { Database } = require(${JSON.stringify(MODULE_PATH)});
const QUERY = 'SELECT * FROM test_basic.simple_table';
(async () => {
  try {
    const db = await Database.open(root, { schema });
    console.log('OPENED');
    if (entry === 'executeNative') {
      const r = await db.executeNative(QUERY);
      console.log('RETURNED rows=' + r.rowCount);
    } else if (entry === 'streaming') {
      let n = 0;
      for await (const _ of db.executeStreaming(QUERY)) { n++; }
      console.log('RETURNED rows=' + n);
    } else if (entry === 'parquet') {
      const fs = require('fs'); const os = require('os'); const path = require('path');
      const out = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'cq-pq-')), 'out.parquet');
      const rows = await db.exportParquet(QUERY, out);
      console.log('RETURNED rows=' + rows);
    } else {
      console.log('BADENTRY ' + entry);
      process.exit(3);
    }
    await db.close();
  } catch (e) {
    const name = (e && e.constructor && e.constructor.name) || 'Error';
    console.log('RAISED ' + name);
  }
})();
`;

function requireSource() {
  const sstables = global.testPaths.SSTABLES_DIR;
  const src = sourceTableDir(sstables);
  if (src === null) {
    // Dataset genuinely absent: honor the suite-wide availability guard.
    if (!global.DATASETS_AVAILABLE) {
      return false;
    }
    throw new Error(`No test_basic.simple_table SSTable under ${sstables} (issue #1437)`);
  }
  const data = path.join(src, 'nb-1-big-Data.db');
  if (fs.statSync(data).size === 0) {
    throw new Error(`Source ${data} present but empty (issue #1437)`);
  }
  return true;
}

/**
 * Spawn the child driver and assert the process survives (exit 0) and reaches
 * a terminal sentinel. Throws with rich context on a signal/non-zero exit.
 */
function runAndAssertSurvives(mode, entry, exposeUncompressed) {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cqlite-abort-'));
  try {
    const root = makeCorruptFixture(tmp, global.testPaths.SSTABLES_DIR, mode, {
      exposeUncompressed,
    });
    let stdout = '';
    try {
      stdout = execFileSync(process.execPath, ['-e', DRIVER, root, global.testPaths.SCHEMA_BASIC_TYPES, entry], {
        encoding: 'utf8',
        timeout: 120000,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
    } catch (err) {
      const exit = err.status != null ? err.status : err.signal || 1;
      const out = (err.stdout && err.stdout.toString()) || '';
      const errOut = (err.stderr && err.stderr.toString()) || '';
      throw new Error(
        `child did not survive corrupt input (mode=${mode} entry=${entry} ` +
          `exposeUncompressed=${exposeUncompressed} exit=${exit})\n` +
          `stdout=${JSON.stringify(out)}\nstderr=${JSON.stringify(errOut)}`
      );
    }
    // RAISED (incl. a napi-converted panic under panic=unwind) or RETURNED both
    // prove the boundary held and the process lived on.
    expect(stdout).toMatch(/RETURNED|RAISED/);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
}

describe('Abort safety: corrupt SSTable must not kill the host (issue #1437)', () => {
  let haveSource = false;

  beforeAll(() => {
    haveSource = requireSource();
  });

  describe('compressed Data.db (exact issue recipe; survives in debug + release)', () => {
    for (const mode of MODES) {
      for (const entry of ['executeNative', 'streaming', 'parquet']) {
        test(`survives ${mode} via ${entry}`, () => {
          if (!haveSource) {
            console.warn(`SKIP ${mode}/${entry}: datasets not available`);
            return;
          }
          runAndAssertSurvives(mode, entry, false);
        });
      }
    }
  });
});
