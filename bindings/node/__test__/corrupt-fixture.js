/**
 * Runtime corrupt-SSTable fixture generation for the abort-safety harness.
 *
 * Issue #1437. No large binaries are committed. Given a temp dir this copies
 * the real `test_basic.simple_table` SSTable directory and mutates
 * `nb-1-big-Data.db` in one of two modes, both exercised by the harness:
 *   - "truncate": copy the dir, then truncate Data.db to ~50% of its length.
 *   - "bitflip":  copy the whole dir, then XOR 0x01 into a middle byte.
 *
 * Sibling components (-Statistics.db / -Index.db / -Summary.db /
 * -CompressionInfo.db / -TOC.txt) are kept so Database.open() proceeds far
 * enough to read the corrupt Data.db during a scan.
 *
 * Dataset rule (issue #1437): if the real source Data.db is missing or
 * zero-length, THROW (never silently `return`); a broken source is a hard
 * failure, not a skippable condition.
 */
'use strict';

const fs = require('fs');
const path = require('path');
// The canonical table-directory predicates, shared with the golden lookups so all three
// consumers and the manifest agree on what a table directory IS (#3493).
const { isTableDirFor, isCommittedTableDir } = require('./parity-utils.js');

const KEYSPACE = 'test_basic';
const TABLE = 'simple_table';
const DATA_COMPONENT = 'nb-1-big-Data.db';
const COMPRESSION_COMPONENT = 'nb-1-big-CompressionInfo.db';
const TOC_COMPONENT = 'nb-1-big-TOC.txt';
const MODES = ['truncate', 'bitflip'];

/**
 * Classify the source table directory: `ok` | `broken` | `absent`.
 *
 * THE THREE-VALUED ANSWER IS THE POINT (roborev #3493 round 52). `sourceTableDir` returns a
 * directory or `null`, and round 48 added a nonempty-regular-file filter to it -- which
 * silently converted "present but UNUSABLE" into "ABSENT". `abort-safety.test.js` keys its
 * gating on exactly that distinction: `broken` is a HARD FAILURE, while a non-strict
 * `absent` is a real `test.skip`. So a truncated fixture stopped hard-failing and started
 * SKIPPING, inverting #1437's stated design ("a broken source is a hard failure, not a
 * skippable condition") and making that test's `broken` branch unreachable.
 *
 * `broken` means a Data.db ENTRY EXISTS in a canonical candidate but is not a nonempty
 * regular file -- zero-length, a directory, a dangling symlink. A canonical directory with
 * no Data.db entry at all stays `absent`, which is the pre-round-48 behaviour and the
 * honest reading: the fixture was never fetched, as opposed to fetched and damaged.
 *
 * A VALID candidate still WINS over a broken sibling. That is deliberately laxer than
 * `check-dataset-manifest.sh`, which disqualifies a table when ANY candidate is unusable --
 * and the two are right for different reasons: Jest's discovery picks a directory blind, so
 * the manifest must assume the worst, while this function picks deterministically and can
 * simply choose the good one.
 *
 * @param {string} sstablesRoot
 * @returns {{status: 'ok', dir: string} | {status: 'broken', dir: string, reason: string}
 *           | {status: 'absent', reason: string}}
 */
function classifyTableDir(sstablesRoot) {
  const ksDir = path.join(sstablesRoot, KEYSPACE);
  if (!fs.existsSync(ksDir) || !fs.statSync(ksDir).isDirectory()) {
    return { status: 'absent', reason: `No ${KEYSPACE} keyspace directory under ${sstablesRoot} (issue #1437)` };
  }
  const candidates = fs
    .readdirSync(ksDir, { withFileTypes: true })
    // A REAL directory. `Dirent.isDirectory()` is FALSE for a symlink, which is exactly
    // what Jest's discovery and the manifest's `[ -L "$cand" ] && continue` both do, so a
    // symlinked table dir is invisible to them and must be invisible here too.
    //
    // Not merely a consistency point -- it is what stops this harness DESTROYING THE
    // SOURCE CORPUS (round 49). `fs.cpSync` preserves a symlink by default, so a symlinked
    // table dir was copied AS A SYMLINK and the truncate/bitflip below then wrote THROUGH
    // it into the real fixture: truncating the "copy" cut the original from 12 bytes to 4.
    // On a shared machine-local corpus that is damage to every other lane on the box.
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .filter((name) => isTableDirFor(name, TABLE) && isCommittedTableDir(KEYSPACE, name))
    .map((name) => path.join(ksDir, name))
    .sort();

  const usable = candidates.filter((dir) => isNonemptyFile(path.join(dir, DATA_COMPONENT)));
  if (usable.length > 0) {
    return { status: 'ok', dir: usable[0] };
  }
  const damaged = candidates.find((dir) => entryExists(path.join(dir, DATA_COMPONENT)));
  if (damaged !== undefined) {
    return {
      status: 'broken',
      dir: damaged,
      reason:
        `Source ${path.join(damaged, DATA_COMPONENT)} is present but not a nonempty ` +
        `regular file (zero-length, a directory, or a dangling symlink) (issue #1437)`,
    };
  }
  return {
    status: 'absent',
    reason: `No ${KEYSPACE}.${TABLE} SSTable with ${DATA_COMPONENT} under ${sstablesRoot} (issue #1437)`,
  };
}

/**
 * The usable source table dir, or null. Thin wrapper over `classifyTableDir` -- callers
 * that need to tell `broken` from `absent` must use the classifier (see its header).
 *
 * @param {string} sstablesRoot
 * @returns {string|null}
 */
function sourceTableDir(sstablesRoot) {
  const c = classifyTableDir(sstablesRoot);
  return c.status === 'ok' ? c.dir : null;
}

/**
 * True iff a directory ENTRY exists at `p`, whatever its type.
 *
 * `lstatSync`, not `existsSync`: the latter follows symlinks and so answers FALSE for a
 * DANGLING one -- which would classify a dangling-symlink Data.db as `absent` when it is
 * exactly the damaged-fixture case `broken` exists to report.
 *
 * @param {string} p
 * @returns {boolean}
 */
function entryExists(p) {
  try {
    fs.lstatSync(p);
    return true;
  } catch (_e) {
    return false;
  }
}

/**
 * True iff `p` is a NONEMPTY REGULAR FILE (following symlinks).
 *
 * `fs.existsSync` is true for a directory and false for a dangling symlink, and says
 * nothing about size -- and a zero-length Data.db is exactly what a truncated fetch
 * leaves, which this harness would then "corrupt" and assert against.
 *
 * @param {string} p
 * @returns {boolean}
 */
function isNonemptyFile(p) {
  try {
    const st = fs.statSync(p);
    return st.isFile() && st.size > 0;
  } catch (_e) {
    return false;
  }
}

/**
 * Force the reader down the uncompressed Data.db path by dropping the
 * CompressionInfo sidecar (and its TOC line). The raw VInt/row parser is where
 * the audited corrupt-input panics live; with compression present, Snappy
 * decompression contains any corruption before the panics are reached. A
 * missing CompressionInfo.db is itself a plausible real-world corruption.
 * @param {string} destTable
 */
function dropCompressionInfo(destTable) {
  const comp = path.join(destTable, COMPRESSION_COMPONENT);
  if (fs.existsSync(comp)) {
    fs.rmSync(comp);
  }
  const toc = path.join(destTable, TOC_COMPONENT);
  if (fs.existsSync(toc)) {
    const kept = fs
      .readFileSync(toc, 'utf8')
      .split('\n')
      .filter((line) => !line.includes('CompressionInfo'));
    fs.writeFileSync(toc, kept.join('\n') + '\n');
  }
}

/**
 * Build a corrupt copy under `destParent` and return the `sstables/` root to
 * pass to Database.open. Throws when the source Data.db is missing/empty.
 * @param {string} destParent
 * @param {string} sstablesRoot
 * @param {'truncate'|'bitflip'} mode
 * @param {{exposeUncompressed?: boolean}} [opts] - when exposeUncompressed is
 *   true, also drop CompressionInfo.db to read Data.db on the raw parse path.
 * @returns {string}
 */
function makeCorruptFixture(destParent, sstablesRoot, mode, opts = {}) {
  if (!MODES.includes(mode)) {
    throw new Error(`unknown mode ${mode}; expected one of ${MODES.join(', ')}`);
  }
  const src = sourceTableDir(sstablesRoot);
  if (src === null) {
    throw new Error(
      `No ${KEYSPACE}.${TABLE} SSTable with ${DATA_COMPONENT} under ${sstablesRoot}`
    );
  }
  const srcData = path.join(src, DATA_COMPONENT);
  if (fs.statSync(srcData).size === 0) {
    throw new Error(`Source ${srcData} present but empty (issue #1437)`);
  }

  const destRoot = path.join(destParent, 'sstables');
  const destTable = path.join(destRoot, KEYSPACE, path.basename(src));
  fs.cpSync(src, destTable, {
    recursive: true,
    // COPY WHAT A SYMLINK POINTS AT, never the link (#3493 round 49). Defence in depth
    // behind sourceTableDir's symlink rejection: that guard covers the TABLE DIRECTORY,
    // while a symlinked COMPONENT inside a real directory would still be copied as a link
    // and the mutation below would write through it into the source corpus.
    dereference: true,
    // Skip only the multi-megabyte JSONL golden; the small -TOC.txt MUST be
    // kept (issue #1437). The reader consults only TOC-listed binary files.
    filter: (s) => !s.endsWith('.jsonl'),
  });

  if (opts.exposeUncompressed) {
    dropCompressionInfo(destTable);
  }

  const destData = path.join(destTable, DATA_COMPONENT);
  const length = fs.statSync(destData).size;
  if (mode === 'truncate') {
    fs.truncateSync(destData, Math.floor(length / 2));
  } else {
    const offset = Math.floor(length / 2);
    const fd = fs.openSync(destData, 'r+');
    try {
      const buf = Buffer.alloc(1);
      fs.readSync(fd, buf, 0, 1, offset);
      buf[0] ^= 0x01;
      fs.writeSync(fd, buf, 0, 1, offset);
    } finally {
      fs.closeSync(fd);
    }
  }
  return destRoot;
}

module.exports = {
  KEYSPACE,
  TABLE,
  DATA_COMPONENT,
  MODES,
  classifyTableDir,
  sourceTableDir,
  makeCorruptFixture,
};
