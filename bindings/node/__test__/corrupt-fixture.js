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

const KEYSPACE = 'test_basic';
const TABLE = 'simple_table';
const DATA_COMPONENT = 'nb-1-big-Data.db';
const COMPRESSION_COMPONENT = 'nb-1-big-CompressionInfo.db';
const TOC_COMPONENT = 'nb-1-big-TOC.txt';
const MODES = ['truncate', 'bitflip'];

/**
 * Find the real `simple_table-<uuid>` dir under the sstables root, or null.
 * @param {string} sstablesRoot
 * @returns {string|null}
 */
function sourceTableDir(sstablesRoot) {
  const ksDir = path.join(sstablesRoot, KEYSPACE);
  if (!fs.existsSync(ksDir) || !fs.statSync(ksDir).isDirectory()) {
    return null;
  }
  const matches = fs
    .readdirSync(ksDir)
    .filter((name) => name.startsWith(`${TABLE}-`))
    .map((name) => path.join(ksDir, name))
    .filter((dir) => fs.existsSync(path.join(dir, DATA_COMPONENT)))
    .sort();
  return matches.length > 0 ? matches[0] : null;
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

module.exports = { KEYSPACE, TABLE, DATA_COMPONENT, MODES, sourceTableDir, makeCorruptFixture };
