# compaction-parity

Differential compaction-parity harness for CQLite (issue #842).

Runs the **same input SSTables** through two compaction engines and asserts the
outputs are equivalent:

1. **Reference** — Apache Cassandra's compaction, in-JVM via `CQLTester`.
2. **Candidate** — the external `cqlite compact` binary.

See the design doc: `docs/plans/2026-06-18-compaction-parity-harness-design.md`.

## Status

- **Logical tier** (this module): a canonical `sstabledump` of every output must
  match, run with the same tool over both engines' output.
- **Byte tier** (#842 north star): per-component `cmp` — not yet implemented.

## Layout

```
scripts/bootstrap-cassandra.sh   # clone apache/cassandra @ cassandra-5.0.2; ant jar build-test
build.gradle.kts                 # builds against that checkout; JDK 17 test JVM
src/test/java/org/cqlite/parity/
  CqliteCompactionRunner.java    # shells out to `cqlite compact`
  DifferentialParityTester.java  # build inputs → reference + candidate → logical diff
  BasicDifferentialTest.java     # scenario: live rows, last-write-wins
```

## Running

Prerequisites: JDK 11 or 17 (for the Cassandra build), Ant, a `cqlite` binary
built with `--features write-support`, and Gradle.

```bash
# 1. Build the pinned Cassandra source once (cached under ~/.cache/cqlite).
compaction-parity/scripts/bootstrap-cassandra.sh

# 2. Build the cqlite binary under test.
cargo build --features write-support

# 3. Run the harness (uses target/debug/cqlite by default; override with -Dcqlite.bin).
cd compaction-parity && gradle test
```

The Cassandra checkout location can be overridden with `-PcassandraSrc=<dir>` or
`$CQLITE_CASSANDRA_SRC`.

## Known divergences

The harness is wired to catch writer/compaction divergences; each is tracked as a
sub-issue of #842 and the corresponding scenario is `@Ignore`d until fixed.

- **`liveRowsLastWriteWinsAcrossTwoSSTables`** — `@Ignore`d, tracked as **#857**.
  cqlite's compacted
  `Data.db` for a table **with clustering columns** is not Cassandra-readable:
  `sstabledump` decodes the first partition then fails with
  `CorruptSSTableException` / `EOFException` at `Columns$Serializer.deserializeSubset`
  (row-body / column-subset / index-offset encoding). cqlite also reads its own
  such output as 0 rows via the CLI query path. (No-clustering tables round-trip;
  see `cqlite-core/tests/compact_command.rs`.) Un-ignore once the writer is fixed.
