# compaction-parity

Differential compaction-parity harness for CQLite (issue #842).

Runs the **same input SSTables** through two compaction engines and asserts the
outputs are equivalent:

1. **Reference** — Apache Cassandra's compaction, in-JVM via `CQLTester`.
2. **Candidate** — the external `cqlite compact` binary.

See the design doc: `docs/plans/2026-06-18-compaction-parity-harness-design.md`.

## Status

- **Logical tier** (hard gate): a canonical `sstabledump` of every output must
  match, run with the same tool over both engines' output. Asserted by
  `gradle test`.
- **Byte tier** (#842 north star, issue #1016): per-component byte-for-byte `cmp`
  with **no allowlist** over every output component (`Data.db`, `Index.db`,
  `Statistics.db`, `Summary.db`, `Filter.db`, `CompressionInfo.db`,
  `Digest.crc32`, `TOC.txt` as present), reporting the first differing byte/offset
  per component. Run with `gradle byteParity` (or `gradle test -Dparity.tier=byte`).
  Non-blocking (continue-on-error) until the writer is byte-stable. This is the
  `nightly_docker` tier: CI runs the byteParity assertion only on
  `workflow_dispatch`, not per-PR (the per-PR `gradle test` run already persists
  the byte diff + checksums as artifacts, so PRs lose no evidence and we avoid
  doubling the expensive Cassandra compaction).

Both tiers run every scenario. `gradle test` always *computes and preserves* the
byte diff as an artifact but only *asserts* the logical tier; `gradle byteParity`
additionally asserts byte equality.

## Artifacts (every run, success AND failure)

Each scenario writes a forensic bundle under
`build/parity-artifacts-<task>/<Class>.<method>/`:

```
inputs/              # the input SSTables fed to BOTH engines
cassandra-output/    # Cassandra reference output components
cqlite-output/       # cqlite candidate output components
schema.cql           # the standalone DDL handed to cqlite
commands.txt         # exact command lines (cqlite compact + sstabledump)
cqlite-compact.stdout / cqlite-compact.stderr
reference.jsonl / candidate.jsonl   # normalized sstabledump output
checksums.txt        # SHA-256 per component, both engines
byte-diff.txt        # first byte/offset diff per component (the byte tier report)
```

The CI workflow uploads these as the `compaction-parity-reports` artifact.

## Fail-loud policy

The harness never silently skips. A missing `cqlite` binary, missing
`sstabledump`, missing Cassandra checkout, missing input set, missing output
files, or unexpected multiple outputs all throw / fail the build (no
`Assume.assumeTrue`).

## Layout

```
scripts/bootstrap-cassandra.sh   # clone apache/cassandra @ cassandra-5.0.2; ant jar build-test
build.gradle.kts                 # builds against that checkout; JDK 17 test JVM; test + byteParity tasks
src/test/java/org/cqlite/parity/
  CqliteCompactionRunner.java    # shells out to `cqlite compact`
  DifferentialParityTester.java  # build inputs → reference + candidate → logical + byte diff
  ComponentByteComparator.java   # per-component byte-for-byte cmp (no allowlist)
  ParityArtifacts.java           # preserve inputs/outputs/diffs/checksums per scenario
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
cd compaction-parity && gradle test          # logical tier (hard gate)
cd compaction-parity && gradle byteParity    # logical + byte tier (no allowlist)
```

The Cassandra checkout location can be overridden with `-PcassandraSrc=<dir>` or
`$CQLITE_CASSANDRA_SRC`.

## Scenarios

Both current scenarios PASS — the harness confirms cqlite's compaction matches
Apache Cassandra's for these cases:

- `liveRowsLastWriteWinsNoClustering` — partition-key-only table, LWW overlap.
- `liveRowsLastWriteWinsAcrossTwoSSTables` — clustering table, LWW overlap.

## Resolved divergences

The harness is wired to catch writer/compaction divergences; each is tracked as a
sub-issue of #842 and its scenario is `@Ignore`d only while open.

- **#857 (fixed)** — cqlite's compacted `Data.db` for a table **with clustering
  columns** was not Cassandra-readable: `sstabledump` decoded the first partition
  then failed at `Columns$Serializer.deserializeSubset`. Root cause: the merge left
  clustering columns inside the row's cells, so the writer emitted each clustering
  value a second time as a phantom regular cell. Fixed by dropping primary-key
  columns from a merged row's cell ops in `merge_row_group` (the clustering value is
  still written positionally in the clustering prefix). Regression tests:
  `data_writer::tests::merge_row_group_excludes_*` and
  `cqlite-core/tests/compact_command.rs::compact_clustering_table_preserves_rows_and_lww`,
  plus this harness's `liveRowsLastWriteWinsAcrossTwoSSTables`.
