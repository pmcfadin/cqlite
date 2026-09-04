# Smoke Test Golden Snapshots

This directory contains golden snapshot files for the CI smoke test suite.

## Snapshot Files

These files represent the expected output for various one-shot query tests:

- `select_simple_json.golden` - JSON output for basic SELECT query on simple_table
- `select_simple_csv.golden` - CSV output for basic SELECT query on simple_table
- `select_simple_table.golden` - Table output for basic SELECT query on simple_table
- `select_columns_json.golden` - JSON output for column projection query
- `select_collections_json.golden` - JSON output for collections query

## Which side is authoritative (read this before regenerating)

**A mismatch does NOT mean the golden is stale.** These goldens are snapshots of
CQLite's own output, so regenerating one to match the CLI is a no-op check — it
records whatever the CLI currently does, right or wrong. Before you regenerate,
adjudicate against the oracle.

Format authority, in order (CLAUDE.md, "Format authority"):

1. The committed `sstabledump` output for the same table — the `*-Data.db.jsonl`
   files under `test-data/datasets/sstables/<keyspace>/<table>-<id>/`. This is the
   oracle these goldens should agree with.
2. Cassandra 5.0 source at the pinned tag
   (`git show cassandra-5.0.8:src/java/org/apache/cassandra/serializers/...`).
3. `docs/sstables-definitive-guide/`.

A CQLite `file:line` is **never** format authority for CQLite's own behaviour.

Worked example — issue #3689. `select_simple_csv.golden` carried
`1.6699999570846558` for the `FLOAT` column `height` while the CLI emitted
`1.67`. The oracle settled it:

```bash
grep -o '{"name":"height","value":[^,}]*' \
  test-data/datasets/sstables/test_basic/simple_table-*/nb-1-big-Data.db.jsonl | head
# {"name":"height","value":1.84
# {"name":"height","value":1.65
```

`sstabledump` spells a `float` as the shortest f32 round-trip, so the CLI was
right and the golden was the stale side. Had it gone the other way, the golden
would stand and the CLI would be the bug.

## Generating Snapshots

**Only regenerate once you have adjudicated against the oracle above.**

The invocation must match the one CI uses, or you will pin output the lane never
produces. CI runs `test-data/scripts/ci-one-shot-smoke.sh` from
`.github/workflows/ci.yml` (job "CLI tests and smoke") in **data-dir mode**:

```bash
cargo build --package cqlite-cli --release

export CQLITE_CLI="$PWD/target/release/cqlite"
export CQLITE_SCHEMA="$PWD/test-data/schemas/basic-types.cql"
export CQLITE_DATA_DIR="$PWD/test-data/datasets/sstables"
export GOLDEN_DIR="$PWD/test-data/scripts/smoke-test-snapshots"
# Write the .actual files somewhere scratch: the default OUTPUT_DIR is
# test-data/scripts/smoke-test-results/, which holds git-TRACKED files, so the
# default run dirties your worktree.
export OUTPUT_DIR="$(mktemp -d)"

bash test-data/scripts/ci-one-shot-smoke.sh
```

Dataset mode (`CQLITE_DATASET=test_basic` plus `CQLITE_DATASETS_ROOT` from the
line `test-data/scripts/fetch-datasets.sh` prints) produces byte-identical
output and is the usual choice on a fleet box, where the worktree has no
Data.db binaries.

Then inspect each `.actual` and copy the ones you have justified:

```bash
cp "$OUTPUT_DIR/test_select_json_simple.actual"  "$GOLDEN_DIR/select_simple_json.golden"
cp "$OUTPUT_DIR/test_select_csv_simple.actual"   "$GOLDEN_DIR/select_simple_csv.golden"
cp "$OUTPUT_DIR/test_select_table_simple.actual" "$GOLDEN_DIR/select_simple_table.golden"
cp "$OUTPUT_DIR/test_select_columns.actual"      "$GOLDEN_DIR/select_columns_json.golden"
cp "$OUTPUT_DIR/test_select_collections.actual"  "$GOLDEN_DIR/select_collections_json.golden"
```

Record the oracle justification in the commit message, not just "update
snapshots" — #3689 exists because a regeneration commit titled for the JSON work
rewrote the CSV and table goldens with the JSON path's spelling.

## Updating Snapshots

When the CLI output changes intentionally:

1. Adjudicate against the oracle (above). Decide which side is wrong.
2. Regenerate with the CI-matching invocation.
3. Diff each golden and confirm every changed column is one you can explain.
4. Commit with the oracle citation in the message.

**Regenerate every format the change touches.** The CSV and table writers share
`cqlite_core::util::value_fmt::ValueFormatter`; the JSON writer does not. A
change to the shared formatter moves CSV *and* table together, and it is easy to
update one and leave the other stale — which is exactly what happened in #3689.

## Known divergence from the oracle (not yet fixed)

`select_simple_json.golden` deliberately still carries the widened spelling
`1.6699999570846558` for `height`. That is not staleness — it is what the JSON
writer actually emits, because `cqlite-cli/src/output/json.rs` widens an f32 to
f64 (`Number::from_f64(*f as f64)`) before serializing, while CSV and table
render the f32 directly.

So CQLite's JSON egress disagrees with `sstabledump` on every `FLOAT` column.
This golden pins the current behaviour so the divergence stays visible and
counted; do not "align" CSV and table up to it. Fixing it is an egress
behavioural change and is tracked separately — see the #3689 thread and #3644
(AD2 declared divergences). Note the AD2 parity lane
(`cqlite-cli/tests/issue_1491_json_csv_golden_parity.rs`) cannot catch this: none
of its cases covers a table with a `FLOAT` column.

## Snapshot Format Notes

- **JSON snapshots**: Should contain valid JSON (array or object)
- **CSV snapshots**: Should contain comma-separated values with headers
- **Table snapshots**: Should contain ASCII table formatting with borders

## CI Integration

**Where this actually runs:** only in `ci.yml`'s "CLI tests and smoke" job. That
workflow is registry-EXEMPT from the `required` merge gate
(`.github/ci-gating-tiers.yml`: "Broad opt-in lane"), and the local agent gate's
`smoke` component runs a *different* script (`smoke-test-all-tables.sh`), not
this one. So these goldens are gated by neither the merge gate nor the gate of
record — which is how #3689 stayed deterministically red and unfiled for months.
Treat a red here as real even though nothing blocks a merge on it.


The smoke test script:
- Compares actual output against these golden files using `diff -u`
- Fails if any test output doesn't match
- Runs the WHOLE suite and reports every result, then exits non-zero if any
  test failed. It used to abort on the first failure, so a single stale golden
  hid every later test (#3689); `scripts/tests/test_ci_one_shot_smoke_no_abort.sh`
  (gate component `tooling-tests`) pins the run-everything behaviour.
- Allows tests to run without snapshots (only validates exit codes)
- Reports detailed diff output on mismatch

## First Run Without Snapshots

If this is the first run and no `.golden` files exist, the script will:
- Still run all tests
- Validate exit codes
- Generate `.actual` files for manual inspection
- Warn that snapshots are missing
- Pass tests that have correct exit codes (even without snapshots)
