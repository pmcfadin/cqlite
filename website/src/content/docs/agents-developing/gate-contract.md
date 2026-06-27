---
title: Gate Contract
description: scripts/agent-gate.sh is THE gate. What it runs, what "passing" means, and the machine-checkable summary block format.
sidebar:
  label: Gate contract
  order: 1
---

`scripts/agent-gate.sh` is THE gate. A builder claiming "the gate passed" must have
run this script and pasted its summary block verbatim. Ad-hoc `cargo` invocations do
not count. This rule exists because epic #646 shipped three false-green reports from
ambiguity about "which commands count" — specifically, feature-gated tests silently
skipping and partial runs reported as full runs.

## Components

The gate mirrors the enforced CI gates (`.github/workflows/ci.yml`,
`ci-minimal-features.yml`) plus the local smoke suite:

| Component | Command |
|-----------|---------|
| `fmt` | `cargo fmt --all --check` |
| `clippy` | `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features` |
| `core-tests` | `cargo test -p cqlite-core --features cli-helpers` (one test skipped — see script) |
| `integration-tests` | seven named `--test` targets in `cqlite-integration-tests` |
| `write-tests` | `cargo test -p cqlite-core --features write-support` (lib + roundtrip + compaction) |
| `cli-tests` | `cargo test -p cqlite-cli --test unit_tests` |
| `minimal-build` | `cargo build -p cqlite-core --no-default-features --features all-compression` |
| `smoke` | `bash test-data/scripts/smoke-test-all-tables.sh` (against a freshly built debug binary) |

All components run even after a failure so one run reports everything.

## Pre-condition: test data must be present

The gate aborts with exit code 1 if no `*-Data.db` files exist under
`$CQLITE_DATASETS_ROOT/sstables`. Fetch them first:

```bash
bash test-data/scripts/fetch-datasets.sh
```

This prevents the failure mode where dataset-dependent tests silently pass on an
empty dataset by returning 0 rows.

## Running the gate

```bash
# Full gate — the only run that counts
scripts/agent-gate.sh

# Debugging aid only — output marked PARTIAL, never counts
scripts/agent-gate.sh --only fmt,clippy

# List components without running
scripts/agent-gate.sh --list
```

Exit codes: `0` = PASS, `1` = FAIL, `3` = PARTIAL (--only mode).

## Machine-checkable summary block

The gate emits a block between `==== AGENT-GATE SUMMARY ====` markers. The last
line is always `RESULT: PASS` or `RESULT: FAIL`. Paste this block verbatim in your
PR report — prose summaries are not accepted.

**Format (exact, as emitted by `scripts/agent-gate.sh`):**

```
==== AGENT-GATE SUMMARY ====
commit: <short-sha> branch: <branch> dirty: yes|no
datasets: <N> Data.db files under <CQLITE_DATASETS_ROOT>
ci-pins: DATASET_TAG: <tag>  DATASET_ASSET: <asset>  DATASET_SHA256: <sha>  
fmt:               PASS|FAIL (<Ns>)
clippy:            PASS|FAIL (<Ns>)
core-tests:        PASS|FAIL (<Ns>)
integration-tests: PASS|FAIL (<Ns>)
write-tests:       PASS|FAIL (<Ns>)
cli-tests:         PASS|FAIL (<Ns>)
minimal-build:     PASS|FAIL (<Ns>)
smoke:             PASS|FAIL (<Ns>)
logs: /tmp/agent-gate.<random>
RESULT: PASS
==== END AGENT-GATE SUMMARY ====
```

**If `--only` was used** (PARTIAL run — never counts as gate):

```
==== AGENT-GATE SUMMARY ====
commit: <short-sha> branch: <branch> dirty: yes|no
datasets: <N> Data.db files under <CQLITE_DATASETS_ROOT>
ci-pins: ...
mode: PARTIAL (--only fmt,clippy) - does NOT count as the gate
fmt:               PASS (<Ns>)
clippy:            PASS (<Ns>)
logs: /tmp/agent-gate.<random>
RESULT: PARTIAL
==== END AGENT-GATE SUMMARY ====
```

## CI parity

The gate reads dataset pins from `.github/workflows/sstabledump-parity-gate.yml`
and includes them in the summary block as `ci-pins`. Local validation must target
the same asset CI uses. Current pins (as of the script source):

```
DATASET_TAG:    datasets-v3
DATASET_ASSET:  cassandra5-small-full-v3.2.tar.gz
DATASET_SHA256: bebc763752c8d68c7fb0483a1b31294b4d1d21343d3f7d124da069e5073202fa
```

See [Test data](/cqlite/agents-developing/test-data/) for how `fetch-datasets.sh` uses these pins and why
the SHA256 is the cache key.

## Feature-gated tests

`core-tests` skips one test (`test_legacy_format_allows_blob_fallback_with_feature`)
that requires a feature flag incompatible with `cli-helpers`. This skip is listed in
the script explicitly — it is not a silent omission.

The `minimal-build` component verifies the library compiles without the query engine
(`--no-default-features --features all-compression`). This catches feature-gate
regressions that `clippy --all-features` won't find.
