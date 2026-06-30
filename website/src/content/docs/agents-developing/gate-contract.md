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
| `tooling-tests` | `bash scripts/tests/test_agent_gate_summary.sh` (SUMMARY-capture regression, #1175; SKIP-aware on missing python3) |
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

## Capturing the gate robustly (issue #1175)

The SUMMARY block is the only artifact that counts, so it must survive however
you capture the run. Use the foreground redirect — it writes each line straight
to a file descriptor and never buffers:

```bash
bash scripts/agent-gate.sh > gate.log 2>&1 < /dev/null
```

Under **non-foreground** capture (a `script`/pty, a buffering wrapper, a
"drain-until-EOF then write" reader, or a backgrounded pipeline) the streamed
SUMMARY block can be lost entirely: a gate component sometimes leaks a descendant
(a `cargo`/`rustc` build server, a daemonizing test, etc.) that keeps the gate's
stdout pipe open, so an until-EOF reader never sees EOF, gets killed by a
timeout, and discards its in-memory buffer — even though the gate exited 0.
(Detaching the gate's *own* stdout cannot fix this: the leaked child still holds
its inherited copy of the pipe write-end.)

The recovery contract does not depend on the stream at all — pick the path in
advance and read it:

- **Set `AGENT_GATE_SUMMARY_FILE=/path` before running.** The gate writes the
  complete SUMMARY to that exact path with plain redirection, so the file is
  complete no matter what happens to stdout. `cat` it afterward; it always
  contains the full block (start marker → `RESULT:` → end marker).

  ```bash
  AGENT_GATE_SUMMARY_FILE=/tmp/gate-summary.txt \
    bash scripts/agent-gate.sh > gate.log 2>&1 < /dev/null
  cat /tmp/gate-summary.txt   # complete SUMMARY, even if gate.log truncated
  ```

- **If you don't set it,** the gate writes the same complete block to the
  documented default `$PWD/.agent-gate-summary.txt` (gitignored). If your streamed
  capture looks truncated (missing the `==== END AGENT-GATE SUMMARY ====`
  marker), `cat` that file — it is always complete.

> **Concurrency caveat (#1175):** the default `$PWD/.agent-gate-summary.txt` is
> per-*checkout*, not per-run. If you run multiple gates concurrently **in the same
> checkout**, each MUST set a unique `AGENT_GATE_SUMMARY_FILE` or they will clobber
> each other's recovery artifact. Separate worktrees get distinct repo roots and so
> distinct default paths — already isolated, which is CQLite's normal model. The
> `run-id:` line lets a caller that captured the invocation's run-id confirm it is
> reading the right run; a caller with no expected run-id and a fully-lost stream
> cannot disambiguate two same-checkout runs, so it must use a unique path.

The path the gate used is also echoed on the `summary-file:` line inside the
block, and a copy is kept in the `logs:` bundle. The streamed copy is best-effort
only.

A fast regression test for this emission path lives at
`scripts/tests/test_agent_gate_summary.sh` (run it directly:
`bash scripts/tests/test_agent_gate_summary.sh`). It exercises
`scripts/agent-gate.sh --emit-summary-selftest`, which prints a representative
SUMMARY block through the real emission code without running the 5–8 minute gate.
The gate runs this test automatically as the `tooling-tests` component, so the
capture guarantee is enforced on every gate run.

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
summary-file: <AGENT_GATE_SUMMARY_FILE or $PWD/.agent-gate-summary.txt>
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
summary-file: <AGENT_GATE_SUMMARY_FILE or $PWD/.agent-gate-summary.txt>
RESULT: PARTIAL
==== END AGENT-GATE SUMMARY ====
```

## Parity CI tier contracts

The agent gate proves a change is *correct*; the **parity CI tiers** define what
each Cassandra-parity gate *promises*. The two are read together: see
`docs/development/parity-ci-tiers.md` for the per-tier contract (purpose, accepted
`evidence.type`, skip/failure policy, artifact retention, promotion rules) and the
gate-strength classification — **smoke** vs **canonical-semantic** vs
**byte-for-byte** — that bounds what a green gate can claim. Smoke alone cannot
satisfy a P0 data-loss scenario without a recorded gap. Before publishing a broad
public parity claim, run `docs/development/parity-release-checklist.md`. A
fast-PR cross-check (`cargo run -p cassandra-parity -- tier-contract-check`) keeps
the documented tier enum in sync with the code (`enums::CI_TIER`) and the manifest
schema (issue #1022).

## CI parity

The gate reads dataset pins from `.github/workflows/sstabledump-parity-gate.yml`
and includes them in the summary block as `ci-pins`. Local validation must target
the same asset CI uses. Current pins (as of the script source):

```
DATASET_TAG:    datasets-v3
DATASET_ASSET:  cassandra5-small-full-v3.4.tar.gz
DATASET_SHA256: 3cae644360e0142a6bb5e96ddab445ff18e3478e7058104842ce1a455fba8a33
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
