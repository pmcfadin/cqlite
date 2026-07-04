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

**CI-enforced as a nightly deep-check (issue #1269, reconciled with epic #1360).** The gate is no longer
local-only: `.github/workflows/gate.yml` runs the *full* `scripts/agent-gate.sh` (never `--only`) in CI
as a **nightly, path-independent deep-check backstop** (`schedule:` cron + `workflow_dispatch` for
on-demand runs). It is **NOT** a required per-PR check — under epic #1360's tiered model the ONE required,
always-running PR check is the light `.github/workflows/pr-gate.yml` (fmt + cqlite-core clippy
`-D warnings` + all-feature build + fast tests; no Docker/datasets/agent-gate). The nightly `gate.yml`
lane fetches the pinned datasets and sets `CQLITE_DATASETS_ROOT` so the dataset-dependent components
execute rather than skip, and uploads the SUMMARY block as an artifact. So a change that breaks a gate
component (e.g. `node-bindings`) that the light PR check cannot see is still caught within 24h and
surfaces on the Actions dashboard.

## Components

The gate mirrors the enforced CI gates (`.github/workflows/ci.yml`,
`ci-minimal-features.yml`) plus the local smoke suite:

| Component | Command |
|-----------|---------|
| `fmt` | `cargo fmt --all --check` |
| `clippy` | `RUSTFLAGS="-D warnings"` clippy, **scoped per-package** (issue #1844 — see below) |
| `core-tests` | `cargo test -p cqlite-core --features cli-helpers` (one test skipped — see script) |
| `integration-tests` | seven named `--test` targets in `cqlite-integration-tests` |
| `write-tests` | `cargo test -p cqlite-core --features write-support` (lib + roundtrip + compaction) |
| `cli-tests` | `cargo test -p cqlite-cli --test unit_tests` |
| `tooling-tests` | `bash scripts/tests/test_agent_gate_summary.sh` (SUMMARY-capture regression, #1175; SKIP-aware on missing python3) |
| `minimal-build` | `cargo build -p cqlite-core --no-default-features --features all-compression` |
| `smoke` | `bash test-data/scripts/smoke-test-all-tables.sh` (against a freshly built debug binary) |

All components run even after a failure so one run reports everything.

### Scoped clippy (issue #1844)

`--workspace --all-features` enables *every* feature on *every* package, which pulls
in two costly artifacts on **every** gate run in **every** worktree:

- the **source-built DuckDB C++ amalgamation** (cqlite-cli `duckdb-tests` feature), and
- the full **OpenTelemetry/OTLP** stack — both the tonic and reqwest transports
  (`observability`/`observability-testing` on core/cli/flight/bindings).

Neither is reusable by any other gate component (`-D warnings` gives clippy a distinct
compile fingerprint), so they were pure per-gate tax. The `clippy` component therefore
runs a **scoped per-package** lint that still covers the whole workspace with
`-D warnings` but excludes only those two feature families. **parquet/arrow are NOT
excluded** — they are reachable in normal builds (the
cli-helpers→state_machine→`cqlite-core/parquet` chain) and stay linted. Both the full
gate and `--lite` use the same scoping. See `run_clippy()` in `scripts/agent-gate.sh`.

Coverage of the excluded features is **moved, not deleted**: set `CQLITE_CLIPPY_FULL=1`
to run the historical `cargo clippy --workspace --all-targets --all-features -D warnings`
matrix. `.github/workflows/gate.yml` (the nightly deep-check) sets it, so a lint that
only fires behind `duckdb-tests` or `observability*` is still caught within 24h. The
per-package feature lists in `run_clippy()` can drift as features are added; that
nightly full pass is the drift backstop.

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

## Tiered gate: `--lite` iterate, full gate once (issue #1821)

The gate is tiered. `scripts/agent-gate.sh --lite` runs only the fast subset
(file-size + fmt + scoped workspace clippy + blast-radius-scoped tests, ~1–5 min).
It is the **fast iteration loop, NOT the gate of record** — it emits a DISTINCT
`==== AGENT-GATE LITE SUMMARY ====` block (`MODE: lite`) that must **never** be
pasted as the full SUMMARY. Iterate on `--lite` every fix round; run the FULL
`scripts/agent-gate.sh` **exactly once** before merge. `--lite` never replaces the
full gate.

**Division of labor (issue #1855).** In the worker → subagent model, an
implementer subagent (`sstable-developer`) edits, commits, pushes, and verifies
with `--lite`/targeted tests **only** — it must **never** invoke the full gate. The
worker/orchestrator runs the FULL gate itself, exactly once. A subagent idle-waiting
on a 12–20 min full gate gets killed by the stall watchdog and takes its child gate
process down with it.

## New-machine setup

A fresh machine that will run the gate should first run
`bash scripts/bootstrap-agent-machine.sh` (see
`docs/development/agent-machine-setup.md`): it installs/verifies the accelerators
below, the datasets, `gh` auth + the `project` scope, and roborev's local config,
then prints the gate's `accelerators:` line as a health check.

## Accelerators are LOUD when missing (issue #1848)

Every optional accelerator the gate depends on is auto-detected, and every SUMMARY
block (full **and** `--lite`) carries a machine-checkable line:

```
accelerators: sccache=on nextest=on lanes=on
```

- **`sccache`** — cross-worktree compile cache (~25.6% faster fresh builds).
- **`nextest`** — parallel `core-tests` (the gate's long pole).
- **`lanes`** — parallel gate components (needs bash ≥4.3 for `wait -n`).

State values: **`on`** (detected & used) · **`absent`** (missing → the gate prints a
loud `WARN:` on STDERR with the one-line install command) · **`off`** (intentionally
disabled via `CQLITE_DISABLE_SCCACHE` / `CQLITE_DISABLE_NEXTEST` / `AGENT_GATE_JOBS=1`;
**no warn**) · **`lanes=serial`** (degraded by bash <4.3). An intentional opt-out is
`off`, never `absent`. This exists because a machine silently ran ~3x slower for weeks
with sccache and nextest both un-installed and no signal. If a pasted SUMMARY shows
`absent`, install the tool — the state is visible in the block, not just scrollback.

## Machine-wide concurrency cap (issue #1825)

Running many sessions/worktrees at once used to let ~15 full gates hit the CPU
simultaneously (load 30–60), which SIGKILLed gates mid-`core-tests`. The full gate
now takes a **cross-process bounded semaphore**: at most **N** full
`agent-gate.sh` runs execute machine-wide at once. Excess invocations **queue**
(block) for a slot and print one line — `waiting for gate slot (N in use)…` — then
proceed when a slot frees. **They never fail from the cap**, and a non-interactive
caller blocks cleanly rather than spin-failing.

- **`--lite` and `--only` runs are EXEMPT** — never queued. `--lite` is cheap by
  design; `--only` is a PARTIAL run (and is used by nested tooling self-tests, so
  capping it could self-deadlock the queue).
- **N** defaults to `max(2, floor((ncpu-2)/4))` — a conservative fraction of cores
  that still lets a couple of gates run on a small box. Override with
  `CQLITE_GATE_MAX_CONCURRENCY`.
- **SIGKILL-safe stale-slot reaping:** each slot is an `fcntl.flock` held by a
  small background daemon (`scripts/lib/gate_slot_daemon.py`) that the gate starts
  and monitors. Because the daemon opens the lock fd *after* it is forked, the
  gate's heavy children (`cargo`/`nextest`) never inherit the lock — a SIGKILLed
  gate frees its slot within one poll interval even while orphaned children run on.
  A crashed gate can never permanently leak a slot.
- Works **across worktrees** (shared slot dir, not per-checkout) and composes with
  the per-gate component parallelism (`AGENT_GATE_JOBS`) and `sccache`: the cap
  bounds the *worst case* (several sessions hitting their one full gate at once),
  the others cut average load and per-compile time.

**Environment knobs** (all optional):

```bash
CQLITE_GATE_MAX_CONCURRENCY=4 bash scripts/agent-gate.sh   # raise N on a big box
CQLITE_GATE_SLOTS_DIR=/path bash scripts/agent-gate.sh     # slot dir (default $TMPDIR/cqlite-gate-slots)
CQLITE_GATE_POLL_SECS=1 bash scripts/agent-gate.sh         # queue/liveness poll (default 2s)
CQLITE_GATE_DISABLE_CAP=1 bash scripts/agent-gate.sh       # force-disable the cap
```

The cap fails **open** (disabled, with a loud stderr note) when `python3` or the
daemon is unavailable — the gate must never be un-runnable because of the cap. A
hermetic self-test proving queueing at N, `--lite` exemption, and SIGKILL slot
release lives at `scripts/tests/test_gate_concurrency_cap.sh` and runs inside the
`tooling-tests` component.

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
