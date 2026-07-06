# Gate Operations Deep Reference

**Audience**: the delivery lead and gate operators tuning `scripts/agent-gate.sh`
on a given machine — sccache setup, accelerator-degradation internals, disk
hygiene, gate parallelism knobs, the machine-wide concurrency cap, and the deep
`--delta` re-certification mechanics. Every implementing agent only needs the
canonical invocations and tier rules that stay in `CLAUDE.md` (**Essential
Commands** / **Agent-team conventions**); this page is the deep reference those
sections point to. Nothing here is new doctrine — it is the operator-level
prose moved out of `CLAUDE.md` (issue #2082) so it is not baseline context on
every one of the seven specialist subagent spawns.

## Shared Compiler Cache (sccache)

The gate uses **sccache** (Mozilla's shared compiler cache) to eliminate duplicated compilation across worktrees. Each worktree is **independent** (owns its `target/` dir, no lock contention), but reuses cached compilation artifacts from any prior worktree, giving **25.6% wall-clock speedup on fresh-worktree scenarios** (measured in issue #1822).

**Setup** (one-time per machine):
```bash
# macOS:
brew install sccache

# Linux:
cargo install sccache

# Or download a release binary: https://github.com/mozilla/sccache/releases
```

**Configuration** (optional; auto-detects on first use):
The gate auto-enables sccache if it's on `$PATH`. To customize:
```bash
# Set cache location (default: ~/.cache/sccache on Linux, ~/Library/Caches/Mozilla.sccache on macOS)
export SCCACHE_DIR=/custom/cache/path

# Set size limit (default 10 GiB; raise for multi-worktree teams)
export SCCACHE_CACHE_SIZE=50G

# Disable sccache for a single gate run (if needed for diagnostics)
CQLITE_DISABLE_SCCACHE=1 bash scripts/agent-gate.sh

# Disable sccache permanently (not recommended)
export CQLITE_DISABLE_SCCACHE=1
```

**Rationale: sccache vs shared `CARGO_TARGET_DIR`** (issue #1822):
- **sccache (chosen):** Each worktree has its own `target/` dir (parallel gates do not contend for the build lock); the shared object cache deduplicates `rustc` invocations. Empirically: 7 concurrent worktree gates run in parallel, all benefiting from the cache.
- **Shared `CARGO_TARGET_DIR` (rejected):** `cargo` takes an exclusive build lock on the shared target dir, so concurrent gates serialize (throughput bottleneck), thrashing the cache with different feature sets (each gate component uses different flags / features).

**Cache management**:
```bash
# View cache stats (shows hit rate, size, cache location)
sccache --show-stats

# Zero stats for measurement
sccache --zero-stats

# Stop the background server (if needed for diagnostics)
sccache --stop-server

# Start the server explicitly (normally auto-starts)
sccache --start-server
```

## Accelerator degradation is LOUD, not silent (issue #1848)

Every optional accelerator the gate depends on — **sccache** (cross-worktree
compile cache), **cargo-nextest** (parallel core-tests), and **parallel component
lanes** (needs bash ≥4.3 for `wait -n`) — is auto-detected. When one is **missing**
the gate now emits a **loud `WARN:` line on STDERR** with the one-line install
command, so a machine can never silently run ~3x slower again (the 2026-07-03/04
field failures: sccache and nextest both un-installed for weeks, and stock macOS
bash 3.2 serializing the lanes — all inert wins with no signal):

```
agent-gate: WARN: sccache not installed — cross-worktree compile caching DISABLED (~25.6% slower fresh builds); install: brew install sccache (#1848)
agent-gate: WARN: cargo-nextest not installed — core-tests fall back to serial 'cargo test' (much slower long pole); install: brew install cargo-nextest (#1848)
agent-gate: WARN: bash <4.3 lacks 'wait -n' — gate components run SERIALLY (no parallel lanes; AGENT_GATE_JOBS=1); install: brew install bash (#1848)
```

Every SUMMARY block (full **and** `--lite`) carries a **machine-checkable
`accelerators:` line**, so degradation is visible in the pasted block, not just
scrollback:

```
accelerators: sccache=on nextest=absent lanes=serial
```

State values: `on` (detected & used) · `absent` (missing → WARN) · `off`
(intentionally disabled via `CQLITE_DISABLE_SCCACHE=1` / `CQLITE_DISABLE_NEXTEST=1`
/ `AGENT_GATE_JOBS=1`; **no WARN**) · `lanes=serial` (degraded by bash <4.3). An
intentional opt-out is `off`, never `absent`, and never warns. Self-test coverage:
`scripts/tests/test_agent_gate_summary.sh` (cases 9a/9b assert the `off`/`absent`
markers and the WARN).

## Disk hygiene for multi-worktree gates (issue #1848)

Each active worktree owns its own ~25–30GB `target/` dir. Several concurrent
worktrees can exhaust the disk mid-gate (a confusing hard failure). `flow-finalize`
removes a finished issue's worktree; additionally prune stale worktrees' `target/`
dirs and size the shared cache with `SCCACHE_CACHE_SIZE` (recommend `30G` on the
10-core machine).

**macOS Time Machine local-snapshot gotcha:** deleting `target/` dirs alone often
reclaims **nothing** while a Time Machine *local snapshot* is pinning the freed
blocks. If free space does not recover after deleting build artifacts, check and
thin snapshots:

```bash
tmutil listlocalsnapshots /                 # any snapshot pins freed blocks
tmutil thinlocalsnapshots / 40000000000 4   # thin to reclaim (field: 9.1Gi -> 72Gi)
```

## Gate Parallelism and nextest (issue #1737)

The gate runs **~75% faster** than v0.12.0 on warm machines via two levers:

1. **nextest for core-tests** (the 67% execution floor): `cargo-nextest` parallelizes across test binaries + CPU cores; `core-tests` runs under nextest with an additional `cargo test --doc` pass (nextest skips doctests). Auto-detected; falls back to `cargo test` when unavailable.

2. **Capped 2-lane component parallelism** (issue #1737): a **serial MAIN cargo lane** (shared target, no NEW feature-thrash) runs concurrently with a **SIDE lane** that runs python-bindings and node-bindings in isolated `CARGO_TARGET_DIR`s (kills the cross-lane build-lock / feature-cache-invalidation that would balloon binding times under a naive shared-target pool). Concurrency is capped by `AGENT_GATE_JOBS` (default `min(4, ncpu/2)`), composing safely with #1825's machine-wide bound. Each component records its verdict to a file; the parent reconstructs the SUMMARY in canonical order after lanes drain, so interleaved output never corrupts the machine-checkable block.

**Environment knobs** (all optional; auto-configured):

```bash
# nextest parallelism for core-tests (auto-detected on PATH)
CQLITE_DISABLE_NEXTEST=1 bash scripts/agent-gate.sh      # force plain cargo test

# Component concurrency cap (default: min(4, ncpu/2))
AGENT_GATE_JOBS=1 bash scripts/agent-gate.sh              # sequential (legacy behavior)
AGENT_GATE_JOBS=8 bash scripts/agent-gate.sh              # increase cap (with caution)

# Live Docker parity tests (issue #911, default: skip for static-golden mandate)
CQLITE_SKIP_DOCKER_TESTS=0 bash scripts/agent-gate.sh     # include live Cassandra sstabledump tests
#   (normally skipped; still run in nightly Docker CI lanes; adds ~30s non-determinism when Docker is present)
```

**Graceful fallback**: absent `cargo-nextest`, no `/bin/bash wait -n` (macOS stock 3.2), or `AGENT_GATE_JOBS=1` → gate degrades gracefully to the historical sequential run without loss of coverage.

## Machine-wide full-gate concurrency cap (issue #1825)

Running many sessions/worktrees at once used to let ~15 full gates hit the CPU at once (load 30–60) and SIGKILL gates mid-`core-tests`. The FULL `agent-gate.sh` run now takes a **cross-process bounded semaphore**: at most **N** full gates execute machine-wide at once; excess invocations **queue** (block) for a slot — printing `waiting for gate slot (N in use)…` once — and then proceed. **They never fail from the cap**; non-interactive callers block cleanly.

- **`--lite` and `--only` runs are EXEMPT** (never queued): `--lite` is cheap, and `--only` PARTIAL runs are used by nested tooling self-tests (capping them could self-deadlock the queue).
- **N** defaults to `max(2, floor((ncpu-2)/4))`; override with `CQLITE_GATE_MAX_CONCURRENCY`.
- **SIGKILL-safe stale-slot reaping**: each slot is an `fcntl.flock` held by a background daemon (`scripts/lib/gate_slot_daemon.py`) whose lock fd is NOT inherited by the gate's `cargo`/`nextest` children, so a killed gate releases its slot within one poll — no permanent leak/deadlock.
- Works **across worktrees** (shared slot dir) and composes with `AGENT_GATE_JOBS` (per-gate) + `sccache`. The cap bounds the *worst case*; those cut average load / per-compile time.

```bash
CQLITE_GATE_MAX_CONCURRENCY=4 bash scripts/agent-gate.sh   # raise N on a big box
CQLITE_GATE_SLOTS_DIR=/path bash scripts/agent-gate.sh     # slot dir (default $TMPDIR/cqlite-gate-slots)
CQLITE_GATE_POLL_SECS=1 bash scripts/agent-gate.sh         # queue/liveness poll (default 2s)
CQLITE_GATE_DISABLE_CAP=1 bash scripts/agent-gate.sh       # force-disable the cap
```

The cap fails **open** (disabled, loud stderr note) when `python3`/the daemon is unavailable — the gate is never un-runnable because of the cap. Self-test: `scripts/tests/test_gate_concurrency_cap.sh` (wired into `tooling-tests`).

## `--delta` mechanics: test/docs-only re-certification (issue #1892 / #2081)

`CLAUDE.md` keeps only the invocation and the tier rule (NOT the gate of
record; FAILs CLOSED; record both SUMMARY artifacts). This section is the deep
mechanics behind that rule.

After a full-gate PASS at commit `X`, if the diff `X..Y` touches ONLY files the
re-cert can **EXECUTE** — rust cargo test code (`.rs` under `tests/` dirs,
`*_test(s).rs`), python binding tests (`bindings/python/tests/`, run by the
#1893 python tier), Node.js binding tests (`bindings/node/__test__/*`, run via
the jest suite scoped to the changed files), shell self-tests
(`scripts/tests/*.sh`, executes exactly the changed scripts), and/or docs
(`*.md` anywhere; TOP-LEVEL `docs/`, `website/`) — re-certify with `--delta`
instead of forcing a whole new full gate:

```bash
scripts/agent-gate.sh --delta <anchor-sha> --anchor-run-id <full-gate-run-id>
#   # or, to read the anchor run-id from the recorded full SUMMARY:
scripts/agent-gate.sh --delta <anchor-sha> --anchor-summary-file <path-to-full-SUMMARY>
```

**Refusal list (issue #2081 update):** `node __test__/` files and
`scripts/tests/*.sh` used to be in the refusal list — they are not anymore.
`--delta` now EXECUTES them. Everything else stays refused: `src`, `Cargo.*`,
workflows, config, test-data, and any `.rs` that is not a Cargo `--test`
target.

**Fail-closed design point for node (issue #2081):** `bindings/node/__test__/*`
runs against the **already-built** native module. If the native module is not
built (or node/npm is unavailable), `--delta` **REFUSES** the re-cert — it
NEVER builds with cargo and never passes vacuously. Build it first
(`cd bindings/node && npm run build`) or run the full gate.

On pass, `--delta` runs ONLY file-size + fmt + the diff's changed test targets
and emits a DISTINCT `==== AGENT-GATE DELTA SUMMARY ====` block (MODE: delta)
that names the gate of record (the full PASS at `X`) + the anchor run-id, so it
can NEVER be pasted as a full SUMMARY. The DELTA SUMMARY also carries a
`delta-executors:` line naming which executors ran (e.g.
`scoped-tests(rust/python) node-tests(2) shell-selftests(1)`).

Record BOTH the anchor's full SUMMARY and this DELTA block in the PR. Standing
backstop: the nightly `gate.yml` deep-check re-runs the FULL gate on `main`.
Recovery default: `.agent-gate-delta-summary.txt`.

---

Back to [`CLAUDE.md`](../../CLAUDE.md).
