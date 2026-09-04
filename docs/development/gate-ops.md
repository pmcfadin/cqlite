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

## sccache cache-health monitoring (issue #2641)

A single incident was reported of sccache serving corrupted objects under extreme
load (loadavg ~150). Issue #2641 **characterized before mitigating**: across ~31k
requests on a sustained-high-load gate machine, sccache's **own authoritative error
counters** — `Cache read errors`, `Cache write errors`, `Cache errors`,
`Cache timeouts` — were **all zero**, the eviction-capped cache held **zero
torn/zero-byte objects**, and the cache disk had ample free space (not a disk-full
artifact). There was **no evidence of a load→corruption mechanism**, so the gate
does **NOT** auto-disable caching under load: doing so would forfeit the measured
25.6% build speedup and *increase* build pressure on exactly the loaded machines
that can least afford it, to defend an unreproduced failure mode.

What the incident *did* expose is that sccache's error counters — the one signal
that would catch real corruption — were invisible in the SUMMARY. The
evidence-based mitigation is **monitoring that real signal**, not a blind
auto-disable. Every SUMMARY's `accelerators:` line now carries a trailing
`sccache-health=` token:

```
accelerators: sccache=on nextest=on lanes=on sccache-health=ok
```

- `sccache-health=na`   — sccache not in use (nothing to probe).
- `sccache-health=ok`   — sccache in use, all error/timeout counters zero.
- `sccache-health=warn` — a counter is non-zero → **LOUD `WARN:` on STDERR** naming
  the count and pointing at `sccache --show-stats`. Caching stays **ENABLED** and
  the gate does **not** fail — the WARN is a signal to inspect the cache, not a
  blind kill switch.

The counter sum is probed via `sccache --show-stats` only at SUMMARY emission
(memoized; never in the latency-sensitive classify hooks). On a `warn`, inspect
and, if you confirm corruption, reset the cache:

```bash
sccache --show-stats          # confirm which counter fired
sccache --stop-server && rm -rf "$SCCACHE_DIR" && sccache --start-server
```

If a future *reproduced* incident correlates non-zero counters with load, the
per-gate counters are now recorded to drive that decision on evidence — the point
at which load-aware behavior could be reconsidered. Self-test coverage:
`scripts/tests/test_agent_gate_summary.sh` (case 9c, na/ok/warn + no-auto-disable).

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
accelerators: sccache=on nextest=absent lanes=serial sccache-health=ok
```

State values: `on` (detected & used) · `absent` (missing → WARN) · `off`
(intentionally disabled via `CQLITE_DISABLE_SCCACHE=1` / `CQLITE_DISABLE_NEXTEST=1`
/ `AGENT_GATE_JOBS=1`; **no WARN**) · `lanes=serial` (degraded by bash <4.3). An
intentional opt-out is `off`, never `absent`, and never warns. Self-test coverage:
`scripts/tests/test_agent_gate_summary.sh` (cases 9a/9b assert the `off`/`absent`
markers and the WARN).

## mold linker accelerator — Linux only (issue #2859)

Linking is the **one build cost sccache cannot cache**: every `--lite` round and
full gate re-links every test binary from scratch (`debug = true`), so on a warm
worker link time is a large slice of the remaining wall-clock. On Linux agent
workers `scripts/bootstrap-agent-machine.sh` provisions the **mold** linker and
wires it through a delimited managed block in the **per-machine** `~/.cargo/config.toml`
(honoring `$CARGO_HOME`) — it never touches the repo-committed `.cargo/config.toml`,
so GitHub-hosted CI runners (which have no mold) stay on their defaults.

On **Linux** hosts the `accelerators:` line gains a trailing `mold=` token; on
**macOS** the line is byte-identical to before (mold is Linux-only — Apple's
ld-prime is already the fastest linker on macOS, so a permanent `n/a` token would
churn every existing summary parser for zero signal):

```
accelerators: sccache=on nextest=on lanes=on sccache-health=ok mold=linked
```

State values (Linux only):
- `mold=linked` — mold on `$PATH` **and** the bootstrap-managed block is active in
  the resolved cargo config (the wired, fast path).
- `mold=overridden` — the managed block is active but a **non-empty `RUSTFLAGS`** is
  exported in the gate environment: env `RUSTFLAGS` suppresses cargo's
  `target.rustflags` entirely, so the wired `-fuse-ld=mold` is NOT applied and a
  bare `linked` would lie. **Never export a global `RUSTFLAGS` on a worker** — scope
  it per-command (as the gate's own clippy/minimal-build components do).
- `mold=present-unconfigured` — mold on `$PATH` but **no** managed block (bootstrap
  not re-run) → the installed-but-unwired silent-degradation the token exists to
  surface; re-run `bash scripts/bootstrap-agent-machine.sh`.
- `mold=absent` — mold not installed.

Provisioning is **advisory** (mirrors sccache/nextest): a missing or uninstallable
mold never fails the run. Bootstrap installs mold via the native package manager
(apt/dnf/yum/pacman) and writes the managed block **only after a link probe** proves
the resolved C compiler accepts `-fuse-ld=mold` (fail-safe: a probe failure warns
and writes nothing — a machine never ends up with a config that breaks linking).
When only `clang` passes the probe, the block adds `linker = "clang"` per triple.
Self-test coverage: `scripts/tests/test_agent_gate_summary.sh` (case 9d asserts the
four Linux states — incl. `overridden` and a no-override real-detection case — plus
the Darwin no-token contract) and
`scripts/tests/test_bootstrap_agent_machine.sh` (case 6 asserts detection, install
print-only, the link probe, the managed-block write, idempotency, user-config
preservation, and the Darwin no-op). See fleet-runbook for the one-time sccache
cold-rebuild note at enablement.

### The `perf=` profiling-capability token (Linux only, issue #3249)

After `mold=`, a Linux `accelerators:` line carries a `perf=` token answering *can this
box be profiled at all?*

```
accelerators: sccache=on nextest=on lanes=on sccache-health=ok mold=linked perf=ok
```

It is a **free** read of `/proc/sys/kernel/{perf_event_paranoid,kptr_restrict}` through
shell builtins — no `perf` exec, no new binary dependency (the functional
`perf stat -C 0 -e cycles` verification is **bootstrap's** job, not the gate's). "Free"
is a *measured* cost, enforced by `test_agent_gate_summary.sh` case `perf-free`: the
emit-time path performs **0 external processes and 0 command substitutions** — each
`$( )` is a forked subshell, so the token is returned through a caller-named variable
(`perf_capability_token_into <outvar>`) rather than stdout — and
`scripts/perf-capability.sh` is sourced **once per gate run**, never per summary. The
test asserts both halves: the substitution count statically, and the extracted path
re-executed with an unresolvable `PATH` under xtrace subshell counting (so a
stderr-silenced exec cannot hide). State values (Linux only):
- `perf=ok` — unprivileged per-CPU profiling **and** kernel symbol resolution available.
- `perf=paranoid-<N>` — `perf_event_paranoid = N >= 1`. Cumulative: `>= 1` forbids
  **CPU-wide** event access, which is exactly what the mandated `perf stat -C <cpu>`
  needs, so it is **denied**. Agent images ship `4`; on Debian/Ubuntu kernels `>= 3`
  denies unprivileged perf entirely. This is a **permission** verdict whose "access
  limited" help text reads like a missing *capability* — the confusion that cost two
  measurement cycles.
- `perf=kptr-restricted` — paranoid is fine, `kptr_restrict != 0`: kernel frames render
  as bare addresses (a **silent attribution loss**, not an error).
- `perf=absent` — the `/proc` controls are not present (container without a writable
  procfs → tune the HOST). `perf=unknown` — present but unparseable, never guessed.

Anything but `ok` on a box you intend to measure means **re-run
`bash scripts/bootstrap-agent-machine.sh --yes`** (installs + applies + verifies
`/etc/sysctl.d/99-cqlite-perf.conf`), not "perf is unavailable here". Rationale
(`-1`, not `1`), the BPF-still-needs-sudo caveat, the single-tenant security posture
and the `/etc/sysctl.conf` precedence trap: `docs/development/fleet-runbook.md`.

Self-test coverage: `scripts/tests/test_agent_gate_summary.sh` (cases 9f* assert every
state via the test seam, the Darwin no-token contract, **and** the production branch
against a real `/proc` fixture with the seam unset) and
the pair `scripts/tests/test_perf_capability.sh` (the helper's unit contract) +
`scripts/tests/test_perf_capability_bootstrap.sh` (the bootstrap
write/read-back/verify path, including the silent-revert and denied-`perf` cases),
which share `scripts/tests/lib/perf-capability-test-lib.sh`. Both are in the
`tooling-tests` `&&`-chain; together they also pin the fail-closed identity rules (an
unusable `id -u`, an inconsistent `SUDO_USER`) and the enforced hermeticity of test
mode (both path seams mandatory, no production fallback).

## Disk hygiene for multi-worktree gates (issue #1848)

Each active worktree owns its own ~25–30GB `target/` dir. Several concurrent
worktrees can exhaust the disk mid-gate (a confusing hard failure). `flow-finalize`
removes a finished issue's worktree; additionally prune stale worktrees' `target/`
dirs and size the shared cache with `SCCACHE_CACHE_SIZE` (recommend `30G` on the
10-core machine).

**A single `--lite` round can be the thing that exhausts the disk.** Measured by
another lane and reported in issue #3764: one `--lite` on a `cqlite-core/src/` diff
grew `target/debug/deps` by **~18 GB in roughly ten minutes**, taking that box from
34 GB to 16 GB free — because that diff shape triggers the issue #2658
dependent-crate leg, which runs `cargo test -p <pkg> --all-targets --no-run` for
every workspace member that directly declares a dependency on `cqlite-core`. So do
not budget `--lite` as a cheap, disk-neutral round on a shared box. Worse, **`--lite`
is EXEMPT from the issue #1825 gate-slot cap**, so nothing serialises that build
against a peer's concurrent gate of record — the two compete for the same disk with
no arbitration. There is no admission check for `--lite` today; issue #3763 owns that
gap.

**And the cost lands on the NEXT lane, not the one that spent the disk.**
`scripts/local/worker-supervisor.sh` sets `DISK_FLOOR_GB="${DISK_FLOOR_GB:-40}"`
(`:155`) and enforces it in `preflight_reason()` (`:3204-3208`), so the incident's end
state — 16 GB free — is **below** that floor: nothing stopped the `--lite` that spent
the disk, but the very next worker iteration on that box would have held on `disk`.

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

2. **Capped 2-lane component parallelism** (issues #1737, #2657): a **serial MAIN cargo lane** (shared target, no NEW feature-thrash) runs concurrently with a **SIDE lane** whose components each get their own isolated `CARGO_TARGET_DIR` (kills the cross-lane build-lock / feature-cache-invalidation that would balloon times under a naive shared-target pool). The SIDE lane holds every **isolatable non-core** component — the two bindings (python-bindings, node-bindings) plus (issue #2657) the non-cargo / isolated-feature components **parity-report, delivery-telemetry, binding-unwind-profile, smoke, and memory-budget**. These build a *different* feature set of cqlite-core (memory-budget: `dhat-heap,arrow`), a *dependent* crate (smoke → cqlite-cli, parity-report → cassandra-parity), or *no cargo at all* (delivery-telemetry, binding-unwind-profile) — so running them against MAIN's `cli-helpers` target dir would thrash it. Isolating each into its own target dir lets them overlap the core cargo long pole instead of tailing it (sccache still dedups the real compiles across dirs). **`tooling-tests` is deliberately EXCLUDED from SIDE and stays SERIAL on MAIN** (issue #2657 gate FAIL): although it runs no shared-target cargo, it embeds **timing-sensitive shell self-tests** — notably `test_worker_supervisor.sh`'s exit-latency assertion (#2666, `<15s` ceiling) — that **starved under co-scheduled SIDE-lane CPU load** (measured ~20s under the parallel pool vs ~7s in isolation), so parallelizing it degraded the very component it moved. Keeping it serial preserves its latency headroom. The MAIN lane keeps every component that builds cqlite-core under MAIN's feature set (core-tests, write-tests, cli-tests, integration-tests, the guards, clippy/fmt) plus tooling-tests strictly serial — that shared-target set is exactly why only the bindings could parallelize before. Concurrency is capped by `AGENT_GATE_JOBS` (default `min(4, ncpu/2)`), composing safely with #1825's machine-wide bound; MAIN takes one slot and the SIDE lane runs up to `AGENT_GATE_JOBS-1` of its components concurrently. Each component records its verdict to a file; the parent reconstructs the SUMMARY in **canonical COMPONENTS order** after lanes drain, so widening the SIDE lane changes only *which lane* a component runs in — the machine-checkable SUMMARY block (component set, order, line format) is **unchanged in contract**. The `main`/`side` split has one source of truth (`_component_lane`); the hidden `--classify-lanes` hook prints it and `scripts/tests/test_agent_gate_sublanes.sh` (run inside `tooling-tests`) pins it.

   **Measured effect** (issue #2657, warm macOS 8-core `min(4, ncpu/2)=4`, `AGENT_GATE_JOBS=4`): the five newly-isolatable components previously ran serially *after* the core cargo lane — dominated by memory-budget (**~146s**, four dhat lanes at `--test-threads=1`), with parity-report ~6s, smoke ~6s (warm), binding-unwind-profile ~2s, delivery-telemetry ~1s. Moving them to the concurrent SIDE lane overlaps that work with the core cargo long pole, cutting full-gate wall-clock by roughly the overlapped span (dominated by memory-budget's ~146s; bounded by the MAIN lane's duration and the `AGENT_GATE_JOBS-1` side-slot cap). **Note (issue #2657): the reduction is now smaller than the original six-component estimate** — the previously-planned multi-minute `tooling-tests` was pulled back onto the serial MAIN lane after it starved its own exit-latency self-test under co-scheduled load, so it no longer overlaps; the exact post-exclusion figure is approximate and should be re-measured on a warm machine. On `AGENT_GATE_JOBS=1` or bash <4.3 the run collapses to the historical strictly-sequential order with identical coverage (no reduction, no contract change).

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

**Graceful fallback**: absent `cargo-nextest`, no `/bin/bash wait -n` (macOS stock 3.2), or `AGENT_GATE_JOBS=1` → gate degrades gracefully to the historical sequential run (all components, incl. the widened #2657 SIDE lane, run serially in canonical order) without loss of coverage or any change to the SUMMARY contract.

## nextest test-groups, retries, and slow-timeout (issue #2643)

`.config/nextest.toml` at the repo root is **auto-discovered** by `cargo nextest run` — no `--config-file` flag needed — so the gate's `core-tests` nextest invocation (#1737) picks it up transparently. It is **orthogonal to the per-gate CPU budget (#2640)**: that derives the global `--test-threads` ceiling; this file adds test-*groups* (bounded co-scheduling of load-sensitive tests), scoped retries, and a hung-test `slow-timeout`. The two levers never fight — `--test-threads` caps total concurrency; a group's `max-threads = 1` caps concurrency *within that group*.

The design principle is **retries scoped to load-variance, never to correctness**:

- **`timing` group** (`max-threads = 1`): the general **load-sensitive serialization** group. **Group membership and retry policy are SEPARATE (#3514)** — `max-threads` is a property of the *group*, but `retries` is set by each `[[profile.default.overrides]]` block that assigns the group, so two members can share the serialization and disagree about retries. Read the overrides, not the group name.
  - **Timing tests, `retries = 2` exponential backoff**: the ratio/latency/throughput tests that legitimately vary on a loaded, oversubscribed box — `tail_latency_harness` (the documented `mixed_p99_bounded_by_k_times_baseline` tail-latency flake under concurrent gates) and `sstable_performance_regression_tests` (wall-clock `MAX_*_MS` budget asserts; their retirement is #2642 — until that lands, scoped retries de-flake the gate). Serialized so they never perturb each other's timings.
  - **Thread-budget correctness pins, NO retries (#3514)**: `issue_2316_merge_thread_budget` (one k-way merge, O(M) producer threads) and `issue_2370_concurrent_merge_thread_budget` (C concurrent merges, O(C·M)). These are **correctness** pins, not timing tests. They join the group *only* for its `max-threads = 1` co-scheduling — both observe a **whole-process** OS thread count, and their failure mode under CPU starvation is delayed thread **reaping**, so serializing them against each other and against the two heaviest CPU consumers above removes the worst contention. They keep `retries = 0` (the profile default) **deliberately, not by omission**: a genuine thread amplification must fail deterministically, and retrying it could only turn a real regression into flap. The starvation half is handled *inside* the tests by affirmative reap confirmation — a mechanism, not a re-roll. Note `max-threads = 1` bounds contention **within** the group only, so it reduces the starvation source rather than removing it.
- **`docker` group** (`max-threads = 1`, **no retries**): `docker_probe_timeout` plus the `*under_cassandra5_sstabledump` live-Cassandra parity tests (#911, skipped by default in the gate, run in nightly Docker lanes). A single Docker host → serialize; but a parity divergence must **fail**, never flap-retry to green.
- **Everything else — parity, byte-for-byte, read-path, type-decode — keeps `retries = 0`** (the `profile.default` default). A wrong byte or a diverged `SELECT` must fail deterministically, never be masked by a retry.
- **`slow-timeout`** = warn at 60s, hard-kill after 4 periods (240s) — a generous backstop for a genuinely wedged test/process, never a killer of slow-but-honest tests on a loaded box.

Verify groups resolve (no TOML parse errors, membership as intended):

```bash
cargo nextest show-config test-groups --package cqlite-core --features cli-helpers
# prints: group: docker (max threads = 1) … group: timing (max threads = 1) … with members
```
## Guard-cluster compile/link/exec profile (issue #2647)

**Question (epic #2636):** the guard-cluster components — `tombstones-scan`,
`scan-offload-guard`, `work-counters-guard`, `byte-budget-guard`, `write-tests` —
each invoke `cargo test -p cqlite-core` with a *different* `--features` set. Each
distinct feature set is a distinct `cfg` fingerprint, so `cqlite-core`'s own crate
(lib + the component's test harnesses) recompiles per set even with a warm shared
target (sccache caches *dependency* crates across sets, not the first-party crate
whose `cfg` changed). The #2636 hypothesis was that collapsing them onto one
superset `--features` invocation would save ~4–6 min of redundant compile.

**Measured (2026-07-18, 16-core arm64, warm sccache 66% hit rate, shared target
dir, `CARGO_BUILD_JOBS=4` to mimic a serial main-lane compile under load).** The
distinct feature sets, additive over the `cqlite-core` defaults
(`all-compression,state_machine,write-support`):

| component | `--features` delta vs default | first-`cargo`-in-set recompile |
|-----------|-------------------------------|-------------------------------|
| (warm baseline: deps + first `cqlite-core` cfg) | `write-support,cli-helpers,state_machine` | 476 s (deps-dominated, paid once) |
| `tombstones-scan` | `+cli-helpers,+tombstones` | **24 s** |
| `scan-offload-guard` | `cli-helpers,scan-offload-probe` (drops write-support) | **36 s** |
| `work-counters-guard` | `+cli-helpers,+state_machine,+work-counters` | **30 s** |
| `byte-budget-guard` | `+cli-helpers,+state_machine` (== default cfg) | **1 s** (cache hit — no recompile) |
| `write-tests` | default only (drops `cli-helpers`) | **56 s** |

Redundant per-`cfg` recompile in the cluster ≈ **24 + 36 + 30 + 56 = 146 s** (the
byte-budget set is identical to the default cfg and pays nothing). A single
**unified superset** (`write-support,cli-helpers,state_machine,tombstones,scan-offload-probe,work-counters`)
built once measured **275 s from a cold target** and then compiled ALL the cluster's
`--test`/`--lib` targets under that one cfg with **0 s** incremental thrash. So the
realizable saving is ~one first-party recompile pass — **≈ 1.5–1.6 min**, NOT the
4–6 min hypothesized. gate-ops' 67% execution floor for `core-tests` bounds the
other side: the guard cluster's *execution* (short, targeted `--test` runs) is a
small fraction of gate wall-clock; compile is the dominant cost only for these
short-execution components, and 146 s of it is what's on the table.

**Decision (issue #2647): NOT UNIFIED — measurement is the deliverable.** Two
reasons the ~1.5 min saving does not justify collapsing the cluster:

1. **Coverage would not be identical (`--features` isolation risk).**
   `scan-offload-probe`, `work-counters`, and `tombstones` do NOT gate test
   modules only — they gate **production code paths and `pub`/`pub(crate)`
   visibility** (`scan_stream_windowed*`, `scan_admission`, `read_work_counters`,
   `work_counters`, the tombstone/GC branches in `select_executor`/`generation_merge`).
   The gate deliberately exercises BOTH postures: probes-OFF (`core-tests`,
   `write-tests --lib` on the default cfg with `cli-helpers` and probes absent) and
   probes-ON (the guards). A superset cfg changes the compiled-code-under-test for
   the `--lib` runs (extra `record_*` call sites, extra pub surface, the
   `scan-offload-probe` deadlock module), so the probes-OFF regression net —
   "a default/release build links no counter/probe statics and pays nothing"
   (Cargo.toml `work-counters`/`scan-offload-probe` docs) — would no longer be
   proven by the same run. Acceptance requires *identical* `--test` coverage; a
   superset silently trades a coverage posture for ~90 s.
2. **The parallel-lane design already amortizes most of the compile.** #1737's
   capped 2-lane pool runs these components concurrently against a shared target;
   the 146 s is wall-clock-overlapped with `core-tests`/`integration-tests`/binding
   lanes, so the *serial* redundant-compile figure over-states the on-the-clock
   cost. The net PR-visible saving is well under a minute, against a real coverage
   regression and a more brittle single invocation.

`dhat-heap` (`memory-budget`) and `arrow` (`arrow-parity-guard`) were out of scope
for unification regardless (global allocator needs `--test-threads=1`; `arrow`
pulls the arrow crate) and stay isolated. Re-open only if a *measured* redundant
compile > ~4 min appears (e.g. after the cluster grows), and only with a scheme
that preserves the dual OFF/ON coverage posture.

## Machine-wide full-gate concurrency cap (issue #1825)

Running many sessions/worktrees at once used to let ~15 full gates hit the CPU at once (load 30–60) and SIGKILL gates mid-`core-tests`. The FULL `agent-gate.sh` run now takes a **cross-process bounded semaphore**: at most **N** full gates execute machine-wide at once; excess invocations **queue** (block) for a slot — printing `waiting for gate slot (N in use)…` once — and then proceed. **They never fail from the cap**; non-interactive callers block cleanly.

- **`--lite` and `--only` runs are EXEMPT** (never queued): `--lite` is cheap, and `--only` PARTIAL runs are used by nested tooling self-tests (capping them could self-deadlock the queue).
- **N** defaults to `max(2, floor((ncpu-2)/4))`; override with `CQLITE_GATE_MAX_CONCURRENCY`.
- **Every SUMMARY says WHERE N came from (issue #3414)**: the `cpu-budget:` line stamps
  `max-concurrency=N(pinned|default|invalid|clamped)` — `pinned` = the env var held a valid
  integer >= 1, `default` = it was UNSET so N is the formula, `invalid` = it was empty or
  non-numeric and was silently discarded for the formula, `clamped` = it was a valid integer
  < 1 and was silently raised to 1. `3` and `3 because nothing set it` are different
  operational facts: the whole fleet ran at `N=3` for months with the pin present in
  `~/.bashrc` and invisible to every non-interactive shell (stock Ubuntu `.bashrc` returns
  early when not interactive), and no artifact said so.
- **The remedy differs by token.** `default` = no pin line at all, so
  `bash scripts/bootstrap-agent-machine.sh --fix-gate-pin` (or `--yes`) persists one into
  `/etc/environment`, and its `gate-pin:` line VERIFIES the result by probing a fresh
  profile-free PAM session rather than by grepping the file it wrote. `invalid`/`clamped` =
  the line is ALREADY there with a bad value, and bootstrap never rewrites an existing value,
  so re-running it is a **silent no-op** — fix the VALUE by hand. Bootstrap reports that fork
  as `gate-pin: NOT-HONOURED`.
- **SIGKILL-safe stale-slot reaping**: each slot is an `fcntl.flock` held by a background daemon (`scripts/lib/gate_slot_daemon.py`) whose lock fd is NOT inherited by the gate's `cargo`/`nextest` children, so a killed gate releases its slot within one poll — no permanent leak/deadlock.
- Works **across worktrees** (shared slot dir) and composes with `AGENT_GATE_JOBS` (per-gate) + `sccache`. The cap bounds the *worst case*; those cut average load / per-compile time.

```bash
CQLITE_GATE_MAX_CONCURRENCY=4 bash scripts/agent-gate.sh   # raise N on a big box
CQLITE_GATE_SLOTS_DIR=/path bash scripts/agent-gate.sh     # slot dir (default $TMPDIR/cqlite-gate-slots)
CQLITE_GATE_POLL_SECS=1 bash scripts/agent-gate.sh         # queue/liveness poll (default 2s)
CQLITE_GATE_DISABLE_CAP=1 bash scripts/agent-gate.sh       # force-disable the cap
```

The cap fails **open** (disabled, loud stderr note) when `python3`/the daemon is unavailable — the gate is never un-runnable because of the cap. Self-test: `scripts/tests/test_gate_concurrency_cap.sh` (wired into `tooling-tests`).

## The startup `INCOMPLETE` sentinel is a liveness placeholder, not a verdict (issue #3041)

`agent-gate.sh` writes a startup sentinel into `$AGENT_GATE_SUMMARY_FILE` **before any component runs** — before `acquire_gate_slot` even grants the #1825 slot — whose terminal line is exactly:

```
RESULT: INCOMPLETE (gate did not finish)
```

It is overwritten with `RESULT: PASS` / `RESULT: FAIL` only at the terminal emit. The sentinel is deliberate and load-bearing: it is what makes a killed/orphaned/queued gate detectable (and since #2926 it also carries `tree-start:`, so a killed run still records the tree it began on).

**Consequence for every poller: `INCOMPLETE` is a liveness placeholder, not a verdict.** A bare `grep -q` on the bare `RESULT:` token is satisfied the instant the gate launches, so an agent polling that way can read a **just-launched or still-queued** gate as a finished one, treat the placeholder as its gate of record, and advance toward merge on a verdict that does not exist — silently voiding the only run that counts. There is one correct completion predicate PER RUN MODE — never one for both (#3750) — and in agents, skills, docs, and any helper that polls a summary file they are:

```bash
# RECORD grammar — full / --lite. Anchored + token-terminated, and it MUST keep refusing PARTIAL
# (and ERROR and REFUSED). Widening it would weaken the gate-of-record probe for nothing.
grep -qE '^RESULT: (PASS|FAIL)([[:space:]]|$)' "$AGENT_GATE_SUMMARY_FILE"   # a VERDICT ⇒ gate finished

# ONLY grammar — `--only <component>` ONLY, and NEVER on the gate of record (#3750). `--only` demotes a
# SUCCESSFUL run to `RESULT: PARTIAL`, so the record grammar above spins on green. Prefer the EXIT STATUS
# (3 = completed PARTIAL); this is the fallback for a detached run whose exit code you never see.
grep -qE '^RESULT: (PASS|FAIL|PARTIAL)([[:space:]]|$)' "$AGENT_GATE_SUMMARY_FILE"

# DELTA grammar — `--delta <anchor>` ONLY, and it is the one that bites (#3750 round 3). `run_delta` can
# terminate with ERROR (4 emit sites) or REFUSED (3 more, via `emit_summary "$(_tree_result REFUSED)"` —
# which is why grepping `emit_summary REFUSED` finds nothing and the token looks unemitted; it IS
# emitted, and gate-liveness.sh's comment enumerating it is accurate, not stale). All seven are inside
# `run_delta`, so a --delta poller on the RECORD grammar HANGS on a terminal outcome. This set is
# gate-liveness.sh's own enumerated terminal set token for token — ONE source of truth, not a second
# list — hence PARTIAL (unemittable by --delta; that is the --only demotion) and the defensive REFUSED.
grep -qE '^RESULT: (PASS|FAIL|PARTIAL|ERROR|REFUSED)([[:space:]]|$)' "$AGENT_GATE_SUMMARY_FILE"

# Widening a COMPLETION grammar is safe here and would NOT have been before #3750 split completion from
# verdict: matching ERROR/REFUSED cannot create a false pass, because the verdict is now a separate
# affirmative read (the PASS token exactly, or the component's own line). Three grammars are therefore
# not three chances to be wrong. Better than any of them: ask gate-liveness.sh, the single source of
# truth executable rather than transcribed.

# And COMPLETION IS NOT A VERDICT: `PARTIAL` says the run ENDED, not that your component passed. Read the
# component's OWN line, as a separate assertion. A completed run whose component SKIPped is NOT a pass.
bash scripts/gate-component-verdict.sh "$AGENT_GATE_SUMMARY_FILE" \
     --mode only --component tooling-tests --run-id <id>
# 0 PASS / 1 NOT-PASS / 4 COULD-NOT-MEASURE (no verdict available, whatever the reason) / 64 USAGE.
# It REFUSES a LITE/DELTA block (4). A block whose `tree-integrity:` token is FAIL returns NOT-PASS (1)
# — an AFFIRMATIVE reading, because the gate itself declared that run non-certifying and that
# invalidates every component in the block; SKIP/PENDING/absent/unrecognised return 4, because tree
# stability was then never measured. Either way it never answers PASS about such a run.
#
# NOT A COMPLETION PROBE, AND NO OPINION ABOUT LIVENESS — NEVER IN A LOOP. Establish completion with
# one of the two probes above (or the exit status); `gate-liveness.sh` is the three-valued liveness
# authority and the only one that may be polled. A retryability taxonomy here was DESCOPED (#3750):
# `--no-wait` makes the reader's STALLED unreachable, so a LIVE gate whose beat is merely stale
# arrives as UNKNOWN and was reported permanent — and a lane obeying that relaunches a live gate,
# putting two gates on one summary path.
```

Corollaries:

- A summary file holding only the sentinel means **still running, died, or queued** — never "certified".
- A queued gate (`waiting for gate slot (N in use)…`) *already has* a sentinel-bearing summary file; the file's existence is not progress. See also the closer's queue-aware deadline rule in `.claude/agents/flow-closer.md`.
- The reader contract still applies on top of this: validate the block's `run-id:` line and read `tree-integrity:` alongside the verdict (#2874/#2926) — a foreign `RESULT: PASS` is a peer's verdict, not yours.
- The stronger *mechanism* fix (a distinct `.running` sentinel that cannot be misread, plus a pin in `scripts/tests/test_agent_gate_summary.sh`) is tracked in **#2908**; #3041 corrected the documented predicate everywhere.

## Liveness diagnosis: is my gate alive, queued, or dead? (issue #3042)

The sentinel above tells you a gate has not *finished*; it says nothing about whether it is still
running. Diagnosing that wrongly is expensive in both directions — killing and relaunching a healthy
queued gate wastes 15–25 min, and waiting on a dead one wastes the whole session. Use these probes,
in this order:

- **The authoritative aliveness probe is `scripts/gate-liveness.sh` (issue #3473).** The gate itself
  now beats `<summary-file>.heartbeat` every 20s for as long as its process lives, and the startup
  sentinel names that path. One command answers the question this whole section is about:
  ```bash
  bash scripts/gate-liveness.sh "$AGENT_GATE_SUMMARY_FILE" --run-id <run-id>
  #   COMPLETE (0) | RUNNING (2) | STALLED (3) | UNKNOWN (4, with a named cause)
  ```
  `RUNNING` covers queued-and-alive, so it needs no separate queue check. `STALLED` is the state
  nothing could previously express: **this run has published no liveness for a while.** It is
  deliberately NOT a claim that the process is dead — #3473 tried that (`REAPED`) and descoped it
  after four review rounds, because proving a process dead means proving a negative about a
  machine you may not be on. Act on it like this: the gate relaunches its beater at every
  component boundary, so a live gate whose beater alone died recovers to `RUNNING` within one
  component; re-read before acting, and if it is still `STALLED` after a component's worth of
  time treat the gate as gone and relaunch it — and read that duration OFF THE COMPONENT TABLE IN
  YOUR OWN SUMMARY (`<name>: PASS (<n>s)`), never off a figure in prose. The figure that used to sit
  here, "~850s", was understated by 2.4x (`tooling-tests` measured **2073s**, #3473), and acting on
  an understated bound is exactly what makes a closer declare a LIVE gate gone and relaunch it —
  putting two gates on one summary path. Pass `--run-id` whenever
  you know it; a concurrent peer's beat on a shared default path otherwise answers about the
  peer's gate (#2874). A **missing** beat is `UNKNOWN`, never `STALLED` — an older gate simply
  has no beat.
- **Fallback, and only a fallback: the gate LOG FILE's mtime advancing.** Use this when
  `gate-liveness.sh` reports `UNKNOWN` because there is no heartbeat.
  ```bash
  stat -f %m gate-<N>.log   # macOS; GNU: stat -c %Y
  ```
  An advancing mtime means alive. **The converse does NOT hold** — this probe is one-directional. A
  queued gate writes nothing at all, and a live gate inside a long silent component can leave the
  mtime flat for minutes, so a static mtime is not evidence of death. That asymmetry is why the
  heartbeat exists. (You are only reading the *timestamp* here — never read `gate-<N>.log`'s contents
  into context; the SUMMARY file remains the only gate text you retain.)
- **A gate launched in-session dies with its session's cgroup (#3473).** If a gate keeps turning up
  `STALLED`, the cause is probably that it was launched inside an agent session rather than with
  `scripts/flow/gate-detached.sh`; `nohup`/`setsid` do not help. See
  `docs/development/lane-gate-execution.md`.
- **`ps` is unreliable for this** and should not be your primary signal. A gate spends long stretches
  inside child `cargo`/`nextest`/`rustc` processes under different names, and a **queued** gate is
  legitimately running no cargo at all — so "I don't see it in `ps`" is not evidence of death.
- **`waiting for gate slot (N in use)…` means QUEUED and ALIVE, not hung.** Under the #1825 cap a
  gate can sit in the queue for 20+ minutes before executing anything. It already has a
  sentinel-bearing summary file (written before the slot is granted), so neither the file's existence
  nor its `INCOMPLETE` content is progress. A queued gate's wall-clock does not count against an
  active-gate deadline — extend the deadline by the observed queue wait
  (`.claude/agents/flow-closer.md` step 1).
- **A missing slot daemon IS meaningful evidence of death — but only comparatively.** Each live full
  gate has its own background `scripts/lib/gate_slot_daemon.py` holding its slot for as long as the
  gate process lives. So the total absence of *your own* gate's daemon **while sibling gates' daemons
  are present** is real evidence your gate died (the daemon polls the gate PID and exits when it
  vanishes). No daemons at all for anyone is inconclusive — the cap fails open when `python3` or the
  daemon script is unavailable, and `--lite`/`--delta`/`--only` runs never take a slot.
  ```bash
  pgrep -fl gate_slot_daemon.py   # one line per gate currently holding a slot
  ```
- **Gate slot acquisition is NOT FIFO.** The daemon sweeps the N slot lockfiles with non-blocking
  `flock` and retries the whole sweep after a poll interval, so there is no queue order and no
  fairness guarantee: a gate that started waiting later can win a freed slot first, and an unlucky
  gate can be passed over repeatedly. Do not infer "my gate must be next" from having waited longest,
  and do not read a long wait as a stall.

Putting it together: an `INCOMPLETE` summary + an advancing log mtime = **alive, keep waiting**. An
`INCOMPLETE` summary + a log mtime frozen for many minutes + your daemon absent while peers' daemons
are present = **dead, relaunch**. Anything else is inconclusive — prefer waiting to relaunching, and
report `gate-timeout` on the hard deadline rather than guessing.

## Component logs under `logs: <dir>` (issue #3401)

Every component that RUNS writes `<dir>/<component>.log`, where `<dir>` is the SUMMARY's `logs:`
line — that is the ONLY gate text besides the SUMMARY an agent should open, and it is where you go
when a component's one-line verdict is not enough. A component that **SKIPs** (`python-bindings`,
`oom-audit` and `tooling-tests` each have a no-toolchain SKIP path) writes no log at all: its reason
is in the SUMMARY line only, so do not read an absent `<component>.log` as a missing artifact. In particular `file-size.log` carries the whole
ratchet computation the verdict summarises: the thresholds applied, the resolved base sha (and
the ref it came from, or an explicit "base ref unavailable — growth ratchet skipped"), the full
list of changed `.rs` files currently over threshold, and one `path: before -> after (limit N)`
line per over-threshold file the change grew. It is written on **every** run, PASS included, so a
`file-size: FAIL` never again requires re-deriving line counts across the diff by hand. If the
component cannot write that log at all (unwritable path, filesystem full, rejected appends) it FAILs
rather than passing silently, and puts the diagnostic — including the grown-file list, which would
otherwise die with the log — in the sibling `file-size.persistence-error.log` under the same `logs:`
directory, so the failure of the log has a log of its own.

## Nested / concurrent-gate isolation (issue #2874)

The gate of record is **structurally immune** to nested and concurrent gate activity — no box-exclusive ops rule and no "serialize every self-test lane" discipline is needed. The historical `#2751` workaround ("run the full gate **without** `AGENT_GATE_SUMMARY_FILE`") is **OBSOLETE**: the summary-file redirect invocation (`AGENT_GATE_SUMMARY_FILE=… bash scripts/agent-gate.sh`) is once again the documented default for callers, and running it concurrently with another lane's gate self-tests on the same box is safe. Three mechanisms guarantee this:

- **Nested-run summary auto-isolation.** Every gate exports a per-run marker `AGENT_GATE_PARENT_RUN_ID` (= its unique mktemp log dir) for the duration of its component runs. Any gate that starts with that marker in its env **and no explicit `AGENT_GATE_SUMMARY_FILE`** is a *nested* run: it defaults its summary to a **private path inside its own log dir** (`<log-dir>/summary-primary.txt`, stamped `nested-under: <parent-run-id>`), never the enclosing checkout's shared default (`.agent-gate-summary.txt` / `-lite-` / `-delta-`). This closes the residual same-checkout default-path clobber vector left after `#2751` closed the `AGENT_GATE_SUMMARY_FILE` env-inheritance vector — independent of any self-test's own unset/pin discipline. An explicit `AGENT_GATE_SUMMARY_FILE` from the nested caller still **wins** (self-tests keep pinning it to assert on summary content).
- **Mid-run summary-integrity guard + no-clobber publish.** At every component boundary — and again at the terminal emit — the gate verifies its summary file still carries its own `run-id`. A **foreign run-id** (a stray nested/concurrent write) is caught **immediately** with a named `summary-integrity: FAIL (foreign run-id detected mid-run; expected <run-id>)` line and a non-zero exit — never the bare `INCOMPLETE` death that used to cost a ~1h diagnostic re-run. Crucially, when the contended path is found holding a foreign block the gate **does NOT rewrite it** (that would clobber the live peer): it publishes its own FAIL verdict — carrying the full component table — to a **non-clobbering sibling `<summary-file>.integrity-fail.<run-id>`** plus the `logs:` bundle (and stdout/stderr), and exits non-zero, leaving the peer's block on the pinned path. **Reader contract:** the process **exit code is primary**, and any pinned-path block **MUST be validated by its `run-id:` line** before it is trusted — a block whose `run-id` is not the one you launched (even `RESULT: PASS`) is a peer's, not yours; on a mismatch read the `.integrity-fail.<run-id>` sibling / `logs:` bundle. This decision is on the **observable condition alone** (`SENTINEL_WROTE=1 && the path lacks our run-id`), so a peer write landing *after* the last component boundary is caught at the terminal emit too, not just at component boundaries.
- **Hermetic self-test fixtures.** Every gate self-test under `scripts/tests/` derives all fixture/tmp paths from per-run `mktemp` namespaces (terminal `XXXXXX`, macOS-safe); no fixed shared names. In particular the `parity-report` self-test's mutated-manifest fixture is a per-run unique file (still under the real repo root's `test-data/` as its tooling requires). A structural lint (`scripts/tests/test_gate_selftest_hermetic.sh`, wired into `tooling-tests`) FAILs the component if a fixed `.tmp-*` fixture name or a non-terminal-`XXXXXX` mktemp template is reintroduced.

Self-tests: `scripts/tests/test_agent_gate_nested_isolation.sh` (nested-clobber immunity, explicit-wins, mid-run integrity FAIL, same-checkout concurrency) and `scripts/tests/test_gate_selftest_hermetic.sh` (the fixture lint), both wired into `tooling-tests`. Peer **full** gates in the *same* checkout still need distinct summary paths / separate worktrees (out of scope here) — the guarantee above is about *nested* and *self-test* activity, not two top-level full gates sharing one `.agent-gate-summary.txt`.

## Mid-run tree mutation: `tree-mutated-midrun` (issue #2926)

Sibling guard to the one above: `summary-integrity` protects **who owns the summary artifact**; `tree-integrity` protects **what that artifact describes**. The gate captures a *tree identity* at start (HEAD + dirty flag + a sha256 of a per-path content manifest covering every uncommitted tracked change and every untracked, non-ignored file), re-captures it at every `record_result` boundary and once immediately before the terminal emit, and stamps `tree-start:` / `tree-end:` / `tree-integrity:` into **every** SUMMARY block (full, `--lite`, `--delta`, `--only`) plus `tree-start:` into the startup `INCOMPLETE` sentinel.

**The shape that causes it** (#1582 / #1930): a lead legitimately runs a `flow-closer` (gating) and a fixer (editing) that overlap on ONE worktree — this does *not* violate the one-worker rule, and it happened for real on 2026-07-26 while gating PR #2916 (a review-fix commit landed 
into the worktree of a live full gate; the run was killed and left `RESULT: INCOMPLETE`, so the fail-safe held **by timing luck**). Before #2926 a completed run would have emitted `commit: <the fixer's sha> … RESULT: PASS` for a tree most components never compiled.

**What you see:**

```
tree-start: 4686c37a1b2c dirty: no  digest: 2ca89bd8f01e
tree-end:   116d0b9e77aa dirty: yes digest: 47f1c40355ab
tree-integrity: FAIL (tree-mutated-midrun; head 4686c37a1b2c→116d0b9e77aa; changed: cqlite-core/src/foo.rs docs/bar.md (+3 more); detected-after-component: clippy)
RESULT: FAIL
```

**Recovery: re-run on a stable tree.** There is nothing to "fix" in the gate — the FAIL is accurate and the run is unsalvageable, because component→file attribution does not exist (there is no way to know the already-run components were unaffected). The named line lists the changed paths; the retained manifests live at `<logs>/tree-identity.{start,end}` if you need the full set. Prevention: do not edit a worktree while its gate runs — park the fix until the gate reports, or run the fix round in a second worktree.

Notes:

- **No bypass** — no environment variable turns a mutated run green. `AGENT_GATE_TREE_HASH_CAP_BYTES` (default 8 MiB) is a *performance* knob capping content hashing of oversized *untracked* files only; any non-default value or fallback use is stamped as `tree-hash-cap:`.
- A detected mutation is a **verdict** (`RESULT: FAIL`), never the `INCOMPLETE` liveness placeholder, and it is `FAIL` even for an `--only` run that would otherwise be `PARTIAL`.
- Exclusions are the repo's own `.gitignore` rules (so all `target/**`, `*.log`, `.agent-gate-*summary.txt` churn is invisible) plus the run's own summary file when a caller pins a relative in-repo path. `docs/**`, `*.md`, `test-data/**` and `openspec/**` are deliberately NOT excluded. **Limitation**: gitignored *inputs* (the fetched `test-data/datasets/**` binaries) are outside the digest — the `datasets:` and `ci-pins:` stamps cover those.
- **Reading the `changed:` list.** It is space-joined, so paths are printed with the manifest's own backslash escapes: `\s` = a space, `\t` = a tab, `\n` = a newline, `\\` = a literal backslash. `changed: two\swords.txt` is ONE path, not two.
- One named non-fatal class: a `Cargo.lock`-**only** difference stamps `tree-integrity: PASS (lockfile-settled: …)` naming every settled lockfile, because the gate runs cargo without `--locked` (#2962 removes the need for this carve-out); a lockfile change alongside any other path is fatal.
- **The window starts when work starts.** The FULL gate (re-)captures its start identity immediately *after* `acquire_gate_slot` returns, so an edit made while it sat in `waiting for gate slot (N in use)…` — where it executed nothing and certified nothing — does not invalidate it. `--lite`/`--delta` never queue and certify from the capture taken before their first component. Once a slot is granted, every later edit is inside the window.
- **A capture that cannot be validated is a FAIL, not a SKIP.** Every capture validates its OWN manifest before anyone can compare it: the first record must be the `H<TAB><head>` header, the LAST must be an `N<TAB><count>` trailer, and the trailer's count must equal the records actually read back. So a failing hash tool, a short write, or a manifest truncated mid-file (e.g. `$TMPDIR` full during a long run) is rejected — a truncation cannot leave two captures sharing a byte-identical prefix that compares equal. The run reports `tree-integrity: FAIL (tree-capture-failed; …)`. Only "there is no git worktree at all" produces a `SKIP`, and only at the FIRST capture: a transient git failure at the slot-grant re-capture retains the pre-queue capture and keeps the guard armed rather than downgrading a live guard to `SKIP`.
- **`commit:`/`dirty:` come from the verified terminal capture**, not from a fresh `git rev-parse --short HEAD` / `git status --porcelain` at emit time — that emit-time read was the original #2926 defect (a HEAD move landing between the capture and the stamp certified a sha the guard never verified). A block can only ever name a sha a validated capture observed; with no validated capture it reads `commit: unverified … dirty: unverified` and the run is FAIL-closed.
- **A boundary FAIL block is a FULL block.** When the mutation is caught at a component boundary the run stops there, and the block it publishes carries the same provenance as any other terminal block — `commit:`, `datasets:`, `ci-pins:`, `accelerators:`, `cpu-budget:`, the tree lines, the verdicts of the components that had already recorded one, and `components-completed: N of M selected` — plus `detected-after-component:`. You never have to re-run to find out what the run was.
- Cost: ~30 ms per capture, i.e. ~1 s added to a 40–60 minute gate.
- **A mutation-detected block never stamps the post-mutation sha.** Whichever path catches it — a component boundary, a SIDE-lane marker, or the terminal capture — `commit:` names the VERIFIED START (the identity the run executed against) with an explicit `(VERIFIED START — …)` label, and the post-mutation reading sits on `tree-end:` with an explicit `(POST-MUTATION observation — …)` label. The labelling lives in one place, so the three paths cannot drift (#2926 review J1).
- **The run's own output is not a mutation.** Besides the summary file, the run's stdout/stderr redirect target is carved out when the platform can name it and it is a regular file inside the checkout — so `> gate-out.txt` inside the repo does not make the gate trip on its own log. Where the fd cannot be named (no `/proc`), nothing is excluded and the FAIL text says so.
- Self-tests, all three wired into `tooling-tests`: `scripts/tests/test_agent_gate_tree_integrity.sh` (the behavioural guard), `scripts/tests/test_agent_gate_tree_portability.sh` (the same guard under BSD/macOS `sed`/`stat`/`sort` shims, plus a lint — over a function inventory DERIVED from the gate — that FAILs on any GNU-only construct in the tree-integrity code; macOS is a first-class gate host) and `scripts/tests/test_agent_gate_tree_provenance.sh` (the labelling contract on every detection path, the boundary block's component table, and the stdout carve-out).

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

## The `oom-audit` component (issue #2012)

`oom-audit` is a SKIP-aware full-gate component that structurally audits the
codebase for the "never materialize an unbounded read" memory-safety invariant.
It runs the `xtask` crate's static AST audit — `cargo run -p xtask -- oom-audit
--enforce` — over a committed v1 scope (`cqlite-core/.../data_access/**`,
`cqlite-core/src/query/**`, and the `cqlite-flight` producers + `streaming.rs`).

- **Rule (v1): `STREAM_RETURNS_VEC`.** A `syn`-based (never regex) per-function,
  path-scoped rule that flags a `.collect::<Vec<..>>()` or a `Vec::push`/`extend`
  loop over a row/partition/cell iterator in a scan/producer function when no
  bound (`ResultBudget`, a `buffer_size`/`batch_size`/`limit`/`max_*` param, or a
  `.take(n)`) is in scope. It fires only when both the shape and the iterator
  element type are syntactically visible (favor false-negatives), so its residue
  is small and reviewable. Rules 2 & 3 (`UNBOUNDED_RANGE_READ`,
  `CLONE_IN_SCAN_CLOSURE`) and the wider surface are deferred follow-ups.
- **SKIP-aware (delivery-telemetry model):** no `cargo`, an absent `xtask` crate,
  or a failed `xtask` build → **SKIP** (loud, never a silent PASS); a clean build
  whose enforce run exits non-zero → **FAIL**; otherwise **PASS**. Not in
  `DATASET_COMPONENTS` (it reads source, needs no SSTable fixtures).
- **Suppression — the allowlist** (`xtask/oom-audit-allowlist.toml`): the ONLY
  way to suppress a finding. Each entry carries a content fingerprint (`f1:<hex>`,
  reformat-stable, changes when the code changes) plus a **mandatory non-empty
  `issue`** and **mandatory non-empty `justification`**. An entry whose
  fingerprint matches no current finding is **orphaned** and FAILs (the list
  cannot rot); `expiry` is **optional** and FAILs only when present-and-past (per
  design fork F-expiry — no mandatory wall-clock time bomb; review cadence is
  manual). The v1 seeded allowlist is empty (the report is clean over scope).
- **Self-test:** `scripts/tests/test_agent_gate_oom_audit.sh` (run inside the
  `tooling-tests` component) drives `agent-gate.sh --only oom-audit` to assert the
  SKIP/FAIL/PASS outcomes via the `OOM_AUDIT_XTASK_DIR` / `CQLITE_OOM_AUDIT_ROOT`
  test seams.

---

Back to [`CLAUDE.md`](../../CLAUDE.md).

## Gate mechanics moved verbatim from CLAUDE.md (#4092)

CLAUDE.md keeps the mode table, the required invocation and the merge-blocking rules.
Everything below is the depth behind them, byte-identical to what it replaced.

**Compiling a feature is not covering it (#1699).** The scoped clippy matrix enables ~30 cqlite-core features
at once under `--all-targets`, so a feature can be *test-compiled* on every full gate and have **executed
nothing** — and a combined feature set is exactly what MASKS cross-feature coupling (an item gated on feature
A referencing feature B's items compiles fine while both are on). Measured, not argued: turning EXECUTION on
for `legacy-heuristics` surfaced 4 tests that had never run once, two of which assert behaviour CQLite
deliberately does not support (#3372 five `not yet implemented` stubs behind the flag; #3374 filler-byte mock
`Statistics.db` plus pre-`na` `mc-` names); and `flight-tests` surfaced **14 cqlite-flight targets that
execute NOWHERE** — not locally, not in CI — because their module-level
`#![cfg(feature = "observability-testing")]` is off in every lane that runs them (#3375), a gap #2910's tier
aggregation cannot see because the tier *runs* and silently executes 0 tests. So when you add a feature flag,
ask which lane **executes** it, not which lane compiles it; if the answer is none, the feature is uncovered
however green the gate looks. `experimental` is **one** remaining instance (#3373) and NOT the only
one: in `cqlite-core` the crate-level-gated integration targets for `observability-testing` (14) are
named by no `--test` in the gate and execute ZERO tests at `core-tests`' feature set, as do 3 of the
5 `dhat-heap` ones (#3522 audit). **The `delta-scan` half of that census is CLOSED (#3725):**
`feature-iso-delta-scan` was `--lib --no-run`, so the 13 crate-level `delta-scan`-gated targets AND
the `delta_scan` module's own 39 lib tests executed in no gate component — the lib half only in the
`required`-exempt `ci.yml`. That lane now EXECUTES, at its own isolated feature set, with its target
set DERIVED at run time and a zero-tests guard, so those tests are gated locally.
**And the closure PAID FOR ITSELF ON THE FIRST RUN, which is the argument for doing this to the
remaining families rather than filing them:** executing the lane surfaced **2 of
`scan_delta_parity_test`'s 14 tests FAILING against the real corpus and PASSING against an absent
one** — a real defect (the per-cell `expires_at` comparison did not model
`JsonTransformer.serializeCell`'s suppression rule at `cassandra-5.0.8`) that had been latent for as
long as nothing executed the target. This is #1716's "expect latent failures the first time you
touch a long-unwired crate" at feature granularity: they are pre-existing, not yours, but they are
yours to fix because your diff is what runs them — so budget for them when you wire a lane, and
never fix one by weakening the assertion (the oracle is Cassandra's source at the pinned tag, never
CQLite's own output).
**AND "EXECUTED BY NO MERGE-GATING LANE" IS NOT "EXECUTES NOWHERE" (#3725).** Those 6
`issue_1007_complex_type_parity` cases DID execute, strictly (`CQLITE_REQUIRE_FIXTURES=1`, after a
Docker corpus regeneration), in `parity-regen-matrix.yml`'s `cql-type` leg — a lane exempt from
`required`. The defect was the GATING half. Getting that wording right is not pedantry: "executes
nowhere" invites adding a second executor, when the fix is to make an existing one gate.
**AND THE SAME REASONING RUNS AT PACKAGE GRANULARITY, WHICH IS WHERE IT WAS COSTLIEST (#3522).**
`cargo clippy --workspace --all-targets` compiles EVERY workspace member on every full gate, so a
whole CRATE can be built by every run and execute nothing — and it reads as covered precisely
because the workspace builds clean. Measured: `cqlite-ffi-common` appeared **zero times** in
`scripts/**` and `.github/workflows/**` (37 unit tests + `tests/dependency_boundary.rs` +
`tests/error_contract_table.rs`, executed by nothing anywhere), and `cqlite-node`'s 53 Rust unit
tests were in the same hole because `node-bindings` runs jest against the BUILT ARTIFACT and never
`cargo test`. Both now run in `binding-rust-tests`. Two design rules came out of it. **A
never-SKIPping lane must not be folded into a SKIP-aware one**: `node-bindings` correctly SKIPs
without node/npm, and putting cqlite-node's *Rust* tests behind that SKIP would be a coverage hole
wearing a SKIP's clothes — so the Rust lane depends on nothing beyond cargo and never SKIPs. **And
enrolling a lane in `DATASET_COMPONENTS` is not enough to stop a corpus-dependent suite skipping**:
the widened `node-bindings` also exports `CQLITE_REQUIRE_FIXTURES=1` on the full gate, which buys ONE
named setup failure instead of 14 separate `beforeAll` throws and closes `parity.test.js`'s `test.skip`
placeholder — the one corpus-conditional path in that suite that would pass silently. (An earlier draft
of this paragraph said those suites `describe.skip`; **measured, none does** — the repo's Node
convention THROWS. A false rationale in a gate log is worse than none, because it is what stops the
next person looking.) **The throwing helper was NAMED `skipIfNoDatasets()` until #3641** — a
skip-named function that could not skip, believed by design work until someone measured it (an empty
root gives `prepared.test.js` 16 FAILED of 16, not 16 skipped). It is now
`assertDatasetsAvailable()`, the three suites that had copied its body verbatim call it, and its
contract — throws on an absent corpus, in BOTH strict-mode directions, never skips — is asserted in
`bindings/node/__test__/helpers.test.js` rather than only described in a doc comment. **A name is a
claim about behaviour, and this repo's gate design consumed that claim**; the doc comment that said
otherwise sat two lines above the throw and did not travel to a single one of the 14 call sites. The durable question is the same one shape up: for each workspace member, **which component
EXECUTES it** — recorded, member by member, in `scripts/tests/workspace-test-disposition.txt`
(`EXECUTED`/`PARTIAL`/`NOT-EXECUTED`, a closed label set enforced under `tooling-tests`), so a new
crate cannot join the unexecuted set unannounced. Each record also carries a CLASS — `silent` (no
committed doctrine claims it is covered) vs `contradicts-doctrine` (doctrine says it is and it is not)
— coupled to the label (`EXECUTED` ⇔ `no-gap`), because a gap our own doctrine denies is a false
certification and not a backlog item. That census records completeness and labeling, **not
truth** — deliberately, on #1716's precedent.
Two corollaries the lanes are built on. **Derive, never curate**: both executing lanes compute their subject
set from committed source at run time — `legacy-heuristics` its `--test` targets (from cargo metadata plus a
module closure, so a manifest-gated or directory-style target is not missed) and its allowed-zero set, and
`flight-tests` its unit-target set from cargo metadata — so a new gated file is picked up and a feature joining
`default` shrinks the excusal set with no gate edit. A failed derivation is a FAIL naming the derivation, never
a fallback to "nothing enabled", which would silently excuse every gated target. **And a narrowed lane
DECLARES the narrowing at run time**: `flight-tests` prints what it does not execute on every run, because a
lane that omits coverage silently is indistinguishable from one that covers it — the same reason this whole
component set exists. `legacy-heuristics` declares a second, subtler narrowing the same way: a test target can
reach a child module through a cfg the derivation does not evaluate (`#[cfg(all(feature = …))] #[path = …] mod
support;` on a shared helper — 3 such targets in `cqlite-core` today), and the closure used to follow that child
while DISCARDING the attribute gating it, so a gated test inside counted as executable while an ungated sibling
kept the target non-zero and the co-required census reported **no gap**. Such a subtree is now reported as a
`DECLARED GAP` with a `cfg-gated-subtree gaps: N RECOGNISED` census line that states its own non-exhaustiveness and is affirmative at `0` — **`0 RECOGNISED`, never a bare `0`**, because a bare zero in a gate log reads as a verified all-clear from a scan that is documented as incomplete. Deliberately **declared, not
fatal**: failing the lane on it was tried and reverted, because those helpers are correct code and **a lane that
reds on correct input is the lane agents learn to waive**. The `UNRESOLVED` half stays fail-closed — an
incomplete source set is permissive everywhere, an unevaluated one is merely unattributable. And
**a lane in `--list` is not a lane that works**: `feature-iso-parquet` reports `PASS (0s)` warm, so presence
proves nothing. `scripts/tests/test_agent_gate_feature_matrix_lanes.sh` (opt-in) plants each lane's
incident-class break in a throwaway `git worktree` and requires the lane to red **and** to NAME the planted
symbol — a bare red is not evidence either, since an unrelated breakage produces an identical exit code and
SUMMARY line. `scripts/tests/test_agent_gate_binding_rust_lanes.sh` does the same for the #3522 binding
lanes, and adds the case the failing-assertion plants cannot reach: one that cfg's a unit suite OUT, so it
compiles, runs **zero** tests and exits 0 — the only plant that exercises the non-zero-count half of
`check_unittest_targets_ran`.

**AND THE SAME QUESTION APPLIES TO A LINT'S SUBJECT SET, WHICH IS WHERE IT WAS HARDEST TO SEE
(#3756).** A gate component's subject set is a package or a target; a LINT's is a LIST OF FILES,
and an enumerated list that honestly declares its own non-exhaustiveness is **honest without
being coverage**. Measured: `scripts/tests/test_roborev_guard_portability.sh` has carried the
rule *"`xargs -r` (and GNU long options) are not in BSD xargs"* verbatim since #3296, and
`xargs -0 -r` still shipped in `scripts/tests/test_bootstrap_agent_machine.sh`'s tree-identity
digest — the worst available place, since a silent BSD failure inside an INTEGRITY digest made
an edited untracked file report `STABLE` — because neither that lint nor its sibling
`test_agent_gate_tree_portability.sh` named either bootstrap file (**0 references in either**).
A human reviewer caught it; the lint that already knew the rule never looked. **A FULL
DERIVATION WAS MEASURED AND REJECTED, NOT ASSUMED AWAY**: sweeping all 167 tracked
`scripts/**/*.sh` reds 10 of 15 rules across ~40 sites — mostly OTHER portability lints' own
rule TABLES and deliberate GNU-first/BSD-fallback pairs — a cross-cutting cleanup with its own
review surface that would red `roborev-lints` in every lane's `--lite`. So the set stays
ENUMERATED and **DECLARES ITSELF AT RUN TIME**, the same move `flight-tests` makes: every
scanned path is printed beside an affirmatively MEASURED `unscanned: N of M tracked
scripts/**/*.sh` line, which reads `NOT MEASURED` — never a number — when the census cannot be
taken, because a number in a scope declaration reads as authority. Two rules fall out.
**MEMBERSHIP IS NOT DETECTION**: adding a file to the list proves it reaches `grep`, not that
the incident's own construct would be caught in it, so each newly-scanned file carries a control
that PLANTS the construct into a throwaway copy and requires the scan to **NAME** the planted
line — in a 3000-line file a bare red is produced identically by an unrelated rule. And
**WIDENING A LINT FINDS THINGS, WHICH IS THE POINT AND ALSO THE COST**: the two bootstrap files
yielded a real macOS defect (`readlink -f` behind a `|| echo` fallback that made a symlinked
cargo config get REPLACED by a plain file) and one rule FALSE POSITIVE — `timeout[[:space:]]+[0-9]`
matched the `2` of `command -v timeout 2>/dev/null`, i.e. the rule red on **the very guard its own
message recommends**, at three real call sites. Fix the rule, never the caller, and pin both
directions: a false-positive fix that also loses the true positive is not a fix.

**A CI exemption that defers to a local gate component is only as true as that component's SCOPE
(#3493).** `.github/ci-gating-tiers.yml` excuses a workflow from `required` by naming the local
component that supposedly owns its merge-gating half — and nothing checks that the named component
actually covers it. Measured instance, since FIXED by #3522/#3574: the `node-ci.yml` exemption read
*"the merge-gating half is the local gate's node-bindings component"* while `node-bindings` ran ONE
of the Node suite's 27 test files (`npx jest write-readback-content`, narrowed for speed under
#1255), so **26 files were gated by neither side** — and a deterministic export-surface red sat on
`main` for ~2 days across 4 Node contexts without blocking a merge. Its sibling is the control:
`python-bindings` runs the whole pytest suite, so the identically-worded Python exemption was true.
This is the **circular-deferral** shape #3544 records for `ci-minimal-features.yml` — each side's
coverage justified by the other's, the content exercised by neither, **with a documented rationale
on both sides explaining why that is fine** — and it is a confirmed family, not a one-off. Two rules
follow. **Narrowing a component for speed is a CHANGE TO A MERGE GATE**: if a registry exemption
names it, correct that entry in the same diff or the exemption silently becomes false. And **when
you widen or narrow, measure first** — the #1255 narrowing outlived its own premise (the widened
component measures **138s**, dominated by the `release-unwind` LTO build it already paid), so the
speed argument that justified the hole had stopped being true long before anyone re-checked it.
That figure is the POST-REBASE build of PR #3555, the one that merged; **133s** appears in that
PR's body and is the same measurement taken on its PRE-REBASE build. Two real measurements of two
builds, not a discrepancy — cite the number with the build it describes, and do not average them
or quote one as the other (#3642).

**THIRD CONFIRMED INSTANCE, AND IT WAS FOUND WITHOUT LOOKING FOR IT (#3725) — SO THE DEFERRAL MUST
NAME A LANE THAT BOTH EXECUTES AND GATES.** `test-data/scripts/smoke-test-all-tables.sh` skips
`test_types` from the smoke sweep on the stated ground that those keyspaces are *"validated by
dedicated Rust parity tests (tombstone/TTL + CQL-type)"* — and **every test it names**
(`issue_1007`/`1003`/`1006`/`1008` for CQL-type, `issue_1010`/`1011` for tombstone/TTL) was
crate-level-gated on `delta-scan` and executed by no merge-gating lane. A closed, empty loop, with an
honest and specific rationale on each side. Note what was NOT wrong: the smoke skip itself is
CORRECT and was left alone (those keyspaces genuinely hold partitions with zero live rows, which its
"must emit ≥1 entry" check would mis-flag) — what was wrong is that its deferral had no gating
target. Three instances now say the same thing: **a deferral rationale gets written once and never
re-checked**, so when you point coverage at another lane, name it and verify that lane EXECUTES the
content AND gates the merge. Neither half alone is coverage.

**AN EXEMPTION NOW DECLARES ITS MERGE-GATING HALF IN A MACHINE-CHECKED FIELD, AND THAT CHECKS
EXISTENCE, NOT SCOPE (#3725).** Every `.github/ci-gating-tiers.yml` `exempt:` entry carries a
structured declaration of what gates the merge in its place, under a closed grammar, and a named
local gate component must EXIST in `scripts/agent-gate.components` — so an exemption deferring to a
component that was renamed, deleted or never existed is caught. **It deliberately does not claim
more than that**: SCOPE is what #3493 showed to be the actual hazard (`node-bindings` existed and ran
1 of 27 files) and it is not decidable from the registry, so the check is stated as an existence
check and nothing reads it as a coverage proof. The prose `reason` is **NOT** machine-checked, by
design — a recogniser over author-controlled prose never closes (#3312) — so the structured field is
the control and the prose's truthfulness is a **DECLARED RESIDUAL**. Two measurements shaped this and
are worth carrying: **all 23 exempt workflows ARE `pull_request`-triggered**, so a stored
`pr_triggered` boolean would have been constant across every entry AND a second source of truth that
can drift from the workflow — *derive it, do not store it* (#3544's "remove the second source rather
than reconcile it"); and the enrolment rule ALREADY catches a new PR trigger on any workflow absent
from the registry, so that half needed no new code and writing one would have been a guard with an
empty subject set, which greens vacuously.
**A `rescue` COLLAPSES "CANNOT TELL" ONTO THE PERMISSIVE ANSWER JUST LIKE A `[` PREDICATE (#3725).**
Measured, on `main`, in a merge-gating validator: `load_workflows` mapped a `Psych::SyntaxError` to
`{}`, so an unparseable **`pull_request`-triggered** workflow had no triggers, failed
`pull_request_workflow?`, and ESCAPED the enrolment rule entirely — `policy_errors` reported the tree
CLEAN (`TOTAL_ERRORS=1` well-formed vs **`0`** unparseable). The two-valued-predicate rule is not
about bash: any construct that turns an unreadable input into a value — a `rescue`, a `||`, a
`${VAR:-default}`, an `.ok()` — picks the permissive branch unless you make it three-valued and NAME
the input it could not read. Ask it of every `rescue` that returns a default.

**AND "DOES EVERY TEST RUN" IS NOT "IS THE CORPUS COMPLETE" (#3493).** #3522's census answers the
first; it cannot answer the second, and neither can its per-suite guard. The Node parity cases
**derive their table set FROM DISK**, so a partial extraction is green BY OMISSION: every suite
runs, every suite does real work, and the missing tables are simply never enumerated. Hence
`test-data/scripts/check-dataset-manifest.sh`, paired with `npm test` in `node-bindings`, asserting
that every expected table is present AND usable. Measured against the real binding, on an otherwise
intact generation: a **zero-length `CompressionInfo.db` or `Statistics.db` makes `SELECT` return 0
ROWS silently** (not an error — the "0-rows-when-present" failure this repo says must never pass),
and a second generation whose `Data.db` is well-formed garbage makes the reader throw. A
completeness check proves files are present and usable AS FILES; it cannot prove they parse — that
is the reader's job, and no amount of `stat`ing substitutes for it.

**A GREEN FULL GATE DOES NOT SUBSUME `pr-gate-core` (#3453).** The two check sets overlap; neither
contains the other, and this is structural, not a backlog item. The gate runs lanes CI cannot
(`arrow-parity-guard` names a `#![cfg(feature = "arrow")]` integration target that pr-gate's `--lib
--all-features` compiles no path to), and pr-gate runs a lane the gate does not: `cargo test -p
cqlite-core --lib --all-features` EXECUTES cqlite-core's unit suite with the OTLP stack ON, which no
gate component executes. **MEASURED ON `main`, NOT CITED FROM AN INCIDENT: the gate of record
DISCOVERS 3562 cqlite-core `--lib` tests (`--features cli-helpers`); pr-gate-core discovers 3782
(`--all-features`) — so 220 lib tests execute in CI and NOWHERE in the gate of record.** #3382's own
fix pin (`a_stats_only_name_cannot_create_an_instrument_through_the_emit_path`) is one the gate cannot
even list (`-- --list` finds 0 vs 1). That is how PR #3382 earned a 31/31 gate PASS without executing
the test pinning its own fix — the issue was filed around one instance; the standing gap is 220 tests
wide. `all-features-check` now closes the **compile/lint half** — a type error or a
`-D warnings` lint under `#[cfg(feature = "observability")]` reds the gate of record — and
**deliberately not the runtime half**: it executes NONE of those 220, so an order-dependent defect like #3382's
(a process-wide `OnceLock<Instruments>` poisoned by whichever test binds the global meter to a no-op
provider first, invisible to `#[serial_test::serial]` grouping) STILL fails only in CI. Note the
tests in question are gated on `observability-testing`, not `observability`. So never read a green
SUMMARY as a prediction about `required`; a red CI check on a green-gate PR is an ordinary event.

**Required invocation — summary-file redirect, never raw stdout (issues #1175/#2079), full AND lite:**

```bash
AGENT_GATE_SUMMARY_FILE=/tmp/gate-summary.txt bash scripts/agent-gate.sh > gate.log 2>&1 < /dev/null
cat /tmp/gate-summary.txt   # the SUMMARY block is the ONLY gate text an agent retains; NEVER read gate.log
```

- Prefer `run_in_background` (or a long timeout) so a subagent never idle-waits and gets
  watchdog-killed (#1855). A queued gate ≠ hung gate: under load it prints `waiting for gate slot`.
- **COMPLETION AND VERDICT ARE TWO ASSERTIONS, AND #3750 IS WHAT HAPPENS WHEN ONE TOKEN IS ASKED TO
  ANSWER BOTH.** `INCOMPLETE` is a liveness placeholder, NOT a verdict (#3041; mechanism follow-up
  #2908): the gate writes `RESULT: INCOMPLETE (gate did not finish)` into the summary file **at
  launch** (EXIT-trap sentinel, before the #1825 slot is even granted) and only overwrites it at the
  terminal emit. So a bare `grep -q` on the bare `RESULT:` token fires the instant the gate starts and
  would let an agent accept a **just-launched or still-queued** gate as its gate of record — a verdict
  that does not exist. A sentinel-only summary means "still running, died, or queued", never certified.
  **But the corrected probe was published as `grep -qE 'RESULT: (PASS|FAIL)'` for every mode, and
  `--only` demotes a SUCCESSFUL run to `RESULT: PARTIAL`** (`agent-gate.sh`, deliberately — a component
  probe must never be pastable as the gate of record). **The documented probe was therefore asymmetric:
  it terminated on failure and SPUN FOREVER ON SUCCESS.** Measured: a lane spun 8+ minutes past a
  terminal PASS and then re-ran an 18-minute component that had already passed. Three rules:

  **(1) COMPLETION — the EXIT STATUS IS PRIMARY, the text grammar is the FALLBACK.** *If you can
  observe an exit status at all, the run has completed* — that is what an exit status means, and a probe
  keyed on it cannot be defeated by a wording change. `--only` exits **3** (`PARTIAL`), a full-gate PASS
  exits `0`, anything else `1`. Only where the exit status is unobservable (a **detached** run, a peer's
  run, `gate-detached.sh`) do you poll text — and then the accepted set is **A PARAMETER OF THE RUN
  MODE**, never one grammar for both:

  ```bash
  # RECORD grammar — full / --lite. MUST keep REFUSING PARTIAL (and ERROR and REFUSED).
  grep -qE '^RESULT: (PASS|FAIL)([[:space:]]|$)'                            "$AGENT_GATE_SUMMARY_FILE"
  # ONLY grammar — `--only <component>` ONLY. NEVER use this on the gate of record.
  grep -qE '^RESULT: (PASS|FAIL|PARTIAL)([[:space:]]|$)'                    "$AGENT_GATE_SUMMARY_FILE"
  # DELTA grammar — `--delta <anchor>` ONLY. It alone can terminate ERROR or REFUSED.
  grep -qE '^RESULT: (PASS|FAIL|PARTIAL|ERROR|REFUSED)([[:space:]]|$)'      "$AGENT_GATE_SUMMARY_FILE"
  ```

  **THREE MODES, THREE SETS — and `--delta` is the one that bites.** `run_delta` can terminate with
  `ERROR` (4 emit sites) or `REFUSED` (3 more, reached via `emit_summary "$(_tree_result REFUSED)"`,
  which is why grepping for `emit_summary REFUSED` finds nothing and the token *looks* unemitted — it
  **is** emitted, and `gate-liveness.sh`'s comment enumerating it is accurate, not stale). All seven
  sites are inside `run_delta`, and a full gate emits only `PASS`/`FAIL` — so a `--delta` poller using
  the RECORD grammar **hangs forever on a terminal outcome**, #3750's own defect in a third mode.
  Record therefore stays exactly `PASS|FAIL`: widening it would weaken the gate-of-record probe for
  nothing, and that refusal is load-bearing (AC4). The delta set is **`gate-liveness.sh`'s
  already-enumerated terminal set, token for token** — ONE source of truth for "what is terminal", not
  a second list — so it carries `PARTIAL` (which `--delta` cannot emit; that is the `--only` demotion)
  and the reader's defensive `REFUSED`, with `ERROR` the emit you will actually meet. Better than any
  of the three: **ask the reader**, which is that one source of truth executable rather than
  transcribed.

  **Widening a COMPLETION grammar is safe here and would not have been before**: matching
  `ERROR`/`REFUSED` cannot create a false pass because completion and verdict are now separate
  assertions (rule 2), so this fix is *enabled* by the split it was a finding against — which is why
  three completion grammars are not three chances to be wrong.

  All three **ANCHORED and token-terminated**, because unanchored the first matches `RESULT: PASSENGER`,
  the second `RESULT: PARTIALLY` and the third `RESULT: ERRORS` — a spelling check masquerading as a
  state check, the `PASS*` accepts `PASSthisNeverRan` defect this repo has now made three times. Better than either: ask the shared
  reader, `bash scripts/gate-liveness.sh <summary-file> --run-id <id>` (exit 0 = COMPLETE), which
  enumerates the terminal set from `agent-gate.sh`, requires the block's END marker and enforces the
  #2874 run-id binding. **One implementation, one grammar** — a caller that re-greps it is a second
  place for all three to drift (roborev job 172 is that defect, already paid for once).

  **(2) VERDICT — read the COMPONENT'S OWN LINE, never the terminal token.** `PARTIAL` says *the run
  ended*; it does not say *my component passed*, and `PARTIAL` is **true**, so it keeps being emitted.
  Deriving success from it is a positive verdict taken from a completion marker — the vacuous pass, one
  level down from the hang. Ask:

  ```bash
  bash scripts/gate-component-verdict.sh "$SUM" --mode only --component tooling-tests --run-id <id>
  # exit 0 = PASS | 1 = NOT-PASS | 4 = COULD-NOT-MEASURE (no verdict available, whatever the
  #                 reason) | 64 = USAGE
  ```

  **THIS IS NOT A COMPLETION PROBE AND IT HAS NO OPINION ABOUT LIVENESS — NEVER CALL IT IN A LOOP.**
  Establish completion FIRST (rule 1: the exit status, else `gate-liveness.sh`, which is the
  three-valued liveness authority and the only one of the two that may be polled), then ask this once.
  A retryability taxonomy was tried here and **DESCOPED** (#3750 round 2): a second exit code for
  "still running" produced three findings in one review round, and the harmful one could not be
  patched — `--no-wait` makes the reader's `STALLED` unreachable, so a live gate whose beat is merely
  STALE arrives as its `UNKNOWN` code and was reported as permanent. A lane obeying that **relaunches a
  live gate: two gates on one summary path.** So the tool makes only the binary distinction it can
  support and quotes the reader's cause verbatim. Subtraction cannot introduce a false PASS.

  A completed run whose component **SKIPped or is ABSENT is NOT a pass**: a SKIP means the check never
  ran, which is the vacuous pass itself — and note a SKIPping component still leaves `RESULT: PARTIAL`
  and **exit 3**, so exit 3 alone is a completion signal and never a green. `COULD-NOT-MEASURE` is
  never read as a pass. It also refuses to answer about a LITE/DELTA block, requires the block's
  `tree-integrity:` token to be `PASS` (a mutated-mid-run run is non-certifying, #2926, and that
  invalidates every component in the block — unlike a *sibling* component's FAIL, which says nothing
  about yours), and bounds every read to the validated block, because the shared reader's framing
  check constrains counts and ordering and NOT that no stale lines sit outside the span. `--mode record`/`lite`/`delta` are **named refusals** naming their authority
  (`scripts/flow/premerge-assert.sh` owns the gate-of-record grammar, binds the certified sha and
  refuses `PARTIAL` token-exactly), so the component grammar can never be misused as a certification.

  **(3) EVERY POLL SITE SAYS WHICH GRAMMAR IT USES.** A reader must be able to tell a record poll from
  an `--only` poll at a glance, or the wrong one gets copied — which is exactly how the single published
  string reached the fleet. Guard: `scripts/tests/test_gate_component_verdict.sh` (in `tooling-tests`)
  runs both published strings against real fixtures, so a grammar that stops behaving as documented reds
  the gate instead of hanging a lane.
- **A gate launched in-session dies with its session's CGROUP, and no detach idiom saves it — run it
  with `scripts/flow/gate-detached.sh` and poll `scripts/gate-liveness.sh` (#3473).** Every process an
  agent session spawns inherits the session's `tmux-spawn-<uuid>.scope`, which carries
  `KillMode=control-group` + `SendSIGKILL=yes`: stopping it signals **every task in the cgroup**.
  Cgroup membership is inherited across `fork` and **cannot** be shed by `nohup`, `setsid`, closing
  fds or being reparented to init — measured, both directions, on an equivalent cgroup, where the
  victim died leaving **no signal record at all** (the field symptom of a traceless kill). A subagent
  gets its OWN pane scope. **What is NOT true, and was tested: an agent FINISHING does not tear its
  scope down** — a killed subagent's tickers kept running, orphaned, because systemd releases a scope
  only when its LAST process exits, so a long gate holds its own scope open and outlives the agent
  that launched it. The exposure is to pane/session teardowns (a supervisor recycle, `kill-pane`,
  logout), not to your turn ending. **#3473's "~10 minute ceiling" does not exist**: six instrumented
  tickers (plain `nohup`, `setsid`, renamed argv, harness-background, and two launched by a subagent)
  each ran the full **2400s with zero signals** and self-terminated. The 600s stall watchdog is
  **untested, NOT cleared** — the attempt to induce a stall failed, because this harness version
  **backgrounds** an over-timeout foreground call instead of killing it (the blocker completed
  unmolested, exit 0, after its full 700s). The cgroup mechanism explains
  the lead's `ssh` + `nohup` control completing on the same box and sha (an ssh login gets its own
  `session-N.scope`), but **AC2 landed as a PARTIAL: a sufficient, demonstrated mechanism with
  alternatives ruled out — NOT a confirmed diagnosis** of the original deaths, whose correlation with
  ~10 minutes nothing measured here explains. So **"lanes cannot run a full gate" is RETRACTED** — a
  lane can, detached. `gate-detached.sh` forwards the caller's environment **except the three
  build-flag variables it deliberately drops** — `RUSTFLAGS`, `CARGO_ENCODED_RUSTFLAGS`,
  `RUSTDOCFLAGS`, named in the launch banner's `SKIPPED` list, because `systemd-run --scope`
  inherits the caller's shell and a non-empty `RUSTFLAGS` SUPPRESSES cargo's managed
  `target.rustflags` block, so a lane that exported it once poisons every detached gate it starts
  (that contamination reddened a clean tree and halted the fleet on a P0 that did not exist).
  Everything else is carried across (a transient systemd unit inherits **none** of it, and an
  allowlist of remembered variables fails silently), and it **refuses with exit 69** where it
  cannot deliver a separate cgroup, rather than falling back to a session-scoped launch the caller
  would believe was protected.
  **And the killed-vs-running ambiguity is now answerable without `ps` on the box**: the gate beats
  `<summary-file>.heartbeat` every 20s for as long as it lives (the startup sentinel names the path),
  and `gate-liveness.sh <summary-file> [--run-id <id>]` reports `COMPLETE`(0) / `RUNNING`(2) /
  `STALLED`(3) / `UNKNOWN`(4). Pass `--run-id` whenever you know it — a peer's beat in the same
  checkout otherwise answers about the peer's gate (#2874). A **missing** beat is `UNKNOWN`, never
  `STALLED`, and there is deliberately **no env var** to widen the staleness window or disable the
  beat. **`STALLED` means "no liveness published", NOT "the process is dead"** — a death claim
  (`REAPED`) was built and then DESCOPED after four review rounds each found another way it was
  unsound (a beater can die under a live gate; the reader's `/proc` is not the gate's; two boxes can
  share a hostname), because proving a process dead means proving a negative about a machine you may
  not be on. The replacement needs no process inspection and so is correct on every host, macOS
  included: the gate relaunches its beater at each component boundary, so a live gate whose beater
  alone died recovers to `RUNNING` within one component — re-read before acting, and treat a
  still-`STALLED` run as gone only after **longer than the LONGEST COMPONENT OF YOUR OWN RUN,
  derived from the component table in your own SUMMARY** — never from a constant in prose. The
  figure previously written here, "~850s", was understated by 2.4x (`tooling-tests` measured
  **2073s** on #3473's gate of record #4), and acting on an understated bound makes a closer declare
  a LIVE gate gone and relaunch it, putting two gates on one summary path.
  Full record: `docs/development/lane-gate-execution.md`.
- **A GENUINELY PROSE diff cannot change the compiled binary — so a test failure in its full gate
  is BY DEFINITION pre-existing on `main` or a flake, and the correct response is CITE-AND-WAIVE
  (#3042).** The waiver's precondition is that the diff touches no compiled input (no `src`, no
  `Cargo.*`, no build script, no workflow, no test-data). **That qualifier is a path-shape test, and a
  path shape is not evidence — DON'T JUDGE IT, RUN IT (#3250):**
  ```bash
  git diff --name-only origin/main...HEAD | bash scripts/ci/classify-docs-only.sh   # exit 0 = prose
  ```
  A non-zero exit means the waiver does not apply, full stop. **The falsifying case is not
  hypothetical**: this repository ships measurement harnesses under `docs/reports/*-artifacts/` **by
  convention**, so a #3222-shaped diff contains `src/main.rs` **and** `Cargo.toml` under
  `docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness/` — it satisfies "no `src`, no `Cargo.*`"
  **textually** while being false **materially**, and an agent correctly following the old wording
  waives a red that is genuinely theirs. (Read "docs-only" here the same way roborev doctrine does — a
  **code-free census**, never a `docs/` path prefix; the classifier above is the gate-side spelling of
  the same idea, and `scripts/tests/test_classify_docs_only.sh` pins it.) **NEVER patch source to turn such a
  gate green** — that is a real change smuggled in under a docs diff, certified by nothing, and it
  masks the actual main-red. Instead: (1) confirm the diff really is non-compiling-input, with the
  classifier above rather than by eye; (2) identify the failure as a known main-red issue or a
  known flake — reproduce it on a clean `origin/main` checkout if it is not already filed, and FILE it
  if it is not; (3) record the waiver in the PR body naming the failing component and the issue number
  it belongs to. A waiver with no cited issue is not a waiver — it is an unexplained red. Conversely,
  if ANY compiled input is in the diff the waiver is void: the failure is presumed yours until proven
  otherwise.
- Defaults if `AGENT_GATE_SUMMARY_FILE` unset (per-checkout; give concurrent gates in ONE checkout
  unique paths): `.agent-gate-summary.txt` / `.agent-gate-lite-summary.txt` / `.agent-gate-delta-summary.txt`.
  **Nested exception (#2874):** a gate started with `AGENT_GATE_PARENT_RUN_ID` in its env (i.e. spawned
  by an enclosing gate) and no explicit `AGENT_GATE_SUMMARY_FILE` defaults to its OWN
  `$LOG_DIR/summary-primary.txt` (never the checkout default) and stamps `nested-under: <parent-run-id>`, so a
  nested/self-test sub-gate can never clobber the parent's summary. A mid-run summary clobber (foreign
  run-id) is caught at the next component boundary — and at the terminal emit — with a named
  `summary-integrity: FAIL` line + `RESULT: FAIL`, never a bare INCOMPLETE. **No-clobber + reader
  contract (#2874):** when the contended path is found holding a FOREIGN `run-id` (a live peer owns
  it) the gate does NOT rewrite that path; it publishes its own FAIL verdict to a non-clobbering
  sibling `<summary-file>.integrity-fail.<run-id>` + the `logs:` bundle (+ stdout/stderr) and exits
  non-zero, deliberately leaving the peer's block on the pinned path. A reader therefore MUST treat
  the process EXIT CODE as primary and MUST verify the `run-id:` line matches the run it launched
  before trusting a pinned-path block — a mismatched/foreign `run-id` block (even `RESULT: PASS`) is a
  peer's, not yours; on a mismatch, read the `.integrity-fail.<run-id>` sibling / `logs:` bundle instead.
- **A gate parser must be colour-immune AT THE PARSE SITE (#3400).** 18 workflows set
  `CARGO_TERM_COLOR: always` (incl. the nightly `gate.yml`) plus `scripts/local/pre-merge.sh`, and
  **colour SURVIVES redirection to a file** (measured: 25 ESC bytes vs 0) — the gate's own mandated
  `> gate.log 2>&1` capture is coloured too, so this is not a tty-only artifact. Cargo colours the
  STATUS WORD and emits the reset immediately after it (`Running<ESC>[0m tests/foo.rs`), so a
  pattern anchored on the status word alone survives while one spanning `<status> <payload>` — the
  literal `Running tests/`, or `warning:` — matches NOTHING. **It breaks BOTH ways, and neither is
  safe**: the cli-tests zero-tests guard reported OK having judged no target at all (a vacuous PASS,
  live on `main` for months, fixed by #1699); the declared-vs-observed reconciliation reported EVERY
  declared target unobserved on a healthy run (a false RED, fixed by #3400). Conversely
  `test result:` / `running N tests` are libtest's, and cargo does not pass `--color` through to the
  harness, so they carry no escapes — safe for a reason that is NOT in the code, which is why this
  is a lint and not a comment. Route every cargo-output parse
  through `_ansi_stripped_log` and read by **redirection, never a pipe** (a piped `while read` runs
  in a subshell and its verdict is discarded — a second, independent silent pass). **This rule is
  DOCTRINE and is NOT mechanically enforced.** A structural lint over the parse sites was built on
  #3400 and **descoped**: its own false-PASS count rose across review rounds (2, 2, 3) and two of
  the last round's three defects were inside the two preceding fix rounds — the same shape, and the
  same ruling, as #3229's removed `census-exclusion:` key, because a guard with known documented
  false-PASSes is worse than no guard, since it invites reliance it cannot support. Mechanization is
  deferred to **#3499**; until it lands, this is a review-time rule, and the standing coverage is
  behavioural (`scripts/tests/test_cargo_output_parsers.sh`, in `tooling-tests`), which pins the
  defect against real code rather than predicting it from source shape — it EXTRACTS each guard from
  the shipped `agent-gate.sh` and runs it, so unrouting one reds the suite instead of greening it.
  `CARGO_TERM_COLOR=never` at the invocation is belt, not the fix; `gate.yml` KEEPS
  `always` — colour is a presentation property of a log for humans, and moving correctness into a
  workflow file 18 files from the parse is a worse coupling than the one being removed.
- clippy is scoped per-package (#1844): whole workspace `-D warnings` but skips the source-built
  DuckDB amalgamation (cqlite-cli `duckdb-tests`) + OTel stack (`observability`/
  `observability-testing`); parquet/arrow stay linted. `CQLITE_CLIPPY_FULL=1` (nightly `gate.yml`)
  runs the full matrix.
- The FULL gate FAILs CLOSED on **either half** of the fixture contract; `--lite`/`--only` stay
  lenient for both.
  - Fetched corpus absent (#2078): `missing-fixtures: FAIL-CLOSED (#2078)`, remedy
    `bash test-data/scripts/fetch-datasets.sh`; `AGENT_GATE_ALLOW_MISSING_FIXTURES=1` opts out
    visibly (`missing-fixtures: OPT-OUT (...)`).
  - Committed CQL schemas unreachable (#3148): `missing-schemas: FAIL-CLOSED (#3148)` — textually
    distinct from #2078's marker, with two causes, an unreadable `test-data/schemas/*.cql` or a
    **rejected relative `CQLITE_SCHEMAS_ROOT`**, each carrying its own remedy line. Success stamps a
    positive `schemas: N/N canonical .cql readable under <root> (<source>)` line, so a pasted SUMMARY
    shows the check RAN. **There is deliberately NO opt-out env var, and none may be added**:
    committed source in a checkout is never legitimately absent, so an escape hatch could only buy a
    vacuous green.
- **A gate script BEHIND `origin/main` cannot certify (#3544).** `agent-gate.sh` is read from
  the tree under test, so a branch cut before a component-set expansion runs the OLD script and
  reports a true `N/N nonpass=0` while being **silent about every component added since**
  (measured: PR #3467's gate would have certified 31 of 35). Merge-cleanliness cannot see it
  (`git merge-tree` returns CLEAN — the skew is semantic), and `required` cannot backstop it:
  `.github/ci-gating-tiers.yml` exempts the CI feature-matrix lane *because the local gate owns
  it*, so each side's coverage is justified by the other's and the component is exercised by
  neither. At the mode dispatch — before the #1825 slot and any component — every mode compares
  its component **SET** (never a line count or blob hash) against a
  baseline **fetched in that same invocation** (a remote-tracking ref is a *cached observable*;
  a stale one returns "no skew" against a superseded `main`), and stamps `component-set:` into
  every SUMMARY: `PASS (36/36 vs origin/main <sha40> via the committed manifest)` — affirmative,
  **naming its baseline sha AND how the baseline was read**; `FAIL-CLOSED (#3544) — this tree is BEHIND …; MISSING: <names>` (remedy: `git fetch
  origin && git rebase origin/main`); `DECLARED (#3544) — this branch REMOVES <names>` when
  `origin/main` IS an ancestor of `HEAD` **AND the components are absent at `HEAD` too** — **loud,
  not fatal**, because the author has nothing to rebase and a guard that reds on correct input
  is the guard agents learn to waive. **ANCESTRY ALONE IS NOT PROVENANCE, and trusting it was a
  reproduced false PASS**: `is origin/main reachable from HEAD?` is not `did this branch's
  committed diff remove the component?`, so deleting one component from the WORKING COPY alone
  yielded a non-fatal `DECLARED` in a certifying mode (a full gate would have certified 35 of 36)
  under a line that asserted committed provenance for an uncommitted edit. A removal still
  PRESENT at `HEAD` is therefore its own fail-closed `UNCOMMITTED` verdict (remedy: commit or
  restore — never rebase), measured against `HEAD`'s OWN component set rather than the proxy "is
  the tree dirty" (which would red every mid-edit branch and still prove nothing on a clean-but-
  stale one); an **uncommitted ADDITION still PASSes**, because extra components are never skew.
  **A CHECK MUST BE INSIDE THE WINDOW IT CERTIFIES — NOT BEFORE IT, NOT AFTER THE HARM (roborev
  job 290).** The mirror of this issue's earlier ruling ("a check placed AFTER the harmful effect
  can only report it"), and the same family as the two sha-equality failures. The component-set
  pre-flight ran BEFORE `acquire_gate_slot`, and `_tree_recapture_after_slot` then RESET the
  certification window to the tree present when the slot was granted — so an edit made WHILE QUEUED
  became the new starting tree under a STALE `component-set:` verdict. The recapture is deliberate
  and stays; the pre-flight is **repeated inside the window** (the earlier call is kept, because it
  is what stops an uncertifiable run from queueing or compiling at all). **Second half, one
  asymmetry down:** the LOCAL manifest is verified against the LOCAL declaration every run, so it is
  a checked claim — while HEAD's manifest was TRUSTED, letting a stale one at HEAD excuse an
  uncommitted removal as `DECLARED`. Provenance now reads HEAD's committed `COMPONENTS`
  **declaration** as data and does not consult HEAD's manifest at all: **remove the second source
  rather than reconcile it.**
  **AND EVERY INPUT THE CHECK REASONS ABOUT MUST BE INSIDE THAT WINDOW TOO (roborev jobs 292–294) —
  BUT "IS THE CODE I AM EXECUTING THE CODE I CERTIFY" IS NOT ANSWERABLE FROM INSIDE THE RUNNING
  PROCESS, AND IS SPLIT OUT TO #3705.** Being inside the window yourself is not enough if you compare
  against a snapshot taken outside it: `COMPONENTS` is an array bash loaded before the queue, so a
  script that GAINED a component while queued was validated against the OLD array (292); and change a
  component's **executor function** while queued and the recaptured tree becomes the certification
  window while the process keeps running the definitions it loaded before it (293). **The RULE stands.
  The MECHANISM built for it does not**: a whole-file startup digest of `$GATE_SELF` (with the field
  comparisons demoted to the message) is REMOVED, because **bash parses a script INCREMENTALLY** — the
  digest is taken only after thousands of lines are already parsed, so an atomic replace before that
  point leaves bash executing the OLD inode while the digest reads the NEW path (294). Answering the
  question needs a **bootstrap/re-exec handshake**, i.e. a change to how `agent-gate.sh` STARTS UP,
  and it cannot ride inside a component-set comparison. **Five consecutive rounds landed in that one
  mechanism (290/292/293/294) while #3544's own property produced one finding in five** — the standing
  signal to SPLIT rather than carve the same place again. What stays here: job 290's REPEAT of the
  pre-flight after the slot is granted (cheap, and it makes the component-set verdict current with
  respect to the recaptured tree), and job 285's MANIFEST mode validation. The gate-script symlink
  refusal went to #3705 with the check it belonged to; a `gate-script-*` kind no longer exists.
  **A SYMLINK IS A BLOB, AND A GRAFT OUTLIVES `--no-replace-objects` (roborev job 285).** Two
  false-green routes, both closed by moving rather than flagging. (1) The presence probe accepted
  every `blob`, but a symlink IS one — the difference is the MODE (`120000`) — so the two halves of
  the manifest check read DIFFERENT DOCUMENTS: the working-tree validation FOLLOWS the link and
  sees a full manifest while `git show <rev>:<path>` prints the link's TARGET TEXT, so
  `agent-gate.components -> fmt` validated locally and published a ONE-COMPONENT baseline. The mode
  is now validated on both halves. (2) `$GIT_DIR/info/grafts` rewrites parentage and
  `--no-replace-objects` does **not** disable it (measured: no → YES → YES across
  before-graft/plain/`--no-replace-objects`), so on the object-REUSE path — where ancestry still ran
  in the live repository — a graft could reclassify missing components from fatal `BEHIND` to
  non-fatal `DECLARED`. **Ancestry now runs in the isolated repository on BOTH paths**, live objects
  exposed only through an alternate; the reuse path keeps what it was for (no fetch, no transfer)
  and loses only a `mktemp`+`git init`. **The pattern the owner named while ruling on it: every
  live-repository read preserved for speed has turned into a route** (round 16's partial-clone lazy
  fetch, now grafts) — so a third finding there should remove the reuse optimisation rather than
  carve it again. **And a test-suite lesson from the same round: a span-replacing edit silently
  deleted FOUR cases and the suite reported `failed: 0` at 102 instead of 105 for a whole round —
  a green tally over a shrunken suite is #3544's own subject inside its own test file. That suite
  now asserts a CASE FLOOR**, the idiom `test_agent_gate_summary.sh` already used.
  **THE SHARED OBJECT STORE IS TRUSTED, NOT VERIFIED PER-READ — DECLARED IN THE EMITTED LINE, AND
  RESOLVED BY #3749 (roborev job 311; lead ruling on `REQ-3544-OBJTRUST`, then #3749's owner ruling
  2026-09-01).** Git does not rehash a packed object against the id it was asked for on an ordinary
  read, and on this fleet **every lane on a box is a worktree of ONE shared `.git`** (measured:
  `/data/lanes/repo/.git/objects` for lane-3544, lane-3473 and lane-3629 alike), so a planted
  pack/index can make a canonical sha resolve to a shortened manifest — a **false PASS**. What an
  ordinary read DOES verify is the pack CRC and the zlib stream, which catch **accidental** damage
  (bit rot, a truncated or torn write) but never rehash content against its own name.
  **#3749 SPLIT THAT INTO TWO SUBJECTS AND OVERTURNED THE EARLIER FRAMING, which this text used to
  carry: "a NON-INVOKER route, hence a defect" is RETRACTED.** DELIBERATE forgery by a same-host
  peer is **INVOKER-CLASS and OUT OF MODEL** — the #3312 triage rule already says same-host actors
  able to write these scripts are invoker-class, and a peer wanting a false PASS can simply edit
  `scripts/agent-gate.sh`, which is cheaper than forging pack data; no check inside a process
  defends against the party that controls the process. **ACCIDENTAL corruption IS in model**, and
  its control is a periodic full-rehash sweep: `scripts/check-object-store-integrity.sh` (full
  `git fsck`, never `--connectivity-only`, which does not rehash content), emitting an anchored
  four-valued `VERIFIED`/`CORRUPT`/`UNSWEEPABLE`/`UNMEASURED` verdict — run at machine onboarding
  (`bootstrap-agent-machine.sh` section 5d, where VERIFIED is the ONLY `[ok]`) and on the
  worker supervisor's throttled per-iteration cadence (default 6h; `CORRUPT` **stops that
  supervisor loudly** rather than holding, because corruption is non-self-clearing, and so does
  `UNSWEEPABLE` (round 10, below), while
  `UNMEASURED` is reported and deliberately does NOT stop the loop — refusing to run any worker
  **THE FATAL VERDICT IS AFFIRMATIVE, AND IT HAD TO BE MADE SO (#3749 review).** A `git fsck` over
  a store up to eight peer lanes are concurrently writing prints `error:` lines on a **healthy**
  store — measured on this fleet: `invalid reflog entry` naming a DIFFERENT branch each run, on a
  quarter to a half of all runs — so recognising damage from the TEXT SHAPE of a diagnostic
  (`/^error/p`) made a healthy box page high, stop its supervisor and fail `--strict` bootstrap.
  The class now comes from fsck's exit **BITMASK**: bits `1 ERROR_OBJECT` / `4 ERROR_PACK` are this
  sweep's subject; `2 ERROR_REACHABLE` / `8 ERROR_REFS` / `16 ERROR_COMMIT_GRAPH` /
  `32 ERROR_MULTI_PACK_INDEX` are **not demoted to clean** but land on their own non-passing
  `UNMEASURED` cause — **except that `2 ERROR_REACHABLE` HAS TWO CAUSES AND ROUTING BOTH TO
  `UNMEASURED` WAS A FALSE NEGATIVE ON REAL CORRUPTION (round 4).** That bit fires for a stale
  reflog entry naming an object a peer's gc pruned (routine, not the subject) **and** for an
  object genuinely ABSENT while a live ref, the index or HEAD still needs it (corruption). Both
  reproduce, so the reproduction discriminator cannot separate them, and `UNMEASURED` is
  deliberately non-fatal to the supervisor's loop — so workers kept running against a
  demonstrably damaged store, reported as "not measured". They are separated by a **THIRD walk
  with `--no-reflogs`**, which drops the reflogs from the reachability roots and keeps everything
  else: a complaint that SURVIVES it is reachable from a live root and is **`CORRUPT`** (with a
  remedy that says the reflog remedy does not apply); one that CLEARS is `REFLOG-SCOPED` and stays
  `UNMEASURED` where it was. Measured on git 2.43.0 on real fixtures, both directions: a blob
  deleted while HEAD's tree names it exits 2 with AND without reflogs; a commit deleted after
  `reset --hard`, named only by the reflog, exits 2 with and **0** without. **This is NOT the
  `--no-reflogs` SUPPRESSION rejected in round 1** — that proposal decided whether to REPORT and
  was measured not to help; this decides WHICH CAUSE, on a class that has already reproduced
  twice, and can only make the verdict stronger. Passes 1 and 2 never carry the flag, asserted
  structurally over the shipped call sites. Where the attribution itself fails (a third walk
  killed, unlaunchable or unclassifiable) the complaint is `UNATTRIBUTED` and non-passing, and a
  damage bit appearing ONLY in the third walk has not reproduced across the sweep walks: neither
  is `CORRUPT` and neither is clean. That split stays in the CAUSE text rather than in a new token,
  because both its outcomes CONTINUE the loop. **The rule generalises to DISPOSITION, not to a
  count, and round 10 is where that mattered:** `UNSWEEPABLE` STOPS the box, and a token is what
  every consumer keys its disposition on, so a state whose disposition differs from all existing
  ones CANNOT be expressed as cause text — the verdict set is closed at **four**, and a fifth needs
  a fifth disposition, not a fifth cause.
  **`UNSWEEPABLE` HAS A SECOND CAUSE, AND IT IS THE ONE THE ENVIRONMENT ALLOWLIST CANNOT REACH
  (round 11).** `env -i` + the allowlist closes git's *environment* config sources; it cannot close
  the store's OWN config, because **a local config is a FILE in the repository, not an environment
  variable** — the same distinction this file already records for the transport hop. And `git fsck`
  is CONFIGURABLE: measured on git 2.43.0 against a real fixture (a commit object with no author
  email — an ERROR-severity message, exit bit 1, the sweep's own damage class), plain fsck exits
  **1** while both `fsck.<msg-id>=ignore` and a `fsck.skipList` naming that object exit **0**, so
  the sweep reported **`VERIFIED` about a damaged store** — the exact false affirmative this whole
  control exists to prevent, and it is IN MODEL because the file is the SHARED
  `/data/lanes/repo/.git/config` a peer lane (or an accident — one `skipList` added to work around
  one known-bad object) writes. It is **REFUSED, not overridden**: `fsck.<msg-id>` is an
  OPEN-ENDED key space (one key per message id, new ids with new git versions), so a `-c` per key is
  a list that is wrong the moment git adds a message and a partial override is the "one axis closed,
  space declared done" shape this issue has hit five times — the same ruling as the protocol
  allowlist (*"a protocol allowlist is not expressible either … So there is no import at all"*).
  So: **any `fsck.*` key in the configuration the walk would read ⇒ no walk at all ⇒ `UNSWEEPABLE`**,
  naming the keys and the `config --list --show-origin --name-only` that lists them, claiming **NO
  damage** (nothing was rehashed). It **STOPS** rather than reporting, on the round-10 disposition
  argument plus one that decides it: an `fsck.<msg-id>` for an id THIS git does not know **already**
  stops the box (exit 128 twice, the fatal cause), so the permissive reading would stop a box whose
  config merely BREAKS fsck while letting one that SUPPRESSES REAL DAMAGE carry on. Zero such keys
  exist on this fleet's shared store (measured 2026-09-02), the remedy is local and repairs nothing,
  and detection would otherwise be OFF on that box forever. **"Could not ASK" stays permissive and
  is its own state**: a failed probe — or an rc-0 answer listing NO key at all, which is an UNREAD
  policy and not an empty one, since every repository's config declares
  `core.repositoryformatversion` — is `UNMEASURED` with its own cause and no walk, so `VERIFIED` is
  reachable only from a policy that was READ and found empty. **And the CONSUMERS' text is now
  derived from the TOKEN alone**: the latch records only the verdict, so a reader that named the
  fatal mechanism was affirmatively false on the other cause — they say the store's content is
  UNKNOWN and NO DAMAGE was established, and quote the sweep's own `verdict-detail` lines for the
  cause. Two rejected shapes, recorded: sweeping through a scratch repository with a clean config
  (it loses round 7's measured worktree roots and would have to reconstruct git's administrative
  layout — a second implementation of it, with its own false-clean routes), and walking anyway while
  downgrading only the affirmative verdict (it keeps a `VERIFIED` path alive under a policy nobody
  read).
  **AND "THE ROOTS THE WALK HAS" WAS ITSELF AN UNCHECKED ASSUMPTION, WRONG IN BOTH DIRECTIONS
  (review round 7).** A review finding held that `--git-dir=<common>` discards linked worktrees'
  private administrative context, so a missing object needed only by a lane's private HEAD or index
  would be overlooked — *"the normal state of eight lanes"*. **Measured on git 2.43.0, that is
  FALSE**: a common-dir fsck DOES walk every registered worktree's private `HEAD` (it names it
  `worktrees/<name>/HEAD`), its private INDEX, and the HEAD of a **prunable** worktree whose
  directory has been deleted — all three surviving `--no-reflogs`, hence already `CORRUPT`. What it
  does NOT walk is a LINKED worktree's **per-worktree refs** (`refs/worktree/*`, `refs/bisect/*`,
  `refs/rewritten/*`; the MAIN worktree's live in the common dir and ARE walked). Delete an object
  named only by one and the sweep exits **0 VERIFIED** — the HEAD reflog echoes the id and that echo
  CLEARS under `--no-reflogs`, so the attribution walk calls real damage reflog-scoped. So the
  finding's premise was wrong and there was a hole one namespace over. The covered roots are
  PINNED by fixtures (a future git that narrows fsck's enumeration reds a case); **the hole is a
  DECLARED GAP, and the probe built to close it was REMOVED (review round 8).** Three measurements
  rule out the fsck-shaped fixes and are recorded so they are not re-derived: `git fsck <sha>`
  **REPLACES** the default heads (a missing blob reachable from HEAD went undetected), so private
  roots can never be appended to the sweep walk; an explicit-head fsck still scans the whole object
  directory, so it costs a FULL rehash; and `rev-list <missing-root>` dies 128, so `--missing=print`
  cannot answer the case that fires. What was built instead was O(refs) — enumerate each linked
  worktree's refs, subtract the common ones, ask `cat-file --batch-check` whether the remainder's
  targets are present — and **its first review returned three false-`VERIFIED` routes of its own,
  two of them High**: a present root whose reachable **CHILD** is missing is invisible to a
  target-presence check; a per-worktree ref is DISCARDED by a name subtraction when a common ref
  shares its name, even pointing at a different, missing object; and a failed `awk`/`sort`/`comm`
  degrades to a zero-root census and then permits `VERIFIED`. **A mechanism added to prevent one
  false clean produced three new ways to reach one, in one review** — #3229's ruling (*a guard with
  known documented false-PASSes is worse than no guard, because it invites reliance it cannot
  support*), its companion (*subtraction cannot introduce a false PASS*) and #3544's posture on this
  very subject (*a check that claims nothing false is worth more than one claiming a closure it does
  not deliver*) all point the same way. **And the class has ZERO INSTANCES on this fleet** —
  measured twice, independently, 2026-09-02: 14 registered worktrees, all three namespaces absent
  from every linked worktree's admin dir AND from the common dir, 0 mentions in `packed-refs`. The
  falsifier was stated before acting: **live per-worktree refs would have made removal leave a live
  hole**, and the right answer would then have been to fix the three findings instead. **The
  declaration is EMITTED ON EVERY RUN** — `declared-gap` lines, on all three verdict classes,
  because what a run did not walk does not depend on what it found — in **`1 RECOGNISED`** form
  (never a bare count, which reads as a completed census), naming the un-walked namespaces, the
  coverage that is NOT in the gap (the measurement above, which is what keeps the gap one namespace
  wide) and the fleet measurement WITH ITS DATE, since "zero instances" expires. One measurement is
  kept for whoever revisits this: **`git worktree list --porcelain` is FAIL-OPEN** — it silently
  drops a worktree whose admin `gitdir` file is missing (rc 0, no diagnostic) — so any future
  enumeration must be filesystem-first over `$GIT_COMMON_DIR/worktrees/*`, git's own administrative
  directory, with the command only as a cross-check in the direction it can fail. **THE MASK DOES NOT END AT 31, AND
  ASSUMING IT DID DROPPED REAL DAMAGE (review round 2).** A range check over `1..31` — reasoned from
  128 being `die()` and `127 & 1` being 1 — classified **33** (`32|1`) and **36** (`32|4`) as
  unclassified and so `UNMEASURED`: a FALSE NEGATIVE on genuine object corruption, the one direction
  this control exists to prevent. Measured on git 2.43.0 rather than read off a header: a truncated
  `multi-pack-index` exits 32, and that same store with one corrupted blob exits 35. So the **damage
  bits are tested INDEPENDENTLY, and FIRST**, before any completeness check on the rest of the
  status, and an unrelated bit can therefore never MASK damage — it only travels with it. Only a
  status at or above **124** (the timeout/shell conventions, and `die()`/signal deaths above them) is
  refused bit-testing outright — **and that range HAS TWO HALVES, WHICH SHARING ONE VERDICT MADE A
  FALSE NEGATIVE ON REAL COMMIT-OBJECT CORRUPTION (round 10).** Measured on git 2.43.0: overwrite
  the loose object a live ref points AT with unparseable bytes and fsck prints `fatal: loose object
  <sha> … is corrupt` and exits **128**, while the same damage to a BLOB — or a VALID object stored
  under the commit's name — exits 3 (`2|1`) and is caught. So the exact damage this control exists
  for has a shape landing OUTSIDE the mask, where `unclassified` ⇒ `UNMEASURED` ⇒ the supervisor
  writes a fresh throttle stamp and keeps spawning workers. **`124..127` is the timeout/exec
  convention — the fsck NEVER RAN**, which is a fact about this box's tooling and stays permissive
  (`unrunnable`); **`128`+ is git's own `die()` or a signal death — it RAN and did not finish**
  (`fatal`), and reproduced on BOTH walks it is the STOPPING verdict `UNSWEEPABLE` (exit 6). The
  boundary is argued from the exec chain, not assumed: `env`/`nice`/`timeout` each report their own
  failures inside `125..127`, so a status at or above 128 is the innermost command having actually
  started, which together with the `.started` marker below makes "fsck ran and died" an
  AFFIRMATIVE observation rather than an absence. **ON THAT CAUSE `UNSWEEPABLE` CLAIMS NO CAUSE AND
  PRINTS NO REPAIR** — scoped to the FATAL cause since round 11 gave the verdict a second one, below,
  which does name its cause — the alternative was to read the `fatal:` text for a damage signature, i.e. round 1's
  classifier again, narrower, and with every wording nobody enumerated falling back to the SAME
  permissive state, so it would close only the cases somebody thought of. It tells the operator to
  run the walk by hand and act on what the `fatal:` line names (that line is now kept in the
  findings **for DISPLAY only** — it reaches no branch), and it makes a completed sweep the
  condition for resuming. A status carrying a bit OUTSIDE the supported mask is
  `unclassified` rather than folded into a class whose remedy would be wrong. That is what makes it
  **degrade safely** as git adds bits: a new bit alongside damage is still damage, a new bit alone is
  non-passing, and widening `FSCK_KNOWN_MASK` is then a wording change rather than a correctness fix. **AND A STATUS IS ONLY A BITMASK IF THE COMMAND THAT PRODUCED IT ACTUALLY RAN
  (round 3).** The status space fsck uses is SHARED with the shell's, so the classifier had a
  precondition it never checked: the two capture redirections are part of the fsck command, and a
  failure to open the scratch output file (a full or reaped `TMPDIR`) means bash execs nothing and
  exits **1** — `ERROR_OBJECT`. Both passes then fail identically, both "reproduce", and the sweep
  emitted **`CORRUPT` about a store it never opened** — the same false-CORRUPT harm as the `/^error/p`
  classifier above, arriving through the shell instead of through a concurrent writer. It CANNOT be
  inferred from the status, which is the whole point; the evidence is **affirmative** — a marker
  written INSIDE the redirected group as its first statement (a redirection failure on a compound
  command means the body does not execute at all), plus both capture files existing afterwards — and
  its absence is a `launchfail` class routed to `UNMEASURED` and **never bit-tested**. The
  generalisation: **before reading a status as a structured value, establish that the process whose
  convention you are applying is the process that produced it.**  And **no non-clean walk is fatal on ONE observation**: it is re-run exactly ONCE as
  a **discriminator** — a concurrency artefact does not survive a second independent walk, real
  damage does — never a retry-until-clean loop, and a damage class seen once and not twice is
  `UNMEASURED`, neither established damage nor a clean store. (The reachability ATTRIBUTION walk
  above is a third walk and a different question; `MAX_SWEEP_WALKS` in the sweep script is the
  declared ceiling, and every caller's bound is derived from it.)
  **AND THE TWO CHANNELS MUST AGREE BEFORE THE FATAL VERDICT IS ACTED ON (round 4).** Both
  consumers read the exit status AND the anchored `verdict ` line, and both tested them with `||`
  while the comment above each asserted the conjunction — the false-rationale class, and the
  reason nobody looked. An exit 4 with no verdict line, or a stray `CORRUPT` line under any other
  status, was therefore enough to create a **STICKY, BOX-WIDE, operator-cleared latch that halts
  every lane on the box**. Blast radius decides the direction: a false latch stops four lanes
  until a human notices, while one more iteration over a genuinely damaged store costs the next
  sweep, which reproduces the damage and latches it properly. So a disagreement is routed to
  `UNMEASURED` and **NAMED** (`INCONSISTENT sweep result`) rather than folded into the generic
  "could not measure" line. The `UNMEASURED` tests stay disjunctive on purpose: a disjunction that
  can only reach a NON-PASSING branch cannot manufacture a verdict. **The `CORRUPT` verdict is PERSISTED
  for the box in its OWN CREATE-ONLY FILE** (`<stamp>.STOP`, which RECORDS WHICH stopping verdict it
  is, read affirmatively — round 10 renamed it from `.CORRUPT`, because a file whose NAME asserts
  damage is a confidently-wrong claim on disk for a verdict that establishes no cause, and every
  message naming that path would have sent an operator to the re-clone remedy; an unrecognised
  second line is a cause-free stop and is never defaulted to the damage text), because a
  timestamp-only stamp let
  the detecting lane stop while its three peers saw a fresh stamp, skipped their own sweep for the
  whole interval and kept spawning workers over the damaged store. **PUTTING THE VERDICT IN THE
  THROTTLE STAMP WAS THE FIRST FIX FOR THAT AND WAS ITSELF A DEFECT (round 2):** stamp writes are
  unsynchronised overwrites, so a lane whose sweep STARTED before the detection can finish AFTER it
  and replace `CORRUPT` with `VERIFIED`, after which the peers throttle on a fresh non-corrupt stamp
  and keep working — the same harm through a different door. **Two consecutive rounds found a defect
  in that ONE shared mutable cell, so the channel is REMOVED rather than serialised** (CLAUDE.md's
  standing ruling for this family): the latch's only transition is ABSENT -> PRESENT, created under
  `set -C` so the KERNEL arbitrates and a losing writer cannot overwrite the winner, while the
  timestamp stays a freely overwritable cell because moving a timestamp backwards costs one extra
  sweep and nothing else. **A LOCK WAS CONSIDERED AND REJECTED**, and the reason generalises: it
  makes the read-modify-write atomic but leaves a value any writer can move BACKWARDS, so "CORRUPT is
  sticky" would rest on every present and future writer remembering to honour it — plus a lock has
  its own could-not-acquire path that must then not silently skip the latch. The latch does not
  expire (corruption is non-self-clearing) and is cleared by hand with the `rm -f <latch>` that every
  message naming it prints; a create that could not happen is REPORTED, never read as a latched box.
  **THE LATCH IS CREATED BEFORE THE THROTTLE STAMP, AND RE-READ BEFORE EVERY RETURN THAT LEADS TO A
  SPAWN — AND THE REMAINING RACE IS DECLARED, NOT CLOSED (round 3).** Two ordering defects, neither
  needing a lock. The stamp used to be written for EVERY outcome BEFORE the CORRUPT branch ran, so
  between those two writes a peer read a fresh non-corrupt stamp and throttled past a corruption
  about to be recorded — round 1's own harm, one instruction earlier. And a lane whose OWN sweep
  said `VERIFIED` returned toward a spawn without re-reading the latch, so a peer latching the box
  DURING that lane's two fsck walks was simply not seen. So: latch first, stamp second, and one
  `obj_sweep_stop_if_latched` helper called before the throttled, `VERIFIED` and `UNMEASURED`
  returns alike — one rule ("no such return without a fresh latch read") instead of a set of paths
  someone must remember to audit.
  **AND ORDERING WAS NOT ENOUGH: THE STAMP IS WRITTEN ONLY IF THE LATCH IS CONFIRMED PRESENT,
  ELSE IT IS FORCED STALE (round 9, item 1).** "Latch first, stamp second" left the stamp
  UNCONDITIONAL, so on the one branch where the latch could not be PERSISTED — a writable stamp
  inside a directory that is not writable, so creating `<stamp>.STOP` fails while rewriting
  `<stamp>` succeeds — the detecting lane stopped (correct) and still advertised a freshly swept box
  (not correct): its peers read the fresh stamp, skipped their own sweep for the whole interval, and
  kept working against a store already confirmed damaged. Round 1's harm, surviving in the one branch
  that never got round 1's treatment, under a round-2 note ("journalled, and this lane still stops")
  that was true and said nothing about the peers. The stamp write is now GATED on
  `obj_sweep_latch_present` — the AFTERWARDS-EXISTENCE check, never the create's exit status, which
  cannot tell "a peer got there first" from "the directory is unwritable" — and where the latch is
  missing the stamp is FORCED to epoch `0` rather than merely left alone, because a peer whose sweep
  started earlier can finish DURING this one and write a fresh timestamp. Forcing can only ever cause
  MORE sweeping, so it cannot manufacture a false clean; a repeated sweep per lane is the correct
  price for a verdict with nowhere durable to live, and six hours of silence is not.
  **AND THE PRINTED REMEDY HAS TO WORK, WHICH IT DID NOT (round 9, item 2).** All three copies of the
  repair instruction said `git fetch --force origin`, which repairs NOTHING: `--force` only permits
  non-fast-forward REF updates and re-downloads no objects at all, so with the advertised tips
  unchanged the negotiation can transfer nothing — an operator following it exactly kept the
  corruption AND gained the impression of a repair, this repo's false-rationale class landing on the
  one text a human gets at the moment the box has stopped. MEASURED on git 2.43.0 against planted
  damage, by the sweep's own verdict: `--force` leaves a corrupt loose object, a flipped pack byte
  and a missing object exactly as they were; `git fetch --refetch origin` (git 2.36+) restores a
  MISSING object but NOT damaged content, because the damaged bytes stay in the object directory
  where fsck still finds them — so content damage needs the pack/loose object DELETED first, and then
  `--refetch` verifies clean. A fresh clone works, **but only from the canonical remote over the
  network**: `git clone <local path>` HARDLINKS the object files, so a clone of the damaged
  repository is damaged too (measured). Two rules follow. The sweep OWNS the text — it knows the
  damage class and the measurements live beside it — and both consumers QUOTE its `verdict-detail`
  lines rather than paraphrasing (the #3369 ruling this file already carries; three paraphrases is
  three places to correct and one of them will be missed), with a fail-closed fallback so a stopped
  box is never left with no remedy. And clearing the latch is gated on a RE-RUN of the sweep
  reporting its affirmative verdict: "I think I fixed it" is not an exit condition.
  **THAT RULE WAS STILL A SET OF SITES, AND ROUND 5 FOUND THE FOURTH — SO THE READ IS NOW AT
  ENTRY, ABOVE EVERY BRANCH (round 5, item 1).** The documented opt-out
  `OBJ_SWEEP_INTERVAL_HOURS=0` returned before ANY latch read, so switching the sweep off also
  switched off an already-recorded CORRUPT verdict — while this paragraph asserted a latch ignores
  the interval. Round 3 fixed three such returns and introduced the helper *for* them; round 5's
  fourth is the same defect through a new door, because the design required every early return to
  independently REMEMBER the check, and patching site four leaves site five to the next reviewer.
  So the read is hoisted to the TOP of `object_store_sweep` — the prologue before it is
  declarations and assignments ONLY — and the invariant becomes a property of the FUNCTION ("it
  cannot be entered without a latch read") rather than of an audited list, which holds for branches
  nobody has written yet; it is asserted structurally, both that the first control-flow line comes
  after the gate and that nothing before it can branch, call out or return. The post-sweep reads
  STAY: a peer can latch the box while this lane spends up to `MAX_SWEEP_WALKS` walks, which is a
  different question. Cost: one `--print-store` resolution per iteration (measured **5 ms**), which
  the opt-out path did not pay before, against a supervisor iteration measured in minutes.
  **AND THE LATCH QUESTION IS FOUR-VALUED, BECAUSE A FILE PREDICATE IS NOT.** `[[ -e "$latch" ]]`
  is false both for an absent latch and for one the process cannot look at — CLAUDE.md's standing
  two-valued-predicate rule, landing on the single file whose job is to stop the box. `present` and
  `absent` (an affirmative measurement: the absence was ESTABLISHED THROUGH SEARCHABLE ANCESTORS —
  **`-d` on the holding directory is ITSELF two-valued and reading it as absence was the same trap
  one level up (round 10, item 2): it is false both for a directory that does not exist and for one
  whose ANCESTOR cannot be searched, so a latch under an inaccessible ancestor answered `absent`,
  the permissive value, bypassing the fail-closed state built for exactly it.** The probe now walks
  up to the deepest ancestor it can stat and counts the absence only if THAT one is searchable,
  which is what makes it a measurement: with a searchable ancestor every stat below it gives a true
  answer, by induction) join
  **`unknown`**, which STOPS under its own reason `object-store-latch-unreadable` — never as
  CORRUPT, since nothing observed damage and that remedy would send an operator to re-clone a
  healthy store — and **`unkeyed`**, the one permissive answer, announced once: no key means no
  latch was ever NAMED, the writer derives the name through the same resolver, and stopping instead
  would make a missing `check-object-store-integrity.sh` (ordinary on any branch cut before #3749)
  halt every lane for want of a hygiene probe. Its residual is stated at the branch: a TRANSIENT
  resolver failure in one lane misses a peer's verdict for one iteration.
  **THE THUNDERING HERD IS CLOSED, WITH THE HAZARD ITS FIX INTRODUCES (round 5, item 2 — a Low in
  round 1, "not disposed of" in round 2, a Medium in round 5).** The throttle read and the sweep
  invocation were unsynchronised and the stamp is written at the END of a sweep, so at the moment
  the interval expires every lane read the same stale stamp and started its own full-store fsck —
  and the consequence is not merely CPU: the walks are I/O-bound, so contention pushes them toward
  the per-walk bound, and an expired bound is `UNMEASURED`, i.e. a CORRELATED loss of the
  measurement on every lane at once. The serialiser is a per-store claim DIRECTORY created with
  `mkdir` — the same kernel-arbitrated create-only primitive as the latch, and deliberately **not
  `flock`**, because this file's own single-instance lock is mkdir-based precisely since **macOS
  ships no `flock(1)`**, and a second locking mechanism is a second set of failure modes.
  **A LOSER *DOES* WAIT, WHICH REVERSES THE ORIGINAL DESIGN (round 12).** It used to skip its probe
  and carry straight on to the spawn path after one latch read, on the argument that *"a peer is
  doing it right now"* is a complete answer for a 6-hourly hygiene sweep. That is a complete answer
  about the SWEEP and no answer at all about the SPAWN: the loser KNOWS a full-store fsck is in
  flight — that is why its claim failed — and that fsck can end in `CORRUPT` or `UNSWEEPABLE`, in
  which case the peer latches the box. So the discarded window was the peer's **ENTIRE SWEEP**,
  measured **13-80s** on this fleet and up to `MAX_SWEEP_WALKS` x `OBJ_SWEEP_TIMEOUT_SECS` (**600s**
  at the shipped defaults) by construction — **not** the narrow post-read spawn gap the residual note
  described, which is the false-rationale class landing on the very text that discloses the race.
  The loser now waits, re-reading the latch, the stamp, the claim and the STOP FILE every
  `OBJ_SWEEP_CLAIM_POLL_SECS` (5s; granularity, not a bound), and **the bound is the claim's OWN
  derived staleness value** — the same 660s that says "this claim may be TAKEN OVER" says "stop
  waiting for it", so a dead peer cannot wedge the wait and no second, driftable constant exists.
  **The timeout is NOT a clean result and does not carry on**: when the peer neither finishes nor
  vacates, that claim is by construction stale at the deadline, so the next acquire RECOVERS it and
  **the loser becomes the sweeper** rather than proceeding unmeasured.
  **AND A COMPLETED PEER ENDS THE WAIT — IT DOES NOT SEND THE LOSER BACK TO CONTEND (round 14).**
  The wait's `completed` (the throttle stamp went FRESH) was treated as "go round and acquire
  again", and it is reached **WITHOUT SLEEPING**, on the pass that observes the stamp. So a peer
  that writes the stamp and then DIES before releasing leaves the claim PRESENT and the stamp
  FRESH — a state in which every acquire answers `held` and every wait answers `completed` in
  microseconds — and the lane **spun CPU-hot until the supervisor's whole `MAX_HOURS` budget
  expired** (measured: **2165** completions in 20s). A liveness and CPU defect, not a wrong
  verdict, and on a four-lane box it is a burnt core that reads as *"the fleet got slow"* with no
  attributable cause. **The age-based recovery built for exactly this dead peer could not save it,
  which is the transferable part: `completed` was tested BEFORE the claim's deadline on every
  pass, so the recovery the design relies on never ran.** The fix adds no second notion of "the
  peer is gone" — a second mechanism is a second thing to drift: a fresh stamp is the SAME answer
  the throttle at the top of `object_store_sweep` gives, so the lane SKIPS exactly as it would
  have there and stops contending, which is strictly LESS work than the acquire-and-release round
  trip the ordinary case used to make (so the common case — a peer that released a moment ago —
  gains no latency). **The loop is now bounded in WALL TIME BY CONSTRUCTION and by a value it
  already derived**: `exhausted` and `completed` break, and because neither surviving state
  (`vacated`, `expired`) is REQUIRED to have slept, the loop itself is bounded by the iteration's
  own `claim_stale` budget — never `MAX_HOURS`, and never a second constant. **The general rule:
  a state a loop can reach WITHOUT SLEEPING must not be a loop-CONTINUING state**, and a bound
  placed after a fast-path test is not a bound at all. Only where peers keep handing
  the claim around for the whole budget does the lane skip — journalled, paged once, reported as
  `NOT SWEPT AND NOT MEASURED`, never as clean. It does NOT stop the box there: a peer sweeping is
  also the shape of a HEALTHY box recovering a wedged claim, and stopping four lanes over a hygiene
  probe's contention is the self-DoS refused everywhere else in that file. Cost is paid only when a
  sweep is genuinely running, i.e. at most once per throttle interval per lane. The throttle is **RE-READ after acquiring**, which
  is what stops the claim converting a herd into a QUEUE of redundant sweeps. **A stale claim must
  not wedge the box, which would be strictly worse than the herd**, so the recovery threshold is
  DERIVED and not chosen — a claim cannot be stale while the sweep it represents could legitimately
  still be running, so it is the sweep's own `MAX_SWEEP_WALKS` (**read from that script**, never
  re-typed) x this supervisor's per-walk bound + slack = 3 x 200 + 60 = 660s — and a claim carrying
  NO parseable start time reads as stale, deliberately: its cause is a lane killed in the
  microseconds between the `mkdir` and the write, and the other reading wedges the sweep forever on
  a file nobody can age (cost of this direction: one extra sweep, once). Where the bound cannot be
  derived, **no claim is taken at all** and the previous behaviour is restored, announced — an
  unrecoverable claim is worse than the herd it prevents. The claim is a **REGISTERED** resource
  (`OBJ_SWEEP_CLAIM_OWNED`, read by the EXIT trap installed before any sweep can run), so the
  CORRUPT path — which ends the process — does not leave its peers waiting out the bound; CLAUDE.md's
  roborev-job-282 ruling, applied to a resource this fix ADDED.
  **AND A CLAIM IS RELEASED ONLY WHILE IT IS STILL OURS — A PATH IS NOT AN IDENTITY (round 11).**
  The release `rm -rf`'d the claim path unconditionally, so a lane whose sweep overran the recovery
  bound deleted the claim of the SUCCESSOR that had legitimately taken over — permitting the second
  concurrent full-store fsck the claim exists to prevent — and a lane on the `unavailable` path,
  which never owned the claim at all, did the same at the end of its own sweep. The claim now carries
  an ownership TOKEN written **before** its `started` file (so a claim a peer can AGE already names
  its owner), and only an AFFIRMATIVE token match deletes: the question is four-valued
  (`ours`/`other`/`gone`/`unknown`) and the three non-affirmative answers all LEAVE the directory,
  because deleting costs a concurrent sweep while keeping costs one skipped 6-hourly probe that the
  staleness bound recovers. **It is NOT atomic and does not claim to be**: the read and the `rm` are
  two operations, so a takeover landing between them is still deleted — a microsecond window against
  the previous "always". A rename-then-verify was rejected (it moves a successor's claim aside before
  it can be checked, and restoring it is not atomic either, since `mv` onto an existing directory
  moves INTO it). Still not closed, and it is the
  pre-existing half: this file installs no INT/TERM handlers, so a SIGNALLED supervisor releases
  neither its claim nor its lock — for the claim a delay rather than a wedge, and adding signal
  handlers changes the LOCK's lifetime too (#3683's subject).
  **WHAT IS NOT CLOSED, PER PATH AND WITH ITS REAL MAGNITUDE — because a residual whose size is
  understated is worse than one described honestly (rounds 12 and 13), AND A RESIDUAL LEFT
  STANDING WHEN IT COULD BE CLOSED IS THE OTHER HALF OF THAT.** **PATH 1, the claim loser: CLOSED**
  — it was the peer's whole sweep (13-80s measured, 600s by construction), and is now bounded by one
  `OBJ_SWEEP_CLAIM_POLL_SECS`. **PATH 2, a lane's own sweep: COVERED** by the entry read plus the
  post-sweep reads. **PATH 3, the last latch read to the worker spawn: NARROWED FROM UP TO AN HOUR
  TO ONE PRE-SPAWN PROLOGUE, AND IT IS NOT ZERO.** The hold loop used to run after the last latch
  read and never re-read it, so at the shipped defaults a lane could sit for `BUILD_HOLD_MAX` x
  `HOLD_POLL_SECS` = **12 x 300s = 3600s** (leftover-worker family: `LEFTOVER_HOLD_MAX` x
  `HOLD_POLL_SECS` = 900s) and then spawn onto a box a peer had latched meanwhile. `preflight_wait`
  is now a WRAPPER: the loop lives in `preflight_wait_holds` and the STOP latch is re-read on
  **every** return out of it, so the hold window is closed STRUCTURALLY — a property of the function
  boundary, not an enumerated set of returns (round 3 fixed three such returns, round 5 found a
  fourth; that shape does not converge). **What remains is `run_iteration`'s pre-spawn prologue**:
  `stamp_claim`, i.e. ONE `claim-heartbeat.sh stamp` ref push to `origin` (plus a best-effort delete
  push on a lane transition), around an `rm`/`mkdir`/assignments. Measured lower bound on this
  fleet: a bare `git ls-remote origin HEAD` is **0.26-0.28s**, and a push negotiates and updates a
  ref on top of that; the supervisor puts NO timeout on it, so the window is
  sub-second-to-seconds in practice and unbounded in principle if the network stalls — **not the
  "milliseconds" a purely local prologue would give** (a lane with `CLAIM_CMD` empty does pay only
  that). The harm bound is unchanged: it is not silent and not durable — that lane's next iteration
  reads the latch at the top of its sweep and stops, and nothing certifies a merge in that window
  without a full gate, which a latched box refuses. Closing it entirely still needs per-store
  synchronisation shared by the sweep and the spawn decision (a lock held across both, with the
  spawn decision inside it), which is **split to its own issue**; a further read immediately before
  the spawn would shrink the window again and cannot remove it, because a read and a spawn are two
  operations. **Path 1 and the hold window are closed; the pre-spawn window is open. Do not
  describe the race as gone.**
  **The sweep is also NOT INTERRUPTIBLE, so its cost is bounded in WALL TIME instead:** its
  walks run in a CHILD process, so the supervisor cannot check the stop file between them; it checks
  it (and the wall-clock budget) immediately BEFORE the sweep, and the supervisor's per-walk bound
  defaults to the sweep script's own bound **divided by `MAX_SWEEP_WALKS`** (200 vs 600/3), so every
  walk it can pay for still costs no more than one walk at the script's bound. **THE WALK COUNT IS
  READ FROM THE SHIPPED SCRIPT, NOT RE-TYPED IN THE TEST (round 4):** the relation was asserted with
  a hard-coded factor 2, so ADDING the third walk would have kept it green while the supervisor's
  real worst case rose to 900s — a relation whose own constant is restated is two magic numbers
  wearing a relation's clothes. Raising a bound, or the walk count, is a change to somebody's stop
  latency.
  **And the stamp's KEY is resolved by the sweep script itself (`--print-store`), never by a `git`
  call in the supervisor:** a bare `git rev-parse --git-common-dir` inherits the caller's
  environment, so an inherited `GIT_DIR`/`GIT_COMMON_DIR` keyed the stamp — and hence the latch — on
  ANOTHER repository, which is roborev job 276's "the allowlist did not reach the sites the fix
  added" at a site the same round left behind. ONE resolver, in the file that owns the `env -i`
  allowlist, so a future caller has no un-isolated shape available to it. **And that key is a
  DIGEST of the canonical path, because flattening is not INJECTIVE (round 4):** replacing `/` and
  every unsupported character with `_` maps `/tmp/a/b/objects` and `/tmp/a_b/objects` to ONE name, so
  two repositories on a box shared a throttle stamp *and* a CORRUPT latch — one store suppressing the
  other's sweep, or stopping every lane with the other's damage. `<sanitised tail>.<16 hex of
  sha256>`: the tail is READABILITY ONLY and carries no identity.
  **AND THE DIGEST IS OVER THE RAW PATH, COMPUTED IN THE RESOLVER — DIGESTING THE RENDERED VALUE WAS
  ITSELF THE NEXT DEFECT (round 5, item 3).** Round 4 made the key injective over the flattening and
  then fed the digest the value `--print-store` had already passed through the sweep's `sane()`
  escaper: a DISPLAY encoding, and a LOSSY one, so a path holding a real newline and one holding the
  two literal characters `\n` produced ONE key — the same shared-stamp-and-latch harm, arriving
  through the fix for it. `sane()` exists so a control character cannot break the anchored output; it
  is not reversible and was never an identity. So `store_key` lives in the sweep script, the only
  process holding the raw bytes, and publishes a second anchored `store-key` line the caller
  VALIDATES and uses verbatim; the supervisor's own digest helper is GONE, so there is no lossy value
  left for a future caller to digest. The general rule, worth more than the instance: **a value
  sanitised for DISPLAY is not an IDENTITY, and a comparison key must be derived before any rendering
  — where both are needed, publish them as two fields rather than deriving one from the other.**
  Three digest tools (`sha256sum`,
  `shasum -a 256`, `openssl dgst -sha256`) cover both platforms, the output is parsed from both ends
  and then VALIDATED as hex, and a host with none **fails closed to the EMPTY key** — no throttle, no
  latch, announced once naming both causes — never a silent fall back to the colliding form on
  exactly the hosts nobody tested. `cksum` is present everywhere and deliberately unused: CRC32 is
  not collision-resistant, and a colliding key is the defect being removed.
  because a hygiene probe could not run is a self-DoS). **That sweep is PERIODIC, NOT PER-READ, so
  the emitted clause still says `store TRUSTED, not verified (#3749)` — do not inflate it.**
  **THREE ALTERNATIVES WERE REJECTED and the reasons are recorded so they are not re-derived:**
  per-lane full clones (a permanent multi-GB tax for an out-of-model threat); per-read rehashing
  (the FOURTH carve into one pre-flight, charged to every `--lite` round); and **removing the reuse
  optimisation, whose original argument still stands** — the recorded "a third finding here should
  REMOVE the reuse optimisation" ruling does NOT dispose of it, because removal does not CLOSE it:
  the ancestry walk and the provenance leg read HEAD's **committed** content, which has no source
  other than that store — the working tree cannot substitute, since `UNCOMMITTED` exists precisely
  to compare against what is committed — so a forged HEAD object still turns `UNCOMMITTED` (fatal)
  into `DECLARED` (non-fatal) after removal, while charging every `--lite` round for the privilege
  (measured 2026-08-31: **3.41 s / 93 MB** full, **3.58 s / 45 MB** at `--depth=1` — shallow is NOT
  cheaper, it still ships the tip's whole tree). **A permanent tax for a half-closure is the guard
  agents learn to waive.** So the boundary is **DECLARED**: every baseline-bearing verdict line ends
  by naming the object provenance (`REUSED` from the shared store / `FETCHED` from the canonical
  remote / `NOT RECORDED`) plus `store TRUSTED, not verified (#3749)`. **A check that claims nothing
  false is worth more than one claiming a closure it does not deliver** — the same move the roborev
  waiver's threat model makes where a dependency cannot be removed. The declaration is folded into
  the ONE `src_note` suffix eleven printf arms already consume, never appended per-arm, and the
  self-test pins it as a **closed set of three renderings** by string equality: pinning one
  literal would red on correct input (which clause fires depends on whether this box's store
  already held the commit), pinning nothing would let a wording pass delete it.
  **STOP RENDERING THE VALUE, DO NOT SANITISE IT AGAIN — AND A FIX THAT ADDS A RESOURCE INHERITS
  THAT RESOURCE'S LIFETIME BUGS (roborev job 282).** Two closures. (1) The rejected-origin
  diagnostic was the FIFTH finding in one family — raw URL rendered (227) → redacted but not
  flattened (234) → flattened but not redacted (239) → scheme-only redaction (264) → **query
  strings verbatim and multi-`@` authorities redacted only to the first `@`** (282). Every fix
  improved the sanitiser, which is the "rarer delimiter" the mechanism ruling warns against, so the
  URL is **no longer published**: the diagnostic names the AXIS it was rejected on, plus the
  normalised identity **only when that identity is itself grammatically clean** (a
  `…/repo?token=SECRET` normalises to a value CARRYING the query, so the shape is checked rather
  than assumed). Two self-inflicted defects on the way, both worth knowing: a fall-through printed
  `${v%%:*}` which with no colon **is the whole value**, reproducing the finding; and reducing the
  NORMALISER's output instead of the RENDERED text made every local path normalise identically, so
  a canonical identity pinned to a local path matched **any** local path — **the normalised value
  is a COMPARISON KEY, not a diagnostic string.** (2) Round 17's own fix created the owned
  supervisor and never registered it with the signal path — the third instance of one family
  (round 9 register-before-create, round 14 clean-up-on-signals), i.e. **fixing a resource-lifetime
  bug added a resource with the same bug**. Any owned child is now registered the moment it exists
  and cleared the moment it is reaped, and cleanup reaps it BEFORE deleting the files it could
  otherwise recreate.
  **NEVER SIGNAL A PROCESS GROUP YOU NO LONGER OWN — AND OWNERSHIP ENDS AT REAP, NOT AT EXIT
  (roborev job 279).** The bounded runner's watchdog arm backgrounded the COMMAND, so the pgid was
  the command's pid, and after TERM + a 1s grace it sent an unconditional `kill -KILL -$pid` — by
  which time bash may already have REAPED the leader, releasing that id. On a four-lane box the
  group that inherits it is most likely **a peer lane's gate** (this repo has the incident: a
  pattern-based `pkill` killed a peer's gate at component 28 of 30). The leader is now a
  **supervisor kept alive on purpose** — it runs the command, records the status to a file with a
  completeness marker, then parks (bounded at `secs+5` so a SIGKILLed gate leaves nothing) — so
  every signal targets an id we still hold. Two things fall out: a successful call now reaps its
  STRAY descendants, and **the race itself cannot be tested** (pid reuse is not controllable), so
  the coverage is the observable before/after difference plus a **structural** assert of the
  ownership invariant, labelled as such rather than dressed up as behavioural. Related, from the
  same round: `[ -z "$(find …)" ]` collapses "the scan FAILED" onto "no match" — a three-valued
  signal read two-valued — and this repo LINTS for that shape (`1699-find-tristate`).
  **THE ALLOWLIST HAS TO REACH THE SITES A LATER CHANGE ADDS (roborev job 276).** The migrated
  object reads ran under a bare `env`, inheriting the caller's environment — the round-13 hole
  re-opened at the NEW sites, not a new route: an inherited `GIT_DIR` points a read at another
  repository, and `GIT_CONFIG_COUNT`/`GIT_CONFIG_PARAMETERS` injects a promisor or an `insteadOf`.
  Every git call in the pre-flight now runs under `env -i` + the ONE allowlist, with only
  location-specific values (the alternate) layered on top — **including the STATE probes**, since
  injected config could have made a real partial clone look non-partial and re-opened the fast
  path. Two corrections came with it: **a config file does NOT keep a URL out of every argv** —
  git passes the configured URL to a transport HELPER, whose command line then carries the token —
  so a credential-bearing origin is now **refused** (userinfo must be absent or exactly `git`;
  refusing ALL userinfo red the standard `ssh://git@github.com/…`, a false FAIL on correct input — **that ssh example is now moot, since job 296 refuses ssh forms outright; what KEEPS the rule is CI's `https://x-access-token:<TOKEN>@github.com/…`**);
  and **a specified control must be required to have WORKED** — the `chmod 600 … || true` on the
  isolated config is now fail-closed with the resulting mode VERIFIED (`find -perm 600`, since
  `stat` is GNU-vs-BSD incompatible), because "chmod exited 0" and "the file is 0600" are
  different claims.
  **AND A LOCAL READ CAN BE A NETWORK OPERATION (roborev job 268).** In a PARTIAL clone,
  `ls-tree`/`show`/`cat-file` answer a missing object by fetching it from the **promisor remote**,
  under the live repository's **local config** — so `url.*.insteadOf` plus an enabled external
  protocol executes a remote helper, and the lazy fetch writes objects into the shared store. That
  was the THIRD route of one family (`insteadOf` on the fetch, `ext::` on the transfer hop, the
  promisor), and per-call-site suppression had failed each time — so **every baseline/HEAD object
  read and the ancestry walk moved INTO the isolated scratch repository**, with the lane's object
  directory supplied as an alternate (pure object storage: no config, hence no promisor, no
  `insteadOf`, nothing for a helper to be invoked from; a missing object there is a named refusal).
  Ancestry compares against **HEAD resolved to a sha in the checkout**, because inside the scratch
  the ref `HEAD` means the SCRATCH's own unborn HEAD. **The fast path is gated on the clone not
  being partial** (`_component_set_is_partial`, three-valued, UNKNOWN ⇒ treated as partial: the
  conservative branch costs a fetch, not correctness), because that path reads the baseline in the
  live repository — and `cat-file -e` cannot even probe presence there: measured with
  `GIT_NO_LAZY_FETCH=1` set, it answered 0 for a blob whose `show` then FAILED, since it answers
  about PROMISED objects. `GIT_NO_LAZY_FETCH=1` is carried as a **belt, not the control** (git ≥ 2.36;
  an unset variable does nothing silently, which is exactly why it cannot be the control).
  **UNTRUSTED REPOSITORY STATE IS BIGGER THAN CONFIG (roborev job 264).** Closing git's *config*
  sources and treating "untrusted repository state" as closed with them left three holes, and the
  shape of the error is the recurring one — one axis closed, space declared done. **(1) Replacement
  refs**: `refs/replace/<sha>` transparently substitutes another commit, so the pre-flight reported
  the CANONICAL sha while reading a FORGED, smaller manifest, and PASSed — the worst pairing, since
  the audit trail looks right. Now `GIT_NO_REPLACE_OBJECTS=1` in the allowlist plus
  `--no-replace-objects` on every lane-local object read. **(2) The transfer hop could EXECUTE**:
  `git fetch` in the LIVE repository reads its LOCAL config (only the *environment* is sanitisable
  — a `.git/config` is a file), so a local `url.*.insteadOf` + `protocol.ext.allow=always` rewrote
  the scratch path to an `ext::` helper and ran commands DURING the fetch, before the sha
  comparison that was meant to make the hop "untrusted but safe". **A check placed AFTER a harmful effect can only REPORT it, never PREVENT it — so if the harm is
  EXECUTION, the control must be that the execution cannot be REACHED, not that its result is
  detected** (lead ruling, round 14; the sha-equality assert sat downstream of the fetch it was
  meant to validate). The corollary for tests: assert UNREACHABILITY, with a positive control
  proving the attack executes in a plain repository, or the green means nothing. A protocol allowlist is not expressible either
  (`-c protocol.allow=never` loses to a more specific local `protocol.<name>.allow=always`, and
  the helper-name space is whatever `git-remote-*` is on PATH). So there is **no import at all**:
  the scratch object store is made visible via `GIT_ALTERNATE_OBJECT_DIRECTORIES` — an object
  SOURCE, not a transport — and NOTHING is written into the shared `.git` (no pack, no ref, no
  `FETCH_HEAD`). Safe for the reason the transport was not: every read is BY A SHA whose provenance
  is the isolated chain, and objects are **content-addressed**. `baseline-transfer-mismatch` and the
  private-ref machinery are gone with it — the class is ELIMINATED, not detected. **(3) The scp-form
  leak, third instance of one family** (raw → flattened-not-redacted → scheme-only redaction):
  `TOKEN@github.com:owner/repo` was canonical because the normaliser dropped userinfo before
  comparing, and an ssh error then echoed it into the SUMMARY. Fixed by **narrowing what is
  accepted** (scp userinfo must be exactly `git`) rather than widening the scrubber again — though
  the scrubber covers scp form too, since a REJECTED value is still rendered. **(4) Cleanup on
  SIGNALS**, the second axis of round 9's "cleanup registration precedes resource creation": bash
  runs no EXIT trap for a signal with its default disposition, so INT/TERM/HUP now have handlers,
  installed before the resources exist, saving and restoring the caller's.
  **THE ISOLATED HOP'S ENVIRONMENT IS AN ALLOWLIST, AND THE OBJECTS ARE FETCHED ONLY WHEN
  ABSENT (roborev job 258).** Neutralising `GIT_CONFIG_GLOBAL`/`GIT_CONFIG_SYSTEM` and stopping
  there left the "isolated" hop inheriting `GIT_CONFIG_COUNT`/`KEY_*`/`VALUE_*`,
  `GIT_CONFIG_PARAMETERS` and `GIT_TEMPLATE_DIR` — each measured to redirect a fetch via
  `url.<attacker>.insteadOf`, the template by seeding the *new* repo's own LOCAL config. A HOP 1
  redirect is worse than a hop 2 one: the sha the isolated hop observes and the commit
  transferred in then BOTH come from the attacker, so the equality assert compares two values
  that AGREE and emits a **false PASS**. That was **enumerating one axis and declaring the space
  enumerated** — so every isolated git call now runs under **`env -i` plus an allowlist**
  (`ADMIT` what git needs to REACH and AUTHENTICATE to the remote, each entry carrying its
  reason; `CLEAR` everything that can change WHAT it fetches or WHAT it runs), which makes new
  git environment variables cleared BY DEFAULT rather than needing to be discovered. Lane-local
  reads are deliberately NOT wrapped: the only value needing provenance is the SHA, and
  everything addressed by it is **content-addressed**. **And the baseline sha now comes from a
  ref ORACLE (`git ls-remote`), with objects fetched only when this repository lacks that
  commit** — measured 3.74 s / **92 MB of full history per invocation** → 0.51 s / no transfer;
  `--filter=blob:none` was rejected on measurement (it leaves the manifest blob absent exactly
  when `main` changed it, failing a correct tree). The ref value is still read live, which is
  what "fetched in THIS invocation" is about; the oracle's output is remote-controlled text and
  is VALIDATED (`baseline-ref-unparsable`), never merely parsed.
  **THE BASELINE IS READ AS DATA; NOTHING FETCHED IS EVER EXECUTED (REQ-3544-01, lead ruling).**
  The first design derived the baseline set by extracting `origin/main:scripts/agent-gate.sh` and
  RUNNING it (`bash <fetched> --list`). **Six of that mechanism's seven High-severity findings
  traced to that one decision**, and its three fixes each moved the hole one layer outward (a
  symbolic remote name ⇒ the validated URL ⇒ the URL in `argv`) — the signature of a **shared
  channel between data and control**, where the standing ruling (#3312) is to REMOVE the channel,
  not to choose a rarer delimiter. So: the branch side is the **in-process `COMPONENTS` array**
  (what this run will actually dispatch), and the baseline side is **`git show
  <sha>:scripts/agent-gate.components`** — a committed DATA manifest, parsed under a CLOSED
  grammar (one name per line; blank lines and `#` comments skipped; anything else, INCLUDING a
  name with leading/trailing whitespace, is a NAMED refusal — a parser that trims is a parser
  that guesses). **What this CONVERTS the six findings into, rather than eliminating:** a
  redirected or hostile baseline now yields a **wrong component list, which the comparison itself
  detects**, instead of arbitrary code execution with the developer's credentials. Everything
  built for the old mechanism is **KEPT as defence in depth** — identity/transport/host/path
  pinning, the isolated fetch (URL written into a 0600 config by a shell builtin so it never
  enters `argv`), the verified transfer hop, the mode-dependent bound, shallow ancestry, the
  redact+flatten detail path. **The local manifest is ASSERTED against the running array on every
  run** (`manifest-missing`/`-garbage`/`-stale`, fail-closed, ORDER included), and that assert is
  what makes a manifest baseline trustworthy at all: without it the file is an unverified claim,
  and a branch that grew `COMPONENTS` without regenerating the manifest would — once merged —
  leave `main`'s manifest SHORT, so every later branch would compare against a too-small baseline
  and silently excuse real skew. Regenerate with `{ sed -n -e '/^[^#]/q' -e p
  scripts/agent-gate.components; scripts/agent-gate.sh --list; }` and commit it.
  **One TRANSITIONAL fallback, also data-only, and it is UNREACHABLE BY ASSERTION rather than by
  reasoning:** the baseline's tree is **probed first**, as its own step, with **three** outcomes —
  `present` ⇒ the manifest and NOTHING ELSE (every failure of that read is an ERROR; the textual
  path is a **hard refusal** here), `verified-absent` ⇒ the gate script's **single-line top-level
  `COMPONENTS=(…)` declaration extracted AS TEXT** (never executed), `could-not-tell` ⇒ **REFUSE**
  (`baseline-probe-unmeasured`). "The fallback is self-limiting" was true and **not enough**: that
  is a property someone reasoned about and nothing measured, so a refactor or a deleted manifest
  would silently re-enable the brittle path — a pass derived from the ABSENCE of a bad signal.
  **`git show` cannot answer the question**: its non-zero exit conflates "no such path" with "bad
  object" with "unreadable repository", so absence is never inferred from it; `git ls-tree <rev> --
  <path>` separates them affirmatively (rc 0 + an entry / rc 0 + NO entry / rc ≠ 0), and a non-blob
  entry is its own refusal. The payoff is **mechanical expiry instead of trust**: once the manifest
  is on `main` every baseline measures `present`, so path 2 is dead code that any attempt to enter
  ERRORS. The extractor refuses loudly on any shape it does not recognise and **NAMES it** ("is not
  a SINGLE-LINE literal"), so a future reflow on `main` — which would refuse for **every branch at
  once**, fail-closed rather than a false green — surfaces as that sentence and not as a mystery.
  Every baseline-bearing verdict line ends by naming its baseline source, so use of the fallback is
  visible rather than inferred.
  And `origin` must **NAME the canonical upstream**, HOST INCLUDED
  (`github.com/pmcfadin/cqlite`, one hard-coded literal, EXACT equality after normalising the
  spellings git accepts — scheme forms, scp-like, userinfo, an ssh port, `www.`, `.git`, case):
  `origin` merely EXISTING made `git remote set-url origin <anything>` a git-config-shaped
  opt-out, and it fires BY ACCIDENT in the fork workflow, where a contributor's fork `main` is a
  stale baseline stamped `PASS`. **An OWNER/REPO-only match is NOT enough, and "err toward
  accepting an ambiguous host" was WRONG here** — it accepted `evil.example/pmcfadin/cqlite` and,
  needing no hostile host at all, ANY LOCAL PATH ending in those two segments — which, while the
  pre-flight still RAN the baseline's copy of the gate, admitted arbitrary code and not merely a
  wrong baseline (identity and execution were one concern, not two). Under REQ-3544-01 what a
  loose identity buys is a baseline of unknown PROVENANCE, from which no PASS may be derived, so
  the check stays exactly as strict — as defence in depth rather than as the only thing standing
  between a re-pointed remote and code execution. Anything unverifiable from the string (an ssh alias, a mirror, a local
  path, `file://`, a look-alike host) is a NAMED non-PASS, as is a URL-less `origin`. **And the URL grammar is CLOSED AXIS BY AXIS, because three rounds were "too permissive" in
  a NEW place each time** (no host; host but no transport; then `http://`/`git://` accepted):
  transport (**`https://` ONLY since #3544/job 296** — `http://` and `git://` authenticate
  nothing, so an on-path impersonator supplies the objects this run certifies against; when the
  rule was written those objects were EXECUTED, which is why it was a High. **`ssh://`,
  `git+ssh://`, `ssh+git://` and scp-form `git@host:path` WERE accepted and are now REFUSED
  (`ssh-transport:<form>`)**: the isolated environment must admit `HOME` for key and
  `known_hosts` discovery, so OpenSSH still honours **`~/.ssh/config`**, where a
  `Host github.com` rule rewrites `HostName` or runs `ProxyCommand`/`Match exec`. That is a
  redirected baseline AND arbitrary execution behind a URL string that passes the identity check.
  It is IN MODEL because HOME IS SHARED — every lane runs as one user with a writable
  `/home/ubuntu`, so the planter is a PEER LANE, not the invoker (an invoker editing their own
  config is out of model; a non-invoker route is a defect — same shape as #3617). It was met by
  **DESCOPE, not hardening**, under the standing ruling on this family: a bounded residual was
  unavailable because `ProxyCommand` EXECUTES, and the usual mitigation — a redirected baseline
  degrades to a wrong component list the comparison detects — does not reach a harm that lands
  during TRANSPORT, before any comparison. Measured cost: nil, every lane and CI already use
  https; an ssh-form checkout now fails closed with the remedy named), host,
  port (default only), path, and userinfo (ACCEPTED — GitHub Actions writes
  `https://x-access-token:<TOKEN>@github.com/…`, so rejecting it would red a legitimate CI
  checkout — and therefore REDACTED everywhere it is rendered, since SUMMARY blocks get pasted
  into PR comments). Each axis has one stated rule beside the check; a new variant would be a
  change to git's URL syntax, not a gap. **The baseline is fetched into a PRIVATE per-run
  `refs/worktree/…` ref, never `FETCH_HEAD`**: `--refmap=` removed the shared *tracking* write
  and left `FETCH_HEAD`, which is itself one shared mutable file a concurrent fetch overwrites
  between the fetch and the read — the run would then compare against a commit it never fetched. **And `--is-ancestor`'s rc 1 is itself three-valued**: in a SHALLOW clone it
  also means "the connecting history is absent", so rc 1 is definitive only in a repo PROVEN
  complete (`unknown` shallowness ⇒ INDETERMINATE) — otherwise a legitimate committed removal
  in a shallow checkout reds as BEHIND. **Corollary
  for tests**: hermetic fixtures use local origins, so they SUBSTITUTE THE ARTIFACT — one shared
  helper rewrites the canonical literal in the fixture's own scratch copy of the gate and verifies
  the pin took (`scripts/tests/lib/agent-gate-canonical-pin.sh`) — never a settable seam. The
  first design let local paths through so the fixtures would work, i.e. **the test hook and the
  vulnerability were the same fact**; and the check REGRESSED three suites whose local origins it
  rejected (`test_agent_gate_delta.sh`'s two real `--delta` fixtures stopped at the pre-flight
  instead of reaching their REFUSED paths — a `tooling-tests` FAIL invisible to `--lite`). Or `FAIL-CLOSED … baseline NOT measured (<kind>)` for a
  failed fetch/absent `origin`/an empty or ungrammatical baseline manifest/a baseline declaration
  that cannot be read as text/an unreadable baseline-or-`HEAD` set/**a host on which the probe
  cannot be BOUNDED** (in which case the fetch is not run at all — an unbounded fetch
  could hang `--lite` on a stall or an auth prompt, and a missing capability must not inherit
  the permissive branch) — **never a SKIP and never a fallback to an empty baseline**, which
  would excuse every branch. A branch-only
  component is NOT skew. Fail-closed in the **certifying** modes (full, `--delta`); `--lite`
  and `--only` stamp the same line `ADVISORY-*` and cannot fail on it. **No opt-out env var,
  and none may be added** — rebasing is always available, so an escape hatch could only buy a
  vacuous green.
- **A run whose worktree mutates MID-RUN cannot certify (#2926).** Every mode captures a tree
  identity at start, re-verifies it at each component boundary + the terminal emit, and FAILs closed
  with `tree-integrity: FAIL (tree-mutated-midrun; head <a>→<b>; changed: …)`. Every SUMMARY carries
  `tree-start:`/`tree-end:`/`tree-integrity:`, so **closers verify `tree-integrity:` alongside
  `RESULT:`** — a mutated-mid-run block is not a certification and cannot be pasted as one. The
  block's `commit:`/`dirty:` are derived from that verified capture, never a fresh emit-time git
  read. No env var bypasses it; remedy is to re-run on a stable tree (don't edit a worktree while
  its gate runs).
- **Every component line NAMES the feature matrix it ran, in ALL THREE modes (#3453).**
  `core-tests: PASS (412s)  [test cqlite-core --features cli-helpers]` — read as
  `<subcommand> <scope> <features>`, one entry per distinct invocation, `xN` for repeats. A bare
  `PASS (412s)` could not distinguish a run that certified the OTLP stack from one that never
  enabled it, which is this issue's whole subject. It is **DERIVED, never curated**: `cargo` and `env` are shell FUNCTIONS in the gate, so a
  matrix is described from the REAL argv about to execute. **AND IT RECORDS EXECUTION, NOT INTENT.**
  The eight components whose cargo calls live in a single-quoted `bash -c` body (core-tests'
  nextest branch, memory-budget, integration-tests, write-tests, cli-tests,
  compaction-byte-parity, minimal-build, smoke) first declared their sets in the PARENT, before the
  child ran — so `cli-tests: FAIL` named BOTH of its feature sets even when Pass 1, or the
  fail-closed target derivation above it, died before Pass 2 started, and write-tests claimed the
  same set `x3` after failing on the first of three `&&`-chained passes. **A failure summary that
  claims an invocation which never occurred is affirmatively false, and strictly worse than
  silence** — it is what stops the next person looking. Each body now calls the EXPLICIT recorder
  `_fm_observe_child` on the line immediately BEFORE each cargo command, from the same hoisted
  package/feature variables the argv is built from, so a short-circuit records nothing later. The
  cargo/env INTERCEPTORS stay deliberately NOT `export -f`-ed — exporting an interceptor would make
  every bash DESCENDANT record, so `tooling-tests` (which runs nested agent-gate self-tests) would
  attribute a nested run's cargo to itself — while `_fm_observe_child`, which intercepts nothing and
  fires only where a body calls it by name, IS exported (with the gate's own `_fm_describe_cargo`,
  so there is no second formatter to drift). It **never renders blank**: `[UNDECLARED]` (cargo
  expected, nothing observed), `[no-cargo]`, `[via <driver>: feature set NOT observed]`,
  `[cargo not observable: <why>]`, or a named SKIP / FAILed-before-its-first-cargo /
  never-reached-its-driver state; a long list abbreviates as `33:a,b,c,+30 more`, never
  a silent truncation. **A driver we cannot see is NAMED, not guessed** — `python-bindings`,
  `node-bindings` and the `--lite` scoped-tests PYTHON TIER (whose maturin build runs in a child
  process) render `via <driver>: feature set NOT observed`, ADDITIVELY beside the rust sets a mixed
  diff also observes (`[test cqlite-core --features cli-helpers | via maturin: feature set NOT
  observed]`): "nobody said" and "known to be indirect, therefore unobservable" are different facts
  and only one of them is a defect.
  **AND THE CLASS DECIDES WHAT MAY BE CLAIMED — three rules, from one family of findings (roborev
  job 273).** (1) A component whose cargo runs ONLY IN A CHILD PROCESS is **never class `cargo`**:
  the interceptors are unexported by design, so `cargo` means "observable in this shell (or
  self-recorded from a `bash -c` body)". `tooling-tests` was declared `cargo` while its only cargo
  runs inside ~60 nested test scripts, so a PASS read `[UNDECLARED]` and a FAIL could claim it
  "FAILed before its first cargo invocation" after a child `cargo build` really ran — hence the
  fourth class `unobservable:<why>`, which asserts NOTHING in either direction and takes no
  SKIP/FAIL note. (2) An `indirect:<driver>` component must **RECORD whether its driver was
  REACHED, from an explicit signal** (a build-verify rc, or a recorder call on the line before the
  driver runs) — never inferred from the terminal status: `python-bindings` can die in venv/pip
  before maturin and `node-bindings` in `npm ci` before `npm run build`, and both used to claim an
  unobserved cargo run. An indirect component with NO record renders `UNDECLARED` **naming the
  driver** — a visible recording gap, not a claim. (3) The misclassification is now
  **MECHANICALLY DETECTABLE**, because the census that missed (1) READ THE TABLE instead of
  exercising it: every `cargo`-class component is RUN under `--only` with a recording shim `cargo`
  and an `UNDECLARED` annotation is a FAIL, while a component that cannot be exercised without
  recursion (`tooling-tests` runs the guard) must be declared non-`cargo` — also a FAIL.
  **Observation beats declaration** — a component
  declared `no-cargo` that IS observed running cargo renders the observed sets plus
  `!declared-no-cargo`, so a mis-declaration self-corrects. Guard (hermetic, in
  `tooling-tests`): `scripts/tests/test_agent_gate_feature_matrix_annotation.sh` — every
  `COMPONENTS` name must resolve to a declared class (a new component cannot join with a blank
  matrix), all six emit sites must route through the one renderer, the DECLARED matrix of each
  `bash -c` component must equal the argv that ACTUALLY EXECUTED under a recording PATH-shim
  `cargo` (described through the gate's own `_fm_describe_cargo`, never re-derived), and the same
  differential is re-run under a FAILING shim, where each body must name exactly the one invocation
  it reached — with the short-circuit proved by measurement (strictly fewer invocations than the
  passing run) rather than assumed.
- **Every SUMMARY's `cpu-budget:` line says WHERE the slot cap came from (#3414):**
  `max-concurrency=N(pinned|default|invalid|clamped)`, the same idiom as `build-jobs=N(derived|caller)`
  beside it. `pinned` = a valid `CQLITE_GATE_MAX_CONCURRENCY` >= 1 used verbatim; `default` = the var
  is UNSET so N is the #1825 formula; `invalid` = it was EMPTY or non-numeric and was silently
  discarded for the formula; `clamped` = it was a valid integer < 1 and was silently raised to 1.
  Read `N(default)` on a fleet box as **the pin is not provisioned** — `3` and `3 because nothing set
  it` are different operational facts, and the second one is what ran unseen for months. `invalid`
  and `clamped` exist because `${VAR:-dflt}` cannot tell unset from set-empty (`${VAR+set}` can), so a
  mis-set variable was textually identical to a healthy defaulted box.
  **THE REMEDY DIFFERS BY TOKEN, and getting that wrong sends an operator in a circle.** A
  `default` box has NO pin line, so `bash scripts/bootstrap-agent-machine.sh --fix-gate-pin`
  (or `--yes`) persists one. An `invalid`/`clamped` box ALREADY HAS the line, with a bad value —
  and bootstrap deliberately never rewrites an existing value (a box running >1 gate on purpose
  must not be clobbered), so re-running it is a **silent no-op**: fix the VALUE in
  `/etc/environment` by hand. Bootstrap says the same thing at the same fork, as
  `gate-pin: NOT-HONOURED`.
- **Every component line states WHAT IT VERIFIED, not just how long it took — and a component
  that verified NOTHING cannot report PASS (#3625).** `PASS (0s)` was indistinguishable, in a
  pasted block, from a component that did nothing. A duration is a PROXY for work; a COUNT is
  the work. So `_fm_summary_line` now appends a census suffix — `{verified: 3562 tests passed}`,
  `{verified: 2 test binaries built/verified}`, `{no census — <declared reason>}`,
  `{census NOT-MEASURED: <reason>}` — plus ONE aggregate `census:` line per block. **The
  measured oracle behind it, and the answer to the issue's two-run comparison: cargo caches
  COMPILATION, never test EXECUTION** — a WARM `cargo test` re-prints `test result: ok. N
  passed` and a WARM `cargo test --no-run` still prints one `Executable ` line per binary — so
  those `0s` lanes DID re-verify their subjects and the count was in the log all along; nothing
  put it in the SUMMARY. A `libtest`/`compile`/`both` lane whose measured subject count is ZERO
  is recorded as **`VACUOUS`**, a fourth component-status token beside PASS/FAIL/SKIP, and it
  fails the run. **That required making every aggregation AFFIRMATIVE**: `[ "$st" = FAIL ] &&
  OVERALL=FAIL` failed only the ONE named bad token, so every other value — an unrecognised
  token, an empty result file, VACUOUS itself — took the permissive branch; `_status_is_nonfailing`
  is now a closed set (PASS, SKIP) and everything else fails. Two states are DECLARED and
  deliberately NON-FATAL, because a lane that reds on correct input is the lane agents learn to
  waive: `NOT-MEASURED` (an unreadable log, a failed ANSI strip, an unrecognised driver report)
  and `gap:<reason>` (14 components today — fmt, clippy, all-features-check, the shell/python
  guards, smoke, tooling-tests — each PRINTING its reason every run). Neither is ever read as
  verified: the aggregate line counts them separately and always as `N RECOGNISED`, never a bare
  `N`, and it DECLARES its own non-exhaustiveness, because the gap set is curated. One asymmetry
  worth knowing: for a cargo lane the subject markers are cargo's OWN guaranteed output, so their
  absence really does mean nothing ran — but for `indirect:<driver>` (python-bindings/pytest,
  node-bindings/jest) an ABSENT tally is `NOT-MEASURED`, since a third-party report format is not
  ours and its absence is a measurement failure, not proof of vacuity. **#3400 HAS A SECOND
  DIMENSION, AND QUIET IS IT**: that rule is about a cargo-output parse keyed on a
  PRESENTATION property, and an anchor can be perfectly colour-immune while still depending
  on another one. `CARGO_TERM_QUIET=true` in the environment, or `[term] quiet = true` in any
  `.cargo/config.toml`, suppresses EVERY cargo status line — measured: a
  `cargo test --lib --no-run` under quiet emits a COMPLETELY EMPTY log — while leaving
  libtest's `running N tests`/`test result:` untouched. Neither is visible at the call site,
  so a box carrying either would have made `feature-iso-parquet` and `minimal-build` measure
  a *zero* `Executable` count and read VACUOUS on every gate, fleet-wide, on correct input.
  The fix is THREE-VALUED, not an env override (#3400 records that moving correctness into a
  setting far from the parse is the worse coupling): the tally reports
  `<Executable lines> <cargo status lines>`, and a log with **no cargo status output at all**
  is `NOT-MEASURED (suppressed)` while only a log that demonstrably carries status output
  *and* zero `Executable` lines is a real `ZERO`. Generalise: **"the marker is absent" and
  "the marker could not have been printed" are different facts, and a fatal state may only
  be derived from the first.** Declaration site:
  `_census_kind` (a CLOSED set; an undeclared component is a named FAIL, so a new component
  cannot join with a blank census) — **and that guarantee is only as strong as WHERE the
  state is judged**: the verdict coupling used to return every non-`PASS` status untouched,
  so `UNDECLARED` was not fatal when the component SKIPped, i.e. the completeness rule failed
  exactly on a NEW component that SKIPs on the box where it is first run. The census RECORD
  is now judged before the run's status (an unsound record is a fact about the TABLE, not
  about this run), and only then does the status decide. **BUT A STATIC DECLARATION IS NOT
  ALWAYS POSSIBLE, AND
  ASSUMING IT WAS COST A HIGH**: `scoped-tests` was declared `both`, and a diff confined to
  `bindings/python/**` dispatches NO cargo at all (`classify_scoped_plan` diverts `cqlite-py`
  and the `cqlite-core` fallback is deliberately guarded on `python_diff -eq 0`), so its log
  holds only maturin + pytest output and the lane measured ZERO — reddening a CORRECT `--lite`
  fix round and a CORRECT `--delta`, a certifying mode. A lane whose SUBJECT DEPENDS ON WHAT
  THE RUN ROUTED TO gets the `runtime:<why>` kind and writes its own record from the same
  routing variables the dispatch was made from; "no executable subject was dispatched" is an
  affirmative `NOT-APPLICABLE`, never `VACUOUS`. **The general rule: before declaring a lane's
  subject, ask whether the lane always HAS that subject — a kind that is right for the common
  route and wrong for a rarer one is a guard that reds on correct input.** **Its domain is WIDER than `COMPONENTS`, and getting that
  wrong was measured, not theorised**: a name reaches a component line from `COMPONENTS`, from a
  `NAMES+=("<literal>")` append in the `run_delta_*` helpers, AND from a `record_result
  "<literal>"` call — the #2926 `tree-selftest` hook is the third kind, and enumerating only the
  first two rendered its row `FAIL` in a real self-test block. Guard: `scripts/tests/test_agent_gate_census.sh`
  (`tooling-tests`), which plants a no-op in a real component under `--only`, requires the block
  to NAME it, and carries a positive control on the same lane differing in ONE property.
  **ADDING A STATUS TOKEN INVALIDATES EVERY HARD-CODED STATUS-SET LITERAL, including the ones in
  the test suites**: three `(PASS|FAIL|SKIP)` alternations survived `VACUOUS`'s arrival, and the
  failure direction is the nasty one — such a pattern stops SEEING exactly the rows that report a
  component verified nothing (one of them then REDDENED A CORRECT boundary block, because a
  sibling count did see them). `test_agent_gate_census.sh` case R1 is the standing sweep; its
  needle is deliberately SPLIT so the guard cannot match its own source, and case R2 proves it
  discriminates the bare three from the roborev block's longer verdict vocabulary, which
  legitimately begins with the same tokens.
  **Two lessons from its review worth carrying elsewhere. (1) A "present-and-zero" tally has more
  than one spelling, and keying on the GOOD word misses all the others**: the pytest reader matched
  `N passed`, so every terminal summary reporting zero passed WITHOUT that word — `61 skipped in
  1.20s`, `1 xfailed in …`, `2 deselected in …`, `3 errors in …` — fell into the ABSENT branch,
  which is `NOT-MEASURED` and therefore PRESERVES `PASS`. A suite whose every test was skipped is
  the vacuous pass this mechanism exists to catch, so RECOGNISE THE SUMMARY LINE FIRST (an outcome
  pair from the driver's own closed vocabulary **plus** a duration tail) and read the count off it
  second. **(2) A near-miss in a FORMAT STRING can hide an entire emit path from a uniformity
  guard**: #3453's B1 grepped for the literal `printf '%-18s %s (%s)'` while the tree-integrity
  BOUNDARY printer spelled its format `(%ss)` — one character — so a whole mode rendered component
  rows with neither annotation and the guard reported zero bypasses. The needle is now the `%-18s`
  NAME FIELD (comment-blind), whose only legitimate occurrence is the renderer's own definition.
  Generalise: **when you assert "everything goes through ONE X", key the assert on the narrowest
  thing that MAKES it an X, never on a whole literal a caller can spell differently** — and
  re-derive the emit-site set from the code rather than from a count someone wrote in a report.
  **(3) A LABEL MAY NAME A STATUS ONLY IF IT WAS DERIVED FROM THE OBSERVED STATUS — this issue
  produced FOUR findings of that one shape** (a progress line printing `PASS` beside a
  `VACUOUS` SUMMARY; a FAILing `gap:` component counted as `DECLARED-GAP` rather than
  not-applicable; `NOT-APPLICABLE` labelled `(SKIP/FAIL)` on a row that PASSes, once the
  `runtime:` route made that pair reachable; and the `ZERO` STATE counted under a heading
  reading `VACUOUS`, a STATUS word, which a shipping mode already contradicted by emitting
  `fmt: VACUOUS (0s)` beside `0 VACUOUS`). The root was structural — the aggregate took
  component NAMES and no statuses, so every status word in it *had* to be an assumption about
  which statuses reach a given state. It takes name/STATUS pairs now, the state buckets carry
  no status word, and the two status-derived figures are counted from the status. **Ask of
  every label: is this word derived from the state I am rendering, or from an assumption about
  which states get here?** And prefer *deriving* the qualifier to deleting it — `(did not
  PASS)` carries real information when it is true. **(4) THE SAME ROOT APPEARED A THIRD TIME, IN
  THE RENDER-TIME FALLBACK, AND THE ANSWER WAS CONVERGENCE RATHER THAN A SIXTH PATCH.**
  `_census_measure` (verdict time) and `_census_record` (render time) answer the same question —
  what is the truthful census state for (component, status)? — and answered it differently for
  five rounds, because the fallback *took no status* and dispatched on kind alone: a gap-declared
  component that CRASHED before `record_result` rendered its GAP reason. Both now delegate to one
  `_census_classify`, with exactly one declared asymmetry (only the measurer may read the
  component log), and `test_agent_gate_census.sh` case S1 drives BOTH over the same 64-cell
  (kind × status × sidecar) matrix requiring identical output wherever the log is not needed —
  because **a second implementation's agreement is only knowable by testing it**. Generalised:
  when two functions answer one question, converging them and pinning the agreement ends the
  class; patching the sixth label does not. **(5) AND A COUNT OF INPUTS IS STILL A PROXY** — the
  delta `node-tests` lane censused *the number of changed files it selected*, which is this
  doctrine's own premise ("a duration is a proxy for work; a count IS the work") violated inside
  its implementation, and wrong in BOTH directions at once: jest EXITS 0 when every selected test
  is skipped, so an all-skipped run reported a confident count and kept its PASS, while a changed
  HELPER runs the WHOLE suite and was censused as one file. The subject must be what the DRIVER
  reports it did, so that lane is `indirect:jest` like `node-bindings` — one tally, not two. **The
  sibling question is answered AT THE DECLARATION rather than left for the next reviewer**:
  `shell-selftests` keeps "scripts executed" because `_run_shell_selftest_files` invokes every file
  unconditionally (selected == executed, which is exactly what was NOT true of jest) and because no
  uniform per-script assertion tally exists to prefer; its residual — a script that runs and
  asserts nothing — is declared there too.
- **Admission is re-taken AT SLOT GRANT, and every FULL SUMMARY carries a `disk-admission:`
  line naming BOTH readings (#3755).** Admission and CONSUMPTION are different moments: a gate
  admitted with 167G free can queue an hour behind a peer and begin building at 30G — the queue
  wait wasted, the build aborting into a floor **while still holding the slot**. So one predicate
  is evaluated twice inside `acquire_gate_slot` (which self-exempts `--lite`/`--delta`/`--only`, so
  the guard is full-gate-only by construction): once at LAUNCH, **ADVISORY** — a low reading there
  can be freed by the very peer we are about to queue behind, and a guard that reds on correct
  input is the guard agents learn to waive — and again the instant the slot is granted, before
  `_tree_recapture_after_slot` and the first component, where it is **FAIL-CLOSED**. A refusal
  RELEASES the slot first, then emits a complete terminal block; `RESULT` stays **`FAIL`**, never a
  new token (`RESULT: REFUSED` would break the mandated `grep -qE 'RESULT: (PASS|FAIL)'` completion
  probe and reintroduce #3041 from the other side — a poller would read a FINISHED refusal as
  still-running), so distinctness is carried by the named `disk-admission: FAIL-CLOSED (#3755)` and
  `refusal:` lines, the `missing-fixtures`/`missing-schemas` precedent. The line always states the
  value observed, the bar, and `evaluated 1x|2x`, because *"admitted once"* and *"admitted twice"*
  are the whole point. The bar is `CQLITE_GATE_MIN_FREE_GB`, **default 40GiB**, carrying a
  `default|pinned|invalid|clamped` source token for the #3414 reason (`${VAR:-40}` renders an unset
  and a mis-set variable identically). A reading that could not be TAKEN is `UNMEASURED (<why>)`,
  **declared and non-fatal at both moments** — the cap's own doctrine is that the gate must never
  be un-runnable because of the cap — but never silently: the block says the bar was NOT APPLIED
  rather than PASS. Self-test: `scripts/tests/test_agent_gate_disk_admission.sh` (in
  `tooling-tests`), which drives the readings with a PATH-shim `df` and a real queued slot, never a
  seam in the shipped script.
- Every SUMMARY carries an `accelerators:` line (sccache/nextest/lane state, plus a `mold=` token and
  a `perf=` profiling-capability token on Linux hosts, #2859/#3249) — degradation there is
  actionable, not noise. `perf=paranoid-<N>`/`kptr-restricted` means THIS BOX CANNOT BE PROFILED (a
  PERMISSION verdict, not a missing capability): re-run `bash scripts/bootstrap-agent-machine.sh
  --yes`, which installs + verifies `/etc/sysctl.d/99-cqlite-perf.conf`. Self-test:
  `bash scripts/tests/test_agent_gate_summary.sh`.

