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
