# CQLite Agentic Quality Gate Audit

## Inventory

**Gate Entry Point**: `scripts/agent-gate.sh` (2,707 lines)
- **Full gate** (default): runs 21 components, ~12–25 min on warm machine with sccache/nextest
- **--lite**: ~1–5 min; fmt + file-size + scoped clippy + blast-radius-scoped tests (touched package `--lib` + new `--test` targets)
- **--delta**: ~2–8 min; post-PASS polish (test/docs-only re-cert); fail-closed on any src/script/config change
- **--only**: debugging aid; runs named components, marked PARTIAL (never counts as gate)
- **--emit-summary-selftest**: proves SUMMARY survives capture, exits before any work

**Supporting Scripts**:
- `scripts/lib/gate_slot_daemon.py` (111 lines): background lock-holder for machine-wide concurrency cap
- `scripts/delivery-telemetry.py` (650+ lines): ledger + retro for pipeline self-improvement
- `scripts/tests/test_agent_gate_summary.sh` (37K): regression tests for SUMMARY emission + capture
- `scripts/tests/test_gate_concurrency_cap.sh` (18K): hermetic stub tests for slot daemon behavior
- `scripts/tests/test_agent_gate_delta.sh` (27K): delta mode edge cases
- `scripts/flow/finalize-cleanup.sh` (15K): worktree + branch cleanup on issue close
- `scripts/local/pre-merge.sh` (10K): local merge checklist (deprecated; redundant w/ gate)

## How It Works

### Full Gate: 21 Components (Run Sequentially / In 2 Lanes)

1. **fmt** (fast): `cargo fmt --all --check`
2. **clippy** (medium): scoped workspace `-D warnings` (excludes duckdb C++ / OTEL; full coverage via nightly)
3. **core-tests** (8–12 min): `cargo test -p cqlite-core --features cli-helpers` ± nextest parallelism + `cargo test --doc`
4. **scan-offload-guard**: 3 feature-gated windowed-scan + I/O-offload regression tests
5. **work-counters-guard**: wiring-evidence tests for read/parser work counters
6. **byte-budget-guard**: error-threshold tests for `Error::ResultTooLarge`
7. **memory-budget** (slow): dhat allocator regression; pins peak-heap <6 MB, full-scan <252 MB
8. **integration-tests**: compile all targets (`--no-run`), run 7 enforced CI ones
9. **format-compat**: tests the `oa` format crate (folded into workspace 2026-07-01)
10. **write-tests**: write-support roundtrip + compaction parity
11. **cli-tests**: CLI unit tests + write-readback content proof
12. **python-bindings**: maturin develop + pytest in throwaway venv; SKIPs (no silent PASS) if python3 absent
13. **node-bindings**: napi build + write-readback-content Jest proof; SKIPs if node/npm absent
14. **parity-report**: cassandra-parity `--check` (static goldens only; Docker tests in nightly)
15. **binding-unwind-profile** (fail-closed): inspect 4 build configs for `--profile release-unwind` (not `--release`)
16. **tooling-tests**: shell-tooling regression tests (4 self-tests: summary emission, generator scoping, UDT tuple shape, concurrency cap)
17. **minimal-build**: `cargo build --no-default-features --features all-compression` (pure library test)
18. **smoke**: `bash test-data/scripts/smoke-test-all-tables.sh`
19. **file-size**: campsite-rule ratchet; FAILs if a change grows an over-threshold file
20. **delivery-telemetry** (issue #1283): optional; records cycle-time + phase-duration telemetry
21. **compaction-byte-parity** (issue #1340): byte-for-byte CQLite vs Cassandra compaction proof

### Execution Model

- **Component parallelism**: Bounded pool (default `min(4, ncpu/2)`, cap 4); each component records verdict to file
- **MAIN lane**: shared target dir, serial build (cargo's own advisory lock)
- **SIDE lane** (issue #1737): isolated CARGO_TARGET_DIR for bindings (kills cross-lane build-lock / feature cache thrashing)
- **Bash 3.2 fallback**: no `wait -n` → serial execution (e.g. macOS stock bash)
- **Machine-wide cap** (issue #1825): N = `max(2, floor((ncpu-2)/4))`; full gates queue at cap (--lite/--delta/--only exempt); Python daemon holds fcntl.flock in background; released on gate exit via daemon SIGTERM

### Accelerators (Auto-Detected, Loud When Absent)

| Accelerator | What | Impact | Detection | Disable | Warn if absent |
|---|---|---|---|---|---|
| **sccache** | Cross-worktree compile cache | +25.6% speedup (fresh worktrees) | `command -v sccache` | `CQLITE_DISABLE_SCCACHE=1` | Yes; `brew install sccache` |
| **cargo-nextest** | Parallel test execution | 2–4× faster core-tests | `command -v cargo-nextest` | `CQLITE_DISABLE_NEXTEST=1` | Yes; `brew install cargo-nextest` |
| **Parallel lanes** | MAIN + SIDE component concurrency | Collapses wall-clock | bash ≥4.3 + AGENT_GATE_JOBS>1 | `AGENT_GATE_JOBS=1` | Yes (bash 3.2); `brew install bash` |

Every SUMMARY block carries a machine-checkable `accelerators: sccache=<state> nextest=<state> lanes=<state>` line (state: `on`|`absent`|`off`|`serial`).

### Summary Emission + Recovery (Issue #1175)

**Authoritative artifact**: CALLER-KNOWN file, path chosen in advance
- **Default**: `$PWD/.agent-gate-summary.txt` (gitignored, per-checkout)
- **Override**: `$AGENT_GATE_SUMMARY_FILE` (resolved relative to invocation CWD before any `cd`)

**Robustness**:
- Complete block written to file FIRST (no pipe), via `>` redirection (immune to SIGPIPE)
- File invalidated on startup with sentinel `RESULT: INCOMPLETE (gate did not finish)`
- Block verified by write RC + non-empty + end-marker + THIS run's `run-id:` line (defeats stale files)
- If write fails, LOUD `⚠️` on STDERR, gate exit forced to non-zero, fallback blocks printed to stdout+log
- Best-effort stream to stdout (lost under leaked child / until-EOF reader is OK; file is complete)
- Copy to `$LOG_DIR/agent-gate-summary.txt` for logs bundle (skipped if write failed; fallback writes THIS run's block)

**Per-run identifier**: `run-id: <sha-like>` (random UUID) stamped in block; printed once at startup

### Tier Semantics

| Mode | Components | Time | Recovery File | SUMMARY Mode | Use Case |
|---|---|---|---|---|---|
| Full (default) | All 21 | 12–25 min | `.agent-gate-summary.txt` | `full` | Pre-merge gate of record |
| **--lite** | fmt + file-size + clippy + scoped tests | 1–5 min | `.agent-gate-lite-summary.txt` | `lite` | Fast iterate loop (each fix round) |
| **--delta** | fmt + file-size + changed test targets | 2–8 min | `.agent-gate-delta-summary.txt` | `delta` | Post-full-gate polish (test/docs only) |
| **--only** | Named subset (debugging) | varies | (same file as full) | `partial` | Ad-hoc investigation |

**Note**: `--lite` and `--delta` emit DISTINCT summary blocks that can NEVER be pasted as the full SUMMARY. Full gate must PASS once before merge; `--lite` every fix round.

### Git Metadata + Datasets

- **Commit**: captured at startup (SHA, branch, dirty flag)
- **Datasets**: preflight checks for Data.db files; counts present; SKIPs dataset-dependent components if absent (issue #646)
- **Smoke test**: all 33 tables; requires datasets

### Deterministic Features (No Heuristics)

- Cargo metadata parsing (jq or python3) maps changed `.rs` files to `--test` targets with required-features
- No-metadata fallback: scope to package `--lib` only (skip `--test` targets; emit note pointing to full gate)
- Silent SKIPs only: python3/node/cassandra-parity absent → component skips (SKIP-aware, loud on stderr)
- Fail-closed guards: binding-unwind-profile, byte-budget, memory-budget, etc.

## Measured / Observed Costs

**Full gate on M1 Mac (warm sccache, nextest present)**:
- fmt: ~0.5s
- clippy (scoped): ~45s
- core-tests (nextest): ~6 min (vs ~15 min serial)
- integration/write/cli/python/node/smoke: ~3–5 min combined
- **Total**: ~12–15 min (issue #1737 measurement)

**--lite on changed package**:
- Touched package `--lib` + new `--test` targets: ~1–5 min
- Python diff routes to maturin develop + fast pytest: adds seconds (warm) to ~1–3 min (cold)

**Machine-wide cap (issue #1825)**:
- Default N = `max(2, floor((ncpu-2)/4))`; on 8-core = 2, on 16-core = 3
- Queue wait after first 3.6s (grace period); one poll every 0.2s until slot free
- Daemon startup: <10ms; daemon exit: instant (fcntl auto-release on close)

**Slot daemon PID-reuse caveat**:
- If a SIGKILLed gate's PID is recycled to an unrelated process in the daemon's next poll interval (low probability), slot stays held until that process exits
- Trade-off accepted: rare, brief delay only; avoids complicating the probe

## Friction Points

1. **Large file size (2,707 lines)**: agent-gate.sh is expensive to read/edit; agent context bloat for any change
2. **Bash 3.2 fallback silent degradation**: macOS stock bash serializes components (no `wait -n`); WARN emitted but visible only to stderr; caught in selftest but users may miss it
3. **Dataset preflight coupling**: dataset-dependent components auto-SKIP on empty checkout (silent until smoke runs); hides failures until datasets fetched (issue #646 context)
4. **Metadata parser availability**: `--lite` silently falls back to package `--lib` only when jq/python3 absent; agents on minimal sandboxes may not get `--test` coverage
5. **Python3 dependency for slot daemon**: cap fails open (loud) if python3 unavailable, but error handling adds startup latency (acquire_gate_slot blocks until daemon ready or dies)
6. **Cargo metadata call**: runs `cargo metadata --no-deps` on EVERY gate invocation (even --only); adds ~0.5–1s overhead to all runs
7. **Nextest + doctests split**: nextest doesn't run doctests; core-tests runs BOTH nextest + `cargo test --doc` (redundant fixture loads); affects coverage if either path silently broken
8. **Work-counter features**: wiring evidence tests hidden behind `work-counters` feature; gate must explicitly include them or regressions ship silent
9. **Scope coupling between --lite and full**: blast-radius scoping logic duplicated; changes to component list risk drifting the --lite eligibility heuristics
10. **Post-gate polish (--delta) complexity**: full-gate PASS anchor logic + sha-chain verification adds code paths; one botched --delta-summary-file flag silences the re-cert (no recovery artifact)

## Open Questions

1. **Component ordering**: why serialize all 21 components rather than run more in parallel (beyond MAIN+SIDE lanes)? Cargo's lock + cross-component artifact coupling?
2. **Minimal-build scope**: builds `cqlite-core --no-default-features` only; doesn't test bindings or CLI under minimal features. Intentional (CLI requires full features)?
3. **Parity-report coverage**: static goldens only at gate; Docker tests deferred to nightly. What would it cost to run 1–2 live Cassandra tests at gate?
4. **Memory-budget ratchet**: pins today's measured heap; how often does it drift? Is there a CI lane that validates against real prod workloads?
5. **Delivery-telemetry adoption**: gate stamps runs, but retro requires manual invocation. Is this wired into CI or flow-finalize automation?
6. **--lite Python binding bypass**: `--lite` on bindings/python/ routes to `maturin develop + pytest`; full gate repeats the maturin build. Why not cache the wheel?
7. **Nested helper-file heuristic**: classify_test_targets uses Cargo metadata to exclude nested helpers (e.g., `tests/common/mod.rs`). Does this catch ALL false-positive cases (e.g., deeply nested macros, generated code)?
8. **Slot daemon daemonization**: daemon is a plain `python3 ... &` (not double-forked or nohup'd). Does it inherit the gate's cwd / umask / signal handlers? Any cleanup on gate SIGTERM?

## Reference Files

- Gate definition + usage + component list: `scripts/agent-gate.sh:1–260, 389–2360`
- Concurrency cap design + daemon control: `scripts/agent-gate.sh:2195–2340`
- Summary emission + recovery: `scripts/agent-gate.sh:854–1067`
- Slot daemon: `scripts/lib/gate_slot_daemon.py` (compact + readable)
- Telemetry schema: `docs/reports/delivery-telemetry.schema.json`
- Self-tests: `scripts/tests/test_agent_gate_summary.sh`, `test_gate_concurrency_cap.sh`, `test_agent_gate_delta.sh`
- CLAUDE.md doctrine: `docs/development/pm-operating-loop.md`, `docs/development/parity-ci-tiers.md`
