# Agent Throughput Audit — Build & Test System

**Date:** 2026-07-03
**Scope:** Everything an agent pays per delivered issue — the local gate, cargo build config, test-suite structure, GitHub CI, and the delivery pipeline itself.
**Machine:** 10-core MacBook Pro, 32 GB RAM, ~13 GB free disk.
**Method:** Five parallel audit agents (gate internals, cargo config, test suite, CI + telemetry, prior art + owner's Rust compile-times reference doc), synthesized by the delivery lead.

> **ADDENDUM (same day, post-audit):** Three of the Tier 0 items landed while this audit ran:
> PR #1828 merged (`--lite` tiered gate, issue #1821), PR #1841 merged (#1737: nextest core-tests +
> capped 2-lane parallel gate — **full warm gate now ~258 s, 63 % faster**), and sccache was installed
> on the primary machine. Baseline numbers below (17.3-min gate, 694-s core-tests, "no --lite")
> describe the **pre-landing state** and remain the audit of record; the still-open work is
> #1825 (machine-wide gate cap) and the Tier 1/2 items (#1844–#1848, flaky-gate fixes
> #1803/#1776/#1774, owner decisions 2.1/2.2). Note: with core-tests collapsed, **clippy's
> `--workspace --all-features` build (#1844) is likely the new long pole** — re-measure per-component
> timings after #1844 lands.

---

## Executive summary

The system is not slow because of one bad component — it is slow because **every layer assumes it owns the whole machine and runs everything unconditionally**:

- The gate is **17.3 min warm, strictly sequential**, and builds `cqlite-core` under **7+ distinct feature/profile configurations** per run, none sharing artifacts.
- `core-tests` (694 s, 67 % of the gate) is dominated by **237 serialized test binaries**, not test CPU cost.
- Every cargo invocation defaults to `-j10` and every test binary to 10 threads; **no job cap, no gate semaphore, no nice/taskpolicy anywhere**. N agents = N×10 jobs on 10 cores → load 30–60, SIGKILLed gates, a measured 1 h 22 m implementer stall.
- **sccache is wired into the gate (#1822) but not installed on this machine** — the shipped 25.6 % win is inert, and 4 worktrees carry ~111 GB of duplicated `target/` dirs against 13 GB free disk.
- Telemetry (93 delivered issues): **164 full gate runs ≈ 47 gate-hours** of laptop compute. Mean 1.8 gates/issue, max 9 — driven by roborev rounds each re-running the *full* gate because the `--lite` tier (#1821, PR #1828) hasn't merged.
- GitHub CI is **not** the bottleneck — it is already well-scoped (paths, labels, cancel-in-progress). The waiting is local queueing: median created→PR is 19.6 h vs PR→merge 0.8 h.

**Projected effect of the Tier 0 + Tier 1 items below:** inner-loop iteration drops from 17.3 min → 1–5 min (`--lite`); the full pre-merge gate drops from ~17.3 min → **~6–8 min** (nextest + component parallelism + clippy scoping + sccache); and the machine sustains 3–4 concurrent agents instead of melting at 2 (job caps + gate semaphore). Combined, that is roughly a **4–6× throughput multiplier on the 200-issue backlog** — which is why these items should jump the queue.

---

## Where the time actually goes

### Warm full gate (measured 2026-07-03, sequential)

| Component | Warm | Share | Root cause |
|---|---|---|---|
| core-tests | 694 s | 67 % | 237 test binaries run serially by `cargo test` |
| node-bindings | 174 s | 17 % | builds core under `release-unwind` (LTO, codegen-units=1) — the only release build in the gate |
| python-bindings | 72 s | 7 % | maturin dev build of core in its own target dir |
| write-tests | 38 s | 4 % | three separate cargo invocations |
| tooling-tests | 21 s | 2 % | includes a nested gate self-test |
| 15 other components | ~37 s | 3 % | |
| **Total** | **~1 036 s (17.3 min)** | | strictly sequential; no component parallelism |

### Per-issue pipeline cost (delivery telemetry, n=93)

| Metric | Mean | Median | Max |
|---|---|---|---|
| Full gate runs / issue | 1.8 | 1 | 9 |
| roborev findings / issue | 4.6 | 2 | 40 |
| Rework loops / issue | 2.4 | 1 | 12 |
| created → PR | 86 h | 19.6 h | — |
| PR → merge | 2.3 h | 0.8 h | — |

Sum: **164 full gate runs ≈ 47 gate-hours** across the sample. The tail (review/merge) is healthy; the cost is pre-PR iteration churn, each round paying the full 17.3-min gate, multiplied by machine contention.

### The redundant-compilation matrix

One gate run compiles `cqlite-core` (234 k LOC; `storage/` alone is 152 k) under all of these — each a separate full compile with no artifact sharing (absent sccache):

| Feature/profile combo | Used by |
|---|---|
| default (test) | write-tests, integration-tests, compaction-byte-parity, format-compat *(shared — the one good case)* |
| default + cli-helpers (test) | core-tests |
| + tombstones (test) | tombstones-scan |
| + scan-offload-probe (test) | scan-offload-guard |
| + dhat-heap (test) | memory-budget |
| + parquet, leaked via cqlite-cli `cli-helpers`→`state_machine` (test) | cli-tests |
| same features, **dev** profile | smoke (CLI binary), python-bindings (maturin) |
| same features, **release-unwind** (LTO, cgu=1) | node-bindings |
| `--no-default-features --features all-compression` (dev) | minimal-build |
| **`--workspace --all-features` + `-D warnings`** (dev-check) | clippy — pulls in **bundled DuckDB compiled from C++ source**, the full opentelemetry stack, *two* HTTP/TLS stacks (tonic + reqwest). Zero artifact reuse with anything else. |

---

## Recommendations

### Tier 0 — today, near-zero effort, no quality tradeoff

| # | Action | Impact | Owner action needed |
|---|---|---|---|
| 0.1 | **`brew install sccache`** on every dev machine. The gate auto-detects it (#1822 shipped); it is simply absent here. Consider raising `SCCACHE_CACHE_SIZE=30G` and pinning `rustc-wrapper` in `.cargo/config.toml` so it can't silently regress again; add a loud gate WARN when sccache is absent. | ~25 % off fresh-worktree gates; deduplicates the 7-combo core rebuild matrix at the object level (the near-identical feature combos share ~95 % of their compilation). | 2 minutes |
| 0.2 | **Land PR #1828 (`agent-gate.sh --lite`, issue #1821).** It is open and "almost done". Every roborev round currently pays 17.3 min; with `--lite` it pays 1–5. This is the single highest-leverage unlanded change in the repo. | Inner loop 17.3 → 1–5 min; multiplied by 2.4 rework loops/issue. | Prioritize/merge |
| 0.3 | **Claim and implement #1825 (machine-wide gate concurrency semaphore)** — filed, decided, unowned. Cap concurrent full gates at 2 (flock-based, `CQLITE_GATE_MAX_CONCURRENCY`); have the gate also set `CARGO_BUILD_JOBS` proportional to the cap so two gates don't both claim 10 cores. | Eliminates the load-30–60 / SIGKILL / 1 h 22 m-stall failure mode; makes 3–4 agents sustainable. | Prioritize |
| 0.4 | **Un-stall #1737 (nextest + parallel gate components).** Tracker says in-progress; no worktree exists. The test audit confirms the suite is nextest-ready (see 1.1). | See 1.1/1.2. | Prioritize |
| 0.5 | **Disk hygiene:** 4 worktrees × 25–30 GB `target/` = ~111 GB vs 13 GB free. Add worktree-removal (`flow-finalize` already does this) + a periodic `cargo sweep`/stale-target cleanup to the doctrine. Disk exhaustion mid-gate is an imminent hard failure. | Prevents a class of mystery gate failures. | 10 minutes |

### Tier 1 — this week, small diffs, large wins

| # | Action | Impact | Notes |
|---|---|---|---|
| 1.1 | **Wire `cargo nextest` into `core-tests`** (#1737). 237 serialized binaries → parallel across cores. Audit found only two pre-migration fixes: namespace the hardcoded `/tmp/test-roundtrip-Statistics.db` in `stats_writer_roundtrip.rs`, and note all 12 `#[serial]` files are env-var guards that nextest's process-per-test model makes redundant (safe as-is). CI already uses nextest — the local gate is the laggard. | core-tests 694 s → est. 200–350 s (2–4×). | Identical test set; no coverage change. |
| 1.2 | **Parallelize independent gate components** (#1737 part 2), bounded by the #1825 cap. fmt, clippy, minimal-build, parity-report, telemetry, bindings are mutually independent; per-worktree `target/` dirs mean no lock contention within a run for the non-workspace lanes. | Gate wall-clock collapses toward the long pole (core-tests): 17.3 → ~11.6 min even before nextest; ~6–8 min with it. | Cap total jobs, not just components. |
| 1.3 | **Scope the clippy component off `--workspace --all-features`** → per-package `--all-features` minus `duckdb-tests`/`observability` (matching `scripts/local/pre-merge.sh`'s existing better pattern), with a nightly full-matrix clippy lane as backstop. | Stops compiling **DuckDB from C++ source + two TLS stacks on every local gate run**. | Coverage moves to nightly, not deleted. |
| 1.4 | **Dev/test debuginfo diet:** `debug = "line-tables-only"` for dev/test + `[profile.dev.package."*"] debug = false`, plus a `[profile.debugging]` (inherits dev, `debug = true`) escape hatch. Currently every one of ~450 dependency crates compiles full debuginfo in every dev/test build. | 10–30 % typical codegen+link savings across *all* builds, all worktrees. | From the owner's compile-times doc; zero tracking issue today. |
| 1.5 | **Replace `cargo run --bin cqlite` with `env!("CARGO_BIN_EXE_cqlite")` in 8 CLI test files** (~93 tests currently pay cargo build-graph resolution *per test*). Six other CLI test files already do it right. | Large cut to cli-tests + smoke wall-clock; also removes a dueling-cargo-process hazard under nextest. | Mechanical. |
| 1.6 | **Remove dead `pest`/`pest_derive`** from `cqlite-core` (verified: one call site behind a never-enabled empty feature). Drags a whole proc-macro chain (`pest_generator`/`pest_meta`) into *every* core build including minimal-build. | Removes an unconditional proc-macro toolchain from every build. | ~10-min change. |
| 1.7 | **Fix the flaky gate components that force re-runs:** #1819 (docker probe hang — active), #1803 (python venv ModuleNotFoundError), #1776 (`test_write_throughput` wall-clock assertion under load — exactly the test that fails when the machine is contended, i.e. always), #1774 (`test_memory_monitor` RSS assumption). | `gate_failures` is the #3 recurring failure in telemetry (69 events). Each false red = one wasted 17-min gate. | #1776 is especially perverse: load-sensitivity means contention *causes* gate failures *causing* re-runs *causing* contention. |

### Tier 2 — structural (needs design/owner decisions)

| # | Action | Impact | Decision needed |
|---|---|---|---|
| 2.1 | **Change-scoped gate components.** Only 1 of 20 components (file-size) looks at the diff today. A path→component map exists (drafted in the gate audit): e.g. a `bindings/python/**`-only diff cannot change `compaction-byte-parity`'s outcome. | Docs/bindings/CLI-only changes could skip 60–90 % of the gate. | **Owner call.** The gate's own doctrine ("the only run that counts") exists because of past false-green incidents (#646, #865). A safe design likely = scoped for `--lite`, unconditional for the pre-merge full gate — which is exactly what #1821 already proposes. Recommend: fold path-scoping into `--lite` only; keep the full gate unconditional. |
| 2.2 | **Compress binding parity suites to the conversion boundary.** The same 33-table corpus is fully parity-decoded 4× (core, CLI, Python, Node). Bindings are thin conversion layers; per-type-family representative rows would prove the boundary; full-corpus re-scans (`TestE2ESummary`, `parity.test.js` sweeps) add wall-clock, not coverage. Keep full 4× sweeps nightly. | node+python components 246 s → est. <60 s; also shrinks CI. | **Owner call** — this is a deliberate quality-posture change (parity-is-truth doctrine). Nightly full sweep as backstop preserves the guarantee with a ≤24 h detection delay. |
| 2.3 | **Merge sibling cargo invocations** (write-tests ×3 → 1, cli-tests ×2 → 1 via multiple `--test` flags, as integration-tests already does). | Small; removes repeated dep-resolution + link overhead. | None — mechanical. |
| 2.4 | **Reframe epic #1116 (core split) as a compile-time lever.** `storage/` = 65 % of a 234 k-LOC monolith; one 12.3 k-line file (`write_engine/merge/mod.rs`). Any core edit rebuilds 8 downstream crates. Splitting merge/writer into sub-crates shrinks the rebuild blast radius for every future agent. | Long-term compounding. | Prioritization only. |
| 2.5 | **Shared read-only fixture cache for tests.** Zero `OnceLock`/`lazy_static` fixture caching exists; 236 binaries independently re-open/re-parse the same SSTables. Less urgent once nextest amortizes via parallelism. | Moderate. | Defer until after nextest measurements. |
| 2.6 | **Delete `m1-ci.yml` stub jobs** (2 runners/PR that echo "satisfied by Required PR Gate") once branch protection points at `Required PR Gate / required`. | Runner minutes + noise. | Branch-protection edit (owner/admin). |

### Explicitly rejected / not applicable

- **Linker swap (mold/lld):** macOS Xcode 26 already ships the new fast linker; the Linux-centric advice in the reference doc doesn't transfer.
- **Shared `CARGO_TARGET_DIR`:** correctly rejected in #1822 — cargo's target-dir lock would serialize concurrent gates.
- **Nightly toolchain options** (`-Zthreads`, Cranelift): repo is deliberately pinned to stable 1.88; not worth the correctness risk.
- **cargo-chef:** no Docker build path for the workspace itself.
- **Weakening the pre-merge full gate:** every recommendation above preserves "the full gate is the only run that counts" for merge; the savings come from the inner loop, parallelism, caching, and dead weight — not from testing less before merge.

---

## The compounding failure loop (why it feels so much worse than 17 minutes)

```
N agents each run full gate (no cap, no --lite)
  → 10N cargo jobs on 10 cores → load 30–60
    → load-sensitive tests fail (#1776) / gates SIGKILLed
      → agents re-run the full gate
        → more contention → more failures → agents wedged (1h22m observed)
          → throughput ≈ 0 while the machine is at 100 %
```

Tier 0 breaks every link in this loop: sccache removes duplicate compiles, #1825 caps contention, #1828/`--lite` removes most full-gate runs, #1776/#1774 fixes remove the load→failure coupling.

## Suggested sequencing

1. **Today:** 0.1 sccache install (+ pin), 0.5 disk cleanup, merge PR #1828.
2. **This sprint, ahead of the backlog:** #1825 (cap), #1737 (nextest + parallel components), 1.3 clippy scoping, 1.4 debuginfo diet, 1.5 CARGO_BIN_EXE, 1.6 pest removal, 1.7 flaky-gate fixes.
3. **Owner decisions, then Tier 2:** 2.1 scoping posture (recommend: fold into `--lite` only), 2.2 binding-parity compression (recommend: yes, with nightly full sweep).

## Sources

- Gate dissection: `scripts/agent-gate.sh` (all 20 components, exact invocations, feature matrix).
- Test census: 236 core + 37 CLI test binaries; 1 259 + 491 integration tests; 2 952 core unit tests.
- Telemetry: `docs/reports/delivery-telemetry.jsonl` (93 records).
- Prior art: #1737 (nextest, open/stalled), #1821 + PR #1828 (`--lite`, unmerged), #1822 (sccache, shipped), #1825 (cap, unowned), #1793/#1736 (retro umbrellas), #1819/#1803/#1776/#1774 (flaky gate).
- Reference: `rust_compile_time_best_practices.docx` (26 techniques distilled; 7 had no repo tracking before this audit: debuginfo diet, linker, build.rs hygiene, rust-analyzer config, background checkers, cargo-chef, nightly options).
