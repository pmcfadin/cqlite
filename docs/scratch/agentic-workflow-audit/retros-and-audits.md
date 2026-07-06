# CQLite Agentic Workflow Audit — Retros and Prior Audits

**Compiled:** 2026-07-06  
**Sources:** Primary audit `docs/reports/agent-throughput-audit-2026-07-03.md`, `pm-operating-loop.md`, CLAUDE.md delivery pipeline, MEMORY notes, `delivery-telemetry.jsonl`

---

## Inventory

**Prior audits:**
1. **Agent Throughput Audit (2026-07-03)** — comprehensive gate dissection: build config, test suite, CI, pipeline cost
2. **Delivery Telemetry (93 issues, 164 gate runs)** — per-issue pipeline data: cycle times, rework loops, roborev rounds
3. **PM Operating Loop** — doctrine codified: one manager + workers, merge-on-green, delta re-cert, telemetry+retro

**Fixes shipped (Tier 0, 2026-07-03 post-audit):**
- PR #1828: `--lite` gate (`#1821`) — inner loop 17.3 min → 1–5 min
- PR #1841: nextest + parallel components (`#1737`) — core-tests 694s → 200–350s est.; full gate ~258s (63% faster)
- sccache installed (was MISSING despite being shipped in #1822) — 25.6% fresh-worktree savings inert until install
- Loud accelerator warnings (#1848) — gate warns when sccache/nextest absent or degraded

**Fixes in-flight/open:**
- #1825 (machine-wide gate cap) — claimed/implementing; serialize full gates, set CARGO_BUILD_JOBS proportionally
- #1844–#1848 (Tier 1: clippy scoping, debuginfo diet, CARGO_BIN_EXE, pest removal, flaky-gate fixes)
- #1892 (delta re-cert) — test/docs-only rounds use `--delta` instead of full gate (non-production-touching only)
- #1886 (board dispatch) — Status field as sole dispatch authority (not `status:*` labels) — shipped in pm-operating-loop.md doctrine
- #1930 (one worker per machine) — OWNER DECIDED 2026-07-04; doctrine in pm-operating-loop.md
- Tier 2: change-scoped components (owner decision pending), binding-parity compression (owner decision pending)

---

## How It Works

**Agentic pipeline:**
- One `flow-lead` **manager** controls Ready queue + merge-on-green poller for fleet
- One **worker per machine** (not per session) claims oldest Ready item, implements via subagents, arms merge-on-green, stops
- Workers never busy-poll CI; merge-on-green lands PR when defined green signal passes → flow-finalize auto-triggers

**Gate tiers (issue #1821):**
- `--lite`: fmt + file-size + workspace clippy + blast-radius-scoped tests (~1–5 min) — inner-loop iteration only
- Full `scripts/agent-gate.sh`: 20 components, unconditional, gate of record before merge (~6–8 min projected with all Tier 0 fixes)
- `--delta` re-cert: test/docs-only rounds (fail-closed on src/scripts/config/test-data changes)

**Concurrency model (issue #1930):**
- **Per machine:** one worker serializes full gates (respects #1825 cap), fans out implementation to subagents
- **Cross machine:** origin `issue-<N>-<slug>` branch lock coordinates (first push wins)
- **Full gate:** never run 2+ concurrently (load flakes, tested #1625)

**Board dispatch (#1886):**
- Status field = sole dispatch authority (Backlog/Ready/In Progress/Done)
- `status:*` labels = decorative, never a dispatch source
- Empty Ready = no work ready (not a cue to dredge labels)

**Telemetry + retro:**
- Worker stamps one `delivery-telemetry.jsonl` record per merged issue (timestamps, gate runs, roborev findings, rework)
- Manager runs `delivery-telemetry.py retro` to rank failures by weighted tally → files `flow-meta` improvement issue
- Recorded data: GitHub timestamps (cycle time), observed counters (no fabricated zeros)

---

## Measured/Observed Costs

### Warm full gate (pre-fixes, 2026-07-03)
| Component | Time | Share |
|---|---|---|
| core-tests | 694 s | 67% |
| node-bindings | 174 s | 17% |
| python-bindings | 72 s | 7% |
| write-tests + tooling-tests + others | ~96 s | 9% |
| **Total sequential** | **~1,036 s (17.3 min)** | |

**Root causes:**
- 237 test binaries run serially by `cargo test` (no nextest)
- 7+ feature/profile builds of cqlite-core with zero artifact sharing (absent sccache)
- No component parallelism
- node-bindings builds core under `release-unwind` (LTO, codegen-units=1) — only release build in gate

### Per-issue pipeline cost (n=93, delivery telemetry)
| Metric | Mean | Median | Max |
|---|---|---|---|
| Full gate runs/issue | 1.8 | 1 | 9 |
| roborev findings/issue | 4.6 | 2 | 40 |
| Rework loops/issue | 2.4 | 1 | 12 |
| created → PR | 86 h | 19.6 h | — |
| PR → merge | 2.3 h | 0.8 h | — |

**Sum:** 164 full gate runs ≈ **47 gate-hours** across sample. Tail (merge) healthy; cost is pre-PR iteration + contention.

### Compounding failure loop (observed ~1h22m stall)
```
N agents (no cap) each run full gate
  → 10N cargo jobs on 10 cores → load 30–60
    → load-sensitive tests fail (#1776) / gates SIGKILLed
      → agents re-run full gate → contention feedback
```

**Flaky gates (#1776, #1774, #1803, #1819):** 69 recorded events; each false red = one wasted 17-min gate.

### Redundant compilation matrix (one gate run)
cqlite-core (234 k LOC) compiled under **9 distinct feature/profile combos**, zero artifact reuse absent sccache:
- default (test) — write-tests, integration-tests, compaction-byte-parity, format-compat
- default + cli-helpers (test) — core-tests
- + tombstones, scan-offload-probe, dhat-heap, parquet (test) — various
- same features, **dev** — smoke, python-bindings
- same features, **release-unwind** (LTO, cgu=1) — node-bindings
- `--no-default-features --features all-compression` (dev) — minimal-build
- **`--workspace --all-features` (dev-check) — clippy** → pulls DuckDB C++ compile + OpenTelemetry + 2 HTTP/TLS stacks

**Status 2026-07-03:** sccache installed (was absent); deduplicates ~95% of near-identical combos at object level.

### Machine-wide contention cap (#1825)
**Pre-fix:** N concurrent agents = N×10 cargo jobs on 10 cores, load 30–60, SIGKILL gates.  
**Post-fix (projected):** at most N=2 full gates serialized + other work pipelined → load ~5–15 → no SIGKILLs.

---

## Friction Points

1. **sccache corruption under extreme load (2026-07-06 observed):** intermittently served corrupted objects (`ld: symbol(s) not found`). Workaround: `CQLITE_DISABLE_SCCACHE=1`. Instructs implementer subagents to serialize lite-gate/test runs (one at a time). **Open:** root cause TBD.

2. **Flaky gate components drive re-runs (#1776, #1774, #1803, #1819):** #1776 is perverse — wall-clock assertion in `test_write_throughput` fails under load, load caused by re-runs, creates contention cycle. #1825 cap breaks the loop but doesn't fix #1776 itself.

3. **No change-scoped gate components:** all 20 components run unconditionally even for bindings/docs-only diffs. Projected 60–90% gate savings if scoped (owner decision pending; audit recommends fold into `--lite` only, keep full gate unconditional for pre-merge).

4. **Binding parity suites run full 33-table corpus 4× (core, CLI, Python, Node):** each is a full re-scan. Audit estimates <60s if compressed to conversion-boundary representatives, with nightly full sweep as backstop (owner decision pending).

5. **No per-machine load-balancing signal:** workers serialize full gates via origin branch lock, but no explicit quota/semaphore. #1825 adds it; currently manual coordination.

6. **Delta re-cert scope fragile:** `--delta` fail-closed on src/scripts/Cargo.*/config/test-data; includes node `__test__/` + shell `scripts/tests/*.sh` (component limits). Any production change forces fresh full gate. Requires careful diff scope on roborev address rounds.

7. **"Full gate exactly once" rule violated historically:** every roborev/address round re-triggered full gate (15–25 min) in #1889 retro; #1853 burned ~3 cycles, #1921 ~2 on test/docs-only polish. #1892 closes loophole but requires discipline on addressing PRs.

8. **Disk exhaustion mid-gate:** 4 worktrees × 25–30 GB `target/` dirs = ~111 GB vs 13 GB free (2026-07-03 machine state). Audit recommends periodic `cargo sweep` + flow-finalize worktree removal (already in doctrine, fragility = non-deterministic timing of cleanup).

9. **sccache + nextest missing → gates degrade silently:** prior to #1848, a machine without sccache ran 25.6% slower with no signal. Loud WARN now in gate output.

10. **No graceful accelerator fallback signals in PR summaries:** SUMMARY block now emits machine-checkable `accelerators: sccache=on nextest=absent lanes=serial` line, but in-PR drift detection requires manual block inspection.

---

## Open Questions

1. **#1825 implementation status:** is it claimed? branch exists? blockers?

2. **sccache corruption root cause:** intermittent vs systematic? load-dependent? corrupts local cache or cross-worktree shared cache? Recovery procedure?

3. **Tier 2 design decisions pending:**
   - Change-scoped components: audit recommends fold into `--lite`, unconditional full gate. Owner call?
   - Binding-parity compression: full 33-table 4× vs representative subset + nightly full sweep. Owner call?

4. **#1776 load-sensitive test failure:** is it timeout, assertion, or CPU-dependent flake? Can it be made deterministic or acceptably probabilistic?

5. **One-worker-per-machine doctrine uptake:** was this documented in branch protection / CI / worktree setup? New joiners aware?

6. **Merge-on-green green-signal guard:** current state of required status checks on `main`? Is GitHub landing PRs or manager poller still needed?

7. **Telemetry ledger quality:** any detected data-entry errors, missing counters, or fabricated zeros in `delivery-telemetry.jsonl`?

8. **Nightly deep-check (`gate.yml`):** does it run `CQLITE_CLIPPY_FULL=1`? catching things local gate misses?
