# Design — flight-throughput-benches (AD5, #1494)

## Context

Epic #1469 / read-path audit finding AD5. The export/Flight lane has no perf net. This change builds
the net; the AB/AE children (AB1/AB3/AB7, AE1–AE5) are its consumers. Owner doctrine: **benches
FIRST — baseline before wins**, and any perf gate must be **non-flaky under load** (the #1930 "one
worker per machine, load spikes are normal" reality).

Two facts pin the honest framing:

1. **#1495 already merged (PR #2312).** The baseline captured here already includes its arrow-convert
   win. The baseline is "current `main`," not pre-optimization. #1496 + AB/AE measure ratios against
   it. We say this in the README baseline record so no one misreads the floor.
2. **The `agent-gate.sh` gate of record must not flake.** Wall-clock throughput on a loaded box is
   unusable as a hard gate; allocation counts/bytes are load-independent. So the hard, mandatory
   signal is the dhat budget; wall-clock lives in the CI perf lane (same-runner ratio, `ci:perf`).

## Resolved design questions

### (i) Criterion micro-benches vs an end-to-end `do_get` harness → **both (tiered)**

| Option | Pro | Con |
|---|---|---|
| Micro-benches only (converter + writers) | Localizes per-cell / per-format cost; cheap | No **wiring evidence** — a green converter bench is not proof the *Flight* path is exercised; can't catch a producer/transport regression |
| e2e `do_get` only | Real public surface; catches producer/merge/transport regressions | Can't attribute a regression (converter vs merge vs transport); wall-clock noisy |
| **Both, tiered (recommended)** | Micro-benches attribute cost + are gate-STRICT; the e2e `do_get` supplies public-surface wiring evidence (ADVISORY) | Slightly more bench code |

**Recommendation: both.** The issue names both ("throughput benches for … export, and Flight
`do_get`"). Tier 1 = converter (`rows_to_record_batch`) + per-format export micro-benches in
`cqlite-core/benches` (STRICT, ratio-gated). Tier 2 = an end-to-end Flight `do_get` throughput bench
in `cqlite-flight/benches`, driving the **public tonic `FlightService::do_get`** over the in-process
transport already stood up by `cqlite-flight/tests/do_get_transport_test.rs` (ADVISORY). Tier 2 is
the wiring-evidence surface; Tier 1 is the diagnosable, gate-strict signal.

*Beats micro-only:* micro-only has no public-surface wiring evidence — the exact "green helper test
≠ feature done" trap CLAUDE.md's wiring-evidence rule forbids. *Beats e2e-only:* e2e-only can't
localize a regression and its wall time is too noisy to gate strictly.

### (ii) Where alloc budgets live → **dhat direct observation in gate-wired tests, not bench counters**

| Option | Pro | Con |
|---|---|---|
| dhat `#[global_allocator]` tests (epic-H pattern), wired to `memory-budget`/`work-counters-guard`/`byte-budget-guard` | Deterministic under load; runs in **every** full gate; reuses existing infra + gate components; non-vacuous by construction | Separate test binary per dhat allocator |
| Criterion-integrated alloc counters | One bench run yields time + allocs | Only runs in the CI perf lane, not the mandatory gate; couples the hard budget to the noisy wall-clock lane |

**Recommendation: dhat observation in tests, reusing the epic-H machinery, wired into the existing
deterministic gate components.** Peak/producer memory → the `memory-budget` (dhat-heap) component;
per-cell converter allocation counts/bytes → `work-counters-guard` / `byte-budget-guard`. This is the
`test_issue_1046_scan_alloc_scaling.rs` / #1668 / #1660 pattern verbatim. Allocation counts are
machine-independent, so these are a **hard** per-gate signal that never flakes under load — the
property the owner requires. Explicitly **do not** duplicate the epic-H infra (issue mandate).

*Beats bench-integrated counters:* determinism + it runs in the gate of record, not only the
opt-in perf lane.

**Non-vacuous mandate.** Each budget guard asserts, before checking the bound, that the fixture
produced ≥ 1 row **and** that observed allocations/bytes are > 0. A run that measured nothing (empty
dataset, converter no-op'd) **fails**, never passes at "0 ≤ budget." Fixture present-but-empty →
panic (0-rows-when-present = failure, per the testing rule); fixture entirely absent → the test
skip-registers so the gate reports SKIP, matching the `memory-budget` fail-closed-on-empty policy
in the full gate.

### (iii) Perf-gate entry semantics → **same-runner regression-ratio, SKIP-aware; STRICT micro + ADVISORY e2e; no committed absolute baseline**

**Recommendation:** reuse the existing mechanism unchanged.
- **STRICT** (`benches[]`, `threshold_pct` ≥ 10): the converter + per-format export micro-benches.
  These are CPU-bound and stable; a real regression clears the threshold on a same-runner ratio.
- **ADVISORY** (`advisory_benches`): the end-to-end Flight `do_get` throughput bench — its wall time
  is Tokio-runtime + tonic-transport + I/O dominated (the `write/ingest_wal_on` situation), so it is
  **reported but never fails CI**. Its *correctness/allocation* guard is the dhat producer budget, not
  this wall clock.
- **SKIP** on first landing: benches absent from the `main` baseline report SKIP and never fail —
  guarded in `perf-regression.yml` with the established "bench target may not exist on `main` yet"
  conditional (mirrors `--bench compaction`/`--bench decode`).
- **Baseline artifact + refresh:** the committed artifact is `perf-gate.json` (tracked list +
  thresholds + advisory classification). There is deliberately **no committed absolute-timing
  number** — the `base` baseline is freshly re-measured on `main` every CI run, so it cannot drift or
  flake. The durable "baseline before wins" record is a human-readable current-main numbers table in
  `cqlite-core/benches/README.md` (stamped post-#1495), plus a documented refresh procedure: to retune
  a threshold, edit `perf-gate.json` and update the README numbers in the same PR; to (re)establish a
  bench, land it SKIP-green then let the next PR get the ratio.

*Beats a committed absolute JSON baseline:* no drift, no manual refresh chore, no load-induced flake —
the ratio cancels machine speed within a single run. *Beats hard-failing the e2e throughput:* an
async/transport-dominated wall time hard-gated would flake the merge queue; advisory + a dhat hard
budget gets the honest signal without the flake.

## The smallest honest thing

A converter + export micro-bench group (STRICT, ratio-gated) + one e2e `do_get` throughput bench
(ADVISORY, public-surface wiring evidence) + dhat budget guards on the producer and converter (hard,
load-deterministic, gate-wired, non-vacuous) + `perf-gate.json`/`perf-regression.yml` wiring + a
committed README baseline record. No production code touched; 0.14 gets a usable, honestly-labeled
post-#1495 baseline and a gate signal that cannot flake under load.

## Fail-on-today (genuine)

- The benches **do not exist on `main`**: `cargo bench -p cqlite-flight --bench flight_do_get` (and the
  new core export bench group) fail to resolve today — the machinery's absence is the red.
- A red-run in the PR proves the STRICT gate bites (slow the converter → REGRESSION, non-zero exit)
  and the budget guard bites (inflate a producer allocation → guard FAILs).
- Budget guards land **passing** as baseline locks; the aggressive AB/AE target bounds that
  fail-on-today by construction are the consumer issues' tests, seeded by this infra.

## Risk / open items for Seam 1

- **Flight `do_get` bench harness reuse.** Recommendation reuses the in-process transport from
  `do_get_transport_test.rs`. If the owner prefers the lower-level public `FlightProducer::
  produce_from_resolved` streaming API as the Tier-2 surface (no tonic transport), say so — that is
  still a public surface but weaker wiring evidence for the RPC path.
- **Dataset choice.** Benches use the pinned canonical datasets (`CQLITE_DATASETS_ROOT`); a wide-row /
  type-heavy table exercises per-cell conversion cost. Absent → SKIP, never a fake 0.
