# Proposal: core-aware admission default + published concurrency sizing guidance (issue #3225)

**Milestone:** 0.17 (epic #2817, scan-path throughput program) · **Priority:** P2 ·
**Routing:** design-driven — the choice between "derive the default from available parallelism",
"document sizing guidance only" and "leave it alone" is a product/operator-surface decision with no
on-disk oracle, and a derived default has a container-correctness dimension (cgroup quota, affinity
mask, SMT) that belongs in a design rather than a patch · **Issue:** #3225 ·
**Related:** #3217 / PR #3222 (the measurement this rests on), #2420 (the admission mechanism whose
default this changes — its mechanism is explicitly NOT touched), #3306 (footprint-aware admission —
a different, later rule over the same knob), #3096 / #3058 (per-row-work levers, untouched).

## Read this first — a factual correction to the issue body

**The shipped default is `--max-concurrent-scans 64`, not 16.**
`cqlite-flight/src/admission.rs:43` — `pub const DEFAULT_MAX_CONCURRENT_SCANS: usize = 64;` — wired
at `cqlite-flight/src/main.rs:47` (`#[arg(long, env = ENV_MAX_CONCURRENT_SCANS, default_value_t = DEFAULT_MAX_CONCURRENT_SCANS)]`)
and logged at `main.rs:162`. `16` is the **top of #3217's N ramp** (`1,2,4,8,16`), not a default.

This makes the issue's *direction* right and its *magnitude* an understatement, and it changes two
things materially:

1. **The measured 16.4% is a LOWER BOUND on the 1-core cost.** The C(N) curve at S=1 is monotonically
   decreasing from N=2 to N=16 (252,420 → 211,010 rows/s) with per-scan p50 rising 31 s → 302 s. The
   shipped default sits at **N=64, four ramp steps beyond the last measured point**, and nothing in
   #3217 measured it. The true cost of the shipped default on a 1-core server is **unmeasured and
   worse than 16.4%**.
2. **A cap of 16 would be a REDUCTION at wide widths, not a no-op** — so AC5 ("no regression at the
   widest configuration") stops being a formality and becomes a real measurement obligation. The
   design's chosen ceiling is therefore **64**, preserving #2420's blocking-pool/fd sizing and making
   the derived default a strict no-op at ≥32 available hardware threads.

Nothing else in the issue is affected: the peak-N table, the dispersion, the latency curve and
`requests_unavailable = 0` at all 83 points are all reproduced verbatim from
`docs/reports/ws0-3217-artifacts/results/partA-analysis.json`. Correcting the issue body is an
**owner action** (CLAUDE.md: never change an issue's scope/title without the owner); it is recorded
here and surfaced as a NEEDS-YOU item, not executed.

## Why

### 1. The throughput-optimal concurrency moves with core count; the default does not

From `results/partA-analysis.json`, medians of 3 reps, 120 s steps, warm, bypass merge path, min–max
spread as % of median in parentheses (report §3.1):

| N | S=1 core | S=2 cores | S=4 cores | S=6 cores |
|--:|---|---|---|---|
| 1 | 216,229 (3.9%) | 205,129 (3.2%) | 175,872 (4.7%) | 163,510 (2.6%) |
| 2 | **252,420** (5.4%) | 325,364 (6.7%) | 340,878 (12.3%) | 332,165 (10.7%) |
| 4 | 240,361 (2.2%) | 421,621 (3.0%) | 565,409 (2.7%) | 601,074 (4.5%) |
| 8 | 220,865 (2.1%) | **440,677** (1.1%) | 721,434 (1.3%) | 916,066 (2.7%) |
| 16 | 211,010 (1.9%) | 417,424 (1.1%) | **818,747** (0.9%) | **1,076,917** (0.5%) |

Peak N by width: **1 core → 2, 2 cores → 8, 4 cores → 16, 6 cores → 16.** The shipped default is one
number for all four. Byte basis at the headline point (S=6, N=16, 1,076,917 rows/s): **746.6 MB/s
logical/uncompressed** (× 693.29 B/row) · **211.2 MB/s on-disk compressed** (× 196.09 B/row) ·
13.13 GB/s Arrow buffer *capacity* (not gRPC wire bytes).

### 2. Latency degrades an order of magnitude harder than throughput

Per-scan p50 at S=1: **30,966 ms at N=2 → 301,728 ms at N=16** (`latency_p50_ms_median`). A deployer
who over-admits on a narrow worker pays a ~10× per-scan latency bill to buy a 16.4% throughput
*loss*. Over-admission has no upside on a narrow box at any point on the measured curve.

### 3. It is a defaults-and-guidance gap, not a stability defect

`requests_unavailable_total = 0` at every one of the 83 measured points; `admission_clean = true`
throughout. The mechanism (#2420) works exactly as specified. What is wrong is the **number it
defends** and the **absence of any published relationship** a deployer could size against.

### 4. The two existing "peaks" at 4 and 6 cores are CENSORED observations

#3217 swept `N ∈ {1,2,4,8,16}`. At S=4 and S=6 the maximum measured N *is* the peak, so the true peak
is **≥16 and unknown**. At S=6 server utilisation at N=16 is only **0.967** — the curve had not
saturated. Any rule fitted to "peak = 16 at 4 and 6 cores" is fitting to the edge of the ramp. This is
why AC1's "extend to the widest configuration in scope" is load-bearing and why this change budgets a
new measurement round rather than re-reading the old JSON.

## What Changes

1. **The `--max-concurrent-scans` default becomes derived, not constant.** A new
   `default_max_concurrent_scans()` in `cqlite-flight/src/admission.rs` computes
   `clamp(2 × available_parallelism(), 2, 64)`; `DEFAULT_MAX_CONCURRENT_SCANS = 64` is retained as
   the **ceiling constant** (its #2420 blocking-pool/fd rationale is unchanged and still governs the
   upper bound). Clap's `default_value_t` is replaced by a runtime resolution so the flag's
   *absence* — not merely its value — is observable.
2. **Explicit configuration always wins.** `--max-concurrent-scans` and `CQLITE_MAX_CONCURRENT_SCANS`
   override the derived value with no clamping to the derived number; only the pre-existing #2420
   `[1, Semaphore::MAX_PERMITS]` clamp applies.
3. **The effective ceiling and its provenance are logged at startup.** The existing
   `cqlite-flight starting` event gains `max_concurrent_scans_source` (`flag` | `env` | `derived` |
   `derived-fallback`) and `available_parallelism`, so an operator can tell a derived 8 from a
   configured 8 from a log line alone.
4. **A measurement round reproduces and extends the peak-N table** with the #3217 harness, on the
   same box class, with the N ramp extended past the shipped default (`1,2,4,8,16,24,32,64`) and
   widths extended to `{1,2,3,4,6}` hardware-core sets, ≥3 reps, dispersion published, and the
   throughput **and** per-scan p50 cost of over-admission stated at every width.
5. **Operator-facing sizing guidance is published** in `cqlite-flight/README.md` and the operator
   docs: the measured relationship, the derived-default formula, the 16.4%-at-1-core figure, the
   31 s → 302 s latency curve, and how to override.
6. **A conformance test pins the container behaviour**: the derived value is computed from
   parallelism *available to the process* (cgroup quota + affinity mask honoured), never from host
   topology, with a deterministic test that fails if the derivation is re-pointed at `/proc/cpuinfo`.

## Non-goals

- **No change to the admission mechanism (#2420).** The semaphore, the permit lifetime, the
  `UNAVAILABLE` shed, the wait budget, the metrics and the RAII permit are untouched. This change
  moves one number and adds one log field.
- **No read-path or encode-path performance work.** #3096 (Arrow encode) and #3058 stay untouched.
- **No change to `--batch-size`, `--max-batch-bytes`, `--max-inflight-egress-bytes`, or any channel
  capacity.** That is #F2's territory, filed separately and held.
- **Not the single-stream-slows-with-more-cores phenomenon** (216,229 → 163,510 rows/s at N=1 as S
  goes 1 → 6) — that is the glibc-arena investigation, a different axis of the same matrix.
- **Not footprint-aware admission (#3306).** Admitting by measured LLC bytes is a different rule over
  the same knob and is gated on #3299 E1. The formula here is deliberately a *sizing default*, not a
  resource model, and is designed to be replaceable by #3306 without an operator-visible break.
- **No new tunable.** The formula is not itself configurable; the escape hatch is the flag that
  already exists.

## Impact

- **No-heuristics mandate:** unaffected. The mandate governs *decoding from authoritative metadata*;
  this changes a runtime resource default read from the OS, not a value inferred from file bytes. No
  byte pattern is interpreted anywhere in this change.
- **Public binding surfaces:** the CLI/env surface of `cqlite-flight` only. Python, Node and the
  `cqlite` CLI are untouched. The `AdmissionConfig::default()` and `AdmissionConfig::from_env()`
  library surfaces change their *value*, not their *shape* — pre-0.17, and `cqlite-flight` is not
  a published library API.
- **<128 MB memory budget:** unaffected on wide hosts (the ceiling is unchanged at ≥32 threads) and
  strictly improved on narrow ones (fewer concurrently admitted scans ⇒ fewer live merge/egress
  buffers). No case admits more than today.
- **Behaviour change for existing deployments:** yes, and it is the point. A server on <32 available
  hardware threads that does not set the flag will admit fewer concurrent scans than it does today.
  The startup log makes this visible; §"Rollout" in `design.md` states the one-flag restoration of
  the old behaviour (`--max-concurrent-scans 64`).
