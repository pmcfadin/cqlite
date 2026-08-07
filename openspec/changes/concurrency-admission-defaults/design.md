# Design: core-aware admission default + concurrency sizing guidance (issue #3225)

## Context

Three facts frame every decision below.

1. **The optimum moves and the default does not.** #3217 measured the throughput-optimal concurrent
   stream count at four server widths: 1 core → N=2, 2 cores → N=8, 4 cores → N=16, 6 cores → N=16
   (`docs/reports/ws0-3217-artifacts/results/partA-analysis.json`, report §3.1).
2. **The shipped default is 64, not 16** (`cqlite-flight/src/admission.rs:43`), so the 16.4% figure in
   the issue is the cost at the *last measured point*, not at the default. The default itself was
   never measured. See `proposal.md` §"Read this first".
3. **Two of the four measured peaks are censored.** The ramp stopped at N=16, and at S=6 server
   utilisation at N=16 was 0.967 — not saturated. "Peak = 16" at S=4 and S=6 means "≥16, unknown".

The product question the owner is being asked to settle is exactly one:
**does `--max-concurrent-scans` get a core-aware derived default, or does it stay a constant with
documented sizing guidance only?**

---

## Recommended design

### D1 — DECISION: derive the default from available parallelism (Option B)

**Decision.** `--max-concurrent-scans` gets a derived default. Guidance alone is not sufficient.

**Rationale.** A default is the configuration almost every deployment runs. The measured harm is
concentrated exactly where operators are least likely to tune: a 1–2 core Trino worker sidecar or a
small container, which is a shape someone deploys *because* they did not want to think about sizing.
Documentation-only fixes the deployments that read documentation; the measured population that needs
the fix is the one that does not. The cost of being wrong in the other direction is bounded and
one-sided: the formula's ceiling is today's default, so no deployment is ever admitted *more* widely
than it is today, and any deployment can restore exactly the old behaviour with one flag.

**Alternatives considered** — see D9 for the full option table and why each was rejected.

### D2 — DECISION: the formula

```
N_default(P) = clamp(2 × P, 2, 64)      where P = hardware threads available to THIS process
```

`P` is `std::thread::available_parallelism()` (see D3). In code:

```rust
/// Ceiling retained from #2420 — the blocking-pool (~256) and fd (~1024/M) bound.
pub const DEFAULT_MAX_CONCURRENT_SCANS: usize = 64;
pub const MIN_DERIVED_MAX_CONCURRENT_SCANS: usize = 2;
pub const DERIVED_SCANS_PER_HARDWARE_THREAD: usize = 2;

pub fn default_max_concurrent_scans() -> usize { /* clamp(2 * P, 2, 64) */ }
```

**Fit against the measured table.** #3217's widths are *physical cores* on an SMT-on box where both
siblings of each core were pinned together (report §2.2), so each measured width S corresponds to
`P = 2 × S` hardware threads:

| measured width | P | formula | measured peak N | median rows/s at formula N | at measured peak | deviation |
|---|--:|--:|--:|--:|--:|---|
| 1 physical core | 2 | **4** | 2 | 240,361 | 252,420 | **−4.8%** |
| 2 physical cores | 4 | **8** | 8 | 440,677 | 440,677 | **0% (exact)** |
| 4 physical cores | 8 | **16** | 16 (censored) | 818,747 | 818,747 | **0% (exact)** |
| 6 physical cores | 12 | **24** | ≥16 (censored, util 0.967) | **not measured** | 1,076,917 @ N=16 | **UNMEASURED — AC1/AC5 must close this** |

Two exact hits, one −4.8% miss, one unmeasured extrapolation. The formula is honestly a **two-point
fit with two censored corroborations**, and the artifacts say so everywhere rather than presenting
four confirmations.

**Why the 1-core miss is accepted rather than special-cased.** `P = 2` is produced by two physically
different machines that `available_parallelism` cannot distinguish: one SMT core, and two non-SMT
cores. Their measured peaks are 2 and 8 respectively. So the value chosen at `P = 2` is a
minimax-regret choice, not a fit:

| value at P=2 | cost on 1 SMT core | cost on 2 non-SMT cores (using the S=2 curve) | worst case |
|--:|--:|--:|--:|
| 2 | 0% | −26.1% (325,364 vs 440,677) | **26.1%** |
| **4** | **−4.8%** (240,361 vs 252,420) | **−4.3%** (421,621 vs 440,677) | **4.8%** ← chosen |
| 8 | −12.5% (220,865 vs 252,420) | 0% | 12.5% |
| 64 (today) | worse than −16.4%, unmeasured | unmeasured, ≥ −5.3% | ≥16.4% |

`2 × P` lands on 4 at `P = 2` naturally. A `P == 2 ⇒ 2` special case would fit the measured 1-core
point exactly and cost 26% on the machine it cannot tell apart from it. **Rejected.** The latency
side agrees: at S=1, N=4 gives p50 64,225 ms against 30,966 ms at N=2 (2.1×), where the shipped
default's last measured neighbour N=16 gives 301,728 ms (9.7×).

**The ceiling is 64, deliberately.** It is today's `DEFAULT_MAX_CONCURRENT_SCANS`, so (a) #2420's
blocking-pool/fd sizing rationale survives verbatim as the upper bound, (b) the change is a strict
**no-op at P ≥ 32** (16 SMT cores or 32 non-SMT cores), and (c) no deployment is ever admitted more
widely than today, which is what makes the rollout one-directional and the AC5 risk bounded. A
ceiling of 16 — the number the issue body implies — would *reduce* the wide case on unmeasured
evidence and was rejected for that reason.

**The floor is 2.** A single-permit server serialises every scan, and the measured N=1 column is the
worst point at every width (216,229 at S=1 rising to 252,420 at N=2). The floor also makes
`available_parallelism`'s rounding of a fractional cgroup quota immaterial: any quota below one CPU
still yields 2.

**What it does at unmeasured widths.** P=1 → 2; P=3 → 6; P=6 → 12; P=16 → 32; P=24 → 48; P=32 → 64;
P≥32 → 64 (capped). Monotone non-decreasing, and equal to today above the cap.

**Post-hoc mechanism check, offered as consistency and NOT as derivation.** At all four measured
peaks server utilisation of the pinned set is 0.996 / 0.995 / 0.992 / 0.967 (report §3.2): the peak
is where the admitted streams collectively saturate the CPU. #3217's off-CPU attribution (§4.2) shows
a scan spends a large fraction of its wall time blocked on the handoff/egress path, so roughly two
streams per hardware thread are needed to keep one thread busy. That is consistent with a coefficient
of 2 per hardware thread. It is a *story that fits*, not evidence — the coefficient's warrant is the
fit table above.

### D3 — DECISION: `std::thread::available_parallelism()`, and NOT `num_cpus::get_physical()`

**Decision.** `P = std::thread::available_parallelism().map(NonZeroUsize::get)`, with `Err` handled
as a named fallback (D4). This is already the repo's precedent for exactly this job —
`cqlite-core/src/storage/sstable/reader/scan_admission.rs:169` derives the windowed-scan admission
cap the same way.

**What it honours.** On Linux `available_parallelism` reports the parallelism available to *this
process*: it intersects the thread's `sched_getaffinity` mask and applies the cgroup v1
(`cpu.cfs_quota_us` / `cpu.cfs_period_us`) and cgroup v2 (`cpu.max`) CPU quota. It returns a
`NonZeroUsize`, so it can never yield 0. It reports **logical** CPUs (SMT siblings counted
separately).

**Why NOT `num_cpus`, which the workspace already depends on** (`cqlite-core/Cargo.toml:68`,
`num_cpus = "1.0"`, resolved 1.17.0). Read at
`/data/registry/src/index.crates.io-*/num_cpus-1.17.0/src/linux.rs`:

- `num_cpus::get()` (`linux.rs:32`) is *acceptable* — it tries `cgroups_num_cpus()` first (which takes
  `min(quota, affinity-aware logical)` at `linux.rs:140`) and falls back to `sched_getaffinity`, then
  `_SC_NPROCESSORS_ONLN`. It honours both constraints. It is simply not better than std, and std is
  already the precedent.
- **`num_cpus::get_physical()` (`linux.rs:59`) is DISQUALIFIED, and this is the concrete trap AC2
  names.** It parses `/proc/cpuinfo`, sums `cpu cores` per `physical id`, and applies **neither the
  cgroup quota nor the affinity mask**. Inside a container `/proc/cpuinfo` is the *host's*, so a pod
  limited to 1 CPU on a 96-core node reads 96 and would derive the maximum ceiling on the narrowest
  server — the precise failure AC2 forbids, amplified rather than fixed. It is the obvious API to
  reach for, because #3217's basis is physical cores; reaching for it is the bug.

**Physical vs logical — stated, not glossed.** #3217's widths are physical cores; the formula's basis
is hardware threads. The conversion used to fit is `P = 2 × S`, valid on the SMT-on measurement box
and asserted from `/sys/devices/system/cpu/cpu*/topology/thread_siblings_list` by the harness
(report §2.1), never assumed. The residual, and it is real:

- On an **SMT-on** host the formula yields **4 admitted scans per physical core** — the fitted value.
- On a **non-SMT** host (Graviton, many ARM instances, SMT disabled in firmware) logical == physical,
  so it yields **2 per physical core** — half the fitted per-core value.

#3217 has **no non-SMT arm**, so which of those is right for a non-SMT core is unmeasured. The design
takes the hardware-thread basis anyway, for two reasons: a non-SMT core is one hardware thread and
delivers less concurrent throughput than an SMT core, so scaling by threads is the more defensible
extrapolation; and a quota-limited cgroup has no well-defined "physical core" count at all (a
1.5-CPU quota is a time budget, not a core inventory), so physical cores cannot be the basis of a
container-correct rule even in principle. **Deriving an SMT ratio from sysfs and dividing was
considered and rejected** (D9-A5): it buys precision the measurement cannot support, it is
undefined under a fractional quota, and it needs the affinity mask intersected with the sibling map
to be correct in a container — machinery whose failure modes are worse than the 2× it resolves. The
non-SMT arm is called out in `tasks.md` as the highest-value measurement extension, and the residual
is published in the operator docs rather than hidden.

### D4 — DECISION: provenance is a value, and an unavailable oracle is its own state

`available_parallelism()` returning `Err` (a platform with no way to report it) must not silently
become a number that looks derived. The resolution produces a `(usize, Source)` pair:

| `Source` | meaning | value |
|---|---|---|
| `flag` | `--max-concurrent-scans` given on the command line | as given |
| `env` | `CQLITE_MAX_CONCURRENT_SCANS` set and parseable | as given |
| `derived` | no explicit config; `available_parallelism()` answered | `clamp(2 × P, 2, 64)` |
| `derived-fallback` | no explicit config; `available_parallelism()` returned `Err` | `DEFAULT_MAX_CONCURRENT_SCANS` (64), the pre-#3225 behaviour |

This follows CLAUDE.md's affirmative-measurement rule: the oracle that could not be consulted gets
its own name in the log rather than an indistinguishable pass. `derived-fallback` deliberately
resolves to **today's 64** — an unmeasurable platform gets the status quo, not a guess.

### D5 — DECISION: precedence, and where it is resolved (AC4)

Precedence, highest first: **`--max-concurrent-scans` flag → `CQLITE_MAX_CONCURRENT_SCANS` env →
derived default**. An explicit value is *never* clamped toward the derived value; only #2420's
pre-existing `[1, Semaphore::MAX_PERMITS]` clamp in `Admission::new` (`admission.rs:198-206`) applies,
unchanged. Setting `--max-concurrent-scans 64` restores byte-identical pre-#3225 behaviour on any
host.

**Mechanically**, clap's `default_value_t = DEFAULT_MAX_CONCURRENT_SCANS` (`main.rs:47`) is replaced
by `Option<usize>` plus an explicit `resolve()`, because with `default_value_t` the parser cannot
distinguish "user typed 64" from "nobody typed anything" — and that distinction *is* AC4's
provenance. Clap's `env = ENV_MAX_CONCURRENT_SCANS` attribute already gives flag-over-env precedence,
so the env arm keeps its existing semantics; the resolver only needs to learn whether clap supplied a
value at all (`ArgMatches::value_source`) to label `flag` vs `env`.

`AdmissionConfig::from_env()` (`admission.rs:112`) keeps its shape and its "unparseable falls back
rather than failing startup" contract; its fallback target becomes the derived value.

### D6 — DECISION: startup logging (AC4)

The existing `tracing::info!(… "cqlite-flight starting")` at `main.rs:162` already emits
`max_concurrent_scans = admission_limit` (the post-clamp effective value — correct, keep it). Add two
fields on the same event:

```
max_concurrent_scans = 16                 # effective, post-clamp (existing)
max_concurrent_scans_source = "derived"   # flag | env | derived | derived-fallback
available_parallelism = 8                 # the P actually read; omitted when Err
```

One event, no new log line, greppable, and sufficient to answer "why is this server admitting 16?"
from a log capture alone. Deliberately NOT a metric: the admission limit is already exported as the
`cqlite.flight.admission.limit` gauge (#2420), and provenance is a startup fact, not a time series.

### D7 — DECISION: how AC1 and AC5 get measured, honestly

AC1 and AC5 demand measurement, and the design states what that costs rather than assuming the old
JSON can be re-read. **It cannot**: `partA-analysis.json` stops at N=16, so it contains no data point
at the shipped default 64, and it has no width-3 arm.

**Rig.** One box of the #3217 class is sufficient for every width in scope. Confirmed present:
`Intel(R) Xeon(R) Platinum 8488C`, 8 physical / 16 logical, SMT on, 1 NUMA node, kernel 6.17,
`/data` NVMe — the same machine class as report §2.1. Widths are produced by `taskset` core sets, not
by separate machines, and both SMT siblings of a core are always pinned together (report §2.2).

**Widest configuration in scope = 6 physical cores / 12 hardware threads.** The client is a fixed
2-physical-core constant on the same box (`6,7,14,15`) and `sweep.sh` **refuses to run** if the
server and client sets overlap. Reaching 8 physical cores therefore requires a second machine for the
client — a rig change (network path, new validity gate, new client-headroom baseline) that is not
worth the one extra width. §3.5 shows the client at **13.8% of 2 cores at the hardest point against a
70% gate**, ~5× headroom, so the constraint is core exclusivity, not client capability. The change
declares 6 the widest configuration in scope and says so in the report; it does not silently call the
whole box "widest".

**Sweep matrix.**

- Widths: `S ∈ {1, 2, 3, 4, 6}` physical cores. **S=3 is new** — it is the first uncensored test of
  the formula at a width it was not fitted to (predicts N=12).
- N ramp: `1,2,4,8,16,24,32,64`. Extending past 16 is mandatory: **64 is the status quo** and AC5
  cannot be evaluated against a point nobody measured. 24 is the formula's prediction at S=6.
- Reps: 3 minimum, per-N min/median/max published (report §3.1's ≥3 requirement caught two >10%
  dispersion outliers at low N on wide core sets — reps=1 would have shipped them invisibly).
- 120 s steps, 45 s warm pre-pass, 5 s settle — unchanged from #3217 so the S ∈ {1,2,4,6} arms are
  directly comparable to the published table.
- Client-headroom validity gate re-enforced at every point (report §3.5); any point over 70% client
  utilisation is excluded and *reported as excluded*.

**Time.** 8 N-values × 3 reps × ~125 s ≈ 50 min per arm plus warm/settle ≈ **~1 h per width**;
5 widths ≈ **5–6 h of unattended wall clock**, plus corpus staging. One overnight run on one box.

**Corpus.** #3217's `ws0.events` binaries are **gone from this box** (`/data/ws0` does not exist) and
are gitignored, so the corpus must be re-staged or regenerated. Recorded geometry to match
(`ws0-3217-artifacts/corpus/corpus-geometry.txt`): 3,999,890 rows, 200,000 partitions × 20 rows,
`nb-16-big` single SSTable, LZ4 16 KiB chunks, dataLength 2,773,081,150 B, on-disk 784,334,710 B,
**693.29 B/row uncompressed · 196.09 B/row on-disk compressed**, ratio 3.5356×, no TTL, no
tombstones, `sha256(Data.db) = 3a4ee5cd5ef5937ae52a703cca0ee0359df8ecb959915dea66b3b89f9a9c7c1e`.
A regenerated corpus will have a **different sha**; AC6 requires the new sha and the new geometry to
be recorded, and the geometry to be shown matching the old within its stated tolerances, not assumed.

**Reusable harness, by path** (all under `docs/reports/ws0-3217-artifacts/`, and all of it is
**reviewed code** under this repo's convention, not docs):

| Path | Reuse |
|---|---|
| `harness/common.sh` | **As-is.** Topology read from sysfs, the S→CPU-set table, server launch/readiness/stop, `/proc` helpers, sysctl re-assert. |
| `harness/sweep.sh` | **As-is.** Already takes the N ramp and reps as arguments and accepts a *literal* CPU list as well as `s1|s2|s4|s6`, so S=3 and the extended ramp need no code change. Enforces the server/client non-overlap refusal. |
| `harness/emit-point.py` | **As-is.** Owns the byte-basis labelling (AC6) and the client-saturation stamp. |
| `harness/summarize-sweep.py` | **As-is.** Min/median/max dispersion, speedup, marginal efficiency, three byte bases, the AC5 client table, saturated-point exclusion. |
| `harness/corpus-basis.py` | **As-is.** The AC6 byte basis: on-disk exactly, logical from `CompressionInfo.db`. |
| `harness/selftest.sh` | **As-is.** 36 mechanics checks needing neither corpus nor server — run first. |
| `partA-run/run-partA.sh` | **Adapt** (the arm chain: add S=3, extend the ramp, drop the merge-path reference points). |
| `partA-run/analyze-partA.py` | **Adapt** (emit the new peak-N table + the per-width over-admission cost columns). |
| `harness/profile-*.sh`, `classify-offcpu.py`, `parse-runqlat.py`, `unsym-check.py`, all of `partB-run/` | **NOT reused.** Off-CPU/on-CPU attribution answered #3217's question; this change measures a curve. Skipping them also removes the `perf_event_paranoid` / `kptr_restrict` dependency from the run. |

**The one thing that must not happen**: none of this becomes a `cargo test`. CLAUDE.md bans
wall-clock threshold asserts in the correctness test path. The sweep is a harness run whose output is
a committed report; the only in-repo assertions are the deterministic ones in D8.

### D8 — DECISION: what is asserted in tests, and what is only measured

Deterministic, always-run tests (no wall clock, no sleep, no throughput):

1. **The formula is a pure function and is table-tested** at `P ∈ {1,2,3,4,6,8,12,16,24,31,32,33,64,
   1024}` against the clamp bounds and monotonicity. Pure `fn(usize) -> usize`, so it is testable
   without a machine of that width — that is the point of separating the formula from the probe.
2. **Precedence** (AC4): flag beats env beats derived; an explicit value is not clamped toward the
   derived one; `--max-concurrent-scans 64` reproduces the pre-change ceiling. Asserted end-to-end
   through the real CLI parser, not against a hand-built config.
3. **Provenance labelling**: each of the four `Source` values is produced by its own input, and the
   startup event carries the label — `derived-fallback` included, driven by an injected `Err`.
4. **The container-correctness guard** (AC2): a structural assertion that the derivation path
   contains no `/proc/cpuinfo` read and no `num_cpus::get_physical()` call. This is deliberately
   structural rather than behavioural, because a behavioural test would need a container: the failure
   mode is a future edit re-pointing the derivation at host topology, and that edit is visible in the
   source even when no cgroup is available to the test.
5. **A cgroup/affinity conformance check**, run as a `#[ignore]`d, `perf-gate-allow`-marked test *or*
   as a harness script (a plain `taskset -c 0,1 cqlite-flight … | grep max_concurrent_scans`): under
   a restricted affinity mask the logged `available_parallelism` equals the mask size and the derived
   value follows. Affinity is testable anywhere; the cgroup arm is recorded from the measurement box
   as evidence, not as a gate, because the gate runner's cgroup is not ours to control.

Nothing in this list asserts a rows/s number. The throughput claims live in the committed report and
its artifacts.

### D9 — The options, and why the others were rejected

| # | Option | Verdict |
|---|---|---|
| **B** | **Core-aware derived default `clamp(2 × P, 2, 64)` + published guidance** | **RECOMMENDED.** Fixes the population that does not tune; ceiling = today's default so nothing is ever admitted more widely than now; one flag restores the old behaviour exactly. Cost: a behaviour change on every <32-thread host, and a −4.8% median deviation at P=2. |
| A | **Guidance only — default stays 64, publish the table and a sizing recipe** | Rejected as the primary. It is strictly contained in B (B ships the same guidance), and it leaves the measured harm in place for every deployment that does not read the docs — which is the deployment shape the harm was measured on. Worth reconsidering only if the owner judges any default change too disruptive for 0.17; in that case B's docs, logging and measurement all still ship and only D2 is dropped. |
| A′ | **Lower the constant to 16 (the number the issue body assumes)** | Rejected. Still constant, so it still mis-sizes 1-core (−16.4%) and 2-core (−5.3%) servers, *and* it reduces the wide case from 64 to 16 on evidence that stops at N=16. Strictly worse than both A and B. |
| A″ | **Do nothing** | Rejected. The measurement exists, the cost is quantified, and the mechanism is healthy — this is the cheapest fix on the 0.17 board. |
| C | **Emit a startup WARNING when the configured ceiling is far above the derived one, but do not change the default** | Rejected as primary, and it is not free: a warning on every narrow-container start that the operator cannot action without reading the same docs A would have shipped. It is a reasonable *addition* to B if the owner wants the transition louder, and is noted as such rather than built. |
| D | **Auto-tune at runtime — adapt the ceiling from observed throughput** | Rejected, decisively. A control loop over a noisy signal, with the measured 12.3% dispersion at low N on wide core sets, would oscillate; it makes the server's behaviour irreproducible run-to-run, which breaks every downstream parity and perf comparison; and it is a mechanism change, which the issue puts explicitly out of scope. |
| A5 | **Derive the SMT ratio from sysfs and use physical cores as the basis** | Rejected — see D3. Undefined under a fractional cgroup quota, needs the affinity mask intersected with the sibling map to be container-correct, and buys a 2× precision the measurement cannot adjudicate (no non-SMT arm exists). |
| E | **Wait for #3306 (footprint-aware admission) and fix it there** | Rejected. #3306 is gated on #3299 E1 and admits by measured LLC bytes — a resource model, not a sizing default. It is a *later* rule over the same knob and B is designed to be replaced by it without an operator-visible break. Blocking a measured, cheap default fix on an unstarted, gated epic item is not a tradeoff, it is a deferral. |

### D10 — Rollout

- **The change is one-directional**: at any P, `clamp(2 × P, 2, 64) ≤ 64`. No deployment admits more
  concurrent scans than it does today. The memory/fd/blocking-pool envelope only shrinks.
- **Restoring today's behaviour is one flag**: `--max-concurrent-scans 64` (or
  `CQLITE_MAX_CONCURRENT_SCANS=64`), logged as `source="flag"` / `"env"`.
- **The startup log is the migration aid**: `max_concurrent_scans_source` distinguishes a derived
  value from a configured one, so a deployment that "suddenly admits 8" is diagnosable from one line.
- **CHANGELOG + release notes** carry it as a behaviour change under `cqlite-flight`, not as a fix.

---

## Risks and residuals, stated

1. **The S=6 prediction (N=24) is unmeasured today.** If the new sweep shows 24 is *worse* than 16 at
   S=6, the coefficient is wrong at that width and the formula must be re-fitted before merge — the
   measurement is a gate on D2, not a confirmation of it. `tasks.md` orders the sweep before the
   default flips.
2. **No non-SMT arm exists.** The 2-per-hardware-thread basis yields half the fitted per-core value
   on a non-SMT host. Named in the operator docs as a known unvalidated extrapolation, with the
   override recipe beside it.
3. **`available_parallelism` under a fractional cgroup quota** rounds by std's rule, which this design
   does not restate and does not depend on: the floor of 2 makes any sub-1-CPU quota land on 2
   regardless. The observed `P` is logged, so the actual rounding is visible in the field rather than
   asserted from documentation.
4. **A deployment relying on the old 64 without setting it** gets fewer permits and, on a wide host
   below 32 threads, possibly lower peak throughput than it had. This is the intended tradeoff, it is
   bounded by the AC5 measurement, and it is one flag to undo.
5. **The corpus must be regenerated**, so the reproduction is not byte-identical to #3217's. The
   geometry-match check (D7) is what makes the two rounds comparable; if the regenerated geometry
   diverges materially, the comparison to the published table must be labelled, not asserted.
