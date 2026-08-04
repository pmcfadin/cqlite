# PROPOSED follow-up issues from #3217 — NOT FILED. Priority and scope are the owner's call.

#3217 is measurement-only and files no fixes. These are the candidates its data actually supports,
each with the specific measured number it rests on, an oracle-vs-design routing recommendation, and
draft acceptance criteria. **One candidate has had its headline justification deliberately removed
(F2) and one alternative framing was folded into another (see the closing note) — the data did not
support them as originally shaped.**

Ranking below is by *evidential strength*, not by product priority.

---

## F1 — Establish the microarchitectural cause of the full-box IPC decay on a host with working LLC / memory-bandwidth counters

**Strength: STRONGEST. This is the one genuinely open question #3217 leaves.**

### Rationale

Between the 1-core peak (S=1/N=2) and the 6-core peak (S=6/N=16), CQLite's Flight read path
executes **the same work more slowly**: instructions/row **38,343 → 38,382 (+0.1%, flat)**,
cycles/row **25,200 → 33,793 (+34.1%)**, IPC **1.52 → 1.14 (−25.4%)**. The counter closure model
(predicted 0.7237 vs measured 0.7111 marginal efficiency, gap 1.26 pp) attributes **25.4 of the
29 pp scaling shortfall to IPC decay alone**. But the *cause* of that decay is unmeasured:
`LLC-loads`, `LLC-load-misses` and `cache-references` all read `<not supported>` on #3217's
virtualized host. The counters that did work — L1d miss/row +7.5% relative, dTLB miss/row +40.9%
relative — account for only **~10–13% of the +8,593 cycles/row**, leaving **~87% unattributed**.
This is not a nice-to-have: it is the question that decides whether any per-row-work lever
(#3096 Arrow encode included) can move the *slope* of the scaling curve or only its *level*. Until
it is answered, every ordering argument about the scaling discount rests on a hypothesis
("LLC/memory bandwidth") with no measurement behind it. The work is small — two endpoint points,
one afternoon — and it is gated entirely on host selection.

### Routing

**Oracle-driven / measurement-only — GitHub issue + committed report, no OpenSpec.** Same routing
as #3217 and #3100: no production code changes, the oracle is the measured data.

### Draft acceptance criteria

1. Host selection is **verified before the run**, not assumed: `perf stat -e
   LLC-load-misses,cache-references,cycles true` on the target instance prints real counts, not
   `<not supported>`. The probe output is a committed artefact. If no available host programs
   these counters, the issue closes as **BLOCKED with the probe evidence**, not with a partial
   answer.
2. The two #3217 endpoint points are reproduced on that host: S=1/N=2 and S=6/N=16, same corpus
   geometry (3,999,890 rows, 693.29 B/row logical, single `nb-16-big` SSTable, warm), same
   physical-core basis with both SMT siblings pinned, `--batch-size 8192`. Aggregate rows/s within
   a documented band of #3217's, or the divergence explained.
3. Reported per point, per row: `LLC-load-misses`, `LLC-loads`, `cache-references`, plus a
   memory-bandwidth figure (`uncore_imc/*` counters, `perf stat -M MemoryBandwidth`, Intel PCM or
   `toplev` level-2 — whichever the host supports, named explicitly).
4. A cycles-per-row **accounting** at each point that charges the measured miss counts at stated
   penalties and reports what fraction of the +8,593 cycles/row is now attributed. The residual
   unattributed fraction is stated as a number, not omitted.
5. An explicit statement of whether the memory system is **saturated** at S=6/N=16 (measured
   bandwidth vs the host's achievable peak, e.g. via a STREAM-class reference measured on the same
   host), and therefore whether reducing per-row work would be expected to move the scaling slope
   or only the level.
6. Byte basis named on every throughput figure; `now`-pinning N/A recorded if the fixture keeps no
   TTL/tombstones.

---

## F2 — `cqlite-core` raw-chunk channel granularity: quantify the LATENCY and low-concurrency cost of ~347 channel sends per Flight batch

**Strength: MODERATE, with its headline justification REMOVED. Read the "what the data does NOT
support" paragraph before ranking this.**

### Rationale

The bypass read path stacks four bounded channels between SSTable and wire, and the 16 KiB
raw-chunk channel (`RAW_CHUNK_CHANNEL_CAP` = 8, one send per compression chunk) is the busiest by a
wide margin. Corpus geometry: 3,999,890 rows / 169,257 chunks = **23.63 rows/chunk**, so an
8,192-row Flight batch costs **~347 raw-chunk sends**, of which **265–327 actually park** (76–94%).
By blocked *duration* it is the single largest causal site at every point measured — **473.2 s of
1,963.7 s at S=6/N=16**, and **27.08 s = 49.2% of the causal blocking at S=1/N=1**, the arm with
22% of its pinned set idle. Against that, the `do_get` handoff #3217 was opened to investigate
parks **zero** times per batch. If any channel in this pipeline is worth re-examining, geometry and
both instruments agree it is this one, not the Flight handoff.

**What the data does NOT support, and what has therefore been cut from this proposal:** the claim
that this costs *saturated aggregate throughput*. At S=6/N=16 the server runs at **96.7%
utilisation** with **flat instructions/row (+0.1%)** — a thread parked on a full bounded channel is
overlapped by another thread on the same core, and #3217's own closure model attributes only
**2.2 pp** of the 28.9 pp shortfall to residual idle *in total*. So "coarsen the raw-chunk channel to
recover throughput" is **not** a claim #3217's data can back, and this proposal does not make it.
What remains defensible is narrower and should be scoped as such: (a) **per-scan latency** — at
S=1/N=16 the per-scan p50 is **302 s** against **31 s** at N=2, and pipeline serialization is the
named mechanism; (b) **low-concurrency and single-stream operation**, where the box is *not*
saturated and blocked time is not overlapped (S=6/N=1 runs at 11.5% utilisation, S=4/N=1 at 18.1%);
(c) the memory-traffic hypothesis in **F1** — if the per-chunk pipeline is a principal source of
memory pressure, F1 will say so, and this issue should be **sequenced after F1** rather than funded
on its own.

### Routing

**Design-driven — OpenSpec.** Changing `RAW_CHUNK_CHANNEL_CAP` / chunk-batching granularity trades
against the documented streaming memory bound in
`cqlite-core/src/storage/sstable/reader/scan_stream_windowed.rs` (`window + RAW_CHUNK_CHANNEL_CAP`
raw chunks resident) and against CQLite's <128 MB memory target. That is a design tradeoff with a
stated invariant, not a bug fix with an oracle.

### Draft acceptance criteria

1. A measured latency baseline before any change: per-scan p50/p90/p99 at S=1 and S=6 across
   N = 1, 2, 4, 8, 16, on the #3217 corpus geometry, medians of ≥3.
2. The design proposal states the **memory bound explicitly** for the proposed granularity —
   worst-case resident bytes as a function of chunk size, channel capacity and stream count — and
   shows it against the <128 MB target at `--max-concurrent-scans 16`.
3. A measured before/after at **both** ends of the utilisation range: an unsaturated point
   (S=6/N=1, ~11.5% util) and a saturated one (S=6/N=16, ~96.7% util). **A saturated-point
   throughput improvement is not required and must not be claimed as the justification**; the
   result at that point may legitimately be "no change", and reporting that is a pass.
4. Re-run off-CPU attribution after the change and show the `core_raw_chunk` blocked-duration and
   parks-per-batch figures moving as predicted by the new geometry (predicted sends/batch stated
   before the measurement, not fitted after).
5. No regression in aggregate rows/s at any (S,N) in the #3217 matrix beyond measured dispersion.
6. Correctness unaffected: existing read-path parity oracles green, including the query-semantics
   oracle and the point-vs-full differential lane.

---

## F3 — Investigate glibc allocator arena contention as the mechanism behind "a single stream gets 24% slower as cores are added"

**Strength: MODERATE-TO-STRONG for the phenomenon, CIRCUMSTANTIAL for the mechanism.**

### Rationale

Two #3217 findings point at the same place and neither is explained. First, the phenomenon:
**single-stream throughput declines monotonically as the runtime is given more cores** — 216,229
(S=1) → 205,129 (S=2) → 175,872 (S=4) → **163,510 (S=6)** rows/s, **−24%**, at 11.5% server
utilisation. Second, the candidate mechanism: **S=6/N=1 is the outlier point in the entire matrix**
on two independent instruments. It records **1,677 glibc `malloc` arena-lock parks per 8,192-row
batch** — **64% of all voluntary parks by count**, against 163 at S=1/N=1 and 243 at S=6/N=16 — and
it is the **only** point where instructions/row is not flat (**43,241 vs 38,343 at the 1-core peak,
+12.8%**). glibc creates per-thread arenas up to `8 × ncores`, so a runtime sized to 6 cores spreads
one stream's allocations across more arenas with more cross-arena lock traffic; that is a plausible
mechanism for both numbers, and it is currently only a plausible mechanism.

This finding exists **only because both park instruments ran**: the allocator is 64% of parks by
**count** and just **2.0% of blocked time by duration**. `offcputime` alone would have said the
allocator is invisible; `sched_switch` counts alone would have said it is the dominant problem.
Neither is right — "very frequent, very short" is.

### Routing

**Two-stage.** Stage 1 is **oracle-driven / measurement-only** (GitHub issue, no OpenSpec):
establish whether the allocator is causal for the −24%. Stage 2 — *if* stage 1 confirms it and the
remedy is a different allocator (jemalloc/mimalloc) or an arena-count pin — is **design-driven,
OpenSpec**: it adds a dependency, changes the memory-behaviour profile, and affects every binding
and downstream embedder, which is a product decision and not a tuning knob.

### Draft acceptance criteria (stage 1)

1. The −24% single-stream decline is reproduced across S ∈ {1,2,4,6} at N=1, medians of ≥3, with
   dispersion — confirming it is not a #3217 artefact.
2. A controlled arena experiment at the same points: `MALLOC_ARENA_MAX` = 1, 2, 4, default, with
   rows/s, instructions/row, cycles/row, IPC and allocator parks/batch reported for each. If
   capping arenas does not move the −24%, the allocator hypothesis is **falsified** and that is a
   passing outcome to be reported as such.
3. Both park instruments run at every point (duration-weighted off-CPU *and* `sched:sched_switch`
   event counts), with the count-vs-duration split reported explicitly — because the whole finding
   is invisible to either alone.
4. The +12.8% instructions/row at S=6/N=1 is either attributed (to allocator work, to a runtime
   path, or to something else named) or reported as still unattributed with the fraction stated.
5. Whatever the verdict, the report states plainly whether this matters in production: it is a
   *low-concurrency* effect, and #3217 shows it is absent at N=16 (allocator parks fall to 243/batch
   at S=6/N=16). An improvement here does **not** raise the box's peak throughput.

---

## F4 — Harness + doctrine: bcc `offcputime`'s silent 10,240-key truncation is a fleet-wide measurement hazard

**Strength: STRONG. This is a mechanism gap, and it already invalidated real data once.**

### Rationale

bcc's `offcputime` stores stacks in a `BPF_HASH` that **defaults to 10,240 keys and truncates
silently** — `--stack-storage-size` sizes a *different* map and does not help. In #3217 this
invalidated an entire round of N≥8 off-CPU captures: **10,240 keys captured against 108,475 real
unique stacks at S=6/N=16**, i.e. the tool discarded ~90% of the distinct stacks and reported the
remainder as if it were the profile. Nothing in the output says so. Worse, the failure is directionally
malicious for this exact class of question: a truncated profile, an unprivileged BPF map creation
(`perf_event_paranoid` does **not** cover BPF maps or tracefs), and an `offcputime` window that never
contains a switch-**in** all produce an **empty or thin off-CPU profile — which reads identically to
"the site under investigation is innocent."** #3217's headline result *is* an acquittal, so the
distance between a true finding and a tooling artefact here was one unfixed default. Three other
traps in the same family were paid for in the same run (Rust v0 mangling defeats a demangled-spelling
classifier; bpftrace `ustack` leaves ~50% raw hex where `perf script` symbolizes fully;
`--call-graph=dwarf` hangs on this binary). None of this is #3217-specific — the next agent that
reaches for an off-CPU profile will hit all of it.

A related, smaller hygiene gap from the same run: **16.86–17.94% of SERVER-thread on-CPU frame
instances resolve only to the DSO `[libc.so.6]`** with no function symbol (the un-symbolized system
allocator; all-frames basis 14.25–17.94%), and those frames are **not** counted by the AC3
unsymbolized gate because they carry a DSO name. The gate reads 0.009–0.028% while ~17% of server
frames are opaque at function granularity. (The band is emitted per profile into
`partB-results/oncpu/AC3-oncpu-summary.json` and is recomputable from the committed
`oncpu/*.folded.gz`; an earlier in-flight note quoted an unsourced, narrower 17.2–17.8%.) Installing glibc
debug symbols would close the gap that F3 most needs closed.

### Routing

**Design-driven — OpenSpec (process/doctrine).** It changes the agent-facing measurement doctrine
and adds a committed tool to the repo; per the routing rule, process changes are the OpenSpec front
door. Small enough that the owner may reasonably prefer a plain issue.

### Draft acceptance criteria

1. The patched collector (`ws0-3217-artifacts/partB-run/offcputime-bigmap`, 1e6-key counts map) is
   promoted out of the per-issue artefacts dir into a durable, discoverable location, with a
   one-line README stating what the stock tool does wrong.
2. A **profiling traps** section is added to the agent doctrine (validation playbook and/or
   `docs/development/dev-cookbook.md`) covering, each with the symptom a reader would actually see:
   BPF map creation needing sudo independently of `perf_event_paranoid`; tracefs needing sudo
   likewise; `offcputime` charging only on switch-in; the 10,240-key silent truncation; Rust v0
   mangling in bcc/bpftrace folded output; bpftrace `ustack` vs `perf script` symbolization; dwarf
   unwinding hanging on large binaries; `perf_event_paranoid` silently reverting.
3. The doctrine text states the **generalisable rule**, not just the instances: *an empty or thin
   off-CPU profile is indistinguishable from an innocent verdict, so an off-CPU acquittal is only
   admissible when accompanied by evidence the collector was working* — e.g. a non-trivial total
   blocked time, a reported `stack traces lost` count, and a second instrument that counts events
   rather than duration.
4. A **positive-control** procedure is documented and scripted: a workload with a known-blocking
   site that the collector must find, run before any acquittal is recorded.
5. The `stack traces lost` counter is surfaced in the harness output rather than left in the
   collector's stderr, and any non-zero value is reported in the analysis rather than dropped.
6. glibc debug symbols (`libc6-dbg` or equivalent) are added to the measurement-box bootstrap, and
   the AC3-style unsymbolized gate is extended to report **DSO-only frames** as a second, separately
   named figure alongside fully-unsymbolized frames — so a profile that is 18% opaque cannot report
   0.02%.
7. Doctrine publish is verified by the **new content being served**, not by HTTP 200.

---

## F5 — Concurrency guidance and admission defaults: the throughput-optimal stream count moves with core count

**Strength: STRONG. Directly measured, four arms, tight dispersion.**

### Rationale

#3217 measured the throughput-optimal N at each server width, and it is **not constant**: the peak
sits at **N=2 for 1 core, N=8 for 2 cores, N=16 for 4 and 6 cores**. Running past the peak costs
real throughput on narrow servers — at S=1, N=16 delivers **211,010 rows/s against 252,420 at N=2,
a 16.4% loss**, and per-scan p50 latency degrades from **31 s to 302 s** for that privilege
(S=2 loses 5.3% going from N=8 to N=16). Meanwhile the shipped default is
`--max-concurrent-scans 16` regardless of how many cores the server actually has, and #2420's
admission control has no core-awareness. On a 1–2 core container — which is exactly what a
Trino worker sidecar or a small deployment looks like — the default admits 8× more concurrent scans
than the throughput peak supports, converting throughput into queueing. Admission never errored in
#3217 (`requests_unavailable` = 0 at all 83 points), so this is not a stability bug; it is a
defaults-and-guidance gap with a measured cost.

### Routing

**Design-driven — OpenSpec.** It touches the CLI/operator surface (`--max-concurrent-scans`
default and its documentation), and choosing between "core-aware default", "documented sizing
guidance only", and "leave as-is" is a product decision, not an oracle question.

### Draft acceptance criteria

1. The measured peak-N-by-core-count table from #3217 is reproduced and extended to the widest
   server configuration in scope, with medians of ≥3 and dispersion, and the throughput and
   per-scan-latency cost of over-admission stated at each width.
2. A decision is recorded on whether `--max-concurrent-scans` gets a **core-aware default** — with
   the derivation rule stated as a formula (e.g. peak-N as a function of available physical cores),
   validated against the measured table, and its behaviour under cgroup CPU limits and CPU
   affinity masks defined explicitly (a container-limited server must not read the host's core
   count).
3. Whatever the decision, operator-facing documentation states the measured relationship, including
   the 16.4%-at-1-core over-admission cost and the latency curve, so a deployer sizing a narrow
   worker has the number.
4. Any default change is backwards-observable: the effective value is logged at startup, and an
   explicit flag always wins over the derived default.
5. No regression at the widest configuration, where the current default is already optimal.

---

## Candidates considered and NOT proposed

- **"Fix the `do_get` mpsc handoff" / "reduce `DO_GET_CHANNEL_CAPACITY` pressure" — DROPPED.**
  This was the hypothesis #3217 existed to test and it is falsified: zero voluntary parks per
  8,192-row batch at every point measured, 1.46 s of 1,963 s blocked time at the worst point
  (0.074%), `egress_credit_acquire` ≈ 0. There is nothing to recover. Filing it would fund work at
  a site measured at zero cost.
- **"Raw-chunk channel is costing saturated throughput" — DROPPED as a claim**, and F2 was rewritten
  without it. At 96.7% utilisation with flat instructions/row, blocked time on one thread is
  overlapped by others; the closure model leaves only 2.2 pp of the 28.9 pp shortfall for *all*
  residual idle combined. F2 survives on latency and low-concurrency grounds only, and is sequenced
  after F1.
- **A standalone "single stream slows 24% with more cores" issue — FOLDED into F3.** The phenomenon
  and its leading candidate mechanism are measured at the same point (S=6/N=1) on the same run;
  splitting them would produce one issue that can only observe and one that can only guess.
- **"tokio scheduler overhead" — NOT PROPOSED.** `tokio_scheduler` is 760 s of blocked time at
  S=6/N=16, second-largest in the table, but it decomposes 100% into *idle* park (io-driver epoll
  306.6 s, blocking-pool idle 233.6 s, idle worker park 220.0 s). That is threads with nothing to
  do, not overhead. Filing it would be a misreading of the table.
- **"gRPC / tonic egress" — NOT PROPOSED.** 19.35 s of 1,963.72 s blocked (0.99%), 1 park per batch,
  and `tonic` + `h2` + `hyper` + `prost` together are **≤1.1%** of on-CPU server weight. #3100's
  device acquittal now has an egress acquittal beside it.
- **"Disk / read-path I/O" — NOT PROPOSED.** `disk_io` = 0 s at every point and `read_bytes` = 0
  across all 83 Part A points. The corpus is fully page-cached by design; this run says nothing
  about cold I/O and should not be cited about it.
