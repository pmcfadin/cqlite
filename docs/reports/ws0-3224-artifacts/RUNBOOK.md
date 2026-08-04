# WS0 #3224 RUNBOOK — the metered-box procedure

**Read this before touching the box. Execute it in order. Nothing here should need re-deriving
under time pressure.**

| | |
|---|---|
| Host | `i4i.metal` `i-08c724c871b2103be` — Xeon Platinum 8375C (Ice Lake-SP), **2 sockets, 64 physical / 128 threads, 2 NUMA nodes**, 88 uncore devices |
| Launched | 2026-08-04T00:44Z |
| **Hard stop** | **2026-08-04T12:44Z** (12 wall-clock hours; an extension is a fresh ask on #3224, not a judgment call) |
| Shares the box with | **#2818** — hand off or terminate explicitly; two `i4i.metal`s is the one outcome the spend authorization does not cover |
| Branch | `issue-3224-ipc-mechanism` |

**Binding ordering: ALL MEASUREMENT RUNS FIRST, artefacts committed as you go; the full
`agent-gate.sh` of record LAST.** A gate compiling on 128 threads while you measure IPC invalidates
the measurement, and re-measuring costs metered hours. Export `CQLITE_GATE_MAX_CONCURRENCY=1` and do
not start any build that is not a measurement input while a capture window is open.

**Do not compare absolutes against #3217.** That host was a single-socket virtualized Sapphire
Rapids guest; this is a two-socket bare-metal Ice Lake-SP. Different microarchitecture, different
memory topology. The design is *both endpoints re-measured on THIS box*, and that self-consistent
pair is sufficient for a mechanism claim. Any sentence comparing a #3224 absolute to a #3217
absolute is a defect.

---

## Pre-flight checklist

- [ ] `date -u` recorded; time remaining against the 12:44Z hard stop written down.
- [ ] Claim ref held: `bash scripts/flow/claim.sh verify 3224`.
- [ ] Worktree on `issue-3224-ipc-mechanism`; `git log --oneline -5` shows this artefact set.
- [ ] `nproc`, `lscpu`, `numactl --hardware` captured to `docs/reports/ws0-3224-artifacts/host/`.
- [ ] **SMT sibling map READ FROM SYSFS**, never assumed (step 3).
- [ ] `free -g` shows enough RAM for a 2 GiB control buffer and the corpus page cache.
- [ ] Cassandra (or anything JVM) **stopped** — nothing else competes for CPU (#3217 §2.1).
- [ ] `perf`, `cc`, `taskset`, `numactl`, `timeout` present.
- [ ] Scratch root on the instance-store NVMe (`/data`), not the EBS root.
- [ ] **`bash selftest-guards.sh` PASSes.** Seconds, no perf and no root, so there is no
      excuse for skipping it — and it is the cheapest step in this runbook by two orders of
      magnitude. It drives the harness's fail-closed guards with the bad input each exists to
      catch, so a guard that has been softened or reordered surfaces **here**, before you
      spend metered bare-metal time producing numbers it would have to certify. Six of these
      guards exist because a roborev round found six fail-open paths in this harness
      (report §7.1); a seventh regression is likelier than not, and this is where it shows.

---

## Step 1 — AC1 capability probe, committed as an artefact

Before anything else, and **before** the positive control, because a bare probe is cheap and its
output is an acceptance artefact in its own right:

```bash
mkdir -p docs/reports/ws0-3224-artifacts/host
{
  echo "== perf stat AC1 probe =="
  perf stat -e LLC-load-misses,LLC-loads,cache-references,cycles,instructions -- true
  echo; echo "== sysfs PMUs (AUTHORITATIVE) =="; ls /sys/bus/event_source/devices/
  echo; echo "== uncore_imc instances =="; ls -d /sys/bus/event_source/devices/uncore_imc* 2>&1
  echo; echo "== perf stat -M MemoryBandwidth =="; perf stat -M MemoryBandwidth -a -- sleep 1
  echo; echo "== lscpu ==";  lscpu
  echo; echo "== numactl --hardware =="; numactl --hardware
  echo; echo "== perf --version / uname =="; perf --version; uname -a
} > docs/reports/ws0-3224-artifacts/host/ac1-capability-probe.txt 2>&1
```

**`ls /sys/bus/event_source/devices/` is the authoritative uncore test.** `perf list | grep uncore`
returns non-zero counts on hosts with NO uncore PMU at all (it lists per-model JSON event-table
entries) — see `negative-control-c7i.md`. Never gate on it.

Commit this file immediately. AC1 says the probe output is a committed artefact; if the run dies
later, this still discharges AC1.

---

## Step 2 — re-apply and RE-VERIFY the sysctls (they are not baked in, and they revert)

`kernel.perf_event_paranoid` was found at **4** on this fresh metal box: **#3249's fix is NOT in the
golden AMI and does not survive a reboot.** #3217 additionally records that both values revert on
their own schedule mid-session (§2.6 trap 8), surfacing later as unsymbolized frames — a failure
that looks like a different problem.

```bash
sudo sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0
# ASSERT it took — a write that did not land is not a state:
cat /proc/sys/kernel/perf_event_paranoid /proc/sys/kernel/kptr_restrict   # must print -1 then 0
```

Re-assert **before every capture**, not once per session. `harness/common.sh`'s `ws0_assert_sysctl`
does this and dies if the value did not take; use it.

---

## Step 3 — regenerate the core-set table for THIS topology (#3217's is wrong here)

`ws0-3217-artifacts/harness/common.sh` hardcodes `ws0_server_cpus_for_s()` as `2,10` / `0,2,8,10` /
`0-3,8-11` / `0-5,8-13` and a client set of `6,7,14,15`. Those encode #3217's host: 16 logical CPUs
with sibling pairs `(c, c+8)`. **On a 128-thread two-socket box every one of those sets is wrong**,
and `selftest.sh` will not catch it — it verifies the topology *derivation*, not the hardcoded
table.

```bash
# Read the real sibling map; never assume (c, c+N).
for c in /sys/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list; do
  echo "$(basename "$(dirname "$(dirname "$c")")") $(cat "$c")"
done | sort -V | tee docs/reports/ws0-3224-artifacts/host/thread-siblings.txt
numactl --hardware | tee -a docs/reports/ws0-3224-artifacts/host/thread-siblings.txt
```

Then rewrite the table with three rules:

1. **Both SMT siblings of a physical core are always pinned together** (physical-core basis, #3217 §2.2).
2. **Every server core set stays inside ONE NUMA node/socket.** This is new — #3217's host had one
   NUMA node, this box has two. A server set spanning sockets measures UPI traffic and cross-socket
   LLC behaviour, which is a *different question* from #3217's and would make the endpoint
   comparison meaningless. S=1 and S=6 must both live on node 0.
3. **The client set is a constant across both endpoints**, 2 physical cores, on the **same NUMA
   node** as the server set, and it **must not overlap** the server set — `perf stat -C <server>`
   would otherwise count client work as engine work. `sweep.sh` refuses to run on overlap; keep that
   guard.

Also bind memory: run the server under `numactl --cpunodebind=0 --membind=0` so page-cache and heap
allocations do not land on the far node. Record the exact invocation in the run config; a
mixed-NUMA allocation is a plausible alternative explanation for an IPC delta and must be excluded
by construction, not by argument.

---

## Step 4 — ⛔ THE POSITIVE CONTROL GATE. Nothing proceeds until it passes.

```bash
cd docs/reports/ws0-3224-artifacts
bash positive-control.sh --out-dir /data/ws0/positive-control --cpu <an idle CPU on node 0>
echo "exit=$?"
cat /data/ws0/positive-control/summary.txt
```

Copy `summary.txt`, `verdict.json`, `event-probe.txt`, `env.txt` and every `perf-*.csv` into
`docs/reports/ws0-3224-artifacts/positive-control-run/` and **commit them before proceeding**.

### STOP-AND-REPORT DECISION POINT

| exit | result | what to do |
|---|---|---|
| **0** | **PASS** | Proceed to step 5. Record the verdict path in the report. |
| 1 | **FAIL** — a required counter is `ABSENT_EVENT_NAME` / `NOT_SUPPORTED` / `SILENT_ZERO` / `UNRELIABLE_*` | **STOP.** Post `verdict.json` + `summary.txt` to #3224, terminate per step 9, and close the issue as **BLOCKED with the probe evidence committed**. Do **not** proceed with a partial counter set. Do **not** "characterize the gap in prose" — that is precisely what owner condition 3 forbids and precisely what #3217 did. |
| 3 | **INDETERMINATE** — the microbenchmark itself was not hostile (P1/P2) | Not a host verdict. Raise `--buffer-mib` well above this box's LLC (54 MiB/socket → 2048 MiB default is ~38×; go higher if needed), confirm `--working-kib 256` is under L2, re-run. If it stays INDETERMINATE after two attempts, stop and report — an uninterpretable control is not a passed control. |
| 2 | **ENV_ERROR** | Missing `perf`/`cc`, or this perf lacks `stat --control fifo:` (needs ≥ 5.13), or the FIFO gate probe failed. Fix the environment; do not remove the gate. |

**Read the verdict, do not skim the exit code.** Note in particular what the control does and does
not assert: it gates on **movement in either direction** for `LLC-loads`/`cache-references` and on a
**rise in the LLC miss rate** for `LLC-load-misses`. It deliberately does **not** assert that raw
`LLC-loads` increases — on this very box the owner's manual walk measured raw `LLC-loads` *falling*
3.5× in the hostile arm on healthy hardware (the prefetcher stops issuing them) while the miss rate
rose 4.4×. A control asserting the naive direction would red a good box.

**The owner's manual-walk numbers are not this script's output** and must never be presented as
such. For the record, and captioned as the owner captioned it:

> The manual hostile arm's IPC of **1.14** equals #3217's degraded S=6/N=16 IPC to two decimals.
> **This is a coincidence, not corroboration** — different workload, different host. No inference
> is licensed from it.

---

## Step 5 — corpus, binaries, harness

**Corpus.** Regenerate from the committed recipe
`docs/reports/ws0-3026-artifacts/ws0-corpus/gen-corpus.sh` (Cassandra 5.0.8, `MAX_HEAP_SIZE=8G`,
`cassandra-stress` user profile, flush per 50k-partition batch, then `flush` + `compact` → exactly
**one `nb-16-big` SSTable**, 8 components). `cassandra-stress` is not byte-deterministic, so the
accepted bar — the one #3100 and #3217 both used — is **matched geometry plus a documented new
sha256**.

Target geometry, and the checks that establish it:

| metric | target | oracle |
|---|--:|---|
| rows | 3,999,890 | `sstablemetadata totalRows` **and** an independent `fullscan.py` over 512 token ranges. Never a CQL `count(*)` — it server-side-times-out past 4M rows. |
| `totalColumnsSet` | 35,999,010 | `sstablemetadata` |
| logical B/row | 693.29 | `CompressionInfo.db` `dataLength` ÷ rows (`harness/corpus-basis.py`) |
| on-disk B/row | 196.09 | exact sum of `*-Data.db` ÷ rows |
| SSTable count/format | 1 / `nb-16-big` | `ls` + `sstablemetadata` |
| droppable tombstones | 0.0 | `sstablemetadata` |

Record the new `sha256(Data.db)`. **`now`-pinning is N/A** and must be *recorded* as N/A: the
`ws0.events` fixture carries no TTL and no tombstones (min/max local deletion time
`9223372036854775807`, TTL 0/0), so no read-time reconciliation depends on `now`. Keep it so.

**Binaries** — build from this worktree only, and record the `main` SHA it descends from:

```bash
CARGO_PROFILE_RELEASE_STRIP=none CARGO_PROFILE_RELEASE_DEBUG=true \
  RUSTFLAGS="-C force-frame-pointers=yes" \
  cargo build --release -p cqlite-flight -p flight-loadgen
```

(`[profile.release]` hardcodes `strip = true`, hence the env overrides. Frame pointers because
`--call-graph=dwarf` hangs past 120 s on this binary — #3217 §2.6 trap 6.)

**Harness env.** #3217's `llc-run.sh` sources `/data/ws0/ws0env.sh`, which was **never committed**.
Reconstruct it from the variable table in `ws0-3217-artifacts/harness/README.md`:

```bash
export WT=<this worktree>
export WS0_STAGE=/data/ws0/ws0-corpus/sstables
export WS0_FLIGHT_BIN=$WT/target/release/cqlite-flight
export WS0_LOADGEN_BIN=$WT/target/release/flight-loadgen
export WS0_TICKET_TPL=$WT/docs/reports/ws0-3100-artifacts/ws0-h2h/ws0-events-template.json
export WS0_ROOT=/data/ws0
```

Server flags are fixed to #3100's recorded invocation and are the defaults in `common.sh`:
`--batch-size 8192 --max-batch-bytes 4194304 --max-inflight-egress-bytes 12582912
--max-concurrent-scans 16 --admission-wait-timeout-ms 30000`. **`--batch-size 8192` is an AC2
requirement — do not tune it.**

Run `./selftest.sh` before the first real capture. Remember it does **not** validate the core table
you rewrote in step 3.

---

## Step 6 — the two endpoint captures

Two points only. **No new (S,N) matrix** — widening the sweep is a different issue.

| label | S (physical cores) | N (streams) | notes |
|---|--:|--:|---|
| `llc-s1-N2` | 1 | 2 | #3217's 1-core peak |
| `llc-s6-N16` | 6 | 16 | #3217's 6-core peak |

Both: warm (page-cache resident, `CQLITE_FLIGHT_MERGE_PATH=bypass`), both SMT siblings of each
physical core pinned, server set inside NUMA node 0, client set constant across both points.

Counter set to program at each point — one `perf stat -C <server-cpus>` over the steady-state
window:

```
cycles, instructions,
LLC-loads, LLC-load-misses, cache-references, cache-misses,
L1-dcache-loads, L1-dcache-load-misses, dTLB-load-misses, branch-misses,
task-clock
```

Plus, in a **separate** invocation (uncore events cannot share the core PMU group and would
multiplex):

```
uncore_imc_0..11/cas_count_read/, uncore_imc_0..11/cas_count_write/
```

DRAM bytes = (CAS_read + CAS_write) × 64. Sum across **all** IMC instances on **both** sockets, and
report the per-socket split — a large far-socket component is itself a finding. Also capture
`perf stat -M MemoryBandwidth` at each point and name in the report which source the published
figure came from (AC3 requires the source be named explicitly).

**Check the multiplexing column.** `perf stat -x,` field 5 is the enabled percentage; anything below
100% means the counts are scaled estimates. If the core set multiplexes, split it into two
invocations rather than publishing scaled values silently.

Per point, retain: the raw `perf stat -x,` CSV, the loadgen `step.jsonl`, `cpu-topology.json`, the
run config, and a capture-config JSON in the shape of
`ws0-3217-artifacts/partB-results/counters/llc-capture-config.json` (label → `s_spec`, `N`,
`window_secs`, `server_physical_cores`, `server_hw_threads`, `server_cpus`). **The window is data,
never a literal in the analysis.**

### Three method gaps in #3217 that you must close here, not inherit

1. **The counter captures were reps=1.** #3217 required ≥3 reps for curve points but ran each
   `llc-*` capture exactly once (`run-partB4.sh`), so the headline IPC figures carry no dispersion.
   **Run ≥3 reps per endpoint and publish min/median/max.** The whole deliverable is a delta between
   two points; an undispersed delta cannot be defended.
2. **rows/s came from the whole loadgen step, not from the perf window.** `cycles/row` was computed
   as `counter ÷ (rows_per_s × window_secs)` where `rows_per_s` covers the entire step including
   ramp and drain — at `llc-s1-N2` that was **4 completed requests over 63.99 s** against a 20 s
   counter window. The rate is not uniform across that step, so the s1/N2 per-row figures are the
   most exposed number in #3217. **Emit rows completed *within the counter window*** (or align the
   window to a whole number of completed requests) and state which you did.
3. **Driver `rc=` was fabricated.** Every #3217 driver did `echo "$(date) END rc=$?"`, where the
   command substitution overwrote `$?` — so a failed step logged `rc=0`. The fix is in the committed
   drivers (capture `rc` **before** any other command substitution); do not reintroduce it, and do
   not audit this run from a ledger without checking that fix is present.

Also inherit, unchanged: the **client-saturation validity gate** (client pinned set > 70% busy →
the point measured the loadgen and must not be quoted as a server number; #3217 saw ≤13.8%), and
**warmth verified not assumed** (`/proc/<pid>/io` `read_bytes` = 0 summed over the run; `rchar`,
`read_bytes` and `syscr` are three different layers, reported side by side and **never divided by
one another**).

---

## Step 7 — the cycles-per-row accounting (AC4). The residual is a NUMBER.

For each endpoint compute, from committed inputs only:

```
instructions/row = instructions ÷ rows_in_window
cycles/row       = cycles       ÷ rows_in_window
IPC              = instructions ÷ cycles          (invariant to the window — a pure ratio)
<event>/row      = <event>      ÷ rows_in_window  for every counter above
```

Then attribute the measured cycles/row delta between the endpoints:

1. **Δ = cycles/row(S=6,N=16) − cycles/row(S=1,N=2).** This is *this box's* delta. #3217's +8,593 is
   a different host's number: report both, and report the attribution as a fraction of **this box's
   Δ**, with #3217's figure as context only.
2. For each miss counter, charge `Δ(misses/row) × penalty_cycles`, with **every penalty stated
   explicitly and sourced** (measured on this host where possible — e.g. an idle-latency probe for
   the DRAM penalty — otherwise a cited vendor figure). A penalty with no source is not an
   attribution.
3. Sum the charged components → **attributed cycles/row**.
4. **`residual = Δ − attributed`; publish `residual` and `residual ÷ Δ` as a percentage.**
   **AC4 explicitly fails if this number is omitted.** #3217's equivalent was ~87% unattributed and
   saying so plainly is what made that report usable.
5. Cross-check against #3217's closure model, which predicted 0.7237 marginal efficiency against a
   measured 0.7111 (a 1.26 pp gap). Report where **your** accounting lands against **your** measured
   efficiency, and name the gap. This is a check on your own arithmetic, not a target.

Do not round toward the hypothesis. **A well-measured negative is a pass** (AC7): "the memory system
is not saturated and the decay is something else, here is the residual" is a successful outcome.

Every headline must **re-derive from committed artefacts** before publishing (#3226) — write the
derivation as a committed script that reads only committed inputs, as
`partB-run/parse-llc-counters.py` does for #3217, and run it from a clean checkout.

---

## Step 8 — the AC5 saturation verdict

1. **Measured DRAM bandwidth at S=6/N=16**: (CAS_read + CAS_write) × 64 ÷ window, summed over all
   IMC instances, with the per-socket split shown.
2. **Achievable peak on the SAME host**: `cache-hostile stream` provides a STREAM-triad-class
   reference with no extra dependency —
   ```bash
   ./cache-hostile stream --stream-mib 4096 --threads <threads in the server set> --iters 10
   ```
   Run it **pinned and NUMA-bound exactly like the engine arms** (`numactl --cpunodebind=0
   --membind=0 taskset -c <server set>`), because a peak measured across both sockets is not the
   ceiling the engine faces. It reports two byte bases (24 B/element architectural, 32 B/element
   including read-for-ownership) — **quote which one you used**; never a bare GB/s. Label it
   *STREAM-triad-class, not the vendor STREAM benchmark*. If a real STREAM binary is available,
   prefer it and say so.
   Cross-check the peak against a second source if cheap (`uncore_imc` under the triad itself).
3. **The verdict sentence**, which is the deliverable #2817 and #3096 consume:
   > At S=6/N=16 the memory system is / is not saturated: measured **X GB/s** (basis named) against
   > an achievable **Y GB/s** on this host = **Z%** of peak. Therefore reducing per-row work is
   > expected to move the scaling **slope / only the level**.

   State it as one sentence, with the numbers in it, and with the byte basis named.

---

## Step 9 — the gate, then the verified exit

**Only now** run the full gate of record (measurement is finished; nothing is competing):

```bash
export CQLITE_GATE_MAX_CONCURRENCY=1
AGENT_GATE_SUMMARY_FILE=/tmp/gate-3224.txt bash scripts/agent-gate.sh > /tmp/gate-3224.log 2>&1 < /dev/null
cat /tmp/gate-3224.txt
```

Then open the PR, and hand off or tear down.

### Exit contract — CORRECTED (owner amended condition 2)

`terminate-instances` + `describe-instances` is **NOT sufficient**: `agent-ami` keeps the `/data`
EBS volume on terminate and **it bills until deleted**.

- [ ] **#2818 handoff decided and recorded on that issue**: either the live instance id + remaining
      time box, or "terminated" stated plainly. Do this **before** you tear down.
- [ ] `agent-ami down 4`
- [ ] **Delete the `/data` EBS volume.**
- [ ] **Read back that BOTH are gone**: `agent-ami volumes` shows no lingering data volume, and the
      instance shows `shutting-down`/`terminated`. **A state written but not observed is not a
      state.**
- [ ] Paste the read-back into #3224.

### If the clock runs out

At **12:44Z**, terminate and report what you have. An extension is a fresh ask on #3224 — it is a
cheap ask, and the time box exists to stop a forgotten instance, not to rush the measurement.
Partial results with an honest boundary are worth more than a rushed full attribution: commit every
artefact captured so far, state exactly which acceptance criteria are discharged and which are not,
and post that list.

---

## Artefact layout (mirrors #3217's discipline)

```
docs/reports/ws0-3224-artifacts/
  RUNBOOK.md                      this file
  positive-control.sh             the condition-3 gate
  selftest-guards.sh              pre-flight: every fail-closed guard vs the bad input it catches
  cache-hostile.c                 its microbenchmark + STREAM-triad reference
  negative-control-c7i.md         why virtualized substitution is barred
  negative-control-c7i-probe.txt  raw probe behind it
  harness/verdict-logic.sh        the positive control's verdict math (sourced, testable)
  harness/guards.sh               the shared "a nonzero rc stops the run" guards
  host/                           ac1-capability-probe.txt, thread-siblings.txt, lscpu, numactl
  positive-control-run/           summary.txt, verdict.json, event-probe.txt, env.txt, perf-*.csv
  guard-selftest/                 recorded selftest output + the mutation matrix behind it
  corpus/                         geometry, sha, provenance, deviations from the recipe
  run/                            the capture drivers as invoked, plus the three checkers whose
                                  EXIT CODE is a verdict: penalty-window-check.py (was the
                                  window gated?), rep-complete.py (is this rep skippable?),
                                  ac5-analyse.py (did byte accounting resolve?)
  results/                        per-endpoint perf CSVs, step.jsonl, capture-config.json,
                                  cpu-topology.json, the derivation script + its output
docs/reports/ws0-3224-report.md   the report
```

Logs go to `/data/ws0/logs/` and are **never read into an agent context**.
