# DRAFT — for #2817 (per-box projections). NOT POSTED. Owner posts.

---

## Measured: the marginal-efficiency discount, replacing the assumed 0.6–0.75

#3217 measured the full-box C(N) curve that every per-box projection in this epic has been
quoting from an assumption. Full method, artefacts and the attribution that backs the ordering
call: `docs/reports/ws0-3217-report.md`.

**One box** (Intel Xeon Platinum 8488C, **8 physical** / 16 logical, SMT on, 1 NUMA node, kernel
6.17.0-1019-aws), CQLite `main` @ `693ae41` (post-#3058), Flight `do_get` bypass path, warm
(page-cached, `read_bytes` = 0 verified on all 83 points), server pinned to S physical cores
(both SMT siblings each), client fixed at 2 physical cores for every arm. Corpus: one
`nb-16-big` SSTable, 3,999,890 rows, **693.29 B/row logical / 196.09 B/row on-disk**, no TTL and
no tombstones (so `now`-pinning is N/A). Medians of 3 reps.

**Read "full box" precisely, because this table will be quoted.** The widest arm is **S=6 — six of
the box's eight physical cores** — and the server is **pinned** to those six, not spread across all
eight. The remaining two physical cores are reserved for the client, which the issue's own
"client isolated" requirement forces: a client sharing server cores would make `perf stat -C
<server-cpus>` count loadgen work as engine work. **There is no S=8 point and none is claimed.** So
the 0.711 figure is "6 pinned cores vs 1 pinned core", and any projection to a full 8- or 16-core
box is an extrapolation beyond what was measured — the curve's shape (0.873 / 0.811 / 0.711)
supports extrapolating, but the endpoint is not a measurement.

### The table

| S (physical cores) | best aggregate rows/s | N@peak | server util | **marginal efficiency vs 1-core PEAK** | marginal efficiency vs 1-core N=1 |
|---:|---:|---:|---:|---:|---:|
| 1 | 252,420 | 2 | 0.996 | **1.000** | 1.167 |
| 2 | 440,677 | 8 | 0.995 | **0.873** | 1.019 |
| 4 | 818,747 | 16 | 0.992 | **0.811** | 0.947 |
| 6 | **1,076,917** | 16 | 0.967 | **0.711** | 0.830 |

Byte basis at the 6-core point: **746.6 MB/s logical/uncompressed**, **211.2 MB/s on-disk
compressed** (13.13 GB/s is the Arrow buffer *capacity* figure the loadgen reports — it is **not**
gRPC-on-the-wire bytes and must not be quoted as throughput).

**Two denominators, both published, neither silently chosen.** The primary is S=1's **peak**
(252,420 rows/s at N=2) — the most the engine achieves on one physical core, and the conservative
choice. The naive N=1 denominator is shown beside it. Per-arm *self*-normalised speedups (each arm
÷ its own N=1) are **not cross-comparable** and are deliberately excluded: each arm's own N=1
declines with core count (216,229 → 205,129 → 175,872 → 163,510 rows/s, because one stream spread
over 12 hardware threads loses to work-stealing and locality), so self-normalising flatters the
wide arms by dividing them by a worse baseline.

Client CPU% is published at every (S,N) in the report. Max across the entire run: **13.8%** of the
2-core client budget, against a 70% validity gate. **Zero points excluded**, no server core ever
traded back, every arm honestly labelled at its stated core count.

### What this does to this epic's projections: nothing — and that IS the answer

The measured discount (**0.711 at 6 cores**, 0.811 at 4, 0.873 at 2) lands **inside** the assumed
**0.6–0.75** band at the widest point measured and above it at 2 and 4 cores. **The projections
built on 0.6–0.75 do not need revision.** The open question this epic carried — "is the guessed
discount defensible?" — is now closed with a measurement rather than reopened.

It also **falsifies the pessimistic scenario** that motivated #3217: the discount does **not**
compound across cores. A 6-core server delivers 4.27× a 1-core server's peak, not the ~2–3× the
collapse scenario required. **There is no 2.5–4× box-level handoff lever.** A perfect handoff fix
is worth at most ~1.2–1.4× at box level.

---

## The lever call — one line

**Order a `do_get`-handoff fix LAST, not first: it is measured at ~0 cost (zero voluntary parks,
0.074% of blocked time), so #3096 (Arrow encode) is the better-founded lever of the two — but note
carefully that #3096 is a lever on *absolute* throughput, and this data does NOT show it is a fix
for the *scaling discount*.**

### Why, from the data

**The handoff is measurably not costing anything.** Off-CPU stacks (patched `offcputime`) and
independently-counted `sched:sched_switch` park events agree:

- `do_get_mpsc_handoff`: **0** voluntary parks per 8,192-row batch at every point measured
  (S=1/N=1, S=6/N=1, S=6/N=16). Not "few" — zero.
- `do_get_batch` blocked time: **1.46 s out of 1,963 s** at the worst point = **0.074%** of
  total blocked time, 0.12% of all channel send/recv blocking.
- `egress_credit_acquire`: ≈0 s everywhere (max 0.0014 s).
- **The bound on that acquittal, stated rather than buried**: `unattributed_channel` — parks the
  classifier could bucket but not tie to a named channel — holds **109.63 s (5.6% of total blocked
  time)** at the same point, **75× larger** than `do_get_batch`'s 1.46 s (0.07%). Even if the whole
  residue were secretly the handoff (it is not — the independent park-count instrument records zero
  handoff parks with `other` = 0), the handoff would be a ~6% site, not a 2.5–4× box-level lever.
  The ordering call below survives its own error bar.

The parks that *do* exist (~1,000–2,600 per 8,192-row batch) are **inside `cqlite-core`, below the
Flight layer** — the 16 KiB raw-chunk channel (~347 sends/batch by corpus geometry, 265–327 parks
measured), the 128-row query-rows channel and the 256-row windowed-batch channel. The bypass path
stacks **four** bounded channels; "the mpsc handoff" was never one thing, and the one this epic
worried about is the one that never parks.

**The scaling discount is a memory effect, not a work-volume effect.** Between the 1-core peak and
the 6-core peak:

| | S=1 N=2 | S=6 N=16 | delta |
|---|--:|--:|--:|
| instructions/row | 38,343 | 38,382 | **+0.1% (flat)** |
| cycles/row | 25,200 | 33,793 | **+34.1%** |
| IPC | 1.52 | 1.14 | **−25.4%** |

Closure: predicted 0.7237 (IPC 0.7465 × instr 0.9990 × util 0.9705) vs measured 0.7111 — a 1.26 pp
gap. The 28.9 pp shortfall splits **IPC decay 25.4 pp | residual idle 2.2 pp | extra instructions
0.1 pp | unexplained 1.3 pp**.

### The distinction this epic should not blur

**#3096 reduces per-row work.** `rows_to_record_batch` is 3.8–9.1% of on-CPU server weight across
the matrix (6.6–9.1% at the saturated N=8/N=16 points), `estimate_arrow_row_bytes` another
1.6–2.4% where it reaches the top-12, `alloc` 10.3–11.4%. Removing that work lowers
instructions/row and should raise **absolute** throughput **at every core count**. That much is
well supported.

**#3096 is not obviously a fix for the scaling discount.** The discount is 25.4 pp of IPC decay at
*flat* instructions/row. Cutting instructions/row raises the **level** of the whole curve; whether
it also **flattens** it depends on whether the encode path's allocations and copies are a principal
source of the memory-system pressure. If yes, #3096 improves level *and* slope. If the pressure
comes mostly from the decode/materialize path, #3096 raises the level and leaves the slope alone.
**#3217 cannot distinguish those two cases**, because the counters that would
(`LLC-loads`, `LLC-load-misses`, `cache-references`) read `<not supported>` on the virtualized
host. L1d (+7.5% rel) and dTLB (+40% rel) account for only ~10–13% of the +8,593 cycles/row;
~87% is unattributed.

So the ranking is deliberately not a single line:

1. **Establish the microarchitectural cause of the IPC decay** on a host with working LLC /
   memory-bandwidth counters. This is one afternoon of work on the two endpoint points, and its
   answer decides whether *any* per-row-work lever moves the slope. It is the highest-value next
   step and it is a measurement, not a fix.
2. **#3096 (Arrow encode)** — funded on absolute-throughput grounds, which the profile supports
   directly. Do not credit it with fixing the scaling discount until (1) says so.
3. **`do_get` handoff** — last. It optimises a site measured at zero.

### For projection-modelling purposes

Use the measured curve, not a single scalar: efficiency is **~0.87 at 2 cores, ~0.81 at 4, ~0.71 at
6**, i.e. it degrades roughly log-linearly rather than collapsing, and the peak-N moves outward
with core count (N=2 → 8 → 16 → 16). Per-box absolute at 6 cores: **1,076,917 rows/s = 746.6 MB/s
logical / 211.2 MB/s on-disk**, at 96.7% server utilisation with a 2-core client at 13.8% busy.

Full report, all artefacts, and the honest list of what is *not* established:
`docs/reports/ws0-3217-report.md`.
