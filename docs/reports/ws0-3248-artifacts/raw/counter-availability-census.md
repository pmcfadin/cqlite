# Counter availability census on this host — and a SILENT-ZERO class the rig does not catch

Taken to establish whether the **bytes-touched-per-row differential** (the owner's added deliverable,
and the baseline #3288 "cannot start without") is measurable here at all. It partly is, and the census
turned up a defect class worth more than the differential.

## The census

| event | result | usable? |
|---|---|---|
| `l2_rqsts.all_demand_data_rd` | 1,382,172 | **yes** |
| `l2_rqsts.demand_data_rd_hit` | 386,694 | **yes** |
| `l2_rqsts.demand_data_rd_miss` | 78,966 | **yes** |
| `l2_rqsts.all_demand_miss` | 5,538,782 | **yes** |
| `l2_lines_in.all` | 7,281,136 | **yes** |
| `l2_lines_out.non_silent` | 347,459 | **yes** |
| `mem_inst_retired.all_loads` | 154,108,271 | **yes** |
| `mem_inst_retired.all_stores` | 97,417,339 | **yes** |
| `mem_load_retired.l1_hit` | 154,988,915 | **yes** |
| `mem_load_retired.l2_hit` | 1,900,599 | **yes** |
| `mem_load_retired.l3_hit` | **0** | **NO — silent zero** |
| `mem_load_retired.l3_miss` | **0** | **NO — silent zero** |
| `cache-references` | **0** | **NO — silent zero** |
| `cache-misses` | **0** | **NO — silent zero** |
| `longest_lat_cache.miss` | **0** | **NO — silent zero** |
| `LLC-loads` | `<not supported>` | no (honest refusal) |
| `LLC-load-misses` | `<not supported>` | no (honest refusal) |
| `mem-loads`, `mem-stores`, `offcore_response.*` | event syntax error | no (honest refusal) |

**Everything at L1/L2 works. Everything at L3/LLC is unavailable.** That independently confirms
#3224's premise — the reason it is blocked on a host with working LLC counters — and makes this the
**fourth** distinct capability this virtualized host has cost the WS0 programme (after #3217's
LLC/uncore counters, #3096's unattributed encode, and this issue's unavailable LBR).

## The defect class, which is the more valuable half

**Five events PROGRAM SUCCESSFULLY, report `100.00%` enabled, and return a hard `0`.**

That is materially worse than `<not supported>`, and the difference is the whole point:

* `<not supported>` is an **honest refusal**. The rig's `read_perf_counters` already treats it as
  fatal (`PERF_NOT_A_VALUE`), so a run using such an event fails loudly.
* A **hard `0` at 100% enabled passes every check the rig has.** It is a non-negative integer, it is
  not a perf marker, it is not fractional, and it is not multiplexed — so it satisfies
  `non_negative_int`, the `PERF_NOT_A_VALUE` screen, **and the multiplexing guard added by this very
  issue**. An agent measuring `cache-misses` on this box would report **zero cache misses** and every
  figure derived from it — misses/row, hit rate, bytes-touched — would be a confident wrong number
  with nothing anywhere saying so.

**This is the silent-instrument class in its purest form**, and this issue exists because of that
class, so it is recorded rather than worked around:

> A counter that returns `0` is indistinguishable, to every guard in this rig, from a counter that
> genuinely observed zero events.

**Why the rig cannot simply refuse zeros.** A legitimate zero exists: `requests_error == 0` is the
expected, required value on a healthy rep. So "refuse all zeros" would break correct runs. The
distinction needs a **per-event expectation** — "this event, on this workload, cannot legitimately be
zero" — which is knowledge the rig does not currently hold.

**Recommended fix, proposed and NOT actioned here** (it is a rig-wide contract change, and this issue
should not smuggle one in under a profiling deliverable): a declared set of events that must be
**non-zero** when selected, checked in `read_perf_counters` beside the multiplexing check, with the
diagnostic naming the host-capability cause rather than the artifact. The natural seed set is the
five events above. Filing this as a follow-up.

## What the differential can therefore measure

Not LLC-boundary traffic. What **is** available is the **L2 boundary**, which is a real and stated
answer rather than a proxy for one:

* `l2_lines_in.all × 64 B` — bytes brought **into L2** from L3/memory: the closest available measure
  of "bytes touched beyond the core's private caches".
* `mem_inst_retired.all_loads` / `all_stores` — retired memory **access counts** (architectural, not
  bytes, and inflated by L1 hits).

So the bytes-touched differential is reported at the **L2 boundary, with the LLC boundary explicitly
unavailable** — and #3288's "fit ~1/6 of 54 MiB LLC" target, which is a constraint at the LLC
boundary, **cannot be checked on this host**. That is a finding for #3288 rather than a gap in this
work: the measurement it needs is not blocked on effort here, it is blocked on hardware, which is
exactly what #3224 is for.
