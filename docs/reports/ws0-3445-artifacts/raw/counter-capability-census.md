# Counter capability census — what this host can and CANNOT measure (issue #3445)

Taken BEFORE any measurement, because two of these answers change what AC1 and AC2 are
allowed to claim. Every row is an observation from a live `perf` invocation on
`ip-172-31-5-53` (Intel Xeon Platinum 8488C, kernel 6.17.0-1019-aws, perf 6.17.13), not a
reading of a vendor table.

## Preconditions (re-confirmed at lane start, per the standing instruction)

| precondition | required | observed | verdict |
|---|---|---|---|
| `perf_event_paranoid` | <= 1 for user profiling | **-1** | OK |
| `kptr_restrict` | 0 | **0** | OK |
| hardware `cycles` counter | must work | 1,864,993 over `sleep 0.05` | OK |
| Rust v0 demangling in `perf report` | must fire | `_RNvNt…` rendered as `cqlite_core::…::parse_row_data_with_offset_impl` | **OBSERVED** |

The demangler was verified on THIS binary rather than inferred from #3248's probe: perf
6.17.13 reports `libbfd: OFF`, from which the natural inference is that Rust symbols will
not demangle, and that inference is wrong (perf carries its own v0 demangler). Confirming
it on the actual measured binary costs one command and removes the #3217 failure mode.

## Events

| event | status | consequence |
|---|---|---|
| `cycles` | available | AC1's sampling event |
| `instructions` | available | IPC for the reconciliation |
| `cycles:pp` (PEBS precise) | **`<not supported>`** | **AC1 attribution is NON-PRECISE — see below** |
| `instructions:pp`, `instructions:ppp` | **`<not supported>`** | no precise alternative |
| `mem_inst_retired.all_loads:pp` | **`<not supported>`** | no precise memory attribution |
| `cycle_activity.stalls_total` | available, and **samplable** | AC2's primary stall signal |
| `cycle_activity.stalls_l1d_miss` | available | AC2 memory-stall split |
| `idq_uops_not_delivered.core` | available | AC2 frontend signal |
| `int_misc.recovery_cycles` | available | AC2 mis-speculation signal |
| `exe_activity.bound_on_stores` | available | AC2 backend corroboration |
| `topdown.slots` | **`<not supported>`** | **no Topdown L1 frontend/backend split** |
| `perf stat -M TopdownL1` | **unavailable** (`Unable to find PMU or event`) | ditto |
| `cycle_activity.stalls_mem_any`, `resource_stalls.any` | not in this kernel's event map | — |

Six events count concurrently with no multiplexing, so AC2's counter set fits in the
available general-purpose counters and can hold `pct_running` at 100.00%. That is asserted
from the `-x,` field in every rep, never inferred from the absence of a warning.

## The two capability gaps that bound this issue's claims

**1. No PEBS.** `cycles:pp` is unsupported (this is a virtualised guest; PEBS is not exposed).
Sample IPs are therefore subject to skid: the recorded instruction pointer can sit a few
instructions past the one that actually consumed the cycle. A VInt decode region is ~8-20
instructions, so skid is a first-order concern for AC1 rather than a footnote. It is handled
by measurement, not by assertion: `harness/vint_share.py` recomputes the share with every
sample re-attributed K instructions earlier for K in 0..3 and reports the spread as a band.

**2. No Topdown slots.** The canonical frontend-bound / backend-bound / bad-speculation /
retiring decomposition is unavailable on this host, so AC2 **cannot** be answered in
Topdown's vocabulary. It is answered instead with the `cycle_activity.*` stall family, which
IS available and IS samplable — meaning stalls can be attributed to the inline chain by the
same mechanism as cycles. Where that family cannot resolve a question, AC2 reports the
result as **unmeasurable**, which the issue explicitly permits, and never as zero.
