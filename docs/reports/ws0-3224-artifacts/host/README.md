# Host capability + topology facts for the #3224 metered run

Captured 2026-08-04T01:04Z on `i4i.metal` (`ip-172-31-3-252`), fleet index 4.

| | |
|---|---|
| CPU | Intel Xeon Platinum 8375C @ 2.90GHz (Ice Lake-SP) |
| Sockets / physical / logical | 2 / 64 / 128 |
| NUMA nodes | 2 — **node0 = CPUs `0-31,64-95`**, node1 = CPUs `32-63,96-127` |
| RAM | 1007 GiB |
| Kernel / perf | 6.17.0-1019-aws / perf 6.17.13 |
| Scratch | `/data` on instance-store NVMe (`/dev/nvme3n1`, 295 G, 279 G free) |

## AC1 verdict: CAPABLE

`ac1-capability-probe.txt` holds the raw probe. The three counters that read
`<not supported>` on #3217's virtualized host all program here with real counts:

| event | count on `true` |
|---|--:|
| `LLC-load-misses` | 104 (7.69% of LL-cache accesses) |
| `LLC-loads` | 1,352 |
| `cache-references` | 14,451 |
| `cycles` | 620,942 |
| `instructions` | 689,021 (IPC 1.11) |

`ls /sys/bus/event_source/devices/` (the authoritative test, never `perf list`) shows
**88 uncore devices**: `uncore_imc_0..11` + `uncore_imc_free_running_0..3`,
`uncore_cha_0..35`, `uncore_m2m_*`, `uncore_upi_*`.

## Three facts that change the procedure

### 1. SMT siblings are `(c, c+64)` — #3217's harness hardcodes `(c, c+8)`

From `thread-siblings.txt`, read from sysfs and never assumed:

```
cpu0 0,64      cpu1 1,65      cpu2 2,66      ...
```

`ws0-3217-artifacts/harness/common.sh`'s `ws0_server_cpus_for_s()` encodes its own host's
16-logical-CPU `(c, c+8)` pairing (`2,10` / `0,2,8,10` / `0-3,8-11` / `0-5,8-13`, client
`6,7,14,15`). **Every one of those sets is wrong on this box** and `selftest.sh` cannot catch it —
it verifies the topology *derivation*, not the hardcoded table. Rewritten in step 3.

### 2. `perf stat -M MemoryBandwidth` is NOT available on this perf build

```
Cannot find metric or group `MemoryBandwidth'
```

This is a recorded capability fact, not a gap in the run. AC3 requires a memory-bandwidth figure
from whichever source the host supports, **named explicitly** — so the published figure comes from
`uncore_imc_*/cas_count_{read,write}/`, and the report names that source. The `-M MemoryBandwidth`
cross-check the RUNBOOK offers as an option is unavailable and is recorded as such rather than
silently omitted.

### 3. The per-socket split comes from `--per-socket`, not from device indices

All 12 `uncore_imc_N` devices carry `cpumask=0,32` — CPU 0 is socket 0's proxy, CPU 32 is socket
1's. So `uncore_imc_0..11` are **not** 12 per-socket instances; each device counts on both sockets
and `perf stat -a` sums them. The RUNBOOK's per-socket split therefore requires `--per-socket`:

```
S0,1,0.85,MiB,uncore_imc_0/cas_count_read/,1000921300,100.00,,
S1,1,1.00,MiB,uncore_imc_0/cas_count_read/,1000940961,100.00,,
```

Note the field layout shifts by two leading fields (`S<n>`, cpu-count) versus plain `-x,`, so the
enabled-percentage column the RUNBOOK calls "field 5" is **field 7** in the `--per-socket` form.
Any parser must account for this. Also note perf reports `cas_count_*` already scaled to MiB —
the ×64 B/cacheline conversion the RUNBOOK specifies is applied by perf, so multiplying the MiB
figure by 64 again would overcount by 64×.

## Sysctl state

`kernel.perf_event_paranoid` was found at **4** on this fresh box by the lead at launch and set to
`-1` with `kptr_restrict=0` before this session began. Confirmed `-1` / `0` at 01:04Z. **This is not
in the golden AMI and does not survive a reboot** (#3249's fix is absent here) — re-asserted before
every capture via `ws0_assert_sysctl`, per RUNBOOK step 2.
