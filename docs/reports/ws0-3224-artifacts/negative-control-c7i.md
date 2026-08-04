# Negative control — a virtualized `c7i.4xlarge` cannot answer #3224

**This is NOT AC1's artefact.** AC1's probe belongs to the *target* host (`i4i.metal`) and is
committed separately. This file is a **negative control**: evidence that substituting a virtualized
instance is barred by a **capability fact**, not by a preference. Recorded so that a future run,
under time pressure, does not re-derive it — or worse, decide to "characterize the gap in prose"
and ship #3217's outcome a second time.

| | |
|---|---|
| Host | `c7i.4xlarge`, `us-west-2` |
| CPU | Intel Xeon Platinum 8488C (Sapphire Rapids), 8 physical / 16 threads |
| Virtualization | `Hypervisor vendor: KVM`, full |
| Sysctls at probe time | `kernel.perf_event_paranoid=-1`, `kernel.kptr_restrict=0` |
| Probed | 2026-08-04, before #3224's metal box existed |

## The probe, verbatim

```
perf stat -e LLC-load-misses,LLC-loads,cache-references,cycles,instructions true
   <not supported>      LLC-load-misses
   <not supported>      LLC-loads
                 0      cache-references
            881469      cycles
            714067      instructions   # 0.81 insn per cycle
```

```
/sys/bus/event_source/devices/  =  breakpoint cpu kprobe msr software tracepoint uprobe
perf list                        =  knows only L1-dcache-* and L1-icache-*; no LLC events
perf stat -M MemoryBandwidth     =  Cannot find metric or group
```

Raw capture: [`negative-control-c7i-probe.txt`](negative-control-c7i-probe.txt).

## Three findings, each worth more than the probe itself

### 1. There is no `uncore_*` PMU in the guest AT ALL — AC5 is unreachable, not degraded

`/sys/bus/event_source/devices/` lists seven entries and **not one of them is an uncore device**.
The `uncore_imc/*` counters AC3 names as the memory-bandwidth source are not "unsupported"; they
are **absent from the guest at the sysfs layer**. `perf stat -M MemoryBandwidth` therefore cannot
even resolve a metric group.

Contrast with the #3224 target host, `i4i.metal` (Xeon 8375C, Ice Lake-SP, 2 sockets):
**88 uncore devices present** — `uncore_imc_0..11`, `uncore_cha_0..35`, `uncore_m2m_*`,
`uncore_upi_*`.

The consequence is categorical: **AC5's saturation verdict — measured DRAM bandwidth against the
host's achievable peak — cannot be produced on ANY virtualized instance**, at any budget, with any
amount of care in the harness. That upgrades the substitution ban from a methodological preference
to a capability fact. It is also why "run it on the cheaper box and caveat the result" is not a
lesser version of this study; it is a different study that cannot answer the question.

**Beware one misleading check**: `perf list | grep -ci uncore` returns **15** on this box. Those are
per-CPU-model JSON event-table entries shipped with `perf`, not PMUs present on the host.
`ls /sys/bus/event_source/devices/` is the authoritative test. A capability probe that greps
`perf list` will report uncore support on a machine that has none.

### 2. `cache-references` programs cleanly and returns a hard `0` — the silent instrument, in the
very first probe of the very issue commissioned because of it

It does not say `<not supported>`. It does not error. It programs, and it returns zero cache
references for a process that demonstrably executed 714,067 instructions and touched memory. That
is physically impossible, so it is an unimplemented counter reporting a measurement-shaped value.

**A positive control catches this. Prose characterization does not.** That single sentence is the
reason `positive-control.sh` exists and the reason owner condition 3 is binding: #3217 spent a full
effort on a host that answered this way, and every number it derived from `cache-references` would
have been a confident, wrong report. The failure mode is not "the tool errored"; it is "the tool
succeeded and lied", and the only defence is a differential against a workload whose behaviour is
known in advance.

### 3. `perf_event_paranoid=-1` was ALREADY in place — this is not a permission wall

Both sysctls were at their permissive values when the probe ran. **#3249's fix is confirmed working
and simultaneously confirmed insufficient**: it removes the permission blocker and leaves the
capability blocker completely untouched. Two independent facts from one measurement, and neither is
inferable from the other — which is exactly why the hypothesis that #3217's `<not supported>` was a
misdiagnosed permission wall had to be tested rather than argued. It was tested. It is false.

## Carried into the RUNBOOK

On the **fresh `i4i.metal`**, `kernel.perf_event_paranoid` was found at **4**. #3249's fix is
**not baked into the golden AMI and does not survive a reboot**. Re-apply and re-verify it before
trusting any capture — and note that a stale value produces *unsymbolized frames and EACCES*, i.e.
a failure that looks like a different problem. See `RUNBOOK.md` step 2.

## Provenance

Probe run by the flow-lead worker on `ip-172-31-5-109` while #3224 was blocked on hardware, and
posted to issue #3224 (`REQ-3224-20260804T003227Z`). The owner explicitly approved committing it as
a negative control. The `i4i.metal` uncore-device counts quoted above come from the owner's
provisioning report on the same issue; they were not produced by this probe.
