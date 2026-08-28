# AC0 finding — the #3096 record names a machine SPECIFICATION, never a machine IDENTITY

**Question asked (coordination lead, 2026-08-28):** which box were the #3096 figures measured on? If
not this box, AC0 is not a reproduction but a cross-machine comparison, which under the inherited
"no absolute is reusable cross-session" rule is strictly worse.

**Answer: the record cannot tell you. That is a gap in the recorded method, and it is reported rather
than papered over.**

## What IS recorded

`docs/reports/ws0-3096-artifacts/baseline-2026-08-03.md:70` and `measurement-method.md:133`:

| recorded | value |
|---|---|
| box | Intel Xeon Platinum 8488C, 16 logical / 8 physical (1 socket, 2 threads/core), 30 GiB |
| server pinning | `taskset -c 2,10` — **verified** siblings of one physical core |
| client pinning | `taskset -c 4,12,5,13,6,14,7,15` (disjoint from the server set) |
| counters | `perf stat -x, -e cycles,instructions -C 2,10` — CPU-wide, no `-p` |
| `kernel.perf_event_paranoid` | `-1` |
| build | `--release` |
| metrics | off |
| reps | 3 per (arm, temperature); median reported |

## What is NOT recorded — anywhere

No hostname, no instance id, no `machine-id`, no `boot_id`, no `uname`, no MAC, no microcode revision.

Enumerated key sets of the two JSON artifacts, searching for any identity-bearing field:

* `baseline-results.json` → `client_cpus`, `corpus_identity`, `server_cpus`
* `abc-interleaved-runs.json` → `client_cpus`, `id`, `server_cpus`, `supporting_evidence`

`id` is a measurement id, not a host id. **Nothing in either artifact identifies a machine.**

**The delivery ledger cannot answer it either, and this is the broader gap.** The
`delivery-telemetry.schema.json` has 23 properties and **not one of them names a machine or host**.
So the fleet's own delivery record cannot tie *any* delivery — perf or otherwise — to the box that
produced it. The #3096 record (`docs/reports/delivery-telemetry.jsonl`) carries `gate: pass`,
`gate_runs: 8`, `routing: design`, and no host.

Git does not discriminate either: every agent commit on this repository carries the owner's identity
(`Patrick McFadin <pmcfadin@gmail.com>`), so authorship cannot separate machines.

## What CAN be established

This box matches the recorded specification **exactly**, on every recorded axis:

| axis | #3096 record | this box (`ip-172-31-7-163`) | match |
|---|---|---|---|
| CPU model | Intel Xeon Platinum 8488C | Intel(R) Xeon(R) Platinum 8488C | **yes** |
| topology | 16 logical / 8 physical, 1 socket, 2 threads/core | 16 / 8, 1 socket, 2 threads/core | **yes** |
| memory | 30 GiB | 30 GiB | **yes** |
| `perf_event_paranoid` | `-1` | `-1` | **yes** |
| sibling pair `2,10` | "verified siblings of one physical core" | `cpu2`/`cpu10` both `core_id=2` | **yes** |

So the #3096 pinning is reproducible here as recorded, and no recorded axis diverges.

## The consequence for AC0, stated precisely

AC0 is **a reproduction on an identically-specified box whose identity is unrecorded.** It is
provably not a cross-*specification* comparison. Whether it is cross-*session* (same instance,
strongest case) or cross-*instance* (weaker) is **undecidable from the record**, and no amount of
care by this issue can recover it after the fact.

**Why that matters for reading a divergence.** AC0 is explicitly licensed to report a divergence as a
finding. But the record's silence means a divergence cannot be cleanly attributed between:

1. genuine drift or regression on the measured path, and
2. instance-to-instance variation between two identically-specified c7i.4xlarge instances.

That is a **reduction in AC0's discriminating power**, and it is a limitation of the **inherited
record**, not of this work. It is stated here so that a divergence is not over-read, and equally so
that an *agreement* is not over-claimed: agreement across two unknown-but-identically-specified
instances is weaker evidence than agreement across two sessions on one instance, and the record
cannot tell us which we got.

## The cheap fix, applied here so #3248 does not pass the gap on

This issue's own artifacts record machine identity, so a future reproduction of *these* numbers does
not face the same question:

| field | value |
|---|---|
| hostname | `ip-172-31-7-163` |
| EC2 instance id | `i-04ac0a860eef7f241` |
| instance type | `c7i.4xlarge` |
| `/etc/machine-id` | `ec2a413b505f9f2e0ece99141d073956` |
| `boot_id` | `bb94898a-7792-44e1-8f38-cdafea462803` |
| kernel | `6.17.0-1019-aws` |
| CPU microcode | `0x2b000661` |
| SMT | `on` |

`boot_id` is included deliberately: it distinguishes two sessions on one instance from one session,
which is exactly the axis the #3096 record lost. Microcode is included because it can move
frequency/IPC behaviour under an unchanged kernel and CPU model.

**Recommended follow-up (not actioned by this issue):** add a host-identity block to the
delivery-telemetry schema, or at minimum require perf-issue artifacts to record it. Filing this is
proposed rather than done, because the telemetry schema is not this issue's scope.
