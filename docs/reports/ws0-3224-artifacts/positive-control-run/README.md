# Positive control — PASS, on the second run. The first FAIL was the instrument, not the host.

`RESULT: PASS`, exit 0, `verdict.json` schema `ws0-3224.positive-control/v1`, 2026-08-04T01:1xZ,
`i4i.metal` / `ip-172-31-3-252`, pinned to CPU 8 (NUMA node 0), 3 reps per arm, events counted `:u`.

**Both runs are committed.** The FAIL is kept in `../positive-control-run-FAILED-instrument-bug/`
because the fact that this control can red a healthy box in a second distinct way is itself a
finding, and deleting the evidence would hide it.

## The passing verdict

| check | threshold | measured | |
|---|---|---|---|
| P1 hostility (cycles/access, hostile:friendly) | ≥ 5× | **29.209×** | PASS |
| P2 symmetry (instructions/access) | within ±10% | **1.000×** | PASS |
| P4 LLC miss rate rise (misses/loads) | ≥ 1.5× | **9.818×** (0.055 → 0.540) | PASS |
| P3 `LLC-loads` movement, either direction | ≥ 2× | 18 → 36,986,968 | OK |
| P5 `cache-references` movement, either direction | ≥ 2× | 46 → 36,987,457 | OK |
| `LLC-load-misses` movement | ≥ 2× | 1 → 20,000,591 | OK |
| `cache-misses` (advisory) | — | 8 → 20,000,851 | OK |
| gate integrity | ≤ 1e6 instr for 1000 accesses | 6,298 | PASS |

Hostile arm per-access: `LLC-loads` 1.849, `LLC-load-misses` 1.000, `cache-references` 1.849
(reported, never gated). Wall-clock 4.32 ns/access friendly vs ~127.7 ns/access hostile.

## Why run 1 FAILed, and why proceeding was correct rather than a workaround

Run 1 reported `RESULT: FAIL — 2 of 3 required counters unusable`, diagnosing `LLC-loads` and
`LLC-load-misses` as `ABSENT_EVENT_NAME`. That diagnosis was **false**, and it was falsifiable
against evidence committed *before* the control ever ran:

- `host/ac1-capability-probe.txt` (committed in `4b2bc33`, before this run) shows both events
  programming with real counts — `LLC-load-misses` 104, `LLC-loads` 1,352.
- Direct re-measurement: `perf stat -x, -e LLC-loads:u -- true` → `455,,LLC-loads,218387,100.00`.
  The counter programmed and returned a count.

**Root cause.** perf 6.17.13 echoes the event-name field (`-x,` field 3) *with* the `:u` modifier
for some events and *without* it for others:

| requested | field-3 name printed back | modifier |
|---|---|---|
| `cycles:u`, `instructions:u`, `cache-references:u`, `cache-misses:u` | same, with `:u` | retained |
| `LLC-loads:u` | `LLC-loads` | **stripped** |
| `LLC-load-misses:u` | `LLC-load-misses` | **stripped** |

The script matched only the requested form (`grep -q ",${ev}:u,"`), so for exactly the two LLC
counters this issue exists to read, the match failed and it concluded the event name was absent —
while field 1 held a valid count. The same bug sat in `cell()`/`mux()` on the measurement path, so
even a corrected probe would have yielded empty values and a second, differently-shaped false
failure downstream in the verdict math.

**This is the same failure class the pre-run review already caught once.** The first draft of this
control asserted raw `LLC-loads` must *rise*, which would have FAILed all three counters on a
healthy box; that was fixed before the run. This is a second, independent way the same script red a
good box — a string-matching bug rather than a direction bug. Two instances in one script argue the
lesson is structural: **a control must be validated against a known-good host, not only reasoned
about.** Cost here: one wasted 3-rep gate run (~4 min metered).

**Why this is not "working around a failed control".** The RUNBOOK's FAIL semantics are about *host
capability* — a counter the hardware cannot program. The decision rule that matters: the FAIL was
contradicted by a pre-registered, independently committed artefact (the AC1 probe), and the fix
changed **no threshold and no gate**. Had the AC1 probe agreed with the control, the correct action
would have been to stop and close the issue BLOCKED. It did not agree, so the instrument was at
fault. Fixing a demonstrably broken instrument-check and re-running is correct; proceeding with a
partial counter set — which the RUNBOOK explicitly forbids — is what was avoided.

**The gate's teeth are intact**, verified on this host after the fix:

| condition | probe result | diagnosis still fires? |
|---|---|---|
| bogus name (`not_a_real_event_xyz:u`) | no CSV row at all ("Bad event name") | yes → `ABSENT_EVENT_NAME` |
| unsupported event (`LLC-prefetches:u`) | `<not supported>` in field 1 | yes → `NOT_SUPPORTED` |
| working counter | real count | `PROGRAMS` |

`SILENT_ZERO` / `UNRELIABLE_*` key off values, which the fix is what makes readable at all. Base
event names in this script are distinct, so accepting the stripped form cannot alias one event onto
another. The `NOT_SUPPORTED` path is in fact now *reachable* for the two LLC events, where before it
was unreachable — they short-circuited to `ABSENT_EVENT_NAME` first.

## An AC5 warning this run produced for free

The advisory STREAM-triad reference, run unpinned across all 128 threads (both sockets), reported:

| run | @24 B/elem | @32 B/elem (incl. RFO) |
|---|--:|--:|
| 1 | 285.382 GB/s | 380.510 GB/s |
| 2 | 131.865 GB/s | 175.820 GB/s |

**2.16× apart on the same host minutes apart.** Neither number is usable as an achievable peak.
This is direct evidence for the RUNBOOK's requirement that the AC5 peak be re-measured pinned and
NUMA-bound exactly like the engine arms — an unpinned cross-socket triad is not a ceiling, it is a
contention artefact. Recorded here so the AC5 figure is never sourced from this advisory line.
