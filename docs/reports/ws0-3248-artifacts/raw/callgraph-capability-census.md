# Call-graph capability census — all three mechanisms characterized, and two are unavailable

AC1 needs per-function attribution; establishing *region membership* (which functions sit inside the
encode region) wants a call graph. `perf` offers exactly three unwinding mechanisms. On this host:

| mechanism | status on this box | evidence |
|---|---|---|
| `--call-graph dwarf` | **HANGS** past 120 s | committed in-tree trap, `docs/reports/ws0-3217-artifacts/harness/profile-oncpu.sh:8-15`: "Against this ~143 MB binary dwarf unwinding HANGS past 120s" |
| `--call-graph lbr` | **UNAVAILABLE** | measured here: `perf record --call-graph lbr` exits 255 with `cycles:PH: PMU Hardware or event type doesn't support branch stack sampling.` This is a KVM guest (`Hypervisor vendor: KVM`) and LBR is not virtualized |
| `--call-graph fp` (frame pointers) | **works**, but requires `-C force-frame-pointers=yes`, which **alters codegen** | the committed recipe, `ws0-3217-artifacts/harness/README.md:163-164` |

## The consequence, which is a constraint and not a preference

**Any call-graph evidence in this work necessarily comes from a codegen-perturbed binary.** That is
not a methodological choice made for convenience — it is the only mechanism that functions on this
host. Stated explicitly because the alternative reading ("they could have used dwarf and chose not
to") would misrepresent the evidence's standing.

This is exactly why the reporting split the coordination lead approved is structured as it is:

* **headline per-function figures** come from `perfsym` (symbols only, no debuginfo, no frame
  pointers) — the codegen-faithful vehicle, `.text` within 0.02% of `release`;
* **call-graph / region-membership evidence** comes from a frame-pointer build, reported separately
  and labelled **perturbed**, never summed with or substituted for the headline.

**AC1 therefore has a floor on its granularity set by CODEGEN, not by effort** — and now a second
floor on its *structural* evidence set by host virtualization.

## This is the THIRD capability this virtualized host has cost the WS0 programme

The issue body names itself as "the **second** issue paying for [#3224's] absence". With this census
it is the third distinct instance of the same underlying cause:

1. **#3217** — LLC / memory-bandwidth counters unavailable on the virtualized host, leaving ~87% of
   the IPC decay unattributed.
2. **#3096** — 82% of per-row encode left in one undifferentiated complement.
3. **#3248 (here)** — **LBR branch-stack sampling unavailable**, so the one call-graph mechanism that
   would need no codegen change is off the table, and dwarf (the other codegen-neutral option) hangs.

The pattern is not three coincidences. It is one cause: **we are profiling on hardware whose
performance-monitoring surface is partly virtualized away**, and each issue rediscovers a different
missing piece of it. #3224 is where that capability lives.

## What was NOT verified

The dwarf hang is cited from the in-tree trap rather than re-measured here, on the #3217 harness's
~143 MB binary. `target/perfprof/cqlite-flight` is 75 MB, so a hang is plausible but not established
for *this* binary. It was not re-tested because the finding does not depend on it: LBR's
unavailability alone forces the frame-pointer route, and burning a long run to confirm a documented
trap would buy nothing.
