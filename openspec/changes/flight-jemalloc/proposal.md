# flight-jemalloc — issue #3997

**Milestone:** 0.17 (read-perf headline). **Routing:** design-driven (OpenSpec) — it adds a
dependency and changes the memory-behaviour profile of a shipped binary, which #3217 partC F1 ruled
is a product decision, not a tuning knob.

## Why

glibc `malloc` is the single largest **measured** cost on the served path, and it is the cheapest
lever left in the 0.17 program. #3551 (`docs/reports/ws0-3551-report.md`, 31 clean within-round pairs
across 3 interleaved sets, worst pair-control 2.41%) measured a 2×2 of server **pin** × **allocator**
on the canonical WS0 corpus, one binary across every arm:

| arm | pin | allocator | Δ rows/s vs A | Δ cycles/row | IPC |
|---|---|---|--:|--:|--:|
| A control | `2,10` (1 phys core) | glibc | — | — | 1.46 |
| **D** | `2,10` | **jemalloc** | **+29.21%** | **−21.71%** | 1.56 |
| C | `2,3` (2 phys cores) | jemalloc | +61.17% | −42.37% | 2.12 |
| B | `2,3` | glibc | −19.25% | +17.85% | 1.39 |
| C0 | `2,3` | glibc + `MALLOC_ARENA_MAX=2` | −22.71% | +22.11% | 1.36 |

Arm D is the deliverable: **+29% rows/s per physical core with no pin change**, at the pinning the
rig has always used. On that rig it moves the served path from 266k rows/s to ~344k against a bare
scan of 352k — i.e. it closes almost the whole remaining served-vs-bare gap that is the 0.17
throughput program's stated goal (#3023: "within ~1.3× of bare scan").

The mechanism is a sign-flipping interaction — the same pin change is −19% under glibc and +25% under
jemalloc — so glibc's malloc lock is what serialises the second core. That also supplies a mechanism
for #3248's unexplained +49% allocator term under Flight. The arena-cap alternative (#3228 stage 1)
was run as arm C0 and falsified in the opposite direction; #3228 is closed on that evidence.

## What changes

`cqlite-flight`'s **binary** links jemalloc as its `#[global_allocator]` (`tikv-jemallocator`),
behind a Cargo feature `jemalloc` that is **on by default for the binary target on Linux** once the
linked-build measurement in this change clears its kill criterion. The installation lives in
`cqlite-flight/src/main.rs` **only** — the library target, every test binary, every example and every
other workspace crate keep the system allocator. The binary reports the allocator it was built with
at startup and in `--version`.

Nothing in `cqlite-core`, the CLI, or the Python/Node bindings changes. No pin/topology change is
made (arm B shows the pin alone is harmful; arm C's extra gain needs a deployment design of its own).

## What this change must establish that #3551 did not

1. **Linked allocator, not `LD_PRELOAD`.** Every #3551 arm preloaded `libjemalloc.so` into the same
   binary. The shipped form is linked. The A/B is re-run in the linked form, same rig, same method,
   same pin, before any number is claimed for the shipped binary.
2. **Memory behaviour.** RSS peak (`VmHWM`) and steady-state RSS per arm, plus the existing Flight
   producer dhat budget (`issue_1494_producer_mem_budget`), against the <128 MB target. A throughput
   win that breaks the memory target does not ship.
3. **Scope of imposition.** The allocator is imposed on the `cqlite-flight` **process**, never on
   library consumers (`flight-loadgen` and the crate's own tests link `cqlite-flight` as a library).

## Kill criterion (pre-registered)

Linked-build arm **E** (jemalloc linked, pin `2,10`) vs arm A (glibc, same binary minus the feature):

- **SHIP as default** if median paired Δrows/s ≥ **+15%** (≥3 clean pairs, every pair up, worst
  pair-control < 3%) **and** RSS peak ≤ **1.10×** arm A **and** the dhat producer budget still passes.
- **SHIP as opt-in (non-default)** if +3% ≤ Δ < +15%, or RSS peak in (1.10×, 1.25×].
- **Do NOT ship; record the null** if Δ < +3% or RSS peak > 1.25× — the feature is removed, the
  report is committed, and #3997 closes as an honest negative (#3248 AC6). A null here is a result.

## Non-goals

- Changing server CPU pinning or admission (`--max-concurrent-scans`) — separable, harmful alone.
- Any allocator change in `cqlite-core`, `cqlite-cli`, `bindings/python`, `bindings/node`, or the
  library target of `cqlite-flight`. Embedders own their allocator.
- mimalloc, snmalloc, or any arena/`Value<'arena>` refactor (#3028, #2624 — the 0.18 road).
- `MALLOC_ARENA_MAX` at 1/4/default — moot once glibc is not the production allocator.
- Reducing allocation *count* or *bytes/row* (#3028). This change cuts allocator *cost* only.
- macOS/Windows behaviour: the feature is inert off-Linux; production is Linux.

## Impact statements (openspec rules)

- **No-heuristics mandate:** untouched — no decode path changes.
- **Public binding surfaces:** untouched by construction (bindings do not depend on `cqlite-flight`).
- **<128 MB memory budget:** the subject of requirement R3; measured, not assumed.
- **Gate:** one new structural lint (allocator confined to `main.rs`), one new startup-surface test,
  the existing `memory-budget` component (`cqlite-flight --features dhat-heap`) must still run.
