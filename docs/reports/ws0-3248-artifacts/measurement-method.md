# Issue #3248 — measurement method

This issue **buys a profile, not a patch**. Its output is an attribution and a differential, and the
only thing that makes either worth reading is whether the instruments were verified before they were
believed. So this document records what was **observed**, and marks plainly what is **assumed**.

Companion: `docs/reports/ws0-3096-artifacts/measurement-method.md` (the rig this work consumes),
`scripts/perf/README.md` (the rig's own contract), and `docs/reports/ws0-3299-report.md` §6 (the drift
section this work leans on).

---

## 0. The epistemic rule this document follows

Two consecutive perf issues (#3217, #3096) ended with a large **unattributed majority bucket**, and in
both cases the instrument reported success. #3217 lost a 50.57 s → 2.89 s bucket to a **missing
demangler**, silently. #3096 left 82% of per-row encode in one undifferentiated complement, computed
as `1,746 − 313 = 1,432.9 ns/row` and *labelled* "array build" from the call graph with **zero
per-function data inside it**.

The lesson both taught is the same: **an instrument that cannot measure a thing does not say so — it
returns a confident number about something else.** Therefore every capability this work depends on is
demonstrated firing, on this box, before any measurement is taken. A capability check that was
inferred from a version string or a build-options table is treated as **not done**.

---

## 1. Capability preconditions, each OBSERVED on this box

### 1.1 Rust symbol demangling — the #3217 failure mode

**Verdict: available. Established by observation, and the inference from documentation would have been
WRONG.**

`perf 6.17.13` on this box reports `libbfd: [ OFF ]` in `perf version --build-options`. Reading that
table, the natural conclusion is that Rust symbols will not demangle — binutils' demangler is absent.
That conclusion is false: perf carries its own Rust-v0 demangler independent of libbfd.

Observed, on a purpose-built v0-mangled binary:

| raw symbol in `nm` | rendered by `perf report` |
|---|---|
| `_RNvCslHeE2yMtrtp_7v0probe17spin_marker_alpha` | `v0probe::spin_marker_alpha` |

Also established, because it decides *which* demangler matters: **rustc 1.97.1 emits v0 mangling by
default.** The probe was compiled with no `-C symbol-mangling-version` flag and produced a `_R`-prefixed
symbol. So v0 is the mangling of the binaries actually under measurement, and v0 is the demangler
verified.

Raw evidence and the probe source: `raw/demangler-probe.md`, `raw/demangler-probe.rs`.

**Why this is recorded at length for a check that passed:** the check passing is not the point. The
point is that the documented signal (`libbfd: OFF`) pointed the other way, so an agent who reasoned
from the build-options table instead of running the probe would have concluded the box could not do
per-function attribution — and either abandoned AC1 or, worse, worked around a problem it does not have.

### 1.2 Symbols in the binary under measurement — a blocker the issue does not mention

**Verdict: `[profile.release]` cannot be profiled per-function. A new profile was added.**

The workspace release profile sets:

```toml
[profile.release]
codegen-units = 1
lto = true
panic = "abort"
strip = true      # <-- this one
```

`strip = true` discards the symbol table. Against a stock release binary, `perf record` exits 0 and
`perf report` prints a well-formed table of raw addresses — `[.] 0x00000000000224eb` — which is
**exactly the silent-instrument shape** §0 describes. This is a plausible partial explanation for why
the encode region survived #3217 and #3096 unattributed: the default artifact on the measured path is
not attributable, and nothing about running the profiler says so.

Added by this change:

```toml
[profile.perfprof]
inherits = "release"
debug = 1
strip = "none"
```

Two properties, both deliberate:

* **Codegen fidelity.** It moves only debuginfo emission and symbol retention. Every optimization
  input — `opt-level`, `lto`, `codegen-units`, `panic` — is inherited from `release` untouched.
  `-C force-frame-pointers` is deliberately **not** set, because that *would* alter codegen; call-graph
  runs instead use `perf record --call-graph dwarf` and are reported as **structural evidence only**,
  never as the source of a headline per-function number.
* **No collision with the rig's provenance.** `--profile bench` also unstrips (it inherits release and
  sets `debug = true`, `strip = "none"`), but cargo writes it to `target/release/`, which would clobber
  the binaries the WS0 rig digests in `scripts/perf/ws0_binary_snapshot.py`. A named profile gets
  `target/perfprof/` and cannot collide.

**Codegen equivalence is MEASURED, not asserted** — see §4. Asserting it is the confirmation trap AC0
exists to warn against, and "debuginfo does not change codegen" is a belief about the compiler, not an
observation about these two binaries.

---

## 2. The corpus — re-hashed, not read

AC0 consumes the #3096 4M-row corpus. Its identity file *claims* the pin the issue names. A recorded
claim about a corpus is not the corpus, so every component was re-hashed from the bytes on disk:

* **8/8 components match** the recorded identity, field for field.
* `nb-1-big-Data.db` = `4a903f6fa27c04dbf87a44fddf78615aed73fcd379ecaee6669f6b0d9bbae269`, which is the
  `4a903f6f…ae269` pin named in the issue.

Shape, as the inputs to every per-row figure downstream: **4,000,000 rows**, 40,000 partitions,
100 rows/partition, **12 cells/row**, 2,774,760,422 Data.db bytes, **693.69 bytes/row**, no
`CompressionInfo.db`.

Schema (`ws0.events`, 12 columns) splits **6 var-len / 6 fixed-width**:

| var-len (owned `String`/`Vec` materialization) | fixed-width |
|---|---|
| `part_id` text, `blob_a` blob, `blob_b` blob, `payload` text, `region` text, `status` text | `seq` int, `event_time` timestamp, `device_id` uuid, `metric_a` int, `metric_b` bigint, `metric_c` double |

That 6 is the quantity **prediction P2** (allocations/row ≈ var-len column count) is tested against.

Detail: `raw/corpus-verification.md`.

---

## 3. Box quiescence — measured, because procedural quiescence has already failed once

The rig's own README states the governing constraint: **"this rig produces no reusable absolute"**,
after the untouched warm bare scan read 370,134 rows/s and, an hour later, 333,206 rows/s — ~10% drift
with nothing changed on the measured path.

This box is **shared between delivery lanes**, which the rig does not model. Observed while preparing
this work: `load1` reached **108** on 16 vCPUs with ~17 concurrent `rustc`, from a peer lane's full
agent-gate. A measurement taken into that is worthless, and — the actual hazard — **nothing in the
recorded artifact would say so afterwards.**

There is a measured positive control for the mechanism, from #3299 at an identical `S=1/N=1` point:

| condition | frequency | IPC |
|---|--:|--:|
| co-scheduled (an accidental second sweep) | **2.470 GHz** | 1.61 |
| quiescent | **3.268–3.291 GHz** | 1.42 |

A **25% frequency reduction from co-scheduled load alone**, with only 2 logical CPUs pinned.

So every rep in this work carries an **external load timeseries**, recorded independently of the rig:
`.ws0-3248/box-load.jsonl`, one JSON line per 10 s carrying `load1/5/15`, the kernel runnable count,
and a process census (`rustc`, `cargo`, `perf`, `agent-gate`, `cqlite-flight`, `flight-loadgen`).

Two implementation notes, both defects found and fixed in the sampler itself:

* Counts come from `/proc/*/comm` and `/proc/*/cmdline`, **not `pgrep -f`**: a `-f` pattern matches the
  census command's *own* cmdline and inflates the very count it is measuring.
* `pgrep -x`/`pkill -x` cannot be used as the alternative: the kernel `comm` field is capped at 15
  characters, so a longer binary name can never match — `pkill` even warns that this "will result in
  zero matches". **A census that inflates itself and a kill that matches nothing are the same shape**,
  and both report success.

**What this does and does not buy.** It makes "the box was quiet" a checkable column rather than a
claim. It does **not** establish that quiescence is sufficient for a reusable absolute: #3299 measured
±3% residual drift under *enforced* quiescence, and the drift **reverses sign** between within-session
and across-session scales — 7 of 9 points rose round-over-round while the across-session reads fell.
Pure co-scheduled load predicts a monotone response to load, not a sign reversal. That residual is
**unexplained**, and this work does not claim to explain it.

---

## 4. Planned controls, stated before measuring

Recorded here in advance so that a control which later fails cannot be quietly dropped.

1. **Codegen-equivalence control for `perfprof`** (§1.2). Same-session interleaved throughput on the
   *unprofiled* arm under `release` vs `perfprof`. If they differ beyond resolution, every per-function
   number is attributed to a binary that is not the one whose throughput is reported, and that must be
   stated as a limitation rather than assumed away.
2. **A profiler-overhead control.** Throughput with and without `perf record` attached, same binary.
   Sampling is cheap but not free, and AC5 reports rows/s — a figure taken under an attached profiler
   is not the figure taken without one.
3. **Occupancy-matched clock, enforced fail-closed** (see §5).

---

## 5. The clock basis — AC4, and why stating it is not enough

AC4 asks for the `1,746 ns/row` vs `+4,697 cycles/row` reconciliation "stating the clock basis and
whether `cycles/row` is per-core or sibling-aggregate."

**Under CPU-wide `perf stat -C`, `cycles ÷ task-clock` is NOT a frequency.** `task-clock` accrues
elapsed × nCPUs *including idle CPUs*, so the quotient is **occupancy × frequency**. It is valid only
at **matched occupancy**.

This is recorded as **#3299's finding**, not re-derived here, and the history is the reason this
section enforces rather than explains. #3299 published that quotient as a clock, retracted it when it
read `1.271 "GHz"` at `S=4/N=1` (one busy core diluted across eight pinned logical CPUs) — and then
**made the same error again hours later**, claiming endpoints were licensed by "matched occupancy —
80%/80%, and that WAS measured", when the 0.80 was the counting window over perf's own process
lifetime (20 s / 25 s), matched by *harness parameters* rather than by anything about the hardware. The
first retraction had **overridden a caption written specifically to prevent it**.

**The consequence for this issue: AC4 as written is insufficient.** Stating the clock basis is exactly
the intervention that already failed, twice, in the hands of people who knew about it. So the
reconciliation here prints a clock only through a path that **refuses** when occupancy is unmatched or
unmeasured, after #3299's guard (three independent occupancy figures required to agree within 0.02,
including the window÷lifetime ratio carried under a label that says it is **not** an occupancy, so the
quantity that was misread cannot be misread the same way twice).

True frequency is therefore taken as `msr/aperf/ ÷ msr/mperf/ × 2.4 GHz TSC`, measured **on this
work's own arm at its own occupancy**. #3299's occupancy-matched `f(S=1) = 3.509 GHz` is used **only as
a plumbing check** — a value far from it indicates a setup fault — and is deliberately **not** inherited
into the published reconciliation: it was measured on the **bare-scan** arm, and turbo responds to power
draw, so a compute-heavy encode region can clock lower than a memory-bound scan at identical occupancy.
Should this work's encode-arm clock land materially below 3.509 GHz, **that difference is itself a
finding** about encode's power draw, not an error to be corrected toward the borrowed number.

