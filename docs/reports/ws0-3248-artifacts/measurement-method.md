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

* **Codegen fidelity — and the first claim made here was FALSE, so it is corrected rather than
  removed.** This section originally asserted that moving only debuginfo emission and symbol retention
  left codegen identical. Measured: `debug = 1` grows `.text` by **+12,992 B (+0.156%)** on
  `cqlite-flight` and **+13,632 B (+0.160%)** on `flight-loadgen`. Debuginfo is **not** codegen-neutral.

  That falsification produced a better design, because the two things a profiler wants have different
  costs and only one is needed for a headline number: the **symbol table** is what flat per-function
  attribution needs; **debuginfo** is needed only for `perf annotate` source interleaving and
  `--call-graph dwarf`. So a second profile carries symbols alone:

  ```toml
  [profile.perfsym]
  inherits = "release"
  debug = 0
  strip = "none"
  ```

  `perfsym` differs from `release` by **+0.0185%** (`cqlite-flight`) and **−0.0037%**
  (`flight-loadgen`) — an order of magnitude closer than `perfprof`, and **of opposite sign** on the
  two binaries, which reads as linker/section layout noise rather than codegen divergence (a
  systematic codegen change would not shrink one binary while growing the other).

  Therefore: **AC1 headline per-function figures come from `perfsym`**; `perfprof` is used for
  **structural evidence only** (region membership, call-graph shape, source-line annotation) and never
  as the source of a headline number; **AC0 runs on plain `--release`**, as #3096 did, because a
  reproduction must not silently change the binary. `-C force-frame-pointers` is deliberately **not**
  set on either profile — it would give cheap call-graph unwinding but *does* alter codegen, which is
  the property being protected.

  **CORRECTION to this section's original plan, forced by a measured capability census.** It first
  said call-graph runs would use `perf record --call-graph dwarf`. They cannot. All three unwinding
  mechanisms are now characterized on this host: **dwarf HANGS** past 120 s on a binary this size (a
  committed in-tree trap, `ws0-3217-artifacts/harness/profile-oncpu.sh:8-15`); **LBR is
  UNAVAILABLE** — measured here as `exit 255: PMU Hardware or event type doesn't support branch stack
  sampling`, because this is a KVM guest and LBR is not virtualized; **frame pointers are the only
  mechanism that works.**

  So every call-graph statement in this work rests on a **codegen-perturbed** binary **by necessity,
  not by preference** — worth saying plainly, because the alternative reading ("dwarf was available
  and they chose not to use it") would overstate how freely the evidence was selected. The headline
  figures are unaffected: they are flat self-time, which needs symbols only. Detail, including why
  this is the **third** capability this virtualized host has cost the WS0 programme:
  `raw/callgraph-capability-census.md`.

  `.text` size is a **proxy**: identical size would not prove identical code, and near-identical size
  does not prove identical speed. The pre-registered throughput control in §4 is what settles it.
  Detail: `raw/profile-codegen-fidelity.md`.
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


---

## 6. A named hazard: asserting a state from something ADJACENT to it

Recorded at the coordination lead's request, because it recurred **five times in one session**
across two independent agents, and because it is the **human-side twin of the silent instrument**.

**The shape.** You want to know whether property P holds. You measure something correlated with P —
something adjacent, cheap, already to hand — and you report P. When the adjacency breaks, the report
is confidently wrong and nothing signals it.

**The instances, all from this issue's own delivery:**

1. **This lane** told the coordination lead that AC4 "turns entirely on (a) the clock and (b) the
   sibling-aggregate question." (b) was recorded explicitly in four places and mechanically enforced.
   An absence was asserted **without reading for it** — in the same comment that was insisting on
   affirmative measurement.
2. **The coordination lead** told three lanes the root checkout contained a fix, having verified that
   a *commit existed*, not that the checkout contained it.
3. **The coordination lead** verified a gate pin was **present in a file** and concluded it was **in
   effect**. It was not: `.bashrc` returns early for non-interactive shells, so every gate on the
   fleet resolved `slots=3`, not 1. *A setting present in a file is adjacent to a setting in effect.*
4. **The coordination lead's** probe for the #3451 `SyntaxError` searched for **backslashes at
   end-of-line** and returned clean; the defect is a backslash **mid-expression**. A second probe
   found **zero Python heredocs** in a file that plainly contains them. Both returned clean, and
   **clean was indistinguishable from correct** — the defect was only found by reading a fix commit.
5. **This lane's own quiescence gate** bounded `load1` at both boundaries and refused a clean run.
   `load1` is adjacent to "the box is busy" but not identical to it: it is a one-minute decaying
   average, so immediately after a CPU-bound window it measures **the measurement's own residue**.

**The rule that falls out, and it is the lead's wording:** *a probe that finds nothing proves nothing
until it is validated against known-good input.* A clean result from an unvalidated probe is not
evidence of absence; it is evidence of nothing. Every capability check in this work is therefore
paired with a positive control — the demangler probe was run on a **purpose-built v0-mangled binary**
so that "it demangles" was observed rather than inferred, and the multiplexing question was settled
by **running two perf sessions concurrently and reading the enabled-percentage column**, not by
reasoning about counter allocation.

**And the corollary that cost the most time here:** when a guard fires against your own work, the
first question is not "how do I get past it" but **"is the guard measuring what I think it is?"** Two
of the five instances above were guards firing correctly (#3451's fatal step; the stale-binary
refusal) and one was a guard firing *incorrectly* on a sound run (the `load1` bound). Telling those
apart required reading the guard, not adjusting the threshold — and in the one case where the guard
was wrong, the fix was to **relocate the bound to where it is valid and make the binding check
stronger**, never to loosen it.

### The eighth instance, found by review inside the guard built to stop it

Round 7 of review found the same hazard **in the reporter's own quiescence check** — the code written
to enforce this section. The reporter verified that the quiescence verdict named the same *timeseries
file* the session manifest declared, and treated that as evidence the session was quiet.

`box-load.jsonl` is **one long-lived file spanning every session on this box**. Naming it establishes
which *sampler* produced the verdict, and nothing whatever about *when*. A clean verdict judged over a
different ten-minute window of the same file satisfied every check and certified this session:
**the right instrument, the wrong interval, reported as the property.** The fix binds the verdict's
judged window to this session's own measurement window, derived from the rep payloads' `ts_unix_ms`.

**And the first version of that fix reproduced the hazard in the opposite direction**, which is why it
is recorded here rather than in a changelog. Not knowing which end of the rep `ts_unix_ms` denoted, it
widened every rep by its duration in *both* directions, on the reasoning that symmetric widening is
the conservative choice. It is conservative — and it pushed the window 18 s past the true end and
**refused a correctly covered session**. A guard that reds on correct input is the guard agents learn
to waive, so a false red is not the safe failure it feels like. It was settled by *measurement*
(payload mtime equals `ts` to the second) and by the *producer's source* — `ramp.rs:184-188` takes
both `started.elapsed()` and `SystemTime::now()` after every worker joins — rather than by choosing
the more cautious-sounding assumption. **"Conservative" is not a substitute for "measured": a guess
padded in the safe direction is still a guess, and it can still be wrong in the direction that costs
you a true result.**

### The ninth instance: a PROXY chosen for the property it proxies — and the regress that followed

I wanted to know whether two binaries contained the same machine code. Bytes were unusable — operands
relocate — so I compared **instruction mnemonics** and reported the answer as *machine-code identity*.
A mnemonic sequence is **adjacent to** machine code: it agrees most of the time and diverges exactly
where the interesting cases live. On that basis I published a headline number **twice**.

**Then it happened three more times, and the sequence is the lesson.** Each oracle fixed a real defect
found by review, and each produced a different answer to the same question:

| oracle | "identical" | its excess | its self-time |
|---|---|---|---|
| byte equality | 15/363 (4%) | — | — |
| mnemonic sequence | 291/363 (80%) | +54.5% | 13.80% |
| normalized operands | 136/363 (37%) | +77.1% | 5.88% |
| + ambiguity-aware | 121/363 (33%) | +90.7% | 2.56% |

**The base halved at every step while the ratio climbed. That pattern is itself a finding, and it is
the one worth carrying forward.** Each refinement legitimately discarded samples — and every batch it
discarded was diluting the effect, so the surviving subset looked ever more dramatic on an ever
smaller base. That is what **fitting** looks like from the inside: every individual step is defensible,
the trend is not. The end state, +90.7% on 2.56% of self-time, could not be carried by three reps at
499 Hz, so the claim was **withdrawn** rather than narrowed a fifth time.

**The rules this leaves:**

1. **When you substitute a proxy for the property, say so in the claim.** "Identical mnemonics" is a
   fact; "identical machine code" was an inference. Collapsing the two is where the error entered, and
   labelling the table with what was *measured* would have exposed it immediately.
2. **Ask which direction the proxy errs, and by how much.** Bytes over-report difference; mnemonics
   over-report sameness. Knowing the direction is what let the surviving claim be phrased against a
   *lower* bound while it still stood.
3. **Treat a rising effect on a shrinking base as a stop signal, not a result.** Three consecutive
   corrections that all moved the number the same way should prompt withdrawal, not a fourth
   correction. The question to ask at that point is not "what is the next defect" but "is this
   quantity measurable with this instrument at all".
4. **A retraction is part of the result.** What survived — the bucket-total **+21.2%**, which assumes
   nothing about machine-code identity and was identical under all four oracles — was there the whole
   time and needed none of this. The salvage is a *methodological* fact: symbol presence does not imply
   shared machine code, and 23% of that bucket cannot be attributed by name at all.

### The same shape in the tooling, for completeness

Three further instances landed in *mechanism* rather than in reasoning, and they are the same error:

* `pgrep -f <pattern>` matches the census command's **own cmdline**, so a process census inflates the
  very count it is measuring. One lane's field read `0\n0`; another's probe reported a busy box that
  was idle, with both matches being its own `ssh` command string at 0.0% CPU.
* `pgrep -x` is not the fix: the kernel `comm` field is capped at **15 characters**, so a longer
  binary name can never match — `pkill` itself warns the invocation "will result in zero matches".
* A `pkill -f` whose pattern appeared in the invoking shell's own command line **killed that shell**,
  silently skipping the remainder of a multi-step command (this lane, exit 144).

A census that inflates itself, a kill that matches nothing, and a kill that kills the wrong thing are
one defect: **the observer appeared in its own measurement.** The fix in every case was to attribute
by identity — `/proc/<pid>/comm`, `/proc/<pid>/cmdline`, `/proc/<pid>/cwd` — rather than by a pattern
match over a shared namespace.


---

## 7. `grep -q` under `pipefail` is a RACE, not a shortcut — three instances in one delivery

Recorded as its own section because it bit **three times** in this one issue, **in both
polarities**, and because nothing in this repository writes it down.

**The mechanism.** `grep -q` exits the instant it finds a match. Under `set -o pipefail`, the
pipeline takes the worst status of any stage — so if the producer is still writing when `grep`
closes the pipe, the producer receives **SIGPIPE**, exits non-zero, and **the pipeline reports
failure on the SUCCESS path.** Whether that happens depends on how much the producer has left
to write when the match is found, which makes it **intermittent** and therefore easy to
misfile as a flake.

**The three instances:**

| where | polarity | effect |
|---|---|---|
| `ws0-baseline.sh` symbol check — `nm \| grep -q '_RN'` | **fails CLOSED** | refused every **correct** input: a binary with 2,997 Rust symbols read as having none, which would have blocked every legitimate profiling run |
| `test_ws0_provenance_guards.sh` — `awk <region> \| grep -q` | **fails CLOSED, intermittently** | reported a defect that did not exist, ~45% of runs, in a **gate component** |
| the same file's sibling sites (26 of them) | **dormant** | the match sits near the end of a short output, so the producer has already finished |

**The attribution detail worth keeping**, because it is how a latent defect becomes a live one:
the provenance race measured **0/20 failures against `origin/main`'s driver (a 170-line extracted
region)** and **9/20 against this branch's (197 lines)**. The bug was in the test all along; a
diff that merely **grew the file being inspected** turned it into a ~45% flake for everyone. So
"I did not touch that line" is not a defence — **growing a producer's output is a change to every
`grep -q` pipeline that reads it.**

**The rule.** Do not pipe into `grep -q` under `pipefail`. Capture first:

```bash
region="$(awk '/start/,/end/' "$file")"
if grep -q 'needle' <<<"$region"; then ...
```

…or use `grep -c` and compare the count, which reads all of its input and cannot SIGPIPE its
producer. The same applies to `head -n`, which also closes early.

**Why this belongs in a measurement method doc rather than a style guide.** Both failing
instances were **guards**, and both failed in the direction that destroys trust in a guard: one
refused correct input, the other cried wolf intermittently. A guard that fires on a correct
tree is the guard people learn to ignore, and then delete. This defect class does not produce
wrong *numbers* — it produces **wrong verdicts about whether the numbers can be trusted**, which
is the same harm one level up.

---

## 8. The profiler does not bracket the counted window, and the error is asymmetric

Stated here because it survived six review rounds inside a claim of exactness, and because the
asymmetry — not the magnitude — is what matters to a differential.

The driver arms `perf record`, sleeps **300 ms**, and only then opens the counting window. There is no
setting that makes "brackets exactly" true: arming late leaves the start of the window unsampled,
arming early includes pre-window samples. The chosen direction is the one that cannot silently **drop**
the region under study.

The cost, from this session's own records:

| arm | counted window | 300 ms as % of capture |
|---|---|---|
| `bare_scan` | 11.89–12.15 s | **2.47–2.52%** |
| `flight_bypass` | 59.21–62.06 s | **0.48–0.51%** |

The arms' windows differ by ~5×, so the contamination does too: **~5.2× more pre-window capture in the
scan arm**, in a document whose headline is the *difference* between the arms. The samples land on
server startup and steady-state rather than the encode region, so the bias inflates non-encode
self-time, more in the scan arm. It does not close the reported gaps — the smallest is 21.5% — but it
is a real term that was previously reported as zero, and a reader comparing per-function shares
between the two arms should carry it.

---

## 9. A guard's failure modes include REDING ON CORRECT INPUT, and that is not the safe direction

Recorded at the coordination lead's request after the **third** false red on this issue. All three
came from the same collision: a **stricter check** meeting a **fixture or a precision mismatch** — not
from a wrong idea about what to check.

| # | The stricter check | What it met | Symptom |
|---|---|---|---|
| 1 | window coverage bound to the session's reps | `ts_unix_ms` semantics **assumed** rather than read | window pushed 18 s past the true end; a correctly covered session **refused** |
| 2 | a QUIESCENT verdict must carry boundary evidence | a **test fixture** predating the requirement | the positive control red, and four refusal cases fired on the wrong precondition |
| 3 | the judged window must cover the flight reps | window stamped `date -u +%…%SZ` (**whole seconds**) vs `ts_unix_ms` (**milliseconds**) | a window that genuinely covered a rep ending at `.900` read as ending at `.000` and **refused** it |

**Why this matters more than it looks.** A false red feels like the safe failure — you have refused
something, so surely you cannot have published a wrong number. But a guard that reds on correct input
**is the guard people learn to waive**, and a waived guard protects nothing. Instance 1 was written
*two rounds after* that sentence was recorded in this very document, by the person who recorded it.
**Naming a hazard does not inoculate you against it.**

**What the three have in common, and the rule that follows.** Each fix required knowing something
about the *other* side of the comparison, and in each case the temptation was to pad in the
"conservative" direction instead of finding out:

* Instance 1 was settled by **reading the producer's source** (`ramp.rs:184-188`: both
  `started.elapsed()` and `SystemTime::now()` are taken after every worker joins, so `ts_unix_ms` is
  the rep's END) and by an independent measurement (payload mtime equals `ts` to the second).
  Symmetric widening had been chosen because the record "does not say which end" — a guess padded in
  the safe direction is still a guess, and it was wrong in the direction that cost a true result.
* Instance 2 was settled by making the **fixture assert its own baseline**, so it cannot silently
  drift out from under the checker that consumes it.
* Instance 3 was settled by **one second** of slack on the end bound only — the exact maximum the
  coarser side's resolution can hide (a stamp of `T` means the true instant lies in `[T, T+1)`), and
  applied to the end alone because truncation moves *both* edges earlier, which **widens** at the
  start (safe) and **narrows** at the end (unsafe). Any larger tolerance would be invented; any
  symmetric tolerance would misunderstand the asymmetry.

**So: when a guard compares two recorded quantities, establish the RESOLUTION and the SEMANTICS of
both sides from their producers before choosing a tolerance.** "Conservative" is not a substitute for
"measured", and the cost of getting it wrong lands on correct runs — which is the direction nobody
checks for.
