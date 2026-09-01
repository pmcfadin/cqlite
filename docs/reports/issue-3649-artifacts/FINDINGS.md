# Issue #3649 — what is established in-lane, and what is not

This file records the facts a rig session should not have to re-derive, each with
the citation it rests on and the command that verified it. It contains **no
throughput measurement**, because none was taken: the rig and the corpus are both
absent from this lane, and a number produced here would be exactly the phantom
[#3649](https://github.com/pmcfadin/cqlite/issues/3649) warns about.

Verified in lane `/data/lanes/lane-3649` at `d23403d1e`, 2026-09-01.

---

## 1. The AC's triage step passes: the #2820 mechanism is intact

The acceptance criteria say *"if the measured effect is below target, triage
**before** filing a regression: confirm the send-reduction oracle still passes (it
isolates the mechanism from the served path)."* That step was run, and it passes,
**before** any measurement — so a below-target rig result cannot be read as
evidence of a broken mechanism.

```
$ cargo test -p cqlite-core --test issue_2820_merge_fanin_batch
running 2 tests
test the_merge_fan_in_sends_one_message_per_batch_and_loses_no_row ... ok
test a_sub_batch_merge_delivers_every_row_without_waiting_for_a_full_batch ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

**No regression is indicated and none is being filed.** #2820 is a correct change;
this issue measures its served-path effect, and a null or small result is a valid
outcome, not a defect report.

Arms, resolved in this checkout:

| arm | ref | commit | subject |
|---|---|---|---|
| HEAD | `cfa93fe99` | `cfa93fe99` | `perf(#2820): batch the k-way merge egress fan-in (L1), co-designed with #2765 (#3659)` |
| BASE | `cfa93fe99^` | `674cffa9d` | `chore(#3549): delivery telemetry record for PR #3660 (#3671)` |

Note the BASE arm is a **telemetry-only commit**, so `cfa93fe99^` is a clean
pre-#2820 tree with no unrelated code change riding in it.

---

## 2. Why the measurement cannot run in this lane

Two independent disqualifiers. Either one is sufficient.

### 2a. The host is the excluded class

Measured 2026-09-01T16:30Z on this box:

| fact | value |
|---|---|
| instance type | `c7i.4xlarge` (from `/sys/devices/virtual/dmi/id/product_name`) |
| vCPU | 16 (`nproc`) |
| RAM | 30 GiB (`/proc/meminfo` `MemTotal` 32,308,512 kB) |
| storage | EBS root — **no instance-store NVMe** |
| load average | `19.75 29.84 38.09` at the census; **`81.60 66.10 38.69`** ~20 minutes earlier |
| concurrent lanes | 9 (`/data/lanes/lane-*`) sharing one 16-vCPU box and one `.git` object store |

The AC names the **field i4i narrow rig, not a lane box**, for exactly this
reason. A 1-minute load average that moved from 81.6 to 19.8 inside twenty
minutes is not a host on which a 1.1–1.25× effect is separable from the
neighbours, and no amount of interleaving fixes a co-tenancy swing of that
amplitude within a single replicate.

### 2b. The corpus is four hundred times too small

| root | `*-Data.db` files | **total bytes** | largest single file |
|---|--:|--:|--:|
| `/data/datasets` (the exported `CQLITE_DATASETS_ROOT`) | 155 | **2,405,118** | 647,164 |
| `<checkout>/test-data/datasets` | 34 | 864,735 | 263,327 |

The **entire** fetched corpus is 2.4 MB across 155 tables. `flight-loadgen
--shape full` against a 647 KB table measures request setup and gRPC framing,
not the read path — which is the AC's own point (*"the ~2 MB / ~1k-row lane
tables are not [meaningful]"*). The driver's `--min-corpus-bytes` floor is
268,435,456 (256 MiB); the largest thing available here is **0.24% of it**.

### 2c. The rig cannot be provisioned from here

No `aws` CLI, no `agent-ami`, no ssh alias to a persistent rig. Rig spend is
owner-authorized. The i4i rigs in this program's history (#3224, M0/#2818) were
provisioned fresh, run, and terminated; **there is no standing rig to attach
to.**

---

## 3. TWO quantities, verdicted differently — the issue's warning box collapses them

**Target: ~1.1–1.25× narrow single-stream, ~1.05–1.1× wide.**
`docs/research/phase2-verify-row-engine.md` §3.2 line 107:

> **Revised: ~1.1–1.25× narrow single-stream, ~1.05–1.1× wide** — and flagged
> unmeasured

**1.5–1.9× is NOT a throughput target.** Same section, line 115:

> Keep 1.5–1.9× as a **rig-narrow ceiling**, not a field figure (§8).

and line 111 identifies what it is a ceiling *of*: **utilization** — "an
aggregate-CPU-headroom upper bound realized as throughput **only** because that
tax is what collapses `C(N)`". §3.2's own heading calls the whole slice
"over-quoted and unmeasured".

### They are different measurements and they are reported separately

| quantity | how it is run | how it is verdicted |
|---|---|---|
| **single-stream** | `--ramp 1` | against the **~1.1–1.25× narrow / ~1.05–1.1× wide** band |
| **utilization** | a concurrency ramp | as a **direction with an interval** — "rises measurably" |

The plan of record states the M2 acceptance criterion as util throughput
*"rises measurably toward the 1.5–1.9× ceiling"*
(`docs/architecture/throughput-program-2026-07.md` line 371, verified). "Toward"
is the whole of it: the criterion is a **direction**, not an attainment.

The harness therefore emits **two sections and two verdict lines**
(`verdict single-stream <TOKEN>` / `verdict utilization <TOKEN>`) with disjoint
affirmative token sets, and:

- 1.5–1.9× is **named on every run of both sections** and **tested against in
  neither**;
- `ab_stats.decide_utilization(ci_low, ci_high)` **is not given a threshold
  argument at all**, so a comparison against the ceiling is not expressible —
  which is stronger than a promise not to make one;
- `selftest-analyze.sh` asserts that no ceiling-attainment token appears in
  either section, that the utilization section cannot emit a band token, and that
  the single-stream section cannot emit a direction token.

A single-stream interval landing in the 1.5–1.9× region renders `ABOVE-TARGET`
**against the 1.10–1.25 band**, with the ceiling merely named.

---

## 4. `flight-loadgen` throughput is a point estimate — hence the replicate design

Per ramp step, `flight-loadgen` emits a full latency histogram
(`p50`/`p95`/`p99`/`max`, `hdrhistogram`) but computes throughput as
`count / duration_s`:

```rust
// tools/flight-loadgen/src/record.rs
let per_s = |n: u64| { if duration_s > 0.0 { n as f64 / duration_s } else { 0.0 } };
...
qps: per_s(self.ok),
rows_per_s: per_s(self.rows_total),
bytes_per_s: per_s(self.bytes_total),
```

There is **one throughput number per step and no interval anywhere in the tool**.
Three consequences, all of which the instrument is built around:

1. Dispersion has to come from **repeated runs**. Hence replicates, and hence the
   analyzer's refusal to bootstrap below `--min-pairs`.
2. The runs must be **interleaved** (`base, head, base, head, …`), because
   running one arm to completion then the other aliases session-long host drift
   onto the second arm. This is the failure mode that made the proxy bench
   uninterpretable.
3. All reported dispersion is **between-replicate**. Within-step variance is not
   observable from this JSONL and is not modelled — the analyzer declares that on
   every run as one of its `NON-EXHAUSTIVE` lines rather than leaving a reader to
   assume otherwise.

The rejected proxy bench, for the record: `compaction/narrow`, separate
`--target-dir` per commit, base **78.6 ms [69.5, 88.4]** vs HEAD **66.5 ms
[54.5, 83.2]** — a ~15% point difference with heavily overlapping intervals. It
was correctly **not** reported as AC-2 satisfied. `selftest-analyze.sh` carries
that shape as a case: a fixture whose point estimate is ~1.16× — *inside* the
target band — and whose interval is [0.97, 1.40] must render `INCONCLUSIVE`, not
`MEETS-TARGET`.

---

## 5. The corpus constraint the rig session inherits

The field is **LZ4-compressed** (`docs/architecture/throughput-program-2026-07.md`
line 21: "RF=3, LZ4, ~1.9M partitions/node, 4-vCPU pods"), and that document
repeatedly flags *uncompressed* as a **known artifact** of the WS0 loopback
measurements, not a neutral choice — line 23 ("**uncompressed**, loopback … a
server-direct ceiling for that shape, **not** a field prediction"), line 56, and
line 69 ("warm + uncompressed + loopback artifacts — real in the field").

**`tools/ws0-corpus-gen` cannot supply a compressed corpus**, and says so in its
own module documentation (`tools/ws0-corpus-gen/src/lib.rs:36-41`, verified):

```
//! # Uncompressed by construction (issue #1406)
//!
//! CQLite's production write surface emits UNCOMPRESSED SSTables only and never
//! a `CompressionInfo.db`. The generator asserts the absence of that component
//! rather than assuming it.
```

This is the #1406 claim boundary, not a gap in the generator. So the rig needs a
**real Cassandra** to generate the corpus, or the corpus transported to it. See
`RUNBOOK.md` §2.

Why it matters *for this particular ratio*, beyond comparability with M0: #2820's
lever is the merge fan-in park/wake tax as a **fraction of total server CPU**.
Removing LZ4 decompression removes real CPU from the denominator, so an
uncompressed corpus **inflates** the measured ratio relative to the field. An
uncompressed rig run would bias toward the target, in the direction that is
hardest to notice.

---

## 6. The trap that would have wasted the rig session: #3058's single-source bypass

**Found while building the instrument, not previously recorded on #3649.**

`cqlite-flight` has a single-source fast path on the `do_get` row route, added by
[#3058](https://github.com/pmcfadin/cqlite/issues/3058) in `680abd0d2`
(2026-07-29, "single-SSTable merge bypass on the Flight do_get data plane —
3.32x"). When a request has **one** post-prune source and no static column, no
`dropped_columns`, and no aggregation, the request **never enters the k-way
merger** — and the k-way merger's egress fan-in is the entire subject of #2820.

`680abd0d2` predates **both** arms (verified: `cqlite-flight/src/bypass.rs` is
present at `cfa93fe99^` and at `cfa93fe99`). So on a **single-SSTable** corpus:

> both arms take the fast path, neither arm executes the code #2820 changed, and
> the measured ratio is **1.0 by construction** — a `BELOW-TARGET` verdict that
> looks exactly like a measured no-effect and is in fact a measurement of
> nothing.

Two mechanical guards close it, and both are exercised by the self-test:

1. **`ab-throughput.sh` pins the merge arm.** `--merge-path` defaults to `merge`
   and is exported as `CQLITE_FLIGHT_MERGE_PATH` into **both** arms' servers.
   `merge` is documented as absolute — "never take the fast path"
   (`cqlite-flight/src/bypass.rs`). It is recorded in the manifest, printed by
   the analyzer, and anything other than `merge` produces a disclosure line
   beside the verdict.
2. **`ab-throughput.sh` refuses a corpus below `--min-sstables` (default 2)**,
   cause `corpus-too-few-sstables`, and **`analyze-ab.py` refuses**, cause
   `merge-path-bypassed`, when the manifest records fewer than two `*-Data.db`
   files *and* the merge arm was not pinned. Fewer than two files on disk means
   at most one source, so that refusal is sound in the direction it fires; with
   two or more the arm cannot be settled from the manifest, so the unpinned case
   is disclosed rather than refused.

Note the asymmetry that makes this worth guarding twice: `bypass` is documented
as *"`auto` with an explicit, assertable name"* — it never overrides a
correctness precondition — so only `merge` actually guarantees the path under
test is the path that ran.

---

## 7. Admission control (#2420) will confound the ramp unless it is pinned

`cqlite-flight` admits a bounded number of concurrent `do_get` scans
(`cqlite-flight/src/cli.rs:59-73`, verified). Past the ceiling a request waits
`--admission-wait-timeout-ms` (`DEFAULT_WAIT_TIMEOUT_MS = 30_000`,
`cqlite-flight/src/admission.rs:84`) and is then **shed with gRPC `UNAVAILABLE`**
— which `flight-loadgen` counts separately as `requests_unavailable`
(`tools/flight-loadgen/src/record.rs`).

**Unset, the ceiling is DERIVED** from the parallelism the process may use
(`clamp(2 × hardware threads, 2, 64)`, #3225, honouring the affinity mask and the
cgroup quota). On a 4-vCPU `i4i.xlarge` that is 8 — and pinning the server to two
hardware threads with `taskset` changes it again. So unpinned it is a property of
*the box and the pinning*, not of the experiment.

**The failure mode is not a visible error.** A ramp step above the ceiling
measures *the admission ceiling*, and it presents as a **plateau** — exactly the
shape someone would read as saturation and attribute to the engine.

Three mechanical responses, all exercised by the self-test:

1. `ab-throughput.sh` **requires** `--max-concurrent-scans`, pins it on both
   arms, and **refuses a `--ramp` topping out above it**.
2. It reads the resolved value **and its provenance** back from the server's own
   startup line — `cli::log_startup` logs `max_concurrent_scans` and
   `max_concurrent_scans_source` (`flag` | `env` | `derived` | `derived-fallback`,
   `cqlite-flight/src/admission.rs:183-193`) — and dies on `admission-mismatch`
   or `admission-provenance`. A value we passed and a value the server resolved
   are different facts; only the second is a measurement. An unreadable line is
   `NOT-OBSERVED` and is disclosed beside the verdict, never assumed to agree.
3. `analyze-ab.py` refuses `admission-mismatch` across arms; refuses any shed at
   single-stream concurrency (`run-shed`); and in the utilization section
   **excludes** each shed step, **reports every exclusion as an explicit fact**
   (`excluded-step …` plus `excluded-steps N RECOGNISED`, affirmative at zero),
   and requires the two arms of a pair to have the **same surviving ladder**
   (`ramp-steps-not-comparable`) — a peak taken over different ladders is not a
   ratio.

`--batch-size` (default 8192 rows per Arrow record batch,
`cqlite-flight/src/cli.rs:56-57`) is pinned and recorded on both arms for the
same reason in miniature: it is the record-batch row cap, so it interacts
directly with the egress batching #2820 changed.

---

## 8. What this lane delivered instead

| artifact | what it is |
|---|---|
| `ab-throughput.sh` | the interleaved paired A/B driver — two worktrees, two target dirs, pinned merge arm and admission ceiling, fail-closed pre-flight, per-run validation, a manifest rewritten after every completed run |
| `analyze-ab.py` | the CLI and the two-section report, anchored so it cannot be pasted as a certification |
| `ab_stats.py` | the statistics and **both** verdict rules, with their citations beside them |
| `ab_input.py` | manifest/JSONL loading and every named refusal, including the admission handling |
| `ab_common.py` | the anchored, sanitized emission every module writes through |
| `ab_driver_support.py` | the driver's ramp/record validators and startup parser, as an **executable file** so they can be tested without a rig |
| `selftest-analyze.sh` | 316 deterministic cases, including complete two-arm sessions — measurement and sensitivity control — run end to end under PATH shims |
| `RUNBOOK.md` | the metered-rig procedure: pre-flight, positive control, the run, the termination contract, and the AC checklist |

**Not delivered, and deliberately so: a number.** The AC is discharged by a rig
session, not by this lane.

---

## 9. The eighth lesson: make there be ONE path, not a guard on each path

Three of round 9's four findings were the same shape, and it is the shape round 8
only half-closed: **a value with more than one source, guarded at one source
instead of at the value.**

- the batch-size floor was checked on `--batch-size`; per-arm extras were a
  second route (round 8, fixed for that one value);
- the corpus floors were checked in the driver; an operator lowering
  `--min-sstables` was a second route, and the analyzer trusted the number the
  session under test reported — **the third separate way #3058's single-source
  bypass has been reachable**, after the recursive census and the symlinked
  decoy;
- `--max-concurrent-scans` was declared per-arm overridable and validated
  globally, so any effective override failed at run time.

Guarding each resolved value one at a time is the same trap as reconciling
record fields one at a time — §10 (guard the VALUE, and enumerate the SET) — so
the fix is the same move that closed the
sharing class: **make there be one path.** A single `resolve-session` step takes
every raw input, applies every rule, and emits the complete resolved
configuration; the driver reads nothing else. A new option cannot route around a
guard because nothing else produces the values, and a structural case asserts
every declared resolver input is an option the driver accepts.

**And the analyzer enforces the floors independently.** A verdict must not derive
its validity from a number its own subject chose, so the documented minimums live
in `ab_common.py` and the analyzer checks *those*, ignoring the manifest's own
`min_*_required`. Lowerable only under a control label, where the verdict is
already disclaimed. Same reason the shape is re-checked rather than trusted.

**The `eval`, and where it came from.** Per-arm values were resolved with
`eval "VAR=\"$(...)\""`, which executes a command substitution embedded in an
operator-supplied flag value. It was introduced *by the fix* for round 7's argv
problem — the remedy for a parse defect created an execution defect, which is
#3312's rule (control and data must not share a channel) arriving in the driver.
Removed rather than sanitised: associative arrays carry the values, and the
resolver's output is read as data. **Sanitising and keeping the `eval` would have
been the "rarer delimiter" move this repository has a standing ruling against.**

**One honest note about the case I wrote for it.** My first injection case
asserted only that no side effect appeared — and with the resolver in place a
dangerous payload is refused before it could reach any interpreter, so that
assertion **cannot fail however the code is written**. It was a case that could
not fail, written into the round whose subject is guards that do not bind. It now
asserts the refusal (behavioural, can fail) *and* the absence of side effects
(which would catch an `eval` placed upstream of validation, exactly where the
original was), with the split stated in the case itself.

---

## 10. The seventh lesson: guard the VALUE, and enumerate the SET

Round 8's three findings are three shapes this lane keeps producing, and two of
them were fixed by changing *where* a rule lives rather than adding another rule.

**Guard the resolved value, not the entry point.** The batch-size floor was added
in round 6 on the `--batch-size` flag. Per-arm extras are a second route to the
same value, and **symmetric** extras (`--base-server-extra '--batch-size 0'` and
the same on head) need no control label — so the floor was bypassable, the server
clamped both arms to one row per batch, and the analyzer would have rendered a
measurement verdict for a configuration nothing recorded. That is the fifth time
a guard on one entry point has been reachable around through a route added later.
The fix is not a second guard: the check now lives on the **resolved** value,
computed in one place, so every present and future caller inherits it because
there is only one place the resolved value exists.

**Enumerate the set, or keep finding its members one at a time.** Ten fields
appear in both the manifest and the step records, and nine had been reconciled
individually, each after a review found it missing — `target_concurrency`,
`duration_s`, `rows_total`, `round`, the admission ceiling, `max_batch_bytes`,
the wait timeout, the CPU affinity, the pair order. The tenth was `shape`, and by
then the pattern was the finding. So the set is now **enumerated in two
committed tables** (`RECORD_FIELD_DISPOSITION`, `WORKLOAD_DISPOSITION`), every
entry carrying `reconciled` / `checked` / `excused` with a reason, and the
self-test asserts that **every key of a real step record and a real manifest
appears in them**. A field added to either side is reconciled or explicitly
excused; it cannot join quietly.

**Declared residual, because the completeness is only as good as its direction:**
these prove every field that *exists* is accounted for. Neither can know about a
constraint nobody thought of — a manifest field that *should* bound a record but
was never conceived of is invisible to both. The tables close the "added later
and forgotten" hole, not the "never imagined" one.

**And a defect that only appears after the expensive step is worth its own
category.** A relative `--work-dir` broke both builds: `CARGO_TARGET_DIR` is read
after the driver has `cd`-ed into the worktree, so cargo wrote beneath the
worktree while the driver checked the original-relative path and died
`build-incomplete`. Same economics as the round-5 exit-127 defect — both arms
compile, on a box billed by the hour, and *then* it fails. Note why the harness
missed it: every end-to-end case passes an **absolute** path, which is the
natural thing to write and therefore precisely the input a harness will not cover
by accident. **When a defect's cost is concentrated after an expensive step, ask
what input your harness writes by habit rather than by choice.**

---

## 11. The sixth lesson: two correct rules can compose into an unusable whole

Round 6's High was that **the runbook's own sensitivity control could not be
analyzed**. Round 2 required the analyzer to refuse cross-arm server-config
differences. Round 5 required asymmetric per-arm flags to carry `--control`. Both
were right. Nothing reconciled them — so the control that deliberately sets the
head arm's `--max-batch-bytes 1` was refused as `server-config-mismatch` before
the control label was even considered.

**What makes this worse than an ordinary defect is which check it disabled.** The
sensitivity control is what tells an operator whether an `INCONCLUSIVE` means
"there is no effect" or "this box cannot measure one". Losing it does not corrupt
a number; it removes the ability to interpret the number you get.

**The fix is a declared, structured expectation instead of a blanket rule with an
exception.** The driver computes each arm's effective configuration once
(`effective-flag`), records it in the manifest as data, and the analyzer permits
**exactly** the declared differences, under a control label, only where the
observed values match the declared ones. An undeclared difference is still a
refusal, and so is an observation that does not match its own declaration.

Two things worth carrying:

- **`NOT-REQUESTED` is a value, not an absence.** The first version collapsed
  "this arm takes the server default" to "unknown", which made the sensitivity
  control's difference *undeclared* — the base arm requests nothing and the head
  arm overrides.

  **This is the fourth sentinel bug in this lane, and it bit in the OPPOSITE
  direction from the other three**, which is what turns the observation into a
  rule. The earlier ones read *unobserved* as agreement — permissive, admitting
  something unproven. This one read *not requested* as unknown — restrictive,
  refusing something legitimate. Same root, inverted consequence, depending only
  on which branch the sentinel silently falls into. So the rule is stronger than
  "do not read absence as agreement": **a sentinel needs its own branch, because
  whichever default it silently inherits will be wrong in one direction or the
  other.**

  That is also the tri-state lint (`1699-find-tristate`, pinned in
  `scripts/tests/test_agent_gate_summary.sh`) arriving from the other end: the
  lint catches a three-valued *shell predicate* collapsed into two — `[ -z
  "$(find …)" ]` folding "the scan FAILED" onto "no match" — and this lane found
  three-valued *data* (`NOT-OBSERVED`, `NOT-REQUESTED`, `UNMEASURED`,
  `could-not-tell`) collapsed the same way. Predicate and payload, one rule.
  **Declared gap: the lint covers the predicate half mechanically and the payload
  half not at all** — those four sentinels are conventions in prose, checked by
  review. Whether that is worth mechanising is an open question, and this
  repository's history with guards whose false-PASS count climbs across rounds
  argues against it; what is not open is that the gap should be stated rather
  than implied away by the citation.
- **Only execution finds this class.** Two individually-correct rules, each with
  its own passing tests, composed into an unusable whole. The case that catches
  it runs the control end to end under the shims — and it exists because
  §12 (the driver was never executed) had already made that possible.

**A validator that disagrees with its consumer is now FIVE instances, and the
count is the argument.** The duration grammar, the census enumeration, the census
containment, the per-arm argv construction, and the full-ring ticket check —
every one fixed the same way: read what actually consumes the value, mirror it,
and cite where you read it. The recurrence is why the rule is *check the
consumer's source first*, not *write the obvious validator and find out in
review*. The fifth was instructive twice over: the reviewer's summary of the
server's token semantics was itself partly wrong (it said `wraparound=true`
should be accepted because the server treats it a certain way; the source says
the flag is **not consulted at all** since #3634), so reading `ticket.rs` beat
taking a correct-sounding description — which is the same rule applied one level
up.

**And a sweep, not just a fix.** Findings 1, 3 and 4 were all fallout from the
round-4 restructure: a new rule colliding with an old one, a manifest field
recording the requested value instead of the effective one (`prewarm: true` on a
cold session), and help text pointing at a path the driver no longer writes. So
the restructure's whole surface got swept — and the sweep found **two self-test
cases that had been passing vacuously since round 4**, both asserting things
about `<work-dir>/results/`, a directory the driver stopped writing. A
restructure that closes a class reliably leaves its own debris, and the debris is
shallower but not less real. **When a restructure lands, sweep every path,
default and doc string that named the old design — the cost of that sweep is
lower than one review round.**

**And the sweep has to be a standing step, because of HOW those cases were
found.** Three self-test cases in this lane have been green for the wrong reason,
and **not one was found by the case failing** — they were found by sweeping, or by
fixing something adjacent that forced the quiet one to actually run. A case that
cannot fail is invisible to precisely the signal everything else relies on, so
the only thing that finds it is deliberately going to look. That is what makes
the sweep an argument rather than hygiene: "tidy up after a restructure" gets
skipped under time pressure; "the signal you would normally trust is structurally
blind here" does not.

---

## 12. The fifth lesson, and the one that closes the class: the driver was never executed

Round 5's High finding was that **`ab-throughput.sh` did not run at all**. A
helper had been extracted into `ab_driver_support.py` and one call site was left
invoking it as a bare shell command, so under `set -e` every session died with
`command not found` **during its first replicate, after both release builds**. A
second finding in the same round: the corpus census was computed and never
exported, so every manifest recorded it as zero — the corpus size the acceptance
criteria explicitly require be stated.

**Neither was subtle, and neither was visible to anything we had.** `bash -n`
cannot see a missing command or an unexported variable, and the self-test covered
the analyzer and the *extracted* helpers. 265 cases were green over a program
that could not complete a single session.

This is the FIFTH instance of one class in this lane — the dead utilization path,
ten environment-coupled cases, the silent passer among them, the inline parity
rule, and now the driver itself. §16 (a green suite over an unexecuted subject)
states the class; §13 (when one mechanism keeps producing findings) says that when a
mechanism keeps producing findings you remove the reason it can. **The reason was
that the session loop needed a rig, so nothing could run it.** So it no longer
needs one:

- `cargo`, `cqlite-flight` and `flight-loadgen` are replaced by **PATH shims** —
  the idiom this repository's own gate self-tests already use — with **no source
  seam and no test-only flag** a real invoker could trip over.
- The stub server **binds a real socket** and prints a realistic configuration
  line and a real post-bind readiness line; the stub load generator **connects to
  the endpoint it is handed**, so a wrong or stale address fails the run instead
  of passing quietly.
- A case runs a **complete five-pair, two-arm session** and asserts the manifest,
  the ten replicate files, the census, the per-arm builds, the reaped servers —
  **and that the analyzer reads the driver's own output and renders a verdict**,
  which no fixture can establish because it is the property of the two halves
  meeting.

RED-verified: reintroducing the two findings reds 11 cases and 1 case
respectively.

**DECLARED GAP, stated in the suite's own output rather than only here: the real
cargo build, the real `cqlite-flight` and the real `flight-loadgen` are exercised
by nothing.** These cases prove the driver's logic — ordering, plumbing,
recording, promotion — not that the server works.

**One thing the harness taught immediately, worth keeping.** The first version of
the stub load generator emitted a constant rate per arm, so every pair's ratio
was identical, the bootstrap interval had zero width, and the analyzer refused the
session as `bootstrap-degenerate`. That was **the guard working correctly on a
defective fixture** — and it is the failure mode a synthetic harness invites: a
stub simple enough to be obviously right is often too simple to be realistic, and
the difference surfaces as a false red against real code. The stub now carries a
deterministic per-replicate jitter, derived from `crc32` rather than `hash()`,
which is salted per process and would have made the suite non-deterministic.

---

## 13. The fourth lesson: when one mechanism keeps producing findings, delete the mechanism

Four review rounds produced findings in the driver's session lifecycle — the
work directory, the port, readiness, the census — roughly seven of the last
eight. Each round's fix was correct about the instance in front of it and left
the next layer:

| round | the fix | what it left |
|---|---|---|
| 2 | truncate the ledger only after the lock is held | the *sequential* case: a failed re-use still truncated |
| 3 | don't write until pre-flight passes and both arms build | the port could be taken *during* the builds |
| 4 | stage the manifest and promote it atomically | the replicate JSONLs it references were never staged |

Three fixes, three approximations of one property. The fourth attempt stopped
improving the sequencing and **removed the shared resource instead**:

- **The work directory.** Every session writes to `<work-dir>/run-<session-id>/`,
  a name no other session can produce. Nothing is promoted, truncated or
  overwritten, so *"a manifest never references a file from another session"* is
  true **by construction** rather than by ordering — the manifest and the files
  it names are the only things in a directory one process owns.
- **The port.** `--port` defaults to 0; each server binds an ephemeral port and
  the driver reads the real one from **that server's own** post-bind line. A
  probe could only ever establish that *something* answered, which on a nine-lane
  box is how the loser of a race measures the winner's binary while its own
  configuration asserts all pass against its own pre-bind log. With no shared
  port there is no race to detect.

**The rule: when successive findings land in one mechanism, the mechanism is the
defect.** Not the sequencing around it, not the guard in front of it. Ask what
resource is being shared and whether it needs to be shared at all — the fix that
ends the series is usually a deletion. Same shape as removing the second duration
grammar rather than widening it -- §15 (a parameter accepted without being
checked) -- one level up.

**And a second instance of the mirroring rule from §15 (a parameter accepted
without being checked).** The corpus census
scanned the whole data root recursively while the server reads **one** resolved
directory, flat. So both size gates could pass on files that are never served —
including the ≥2-SSTable gate that exists to stop the #3058 single-source bypass,
i.e. the guard against this harness's own headline phantom could be satisfied by
files the measurement never touches. The census now mirrors `DirSource::resolve`
and the producer's flat enumeration. **A validator must mirror the grammar *and
the scope* of whatever consumes the value.**

**A third instance of partial-observation-as-agreement closed the per-field
approach too.** Rounds 2, 3 and 4 each found one field where an observation was
counted, dropped or compared over a subset. They are now one `Corroboration`
type carrying observed/total and a state, constructed for every readback field,
so a new field cannot be added without inheriting the partial case — there is
nowhere else to decide it.

---

## 14. The third lesson: the dangerous defect is the one no test would have failed on

Round 3's headline finding was that **every pair ran BASE before HEAD**.
Interleaving across replicates — which the design called for and which was
implemented correctly — controls drift *between* pairs. It does nothing about a
gradient *within* one, and a monotonic drift over the ~2 minutes of a pair (a
thermal ramp, a clock adjustment, a neighbour's job starting) lands on the second
arm in **every** pair. That is a systematic bias in the exact estimator the paired
design exists to de-bias, and it would have arrived **with a tight confidence
interval**, which is worse than a noisy one because it looks trustworthy.

Two things about it are worth keeping.

**It would have produced a confident wrong answer, and no test would have
failed.** Every statistical case would still pass; the analyzer would still
render `MEETS-TARGET`; the self-test tally would still read green. There is no
coverage metric that finds this — only someone asking what the design controls
for and what it does not.

**And the rule was, at the time, the one piece of driver logic nothing executed**
— three lines inline in a session loop that needs a rig. So the fix is not only
to alternate the order but to move the rule into `ab_driver_support.py`, where the
self-test runs it and RED-verifies it: reverting it to always-base-first now reds
two cases. That is round 1's lesson applied to the highest-stakes line in the
file, and the ordering principle worth carrying is: **of the code you cannot
easily test, find the part whose failure is a wrong ANSWER rather than an ERROR,
and make that part executable first.**

The counterbalancing is also **counted from the record, not assumed from the
rule**: each run stores the position it actually ran in, and the analyzer refuses
a session where the counts differ by more than the one pair an odd replicate
count forces.

---

## 15. The second lesson: a parameter accepted without being checked against the claim

Round 1's review asked whether the instrument *works*. Round 2's asked whether it
measures *the right thing*, and three of its five findings were one shape: **an
option accepted because it parsed, never checked against the claim the report
would go on to make about it.**

- `--shape limit-k` with a `limit`-bearing ticket was accepted because the file
  was valid JSON — and would then have been scored against a band defined for
  `--shape full`.
- `--batch-size 0` was accepted as a non-negative integer; the server clamps it
  to one row per batch, so the manifest would have recorded a value that never
  ran — of the Arrow batch row cap, which is the very mechanism #2820 changed.
- `--step-duration 60` was accepted by the driver and **refused by the analyzer
  afterwards**, because the analyzer had grown a second, stricter duration
  grammar. That one fails in the expensive direction: both arms built, every
  replicate run, a metered rig — declined over a missing unit suffix, on input
  that cannot be regenerated.

The fix in each case is the same question asked earlier: *what will the report
claim about this value, and has anything checked that the claim is true?* Hence
the ticket-content check, the batch-size floor, and one canonical duration
normalised at pre-flight through the load generator's own grammar. A fourth
finding — corroboration counting values without provenance — is the same shape
one level up: `agreed` is a claim about where the ceiling came from, made by
code that was only checking what it was.

**And the transferable rule: a validator must mirror the grammar of whatever will
consume the value, and it must run before the expensive step.** Stricter than the
consumer rejects completed work; looser lets a bad value through to fail later.
Both are worse than the same grammar, applied early.

---

## 16. The first lesson: a green suite over an unexecuted subject

Two independent reviews found that the **utilization half of the instrument had
no producer** — `ab-throughput.sh`'s inline record validator hard-coded a SINGLE
step record while the driver advertised `--ramp <list>` and the runbook
instructed `--ramp 1,2,4,8`. Every utilization session would have died
`replicate-invalid` **after two release builds, a prewarm and a full measurement
pass**.

A 110-case self-test was green throughout, and could not have been otherwise:
`run_one` needs a rig, so **nothing executed the validator** — it lived as an
inline `python3 - <<'EOF'` heredoc inside a function no test could call. The
suite measured completeness of the *analyzer* and said nothing about the
*driver*, while reading, from its tally, as though it covered both.

This is the repository's own standing question one directory over — *which lane
EXECUTES this?* — and the answer here was **none**. The fix is structural, not
another case: both helpers moved into `ab_driver_support.py` as an **executable
file with subcommands**, and the self-test now drives them with real input,
including the four-step replicate that would have caught it on the first run.

Two rules worth carrying:

- **A helper that cannot be run on its own cannot be tested on its own.** An
  inline heredoc inside a rig-only function is unexecutable by construction, and
  no amount of care around it substitutes for being able to call it.
- **A case count is evidence about the subject it executes, and about nothing
  else.** 110/110 was true and it was not evidence that the driver worked.

---

## 17. A process finding: cadence, not partition

*The sections above are about the artifact. This one is about how we sequenced
the work that produced it, and it is recorded here because this is where the next
person building a measurement harness will be looking.*

This deliverable reached **+5890 lines across three review rounds**, at which point
roborev began delivering the diff by snapshot path — which makes `prompt-content`
FAIL on every round from there and puts the merge behind an owner waiver. Worth
recording how it got that big, because the obvious conclusion is the wrong one.

**The obvious split would have been actively harmful.** Splitting by layer —
analyzer first, driver second — ships a manifest schema that nothing produces.
That is not a missed test; it is a design that *guarantees* an unexecuted subject,
which is precisely the hole §16 (a green suite over an unexecuted subject)
describes. Reflexively partitioning by layer
makes the round-1 defect structural rather than accidental.

**There was one real seam, and it was a requirement-sequencing error rather than
a partitioning one.** The single-stream / utilization split is a genuine seam:
each half is independently useful and each has a producer and a consumer. But the
second quantity arrived as a **requirement change from the lead after the first
build was complete and reviewed**, so it landed as a retrofit into a finished
artifact — and that retrofit is where the dead-producer defect came from. Had
both quantities been in the original brief, "PR 1: single-stream end to end;
PR 2: utilization" would have been clean.

**And a large share of the size is evidence the process worked.** Of the 5890
lines, **1860 are the self-test**, and it roughly tripled across the three rounds
— growth that is a *response* to review and could not have existed in a smaller
first PR. Anyone auditing this by line count should know that before concluding
the deliverable was too big.

**The fix is cadence, not partition.** Get the instrument reviewed once it is
**end-to-end runnable but thin** — one quantity, minimal tests — so review rounds
land on a small diff. This is the repository's review-first doctrine pushed one
step earlier: it already says review before the first full gate; this says review
before the artifact is *complete*. Note what it would have caught here: the four
highest-value findings (the dead utilization path, the #3058 bypass, the
within-pair ordering bias, the ledger ordering) are all **cross-cutting** — they
live in the interaction between driver, analyzer and runbook, so they need the
whole thing present to be visible at all. Reviewing a thin whole finds them;
reviewing a thick layer does not.
