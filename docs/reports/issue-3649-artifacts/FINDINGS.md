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

## 3. The target, and the thing that is not the target

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

`analyze-ab.py` therefore **names** 1.5–1.9× on every single run and **tests
against it never**. A run whose interval lands in that region renders
`ABOVE-TARGET` against the 1.10–1.25 band; there is deliberately no verdict token
that endorses the ceiling, and `selftest-analyze.sh` asserts that no such token
appears in the output.

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

## 7. What this lane delivered instead

| artifact | what it is |
|---|---|
| `ab-throughput.sh` | the interleaved paired A/B driver — two worktrees, two target dirs, fail-closed pre-flight, per-run validation, a manifest rewritten after every completed run |
| `analyze-ab.py` | the paired bootstrap statistics and the closed-set verdict, anchored so it cannot be pasted as a certification |
| `selftest-analyze.sh` | 72 deterministic cases over synthetic fixtures, with a case floor |
| `RUNBOOK.md` | the metered-rig procedure: pre-flight, positive control, the run, the termination contract, and the AC checklist |

**Not delivered, and deliberately so: a number.** The AC is discharged by a rig
session, not by this lane.
