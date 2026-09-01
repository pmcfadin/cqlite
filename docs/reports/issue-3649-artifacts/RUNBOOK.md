# Issue #3649 RUNBOOK — the metered i4i session

**Read this before provisioning anything. Execute it in order. Nothing here
should need re-deriving under time pressure, and nothing here should need a
judgment call that has not already been made below.**

| | |
|---|---|
| Rig | **`i4i.xlarge`**, the M0 server-direct profile: 4 vCPU / **2 physical Ice Lake cores**, **XFS on the i4i NVMe instance store** (`website/src/content/docs/field-validation/m0-throughput.md`, method note) |
| Provisioning | **Fresh instance, run, terminate.** There is no persistent rig and no ssh alias to one. Spend is owner-authorized. |
| Hard stop | Set one **before you start**, write it down, and treat an extension as a fresh ask on #3649 — a cheap ask. The time box exists to stop a forgotten instance, not to rush the measurement. |
| Branch | `issue-3649-measure-2820-merge-fanin` |
| What discharges the AC | **TWO** completed measurement sessions — a single-stream one and a concurrency-ramp one — whose analyzer output is pasted into #3649, **plus** the two controls in step 4 |

## The two quantities. Read this before step 5.

The acceptance criteria carry **two** measurements, and the sources verdict them
differently. Collapsing them is the mistake this runbook exists to prevent.

| quantity | how it is run | how it is verdicted | source |
|---|---|---|---|
| **Single-stream throughput** | `--ramp 1` | against the **~1.1–1.25× narrow / ~1.05–1.1× wide** target band | `docs/research/phase2-verify-row-engine.md` line 107: *"Revised: ~1.1–1.25× narrow single-stream, ~1.05–1.1× wide"* |
| **Utilization throughput** | a concurrency ramp | as a **direction with an interval** — "rises measurably" — and **never** against 1.5–1.9× | `docs/architecture/throughput-program-2026-07.md` line 371 states the M2 criterion as util throughput *"rises measurably toward the 1.5–1.9× ceiling"*; `phase2` line 115: *"Keep 1.5–1.9× as a **rig-narrow ceiling**, not a field figure"* |

So there are **two driver runs and two analyzer sections**, with two separate
verdict lines. The 1.5–1.9× figure is a **rig-narrow utilization ceiling recorded
as unmeasured**; it is named on every run of both sections and **tested against
in neither**. `ab_stats.decide_utilization` is not even *given* a threshold — the
comparison is not expressible, which is stronger than a promise not to make it.
Testing against the ceiling and falling short would file a phantom regression
against #2820, which is a correct change.

**Binding ordering: the two CONTROLS first, the measurement second, the full
`agent-gate.sh` of record LAST.** A gate compiling while you measure invalidates
the measurement, and re-measuring costs metered hours. Export
`CQLITE_GATE_MAX_CONCURRENCY=1` and start no build that is not a measurement
input while a capture window is open.

**Do not compare absolutes against M0.** M0 measured `v0.16.0` against `0.17-dev`
on this profile; this session measures `cfa93fe99^` against `cfa93fe99`. The
design is *both arms re-measured on THIS box, interleaved*, and that
self-consistent pair is the whole deliverable. Any sentence comparing a #3649
absolute to an M0 absolute is a defect.

---

## Step 0 — read `FINDINGS.md` first

It records, with citations, what is already established: the AC's triage step
passes (2/2 at `d23403d1e`), the target-versus-ceiling distinction, why
`flight-loadgen` throughput needs replicates, and — the one that will cost you
the session if you skip it — **§6, the #3058 single-source bypass**.

---

## Pre-flight checklist

- [ ] `date -u` recorded; time remaining against your hard stop written down.
- [ ] Claim ref held: `bash scripts/flow/claim.sh verify 3649`.
- [ ] Worktree on `issue-3649-measure-2820-merge-fanin`; `git log --oneline -5`
      shows this artifact set.
- [ ] **`bash docs/reports/issue-3649-artifacts/selftest-analyze.sh` is green (319 cases).**
      It runs a **complete two-arm, five-pair session end to end** against stub
      `cargo`/`cqlite-flight`/`flight-loadgen` on `PATH`, so a driver that cannot
      complete a session fails here rather than on the rig after both release
      builds — **including the step-4 sensitivity control**, whose deliberately
      asymmetric arms are run end to end. **Declared gap, printed by the suite
      itself: the real binaries are exercised by nothing** — this proves the
      driver's logic, not the server's.
      Seconds, `python3` only, no rig and no root, so there is no excuse for
      skipping it — and it is the cheapest step in this runbook by four orders of
      magnitude. It drives every fail-closed guard with the bad input that guard
      exists to catch, so a guard that has been softened surfaces **here**, before
      you spend metered time producing numbers it would have to certify. Check the
      **case floor** line, not just the tally.
- [ ] Host facts captured to `docs/reports/issue-3649-artifacts/host/`:
      ```bash
      mkdir -p docs/reports/issue-3649-artifacts/host
      { echo "== date =="; date -u
        echo; echo "== instance-type =="
        curl -s --max-time 2 http://169.254.169.254/latest/meta-data/instance-type; echo
        echo; echo "== lscpu ==";     lscpu
        echo; echo "== siblings =="
        for c in /sys/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list; do
          echo "$(basename "$(dirname "$(dirname "$c")")") $(cat "$c")"
        done | sort -V
        echo; echo "== memory ==";    free -g
        echo; echo "== block ==";     lsblk; df -hT /data
        echo; echo "== kernel ==";    uname -a
        echo; echo "== loadavg ==";   cat /proc/loadavg
      } > docs/reports/issue-3649-artifacts/host/preflight.txt 2>&1
      ```
      **Commit this immediately.** If the session dies later, this still stands as
      the record of what was run on.
- [ ] `free -g` leaves room for the corpus page cache after the server's heap.
- [ ] Nothing else is running: no JVM, no gate, no second lane. `nproc` is 4;
      one competing compile is the whole box.
- [ ] Scratch root on the **instance-store NVMe** (`/data`), never the EBS root.
      The default `--work-dir /data/ab-3649` assumes this — check it is on the
      NVMe and not a symlink back to EBS.
- [ ] `taskset`, `python3`, `cargo`, `git`, `curl` present.

---

## Step 1 — the CPU sets, read from sysfs and never assumed

Four hardware threads over two physical cores. Pin the **server** to both threads
of one physical core and the **client** to both threads of the other, so the load
generator never competes with the engine for a core. **Read the sibling map** —
do not assume `(c, c+2)`:

```bash
for c in /sys/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list; do
  echo "$(basename "$(dirname "$(dirname "$c")")") $(cat "$c")"
done | sort -V
```

On the M0-profile `i4i.xlarge` this typically reads `cpu0 0,2` / `cpu1 1,3`,
giving `--server-cpus 0,2 --client-cpus 1,3`. **Use what the map says on your
box.** The driver refuses overlapping sets and refuses one set given without the
other; unpinned is permitted but is recorded as an explicit
`none-unpinned` fact, and is not what this measurement should be run at.

Two physical cores is genuinely narrow, which is the point: the AC says **field
i4i narrow rig**, and the target band is the *narrow single-stream* band.

### The admission ceiling, which you must pin

`cqlite-flight` admits a bounded number of concurrent `do_get` scans (#2420, WS4;
`cqlite-flight/src/cli.rs:59-73`). Past the ceiling a request waits
`--admission-wait-timeout-ms` (server default 30000) and is then **shed with gRPC
`UNAVAILABLE`**, which `flight-loadgen` counts separately as
`requests_unavailable`.

**Unset, the ceiling is DERIVED** — `clamp(2 x hardware threads, 2, 64)`,
honouring the affinity mask and cgroup quota. On a 4-vCPU `i4i.xlarge` that is
**8**, and pinning the server to two hardware threads changes it again. So
unpinned it is a property of *the box and your `taskset`*, not of the experiment,
and two sessions can silently differ.

**Why this is a correctness issue and not tidiness: a ramp step above the ceiling
measures the admission ceiling, not merge throughput — and it looks like a
plateau, which is exactly the shape someone would misread as saturation.**

`ab-throughput.sh` therefore **requires `--max-concurrent-scans`** (usage error
without it), pins it on **both** arms, and **refuses a `--ramp` whose top step
exceeds it**. It then reads the server's own startup line
(`cli::log_startup`, which logs `max_concurrent_scans` **and**
`max_concurrent_scans_source`) and dies on `admission-mismatch` if the resolved
value differs from the requested one, or on `admission-provenance` if the source
is not `flag` — a value we passed and a value the server resolved are different
facts, and only the second is a measurement. An unreadable startup line is
recorded as `NOT-OBSERVED` and disclosed beside the verdict, never assumed to
agree.

**Set the pin at or above the top of your ramp** (`--max-concurrent-scans 16` for
a `1,2,4,8` ramp is comfortable). Any shedding at all means the pin was too low;
the analyzer's exclusion machinery is a backstop for JSONL produced some other
way, not the plan.

After each pass, confirm the pin was not merely *requested* but *observed*: the
analyzer's `admission … corroboration` line is the read-back, and a `partial` or
`none` there is fixable only while the rig is alive. Step 5 says what to do.

Also pinned and recorded on both arms: **`--batch-size` (default 8192)**, because
it is the Arrow record-batch row cap and therefore interacts directly with the
egress batching #2820 changed. `--max-batch-bytes` and
`--admission-wait-timeout-ms` are optional; unset, the server default applies and
the resolved value appears in the captured startup line.

---

## Step 2 — the corpus, and its one hard constraint

### 2a. It must be COMPRESSED, and CQLite cannot generate it

The field is **LZ4** (`docs/architecture/throughput-program-2026-07.md` line 21),
and that document flags *uncompressed* as a **known artifact** of the WS0 loopback
measurements rather than a neutral choice (lines 23, 56, 69). M0's corpus, which
is the corpus this profile is calibrated on:

> LZ4-compressed `cassandra_easy_stress.keyvalue` (`chunk_length_in_kb=16`,
> ~3.4M partitions, ~650 MB Data.db)
> — `website/src/content/docs/field-validation/m0-throughput.md`, method note

**`tools/ws0-corpus-gen` CANNOT supply it.** Verify that yourself before
reaching for it — the generator says so in its own module documentation:

```bash
sed -n '36,41p' tools/ws0-corpus-gen/src/lib.rs
```
```
//! # Uncompressed by construction (issue #1406)
//!
//! CQLite's production write surface emits UNCOMPRESSED SSTables only and never
//! a `CompressionInfo.db`. The generator asserts the absence of that component
//! rather than assuming it.
```

This is the #1406 claim boundary, not a defect. **So the rig needs a real
Cassandra 5.0 to generate the corpus, or the corpus transported to it.** Budget
for that: it is the long pole of the session, and an `i4i.xlarge` generating
~650 MB of `cassandra_easy_stress` data is not a five-minute job. Two routes,
in preference order:

1. **Transport** a previously generated corpus (fastest; check whether the M0 or
   WS0 corpus survives anywhere reachable) and record its provenance + `sha256`.
2. **Generate on the rig** with Cassandra 5.0.8 + `cassandra_easy_stress`,
   `keyvalue` profile, `chunk_length_in_kb=16`, then `flush` + `compact` to a
   known SSTable count. Record the recipe you actually ran and the resulting
   `sha256(Data.db)` — `cassandra_easy_stress` is not byte-deterministic, so the
   accepted bar (the one #3100 and #3217 both used) is **matched geometry plus a
   documented new sha256**.

Why compression matters *for this ratio specifically*, beyond comparability:
#2820's lever is the merge fan-in park/wake tax as a **fraction of server CPU**.
Removing LZ4 decode removes real CPU from the denominator, so an uncompressed
corpus **inflates** the ratio toward the target — biased in the direction that is
hardest to notice.

### 2b. It must have at least TWO SSTables — this is the #3058 trap

Read `FINDINGS.md` §6 in full. The short form: `#3058` gives the Flight row route
a single-source fast path that **never enters the k-way merger**, and it predates
both arms. On a one-SSTable corpus both arms would take that path and the ratio
would be **1.0 by construction**.

The driver defends this twice and you should still check it yourself:

- `--min-sstables` (default **2**) — refuses below it, cause
  `corpus-too-few-sstables`.
- `--merge-path` (default **`merge`**) — exports
  `CQLITE_FLIGHT_MERGE_PATH=merge` into **both** arms' servers.
  `merge` is documented as absolute, "never take the fast path". `bypass` is
  **not** its mirror image — it is *"`auto` with an explicit, assertable name"*
  and never overrides a correctness precondition — so `merge` is the only value
  that guarantees the code under test is the code that ran.

**Record the SSTable count.** It is a first-order parameter of a k-way merge
measurement, and the manifest carries it (`corpus.data_db_files`).

### 2c. The size floor

`--min-corpus-bytes` defaults to **268435456 (256 MiB)** of `*-Data.db`. Basis:
M0's corpus is ~650 MB, and the floor is set well below it so a smaller but still
meaningful corpus is not refused, while remaining ~100× above anything a lane box
holds (the entire fetched lane corpus is **2.4 MB across 155 tables** —
`FINDINGS.md` §2b). Below this floor `--shape full` measures request setup and
gRPC framing, not the read path. If you deliberately run below it, you are
running a control, not a measurement: pass `--control <label>` so the analyzer
says so in its own output.

### 2d. The ticket template

Copy `docs/reports/ws0-3026-artifacts/ws0-h2h/ws0-events-template.json` and edit
the `keyspace` / `table` / `ddl` to match your corpus. Full ring, no `limit`, no
predicates — `--shape full` uses the template as-is.

**The driver now checks the ticket's CONTENT, not just that it is JSON.** The
#3649 target band and the utilization direction are both defined for a full-ring
scan of every column (the AC's first line is `flight-loadgen --shape full`), so a
ticket carrying a `limit`, a `filter`, an `aggregation`, a column projection, a
token bound or `wraparound` is refused with `ticket-not-full-ring` — and
`--shape` other than `full` is a usage error — **before either arm is built**. A
narrowed workload scored against that band is a wrong answer wearing a
right-looking shape.

If you genuinely want another shape, pass `--control <label>`: the ticket check
is then skipped and the analyzer disclaims the verdict on a
`verdict-detail … SHAPE` line rather than scoring it against the band.

**A token range is not automatically a narrowing.** The check mirrors
`FlightTicket::token_in_range`: absent endpoints are the full ring, and **equal**
endpoints are too (wrapping is derived as `start >= end`, so `token > start OR
token <= end` admits everything). An explicit `(i64::MIN, i64::MAX]` is *not* —
half-open drops the token equal to `i64::MIN`, which is a real token (#3633). The
`wraparound` flag is **ignored**, exactly as the server ignores it since #3634;
a template that sets it is accepted.

Commit the template you actually used, and the corpus census, to
`docs/reports/issue-3649-artifacts/corpus/`.

---

## Step 3 — build both arms

The driver does this for you (two `git worktree`s, two `--target-dir`s), but the
first build is the slow part and you may prefer to pay it before the clock is
under pressure. It is idempotent — the driver reuses an existing worktree and
**asserts the worktree is at the expected commit** before building, so a
half-set-up directory is a named refusal (`worktree-wrong-commit`), not a silent
wrong-arm measurement.

```bash
cd docs/reports/issue-3649-artifacts
bash ab-throughput.sh --help          # exits 3 by design; exit 0 means MEETS-TARGET
```

---

## Step 4 — ⛔ THE CONTROL GATE. Nothing proceeds until both controls behave.

The whole reason #3649 exists is that a previous attempt could not tell the
branch from the box. Before producing any number that will be read as an answer,
prove **on this box, with this corpus** that (a) two inert commits do not appear
to differ, and (b) a real difference is detectable at all. Both controls use the
same driver and the same analyzer, so they also exercise the entire pipeline.

### 4a. The NULL control — two inert commits must not appear to differ

`cfa93fe99^` (`674cffa9d`, a one-line telemetry JSONL append) and `cfa93fe99^^`
(`64802eebc`, `scripts/` only). Neither touches `cqlite-core` or `cqlite-flight`,
so the served path is byte-identical between them.

```bash
bash ab-throughput.sh \
  --control null \
  --base-ref cfa93fe99^^ --head-ref cfa93fe99^ \
  --corpus   /data/ab-3649/corpus/sstables \
  --ticket-template ./corpus/ticket.json \
  --work-dir /data/ab-3649/control-null \
  --ramp 1 --replicates 5 --step-duration 60s \
  --max-concurrent-scans 16 --batch-size 8192 \
  --server-cpus 0,2 --client-cpus 1,3
python3 analyze-ab.py --single-stream /data/ab-3649/control-null/run-<session-id>/manifest.json \
  | tee control-null.txt
```

**The gate is quantitative, and it is the rig's noise floor.** Read the
`ratio ... ci95%` line:

| observation | meaning | action |
|---|---|---|
| interval **inside `[0.95, 1.05]`** and covering 1.0 | the box resolves better than ±5%; the narrow band's lower edge is +10%, so a real 1.1–1.25× is separable | **proceed** |
| interval covers 1.0 but is **wider than `[0.95, 1.05]`** | the box's noise floor is comparable to the effect being looked for | **STOP.** More replicates or a longer step may fix it; if they do not, this rig cannot answer #3649 and that is the finding to report |
| interval **excludes 1.0** | two inert commits appear to differ — the harness or the box has a systematic bias | **STOP and report.** Do not proceed to the measurement |
| `UNMEASURED` | read the `cause` line on stderr and fix that | **STOP** |

### 4b. The SENSITIVITY control — a known handicap must be detected

Same two inert commits, with the HEAD arm's server handicapped by a documented
flag rather than a code edit: `--max-batch-bytes 1` is documented to "degrade to
one row per batch", which is a certain, large throughput penalty and is entirely
independent of #2820.

```bash
bash ab-throughput.sh \
  --control sensitivity \
  --base-ref cfa93fe99^^ --head-ref cfa93fe99^ \
  --head-server-extra '--max-batch-bytes 1' \
  --corpus   /data/ab-3649/corpus/sstables \
  --ticket-template ./corpus/ticket.json \
  --work-dir /data/ab-3649/control-sens \
  --ramp 1 --replicates 5 --step-duration 60s \
  --max-concurrent-scans 16 --batch-size 8192 \
  --server-cpus 0,2 --client-cpus 1,3
python3 analyze-ab.py --single-stream /data/ab-3649/control-sens/run-<session-id>/manifest.json \
  | tee control-sensitivity.txt
```

**The gate:** the ratio interval must **exclude 1.0**, well below it. If a
one-row-per-batch handicap is not detectable on this rig, nothing this session
measures about a 1.1–1.25× effect means anything. **STOP and report.**

Both control outputs carry `control <label>` and a
`verdict-detail CONTROL ...` line saying in their own text that they do not
discharge the acceptance criteria — so neither can be pasted as the answer.
**Commit both, plus their manifests and JSONL, before proceeding.**

---

## Step 5 — the measurement, in TWO passes

### 5a. Single-stream (`--ramp 1`) — the quantity the target band applies to

```bash
bash ab-throughput.sh \
  --base-ref cfa93fe99^ --head-ref cfa93fe99 \
  --corpus   /data/ab-3649/corpus/sstables \
  --ticket-template ./corpus/ticket.json \
  --work-dir /data/ab-3649/measure-single \
  --ramp 1 --replicates 8 --step-duration 60s \
  --max-concurrent-scans 16 --batch-size 8192 \
  --server-cpus 0,2 --client-cpus 1,3 \
  --rows-declared <the corpus row count you recorded> \
  | tee measure-single-driver.txt
```

### 5b. Utilization (a concurrency ramp) — the quantity the ceiling relates to

```bash
bash ab-throughput.sh \
  --base-ref cfa93fe99^ --head-ref cfa93fe99 \
  --corpus   /data/ab-3649/corpus/sstables \
  --ticket-template ./corpus/ticket.json \
  --work-dir /data/ab-3649/measure-util \
  --ramp 1,2,4,8 --replicates 8 --step-duration 60s \
  --max-concurrent-scans 16 --batch-size 8192 \
  --server-cpus 0,2 --client-cpus 1,3 \
  --rows-declared <the corpus row count you recorded> \
  | tee measure-util-driver.txt
```

On the ramp: it must top out **at or below** `--max-concurrent-scans` (the driver
refuses otherwise) and, on a 2-physical-core server pin, `1,2,4,8` already runs
well past the core count — which is the point of a utilization curve. The
comparison quantity is the **peak `rows_per_s` over the surviving ladder**, and
the analyzer requires the two arms of each pair to have the **same** surviving
ladder. Each pass costs `2 × replicates × (steps × step-duration + prewarm +
server start/stop)`, so budget 5b at roughly four times 5a for a four-step ramp.

### The server runs in a controlled environment

Each `cqlite-flight` is launched under `env -i` with a **named allowlist**
(`PATH`, `HOME`, `TMPDIR`, `RUST_LOG=info`, `RUST_BACKTRACE=1`,
`CQLITE_FLIGHT_MERGE_PATH`) and nothing else. Two reasons, both of which turn an
exported variable on the rig into a wrong or dead session:

- every `CQLITE_*` variable the server honours — `CQLITE_MAX_BATCH_BYTES`,
  `CQLITE_ADMISSION_WAIT_TIMEOUT_MS`, `CQLITE_MAX_INFLIGHT_EGRESS_BYTES` — is a
  **silent override of a value the manifest claims to record**;
- an inherited `RUST_LOG=warn` suppresses the INFO readiness line, so **every**
  server would time out having already bound its port.

It is an allowlist and not a denylist for the reason `gate-detached.sh` learned
the hard way: a list of *remembered* variables fails silently. If you need the
server to see something else, add it to that list in the driver with its reason
beside it — do not export it and hope.

### The port is ephemeral, and readiness comes from the server's own log

`--port` defaults to **0**: each server binds an ephemeral port and the driver
learns the real one from that server's own post-bind `listening on` line
(`cqlite-flight/src/cli.rs:228-241`, which is emitted only once a listener
exists). There is no fixed port, so two sessions on one box cannot collide, and
no probe — "something answered on 8815" was never the same claim as "my server
owns 8815", and on a nine-lane box the difference is one session measuring
another's binary. `--port <n>` remains available if you need a fixed one.

### ONE load generator drives both arms

`--loadgen-ref` (default: the HEAD arm's ref) pins the single `flight-loadgen`
both arms are driven by. Building it per arm would make the **client** vary with
the server commit, so a client-side change between the two refs would be
attributed to server throughput — a confound no dispersion reporting could
reveal, because both arms would be internally consistent. Only the server
legitimately differs per arm. The provenance is recorded in the manifest and per
run, and the analyzer refuses a session whose runs name more than one.

If you change this, the question to answer first is the general one: **list what
differs between the arms and give a reason for each; anything on that list
without one is a confound.**

### The floors are floors

`--min-corpus-bytes` (256 MiB) and `--min-sstables` (2) are **not lowerable for a
measurement**. Both the driver and the analyzer enforce the documented minimums,
and the analyzer deliberately ignores the thresholds the manifest records — a
verdict must not derive its validity from a number its own session chose. Lower
them only under `--control <label>`, where the verdict is disclaimed.

This is the third distinct route by which #3058's single-source bypass has been
reachable: a recursive census, a symlinked decoy, and simply passing
`--min-sstables 1`. If you find yourself wanting to lower a floor to make a
session run, the session is not a measurement.

### The census describes the SERVED table, not the disk

`--min-corpus-bytes` and `--min-sstables` are claims about the one directory the
ticket resolves to, enumerated the way the server enumerates it — flat, in
`<data>/<keyspace>/<table>[-<uuid>][/snapshots/<name>]`. Unrelated tables,
snapshot subtrees and hard-linked copies elsewhere under `--data-dir` are
deliberately **not** counted, because a green census over files the server never
opens would let a single-source served table through the #3058 guard, which is
the phantom that guard exists to stop. The run prints the resolved `served-dir`;
check it is the table you meant.

### What the driver refuses before it builds anything

`--work-dir` may be relative; it is canonicalised before anything is derived from
it. All of the following are usage errors or named aborts that cost you seconds,
not a session: `--shape` other than `full` without `--control`; a ticket that narrows
the scan; `--batch-size 0` (the server clamps it to one row per batch, so the
manifest would not record what ran); a `--step-duration` `flight-loadgen` would
itself reject; a `--ramp` that is not strictly increasing or maps to no analyzer
section; `--rows-declared` with separators; and **any arm's RESOLVED configuration** being
out of range — the batch-size floor is checked on the value each arm will
actually be given, so per-arm extras cannot route around it. The step duration is **normalised at
pre-flight through the same grammar the load generator uses**, so a value it
accepts — including a bare `60`, which means seconds — can never be refused later
by the analyzer once the data exists.

**Re-using a work directory is safe now, and not because of sequencing.** Every
session writes to its **own** directory, `<work-dir>/run-<session-id>/`, which no
other session can name — so nothing is ever promoted, truncated or overwritten,
and an earlier session's results stay byte-identical whatever happens to this
one. The work-directory lock still exists, but it is now a
**measurement-validity** guard rather than a data one: two sessions on one box
contend for CPU and invalidate each other's numbers. A session refused by the
lock leaves nothing behind at all.

### ⏱ Check the corroboration line after EACH pass, not at the end

Run the analyzer once against each pass as soon as that pass finishes — before
you start the next one — and read this line:

```
AB-3649: admission max-concurrent-scans requested 16 observed 16 corroboration agreed (14 of 14 runs) ...
```

**Anything other than `corroboration agreed (N of N runs)` is actionable RIGHT
NOW AND ONLY RIGHT NOW.** `partial` or `none` means the driver could not read the
resolved ceiling back out of some or all of the servers' `cqlite-flight starting`
lines. Diagnosing that takes minutes while the box is up — look at
`<work-dir>/logs/<arm>-r<NN>.server.log`, confirm the line is there and that
`ab_driver_support.py parse-startup` can read it — and it becomes **impossible
the moment the instance is terminated**, because the logs go with it. A session
finished and torn down with `partial` corroboration cannot be repaired; it can
only be re-run, and there is no rig to re-run it on.

**Be clear about what it does and does not mean.** It means **less
corroboration**, not evidence that the two arms disagreed. The requested ceiling
is one manifest-level value passed identically to both arms, and the driver
already **dies** on any per-run `observed ≠ requested` it *can* read, refuses a
ramp topping out above the requested pin, and dies on any shed at single-stream
concurrency. So the thing corroboration guards against is separately caught; what
a `NOT-OBSERVED` run costs you is the independent confirmation, and the report
says so rather than quietly reducing to the one value it did see.

**Prefer an EVEN `--replicates`.** The within-pair order alternates with
replicate parity — base first on odd replicates, head first on even — because
interleaving *across* replicates controls drift between pairs but does nothing
about a gradient *within* one: if base always ran first, a thermal ramp or a
neighbour's job starting mid-pair would land on the head arm every single time
and bias every ratio in the same direction, arriving with a tight interval, which
is worse than a noisy one. An even count runs each ordering exactly half the
time; an odd count leaves one pair unbalanced, and the analyzer discloses that
residual on a `COUNTERBALANCE` line rather than hiding it. **8 is the
recommendation**; 7 is fine if time is short, with the residual reported.

**On `--replicates` — the floor is 5, and the reason is arithmetic, not taste.**

A percentile bootstrap resamples the observed pairs with replacement, so its
lower bound is draw number `ceil(0.025 × 10000) = 250` of the sorted draws. The
**all-minimum resample** — every one of the `n` draws landing on the smallest
ratio — has probability `1/nⁿ`. At `n = 3` that is **1/27 = 3.7%, which exceeds
the 2.5% tail**, so the 2.5th percentile of the draws *is* `min(ratios)` and the
97.5th *is* `max(ratios)`:

> At `n ≤ 3` the reported "95% confidence interval" **is the observed range**.
> The bootstrap contributes nothing, coverage is far below 95%, and three
> identical pairs produce a **zero-width** interval that lands inside any band
> containing them — i.e. an automatic `MEETS-TARGET`.

At `n = 5` the all-minimum resample has probability 1/3125, so about 3 of 10000
draws — nowhere near the 250 needed to reach the bound. The floor is therefore
**5** in both the driver (`--replicates`) and the analyzer (`--min-pairs`), and
**7 remains the recommendation**: at `n = 5` a single outlying replicate still
moves the bound noticeably, and at `n = 7` no single pair can.

This is enforced by **measurement, not by a magic number**: `analyze-ab.py`
refuses with `bootstrap-degenerate` whenever the interval it computed is exactly
`(min(ratios), max(ratios))`, so the guard keeps working if someone changes the
resample count, the tail or the floor — and it also catches all-identical ratios,
which are degenerate at any `n`.

**Declared residual, because it is real:** even at `n = 5–7` a percentile
bootstrap of a geometric mean *under-covers* — a nominal 95% interval is
narrower than 95% in truth for small samples. The degeneracy refusal removes the
pathological case, not the general small-sample optimism. Read a marginal
`MEETS-TARGET` at `n = 5` accordingly, and prefer more replicates over a
narrower-looking result.

The cost is linear and known in advance — `2 × n × (steps × step-duration +
prewarm + server start/stop)` — so pick `n` from your remaining time, and **pick
it before you see any result**. Raising `n` after seeing an inconclusive
interval, until it stops being inconclusive, is not a measurement.

**On `--step-duration`.** Long enough that a full-ring scan completes several
times per step. Check the driver's per-run line: `requests-ok` should be
comfortably above 1. A step in which one scan barely completes reports a
throughput that is mostly step-boundary quantization.

Everything is interleaved `base r1, head r1, base r2, …`; the driver rewrites the
manifest after **every** completed run, so if the session dies you still have a
truthful short manifest — and the analyzer will refuse it with
`replicate-shortfall` rather than analysing a short session as a complete one.

---

## Step 6 — the analysis, and the verdict

**Each session writes to its own directory**, `<work-dir>/run-<session-id>/`,
which nothing else can name — so no session can ever overwrite another's results,
and a failed attempt leaves every earlier one byte-identical. The driver prints
the exact analyzer invocation on its `next` line when it finishes; copy it rather
than composing the path. `<work-dir>/latest` is a convenience symlink to the most
recent completed session and is deliberately **not** what you certify.

One invocation, both manifests, two clearly separated sections:

```bash
python3 analyze-ab.py \
  --single-stream /data/ab-3649/measure-single/run-<session-id>/manifest.json \
  --utilization   /data/ab-3649/measure-util/run-<session-id>/manifest.json \
  --profile narrow \
  | tee analysis.txt
echo "exit=$?"
```

Every line is prefixed `AB-3649: `. Each section is bracketed by
`==== section <quantity> ====` / `---- end section <quantity> ----` and carries
exactly one `AB-3649: verdict <quantity> <TOKEN>` line, so the two can never be
confused for one another. **Paste the whole output into #3649** — it is built to
be pasted, and it names its own limits.

Handing a manifest to the wrong section is a named refusal
(`mode-manifest-mismatch`), not a silently wrong answer: a `--ramp 1` manifest is
rejected by `--utilization` and a ramp manifest by `--single-stream`.

### Read the `admission … corroboration` line before you read any verdict

Deliberately repeated from step 5, because this is the other moment you will be
looking at it and the window to act on it is still open only if the rig is still
up:

| corroboration | what it says | what to do |
|---|---|---|
| `agreed (N of N runs)` | every server's startup line was read, every one reported the ceiling you requested, **and every one reported it came from the flag** (`max_concurrent_scans_source=flag`) | nothing; proceed |
| `partial (M of N runs)` | the ceiling was read back for some runs and not others; the ones read agree | **fix the startup-log parse and re-run the affected pass, while the box is up.** Do not terminate first |
| `none (0 of N runs)` | no startup line was readable at all — usually the whole parse is broken, not one run | as above, and suspect the server log format or `--merge-path`/flag plumbing before anything else |

A **value without a `flag` provenance does not count toward corroboration** — a
numeric ceiling that the server says it *derived* is not evidence that the pin
you passed took effect. And an explicitly non-`flag` source (`env`, `derived`,
`derived-fallback`) is not a downgrade at all: it is a refusal
(`admission-provenance`), because the run was served under a configuration this
session did not choose. Check for a stray `CQLITE_MAX_CONCURRENT_SCANS` in the
environment if you see it.

**The analyzer carries this in its own output, so you do not need this page in
front of you** — and neither does whoever reads the block after you paste it. It
prints the state on a `verdict-detail … ADMISSION` line and the fix on a separate
`verdict-detail … ADMISSION-REMEDY` line, **whose first action differs by state**:
with `partial`, some lines parsed, so the format is fine and the fault is
specific to the runs that did not report; with `none`, nothing parsed anywhere,
so the subject is the parse or the log format itself and no individual run is.
That split is deliberate — a shared remedy would send an operator looking in the
wrong place, the same reason the gate-pin verdict distinguishes `NOT-HONOURED`
from `default`.

It does **not** withhold a verdict for any of this: partial corroboration is a
reduction in independent confirmation, not evidence that the arms were served
differently — a genuine disagreement is caught affirmatively by the driver, which
dies on any per-run `observed ≠ requested` it can read. Treat it as a defect in
**the measurement record**, which you can still repair, rather than a defect in
the measurement.

### The single-stream section — verdicted against the band

| token | exit | what it means | what to do |
|---|--:|---|---|
| `MEETS-TARGET` | 0 | the whole interval lies inside the profile's band | report it |
| `ABOVE-TARGET` | 5 | the whole interval lies above the band | report it **against the band**. Do **not** write it up as reaching a 1.5–1.9× ceiling |
| `BELOW-TARGET` | 4 | the whole interval lies below the band; the target is **ruled out** by the data | go to step 7 **before** writing anything that reads as a regression |
| `INCONCLUSIVE` | 6 | the interval overlaps the band without being contained in it | report it **as a non-result**. Do not round it into a number. This is the correct outcome when the box and the effect are the same size |
| `UNMEASURED` | 7 | nothing was measured; the `cause` line on stderr names why | fix the cause and re-run. **Never** read this as a permissive default |

### The utilization section — a direction, and nothing else

| token | exit | what it means |
|---|--:|---|
| `RISES` | 0 | the whole interval lies above 1.0: utilization throughput rose measurably. **This is the M2 criterion.** It is a direction; it claims no attainment of the ceiling |
| `FALLS` | 4 | the whole interval lies below 1.0 |
| `INCONCLUSIVE` | 6 | the interval covers 1.0; no direction is established |
| `UNMEASURED` | 7 | as above |

**No token in this section can express having met the 1.5–1.9× ceiling, and the
rule that produces it is never given the figure to compare against.** The
interval itself is the "toward" in "rises measurably toward"; a reader who wants
to know how far can read the numbers. The report does not compute that
comparison, deliberately.

With both sections present the process exit is the **larger** of the two, so the
least affirmative outcome governs. One unusable session never suppresses the
other — each section is analysed independently.

**Also run the wide profile** if your corpus has a wide-row table, as a separate
invocation with its own corpus — `--profile wide` tests the 1.05–1.10 band on the
single-stream section. Do not run `--profile wide` over a narrow corpus and
present it as the wide result.

---

## Step 7 — triage BEFORE filing anything

The AC requires it, and it has already been done once in-lane and recorded in
`FINDINGS.md` §1 — **re-run it on the rig anyway**, because a rig session is
allowed to discover a real regression and the oracle is what separates
"the mechanism broke" from "the mechanism works and the served-path effect is
small":

```bash
cargo test -p cqlite-core --test issue_2820_merge_fanin_batch
# expected: 2 passed
```

Then, before any regression language reaches #3649, confirm all four:

- [ ] the oracle passes (the send-count reduction is intact);
- [ ] `merge-path merge` appears in the analyzer output (the k-way merge actually
      ran — `FINDINGS.md` §6);
- [ ] `corpus data-db-files` is ≥ 2 and the corpus is compressed;
- [ ] the admission line reads **`corroboration agreed (N of N runs)`** with the
      observed value equal to the requested one, and no `excluded-step` line
      appears (nothing was admission-shed — `FINDINGS.md` §7). A `partial` or
      `none` corroboration does **not** block the verdict, but it does block
      *this* gate: say so in the write-up rather than filing a regression on a
      record you could not fully corroborate;
- [ ] both controls in step 4 behaved.

If all four hold and the verdict is `BELOW-TARGET`, the finding is *"the
served-path effect on this rig is smaller than the estimated band"* — which is a
legitimate, publishable result about an **estimate flagged unmeasured in the
source that produced it**. It is **not** a regression, and #2820 is not at fault.
A well-measured negative is a successful outcome.

---

## Step 8 — commit the artifacts, report, then gate

1. Copy into `docs/reports/issue-3649-artifacts/`: `host/preflight.txt`,
   `corpus/` (census, sha256, ticket template, generation recipe),
   `control-null.txt`, `control-sensitivity.txt`, `results/` (every manifest,
   every `*.jsonl`, `analysis-*.txt`). **Logs stay on the box** and are never read
   into an agent context.
2. Post the analyzer output to #3649, with the corpus size stated (the AC
   requires it — the analyzer prints it on the `corpus` lines).
3. **Only now** run the gate of record, with nothing else competing:
   ```bash
   export CQLITE_GATE_MAX_CONCURRENCY=1
   AGENT_GATE_SUMMARY_FILE=/tmp/gate-3649.txt bash scripts/agent-gate.sh \
     > /tmp/gate-3649.log 2>&1 < /dev/null
   cat /tmp/gate-3649.txt
   ```

---

## Step 9 — the termination contract, with read-back

`terminate-instances` alone is **not sufficient**: the tooling keeps the `/data`
EBS volume on terminate and **it bills until deleted**.

- [ ] Every artifact you intend to keep is **committed and pushed** — the
      instance store is gone the moment the instance is.
- [ ] Terminate the instance.
- [ ] **Delete the `/data` EBS volume.**
- [ ] If you ran the driver against a **persistent checkout** rather than a
      throwaway rig, remove the two build worktrees it registered — the run names
      them on its last line. They outlive the process and a killed session leaves
      them behind.
- [ ] **Read back that BOTH are gone**: the volume listing shows no lingering
      data volume, and the instance shows `shutting-down`/`terminated`.
      **A state written but not observed is not a state.**
- [ ] Paste the read-back into #3649.

### If the clock runs out

Terminate and report what you have. Partial results with an honest boundary are
worth more than a rushed session: commit every artifact captured so far, state
exactly which acceptance criteria are discharged and which are not, and post that
list. An `UNMEASURED` or `INCONCLUSIVE` with the controls committed beside it is
a real contribution; a number produced under time pressure on an unvalidated rig
is the phantom this issue exists to prevent.

---

## The acceptance criteria, mapped item by item

| # | AC (from issue #3649) | artifact that satisfies it |
|---|---|---|
| 1 | `flight-loadgen --shape full` run **server-direct** on the **field i4i narrow rig** (not a lane box), against `main` at/after #2820's merge vs the immediately preceding commit | `ab-throughput.sh` — `--shape full` server-direct over loopback, arms defaulting to `cfa93fe99` / `cfa93fe99^`, run on the `i4i.xlarge` M0 profile per step 1, **twice**: `--ramp 1` (5a) and a concurrency ramp (5b). The manifest records the resolved shas, the host, and the pinned admission ceiling. **Discharged by the session, not by this lane** (`FINDINGS.md` §2). |
| 2 | Report util throughput with **dispersion, not just a point estimate** — CIs or percentiles | `analyze-ab.py` — per-pair ratios, a seeded percentile bootstrap over the pairs, each arm's own mean/median/min/max plus its own interval, and the latency percentiles per arm. The replicate design exists because `flight-loadgen` throughput is a point estimate (`FINDINGS.md` §4). |
| 2b | A point estimate with overlapping CIs is **inconclusive** and must be reported as such, not rounded into a verdict | The `INCONCLUSIVE` token and its rule, pinned by `selftest-analyze.sh` with a fixture whose point estimate sits **inside** the band and whose interval does not. |
| 3 | Corpus large enough that `--shape full` is meaningful. **State the corpus size used.** | `--min-corpus-bytes` (default 256 MiB, refuses below, cause `corpus-too-small`), plus `--min-sstables` for the #3058 trap. The census is in the manifest and is printed on the analyzer's `corpus` lines, so a pasted report always states it. Step 2 of this runbook. |
| 4 | Verdict recorded against **~1.1–1.25× narrow / ~1.05–1.1× wide**, with 1.5–1.9× named as a ceiling | `ab_stats.TARGET_BANDS` + `decide_single_stream` for the band, printed with its source in the single-stream section; `CEILING_TEXT` names 1.5–1.9× in **both** sections. `decide_utilization` takes no threshold argument, so a comparison against the ceiling is not expressible, and `selftest-analyze.sh` asserts that no ceiling-attainment token appears in either section and that neither section can emit the other's tokens. |
| 4b | The M2 criterion — util throughput *"rises measurably toward the 1.5–1.9× ceiling"* (`throughput-program-2026-07.md:371`) | The utilization section's `RISES` token: a direction with an interval, exit 0. Run 5b. |
| 4c | Admission control must not be mistaken for saturation (#2420) | `--max-concurrent-scans` is required and pinned on both arms, the ramp is refused above it, the resolved value **and its provenance** are read back from the server's own startup line, a mismatch between arms is `UNMEASURED`, and any shed step is excluded with the exclusion reported as an explicit fact. |
| 5 | If below target, triage **before** filing a regression: confirm the send-reduction oracle still passes | Already run and recorded in `FINDINGS.md` §1 (2 passed at `d23403d1e`); re-run on the rig as step 7, which also checks the three conditions the oracle alone does not cover. |

### What this artifact set does NOT discharge

The measurement itself. The rig and the corpus are both absent from the lane that
built this, and no number was produced here — deliberately. See `FINDINGS.md` §2.

---

## Artifact layout

```
docs/reports/issue-3649-artifacts/
  RUNBOOK.md              this file
  FINDINGS.md             what is established in-lane, with citations
  ab-throughput.sh        the interleaved paired A/B driver
  analyze-ab.py           the CLI and the two-section report
  ab_stats.py             the statistics and BOTH verdict rules, with their citations
  ab_input.py             manifest/JSONL loading; every refusal, named
  ab_common.py            the anchored, sanitized emission every module writes through
  ab_driver_support.py    the driver's ramp/record validators and startup parser,
                          as an EXECUTABLE FILE so the self-test can drive them
  selftest-analyze.sh     319 cases incl. full sessions under PATH shims; run it first
  host/                   preflight.txt (captured on the rig)
  corpus/                 census, sha256, ticket template, generation recipe
  control-null.txt        step 4a output
  control-sensitivity.txt step 4b output
  results/                both manifests, per-replicate JSONL, analysis.txt
```

Logs live under `<work-dir>/logs/` on the rig and are **never** read into an agent
context.
