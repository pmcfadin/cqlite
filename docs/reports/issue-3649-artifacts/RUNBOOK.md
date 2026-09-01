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
| What discharges the AC | One completed measurement session whose analyzer output is pasted into #3649, **plus** the two controls in step 4 |

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
- [ ] **`bash docs/reports/issue-3649-artifacts/selftest-analyze.sh` is green.**
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
predicates — `--shape full` uses the template as-is. The driver refuses an absent
or unparseable template before it builds anything.

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
  --replicates 5 --step-duration 60s \
  --server-cpus 0,2 --client-cpus 1,3
python3 analyze-ab.py --manifest /data/ab-3649/control-null/results/manifest.json \
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
  --replicates 5 --step-duration 60s \
  --server-cpus 0,2 --client-cpus 1,3
python3 analyze-ab.py --manifest /data/ab-3649/control-sens/results/manifest.json \
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

## Step 5 — the measurement

```bash
bash ab-throughput.sh \
  --base-ref cfa93fe99^ --head-ref cfa93fe99 \
  --corpus   /data/ab-3649/corpus/sstables \
  --ticket-template ./corpus/ticket.json \
  --work-dir /data/ab-3649/measure \
  --replicates 7 --step-duration 60s \
  --server-cpus 0,2 --client-cpus 1,3 \
  --rows-declared <the corpus row count you recorded> \
  | tee measure-driver.txt
```

**On `--replicates`.** The floor is 3 (the driver refuses below it: a percentile
bootstrap over two pairs reports an interval it cannot support). **5 is the
minimum worth reporting and 7 is the recommendation**, for a reason that is
arithmetic rather than taste: a percentile bootstrap's interval is a resample of
the observed pairs, so at `n = 5` the 2.5th percentile is effectively pinned by
the single most extreme pair, and one outlying replicate moves the whole
interval. At `n = 7` no single pair can do that alone. The cost is linear and
known in advance — `2 × n × (step + prewarm + server start/stop)` — so pick `n`
from your remaining time, and **pick it before you see any result**. Raising
`n` after seeing an inconclusive interval, until it stops being inconclusive, is
not a measurement.

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

```bash
python3 analyze-ab.py \
  --manifest /data/ab-3649/measure/results/manifest.json \
  --profile narrow \
  | tee results/analysis-narrow.txt
echo "exit=$?"
```

Every line is prefixed `AB-3649: `; the verdict is on exactly one
`AB-3649: verdict <TOKEN>` line. **Paste the whole output into #3649** — it is
built to be pasted, and it names its own limits.

| token | exit | what it means | what to do |
|---|--:|---|---|
| `MEETS-TARGET` | 0 | the whole interval lies inside the profile's band | report it; the AC is discharged |
| `ABOVE-TARGET` | 5 | the whole interval lies above the band | report it **against the band**. Do **not** write it up as reaching a 1.5–1.9× ceiling — see below |
| `BELOW-TARGET` | 4 | the whole interval lies below the band; the target is **ruled out** by the data | go to step 7 **before** writing anything that reads as a regression |
| `INCONCLUSIVE` | 6 | the interval overlaps the band without being contained in it | report it **as a non-result**. Do not round it into a number. This is the correct outcome when the box and the effect are the same size |
| `UNMEASURED` | 7 | nothing was measured; the `cause` line on stderr names why | fix the cause and re-run. **Never** read this as a permissive default |

**The ceiling.** The analyzer names 1.5–1.9× on every run and tests against it
never, because `docs/research/phase2-verify-row-engine.md` §3.2 records it as a
rig-narrow **utilization** ceiling, explicitly unmeasured (line 115: "Keep
1.5–1.9× as a **rig-narrow ceiling**, not a field figure"). Testing against it
and falling short would file a phantom regression against #2820, which is a
correct change. There is no verdict token that endorses the ceiling and none may
be added.

**Also run the wide profile** if your corpus has a wide-row table, as a separate
invocation with its own corpus — `--profile wide` tests the 1.05–1.10 band. Do
not run `--profile wide` over a narrow corpus and present it as the wide result.

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
| 1 | `flight-loadgen --shape full` run **server-direct** on the **field i4i narrow rig** (not a lane box), against `main` at/after #2820's merge vs the immediately preceding commit | `ab-throughput.sh` — `--shape full` server-direct over loopback, arms defaulting to `cfa93fe99` / `cfa93fe99^`, run on the `i4i.xlarge` M0 profile per step 1. The manifest records the resolved shas and the host. **Discharged by the session, not by this lane** (`FINDINGS.md` §2). |
| 2 | Report util throughput with **dispersion, not just a point estimate** — CIs or percentiles | `analyze-ab.py` — per-pair ratios, a seeded percentile bootstrap over the pairs, each arm's own mean/median/min/max plus its own interval, and the latency percentiles per arm. The replicate design exists because `flight-loadgen` throughput is a point estimate (`FINDINGS.md` §4). |
| 2b | A point estimate with overlapping CIs is **inconclusive** and must be reported as such, not rounded into a verdict | The `INCONCLUSIVE` token and its rule, pinned by `selftest-analyze.sh` with a fixture whose point estimate sits **inside** the band and whose interval does not. |
| 3 | Corpus large enough that `--shape full` is meaningful. **State the corpus size used.** | `--min-corpus-bytes` (default 256 MiB, refuses below, cause `corpus-too-small`), plus `--min-sstables` for the #3058 trap. The census is in the manifest and is printed on the analyzer's `corpus` lines, so a pasted report always states it. Step 2 of this runbook. |
| 4 | Verdict recorded against **~1.1–1.25× narrow / ~1.05–1.1× wide**, with 1.5–1.9× named as a ceiling | `TARGET_BANDS` + `CEILING_TEXT` in `analyze-ab.py`: the band is printed with its source on every run, the ceiling is named on every run and tested against never, and `selftest-analyze.sh` asserts that no ceiling-endorsing token can appear. |
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
  analyze-ab.py           the paired bootstrap statistics + the closed-set verdict
  selftest-analyze.sh     72 deterministic cases + a case floor; run it first
  host/                   preflight.txt (captured on the rig)
  corpus/                 census, sha256, ticket template, generation recipe
  control-null.txt        step 4a output
  control-sensitivity.txt step 4b output
  results/                manifests, per-replicate JSONL, analysis-*.txt
```

Logs live under `<work-dir>/logs/` on the rig and are **never** read into an agent
context.
