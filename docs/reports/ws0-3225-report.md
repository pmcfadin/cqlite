# Peak concurrency by server width, and what the shipped `--max-concurrent-scans` default costs — CQLite #3225

**Measured 2026-08-07 on one AWS box** (Intel Xeon Platinum 8488C, 16 logical / **8 physical
cores**, SMT on, 1 NUMA node, 30 GB RAM, 295 GB NVMe at `/data`), Ubuntu 24.04.4, kernel
`6.17.0-1019-aws`, KVM guest. Same machine and machine class as #3217, which is what makes this a
direct extension of that curve rather than an analogy.

This round exists because **#3217's ramp stopped at N=16 and the shipped default is 64**
(`cqlite-flight/src/admission.rs:53`, `DEFAULT_MAX_CONCURRENT_SCANS = 64`). #3217 therefore never
measured the default at any width, and two of its four peaks sat at the top of its ramp — censored,
i.e. lower bounds. This round re-measures five widths (S ∈ {1, 2, 3, 4, 6} physical cores) over the
ramp `1,2,4,8,16,24,32,64`, **with the shipped default inside the ramp**, 3 reps × 120 s per point,
126 measured points, so `clamp(2 × P, 2, 64)` (`design.md` D2) can be evaluated against a **measured
peak per width** instead of against two uncensored points.

Raw artefacts: `docs/reports/ws0-3225-artifacts/`. **Every number in this report is emitted by
`run/analyze-3225.py` and committed at `ws0-3225-artifacts/results/analysis-3225.{txt,json}`**; none
is hand-computed. Re-derive with:

```bash
python3 docs/reports/ws0-3225-artifacts/run/analyze-3225.py docs/reports/ws0-3225-artifacts/results
```

which reproduces the committed analysis byte-for-byte apart from two provenance lines that
record WHEN it ran: `results:` and the bracketed seal's `seal-measured-after-the-last-arm`
(§2.6). No measured value moves.

**Measurement round (tasks.md §2). It GATES the default flip (§3) and does not perform it.**

---

## 1. Executive summary — three findings, one of which corrects the issue

### Finding 1 — the peak moves with width, and the extension resolves both of #3217's censored points

| S (physical cores) | P (hw threads) | measured peak N | rows/s at peak | dispersion | server util at peak | #3217 said |
|---:|---:|---:|---:|---:|---:|:--|
| 1 | 2 | **2** | 240,693 | 3.2% | 0.994 | 2 — **reproduced** |
| 2 | 4 | **8** | 432,360 | 1.1% | 0.996 | 8 — **reproduced** |
| 3 | 6 | **12** (see §5) | 624,848 | 0.3% | 0.995 | not measured |
| 4 | 8 | **16** | 815,748 | 0.5% | 0.994 | 16 (censored) — **survives extension** |
| 6 | 12 | **24** | 1,173,759 | 3.0% | 0.989 | 16 (censored) — **was a CENSORING ARTIFACT** |

No peak in this round sits at the top of the ramp: every width's curve turns over inside `1..64`, so
**no peak here is censored**. The two censored points of #3217 resolve in *opposite* directions, and
that is the whole value of extending the ramp:

- **S=4's "16" was real.** Extending past it, N=24 falls to 784,934 (−3.8%). 16 is a true optimum.
- **S=6's "16" was an artifact of the ramp stopping at 16.** The true peak is **N=24**
  (1,173,759 rows/s), +8.0% above N=16. `design.md` §4 predicted exactly this.

Stated the general way: a peak at the ramp top is a *lower bound*, and a lower bound is not a
finding. Half of them here moved when the bound was lifted.

### Finding 2 — the shipped default (64) is suboptimal at EVERY measured width, including the widest

| S | peak N | rows/s at peak | rows/s at N=64 | throughput cost | p50 multiple |
|---:|---:|---:|---:|---:|---:|
| 1 | 2 | 240,693 | 188,914 | **−21.5%** | 41.95× |
| 2 | 8 | 432,360 | 373,263 | **−13.7%** | 9.51× |
| 3 | 16 | 624,556 | 561,995 | **−10.0%** | 4.55× |
| 4 | 16 | 815,748 | 734,228 | **−10.0%** | 4.57× |
| 6 | 24 | 1,173,759 | 1,087,912 | **−7.3%** | 2.94× |

Every row is a **within-arm** comparison (one run, one arm, 3 reps per N), which is why the S=3 row
is stated against that arm's own peak N=16 rather than against the width's true peak N=12 — N=12 was
measured in the supplement, so a comparison to it would be cross-run. §5.3 gives that figure with its
caveat; it is *larger* than −10.0%, so this table understates the S=3 cost.

**This corrects the issue's framing.** The issue body reasons that 4- and 6-core servers are
"already optimal at the default" — a premise that is true only of the *misidentified* default of
**16**, which is #3217's ramp top, not the shipped constant. Against the actual shipped default of
64, **nothing is optimal**: the cost is 7–22% of throughput and a 2.9–42× per-scan p50 inflation,
monotonically worse the narrower the server. The narrow-server case is the worst case, but it is not
the only case.

The issue's headline **16.4%-at-1-core is CONFIRMED as a lower bound, and the confirmation is about
the DEFAULT, not about the number 16.4**. Stated precisely, because the distinction matters:

- At the **same N** the issue's figure came from (N=16, #3217's last measured point), this round
  measures **−13.3%** (208,696 vs 240,693) — the same shape, 3.1 pp shallower. That gap is a
  cross-round difference (regenerated corpus, different run; cf. the ~1.7% within-rig cross-run
  offset in §5), so **16.4% does not reproduce to the decimal and this report does not claim it
  does.**
- At the **actual shipped default** (N=64, which #3217 never measured) the cost is **−21.5%**. That
  is the claim "lower bound" was making, and it holds by a wide margin: the true 1-core cost of the
  default is about **5 pp worse** than the figure the issue quotes.

The latency pair the issue quotes as "31 s → 302 s" reproduces as **32.2 s (peak, N=2) → 301 s
(N=16)** and extends to **1,350.6 s (22.5 min) at N=64**.

### Finding 3 — `clamp(2 × P, 2, 64)` holds at all five widths, and the one out-of-fit width CONFIRMS it

| S | P | predicted N | measured peak N | deviation vs that width's peak | vs the constant 64 |
|---:|---:|---:|---:|---:|---:|
| 1 | 2 | 4 | 2 | **−5.0%** (the accepted minimax-regret miss, D2) | +16.5 pp |
| 2 | 4 | 8 | 8 | **exact (+0.0%)** | +13.7 pp |
| 3 | 6 | **12** | 12 | **+1.73% vs N=16, within-run** (§5) | see §5 |
| 4 | 8 | 16 | 16 | **exact (+0.0%)** | +10.0 pp |
| 6 | 12 | 24 | 24 | **exact (+0.0%)** | +7.3 pp |

Three exact hits, one −5.0% miss that `design.md` D2 predicted at −4.8% and accepted in advance, and
— at the width specced as the **out-of-fit falsification test** (S=3, P=6, a width no fit point
touched) — the predicted N=12 **beats** the value the coarse ramp had called that width's peak. The
formula is better than the constant 64 at every width, by 7.3 to 16.5 percentage points. **tasks.md
§2's gate on §3 is MET at every measured width.**

### What this round does NOT establish

- **No non-SMT arm was run** — no non-SMT host was available. The hardware-thread basis therefore
  remains unvalidated on a machine where logical == physical, exactly as `design.md` D3 residual 2
  states. See §9.1.
- **No 8-physical-core width.** 6 physical cores / 12 threads is the widest configuration **in
  scope**, and the reason is the rig, not the hardware (§2.2).
- **Cross-run absolute comparisons carry ~1.7% uncertainty** on this rig — measured, not assumed
  (§5). Every peak determination above is a *within-arm* comparison and is unaffected.

---

## 2. Method

### 2.1 Environment and verified topology

| | |
|---|---|
| CPU | Intel(R) Xeon(R) Platinum 8488C — 8 physical / 16 logical, SMT on, 1 NUMA node |
| SMT siblings | `(c, c+8)`, uniform for every core — **read** from `/sys/devices/system/cpu/cpu*/topology/thread_siblings_list`, never assumed; 8 distinct sibling groups; the `P = 2 × S` relation is derived from those lines |
| RAM / disk | 30 GB / 295 GB NVMe (`/dev/nvme1n1`, `rotational=0`) at `/data` |
| Kernel / OS | `6.17.0-1019-aws` / Ubuntu 24.04.4 LTS, KVM guest (no guest-controllable cpufreq governor) |
| Cassandra | **NOT RUNNING** during any measurement (verified: no `CassandraDaemon` process) |

Full probe output, including competing load and free space: `ws0-3225-artifacts/rig/rig-verification.txt`
(generated by `rig/verify-rig.sh`).

### 2.2 Core allocation, and why 6 physical cores is the widest width IN SCOPE

Both SMT siblings of a physical core are always pinned together. `sweep.sh` **refuses to run** when
the server and client CPU sets overlap, because a shared CPU makes `perf stat -C <server-cpus>`
count client work as engine work.

| arm | server CPUs | physical cores S | hw threads P |
|---|---|---:|---:|
| `cn3225-s1` | `2,10` | 1 | 2 |
| `cn3225-s2` | `0,2,8,10` | 2 | 4 |
| `cn3225-s3` (+ `cn3225-s3-n12supp`) | `0-2,8-10` | 3 | 6 |
| `cn3225-s4` | `0-3,8-11` | 4 | 8 |
| `cn3225-s6` | `0-5,8-13` | 6 | 12 |
| client (constant, every arm) | `6,7,14,15` | 2 | 4 |

**6 of 8 physical cores is the widest configuration in scope, and it is NOT "the whole box".** The
client needs **2 exclusive physical cores on this same box**, and `sweep.sh` refuses an overlapping
set, so 8 server cores would require a second machine for the client — a rig change (network path,
new validity gate, new client-headroom baseline). The refusal is **demonstrated, not trusted**:
`rig-verification.txt` §5 records it firing with exit 1 and the diagnostic
`server and client CPU sets overlap on {5,13}` for a deliberately overlapping set, and the
non-overlapping S=3 literal-CPU-list form passing the same check.

That committed record is the pre-sweep capture (`2026-08-06T22:51Z`, three minutes before the first
arm) and is deliberately **not** regenerated: its competing-load section is only meaningful at sweep
time. `verify-rig.sh` has since been strengthened — a non-firing guard now makes it **exit non-zero**
instead of printing a failure and exiting 0, and the S=3 control now requires affirmative evidence
that execution reached past the overlap gate rather than merely lacking the diagnostic. Neither
changes any field of that run: both verdicts in the committed record are the passing ones.

The client is a **constant** across every width, so no arm buys throughput by taking client cores.
Measured client utilisation of its pinned set never exceeded **0.131** (S=6, N=24) against the
harness's 0.70 saturation gate — ~5× headroom — and **zero** points were excluded as
client-saturated.

### 2.3 The measured server, and why the ceiling was raised to 64 for the sweep

`cqlite-flight` and `flight-loadgen` were built `--release` at **2026-08-06T22:30Z** from this
branch and were **not rebuilt between arms**, so all six arms measured the same binary. That is
checkable rather than asserted: `git diff --name-only 8f745b928~1 f5f631ece` touches only
`docs/reports/**` and `openspec/**` — no Rust input, no `Cargo.*`. The branch point is
`320182217` on `main`. The binary therefore carries the **pre-#3225** constant default.

Server flags, identical at every point (stamped into every record as `server_flags`):

```
--batch-size 8192 --max-batch-bytes 4194304 --max-inflight-egress-bytes 12582912 \
--max-concurrent-scans 64 --admission-wait-timeout-ms 30000
```

`--max-concurrent-scans 64` is deliberate and load-bearing. The harness's `common.sh` defaults it to
**16**, at which every `N > 16` point would measure the admission gate rather than the concurrency
curve. 64 is both the shipped default and the top of the ramp, so the ceiling **could not bind**.

That is **verified, not assumed** — but the verification is the ceiling itself, not the rejection
count. Every point stamps its `server_flags`, and `analyze-3225.py`'s `admission_ceiling` check
reads `--max-concurrent-scans` out of each point and compares it against the largest `N` that arm
actually drove: **64 ≥ 64** in all five primary arms and **64 ≥ 16** in the supplement, uniformly
across all 126 points. Since no point ever asked for more concurrent scans than the ceiling allowed,
the gate had nothing to act on.

**The zero rejection count does NOT show this, and this report no longer argues that it does.** The
sweep runs with `--admission-wait-timeout-ms 30000`: a request arriving over the ceiling does not
fail, it **waits** for a permit and then **succeeds**. A fully throttled curve would therefore still
report `requests_unavailable = 0`. The measured zero (§2.6) is **corroborating** — a non-zero total
would have been proof the ceiling bound — but it is not probative of the converse. The driver now
also refuses to start when an inherited `WS0_MAX_CONCURRENT_SCANS` is below `max(ramp)`, which is
where this should be caught: before the six-hour run, not in forensics afterwards.

Read path: `bypass` (the merge path is out of scope for this round). Sweep parameters: ramp
`1,2,4,8,16,24,32,64`, **120 s** steps, **3 reps**, 45 s warm, 5 s settle, seed 42, `shape=full`.

### 2.4 Corpus — regenerated, geometry-matched against #3217, new sha recorded (AC6)

#3217's corpus binaries were gitignored and are **gone** (`/data/ws0` did not survive), so the
corpus was regenerated on this box to #3217's recipe: 200,000 partitions × 20 rows (`seq` 2 ×
`event_time` 10), Apache Cassandra 5.0.8, `cassandra-stress` user profile, `flush` + `compact` →
exactly **one `nb-16-big` SSTable**, LZ4 / 16 KiB chunks.

| | |
|---|---|
| rows | **3,999,890** (sstablemetadata `totalRows`), agreeing **exactly** with an independent CQL token-range full-scan oracle over 512 ranges |
| `sha256(Data.db)` | **`704ed1f002f0c374504a0a3cbf66c0a55202700a4de8ca18e8386f8706a0402d`** |
| on-disk compressed | 784,086,629 B → **196.03 B/row** |
| logical uncompressed | 2,772,195,010 B → **693.07 B/row** (LZ4 ratio 3.5356×) |

`cassandra-stress` is not byte-deterministic, so the bar — the one #3217 itself set against #3100 —
is **matched geometry plus a documented new sha**, and the full field-by-field comparison is
committed at `ws0-3225-artifacts/corpus/corpus-geometry.txt` (every value **parsed** from an
artifact, not typed): all exact-match fields identical (rows, `totalColumnsSet`, partitions,
generation `nb-16-big`, compressor, rows/partition, no tombstones), every continuous field within
**0.032%** against a 0.50% tolerance, LZ4 ratio identical to 4 decimal places. **Verdict: no
material divergence** — this curve is directly comparable to #3217's published table.

**One corpus, checked arm-to-arm.** `analyze-3225.py` compares every arm's `corpus-basis.json`
field-by-field (stage dir, `*-Data.db` count, both byte totals, compressed/uncompressed SSTable
counts, `rows_per_scan_observed`) and **fails closed** if any two disagree; the digest above is
parsed from the committed shasum artifact rather than trusted from prose. Verdict in the committed
analysis: `PASS: every arm read the same staged corpus, named by sha256 above.`

### 2.5 Harness provenance — three revisions, stated FROM the `harness_commit` stamps

Every point stamps the harness revision that wrote it, so this is read out of the data:

| arm | `harness_commit` | uniform within the arm? |
|---|---|---|
| `cn3225-s1`, `cn3225-s2`, `cn3225-s4`, `cn3225-s6` | `7b84ae65f` (pre-fix) | yes |
| `cn3225-s3` | `fd212ff87` | yes |
| `cn3225-s3-n12supp` | `f5f631ece` | yes |

Two harness defects were found and fixed **mid-round**, and neither can affect another arm's
numbers:

1. **`fd212ff87` — `sweep.sh` emitted a bare `null` into an inline Python heredoc.**
   `sweep.sh:167` interpolated `"server_physical_cores_S": $S_JSON`. For the `s1|s2|s4|s6`
   shorthands `$S_JSON` is a *number* and parses; for a **literal CPU list** — which is how S=3 is
   expressed (`0-2,8-10`) — it is the bare token `null`, which is not a Python name:
   `NameError: name 'null' is not defined`. The arm died in the warm pre-pass before emitting a
   single point. Fix: `json.loads('''$S_JSON''')`. **Why it cannot touch the other arms:** it is one
   line on the code path *only* a literal-CPU-list arm takes, and `json.loads("6") == 6` is
   byte-identical for every value the shorthand arms produced.
2. **`f5f631ece` — `run-3225.sh --list` ignored `--arms`**, printing all five arms while execution
   would run the one requested. `--list` is the "check the plan before a 6-hour run" affordance, so a
   plan that lies is a real defect. It is a **display filter**; it touches no measurement path,
   which is why it cannot explain the offset §5 reports.

The fix for (1) was deliberately **deferred** until `cn3225-s6` finished, so that s1/s2/s4/s6 stayed
on **one** harness revision and only the re-run S=3 moved — a clean provenance split on a code path
no other arm takes, rather than a mid-arm revision change.

**The failed first S=3 attempt is provenance, never data.** It is quarantined at
`results/cn3225-s3.partial-20260807T055535Z/` and contains **no `points.jsonl`** — only its
`run-config.json`, `cpu-topology.json` and `corpus-basis.json`. There was nothing partial to salvage
and nothing was merged; the analyzer skips any directory without `points.jsonl`. The re-run restarted
from rep 1 (the harness always appends from rep 1, so resuming *into* an existing `points.jsonl`
would silently mix a truncated attempt with a complete one and corrupt every median).

### 2.6 Validity controls actually enforced

- **The admission ceiling could not bind** — evidenced by the ceiling, not by the rejection count.
  Every point's `server_flags` records `--max-concurrent-scans 64`, uniformly across all 126 points,
  which is `>= max(N)` in every arm (64 in the five primaries, 16 in the supplement). The gate was
  therefore never asked for a permit it could refuse. `requests_unavailable` did total **0** (and 0
  errors), and that is worth recording — but it is **corroborating, not probative**: with
  `--admission-wait-timeout-ms 30000` an over-ceiling request waits and then succeeds, so a
  throttled curve would report 0 too. See §2.3.
- **Client never saturated**: 0 points excluded; max client util 0.131 vs a 0.70 gate.
- **Those zeros are measured, not absent.** A missing per-point counter would read as `0`/`False` in
  every total above, so the analyzer's `evidence_completeness` check asserts the fields were
  actually recorded: all 8 required counters are present on **126/126** points.
- **Corpus byte-identity: PASS by the BRACKETED SEAL, and the method is named in the output.**
  `analyze-3225.py` reports corpus identity under exactly one of three named methods, because they
  do not prove the same thing:
  1. **`per-arm-digest`** — the strongest, and the method for every future round: `run-3225.sh` now
     stamps `data_db_sha256` into each arm's `corpus-basis.json`, measured immediately before *and*
     after that arm. This round's harness did not, so this method was unavailable here.
  2. **`bracketed-seal`** — the method of record for this round, and an affirmative measurement
     rather than a lowered bar. Every sub-condition holds: both committed prep records
     (`corpus-sha-staged.txt` and `corpus-geometry.txt`) name
     `704ed1f0…7144ac26`; the staged `nb-16-big-Data.db` **re-measured after the last arm** is
     byte-identical to it; its size matches the 784,086,629 B every arm recorded; its **mtime is
     `2026-08-06T22:34:15Z`, which predates the first arm's start (`22:54:59Z`)** — and any write,
     including a swap-and-restore, would have moved mtime — and all six arms' `corpus-basis.json`
     name that same `stage_dir`, file count and byte size.
     **What the seal does NOT prove, stated plainly:** it seals the staged **file**, and it confirms
     what each arm **recorded**; it does not independently witness each arm **opening** that path.
     An arm pointed at a different path whose basis fields coincided would satisfy it. Only a
     per-arm digest closes that residual. Two further limits: `mtime` is forgeable by a deliberate
     `touch`, so the seal is evidence against accidental modification and swap-and-restore, not
     against an adversary; and it requires the staged file to still exist, so it expires when the
     box is reclaimed.
  3. **`unverified`** — neither method available. Not a pass, non-zero exit. Every sub-condition of
     the seal fails closed into this state: a missing prep record, a current sha that differs, an
     mtime at or after the first arm start, an absent staged file, or any arm whose recorded basis
     disagrees. Each was constructed and observed rejecting.
  A per-arm digest still cannot be backfilled for these six arms — a digest taken today records
  today's bytes — which is why the seal is reported as its own named method rather than dressed up
  as one.
- **Server/client core exclusivity**: enforced by `sweep.sh`'s refusal, demonstrated firing (§2.2).
- **S is never guessed.** `sweep.sh` stamps `server_physical_cores_S` only for its `s1|s2|s4|s6`
  shorthands, so the S=3 arms stamp `null`. The analyzer re-derives S from **that arm's own**
  `cpu-topology.json` sibling groups intersected with `server_cpus`, **records which method it
  used** per arm, and reports `UNRESOLVED` rather than dividing a thread count by an assumed SMT
  factor. Both S=3 arms report `derived from this arm's cpu-topology.json: 3 sibling groups touched`.
- **The analyzer was smoke-tested before the data existed**: `analyze-3225.py --smoke` runs the whole
  pipeline over #3217's *committed* points and reproduces **29/29** of #3217's published medians
  exactly. Under `--smoke` every width correctly reports CENSORED and the N=64 columns are correctly
  absent — that is the right answer for #3217's input, not a defect.
- **No interpolation, anywhere.** A predicted N absent from an arm's ramp is reported as
  `UNKNOWN, not 0`; N=64 absent from the supplement's ramp is reported as `no value is inferred`.

---

## 3. The curves — median of 3 reps per point, dispersion = (max − min) / median

`rows/s` is aggregate rows per second; `srvUtl` is server CPU utilisation of the pinned set; `p50` is
per-scan latency in ms (each request is a **full 4M-row scan**, so p50 is in tens of seconds by
construction and grows with N). Peaks in **bold**.

**S=1 (P=2, cpus `2,10`)**

| N | 1 | 2 | 4 | 8 | 16 | 24 | 32 | 64 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| rows/s | 216,298 | **240,693** | 228,657 | 216,503 | 208,696 | 200,605 | 196,655 | 188,914 |
| spr% | 5.0 | 3.2 | 4.1 | 2.4 | 1.2 | 1.3 | 0.7 | 1.3 |
| p50 ms | 18,448 | 32,195 | 67,764 | 143,655 | 300,941 | 476,578 | 648,544 | 1,350,566 |
| srvUtl | 0.774 | 0.994 | 0.999 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |

**S=2 (P=4, cpus `0,2,8,10`)**

| N | 1 | 2 | 4 | 8 | 16 | 24 | 32 | 64 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| rows/s | 199,760 | 310,633 | 386,265 | **432,360** | 411,090 | 403,017 | 393,092 | 373,263 |
| spr% | 9.0 | 2.5 | 12.4 | 1.1 | 2.4 | 3.2 | 1.4 | 2.8 |
| p50 ms | 20,349 | 24,773 | 39,715 | 71,696 | 154,272 | 237,240 | 323,748 | 681,574 |
| srvUtl | 0.420 | 0.745 | 0.921 | 0.996 | 0.999 | 1.000 | 0.999 | 1.000 |

**S=3 (P=6, cpus `0-2,8-10`)** — main arm; see §5 for N=12 and the peak of record

| N | 1 | 2 | 4 | 8 | 16 | 24 | 32 | 64 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| rows/s | 169,887 | 315,434 | 479,324 | 588,316 | **624,556** | 612,577 | 591,564 | 561,995 |
| spr% | 8.1 | 3.5 | 2.9 | 2.1 | 0.6 | 2.0 | 1.0 | 1.8 |
| p50 ms | 23,429 | 23,937 | 30,474 | 53,084 | 98,894 | 152,830 | 214,303 | 450,101 |
| srvUtl | 0.245 | 0.501 | 0.828 | 0.971 | 0.997 | 0.998 | 0.997 | 0.999 |

**S=3 supplement (`cn3225-s3-n12supp`, ramp `12,16`)**

| N | 12 | 16 |
|---|---:|---:|
| rows/s | **624,848** | 614,209 |
| spr% | 0.3 | 0.2 |
| p50 ms | 75,956 | 102,760 |
| srvUtl | 0.995 | 0.999 |

**S=4 (P=8, cpus `0-3,8-11`)**

| N | 1 | 2 | 4 | 8 | 16 | 24 | 32 | 64 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| rows/s | 160,450 | 310,569 | 541,696 | 713,223 | **815,748** | 784,934 | 779,600 | 734,228 |
| spr% | 8.2 | 3.2 | 6.6 | 1.4 | 0.5 | 1.1 | 2.1 | 1.4 |
| p50 ms | 25,395 | 24,248 | 28,082 | 43,188 | 75,301 | 116,654 | 159,908 | 344,457 |
| srvUtl | 0.167 | 0.367 | 0.751 | 0.940 | 0.994 | 0.990 | 0.993 | 0.999 |

**S=6 (P=12, cpus `0-5,8-13`) — the widest width in scope**

| N | 1 | 2 | 4 | 8 | 16 | 24 | 32 | 64 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| rows/s | 142,602 | 303,654 | 562,023 | 904,930 | 1,086,418 | **1,173,759** | 1,146,063 | 1,087,912 |
| spr% | 3.6 | 5.5 | 1.7 | 4.7 | 4.2 | 3.0 | 3.5 | 4.3 |
| p50 ms | 27,869 | 25,674 | 26,673 | 33,849 | 57,410 | 78,250 | 108,069 | 229,769 |
| srvUtl | 0.102 | 0.229 | 0.505 | 0.878 | 0.971 | 0.989 | 0.992 | 0.995 |

Two structural observations, offered as consistency and not as derivation:

- **Every peak sits at 0.989–0.997 server utilisation of the pinned set.** The optimum is where the
  admitted streams collectively saturate the CPU; past it, added streams add contention, not work.
  At S=6, N=16 is at 0.971 — *not* saturated, which is precisely why #3217's censored "16" was low.
- **Over-admission costs latency far faster than it costs throughput.** The two currencies disagree
  by an order of magnitude and an operator needs both: at S=1 the shipped default costs −21.5%
  throughput but **41.95×** p50; even at the widest width it is −7.3% for **2.94×**.

---

## 4. What over-admission costs, in both currencies (AC1)

Relative to each width's own measured peak. Full table in the committed analysis.

| S | N=16 | N=24 | N=32 | **N=64 (shipped default)** |
|---:|:--|:--|:--|:--|
| 1 | −13.3% / 9.35× | −16.7% / 14.80× | −18.3% / 20.14× | **−21.5% / 41.95×** |
| 2 | −4.9% / 2.15× | −6.8% / 3.31× | −9.1% / 4.52× | **−13.7% / 9.51×** |
| 3 | *(peak)* | −1.9% / 1.55× | −5.3% / 2.17× | **−10.0% / 4.55×** |
| 4 | *(peak)* | −3.8% / 1.55× | −4.4% / 2.12× | **−10.0% / 4.57×** |
| 6 | *below peak — §6: N=24 is **+8.0%** above N=16* | *(peak)* | −2.4% / 1.38× | **−7.3% / 2.94×** |

Each cell is `throughput as % of that width's peak / p50 as a multiple of the p50 at that peak`.
Cells at or below a width's peak are not over-admission and carry no cost figure; the analyzer emits
this table only for `N > peak`, which is why S=6's N=16 is quoted from the AC5 block instead.

---

## 5. S=3, the missing prediction, and the bridge point — the methodological core of this round

This section matters more than any single number in it.

### 5.1 The arm as first run could not evaluate its own falsification test

S=3 was specced as the **out-of-fit width**: P=6 is a width no fit point in `design.md` D2 touched,
so it is where the formula could be *falsified*. The formula predicts `2 × 6 = 12`.

**N=12 was not in the ramp** (`1,2,4,8,16,24,32,64`). tasks.md §2 requires publishing the formula's
deviation as a % of each width's measured peak — and at S=3 that deviation was **uncomputable from
the arm as run**. The analyzer says so rather than interpolating: `predicted N=12 is NOT in this
arm's ramp … The deviation is UNKNOWN, not 0 — no value is interpolated.` Every other width's
prediction *was* in the ramp (P=2→4, P=4→8, P=8→16, P=12→24). The one width designed to break the
formula was the one width that could not test it.

### 5.2 The supplement, and the bridge point that was included precisely to check the splice

A supplement arm `cn3225-s3-n12supp` ran ramp **`12,16`**, 3 reps, identical box / corpus / binary /
flags. **N=16 was included deliberately as a BRIDGE POINT** — a value both runs measured — so that
splicing N=12 into the main S=3 curve would be *demonstrated* rather than assumed. This was chosen
over a full 9-point S=3 re-run (~1 h 35 m vs ~22 min) **because** it also buys run-to-run
reproducibility evidence a re-run would not.

The bridge found a real effect:

| N=16 | rows/s median | dispersion |
|---|---:|---:|
| main arm `cn3225-s3` | 624,556 | 0.6% |
| supplement `cn3225-s3-n12supp` | 614,209 | 0.2% |
| **between-run offset** | **−1.66%** | — |

**The offset is LARGER than either run's internal dispersion.** So on this rig run-to-run variation
*exceeds* within-run dispersion, and a cross-run difference of one or two percent is not evidence of
anything. The analyzer states the consequence in the committed output:

> BRIDGE DISAGREES: the between-run offset at N=16 is −1.66%, LARGER than the 0.6% within-run
> dispersion there. … any cross-run absolute delta below ~1.66% is an ARTIFACT, and this width's
> formula verdict must come from a WITHIN-run pair.

### 5.3 What the bridge caught, and the verdict of record

Without the bridge, the natural thing to publish would have been the **naive cross-run splice**:
supplement N=12 (624,848) against main-arm N=16 (624,556) = **+0.05%** — a number that looks like
"the formula's prediction ties the measured peak" and is in fact **an artifact 30× smaller than the
between-run offset**. The analyzer computes it, labels it **REJECTED**, and prints why, so the
number that would have been reported is visible next to the reason it was not.

**The valid comparison is WITHIN a single run**, and within the supplement:

> **N=12 (624,848) beats N=16 (614,209) by +1.73%** — dispersions 0.3% and 0.2%.

So at S=3 the formula's predicted N=12 is not merely acceptable: it **beats the value the coarse ramp
had identified as that width's peak**. The main arm called N=16 the peak only because 12 was never
offered. **The out-of-fit falsification width CONFIRMS the formula.** Against the shipped default,
N=12's 624,848 vs the main arm's N=64 561,995 is +11.2% *cross-run* — clearly better either way, but
it is a cross-run figure and is stated with the ~1.7% caveat attached.

### 5.4 The general limitation, and its exact scope

- **Cross-arm / cross-run ABSOLUTE comparisons on this rig carry ~1.7% run-to-run uncertainty.** No
  cross-run delta below that is presented anywhere in this report as meaningful.
- **Every peak determination at S=1, 2, 4 and 6 is a WITHIN-ARM comparison** — one arm, one
  contiguous run, 3 reps per N — and is therefore **unaffected**. So is the S=3 verdict above, which
  is within-supplement. The AC5 result (§6) is within-arm. The gaps that matter are large relative to
  1.7%: +8.0% at S=6, −21.5% at S=1.
- **Candidate cause, offered as HYPOTHESIS and not as conclusion:** the supplement's ramp starts at
  N=12, so it lacks the main arm's `1,2,4,8` ramp-up history and its associated cache / thermal /
  allocator state. **What it is not:** the harness delta between `fd212ff87` and `f5f631ece` is the
  `--list` display filter (§2.5) and touches no measurement path, so it cannot explain the offset.
  Confirming the ramp-history hypothesis needs a dedicated arm and was not run.

---

## 6. AC5 — no regression at the widest configuration in scope; it is an improvement

Widest width in scope: **S=6 physical cores / P=12 hardware threads** (`cn3225-s6`), for the rig
reason in §2.2 — the client needs 2 exclusive physical cores on the same box and `sweep.sh` refuses
an overlapping set. This is **not** "the whole box".

Derived default at P=12: **N=24**. All three points are medians of **3** valid reps from the **same
arm**, so this is a within-arm comparison and the §5 cross-run caveat does not apply.

| N | role | rows/s median | min / max | dispersion | derived N=24's gain | > dispersion? | rep ranges disjoint? |
|---:|:--|---:|---:|---:|---:|:--|:--|
| **24** | **derived default** | **1,173,759** | 1,168,927 / 1,203,787 | 3.0% | — | — | — |
| 16 | #3217's censored peak / the misidentified default | 1,086,418 | 1,063,033 / 1,108,249 | 4.2% | **+8.0%** | YES (4.2%) | **YES** |
| 64 | the **shipped** default | 1,087,912 | 1,046,778 / 1,094,080 | 4.3% | **+7.9%** | YES (4.3%) | **YES** |

Both comparisons clear the strongest bar available here: the gain exceeds the larger dispersion of
each pair *and* the two N's **rep ranges do not overlap at all** (N=24's slowest rep, 1,168,927,
beats N=16's fastest, 1,108,249, and N=64's fastest, 1,094,080). At the peak the server is at 0.989
utilisation and p50 is 78,250 ms, against 229,769 ms at the shipped default — **2.94× better latency
as well as +7.9% throughput**.

**AC5 verdict: no regression at the widest configuration in scope — an improvement, measured.** The
analyzer states it as `REGRESSION-FREE: yes — the derived default beats every measured alternative at
this width`.

---

## 7. Byte bases — three of them, named at every width's peak (AC6)

`rows/s` is the primary unit throughout this report. Where bytes are quoted they are one of exactly
three **different** quantities over the same rows, never collapsed into a bare "MB/s" (they differ by
the 3.5356× LZ4 ratio, and the third by ~17×):

| basis | definition |
|---|---|
| **logical uncompressed** | `rows/s ×` the corpus `CompressionInfo.db` `dataLength` basis = **693.07 B/row** |
| **on-disk compressed** | `rows/s ×` the summed `*-Data.db` basis = **196.03 B/row** |
| **arrow wire CAPACITY** | `flight-loadgen`'s Arrow buffer **capacity** bytes, summed client-side — **NOT** compressed gRPC-on-the-wire bytes |

At each width's peak:

| S (peak N) | logical uncompressed | on-disk compressed | arrow wire capacity |
|---|---:|---:|---:|
| 1 (N=2) | 166.8 MB/s | 47.2 MB/s | 2,934.4 MB/s |
| 2 (N=8) | 299.7 MB/s | 84.8 MB/s | 5,271.0 MB/s |
| 3 (N=16, main) | 432.9 MB/s | 122.4 MB/s | 7,614.2 MB/s |
| 3 (N=12, supplement) | 433.1 MB/s | 122.5 MB/s | 7,617.8 MB/s |
| 4 (N=16) | 565.4 MB/s | 159.9 MB/s | 9,945.1 MB/s |
| 6 (N=24) | 813.5 MB/s | 230.1 MB/s | 14,309.7 MB/s |

Corpus geometry, sha256 and the field-by-field comparison against #3217: §2.4 and
`ws0-3225-artifacts/corpus/corpus-geometry.txt`. `now`-pinning is not applicable (no TTL/tombstone
semantics in this corpus: `Estimated droppable tombstones 0.0`, `TTL min/max 0`).

---

## 8. What this means for §3 (the default flip)

- **The gate is met.** `clamp(2 × P, 2, 64)` is better than the constant 64 at **every** measured
  width, by +7.3 to +16.5 percentage points, and no width shows it worse. tasks.md §2's re-fit
  trigger did not fire.
- **The ceiling stays 64** and the change stays one-directional: no deployment is admitted more
  widely than today, and `--max-concurrent-scans 64` restores exactly the pre-#3225 behaviour on any
  host.
- **The −5.0% at P=2 is the accepted cost, not a surprise.** `design.md` D2 predicted −4.8% from
  #3217's data and accepted it as a minimax-regret choice, because `P = 2` is produced both by one
  SMT core (peak 2) and by two non-SMT cores (peak 8) and `available_parallelism` cannot tell them
  apart. This round measures the miss at **−5.0%**, i.e. the design's own estimate held to 0.2 pp.
- **The issue's premise needs restating in the operator docs, not just here:** the population that
  benefits is *every* width, not only narrow ones.

---

## 9. What is NOT established

### 9.1 The non-SMT arm was NOT run — open residual (design.md D3 residual 2)

`design.md` D3 takes the **hardware-thread** basis, which on an SMT-on host yields 4 admitted scans
per physical core (the fitted value) but on a non-SMT host (Graviton, most ARM instances, SMT
disabled in firmware) yields **2 per physical core — half the fitted per-core value**. tasks.md §2
names one non-SMT width as the highest-value optional extension.

**It was not run: no non-SMT host was available to this round.** So the hardware-thread basis remains
**unvalidated** on a machine where logical == physical, and the residual stands exactly as D3 states
it. It is published in the operator documentation (`cqlite-flight/README.md`) rather than hidden, and
it is the top candidate for a follow-up measurement.

### 9.2 Everything else this round does not settle

- **No 8-physical-core width**, and no multi-socket / multi-NUMA width. The rig reason is §2.2; the
  formula's behaviour above P=12 is extrapolation up to the P≥32 point where it is a strict no-op.
- **~1.7% cross-run uncertainty** (§5.4) — measured on this rig, cause hypothesised only.
- **One corpus, one query shape, one read path.** `shape=full`, `bypass`, a single-SSTable
  200k×20-row `ws0.events` corpus with no tombstones or TTLs. The peak-N relation is not established
  for point reads, for the merge path, or for a multi-SSTable corpus.
- **p50 here is whole-scan latency**, so the multiples in §4 describe *this* workload's scans; they
  are not a general per-request latency law.
- **No microarchitectural attribution.** By design (`design.md` D7) this round runs no
  `profile-*` / `classify-offcpu` / `runqlat` chain — it measures a curve. The IPC-decay question
  #3217 left open stays open.

---

## 10. Process lessons — two instances of "the test didn't test the thing"

Recorded plainly, because both cost real time and both are the same failure shape.

1. **The dry run was S=1, so the only arm exercising the new custom-CPU-set code path was never
   smoke-tested.** The `null`-into-Python defect (§2.5) lives *exclusively* on the literal-CPU-list
   path, which only S=3 takes. The pre-flight dry run validated the path that already worked. The arm
   died **3.5 minutes into a ~6-hour run** and cost a full arm re-run. The rule: smoke-test the
   **new** form end-to-end, not the familiar one. (This was done before the relaunch — a 10 s / 1 rep
   run on the custom form, confirmed to emit a valid point, then discarded.)
2. **The ramp omitted the formula's own prediction at the one width specced to falsify it.** S=3
   existed to test `2 × P` out of fit; `2 × 6 = 12` was not in `1,2,4,8,16,24,32,64`. The arm ran for
   an hour and produced a curve that could not answer the question it was designed to ask. The rule:
   when a measurement exists to test a prediction, **put the predicted value in the grid** — check
   that mechanically, before launching.

A third, positive note belongs beside them: the supplement's **bridge point** was the cheap decision
that paid for itself. It cost one extra N (~7 minutes) and it converted a would-be published +0.05%
artifact into a measured −1.66% run-to-run offset plus a valid +1.73% within-run verdict. Include the
overlap point whenever two runs will be compared.

---

## 11. Artefact inventory

Everything below is **committed** under `docs/reports/ws0-3225-artifacts/`. Per CLAUDE.md the
harness content here is **reviewed code**, not docs — the PR carrying it is not a docs-only change.

| Path | What |
|---|---|
| `README.md` | Rig layout, launch + resume procedure, and the two things to know before reading the output |
| `rig/verify-rig.sh` → `rig/rig-verification.txt` | Topology/SMT/NUMA read from sysfs, competing load, `/data` capacity, and the **demonstrated** server/client overlap refusal |
| `corpus/regen-corpus.sh` | Corpus regenerator (path-parameterized adaptation of #3026's `gen-corpus.sh`) |
| `corpus/compare-geometry.py` → `corpus/corpus-geometry.txt` | Field-by-field geometry vs #3217, every number parsed from an artifact; non-zero exit on material divergence |
| `corpus/corpus-{provenance,sha-staged,measure,fullscan,geometry}.txt`, `corpus-{sstablemetadata,tablestats,tablehistograms}.txt` | Corpus recipe, all 8 component digests, and the raw Cassandra-side measurements |
| `run/run-3225.sh` | Five-arm sweep driver; per-arm restartable, fails closed on a missing corpus/binary/template or a running Cassandra |
| `run/analyze-3225.py` + `run/analyze_3225_render.py` | The analysis of record (`--smoke` self-check against #3217's committed points: 29/29) |
| `results/analysis-3225.{txt,json}` | **The committed analysis every number in this report comes from** |
| `results/cn3225-{s1,s2,s3,s3-n12supp,s4,s6}/` | `points.jsonl` (126 points), `summary.{json,txt}`, `run-config.json`, `cpu-topology.json`, `corpus-basis.json` |
| `results/cn3225-s3.partial-20260807T055535Z/` | The FAILED first S=3 attempt — provenance only, **no `points.jsonl`, no data** |

Reused **unchanged** from `../ws0-3217-artifacts/harness/`: `common.sh`, `sweep.sh`,
`emit-point.py`, `summarize-sweep.py`, `corpus-basis.py`, `selftest.sh` (`sweep.sh` carries the one
`fd212ff87` fix described in §2.5). **Not run**: everything under `../ws0-3217-artifacts/partB-run/`
and the `profile-*` / `classify-offcpu` / `runqlat` chain (`design.md` D7).

---

## Acceptance criteria — the measurement half (§2 of tasks.md)

| AC | Status | Evidence |
|--:|:--|:--|
| 1 — peak-N-by-width reproduced and extended to the widest configuration in scope; medians of ≥3 with dispersion; throughput **and** per-scan-latency cost of over-admission at each width | ✅ | §1 Finding 1, §3, §4; 126 points, every one recording `--max-concurrent-scans 64` ≥ that arm's max N so the ceiling could not bind (§2.3); both #3217 peak locations reproduced and both censored points resolved |
| 5 — no regression at the widest configuration | ✅ **improvement, not merely no regression** | §6: +8.0% vs N=16 and +7.9% vs the shipped N=64, gains exceeding dispersion with **disjoint rep ranges** |
| 6 — every throughput figure names its byte basis; fixture geometry recorded (rows, B/row, sha256) | ✅ | §7 (three bases, never collapsed), §2.4 (3,999,890 rows, 693.07 / 196.03 B/row, sha256 recorded, geometry matched within 0.032%); corpus identity `PASS (method=bracketed-seal)` with its residual stated (§2.6) |
| non-SMT extension (optional, highest-value) | ⛔ **NOT RUN** — no non-SMT host available | §9.1; residual published in `cqlite-flight/README.md` |
| formula gate on §3 (tasks.md §2) | ✅ **MET at all five widths** | §1 Finding 3, §5, §8 |

ACs 2, 3 and 4 are the implementation half (tasks.md §3–§5: the derived default, the provenance
field, and the operator documentation) and are evidenced there, not here.

## Notes and deviations

- **The issue's framing is corrected, not confirmed.** Its premise that wide servers are "already
  optimal at the default" was true of the *misidentified* default 16; against the shipped 64 no width
  is optimal. The 16.4%-at-1-core figure is confirmed **as a lower bound** — the true cost at the
  shipped default is −21.5% — but it does **not** reproduce to the decimal at its own N (−13.3% at
  N=16 here); §1 Finding 2 states which half of that claim the data supports.
- **Six arms, five widths.** `cn3225-s3-n12supp` is a **supplement** to `cn3225-s3` over the same
  CPUs, not a sixth width; the analyzer labels it `SUPPLEMENT` in the peak table for exactly that
  reason, and its peak must be read through §5's bridge analysis rather than as an independent
  result.
- **No number in this report is hand-computed.** Where a figure looked like it should exist and did
  not, the analyzer was fixed and re-run rather than the number typed in: the cross-run bridge, the
  within-run S=3 verdict, the AC5 block, the harness-provenance table and the fail-closed corpus
  identity check were all added to `analyze-3225.py` for this report.
- **Rounding**: throughputs are the analyzer's medians rounded to whole rows/s; percentages to one
  decimal (two where a sub-1% figure is load-bearing, i.e. §5's −1.66% / +1.73% / +0.05%).
- **Nothing was rounded toward the hypothesis.** The formula's one miss (−5.0% at P=2) is reported at
  the width where it happens, next to the design's own −4.8% prediction, rather than averaged into
  the four hits.
