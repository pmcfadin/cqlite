# CQLite 0.17 talk — cluster run report (issue #4137)

Run date: **2026-09-07** (PDT). Cluster: `talk017`, us-west-2.

This file records the run that produces the talk's charts. **Every value in the pinned-assets
table below was read back from the running system**, not from a plan — where a value could not be
verified it says so rather than carrying a plausible number.

Reporting rules inherited from `docs/reports/ws0-3100-report.md` and enforced here: **warm and cold
are separate claims and are never blended**; every reported figure is a **median of N with its
spread**, N stated; **every ratio names both operands**; and no chart is published against a table
whose row count was not measured.

## Pinned assets

| Artifact | Pin | How it was verified |
|---|---|---|
| cqlite-flight image | `ghcr.io/pmcfadin/cqlite-flight:talk017` | built by `flight-image.yml` run [34150889344](https://github.com/pmcfadin/cqlite/actions/runs/34150889344) |
| — index digest | `sha256:847c93ba109ceeb0a6759a5c23a1b063a86779ab11fa05b6e522df8257c82980` | `ghcr.io/v2/.../manifests/talk017` `docker-content-digest` |
| — `linux/amd64` digest | `sha256:5518f820933608b984b1a8263cbec2889fd27ffee23858e2738a525e829ad5a1` | OCI index `manifests[]` |
| — built from | cqlite `main` @ `f22ce842bd1ed27e9286ec0433d9c76b880db1bb` (2026-09-07) | workflow `headSha` |
| — running digest | **all 3 pods** `…@sha256:847c93ba…c82980` | `kubectl … containerStatuses[0].imageID` |
| Allocator | **`system`** | `cqlite-flight --version` in the image: `allocator: system` |
| Trino connector | `in.mcfad:cqlite-trino:0.16.1` | `repo1.maven.org` POM fetch, HTTP 200 |
| Trino | **481** | `kit install trino --version 481`; coordinator + 2 workers `Running`, 0 restarts |
| Cassandra | **5.0.9** | `nodetool version` → `ReleaseVersion: 5.0.9` on all 3 nodes |
| — topology | 3 nodes `UN`, one rack per AZ (`us-west-2a/b/c`) | `nodetool status` |
| — keyspace RF | **3** (`SimpleStrategy`) | stress-created schema DDL in the job log |
| Cluster — db | 3× `i4i.2xlarge` (local NVMe, 8 vCPU) | `describe-instances` |
| Cluster — app | 2× `m6i.2xlarge` | `describe-instances` |
| Cluster — control | 1× `m5d.xlarge` (added by the lab, not in the plan) | `describe-instances` |
| db cassandra AMI | `ami-051fd33665d85978c` (`easy-db-lab-cassandra-amd64-20260907191321`) | baked for this run — see "AMI rebake" |
| base AMI | `ami-08301cdc978305dd0` (`easy-db-lab-base-amd64-20260907185243`) | baked for this run |
| easy-db-lab | upstream `37b0ea96` (2026-09-07 tip) | `git log -1` |
| Read mode | `snapshot` | `cqlite.read-mode=snapshot` in the rendered catalog |
| Split DC preference | `cqlite.local-datacenter=us-west-2` | rendered catalog — **see issue #4161** |

Both catalogs confirmed present on one Trino before any measurement, which is what makes
"same SQL, two catalogs" a controlled comparison rather than two runs:

```
SHOW CATALOGS  ->  cassandra, cqlite, system, tpcds, tpch
```

The stock `cassandra` catalog was auto-registered by the lab's trino kit with all three
contact points (`10.1.3.100,10.1.1.49,10.1.2.25`) and `local-dc=us-west-2`; the hand-written
fallback that #4137 allowed for was not needed.

### Why the image is tagged 0.16.1 internally

`cqlite-flight --version` prints `cqlite-flight 0.16.1` because `main` is not version-bumped for
0.17. **The image's identity is its digest and build sha, not that string**, and it must not be
confused with the *released* `:v0.16.1` image (`sha256:74ac0934f6bd…`), which is 481 commits
behind. The trunk build was chosen deliberately: `main` since `v0.16.1` carries the read-path work
these claims rest on — #3058 (single-SSTable merge bypass on the `do_get` data plane, 3.32×),
#3225 (admission default from available parallelism), #2825/#2821 (byte-bounded Arrow egress and
per-stream byte budget). A released 0.16.1 image would measure *slower* than the subject of the talk.

The connector needed no equivalent decision: `git log v0.16.1..main -- trino-connector/` is
**empty**, so the published `0.16.1` jar is the same source `main` carries. `flight-trino-e2e.yml`
— which builds both sides from the tree and queries through Trino — passed on `main` on the same
day as the build sha, which is the compatibility evidence for this pairing.

## Corpus

All figures below are **measured** from `nodetool tablestats` and a `count(*)` through the
`cassandra` catalog — never a target, per #4137 ("Never report a chart against a table whose row
count you did not measure").

| Table | Workload | Target | Measured rows | Partitions | SSTables/node | On-disk bytes/node | Compression ratio |
|---|---|---|---|---|---|---|---|
| `keyvalue` | `KeyValue -n 156250 -p 20000000 -r 0.0 --threads 128 --rate 60000` | 20M | **22,339,536** | 22,504,314 (~1 row/partition) | 3 | 4,459,716,529 | 0.92195 |
| `sensor_data` | `BasicTimeSeries -n 156250 -p 20000 -r 0.0 --threads 128 --rate 60000` | 20k partitions | **23,703,992** | 2,548,496 (~9 rows/partition) | 3 | 4,153,175,060 | 0.94011 |
| `kv_small` | — | 5M | **NOT CREATED** | — | — | — | — |

`keyvalue` measured **22,339,536**, above the 20M target: the aborted early load attempts (see the
`-n` trap below) left rows behind. #4137 requires the *measured* count and forbids reporting a
target, so every chart says 22,339,536.

`kv_small` could not be built: `cassandra-easy-stress` rejects `--table`
(`ParameterException: Only one main parameter allowed but found several: "KeyValue" and "--table"`),
so a second KeyValue-shaped table is not expressible through that CLI. D4 and D5 used `keyvalue`
and `sensor_data` instead.

**A second throughput trap, worth as much as the `-n` one:** `cassandra-easy-stress` silently
rate-limits to roughly **5,000 ops/s** by default. Both a 64-thread and a 256-thread run measured
*exactly* 4,800 ops/s with the client at 0.5 core and the db nodes at 10% CPU — identical
throughput at 4× concurrency with everything idle is a throttle, not a bottleneck. Adding
`--rate 60000` took it to **59,939 ops/s (12.5×)** and the db nodes to 75–84% CPU, turning a
68-minute load into 4.4 minutes.

**Row-count target reduced from 50M to 20M**, which #4137 explicitly permits provided it is
disclosed: *"If 50M does not fit the time budget, drop to 20M and say so in every chart title."*
Every chart carrying this table names the **measured 22,339,536**, which is the stronger form of
that disclosure — a reader sees the actual corpus rather than either target.

**The reduction was decided against the throttled rate and would not have been necessary
otherwise.** At the observed ~4,800 ops/s, 20M was ~68 min and 50M ~2.9 h of pure load time before
any demo could start. Once `--rate 60000` removed the throttle (see above), the effective rate was
59,939 ops/s — at which 50M is roughly 14 minutes. The 20M corpus was already loaded by then and
reloading would have cost more than it bought, so the run continued on it. **A future run should
use 50M**; the constraint that forced 20M was a default flag, not the hardware.

The real table name is **`keyvalue`** (from the workload's own DDL), not `kv` as #4137's prose
calls it. Chart titles and CSV headers use the real name.

### One sizing trap, recorded because it is not obvious from the flags

`cassandra-easy-stress`'s `-n` is **per thread, not total**. `-n 20000000 --threads 64` logs
`Running the profile for 20000000 iterations` *once per thread* — 1.28 billion operations, ~83
hours at the observed rate. The 20M total is expressed as `-n 312500 --threads 64`
(312,500 × 64 = 20,000,000), verified by the job log printing `Running the profile for 312500
iterations`. A reader reproducing this run from #4137's `-p 50000000` prose would launch the
1.28-billion-op version.

## Demos

All six ran. **One contradicts the story and is reported as measured** (D3), per #4137: "the talk
changes, not the data."

| Demo | Chart | The number the slide can say | The caveat that must ride with it |
|---|---|---|---|
| D1 OLTP isolation | `charts/d1-isolation.png` | Same analytic costs **5.61×** baseline client p99 through Cassandra's CQL path vs **2.21×** through CQLite → **~2.5× less p99 impact** | Impact is **not zero**. Flight shares the db node's cores and the DaemonSet has no `resources.limits`. Single-pod (see #4175). GC-pause panel **not collected**. |
| D2 same SQL | `charts/d2-scan-warm.png` | Full-table scan **17,100 ms → 10,300 ms**, **1.66× faster**, medians of 3 | Byte-identical work both sides (22,339,536 rows / 3,451,144,643 B), so it is like-for-like — but it is **`keyvalue` only**, and D3 shows the opposite on another table. Single-pod. |
| D2 scaling | `charts/d2-scan-limit-ladder.png` | Scan scales linearly to 1M rows, no cliff | Single runs, a ladder not a claimed median. |
| **rows/sec** | `charts/rows-per-sec-sustained.png` | **1,720,646 rows/s sustained for 8.5 min** (peak 1,775,645), *while Cassandra concurrently served ~6,400 OLTP reads/s* | **Single-pod** (`charts/rows-per-sec-per-pod.png` shows two pods idle). Do not caption as a 3-node aggregate. Rig reference is 1.17M rows/s (#3225). |
| D3 time-series SQL | `charts/d3-timeseries.png` | The **SQL** is the point: cross-partition `GROUP BY`, `approx_percentile`, window function, and a **join that only completes through CQLite** (stock catalog fails `EXCEEDED_LOCAL_MEMORY_LIMIT`) | **CQLite is 1.3–1.4× SLOWER here** (0.70×/0.75×/0.80×) — the opposite of D2. The join win is "leaner delivery fit the same 1 GB Trino budget" (797 MB vs 1.86 GB), **not** "Cassandra cannot join". |
| D4 concurrency | `charts/d4-ladder.png` | **0 errors and 0 restarts** through 80 concurrent clients; 5.5 → 15.0 qps; memory flat idle→80 | qps **not comparable** to R11b's 34 qps floor (hand-authored mix avoiding #4170, different driver). No regression claimed. |
| D5 freshness | `charts/d5-freshness.png` | Staleness **0 rows** at both flush settings | Does **not** show "always fresh". At 2,000 writes/s memtable pressure flushed often enough that cadence never bound. A **low-write-rate table is untested** and is where 0.17's bound would show. |
| D6 cold start | `charts/d6-coldstart.png` | First query after a Flight restart: **288 ms** (`LIMIT 5`), full scan 12,820 ms = 1.24× warm | **Not `drop_caches`-cold** — the drop moved 24 MB of 2.7 GB (Cassandra holds SSTables mmap'd). Flight-process-cold only. |

### The one thing to resolve before building slide 2

**D2 and D3 disagree.** CQLite is 1.66× *faster* on `keyvalue` (~1 row/partition) and 1.3–1.4×
*slower* on `sensor_data` (~9 rows/partition) — same cluster, same Trino, minutes apart. So
"same SQL and it's faster" is **not** a general claim from this run; it is true of the shape it was
measured on. Partition width is the most visible difference and is recorded as a **correlation, not
a cause** — isolating it needs a purpose-built table pair differing only in that dimension.

## Corrections to earlier files in this same directory

Recorded rather than silently patched, because a reader comparing files should see the disagreement.

1. **`results/d4-ladder.csv` says a real VmRSS reading "was NOT collected". That is wrong.**
   `cqlite_proc_rss_bytes` was in VictoriaMetrics throughout and is now exported to
   `results/metrics/proc_rss_bytes.csv`. It is not merely a provenance fix: **real RSS peaks at
   3.5–5.9 GiB**, i.e. *higher* than the 2–3 GiB `kubectl top` figures that file attributed to
   page-cache inflation. So the memory question against #2367's "idle 3–4 MiB" is **open, not
   explained**, and no memory claim from this run should be presented as settled.
2. **`results/d6-coldstart.csv` reports `index_parses_total` as UNAVAILABLE. It is 127, not 0.**
   #2412's lazy Summary-guided open predicts an O(summary) open with no full `Index.db` parse, so a
   nonzero count needs explaining. Exported to `results/metrics/index_parses_total.csv`.

Both were found only because the metric store was drained before teardown; both are cheap to
re-check and neither is asserted as a defect here.

## Defects found (filed, all reproducible)

| Issue | What |
|---|---|
| [#4161](https://github.com/pmcfadin/cqlite/issues/4161) | `trino-cqlite` kit renders the literal `__LOCAL_DATACENTER__` when `--local-datacenter` is omitted, contradicting its own comment. Measurement-corrupting (governs split placement), not cosmetic. |
| [#4170](https://github.com/pmcfadin/cqlite/issues/4170) | Unbounded `SELECT count(*)` through the `cqlite` catalog never completes — 10.83 min at 0 processed rows with an SSTable re-open loop — while `sum(length(value))` over the same table finishes in 9.5 s and a partition-bounded `count(*)` in 1.7 s. |
| [#4173](https://github.com/pmcfadin/cqlite/issues/4173) | `timeuuid` maps to `varchar` on `cqlite` but `uuid` on the stock `cassandra` catalog. Breaks "same SQL" silently — `ORDER BY` differs and neither is time order. |
| [#4175](https://github.com/pmcfadin/cqlite/issues/4175) | Scans do not fan out: one Flight pod (the `sidecar-uri` host) does all the work while the other two replicas idle. Caps throughput at one node and makes the scaling story untestable. |

**#4175 qualifies every throughput figure in this report.** All of them — 1.66×, 1.72M rows/s, the
D4 ladder — are single-pod results. That means the run **understates** CQLite, which is the safe
direction for a public claim, but the 3-node numbers are unmeasured.

## Not run

- **`kv_small` was never created.** `cassandra-easy-stress` rejects `--table`
  (`ParameterException: Only one main parameter allowed`), so a second KeyValue table is not
  expressible through that CLI. D4 and D5 used `keyvalue` and `sensor_data` instead.
- **D2 cold** (`d2-scan-cold.csv`) — the `drop_caches` instrument does not work here (see D6), so a
  cold/warm pair could not be honestly separated. Reported missing rather than published warm-as-cold.
- **D7 / D8** (single-box refresh, Parquet export) — optional in #4137, not reached.
- **Cassandra GC-pause panel** for D1 — upstream swapped the metrics agent to the OpenTelemetry
  Java agent the same day (easy-db-lab #916), so the round-5 plan's series names no longer apply and
  no substitute was verified.

## Environment deviations (disclosed)

#4137's guardrail is "change no Cassandra setting beyond the disclosed heap and the D5 flush
period; record every deviation." **No Cassandra setting was changed** — no heap override was
applied. The deviations below are all build/access infrastructure and touch no measurement input.

1. **Both AMIs were rebaked for this run.** easy-db-lab tip replaces the MCAC/MAAC Cassandra
   metrics agent with the OpenTelemetry Java agent (upstream #916), which rewrites
   `cassandra.in.sh`, deletes `install_maac.sh`, and bumps the OTel Java agent 2.25.0 → 2.31.1 in
   the *base* image. The newest AMI in the account predated that by six weeks. **Nothing in the
   lab detects an AMI-vs-code mismatch**, so the alternative was a cluster whose Cassandra-side
   metrics were wrong or absent, silently. Verified positively rather than by exit status: zero
   MCAC/maac references in the Cassandra bake log, both #916 files installed, OTel agent
   `v2.31.1` in the base, Cassandra 5.0.9 staged.
2. **The packer security group's port-22 allowlist went 7 → 36 CIDRs.** This laptop egresses via
   Zscaler (`165.225.242.252`, zscalerthree.net SF IV) and the SG carried only stale `136.226.*`
   Zscaler entries, so the first bake failed at `Timeout waiting for SSH`. Note
   `checkip.amazonaws.com` reports `66.203.115.15` — inside an *already-allowed* `/24` — because
   that path is Zscaler-bypassed; `ip.zscaler.com` is the authoritative reading for the tunnelled
   path. Ruled out first: source-AMI validity, subnet public-IP assignment, IGW route, NACLs
   (default allow-all), instance health (console showed sshd host keys and a login prompt), and
   general outbound :22. Fixed with the published Salesforce/Zscaler egress CIDR list plus
   `165.225.242.0/24`; no `0.0.0.0/0` rule (policy auto-strips those).
3. **`cqlite.local-datacenter` was set explicitly** (`--local-datacenter us-west-2`) to work
   around **issue #4161**: the kit renders the literal token `__LOCAL_DATACENTER__` when the flag
   is omitted, contradicting its own comment that it "renders BLANK". Since that property governs
   split placement — the variable D2 measures — the default is measurement-corrupting rather than
   cosmetic. Worked around by configuration only; no code change, per #4137.

## Upstream observations (comment-only, never fixed in this tree)

- `easy-db-lab init --up` reported `IllegalStateException: Failed to create 1 infrastructure
  resource(s): Stress instances` with `The instance IDs '…' do not exist`, **after successfully
  creating all six instances** — an EC2 read-after-write race in the post-create describe, with no
  retry. It left six running instances and a half-finished cluster. A bare `easy-db-lab up`
  resumed cleanly without recreating anything.
- The lab CLI starts its SOCKS tunnel **per invocation** and tears it down on exit, so a kit's own
  `kubectl` (run from `bin/start.sh`) can fail with `dial tcp [::1]:1080: connect: connection
  refused`. Worked around with a persistent `ssh -D 1080` for the run — which matters beyond
  convenience, since D1 samples for 40 minutes and D5 for 20: a tunnel dying mid-window corrupts a
  measurement rather than merely failing a command.
- Trino 481 worker `CrashLoopBackOff` from the `web-ui.*` config placement (#2116) **did not
  reproduce** on upstream tip — coordinator and both workers reached `Ready` with 0 restarts.

## Teardown

Cluster `talk017` torn down with `easy-db-lab down` after all deliverables were committed **and
pushed**, and after draining VictoriaMetrics to `results/metrics/` — that store was inside the
cluster and teardown destroys it, so the drain had to precede the down. Confirmation is in the
final comment on #4137.

Not reverted, and deliberately so: the `sg-0ff2000d523e05f3f` port-22 CIDR additions (deviation 2
above). They are a build-infrastructure allowlist for the shared packer security group, not
cluster state, and removing them would break the next local AMI bake from this laptop.
