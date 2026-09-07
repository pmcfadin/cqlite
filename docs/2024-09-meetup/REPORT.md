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

**Not yet measured — no chart may cite this table until the row counts below are filled in from
`nodetool tablestats` / `sstablemetadata`.** Left deliberately empty rather than pre-filled with
targets, per #4137 ("Never report a chart against a table whose row count you did not measure").

| Table | Workload | Target | Measured rows | SSTables/node | On-disk bytes | Compression ratio |
|---|---|---|---|---|---|---|
| `keyvalue` | `KeyValue -n 312500 -p 20000000 -r 0.0 --threads 64` | 20M | _pending_ | _pending_ | _pending_ | _pending_ |
| `ts` | `BasicTimeSeries` | 20k partitions | _pending_ | _pending_ | _pending_ | _pending_ |
| `kv_small` | `KeyValue` | 5M | _pending_ | _pending_ | _pending_ | _pending_ |

**Row-count target reduced from 50M to 20M**, which #4137 explicitly permits provided it is
disclosed: *"If 50M does not fit the time budget, drop to 20M and say so in every chart title."*
Every chart title carrying this table therefore reads **20M**, not 50M. Measured client write
throughput during the load was **~4,800 ops/s** (120,001 ops over a 25 s window), which put the
20M load at ~68 minutes and 50M at ~2.9 hours of pure load time before any demo could start.

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

_(one paragraph per demo — chart, the number the slide can say, and the caveat that must
accompany it. Filled in as each demo lands; a demo whose result contradicts the story is still
reported.)_

| Demo | Status | Chart | Slide number | Caveat |
|---|---|---|---|---|
| D1 OLTP isolation | not started | `charts/d1-isolation.png` | — | — |
| D2 same SQL, two catalogs | not started | `charts/d2-scan-warm.png`, `charts/d2-scan-cold.png` | — | — |
| D3 time-series SQL | not started | `charts/d3-timeseries.png` | — | — |
| D4 concurrency ladder | not started | `charts/d4-ladder.png` | — | — |
| D5 freshness | not started | `charts/d5-freshness.png` | — | — |
| D6 cold start | not started | `charts/d6-coldstart.png` | — | — |

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

_(pending — `$EDB down` and confirmation go here and in the final comment on #4137)_
