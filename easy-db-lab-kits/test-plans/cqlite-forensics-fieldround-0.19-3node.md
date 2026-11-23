# Lab Plan: CQLite 0.19 Forensics Field Round (3-Node Cassandra 5.0.8 on freeside)

REVISED after owner rulings of 2026-11-23 (Q24=b freeside, Q25=a five tools, Q26=a + Q29=a eight tables: six core
plus two resurrection, Q27=a tools pod in the cluster, Q28=a cluster up for the whole run). DRAFT, not yet run.
No cluster exists; nothing external has been contacted. Proposed commit path:
`easy-db-lab-kits/test-plans/cqlite-forensics-fieldround-0.19-3node.md` (same naming family as
`cqlite-flight-loadtest-3node*.md`; note the plan no longer uses easy-db-lab, only the directory convention),
report beside it as `...-3node-report.md`.

## Goal

Issue #4503 (0.19 release criterion, owner ruling 2026-11-10). Point the read-only 0.19 forensics tools at
SSTables nobody built to be read: snapshots of a real 3-node Cassandra 5.0.8 cluster with real write load and
compaction history. Five tools: audit (`verify --mode audit`, #4195), find (#4205), diagnose (#4204), explain
(#4193) and diff (#4201). Vortex export (#4237) is exempt (its Parquet differential covers it).

Release criteria verified (`docs/development/release-0.19-plan.md`, Release criteria):

| # | Criterion | Where checked |
|---|---|---|
| C1 | Each tool completes on every table, exit codes recorded | Phase 4 |
| C2 | No false corruption on data `sstableverify` reports clean | Phase 4.3 |
| C3 | `find` and `explain` for a key sample match the live cluster at the snapshot point | Phase 4.4 |
| C4 | Each tool's peak RSS on the largest table is recorded and under 128 MB | Phase 4.6 |
| C5 | Damaged copies are reported as corruption: not a crash, not a silent pass | Phase 5 |
| C6 | diff: replica divergence and resurrection-risk scenarios behave as specified | Phase 6 |
| C7 | Every anomaly is a filed issue; a correctness anomaly in a 0.19 tool blocks the release | Phase 8 |

One bounded round, not a soak and not a benchmark. Routing per #4503: release validation, no code surface, no
OpenSpec. Bugs found are GitHub issues with preserved reproduction files.

## Scope notes (read first)

- **Five tools, diff included.** #4503's text lists four tools, and `release-0.19-plan.md` still lists diff #4201
  under "Moved to 0.20" (that note is stale: #4201 merged in #4611, its spec was archived in #4614, and the owner
  has moved it into 0.19). When this plan is committed, fix both: the plan doc's tier table and #4503's scope line.
- **#4612 is fixed on main** (`4cd9529a9`, #4615: the reader fails closed on a truncated or corrupt
  `Statistics.db`, naming the path). The damaged-copy section tests the fixed behavior; build from a commit that
  contains #4615.
- **Pinned binary.** One release-candidate build, recorded as full SHA plus `cqlite --version`. A rebuild voids
  results for the tools it changed.
- **Platform: freeside**, the LAN testbed (a vcluster with admin inside it), replacing easy-db-lab/AWS. Pod
  Security Baseline applies, no privileged pods, no NodePort/hostPort, no backups, expiry 7 days.
- **Tools run in a pod inside the cluster** (Q27), so the 128 MB budget is enforced by the pod memory limit as
  well as measured.
- **extract/split (#4199)** is not included. If it has landed, add one optional leg (extract one SSTable, load it
  back, read it); it never gates the release.
- Writers are out of scope. The plan checks that tool inputs are unmodified (checksum manifests).

## Platform: freeside

Follow `/home/dev/.claude/skills/freeside/SKILL.md`. Creating, using and deleting a cluster is authorized
development work (no spend, no external vendor). Use the `freeside_*` MCP tools only for the calls; kubeconfig and
renewal token live at `~/.freeside/<cluster>/` (0600); never print them, never run `kubectl config view --raw`.
Use `kubectl --kubeconfig <path>`.

Capacity at planning time: up to 12 CPU, 14 GiB memory, 400 GiB storage per cluster; per-cluster bounds 2-12 CPU,
2-16 GiB, 5-400 GiB. The cluster's own pods use about 750m CPU.

### Sizing and minimum request

Layout inside the vcluster (limits; requests set lower on CPU):

| Component | Count | CPU limit | Memory limit | Storage |
|---|---|---|---|---|
| Cassandra pod (`cassandra:5.0.8`, heap 1.5 GiB, young 400 MiB) | 3 | 2.5 | 3.5 GiB | 40 GiB PVC each |
| Tools pod, container `runner` (cqlite) | 1 | 1 | **128 MiB (the budget)** | shares tools PVC |
| Tools pod, container `oracle` (`cassandra:5.0.8` tools: sstableverify, sstabledump, sstablemetadata) | 1 | 1 | 1.5 GiB | 40 GiB tools PVC (RWO, mounted by both containers) |
| Generator Job (load phase only, deleted before the tools pod starts) | 1 | 1 | 512 MiB | none |
| Total steady | | about 9.5 | about 12.2 GiB | 160 GiB |

- **Request:** `cpu=12, memory_gi=14, storage_gi=200` (200 leaves room for the damaged-copy staging and a second
  snapshot generation).
- **Minimum** (`min_cpu=8, min_memory_gi=12, min_storage_gi=100`): below this the three-node cluster plus tools pod
  does not fit. If freeside grants less than the request but at least the minimum: keep 3 nodes at 3 GiB (heap
  1.25 GiB) and scale the data set in 1.5 by the ratio. If it refuses (`capacity_exceeded`, `pressure`,
  `needs_owner_approval`, `rate_limited`) report the refusal and stop; per the skill, do not retry or work around.
- Reuse first: `freeside_my_clusters`; use one with `local_credentials: true` if present and large enough (do not
  touch `unverified` entries). Otherwise `freeside_capacity`, then `freeside_create_cluster`.
- 14 GiB is unusually tight for 3 Cassandra nodes; the heap/off-heap split in the manifests below is the
  tuning. Probe, don't assume: run `kubectl top pod` after bring-up and note headroom in the report. If a pod is
  OOMKilled during load, record it as an environment finding and reduce load, not heap.

## Environment

- Cassandra: official `cassandra:5.0.8` image, 3-pod StatefulSet, `NetworkTopologyStrategy`, **RF 3**. With three
  nodes at RF 3, every node holds a replica of every partition, so any two nodes' snapshots of one table are two
  replicas of the same data (what diff needs). Single rack, DC `dc1` (`GossipingPropertyFileSnitch`).
- **Version pin:** after start, on every pod, `nodetool version` must print `ReleaseVersion: 5.0.8`; also record
  the image digest from `kubectl get pod -o jsonpath` (`imageID`). Any other version: stop.
- Egress: the cluster must pull `cassandra:5.0.8` and the runner base image, and the generator needs the Python
  driver. Probe egress at bring-up (a throwaway `kubectl run` pulling the image). No registry is assumed. If a
  pull fails, stop and report; do not work around by contacting other services. Fallback for the Python driver
  only: build a wheelhouse on the dev box (`pip download`) and `kubectl cp` it in.
- **Dev box** (this machine) only builds the binary and drives `kubectl`; it stages no snapshots (Q27), which
  avoids the 40 GiB disk bar.

## Prerequisites

### 0.1 Build and pin the binary (dev box)

```bash
git -C /home/dev/projects/cqlite fetch origin && git -C /home/dev/projects/cqlite log -1 origin/main
# build in a worktree at the release-candidate commit, default features (the Vortex export is not needed;
# if it were, LIBCLANG_PATH comes from ~/.profile)
cargo build --release -p cqlite-cli
./target/release/cqlite --version
./target/release/cqlite verify --help | grep -q audit
for v in diagnose find explain diff; do ./target/release/cqlite $v --help >/dev/null || echo "MISSING $v"; done
ldd ./target/release/cqlite; ldd /usr/bin/time
```

Any verb missing means the build predates the merge: stop. Verbs, flags and exit codes (read from `origin/main`
source): `verify <gen-dir> --mode audit [--deep] --out json` (exit 2 on any FAIL), `sweep <data-dir> --mode audit`,
`diagnose <table-dir> [--now] [--deep] [--top N] --out json`, `find <table-dir> <key> [--commitlog dir] --out json`
(needs `--schema`, exit 2 = unreadable generation), `explain <ks.table> <key> [--clustering ..] [--now]`,
`diff <dirA> <dirB> --table <ks.table> [--partition key] [--now] --out json` (exit 2 = unreadable side).

### 0.2 Getting the binary into the cluster (no registry)

Build on the dev box and `kubectl cp` it into the tools pod; no image registry is used.

- The `runner` container base image must have a glibc at least as new as the dev box's. Check
  `ldd --version` on the dev box and pick the matching `ubuntu:<release>` for the runner (or build with
  `--target x86_64-unknown-linux-musl` if the release feature set builds statically; try this only if the glibc
  match is a problem, since a musl build is not the binary users get).
- Copy: the `cqlite` binary, `/usr/bin/time` (GNU time, so `-v` works without network `apt`), the per-table
  schemas `fr.<table>.cql`, and the wrapper `run-tool.sh`.
  ```bash
  K="kubectl --kubeconfig $KC -n fr"
  $K cp target/release/cqlite tools:/work/bin/cqlite -c runner
  $K cp /usr/bin/time tools:/work/bin/time -c runner
  $K exec tools -c runner -- sha256sum /work/bin/cqlite     # compare with dev-box sha256; record both
  ```
  Release artifacts from GitHub are not used (no external fetch); the binary is whatever the dev box built from
  the recorded SHA.

### 0.3 Oracle tools

`sstableverify`, `sstabledump`, `sstablemetadata` come from the `cassandra:5.0.8` image in the `oracle`
container (5.0.8 tag, matching CLAUDE.md "Format authority"). They are the C2 and C3 oracles (a CQLite result is
never its own oracle).

## Steps

### 1. Create the cluster and Cassandra

1. `freeside_my_clusters`, then `freeside_capacity`, then `freeside_create_cluster` with the sizing above. Record
   cluster name, size granted, creation time T0 and expiry (7 days). Kubeconfig path only; never print it.
2. Namespace and checks: `kubectl create ns fr`; confirm Pod Security Baseline (no privileged pods in any
   manifest below); `kubectl get storageclass` (use the default; record its name and whether it is node-local;
   see "Unresolved" for RWO versus RWX).

Manifests (Baseline-compatible: non-root uid 999, no privileged, no hostPath, no hostPort, no NodePort).

```yaml
# headless Service: stable DNS for seed discovery and pod identity
apiVersion: v1
kind: Service
metadata: {name: cassandra, namespace: fr}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector: {app: cassandra}
  ports: [{name: cql, port: 9042}, {name: intra, port: 7000}]
---
apiVersion: v1
kind: ConfigMap
metadata: {name: cassandra-format, namespace: fr}
data: {format: "big"}          # flipped to "bti" in 1.2
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: cassandra, namespace: fr}
spec:
  serviceName: cassandra
  replicas: 3
  podManagementPolicy: OrderedReady
  updateStrategy: {type: RollingUpdate}
  selector: {matchLabels: {app: cassandra}}
  template:
    metadata: {labels: {app: cassandra}}
    spec:
      securityContext: {runAsNonRoot: true, runAsUser: 999, runAsGroup: 999, fsGroup: 999,
                        seccompProfile: {type: RuntimeDefault}}
      terminationGracePeriodSeconds: 120
      initContainers:
      - name: conf
        image: cassandra:5.0.8
        command: ["bash","-c"]
        args:
        - |
          cp -a /etc/cassandra/. /conf/
          if [ "$(cat /fmt/format)" = bti ]; then
            printf '\nsstable:\n  selected_format: bti\n' >> /conf/cassandra.yaml
          fi
          sed -i 's/^# *concurrent_compactors:.*/concurrent_compactors: 1/' /conf/cassandra.yaml
        securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
        volumeMounts: [{name: conf, mountPath: /conf}, {name: fmt, mountPath: /fmt}]
      containers:
      - name: cassandra
        image: cassandra:5.0.8
        env:
        - {name: CASSANDRA_CONF, value: /conf}
        - {name: CASSANDRA_CLUSTER_NAME, value: fr}
        - {name: CASSANDRA_DC, value: dc1}
        - {name: CASSANDRA_RACK, value: rack1}
        - {name: CASSANDRA_ENDPOINT_SNITCH, value: GossipingPropertyFileSnitch}
        - {name: CASSANDRA_SEEDS, value: "cassandra-0.cassandra.fr.svc.cluster.local"}
        - {name: MAX_HEAP_SIZE, value: 1536M}
        - {name: HEAP_NEWSIZE, value: 400M}
        ports: [{containerPort: 9042}, {containerPort: 7000}]
        resources: {requests: {cpu: "1500m", memory: 3500Mi}, limits: {cpu: "2500m", memory: 3500Mi}}
        securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
        readinessProbe:
          exec: {command: ["bash","-c","nodetool status 2>/dev/null | grep -E \"^UN +$(hostname -i)\""]}
          initialDelaySeconds: 90
          periodSeconds: 15
        volumeMounts: [{name: data, mountPath: /var/lib/cassandra}, {name: conf, mountPath: /conf}]
      volumes:
      - {name: conf, emptyDir: {}}
      - {name: fmt, configMap: {name: cassandra-format}}
  volumeClaimTemplates:
  - metadata: {name: data}
    spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 40Gi}}}
```

The `sed` for the conf file and the `CASSANDRA_CONF` handoff are the plan's expectation of how the official
image reads configuration; verify on the first pod (`kubectl exec cassandra-0 -- nodetool getconcurrentcompactors`
and the sstable file names after the first flush) before relying on it. Bring-up: `kubectl rollout status
sts/cassandra`; start order is ordinal, each waits for `UN`. All three must show `UN` in `nodetool status`.
Seed discovery uses the headless Service DNS of `cassandra-0`.

#### 1.1 Compaction history and tables

| Table | Strategy | Auto-compaction | Intended history |
|---|---|---|---|
| `t_basic` | STCS | on | compacts; ends via manual `compact` as one BTI generation |
| `t_coll` | LCS | on | levels; mixed BIG and BTI |
| `t_udt` | STCS | on | several generations |
| `t_wide` | TWCS (1 h window) | on | a few windowed generations |
| `t_static` | STCS | **off** | 8+ generations preserved |
| `t_tomb` | STCS | **off** | 8+ generations; every deletion kind and per-write TTL |
| `t_resurrect` | STCS, gc_grace 864000 | **off** | diff scenario A (Phase 6) |
| `t_resurrect_purged` | STCS, gc_grace 60 | on, then manual | diff scenario B (Phase 6) |

Table count: this plan has six core tables (`t_basic`, `t_coll`, `t_udt`, `t_wide`, `t_static`, `t_tomb`) plus the
two resurrection tables, eight in all; `t_ttl` and `t_comp` are dropped (owner Q26=a, count confirmed Q29=a
2026-11-23; composite keys are covered by a variant inside `t_static`).

Coverage: TTL (table default, per-write, short TTL expired at snapshot) is exercised in `t_tomb` and the two
resurrection tables (they carry `USING TTL` rows); composite partition keys are exercised in `t_static`
(`PRIMARY KEY ((pk, c1), c2, c3)` variant added to its DDL) so comma-separated key literals still get tested.

Schemas (abbreviated; the generator carries the full DDL):

```sql
CREATE KEYSPACE fr WITH replication = {'class':'NetworkTopologyStrategy','dc1':3};
CREATE TYPE fr.addr   (street text, city text, zip int);
CREATE TYPE fr.person (name text, born date, home frozen<addr>, tags set<text>);
CREATE TABLE fr.t_basic (id bigint, bucket int, a ascii, t text, i int, b bigint, s smallint, ti tinyint,
  f float, d double, dec decimal, vi varint, bo boolean, ts timestamp, dt date, tm time, u uuid, tu timeuuid,
  ip inet, bl blob, du duration, PRIMARY KEY ((id), bucket));
CREATE TABLE fr.t_coll (id uuid PRIMARY KEY, l list<text>, s set<int>, m map<text,int>,
  fl frozen<list<int>>, fm frozen<map<text,frozen<set<int>>>>, nested map<text,frozen<list<text>>>)
  WITH compaction={'class':'LeveledCompactionStrategy'};
CREATE TABLE fr.t_udt (id int, ck int, p frozen<person>, ps list<frozen<person>>, tp frozen<tuple<int,text,uuid>>,
  PRIMARY KEY (id, ck));
CREATE TABLE fr.t_wide (pk int, ck timestamp, v text, n int, PRIMARY KEY (pk, ck))
  WITH CLUSTERING ORDER BY (ck DESC) AND compaction={'class':'TimeWindowCompactionStrategy',
  'compaction_window_unit':'HOURS','compaction_window_size':1};
CREATE TABLE fr.t_static (pk text, c1 int, c2 text, s1 text STATIC, s2 map<text,int> STATIC, v blob,
  PRIMARY KEY ((pk), c1, c2));
CREATE TABLE fr.t_tomb (id int, c1 int, c2 int, v text, l list<int>, m map<int,text>, PRIMARY KEY ((id), c1, c2))
  WITH gc_grace_seconds = 864000;
CREATE TABLE fr.t_resurrect (id int, ck int, v text, PRIMARY KEY (id, ck)) WITH gc_grace_seconds = 864000;
CREATE TABLE fr.t_resurrect_purged (id int, ck int, v text, PRIMARY KEY (id, ck)) WITH gc_grace_seconds = 60;
```

Feature matrix (every row must be hit, checked after load):

| Surface | Table(s) |
|---|---|
| ascii/text/ints/floats/decimal/varint/blob/inet/uuid/timeuuid/date/time/timestamp/duration | `t_basic` |
| list/set/map multicell; frozen list/map; nested | `t_coll` |
| UDT, nested UDT, `list<frozen<udt>>`, tuple | `t_udt` |
| static columns, static map, multi-column clustering, composite partition key | `t_static` |
| wide partition (one partition over 100 MB), clustering DESC | `t_wide` |
| TTL (default and per-write; short TTL expired at snapshot) | `t_tomb`, resurrect tables |
| cell, row, range (single and multi-column prefix), partition, collection overwrite/element, column delete | `t_tomb` |
| BIG, BTI, mixed | all |
| STCS, LCS, TWCS | see table |

Not covered, by decision: counters, SAI, MV, CDC, compressed-write output (#1406), vector columns. Default LZ4
compression is on everywhere, so compressed READS are exercised. `list<frozen<udt>>` and vector issues (#4164,
#4165) are writer concerns, not read ones.

Schema files for the tools: `cqlsh -e "DESCRIBE KEYSPACE fr"` in `cassandra-0`, split into `fr.<table>.cql`
(the CLI's `--schema ks.tbl.cql` convention), and copied into the runner at `/work/schema/`.

#### 1.2 Mixed BIG and BTI generations (config change plus rolling restart)

Cassandra 5.0 writes BIG by default; BTI is `sstable: selected_format: bti`. In the StatefulSet above the
initContainer reads the `cassandra-format` ConfigMap. Procedure:

1. Phase A load with `format: big` (the default).
2. Flip and restart:
   ```bash
   $K patch cm cassandra-format --type merge -p '{"data":{"format":"bti"}}'
   $K rollout restart sts/cassandra
   $K rollout status sts/cassandra --timeout=30m
   ```
   `RollingUpdate` restarts pods in reverse ordinal order one at a time, each waiting for readiness (`UN`) before
   the next. Before each restart the pod does a graceful shutdown (`terminationGracePeriodSeconds: 120`;
   optionally `nodetool drain` via a preStop hook so no commitlog replay muddies the history).
3. Phase B load. Flushes and compactions after the flip emit BTI; generations written before stay BIG, so
   tables have mixed generations, except `t_basic`, which gets a final `nodetool compact` and ends as one
   BTI generation.
4. Confirm per pod: `*-nb-*-Data.db` and `*-da-*-Data.db` both present (`ls /var/lib/cassandra/data/fr/*`);
   version floor is `na`/`nb` and `oa`/`da` only; any other version string is an anomaly to file.

### 1.3 Load generation and data size

No built-in stress workload covers UDTs, nested collections, statics and every deletion kind, so a deterministic
Python generator (DataStax driver, committed with the report) runs as a Kubernetes Job (non-root, 512 MiB) against
the `cassandra` Service. It is seeded (`--seed 20261123`), uses explicit `USING TIMESTAMP` for shadowing
scenarios (so outcomes are known by construction), writes at `LOCAL_QUORUM`, and journals every write to
`ops.jsonl` (the generator-side record, used for key sampling and mismatch triage, NOT an oracle for CQLite).
Phases: A (BIG) then restart (1.2) then B (BTI); `nodetool flush fr` every 120 s for the first 20 minutes of each
phase so several generations exist before compaction.

Data size is smaller than the AWS draft but well above the budget being tested:

| Table | Rows (all RF 3) | Notes | Approx. per node |
|---|---|---|---|
| `t_basic` | 8 M | 5 clustering rows per partition | ~1.0 GB (largest table) |
| `t_coll` | 2 M | collections 5-50 elements | ~0.5 GB |
| `t_udt` | 2 M | | ~0.4 GB |
| `t_wide` | 4 M | 40 partitions, the largest ~150 MB | ~0.7 GB |
| `t_static` | 3 M | | ~0.4 GB |
| `t_tomb` | 2 M live + 1 M deletions | every kind | ~0.3 GB |
| resurrect tables | 3 000 rows each | Phase 6 | trivial |

About 3.3 GB per node, about 10 GB across the cluster, snapshots hardlinked so no growth on the node PVC.
Gates: the largest table must be at least 8x the 128 MB budget per node (about 1 GB) and the largest partition
over 100 MB, so the memory claim is tested; if after load either is smaller, extend the load before
snapshotting. Record the real largest table and partition (from `nodetool tablestats` and, later, `diagnose
--deep`). Load time on a small shared vcluster is uncertain (estimated 5-8 k writes/s, so about 1 to 1.5 h for
the 20 M rows); settle after load, bounded at 30 min (`nodetool compactionstats` until empty, except tables with
auto-compaction off). Cassandra tuning for the small footprint: `concurrent_compactors: 1`, heap 1.5 GiB; if a
pod is OOMKilled, reduce concurrency in the generator.

### 2. Quiesce and ground truth

1. `nodetool compactionstats` empty on all pods; auto-compaction disabled where 1.1 says so, done BEFORE load.
2. `nodetool flush fr` on all pods; `nodetool compact fr t_basic` on all pods.
3. Record per pod and table: `tablestats` (SSTable count, space, partition estimate), `ls` of data
   directories, `sstablemetadata` for every generation. This is the independent expected generation count and
   size for diagnose.
4. Sample keys per table from the journal into `samples/<table>.keys`: 100 existing keys (overwritten, TTL,
   tombstoned, static-bearing, the largest `t_wide` partition) plus 20 never written.
5. Probe: re-check version 5.0.8 on every pod.

### 3. Snapshot procedure and getting snapshots into the tools pod

Per pod: `nodetool snapshot -t $TAG fr` (hard links, flushes first). Writes are stopped (generator done). The
three snapshots are taken within about a minute. The system_schema directory is also snapshotted (see Unresolved
item 2, offline `sstableverify`).

**Transfer method: stream `tar` over two `kubectl exec` calls, per pod and per table, not `kubectl cp` and not a
shared PVC.**

```bash
for n in 0 1 2; do for t in $TABLES; do
  $K exec cassandra-$n -- bash -c "cd /var/lib/cassandra/data/fr/${t}-*/snapshots/$TAG && sha256sum * > /tmp/$t.sha256 && tar -cf - . /tmp/$t.sha256" \
    | $K exec -i tools -c oracle -- bash -c "mkdir -p /snap/db$n/fr/$t && tar -C /snap/db$n/fr/$t -xf -"
done; done
```

Why: the Cassandra data PVCs are ReadWriteOnce and node-bound (a shared PVC between three source pods and the
tools pod is not guaranteed available, and mounting Cassandra's PVC into another pod risks racing the live
node); `kubectl cp` stages through the dev box and its 40 GiB disk bar and fails on large trees; piping tar
streams the data through the dev box without writing it there. Mitigation for exec-stream drops: one table at a
time, and every table is verified by checksum on arrival (`sha256sum -c` in the oracle container against the
manifest made on the source pod); a mismatch means retransfer. Layout under `/snap/<node>/fr/<table>/` is a flat
table directory of generations (the shape `verify`, `diagnose`, `find` and `diff` take); `sweep` uses the same
root. Table-id directory names are recorded.

After copy: `chmod -R a-w /snap` in the oracle container, and `inputs.sha256` of everything, re-computed after
every phase; any change is a blocker (the tools are read-only by contract). Snapshots stay on the node PVCs until
the transfer verifies; do not `clearsnapshot` before. `nodetool snapshot` data on the cluster is discarded with
the cluster; the tools pod PVC is too (freeside keeps no backups: results go to Git or the NAS, see Artifacts).

Never run the tools on a live data directory. Live comparison queries (Phase 4.4) run only after ALL snapshots,
including Phase 6's, are copied and checksummed, because CL=ALL reads can read-repair live state.

## Phase 4: Per-table tool runs and checks

All runs execute in the `runner` container (`kubectl exec tools -c runner -- ...`) against `/snap`, one table at a
time, nodes db0, db1, db2 all included. Output to `/work/out/<node>/<table>/<tool>.{json,err,time}`, copied out at
the end (Artifacts).

### 4.1 Invocations

`NOW` = snapshot time plus 1 h (epoch seconds); a second run with `NOWFAR` = snapshot time plus 30 days exercises
gc_grace logic. The wrapper `run-tool.sh` records command line, exit code, wall seconds, `time -v` max RSS KiB
and the container's `memory.events` OOM-kill count before and after.

```bash
M="/work/bin/time -v -o $OUT/%TOOL%.time"
cqlite verify $TD --mode audit --out json        > audit.json
cqlite verify $TD --mode audit --deep --out json > audit-deep.json
cqlite verify $TD --mode full --out json         > verify-full.json
cqlite sweep /snap/$node --mode audit --out json > sweep-audit.json
cqlite diagnose $TD --now $NOW --out json        > diagnose.json
cqlite diagnose $TD --now $NOW --deep --top 10 --out json > diagnose-deep.json
cqlite --schema /work/schema/fr.$tbl.cql find $TD "$KEY" --out json         > find-$i.json
cqlite --schema /work/schema/fr.$tbl.cql explain fr.$tbl "$KEY" --now $NOW --out json > explain-$i.json
```

(`$M` prefixes each.) Confirm against `--help` of the pinned build: global `--schema` spelling; whether `verify`
takes a multi-generation table directory (its help says generation directory; use `sweep` for per-table coverage
if not); and how `explain` is pointed at an offline staged data root (Unresolved item 1).

### 4.2 Checks per tool

"Healthy" means `sstableverify` (and `-e`) exits 0 on every generation of that table.

| Tool | C1 completes | C2 no false corruption | Output sanity |
|---|---|---|---|
| audit | exit 0 every table-dir; JSON parses | every invariant row `pass` or `skip(<cause>)` where sstableverify is clean; any `fail` on a clean generation is a FALSE CORRUPTION finding; `0 invariants MEASURED` anywhere is a failure | each `skip` carries a cause and is plausible (`compression-coverage` pass since LZ4 is on; `bti-trie-bounds` skip on BIG, pass on BTI; `bloom-no-false-negative` pass where Filter.db exists); an all-skip table fails; `--deep` `statistics-vs-scan` passes |
| diagnose | exit 0, JSON parses | an unreadable-generation field on a clean generation is a finding | per-generation fields equal `sstablemetadata` (min/max timestamp, LDT, partition estimate, droppable estimate, repaired-at, compression ratio); generation count equals `ls` and `tablestats`; every number has a source tag or `unmeasured: <cause>`, no bare `0` where the source is absent; `--deep`: partition count and the largest `t_wide` partition equal `sstabledump`'s; top-N tombstone list on `t_tomb` confirmed by `sstabledump` |
| find | exit 0 on all sampled keys | no unreadable generation on clean data | per-generation `hit` set equals the generations where `sstabledump -k <key>` finds the partition; absent keys: `bloom-negative` or `index-miss`, never `hit`; hit max-writetime equals sstabledump's maximum; partition-deletion flag matches; BIG and BTI both appear |
| explain | exit 0 on all sampled keys | no unreadable generation on clean data | `winner` set equals the live row (4.4); verdicts all from the closed vocabulary; `t_tomb` covers partition, range, row, cell, collection tombstones; absent key says `0 generations hold this key` with each probe result |
| diff | exit 0 on all identical-replica pairs; exit 1 or 2 is a failure | any `only-*`/`newer-*`/`resurrection-risk` on untouched tables for healthy replicas is explained from the journal or filed | symmetry: `diff A B` mirrors `diff B A`; `diff X X`: all `equal`, never a vacuous "0 partitions" on a non-empty table |

Every (table, node, tool) cell must have a recorded exit code (C1); empty cells fail the round.

### 4.3 sstableverify differential (C2)

For every generation, in the `oracle` container, `sstableverify fr <table>` and `sstableverify -e fr <table>`
on the staged tree (how this runs offline is Unresolved item 2). Any disagreement either way is a bug: preserve
the generation files in `/anomalies/<id>/` with sha256 lists. Artifact: `verify-differential.tsv`.

### 4.4 Live comparison for explain and find (C3)

The cluster is up for the whole run (Q28=a), so live CQL is the oracle: after all snapshots are copied, for the
sample keys of every table (100 per table), `cqlsh` in a Cassandra pod, `SELECT *, WRITETIME(col), TTL(col)` at
`CONSISTENCY ALL`. Compare TTL expiry instants (write time plus ttl), not remaining TTL. Compare per node to
`explain`: the set of `winner` cells and their values and writetimes equals the live row in both directions
(#3890); static and collection cells element-wise; tombstoned keys: no live row, no winner, a
`shadowed-by-tombstone` of the right kind; expired rows: absent live and `expired` in explain. For `find`: a key
with at least one non-deleted `hit` must exist live; all generations absent means absent live (generation sets
themselves are checked against `sstabledump` in 4.2, since CQL cannot name generations). A mismatch is
investigated first (re-run `explain` at the CQL read time, confirm snapshot and live are the same point), then
filed or recorded with its cause; no silent clock waiver.

### 4.5 CommitLog leg for find

Before snapshots, on one pod (cassandra-1) write 50 `t_basic` keys and do not flush or drain that pod; copy its
`/var/lib/cassandra/commitlog` with the same tar pipe. Expected: `find --commitlog` lists those mutations as
`unflushed: yes` (or an explicit `unmeasured(<cause>)` for shapes the decoder does not model); earlier flushed
keys `unflushed: no`. A crash or wrong `unflushed` is a finding. If the commitlog copy isn't self-consistent,
record that and skip (least important leg).

### 4.6 Memory (C4)

Budget: **128 MB = 131 072 KiB**, enforced two ways.

1. **Pod memory limit.** The `runner` container has `limits.memory: 128Mi`. A tool that exceeds it is OOMKilled
   (`kubectl describe pod` reason `OOMKilled`, and `memory.events` `oom_kill` count); that is a FAIL for the tool
   on that table. The limit sees cgroup memory including mapped file pages, which is stricter than RSS alone.
   Reading files through the page cache is reclaimable and does not trigger the limit by itself.
2. **Measured max RSS** with `/work/bin/time -v` (`Maximum resident set size (kbytes)`) on every invocation.
   Record per tool: the maximum across all tables, the value on the largest table (the 4.1 per-table runs), and
   for `diagnose --deep`/`explain`/`find` the largest `t_wide` partition. Largest-table cases run three times,
   median and max recorded. If a value is near or over budget, sample `/proc/<pid>/status` (`RssAnon`,
   `RssFile`) at 100 ms and record both; an over-budget value is a finding until ruled on, even if mostly
   file-backed.

PASS: no OOM kill, and every tool's peak RSS on the largest table and across all tables under 131 072 KiB. The
`oracle` container (1.5 GiB) is for the Cassandra tools only and its memory is not part of the claim. `diff` over
the largest-table pair must also fit (its own `max_result_bytes` cap is 32 MiB, exit 2 if exceeded; treat that
exit on this data as a finding with the divergence size). `sweep --mode audit` over a whole node is recorded but
is not the C4 evidence. Because the cluster runs on a shared testbed, wall times are informational only.

## Phase 5: Deliberately damaged copies (C5)

Purpose: confirm the tools report corruption, not crash, and not pass silently.

Method: damage a COPY (`cp -a` inside the oracle container into `/dmg/<case>/`, never `/snap`, never hard
links) of one small healthy table per format: `t_resurrect` (BIG generations) and a BTI table from the phase-B
generations (e.g. `t_static` BTI generation). If a table has several generations, damage one named generation so
"names the damaged generation" is testable. Each case has a `damage.sh` recording offset and size and an
`expected.txt` derived from the CLEAN copy (offsets, sizes, checksums), never from CQLite output. Run the same
tools as Phase 4 on each case in the `runner` (so the 128 MB limit also applies on damaged input).

| Id | Damage | Why |
|---|---|---|
| D1 | `Statistics.db` truncated: (a) half, (b) 0 bytes, (c) 4 bytes | #4612, fixed in #4615 |
| D2 | `Filter.db` replaced by a 0-byte file (BIG only) | owner ruling A3.4: unreadable `Filter.db` is a skip |
| D3 | one byte flipped mid-`Data.db` | Digest/chunk-CRC |
| D4 | `Data.db` truncated by 4 KiB | length checks, index past EOF |
| D5 | BIG `Index.db` byte flipped in an entry position; BTI `Partitions.db` byte flipped | index identity, trie bounds |
| D6 | `Digest.crc32` edited | digest-matches-data |
| D7 | `TOC.txt` lists a component whose file is removed; and an extra file | toc-exact-listing |
| D8 | `Summary.db` truncated (BIG) | summary correlation / open error |
| D9 | `CompressionInfo.db` chunk table truncated | compression-coverage |
| D10 | control: undamaged copy | all tools exit 0, no fabricated finding |

Acceptable outcome per tool on D1-D9 (all listed properties must hold):

| Tool | Acceptable outcome |
|---|---|
| audit | exit 2 (a FAIL row) or a named error; the report names the file or invariant; never exit 0 with all-pass. D6 fails `digest-matches-data`; D7 fails `toc-exact-listing`; D3 fails `digest-matches-data`; D2 shows the bloom invariant `skip(<cause naming Filter.db>)` and the rest run on their merits; D1 gives a named error pointing at `Statistics.db` (#4615). Per #4612's acceptance, re-check how audit and diagnose behave on an unreadable `Statistics.db` (A3.4 covers `Filter.db` only; a skip is NOT assumed for `Statistics.db`) |
| verify `--mode full` | non-zero exit, finding named; D3/D4/D9 by chunk-CRC or length |
| diagnose | exit 0 with the generation shown unreadable and its cause (per `--help`), or a non-zero named error; never plausible numbers for the damaged generation; D1: no Statistics-derived numbers for it, other generations still reported |
| find | exit 2, generation named; never `absent`/`index-miss` for a key the clean copy had as `hit`, EXCEPT D2 (missing Filter.db reports `index-miss` by design; a present key must still be `hit`) |
| explain | non-zero exit naming the generation (never a shorter trace); D1 must fail with a named error, NOT return swapped columns or 2015-rebased timestamps (#4612 symptom); D2 still works with the same `winner` set as clean |
| diff | exit 2 naming the side and the path; D1 blocked by the reader and by #4611's pre-flight; never a report of equal cells |

Failure conditions for any tool and case: panic text (`panicked at`), signal death or exit 101/134/139, an
OOMKill, empty stdout with exit 0, an all-pass report, or an error naming no file or invariant. No tool may
change a byte of the damaged input (checksums before/after). D1 additionally: `cqlite query` (SELECT *) on the
truncated-Statistics copy versus the healthy copy confirms the #4612 symptoms are gone (a named error). Results
are tabulated (case by tool: exit code, first stderr line, verdict).

## Phase 6: diff, replica comparison and resurrection risk (C6)

### 6.1 Healthy replicas

For every table, `diff` over each node pair (db0-db1, db0-db2, db1-db2) at `NOW` and `NOWFAR`, plus `diff X X`
and both directions for symmetry. Pass: exit 0 everywhere; a quiescent cluster with no node loss is expected to
show no divergence, and any divergence is explained from the journal (e.g. a write that failed quorum on one
node) or filed. The largest-table pair is the diff memory subject (4.6). Independent oracle: `sstabledump` of the
two sides reconciled by a small non-CQLite script, used for the scenario tables; for healthy replicas, "no
divergence reported" plus `sstabledump -k` spot checks.

### 6.2 Scenario A: tombstone present (past gc_grace at `--now`)

Table `t_resurrect` (gc_grace 864000), 1 000 partitions by 3 rows (some rows written with `USING TTL`).

1. Write the 3 000 rows at `LOCAL_QUORUM`; flush all pods; snapshot nothing yet.
2. `nodetool disablehandoff` on cassandra-0 and cassandra-1 (and hints off in config for the run) so cassandra-2
   cannot be healed.
3. Take cassandra-2 offline without losing its data: `nodetool drain` on it, then
   `kubectl scale sts/cassandra --replicas=2` (the highest ordinal is removed; its PVC is retained). Verify the
   others see it `DN`.
4. On cassandra-0/1 at `CONSISTENCY TWO`: delete 100 partitions, 200 rows and 100 ranges; flush those two pods;
   do NOT compact. Record the deleted keys and timestamps from the generator.
5. Snapshot cassandra-0 and cassandra-1 (`$TAG-resA`). `kubectl scale sts/cassandra --replicas=3`, wait `UN`,
   and snapshot cassandra-2 IMMEDIATELY, before any read at a level that read-repairs, with hints off and no
   repair. If cassandra-2 shows the deletions healed, record it and redo with another technique (copying its
   PVC data while it is scaled down, using a throwaway pod that mounts the retained PVC; note whether the
   storage class allows that).
6. Transfer and checksum (Step 3).
7. Runs (db0 = A, db2 = B, `--table fr.t_resurrect`):
   - `--now` = snapshot time (within gc_grace): `resurrection-risk: yes / before-gc-grace`
   - `--now` = snapshot time plus 11 days: `resurrection-risk: yes / past-gc-grace`
   - db1 vs db0: no tombstone divergence (both saw the deletes)
   - A/B swapped: mirrored report
   - `--partition <one deleted key>`: only that partition
8. Expected outcomes come from the generator's deletion journal and `sstabledump` of both sides, not from diff:
   reported keys equal the deleted keys in both directions, tombstone side `only-a` with the right kind
   (partition/row/range), db2's live cells `only-b`; undeleted keys absent from the report. Declared limit
   (#4601): a row whose only victim is its liveness marker reports no risk; the generator writes cells in every
   row, so any such case is a generator bug, confirmed by the journal.

### 6.3 Scenario B: tombstone already purged (documented blind spot)

Table `t_resurrect_purged` (gc_grace 60 s). Repeat 6.2 steps 1-4, wait 90 s, run
`nodetool compact fr t_resurrect_purged` on cassandra-0/1 so the tombstones are purged, snapshot all three
(`$TAG-resB`). Expected: A (db0, purged) vs B (db2, live): db2's live cells reported `only-b`; no tombstone exists
anywhere to classify; the report says so plainly and does not invent `resurrection-risk: yes` or call the data
`equal`. Recorded as a documented limit unless the report is wrong about which cells are only on db2
(cross-check with `sstabledump`).

### 6.4 Read-only and resource checks

Input checksum manifests unchanged after every diff run; peak RSS and OOM events per 4.6 on the two largest-table
dirs and on the scenario tables. If extract/split (#4199) has landed: an optional leg may extract the db2-only
cells and load them back; never release-gating.

## Phase 7: Teardown (mandatory)

The cluster stays up through Phase 6 and the live comparison (Q28=a) so live CQL remains available; delete it
after the report's data is saved.

1. Copy results out of the cluster (Artifacts): `/work/out`, `/anomalies`, `inputs.sha256`, `memory.events`
   captures. Freeside keeps no backups; results go into Git (small) or the NAS (large), not the cluster.
2. Renewal: the cluster expires after 7 days from T0. The round is expected to take well under that; if the
   run will pass 7 days, call `freeside_renew` before expiry (record each renewal), or stop and raise it with the
   owner. A long-lived cluster is the owner's or Dumont's decision.
3. `freeside_delete` the cluster (it is a task-scoped cluster; do not keep it unless the owner says so).
4. Verify: `freeside_my_clusters` no longer lists it; the credentials directory `~/.freeside/<cluster>/` is
   removed (the delete or manual clean-up of the local kubeconfig/token files; also remove any old
   `create.json` from curl runs). Record T1.
5. The report states: "Cluster `<name>` (size granted `<cpu>/<mem>/<storage>`) deleted with `freeside_delete` at
   `<T1>`; `freeside_my_clusters` confirms it no longer exists; local credential files removed." The report is
   not final until this is true; if deletion fails, say so in the report and on #4503.

## Phase 8: Failure triage to issues (C7)

Every anomaly becomes a GitHub issue before the report is final, titled `tool: symptom`, labeled `validation` and
`testing`, milestone 0.19 if it is a correctness anomaly in a 0.19 tool (which blocks the release), referencing
#4503, with the files that show it preserved (directory, sha256 list, command, exit code, stderr, minimal repro).
Platform defects (freeside, image) are reported to the owner, not filed in the CQLite tree.

| Failure class | Symptom / trigger | Data to capture |
|---|---|---|
| False corruption | a tool reports FAIL/unreadable where `sstableverify` accepts | generation files, `sstableverify -e` output, tool JSON |
| Missed corruption | a damaged copy passes, or exit 0 with all-pass | damage script, offsets, outputs |
| Crash / OOMKill | panic, signal, exit 101/134/139, `OOMKilled` | stderr, `memory.events`, `describe pod`, files |
| Wrong answer | find/explain/diff/diagnose disagrees with sstabledump, sstablemetadata or live CQL | key, generation, both outputs |
| Over budget | RSS above 131 072 KiB | command, table size, RSS trace (anon vs file) |
| Not offline-usable | a tool cannot be pointed at a staged snapshot (e.g. explain) | exact failing command |
| Input mutated | checksum manifest differs after a run | before/after manifests |
| Vacuous pass | 0 rows or `0 invariants MEASURED` on non-empty data | table, output |
| Environment | wrong Cassandra version, quorum write failures, OOMKilled Cassandra pod, refused freeside request | journal, nodetool, `describe pod`, refusal text |

## Artifacts kept

Committed under `easy-db-lab-kits/test-plans/cqlite-forensics-fieldround-0.19-3node/` (small, reviewable):
the run report, generator script, DDL, manifests (StatefulSet, Service, ConfigMap, tools pod, Job), `run-tool.sh`,
schemas, damage scripts and `expected.txt`, `results.tsv` (table x node x tool: exit code, seconds, max RSS KiB,
OOM count), `verify-differential.tsv`, `sstablemetadata` and `tablestats` captures, sample key lists, journal
summaries, `inputs.sha256` before/after, the binary SHA and `cqlite --version`, image digests.

Kept but not committed (large), on the NAS or a path the owner names, since freeside keeps no backups: raw tool
stdout/stderr (`raw.tar.zst`), and `/anomalies/<issue>/` for every file attached to a filed issue (issue bodies
link the checksums, not multi-GB files). The full snapshot copies (about 10 GB) live on the tools PVC and are
destroyed with the cluster, so anomaly files must be copied out BEFORE deletion. Not retained anywhere: kubeconfig,
renewal token, any secret.

## Run report template

`easy-db-lab-kits/test-plans/cqlite-forensics-fieldround-0.19-3node-report.md`:

```markdown
# 0.19 forensics field round: run report (#4503)

- Plan: easy-db-lab-kits/test-plans/cqlite-forensics-fieldround-0.19-3node.md @ <sha>
- cqlite binary: <full git sha>, `cqlite --version`: <...>; sha256 on dev box and in pod match: yes/no
- Cassandra: 5.0.8 on all 3 pods (nodetool version attached), image digest <...>
- Platform: freeside cluster <name>, size granted <cpu/mem/storage> (requested 12/14/200, minimum 8/12/100),
  created T0 <...>, expiry <...>, renewals <none|...>
- Layout: 3 x cassandra (3.5 GiB, heap 1.5 GiB), tools pod (runner 128 MiB limit, oracle 1.5 GiB), RF 3
- Data: rows and GB per node per table, SSTable counts, formats BIG/BTI/mixed, strategies; largest table and
  largest partition
- Operator/session: <...>

## Verdict
PASS | FAIL | PARTIAL with the one-line reason; C1 to C7 each PASS/FAIL with a link.

## What ran
tool x table x node: command, exit code, seconds, peak RSS KiB, OOM count (link to results.tsv)

## Release criteria
C1 completion; C2 sstableverify differential; C3 find/explain vs live (keys sampled, matches, mismatches);
C4 memory: per tool max RSS on the largest table (name, size), max across tables, anon vs file, OOM kills;
C5 damaged copies table; C6 diff (healthy, scenario A, scenario B, symmetry, read-only evidence)

## Issues filed
#N (short description), severity, blocks release? (correctness anomaly in a 0.19 tool: yes)

## Not covered
counters, SAI, MV, CDC, compressed-write output, vector columns, extract/split leg (state if run)

## Deviations from the plan
what and why (including anything scaled down to fit the granted size)

## Teardown
"Cluster <name> deleted with freeside_delete at <T1>; freeside_my_clusters confirms it no longer exists; local
credential files removed." Where results and anomaly files were copied before deletion.
```

## Time and approvals

No spend: freeside is the LAN testbed. Time estimates (cluster up): create and bring-up 20-30 min, load 1-1.5 h
(including the format flip and rolling restart), settle 30 min, snapshot/transfer 30-45 min (about 10 GB via exec
streams, longer if the vcluster network is slow), live comparison and Phase 6 state building 60 min, tool runs
(offline wrt. nothing, in-pod) 3-4 h mostly unattended, Phase 5 about 1.5 h, report 2 h: about 9 to 11 h of
cluster time, one to two operator sessions, well inside the 7-day expiry.

Approvals: creating, using and deleting a freeside cluster is authorized development work per the freeside skill
and the owner has chosen the platform (Q24=b), so no further spend approval is needed. A refusal
(`capacity_exceeded`, `pressure`, `needs_owner_approval`, `rate_limited`) is final for that request and is
reported to the owner. Anything needing privileged pods beyond `freeside_allowlist` needs the owner through
Dumont; this plan needs none. Nothing external beyond pulling images from the registries the cluster already
reaches; no other external service. Doc edits on commit: #4503 scope line and the `release-0.19-plan.md` tier
table (diff to 0.19).

## Unresolved points (for the owner or the first dry run)

1. **explain on offline snapshots.** `explain <ks.table> <key>` resolves its table directory "as `query` does"
   (from configuration), with no positional directory. How to point it at `/snap/<node>` (a data-root setting?)
   must be read from the pinned build's `--help` before the run. If it cannot read a staged copy, that is itself
   a finding (explain unusable offline) and blocks C3 for explain until resolved.
2. **sstableverify offline.** Cassandra's offline tools need schema and a data-dir layout. Expect to copy
   `system_schema` (snapshotted from a pod) into the oracle container's staging data dir, or run against a
   scratch node. Prove this on a small table in a dry run before the C2 differential counts.
3. **Pod memory limit semantics.** The 128 MiB cgroup limit counts mapped file pages and sees the shell. It is
   stricter than RSS, so a kill may reflect page accounting rather than CQLite's heap; the plan records both
   and a kill is a finding until ruled on.
4. **Storage class.** Unknown until the cluster exists: RWO vs RWX, node-local or not, and whether a retained
   PVC can be mounted by a scratch pod (used in the 6.2 fallback).
5. **Memory fit.** 14 GiB for three Cassandra pods at 3.5 GiB plus tools is tight, and the vcluster's own
   overhead is not counted in my table. The minimum is 12 GiB with scaled-down data.
6. **Table count (resolved).** Owner Q29=a (2026-11-23): eight tables, six core plus two resurrection.
7. **Egress and image pulls.** `cassandra:5.0.8`, the runner base and the Python driver need to be reachable from
   the vcluster; probed at bring-up, and a failure stops the run (the Python driver has a wheelhouse fallback).
8. **Glibc match** between the dev-box build and the runner image (0.2) and `cassandra:5.0.8` image digest
   recorded rather than assumed.

## Validation checklist (copy into the run report)

- [ ] Binary SHA contains #4615 and #4611; `--version` recorded; sha256 matches in pod
- [ ] `freeside_capacity` checked; cluster created at granted size >= minimum; name and size recorded
- [ ] All 3 pods report Cassandra 5.0.8; image digest recorded; RF 3 confirmed
- [ ] Tables and strategies as 1.1; BIG and BTI (and mixed) generations present after the rolling restart (1.2)
- [ ] Feature matrix hit; largest table (>= ~1 GB/node) and largest partition (> 100 MB) recorded
- [ ] Snapshots taken at a quiesced point; tar-streamed; sha256 verified in the oracle container; /snap read-only
- [ ] audit, diagnose, find, explain, diff: exit codes on every table, node and sampled key (C1)
- [ ] `sstableverify` differential: no disagreement, or each is an issue with files (C2)
- [ ] find and explain agree with sstabledump and live CQL for sampled keys (C3)
- [ ] Peak RSS per tool under 131 072 KiB, no OOMKill in the 128 MiB runner; anon vs file recorded (C4)
- [ ] D1-D10: corruption reported with a named file or invariant; no crash, no silent pass (C5)
- [ ] Truncated Statistics.db fails closed (D1); empty Filter.db is a skip (D2)
- [ ] Phase 6 A: before-/past-gc-grace risk for exactly the deleted keys; B: purged tombstone shows as `only-b`
- [ ] Diff symmetry and read-only checks pass; inputs unchanged after every phase
- [ ] Every anomaly is a filed issue (C7); correctness anomalies flagged release-blocking
- [ ] Results copied out; `freeside_delete` run; `freeside_my_clusters` confirms it is gone; teardown stated with T1

## Notes

- Never print the kubeconfig or the renewal token; never `kubectl config view --raw`.
- Do not use a CQLite result as the oracle for another CQLite result.
- Do not run any 0.19 tool against a live data directory; only on staged read-only copies.
- A script or subagent running the tools must never use `pkill`/`killall` on the shared account.
- If `cqlsh` returns `No node was available` while `nodetool status` shows all `UN`, wait 30 s and retry.
