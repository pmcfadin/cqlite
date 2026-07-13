# cqlite-flight + Trino loadtest harness — run log & handoff

**Purpose:** Living record of the #2103 harness bring-up on a real AWS cluster —
what was done, every bug found, and where the run stands — so another agent (or
person) can pick up without re-deriving it.

- Epic: [pmcfadin/cqlite#2103](https://github.com/pmcfadin/cqlite/issues/2103)
- Test plan executed: `easy-db-lab-kits/test-plans/cqlite-flight-loadtest-3node.md`
- Run started: 2026-07-06 (cluster `cqlite-flight-loadtest`, region us-west-1, account 417835013894)
- Executed **directly via Bash** (the `easy-db-lab:run` plugin skill was not installed in the session; user authorized running the plan steps by hand).

## TL;DR status — RUN COMPLETE (12 issues filed)

The full stack was brought up and exercised end-to-end on a real 3-DB + 1-app
cluster. **The documented happy-path does not work without fixes** — 8 hard
blockers had to be worked around by hand. Once patched, the stack functions:
writes OK, SSTables + compaction OK, `cqlite-flight` reads/merges SSTables
correctly, `SHOW CATALOGS` lists `cqlite`, the connector plugin (0.13.1, JDK-25)
loads, and the `trino-loadtest` driver runs and connects.

**Two things fundamentally don't work on the published artifacts:**
1. **Observability emits nothing (#2128)** — the `cqlite-flight` image is built
   without `--features observability`, so `CQLITE_OTEL_*` is inert. No metrics,
   no traces. This defeats the epic's "watch the internals" goal. The kit-side
   wiring (env, collector reachability, dashboard) is all correct — verified.
2. **Queries are unbounded full-scans (#2129)** — no LIMIT/count pushdown, so
   `SELECT * ... LIMIT 5` full-scans ~2M rows (235s, then "Network closed"). The
   read is *correct*, just not bounded. The loadtest driver ran but logged
   qps=0.00 because every built-in query exceeds the reporting interval.

Cluster was torn down after the run (see bottom).

## Cluster facts (this run)

- Workspace dir (all `easy-db-lab` state): `/Users/pmcfadin/projects/easy-db-lab/clusters/cqlite-flight-loadtest-20260706-192113/`
- Wrapper: `$CLUSTER_DIR/easy-db-lab` (sets JAVA_HOME 21, cds into workspace)
- Full raw findings log: `$CLUSTER_DIR/../RUN-FINDINGS-20260706-192103.md`
- Topology: 3× `i4i.xlarge` db (db0 10.1.1.145, db1 10.1.2.84, db2 10.1.3.59), 1× `m5.xlarge` app (app0), 1 control. DC `us-west-2`.
- Cassandra 5.0; Trino pinned 481; connector `in.mcfad:cqlite-trino:0.13.1`; flight image `ghcr.io/pmcfadin/cqlite-flight:v0.13.0`.
- Data: `cassandra_easy_stress.keyvalue`, written by `cassandra-easy-stress KeyValue -d 15m --threads 50` (~1500 wr/s, job Completed). 3 SSTables/node after 6 flushes + compaction.

## Environment gotchas (needed to run the plan by hand)

- **Kit `bin/*.sh` scripts run LOCALLY** (KitRunnerCommand ProcessBuilder) against the remote K3s API. They shell out to `helm` and `kubectl` — both must be on the dev machine. `kubectl` was present; **`helm` was NOT** → `helm: command not found`. Fixed with `brew install helm` (v4.2.2). Not documented as a local prereq (easy-db-lab doc gap; see findings log F1).
- **Cluster API is not directly reachable.** kubeconfig points at the control node's private IP (`10.1.1.163:6443`). A SOCKS5 proxy runs on `localhost:1080` (the `easy-db-lab` CLI uses it internally). For hand-run `helm`/`kubectl`, export:
  ```
  export ALL_PROXY="socks5h://localhost:1080" HTTPS_PROXY="socks5h://localhost:1080" NO_PROXY="localhost,127.0.0.1"
  export KUBECONFIG="$CLUSTER_DIR/kubeconfig"
  ```
- **AWS**: `AWS_PROFILE=edl` (SSO; `aws sso login --sso-session ehc-embark` if expired).
- Every SSH prints a harmless `axonops sudoers wildcards` warning — cosmetic, ignore.
- `trino`/`cassandra` full-table `SELECT count(*)` is slow and blows past a 2-min timeout; Cassandra count hit a LOCAL_QUORUM read timeout — run counts with patience / higher client timeout.

## Bugs filed (all on pmcfadin/cqlite)

### Hard blockers on the happy-path (each worked around to proceed)
- **#2116** — *trino kit (easy-db-lab)*: `web-ui.*` in top-level `additionalConfigProperties` → applied to workers → **worker CrashLoopBackOff at Trino 481** ("Configuration property 'web-ui.authentication.type' was not used"). Fix: scope web-ui to coordinator only. Workaround: removed web-ui lines from rendered `trino/values.yaml`.
- **#2118** — *cqlite-flight kit*: `runAsNonRoot: true` with no numeric `runAsUser` → **CreateContainerConfigError** ("image has non-numeric user (flight)"). Fix: add `runAsUser: 10001`. Workaround: patched rendered `cqlite-flight/daemonset.yaml`.
- **#2120** — *trino-cqlite overlay*: `cqlite start` → only `reapply-plugin-patch.sh` runs, which **references but never creates** the `cqlite-trino-plugin-src` ConfigMap (only `install.sh` does, and it's not wired as a hook) → init container **FailedMount**. Fix: create the ConfigMap in reapply/start. Workaround: ran `kubectl create configmap` by hand.
- **#2122** — *trino-cqlite overlay*: plugin-fetch init image `gradle:9.1.0-jdk21` **can't resolve the JDK-25 connector artifact** (`cqlite-trino:0.13.1` requires JVM 25). Fix: bump init image to `gradle:9.6-jdk25`. Workaround: patched init image on both deployments.
- **#2123** — *trino-cqlite overlay*: catalog file sets `connector.name=cqlite` but the factory is **`cqlite_flight`** → **coordinator crash "No factory for connector 'cqlite'"**. Fix: `connector.name=cqlite_flight` (catalog display name still comes from the filename). Workaround: rewrote catalog in both the sibling `.properties` and the helm `.catalogs-override.yaml`.

- **#2128** — *cqlite-flight image (build/CI)*: image built **without `--features observability`** → `CQLITE_OTEL_*` inert, **no metrics/traces emitted at all**. Verified: VM has 1609 metrics, zero `cqlite_*`; node OTel collector reachable on `:4317`; flight env correct; no obs-init line in flight log. Fix: `cargo build --release -p cqlite-flight --features observability`. Blocks the whole observability goal.
- **#2130** — *trino-loadtest driver*: `int(TRINO_PORT)` crashes on the K8s-injected `TRINO_PORT=tcp://<svcIP>:8080` service env var (Service `trino` in the namespace) → driver never starts. Fix: rename env vars out of the K8s-reserved namespace, or guard the int parse (start.sh already passes `--port` explicitly).
- **#2132** — *trino-loadtest kit*: unset optional `--queries-file` arrives from the easy-db-lab runner as the **literal string `null`** → `start.sh` runs `--from-file=queries.txt=null` → "no objects passed to apply", pod never created. Fix: guard `!= "null"` (also raise with easy-db-lab: unset `default:""` arg should be empty, not `null`).

### Other filed
- **#2117** — *release/CI*: `cqlite-flight:v0.13.1` container **missing on GHCR** (connector JAR published to Maven, container not) + inconsistent v-prefix/bare tag scheme. Workaround: used `v0.13.0`.
- **#2119** — *trino-cqlite overlay*: `reapply-plugin-patch.sh` patches deployments but never deletes the old coordinator → **hostPort:8080 rollout deadlock** on a single app node. Fix: delete coordinator pod after patch (as the trino kit does).
- **#2129** — *connector (perf/enhancement)*: **no LIMIT/count pushdown**. `EXPLAIN SELECT * ... LIMIT 5` shows `Limit` above `TableScan`, scan estimate = full 1,990,586 rows / 208 MB. Every query does a full compaction-merge of all splits. `LIMIT 5` = 235s then "Network closed". Read is correct, just unbounded. Suggested: `applyLimit` → row cap in the flight ticket; count/aggregation pushdown (#893 plumbing exists).
- **#2114** — *cqlite-flight kit* (filed pre-run, low priority): single-disk `--data-dir` assumption misses SSTables on multi-disk db nodes.

### Fix-order recommendation for the team
The happy-path unblocks in this order: **#2116** (trino workers boot at 481) → **#2118** (flight pods start) → **#2117** (a pullable container tag) → **#2120 + #2122 + #2123 + #2119** (overlay: create ConfigMap, jdk25 init image, `connector.name=cqlite_flight`, delete-coordinator-after-patch — all four needed for `cqlite start` to converge) → **#2130 + #2132** (loadtest driver starts) → **#2128** (observability actually emits) → **#2129** (queries become usably fast). The first seven are pure kit/build fixes; #2129 is connector engineering.

## What passed / worked well

- cqlite-flight DaemonSet: correct one-pod-per-db-node placement; after the #2118 fix, all 3 pods Running; **data dir readable** via `supplementalGroups: [999]` (no perm errors); flight server listening `0.0.0.0:8815`; **`:8815` reachable from app0** (hostNetwork model works).
- Plugin assembly from Maven Central works once on JDK 25 (`BUILD SUCCESSFUL`, plugin JARs land in `/usr/lib/trino/plugin/cqlite_flight`).
- `cqlite` catalog registers; `SHOW SCHEMAS FROM cqlite` returns `information_schema` (connector responds, sidecar reachable) — before Phase 1 there was no user keyspace, as expected.
- Template variable substitution in all three kits was correct (image tags, gid, data path, sidecar URI, flight port, connector version).
- Write load produced real multi-generation SSTables (compaction ran: 6 flushes → 3 SSTables/node).

## Where it stands / next steps for the picker-upper

1. **Validate read parity (Phase 3, Step 12):** confirm `SELECT count(*) FROM cqlite.cassandra_easy_stress.keyvalue` (Trino) == Cassandra count. Trino count query was in flight at handoff — check its result. This is the first real exercise of the flight `do_get` path end-to-end.
2. **Phase 3a — snapshot mode read run:** `easy-db-lab trino-loadtest-trino start --ks cassandra_easy_stress --tbl keyvalue --threads 8 --duration 120 --traceparent`. Watch `nodetool listsnapshots` for transient `cqlite-<queryId>` snapshots.
3. **Phase 3b — live mode:** add `cqlite.read-mode=live` to the catalog override, re-run update-catalogs (then re-apply the #2119/#2122/#2123 workarounds — helm upgrade drops the out-of-band init patch!), start a concurrent write churn + read load.
4. **Phase 4 — observability:** confirm `cqlite_rpc_*` / `cqlite_errors_total` metrics in VictoriaMetrics; the auto-installed "CQLite Flight — RPC Metrics" Grafana dashboard; client→flight→core Tempo traces (queries used `--traceparent`); Trino Pyroscope profiles.
5. **Phase 5 / teardown:** file any read-phase anomalies; then `$EDB down --auto-approve`. **Cluster is still UP** at handoff — remember to tear it down (real EC2 cost).

### IMPORTANT operational note for re-runs
Any `helm upgrade` on the trino release (triggered by `update-catalogs.sh` or any
kit start/stop via the overlay's unfiltered hooks) **drops the out-of-band
kubectl-patched plugin init container**, crashing the coordinator until the
plugin patch is re-applied. Until #2119/#2120/#2122/#2123 are fixed in the kit,
the working manual sequence after any helm upgrade is:
1. ensure ConfigMap exists (#2120), 2. reapply plugin patch with the jdk25 image
(#2122), 3. ensure catalog uses `connector.name=cqlite_flight` (#2123),
4. delete the stale coordinator pod to free hostPort:8080 (#2119).
