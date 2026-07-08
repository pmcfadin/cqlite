# Lab Plan: cqlite-flight + Trino Loadtest (3-Node)

## Goal

Tie epic [#2103](https://github.com/pmcfadin/cqlite/issues/2103)'s four pieces
(`cqlite-flight`, the `trino-cqlite` connector overlay, `trino-loadtest`, and the
`cqlite.read-mode` toggle) into one repeatable run: drive real CQL writes into a
3-node Cassandra 5.0 cluster, flush them to SSTables, read them back through Trino
via Arrow Flight in both `snapshot` and `live` mode, confirm the observability
wiring (`cqlite.rpc.*` metrics, client→flight→core traces, Trino CPU/alloc
profiles), then triage anything broken into filed issues. This is the harness that
*produces* numbers and bug reports — it is not itself a benchmark-numbers
deliverable (epic non-goal).

## Environment

- 3 DB nodes: `i4i.xlarge` (local NVMe, no EBS) — Cassandra 5.0 + Sidecar +
  co-located `cqlite-flight` pod (DaemonSet, one per db node)
- 1 App node: `m5.xlarge` — Trino coordinator + worker, `trino-loadtest` driver
  pod, and the `cassandra-easy-stress` K8s jobs
- Cassandra version: 5.0
- Trino version: **481** (pinned — see [Phase 0.3](#03-trino-must-be-pinned-to-481-not-the-lab-default-474))

## Prerequisites

Read all four before starting. Each is a verified finding from epic #2103's
children, not a guess.

### 0.1 The connector must be on Maven Central at the version under test

`trino-cqlite`'s init container resolves the published
`in.mcfad:cqlite-trino:<version>` artifact from Maven Central at pod-start time
(no baked image — see `trino-cqlite/README.md` "Approach: init-container, not a
baked image"). **As of authoring this plan, publishing is blocked** on repo
secrets (`MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD`, `SIGNING_KEY`,
`SIGNING_PASSWORD` — see `.github/workflows/trino-publish.yml`), and the
`cqlite.read-mode` feature (#2105) this plan exercises needs a **new** release
version (e.g. `0.13.1`) published *after* epic #2103 merges — the current
released version predates it.

Until that version is on Maven Central, `easy-db-lab cqlite start`'s init
container (`cqlite-plugin-fetch`) fails with a Gradle dependency-resolution error
(`Could not resolve in.mcfad:cqlite-trino:<version>` / `Could not GET
'https://repo1.maven.org/...'`). Confirm before you start:

```bash
curl -sf "https://repo1.maven.org/maven2/in/mcfad/cqlite-trino/<version>/cqlite-trino-<version>.pom" \
  -o /dev/null && echo "published" || echo "NOT PUBLISHED — stop here"
```

If it's not published, this plan cannot proceed past Phase 1, step 5 (installing
the `trino-cqlite` overlay).

### 0.2 Kit install path: `kit source add`, not `--from`

The issue's assumed `easy-db-lab kit install <kit> --from <dir>` syntax does not
substitute a kit's declared `args:` — verified against `InstallTemplateResolver.kt`
/ `commands/kit/Install.kt` in the easy-db-lab source. `--from` requires an
explicit `--kit <name> --size <size>` and calls `renderAndWrite()` with no
`extraVars`, so only the fixed cluster-level template variables land; every
kit-declared token (`__TAG__`, `__CONNECTOR_VERSION__`, `__SIDECAR_URI__`, etc.)
is left unresolved in the written files.

**Use the out-of-tree source registration instead** — it wires args correctly,
exactly like a built-in kit:

```bash
easy-db-lab kit source add cqlite /path/to/cqlite/easy-db-lab-kits
```

Then `easy-db-lab kit install <name> <args...>` for each of the three kits, where
`<name>` is the **kit's source-directory name** (`cqlite-flight`, `trino-cqlite`,
`trino-loadtest`) — this is the dispatch key `InstallTemplateResolver.listAvailableTemplates()`
registers, read from the registered source directory's own subdirectory names.

**Naming subtlety for the `trino-cqlite` overlay** (verified by reading
`KitInstallCommand.resolveInstanceName()` — not yet confirmed against a live
run): the *installed output directory* (and therefore the `easy-db-lab <name>
start/stop` runner and the Trino catalog name) is **not** the dispatch name.
`resolveInstanceName()` falls back to `config.name` — the kit's own `name:`
field inside `kit.yaml` — whenever the kit declares no `kit-ref`/extension arg.
`trino-cqlite/kit.yaml` deliberately sets `name: cqlite` (see its own header
comment), so:

```bash
easy-db-lab kit install trino-cqlite --connector-version 0.13.1 --flight-port 8815 \
  --sidecar-uri "http://$(easy-db-lab ip db0 --private):9043" --trino-image-tag 481
# ^ dispatch verb "trino-cqlite" (source directory name) ...
easy-db-lab cqlite start
# ^ ... but the installed dir / runner / catalog name is "cqlite" (from kit.yaml's name: field)
```

This is **not** what `trino-cqlite/README.md`'s own "Install order" section shows
— that section only documents the ad-hoc `--from ... --kit cqlite --size 1Gi`
fallback (which shares the exact same arg-non-substitution bug §0.2 describes for
`--from` generally: `__CONNECTOR_VERSION__`, `__SIDECAR_URI__`, etc. would be left
unresolved literal tokens in the rendered files). Use the `kit source add` form
above; it is both correct and still lands at `cqlite/`.

### 0.3 Trino must be pinned to 481, not the lab default 474

The `trino` kit's `--version` default is `474` (`kits/trino/kit.yaml`). The
`cqlite-trino` connector targets Trino SPI `481`
(`trino-connector/build.gradle.kts`). These are **not** binary-compatible —
`trino-cqlite/bin/start.sh` reads the running `trino-coordinator`'s live image
tag and **fails closed** with an explicit remediation message if it isn't `481`:

```
ERROR: SPI mismatch. trino-coordinator is running image '...:474' (tag '474'),
but this cqlite-trino connector build targets Trino SPI 481 ...
```

Install the `trino` kit itself with `--version 481` (see Phase 1, step 3) — this
overlay cannot rewrite the trino kit's own `values.yaml`.

**[UPDATED after the first live run] Known upstream blocker at 481 — worker
CrashLoopBackOff (#2116).** The easy-db-lab trino kit's
`values.yaml.template` puts `web-ui.authentication.type` / `web-ui.user` in
top-level `additionalConfigProperties`, which the Helm chart renders into
BOTH coordinator and worker `config.properties`. `web-ui.*` is
coordinator-only; at Trino 481 workers fail-fast on the unused properties:

```
ERROR io.trino.server.Server Configuration is invalid
1) Configuration property 'web-ui.authentication.type' was not used
```

**Workaround (until fixed upstream — tracked with fix guidance in
cqlite#2116):** after `trino` kit install, remove the two `web-ui.*` lines
from the rendered `trino/values.yaml` and re-run the kit's `helm upgrade`
(web-ui auth is irrelevant to a JDBC load test). Verify both `trino-worker`
pods reach `Ready` before proceeding to step 4.

### 0.4 Flight kit preflight

**[UPDATED after the first live run]** The first run of this plan hit
`CreateContainerConfigError` on every `cqlite-flight` pod: `runAsNonRoot: true`
was set without a numeric `runAsUser`, and the image's `USER flight` directive
is a name, not a uid, so the kubelet couldn't verify non-root and refused to
create the container (issue #2118, fixed — the manifest now pins
`runAsUser: 10001` / `runAsGroup: 10001` explicitly). No command in this plan
changes; this is now expected to just work.

Before `easy-db-lab cqlite-flight start`:

- **Cassandra data-dir gid**: the published image runs as fixed `uid 10001`; the
  manifest adds `supplementalGroups: [<data-gid>]` (default `999`, matching the
  lab's own `cassandra:cassandra` uid/gid convention) so it can read the host
  data directory read-only. If the host directory isn't group-readable for that
  gid, pass `--data-gid <actual-gid>` or loosen host permissions. **Not verified
  live** (`cqlite-flight/README.md` "Open risks" — no cluster was available while
  authoring the kit) — treat as a FIRST-RUN CHECK.
- **GHCR image pull**: `ghcr.io/pmcfadin/cqlite-flight:<tag>` is assumed public
  (no `imagePullSecrets` wired). If it 403s, the image needs to be made public or
  the DaemonSet needs a pull secret added by hand.
- **Trino → db-node `:8815` reachability**: the kit runs `hostNetwork: true`, so
  Flight is reachable at `<db-node-private-ip>:8815` — the same node-IP model the
  connector's Sidecar-based topology discovery already assumes. Confirm this
  reaches from an app node before relying on it (FIRST-RUN CHECK):
  ```bash
  $EDB exec run -t stress -- bash -c 'echo > /dev/tcp/<db0-private-ip>/8815 && echo OK'
  ```

---

## Steps

### 1. Create cluster workspace and provision

```bash
CLUSTER_DIR="clusters/cqlite-flight-loadtest-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$CLUSTER_DIR"
bin/create-easy-db-lab-wrapper "$CLUSTER_DIR"
EDB="$CLUSTER_DIR/easy-db-lab"
$EDB init cqlite-flight-loadtest --db 3 --app 1 \
  --instance i4i.xlarge --stress-instance m5.xlarge --up
```

Wait for provisioning and K3s to come up (~5 min). Once `kubeconfig` exists in
`$CLUSTER_DIR`, export it once so the raw `kubectl`/script invocations used
throughout this plan (outside `$EDB`, which sets `KUBECONFIG` internally per
call) target this cluster:

```bash
export KUBECONFIG="$CLUSTER_DIR/kubeconfig"
```

### 2. Bring up Cassandra 5.0

```bash
$EDB cassandra use 5.0
$EDB cassandra update-config
$EDB cassandra start
$EDB cassandra nt status
```

All 3 nodes must show `UN` (Up/Normal) before continuing. If not, wait 30s and
retry (`nt status` again).

### 3. Install Trino, pinned to 481 (see [0.3](#03-trino-must-be-pinned-to-481-not-the-lab-default-474))

```bash
$EDB kit install trino --version 481
$EDB trino start
```

Expect 2-3 minutes for the rollout. Verify:

```bash
$EDB trino status
```

Coordinator and worker pods must both be Running/Ready.

### 4. Register the kit source and install cqlite-flight

```bash
$EDB kit source add cqlite /path/to/cqlite/easy-db-lab-kits
$EDB kit install cqlite-flight --tag v0.13.1 --flight-port 8815
$EDB cqlite-flight start
```

Expected output ends with:

```
cqlite-flight is ready — Arrow Flight gRPC on <db-node-ip>:8815 (one pod per db node).
```

Verify one pod per db node:

```bash
kubectl get pods -l easydblab.com/kit=cqlite-flight -o wide
```

### 5. Install the trino-cqlite overlay (see [0.2](#02-kit-install-path-kit-source-add-not---from) for the naming subtlety)

**[UPDATED after the first live run]** This step surfaced four bugs, all now
fixed in the kit — no command below changes, but the *observed* behavior
during this step does. **[UPDATED after the second live run]** two more bugs
(#2158, #2159) surfaced on a re-run after #2119's fix proved insufficient —
see those two entries below, both now fixed:

- **#2120** — `cqlite start` never created the `cqlite-trino-plugin-src`
  ConfigMap, so both pods sat `Init:0/1` with `FailedMount`. Fixed:
  `bin/reapply-plugin-patch.sh` now creates/refreshes the ConfigMap before
  patching either deployment, every time it runs.
- **#2122** — the plugin-fetch init container's `gradle:9.1.0-jdk21` image
  couldn't resolve the connector artifact (it requires JDK 25). Fixed: bumped
  to `gradle:9.1.0-jdk25`.
- **#2123** — the rendered catalog used `connector.name=cqlite`, but the
  factory registers `cqlite_flight`, so the coordinator CrashLoopBackOff'd
  with "No factory for connector 'cqlite'". Fixed: `connector.name=cqlite_flight`
  (the catalog name stays `cqlite` — `SHOW CATALOGS` / `cqlite.<ks>.<tbl>` are
  unaffected).
- **#2119** — patching the coordinator's pod template (to add the plugin
  initContainer) started a rollout that could never converge on this plan's
  single app node: the new pod can't bind the coordinator's `hostPort: 8080`
  while the old one still holds it. First fix attempt: the reapply script
  deleted the old coordinator pod **only when the patch actually changed
  something**. **[UPDATED after the second live run]** That delete-pod fix
  was insufficient — under `strategy: RollingUpdate` the surviving
  ReplicaSet just recreates the deleted pod and re-grabs `hostPort: 8080`,
  reproducing the deadlock (**#2158**). The kit now ensures
  `strategy: Recreate` on `trino-coordinator` before every pod-template patch
  (`bin/reapply-plugin-patch.sh`), so the controller tears down the old pod
  before starting the new one. **Expect the `trino-coordinator` pod to be
  recreated once** during this step on a fresh install — that is now
  expected, not a hang. This manual workaround is no longer needed:
  ```bash
  # no longer required — the kit ensures strategy: Recreate itself
  kubectl patch deployment trino-coordinator -p '{"spec":{"strategy":{"type":"Recreate","rollingUpdate":null}}}'
  ```
- **#2159** — **[UPDATED after the second live run]** when `cqlite start`
  aborted on the #2158 deadlock above, the `cqlite` Trino catalog was never
  registered, because registration relied entirely on the trino kit's
  `post-workload-start` hook firing on a *clean* completion — a partial
  failure left `SHOW CATALOGS` without `cqlite` and no automatic recovery
  path. Fixed: `bin/start.sh` and `bin/verify.sh` both now call
  `bin/ensure-catalog-registered.sh`, which checks `SHOW CATALOGS` and, if
  `cqlite` is missing, registers it by invoking the trino kit's own
  `bin/update-catalogs.sh` directly. The manual recovery recipe from the
  first live run is no longer needed:
  ```bash
  # no longer required — bin/start.sh / bin/verify.sh self-heal this
  bash trino/bin/update-catalogs.sh
  ```
  Re-running `easy-db-lab cqlite start` (or `bin/verify.sh`) alone now
  recovers a partial failure with no manual steps.

```bash
DB0_IP=$($EDB ip db0 --private)
$EDB kit install trino-cqlite --connector-version 0.13.1 --flight-port 8815 \
  --sidecar-uri "http://${DB0_IP}:9043" --trino-image-tag 481
$EDB cqlite start
```

This fails closed with a clear message if step 3 didn't pin Trino to 481 (see
[0.3](#03-trino-must-be-pinned-to-481-not-the-lab-default-474)). On success it
prints:

```
cqlite plugin loaded into trino-coordinator/trino-worker at
  /usr/lib/trino/plugin/cqlite_flight
```

Verify the catalog is registered:

```bash
"$CLUSTER_DIR/cqlite/bin/verify.sh"
```

Expected: `SHOW CATALOGS` lists `cqlite`; `SHOW SCHEMAS FROM cqlite` lists at
least `system` (no user keyspace yet — that comes from Phase 1's stress write).

### 6. Install trino-loadtest

```bash
$EDB kit install trino-loadtest --target trino
```

No `start` yet — that's Phase 3.

---

## Phase 1: WRITE

Drive real CQL writes with `cassandra-easy-stress` to produce non-trivial
SSTable volume — multiple flushes and at least one compaction round is the
point; a single tiny SSTable makes the connector's merge path uninteresting.

### 7. Start the write load

```bash
$EDB cassandra stress start --name cqlite-write-baseline --tags "phase=write-baseline" \
  -- KeyValue -d 15m --threads 50
```

`stress start` auto-injects `--host <first-cassandra-node-private-ip>` if you
don't pass one (verified against `StressStart.kt`'s `buildStressArgs`).

### 8. Discover the generated keyspace/table

`cassandra-easy-stress`'s default `KeyValue` workload creates its own
keyspace/table on first run. **Do not assume a fixed name** — known reference
material disagrees with itself on the default (`baselines.keyvalue` in one note,
`cassandra_easy_stress.keyvalue` in the accompanying command in the same
document). Discover it live instead (FIRST-RUN CHECK):

```bash
$EDB cassandra stress status
$EDB cassandra cql "SELECT keyspace_name, table_name FROM system_schema.tables WHERE keyspace_name NOT IN ('system','system_schema','system_auth','system_distributed','system_traces','system_views','system_virtual_schema')"
```

Set `KEYSPACE`/`TABLE` shell variables from the result for the rest of this
plan (e.g. `KEYSPACE=baselines`, `TABLE=keyvalue`).

### 9. Force multiple SSTable generations during the write

A single end-of-run flush produces one SSTable — not enough to exercise
compaction. Flush periodically while the write job is still running so STCS
(default `min_threshold=4`) actually has multiple same-size tables to merge:

```bash
for i in 1 2 3 4 5; do
  sleep 120
  $EDB cassandra nt flush "$KEYSPACE"
  $EDB cassandra nt compactionstats
done
```

Watch `nt compactionstats` for a non-empty pending/active compaction after the
4th or 5th flush.

### 10. Wait for the write job to finish

```bash
$EDB cassandra stress status
```

Wait for `cqlite-write-baseline` to complete (or `stress stop
cqlite-write-baseline --force` once you have enough data for the run size you
want — see [Scale notes](#scale-notes)).

---

## Phase 2: FLUSH (mandatory)

The connector only ever sees **flushed** SSTables — memtable rows are invisible
to Flight until a `nodetool flush`, regardless of read mode. This is not
optional even if Phase 1's periodic flushes already ran; run one final flush to
guarantee no unflushed tail:

### 11. Final flush on all db nodes

```bash
$EDB cassandra nt flush "$KEYSPACE"
```

Confirm no pending flush work:

```bash
$EDB cassandra nt tpstats
```

`FlushWriter`/`MemtablePostFlush` pending should be 0 on all 3 nodes.

---

## Phase 3: READ (two runs)

### 12. Sanity check row count before either run

```bash
$EDB cassandra cql "SELECT count(*) FROM ${KEYSPACE}.${TABLE}"
"$CLUSTER_DIR/cqlite/bin/verify.sh" "$KEYSPACE" "$TABLE"
$EDB trino sql "SELECT count(*) FROM cqlite.${KEYSPACE}.${TABLE}"
```

Compare the Cassandra-side CQL count against the `cqlite.${KEYSPACE}.${TABLE}`
count Trino returns. They must match exactly; a mismatch here is Phase 5's
"wrong row counts" class before you've even started the concurrent runs.

**[UPDATED after the first live run]** The first run of this plan hit two
`trino-loadtest` driver bugs before any query was ever issued, both now fixed
— no command below changes:

- **#2130** — the driver crashed immediately with
  `ValueError: invalid literal for int() with base 10: 'tcp://10.43.73.27:8080'`.
  Kubernetes auto-injects Docker-link-style Service env vars into every pod
  (the Trino Helm chart creates a Service named `trino`, so every pod sees
  `TRINO_PORT=tcp://<ip>:8080`), and the driver's argparse defaults called
  `int()` on that at parser-construction time — before `start.sh`'s explicit
  `--port` flag ever got a chance to win. Fixed: the driver now reads
  `TRINO_LOADTEST_HOST`/`TRINO_LOADTEST_PORT`/`TRINO_LOADTEST_USER`/
  `TRINO_LOADTEST_CATALOG` (namespaced, not the Kubernetes-reserved bare
  names) and falls back to the documented default instead of raising on any
  unparseable numeric env value.
- **#2132** — with no `--queries-file` passed (the normal case for this
  plan's built-in query set), `start.sh` failed before the pod was even
  created: `error: error reading null: no such file or directory` /
  `error: no objects passed to apply`. The easy-db-lab kit runner injects the
  literal string `null` for an unset optional arg rather than leaving it
  empty, and `[ -n "$VAR" ]` alone is true for `"null"`. Fixed: `start.sh`
  now also rejects the literal `"null"` before treating the var as set. **The
  loadtest start below no longer needs a `--queries-file` workaround** —
  omitting it (as both runs in this plan do) now correctly falls through to
  the built-in scan+aggregate query set.

### 13a. Run (a) — default `snapshot` mode: correctness/consistency run

`cqlite.read-mode` defaults to `snapshot` — no catalog edit needed for this run.

```bash
$EDB trino-loadtest-trino start --ks "$KEYSPACE" --tbl "$TABLE" \
  --threads 8 --duration 120 --traceparent
```

While this runs, in a second terminal, watch for the per-query Sidecar snapshot
appearing and disappearing (FIRST-RUN CHECK — exact timing/overlap not verified
live; queries loop continuously for the whole `--duration`, so a snapshot may or
may not be visible in any single poll):

```bash
watch -n2 "$EDB cassandra nt listsnapshots"
```

Expect entries named `cqlite-<queryId>` to appear transiently and be gone by the
time `trino-loadtest-trino start` exits.

```bash
$EDB trino-loadtest-trino stop
```

### 13b. Run (b) — `cqlite.read-mode=live`: the compaction-stress weak-spot hunt

`cqlite.read-mode` **is** a `trino-cqlite` install-time flag (`--read-mode`,
default `snapshot`; issue #2113), threaded straight into the rendered
`trino-catalog.properties`. The cleanest way to run Phase 3b is to install the
overlay with `--read-mode live` (see the overlay README's "Install order"), so
no catalog hand-edit is needed at all.

If the overlay is already installed in `snapshot` mode and you don't want to
reinstall, you can still flip the rendered catalog by hand for this one run:

```bash
echo "cqlite.read-mode=live" >> "$CLUSTER_DIR/cqlite/trino-catalog.properties"
"$CLUSTER_DIR/trino/bin/update-catalogs.sh"
```

Now start a **second, concurrent** write load — the actual point of `live`
mode is racing compaction against an in-flight scan — and the read load
together, so the read window fully overlaps ongoing flush/compaction:

```bash
$EDB cassandra stress start --name cqlite-live-churn --tags "phase=live-hunt" \
  -- KeyValue -d 10m --threads 20
$EDB trino-loadtest-trino start --ks "$KEYSPACE" --tbl "$TABLE" \
  --threads 16 --duration 300 --traceparent
```

Force a few more flushes mid-run to keep compaction pressure up while the read
load is active:

```bash
for i in 1 2 3; do sleep 60; $EDB cassandra nt flush "$KEYSPACE"; done
```

```bash
$EDB trino-loadtest-trino stop
$EDB cassandra stress stop cqlite-live-churn --force
```

Revert to `snapshot` mode afterward if you plan to re-run Phase 3a:

```bash
sed -i.bak '/cqlite.read-mode=live/d' "$CLUSTER_DIR/cqlite/trino-catalog.properties"
"$CLUSTER_DIR/trino/bin/update-catalogs.sh"
```

---

## Phase 4: OBSERVE / VERIFY

**[UPDATED after the first live run]** The first run of this plan found **zero**
`cqlite.rpc.*`/`cqlite_rpc_*` series in VictoriaMetrics and no flight traces in
Tempo, despite a correct pod env (`CQLITE_OTEL_ENABLED=true`,
`CQLITE_OTEL_ENDPOINT=http://localhost:4317` reachable) and no export errors in
the flight log — **root cause: #2128**, the published `cqlite-flight` image was
built without `--features observability`, so all the OTLP metric/trace code was
compiled out and the env vars were silently inert. Fixed:
`cqlite-flight/Dockerfile` now builds with `--features observability`.
**The observability check below now expects an extra startup log line** —
confirm it's present before checking VictoriaMetrics/Tempo (FIRST-RUN CHECK,
not yet verified against a live cluster):

```bash
kubectl logs -l easydblab.com/kit=cqlite-flight --all-containers --prefix | grep -i observability
```

Expect one `observability enabled, exporting to OTLP endpoint` info line per
pod, naming `endpoint=http://localhost:4317`. (If the image were ever
published again without the feature, the fix also makes that case visible
instead of silent: a `WARN` line stating the binary was compiled without
`observability` and that `CQLITE_OTEL_*` vars are inert.)

### 14. Confirm `cqlite.rpc.*` metric names in VictoriaMetrics (FIRST-RUN CHECK)

The OTel collector mangles cqlite-core's dotted metric names
(`cqlite.rpc.requests`, etc.) into Prometheus-style names. Confirm the exact
mangled form live rather than assuming it:

```bash
source "$CLUSTER_DIR/env.sh"
with-proxy curl -s "http://control0:8428/api/v1/label/__name__/values" | grep cqlite_rpc
```

Expected names (per `cqlite-flight/dashboards/cqlite-flight.json`'s panel
queries): `cqlite_rpc_requests_total`, `cqlite_rpc_duration_seconds_bucket` (+
`_sum`/`_count`), `cqlite_rpc_rows_total`, `cqlite_rpc_bytes_total`,
`cqlite_rpc_in_flight`, and `cqlite_errors_total` (label
`cqlite_subsystem="flight"`).

### 15. Confirm the Grafana dashboard populates

`cqlite-flight/dashboards/cqlite-flight.json` auto-installs into Grafana on the
DaemonSet's own `start` (kit contract: "any `.json` files in `dashboards/` are
installed automatically" — `docs/development/kits.md`). Open Grafana:

```
http://<control-node-private-ip>:3000
```

(get the IP from `$CLUSTER_DIR/state.json`, or via SOCKS proxy per
`docs/user-guide/victoria-metrics.md`). Open **CQLite Flight — RPC Metrics**:

- Set `$cluster` to this cluster's name.
- Set `$service_name` — the dashboard's templating variable is
  `label_values(cqlite_rpc_requests_total, service_name)`; the flight
  DaemonSet's default is `CQLITE_OTEL_SERVICE_NAME=cqlite-flight`
  (`cqlite-flight/daemonset.yaml`), so pick `cqlite-flight` unless the kit was
  installed with a custom `--otel-endpoint`/env override.
- Confirm all 5 panel rows populate during the Phase 3 runs: Request Rate,
  Latency (p50/p95/p99), Rows & Bytes Streamed, In-Flight Requests, Errors.

### 16. Confirm client → Flight → core traces in Tempo (FIRST-RUN CHECK)

Both Phase 3 runs used `--traceparent`, so every query carried a W3C
traceparent header. From a panel with a data point during a Phase 3 window,
use the panel's built-in "View Trace" link (wired in
`cqlite-flight.json` to a Tempo TraceQL query
`{ resource.service.name = "$service_name" && span.rpc.method =
"$__field.labels.cqlite_rpc_method" }`), or query Tempo directly in Grafana
Explore with the same TraceQL. Confirm:

- A trace exists spanning client (Trino) → `cqlite-flight` → `cqlite-core`.
- `span.rpc.method` is a filterable span attribute (the TraceQL query above
  depends on it) — not yet confirmed against a live Tempo instance.

### 17. Confirm Trino CPU/alloc in Pyroscope

The `trino` kit already wires the Pyroscope Java agent
(`kits/trino/bin/start.sh.template`): `-Dpyroscope.application.name=trino`,
`-Dpyroscope.profiler.event=cpu`, `-Dpyroscope.profiler.alloc=512k`, labeled
`component=coordinator`/`component=worker`. In Grafana Explore:

- Datasource: **Pyroscope**.
- `service_name = trino`, filter `component=coordinator` and
  `component=worker` separately.
- Profile types `process_cpu` (cpu) and `memory`/`alloc` (allocation) should
  show load concentrated in the `cqlite_flight` plugin's JDBC/RecordCursor
  path during Phase 3, not idle/GC-only stacks.

### 18. Confirm snapshot lifecycle cleanup

```bash
$EDB cassandra nt listsnapshots
```

Expected: empty (or only unrelated snapshots) once both Phase 3 runs have fully
stopped — every `cqlite-<queryId>` snapshot created during Run (a) should have
been deleted by Trino's `cleanupQuery` hook. A non-empty, growing list of
`cqlite-*` snapshots after the runs stop is itself a Phase 5 "wrong row
counts"/leak-class finding (a crashed coordinator mid-query, or a
`cleanupQuery` bug) — capture the list and file it.

### 19. Tear down

```bash
$EDB trino-loadtest-trino stop
$EDB cassandra stress stop --all --force
$EDB cqlite uninstall
$EDB cqlite-flight stop
$EDB trino stop
$EDB cassandra stop
$EDB down --auto-approve
```

---

## Phase 5: FAILURE TRIAGE → ACTIONABLE ISSUES

This is the epic's actual outcome: every anomaly found in Phases 3-4 becomes a
filed, reproducible GitHub issue — not a note left in a run log. **File under
labels `bug` + `performance`** (both exist in this repo today), referencing
epic #2103. Per this repo's doctrine (`CLAUDE.md` "Spec-driven work"): these are
**oracle-driven parity/correctness bugs against a real Cassandra cluster** — file
as a plain GitHub issue + a pinned reproduction (the captured artifacts below),
**not** an OpenSpec change.

| Failure class | Symptom / trigger | Data to capture | The filed issue must contain |
|---|---|---|---|
| **Flight `do_get` errors/panics under live-mode compaction races** | Run (b): errors or connection resets during a scan that overlaps a forced flush/compaction | `kubectl logs -l easydblab.com/kit=cqlite-flight --all-containers --prefix --since=30m` from the affected db node's pod; `cqlite_errors_total` broken down `by (cqlite_error_category)` for the failure window; the Tempo trace for the failed query; the exact Flight ticket JSON (`snapshot` field value, table, projection) from the query's `EXPLAIN`/coordinator log | Repro steps (this plan's Phase 1+3b exactly), the pod logs, the ticket JSON, the error-category breakdown, and whether it reproduces in `snapshot` mode (if not, it's a live-mode-specific race) |
| **Latency cliffs** | p99 (or p50) duration jumps during a flush/compaction window in the Grafana Latency panel | The `cqlite_rpc_duration_seconds_bucket` histogram for before/during/after the cliff; the Pyroscope CPU flame graph for the `cqlite-flight` pod (or Trino worker) during the same window | The duration histogram screenshot/query, the flame graph, and the wall-clock window correlated against `nt compactionstats`/flush timestamps |
| **Wrong row counts: snapshot vs live vs Cassandra `COUNT(*)`** | Step 12/18's count comparison disagrees between `snapshot` mode, `live` mode, and `nodetool`-level CQL `SELECT count(*)` | The exact SSTable file set at the time of the mismatch (`nodetool listsnapshots` + `ls` of the data dir on the affected node), the snapshot name used (if any), and the full query text | Which of the three counts disagreed and by how much, the captured SSTable set, and whether the gap is present in `snapshot` mode alone (data bug) or only in `live` mode (race, not a data bug) |
| **gRPC/Flight stream resets** | Client (Trino) or `trino-loadtest` driver logs a connection reset / stream abort mid-scan | `kubectl logs` from both the Trino worker pod and the affected `cqlite-flight` pod for the same time window; `cqlite_rpc_in_flight` around the reset; whether `--traceparent` produced a partial trace (truncated span) | Both pods' logs side by side, the in-flight gauge graph, and the partial trace if present |
| **OOM / memory growth of the flight DaemonSet** | `kubectl top pod -l easydblab.com/kit=cqlite-flight` shows unbounded growth over the run, or a pod restarts | `kubectl top pod -l easydblab.com/kit=cqlite-flight` sampled every few minutes across the whole run; `kubectl get pods -l easydblab.com/kit=cqlite-flight -o jsonpath='{.items[*].status.containerStatuses[*].restartCount}'`; Pyroscope `alloc` flame graph for the pod over the same window | The memory-over-time samples, restart counts, and the allocation flame graph pointing at the hot allocation site |
| **Trino worker errors** (query failures not attributable to Flight) | A query fails with a Trino-side stack trace, no corresponding `cqlite_errors_total` increment | `kubectl logs deployment/trino-coordinator` and `deployment/trino-worker` for the failure window; the failing query text; whether the SPI-mismatch guard (0.3) is actually the cause before filing as a connector bug | Coordinator + worker logs, the query, and explicit confirmation the SPI/version pin (0.3) was not the actual cause |

---

## Scale notes

| Parameter | 3-node baseline (this plan) | Bigger run |
|---|---|---|
| DB nodes | 3 × `i4i.xlarge` | 6-9 × `i4i.xlarge` (proportionally more SSTables/node) |
| App nodes | 1 × `m5.xlarge` | 2-3 × `m5.xlarge` (more Trino workers — `install trino --workers N`) |
| Phase 1 write threads/duration | `--threads 50 -d 15m` | `--threads 200 -d 1h`+ |
| Phase 1 periodic flush interval | every 120s × 5 | every 60s for the full duration, or drop `min_compaction_threshold` via `cassandra.patch.yaml` to force compaction sooner |
| Phase 3b concurrent write threads/duration | `--threads 20 -d 10m` | `--threads 100 -d 30m`+ (heavier churn under the read load) |
| Phase 3 read threads/duration | `--threads 8-16`, `120s`/`300s` | `--threads 32-64`, `600s`+ |
| trino-loadtest query mix | built-in default (2 `LIMIT` scans + `COUNT(*)`) | `--queries-file` with representative production-shaped queries |

## Validation Checklist

- [ ] Maven Central has `in.mcfad:cqlite-trino` at the version under test (0.1)
- [ ] `easy-db-lab kit source add` + named `kit install` used for all three kits, not `--from` (0.2)
- [ ] `trino` kit installed with `--version 481`; `trino-cqlite/bin/start.sh`'s SPI check passes (0.3)
- [ ] `cqlite-flight` DaemonSet: one Running pod per db node
- [ ] `cqlite` catalog visible via `$CLUSTER_DIR/cqlite/bin/verify.sh` with no keyspace/table args
- [ ] Phase 1 write produces ≥5 flushed SSTables and at least one compaction (`nt compactionstats` shows activity)
- [ ] Phase 2 final flush leaves 0 pending `FlushWriter`/`MemtablePostFlush` tasks
- [ ] Phase 3a (snapshot) row count matches Cassandra's own `SELECT count(*)` exactly
- [ ] Phase 3b (live) completes without a fail-closed connector error (or, if it errors, it's filed per Phase 5)
- [ ] `cqlite_rpc_*` / `cqlite_errors_total` metric names confirmed live in VictoriaMetrics
- [ ] CQLite Flight — RPC Metrics dashboard panels populate for both Phase 3 runs
- [ ] A Tempo trace exists spanning client → flight → core for at least one traced query
- [ ] Trino CPU/alloc visible in Pyroscope under `service_name=trino`
- [ ] `nodetool listsnapshots` is empty of `cqlite-*` entries after both Phase 3 runs stop
- [ ] Every anomaly from Phase 3/4 has either a filed `bug`+`performance` issue or an explicit "not reproducible" note

## Notes

- Never overwrite `cassandra.patch.yaml`; `cassandra use 5.0` generates it with
  the required snitch/data-dir settings.
- `stress start` requires `--` before workload args when passing `--name`/`--tags`:
  `stress start --name foo --tags "k=v" -- KeyValue -d 5m --threads 50`.
- The `trino-cqlite` overlay's `post-workload-start`/`post-workload-stop` hooks
  fire on **any** kit start/stop (unfiltered — matches the trino kit's own
  Pyroscope re-patch behavior) and re-run `bin/reapply-plugin-patch.sh`. This is
  cheap and idempotent; don't be surprised to see it fire during Phase 1/3 stress
  job starts/stops.
- If `cassandra cql` returns `No node was available` despite `nt status` showing
  all nodes `UN`, the Sidecar needs more time — wait 30s and retry.
