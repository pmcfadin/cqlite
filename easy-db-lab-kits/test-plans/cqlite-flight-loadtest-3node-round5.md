# Lab Plan: cqlite-flight + Trino Loadtest (3-Node) — ROUND 5

## Round-5 context (read first)

This is the **first field run of the post-#2227 build** — the multi-node snapshot
fix. It is the epic-AM Wave-0 field validation and is coordinated on **cqlite
epic [#2286](https://github.com/pmcfadin/cqlite/issues/2286)** (single source of
truth for round-5 assets + per-issue verdict checklist). **Post ALL round-5
results as comments on #2286**; per-issue detail may additionally go on the issue
it adjudicates. easy-db-lab-side defects are **comment-only upstream** on
`rustyrazorblade/easy-db-lab` (never fixed in the cqlite tree; standing owner rule).

### Pinned assets (authoritative — do not run with anything else, per #2286)

| Artifact | Value |
|---|---|
| cqlite-flight image | `ghcr.io/pmcfadin/cqlite-flight:dev` @ **`sha256:289f97e0ec07d9f7fb41efd6bcd8d9b9210ba3cca2584ced02302b9d86bb7ce1`** (multi-arch amd64+arm64, built from main @ `19b32461`, run 29036815987) |
| Trino connector | **`in.mcfad:cqlite-trino:0.13.4`** (Maven Central — verified published) |
| Kit | easy-db-lab kit with `--read-mode` flag |
| Cluster shape | 3× `i4i.xlarge` db + 1× `m5.xlarge` app, Cassandra 5.0.x, nb-big/LZ4 |

### What this build contains (vs round-4's `50557839…`, which predated ALL of it)

- **Flight/Rust:** #2264 (cancellation-aware backpressure — the LIMIT-hang fix),
  #2228 (full-ring token semantic), #2240, #2193 error surfacing.
- **Connector/Java (0.13.4 vs 0.13.3):** #2227 (per-replica-host snapshots — the
  multi-node snapshot blocker), #2228 (Java side), #2229 (hide+warn + TIME),
  #2236, #2238, #2239, #2241 (split replica failover).

All of #2227, #2228, #2229, #2241, #2264 are **CLOSED** upstream — this run
adjudicates them in the field.

### Round-5 delta from round 4 (why the round-4 verdicts flipped)

Round 4 (`50557839…` + 0.13.3) found:
- **#2157** LIMIT scans hung → ABANDONED (418s), `in_flight` gauge stuck at 8 —
  this is exactly the parked-`blocking_send`-on-cancel bug **#2264** now claims to
  fix. Round 5 must confirm LIMIT returns in **seconds**.
- **#2193** tiny `nb-big` read → `Failed to read message`, no ERROR log, no
  `cqlite_errors_total`. Round 5's #2193 error-surfacing must make the failure
  *loud* if it still reproduces.
- **#2227** was never field-tested (round 4's read path was too broken to reach
  it). Round 5 is its first multi-node validation.

### Round-5 verdict checklist (mirrors #2286 — fill and post to #2286)

- [ ] **#2157 (LIMIT runtime):** `LIMIT 5` returns promptly (seconds, not
  ABANDONED). PASS → close #2157. FAIL → capture `cqlite_rpc_in_flight` /
  `cqlite_rpc_in_flight_ratio` + phase metrics during the hang; #2264's fix needs
  field escalation.
- [ ] **#2193 (tiny decode):** `SELECT * FROM cqlite.<ks>.tiny` returns 3 rows.
  PASS → close #2193. FAIL → offline framing already ruled out (Phase-1 oracle,
  PR #2284); capture the Trino-runtime arrow-java version (coordinator classpath)
  + a client-side stack trace + the flight pod ERROR line.
- [ ] **#2227 (default snapshot multi-node):** default-mode queries succeed across
  all 3 replicas' splits (splits land on hosts other than the configured
  Sidecar's node). Epic-AM Wave-0 field validation.
- [ ] **#2228 (full-range SELECT \*):** full-range `SELECT *` returns complete
  counts (no silent 0-row).
- [ ] **#2229 (unsupported column):** a table with an unsupported column serves
  its supported columns + warns (not table-poisoned). Includes a CQL `TIME`
  column mapping check.
- [ ] **#2241 (split replica failover):** one query against a killed flight pod
  succeeds via fallback; loud failure only if all replicas down.
- [ ] **Metrics sanity:** `cqlite_rpc_in_flight` returns to 0 after each query
  (the #2264 gauge-leak signature is gone). `cqlite_errors_total` may legitimately
  be absent until the first error (lazy registration — an open Phase-4 item on
  #2193).

## Goal

Same harness intent as rounds 1–4: drive real CQL writes into a 3-node
Cassandra 5.0 cluster, flush to SSTables, read back through Trino via Arrow
Flight in both `snapshot` and `live` mode, confirm observability wiring, and
triage anything broken into issues on #2286. This is the harness that *produces*
bug reports — not itself a benchmark deliverable (epic non-goal).

## Environment

- 3 DB nodes: `i4i.xlarge` (local NVMe, no EBS) — Cassandra 5.0 + Sidecar +
  co-located `cqlite-flight` pod (DaemonSet, one per db node)
- 1 App node: `m5.xlarge` — Trino coordinator + worker, `trino-loadtest` driver
  pod, and the `cassandra-easy-stress` K8s jobs
- Cassandra version: 5.0
- Trino version: **481** (pinned — see [0.3](#03-trino-must-be-pinned-to-481-not-the-lab-default-474))

## Prerequisites

Read all before starting. Each is a verified finding from prior rounds, not a guess.

### 0.1 The connector must be on Maven Central at the version under test

`trino-cqlite`'s init container resolves the published
`in.mcfad:cqlite-trino:<version>` artifact from Maven Central at pod-start time.
Round 5 pins **`0.13.4`** (adds #2227 per-replica-host snapshots, #2229 hide+warn
+ TIME, #2241 split replica failover on top of round-4's 0.13.3). Confirm it is
resolvable before you start (already verified once during planning — re-confirm
at run time):

```bash
curl -sf "https://repo1.maven.org/maven2/in/mcfad/cqlite-trino/0.13.4/cqlite-trino-0.13.4.pom" \
  -o /dev/null && echo "published" || echo "NOT PUBLISHED — stop here"
```

If it's not resolvable, `easy-db-lab cqlite start`'s init container
(`cqlite-plugin-fetch`) fails with `Could not resolve in.mcfad:cqlite-trino:0.13.4`
and this plan cannot proceed past step 5.

### 0.2 Kit install path: `kit source add`, not `--from`

Unchanged from round 4. `--from` does not substitute a kit's declared `args:`
(verified against `InstallTemplateResolver.kt` / `commands/kit/Install.kt`). Use
out-of-tree source registration, which wires args correctly like a built-in kit:

```bash
easy-db-lab kit source add cqlite /path/to/cqlite/easy-db-lab-kits
```

Then `easy-db-lab kit install <name> <args...>` per kit, where `<name>` is the
kit's **source-directory name** (`cqlite-flight`, `trino-cqlite`,
`trino-loadtest`).

**Naming subtlety for the `trino-cqlite` overlay:** `resolveInstanceName()` falls
back to the kit's own `name:` field in `kit.yaml`, and `trino-cqlite/kit.yaml`
sets `name: cqlite`. So the *dispatch verb* is `trino-cqlite` but the *installed
dir / runner / catalog name* is `cqlite`:

```bash
easy-db-lab kit install trino-cqlite --connector-version 0.13.4 ...   # dispatch verb
easy-db-lab cqlite start                                              # runner/catalog name
```

### 0.3 Trino must be pinned to 481, not the lab default 474

Unchanged from round 4. The `cqlite-trino` connector targets Trino SPI **481**;
`trino-cqlite/bin/start.sh` fails closed if the running coordinator isn't 481.
Install the `trino` kit with `--version 481` (step 3).

**Known upstream blocker at 481 — worker CrashLoopBackOff (#2116, still open).**
The trino kit's `values.yaml.template` puts `web-ui.*` in top-level
`additionalConfigProperties`, which the Helm chart renders into BOTH coordinator
and worker `config.properties`; `web-ui.*` is coordinator-only, so 481 workers
fail-fast. **Workaround:** after `trino` kit install, remove the two `web-ui.*`
lines from the rendered `trino/values.yaml`, re-run the kit's `helm upgrade`, and
verify both `trino-worker` pods reach `Ready` before step 4.

### 0.4 Flight kit preflight

Unchanged from round 4 (all previously-hit bugs — #2118 non-root uid,
`CreateContainerConfigError` — are fixed). First-run checks that still matter:

- **Cassandra data-dir gid:** image runs as fixed `uid 10001`; manifest adds
  `supplementalGroups: [<data-gid>]` (default `999`). If the host data dir isn't
  group-readable for that gid, pass `--data-gid <actual-gid>`.
- **GHCR image pull:** `ghcr.io/pmcfadin/cqlite-flight:dev` is public. If it 403s,
  the image needs a pull secret.
- **Trino → db-node `:8815` reachability** (`hostNetwork: true`) — confirm from an
  app node before relying on it:
  ```bash
  $EDB exec run -t stress -- bash -c 'echo > /dev/tcp/<db0-private-ip>/8815 && echo OK'
  ```

### 0.5 SOCKS proxy for kit commands (harness environment)

Non-tailscale clusters reach the K8s API over a SOCKS proxy on `localhost:1080`.
**`easy-db-lab trino start` and all kit `helm`/`kubectl` commands fail with
"kubernetes cluster unreachable" unless these are exported** in the shell:

```bash
export ALL_PROXY=socks5h://localhost:1080
export HTTPS_PROXY=socks5h://localhost:1080
export HTTP_PROXY=socks5h://localhost:1080
```

SSH-based `cassandra` commands work *without* them. This was round-4 harness
observation #735/#736. **PR #737 (branch `fix/735-jdbc-socks-proxy`) routes the
`<kit> sql` JDBC path through SOCKS.** If #737 has NOT merged by run time,
`trino sql` still needs:

```bash
export JAVA_TOOL_OPTIONS="-DsocksProxyHost=localhost -DsocksProxyPort=1080 -DsocksProxyVersion=5"
```

Check whether #737 merged (`gh pr view 737 --repo rustyrazorblade/easy-db-lab
--json state`); if merged and this cluster is provisioned from the merged build,
drop the `JAVA_TOOL_OPTIONS` workaround and confirm `trino sql` works without it
(that confirmation is itself a #737 field-validation datapoint worth noting).

---

## Steps

### 1. Create cluster workspace and provision

```bash
CLUSTER_DIR="clusters/cqlite-flight-r5-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$CLUSTER_DIR"
bin/create-easy-db-lab-wrapper "$CLUSTER_DIR"
EDB="$CLUSTER_DIR/easy-db-lab"
$EDB init cqlite-flight-r5 --db 3 --app 1 \
  --instance i4i.xlarge --stress-instance m5.xlarge --up
```

Wait for provisioning + K3s (~5 min). Once `kubeconfig` exists, export it so raw
`kubectl`/script invocations target this cluster, and export the SOCKS vars (0.5):

```bash
export KUBECONFIG="$CLUSTER_DIR/kubeconfig"
export ALL_PROXY=socks5h://localhost:1080 HTTPS_PROXY=socks5h://localhost:1080 HTTP_PROXY=socks5h://localhost:1080
```

### 2. Bring up Cassandra 5.0

```bash
$EDB cassandra use 5.0
$EDB cassandra update-config
$EDB cassandra start
$EDB cassandra nt status
```

All 3 nodes must show `UN` before continuing. **Confirm RF will exceed 1** — #2227
is only exercised when splits fan out to replica hosts other than the Sidecar's
node, which requires RF ≥ 2 (the default stress keyspace uses RF depending on the
workload; verify after Phase 1, step 8b).

### 3. Install Trino, pinned to 481 (see [0.3](#03-trino-must-be-pinned-to-481-not-the-lab-default-474))

```bash
$EDB kit install trino --version 481
$EDB trino start
$EDB trino status
```

Apply the #2116 `web-ui.*` workaround (0.3) and confirm both worker pods reach
`Ready` before proceeding.

### 4. Register the kit source and install cqlite-flight (DIGEST-PINNED)

Pin the digest, not the moving `dev` tag — OCI accepts the `tag@sha256:` form, so
this reference cannot drift:

```bash
$EDB kit source add cqlite /path/to/cqlite/easy-db-lab-kits
$EDB kit install cqlite-flight \
  --tag "dev@sha256:289f97e0ec07d9f7fb41efd6bcd8d9b9210ba3cca2584ced02302b9d86bb7ce1" \
  --flight-port 8815
$EDB cqlite-flight start
```

Verify one pod per db node **and the digest**:

```bash
kubectl get pods -l easydblab.com/kit=cqlite-flight -o wide
kubectl get pods -l easydblab.com/kit=cqlite-flight \
  -o jsonpath='{range .items[*]}{.metadata.name}{"  "}{.status.containerStatuses[0].imageID}{"\n"}{end}'
```

Every `imageID` must end in
`289f97e0ec07d9f7fb41efd6bcd8d9b9210ba3cca2584ced02302b9d86bb7ce1`. If any pod is
stale (from a prior install on a reused cluster), force recreation:

```bash
kubectl rollout restart daemonset -l easydblab.com/kit=cqlite-flight
kubectl rollout status  daemonset -l easydblab.com/kit=cqlite-flight --timeout=120s
```

**Do not attribute any verdict to a pod whose digest isn't `289f97e0…`.**

Confirm observability compiled in and multi-disk decision (both non-fatal):

```bash
kubectl logs -l easydblab.com/kit=cqlite-flight --all-containers --prefix | grep -i observability
kubectl logs -l easydblab.com/kit=cqlite-flight -c detect-multidisk --prefix
```

Expect one `observability enabled, exporting to OTLP endpoint` line per pod
(endpoint `http://localhost:4317`).

### 5. Install the trino-cqlite overlay — snapshot mode (see [0.2](#02-kit-install-path-kit-source-add-not---from))

All the round-3/4 install bugs (#2119/#2120/#2122/#2123/#2158/#2159) are fixed;
expect the `trino-coordinator` pod to be recreated **once** during this step
(that is `strategy: Recreate`, not a hang).

```bash
DB0_IP=$($EDB ip db0 --private)
$EDB kit install trino-cqlite --connector-version 0.13.4 --flight-port 8815 \
  --sidecar-uri "http://${DB0_IP}:9043" --trino-image-tag 481 --read-mode snapshot
$EDB cqlite start
"$CLUSTER_DIR/cqlite/bin/verify.sh"
```

Expected: `SHOW CATALOGS` lists `cqlite`; `SHOW SCHEMAS FROM cqlite` lists at
least `system`.

### 6. Install trino-loadtest

```bash
$EDB kit install trino-loadtest --target trino
```

No `start` yet — that's Phase 3.

---

## Phase 1: WRITE

Drive real CQL writes with `cassandra-easy-stress` to produce non-trivial SSTable
volume across all 3 nodes (multiple flushes + at least one compaction).

### 7. Start the write load

```bash
$EDB cassandra stress start --name cqlite-write-baseline --tags "phase=write-baseline" \
  -- KeyValue -d 15m --threads 50
```

### 8. Discover the generated keyspace/table and confirm RF ≥ 2 (#2227 precondition)

```bash
$EDB cassandra stress status
$EDB cassandra cql "SELECT keyspace_name, table_name FROM system_schema.tables WHERE keyspace_name NOT IN ('system','system_schema','system_auth','system_distributed','system_traces','system_views','system_virtual_schema')"
```

Set `KEYSPACE`/`TABLE` from the result. **Then confirm RF ≥ 2** — #2227's
multi-node snapshot fix is only exercised when a query's splits touch replica
hosts other than the configured Sidecar's node:

```bash
$EDB cassandra cql "SELECT keyspace_name, replication FROM system_schema.keyspaces WHERE keyspace_name = '${KEYSPACE}'"
```

If RF = 1, raise it so data lands on multiple nodes (else #2227 cannot be
adjudicated — note this in the #2286 comment if you cannot):

```bash
$EDB cassandra cql "ALTER KEYSPACE ${KEYSPACE} WITH replication = {'class':'NetworkTopologyStrategy','<dc>':3}"
# then repair/rewrite so replicas are populated, e.g. re-run the stress write above
```

### 8b. Create the tiny 3-row table (#2193 discriminator)

Same shape as round 4 — 3-row `nb-big` + LZ4 in the stress keyspace:

```bash
$EDB cassandra cql "CREATE TABLE IF NOT EXISTS ${KEYSPACE}.tiny (key text PRIMARY KEY, value text)"
$EDB cassandra cql "INSERT INTO ${KEYSPACE}.tiny (key, value) VALUES ('1','1')"
$EDB cassandra cql "INSERT INTO ${KEYSPACE}.tiny (key, value) VALUES ('2','2')"
$EDB cassandra cql "INSERT INTO ${KEYSPACE}.tiny (key, value) VALUES ('3','3')"
$EDB cassandra nt flush "$KEYSPACE"
$EDB cassandra cql "SELECT count(*) FROM ${KEYSPACE}.tiny"   # must be 3
```

### 8c. Create the unsupported-column table (#2229 discriminator) — NEW round 5

#2229 claims a table with an unsupported column now serves its *supported* columns
+ warns (not table-poisoned), and that CQL `TIME` maps into Trino. Create a table
mixing a supported text column with a CQL `TIME` column, write a few rows, flush:

```bash
$EDB cassandra cql "CREATE TABLE IF NOT EXISTS ${KEYSPACE}.mixed (key text PRIMARY KEY, label text, t time)"
$EDB cassandra cql "INSERT INTO ${KEYSPACE}.mixed (key, label, t) VALUES ('a','alpha','08:30:00')"
$EDB cassandra cql "INSERT INTO ${KEYSPACE}.mixed (key, label, t) VALUES ('b','bravo','14:45:15')"
$EDB cassandra nt flush "$KEYSPACE"
```

(If `time` now maps cleanly rather than being "unsupported", note it — #2229's
scope was TIME-mapping + hide-and-warn for genuinely unsupported types; adjust the
discriminator to whatever type the connector still cannot map if `time` is
supported. Capture the actual behavior either way.)

### 9. Force multiple SSTable generations during the write

```bash
for i in 1 2 3 4 5; do
  sleep 120
  $EDB cassandra nt flush "$KEYSPACE"
  $EDB cassandra nt compactionstats
done
```

Watch for a non-empty pending/active compaction after the 4th–5th flush.

### 10. Wait for the write job to finish

```bash
$EDB cassandra stress status
```

Wait for `cqlite-write-baseline` to complete (or `stress stop
cqlite-write-baseline --force` once you have enough data — see Scale notes).

---

## Phase 2: FLUSH (mandatory)

The connector only sees **flushed** SSTables. Run one final flush regardless of
Phase 1's periodic flushes:

### 11. Final flush on all db nodes

```bash
$EDB cassandra nt flush "$KEYSPACE"
$EDB cassandra nt tpstats
```

`FlushWriter`/`MemtablePostFlush` pending should be 0 on all 3 nodes.

---

## Phase 3: READ

Per #2286's run matrix: default snapshot sweep (the #2227 main event) → live-mode
sweep → targeted verdict checks (#2193, #2157, #2241).

### 12. Sanity check row count before either run

```bash
$EDB cassandra cql "SELECT count(*) FROM ${KEYSPACE}.${TABLE}"
"$CLUSTER_DIR/cqlite/bin/verify.sh" "$KEYSPACE" "$TABLE"
$EDB trino sql "SELECT count(*) FROM cqlite.${KEYSPACE}.${TABLE}"
```

Counts must match exactly. A mismatch here is a #2227/#2228 correctness finding
before the concurrent runs even start.

### 12a. #2227 verdict — default snapshot multi-node (THE MAIN EVENT)

This is the first field validation of per-replica-host snapshots. In `snapshot`
mode (the default from step 5), run a full-ring `SELECT *` whose splits fan across
all 3 replica hosts — NOT just the Sidecar's node. Round 4 could never reach this
because the read path hung; #2264 must have cleared that first.

```bash
# Full-ring scan — must return the complete count, splits on all 3 hosts
$EDB trino sql "SELECT count(*) FROM cqlite.${KEYSPACE}.${TABLE}"
$EDB trino sql "SELECT * FROM cqlite.${KEYSPACE}.${TABLE} LIMIT 20"
```

While it runs, in a second shell confirm splits reached hosts other than
`db0`/the Sidecar node (transient `cqlite-<queryId>` snapshots should appear on
**all** replica nodes, not just the Sidecar's):

```bash
watch -n2 "$EDB cassandra nt listsnapshots"
```

- **Complete count, no `NotFound` → #2227 PASS** (note the digest + that splits
  demonstrably touched non-Sidecar hosts). This is the epic-AM Wave-0 field pass.
- **`NotFound` / partial count** → #2227 FAIL: capture the failing split's ticket
  JSON (snapshot name + host), which host was missing the snapshot dir, and the
  flight pod log on that host. Post on #2227 + #2286.

### 12b. #2228 verdict — full-range SELECT * returns complete counts

#2228 fixed a `start==end` full-ring token range being treated as empty (silent
0 rows). Confirm the unbounded `SELECT *` count equals Cassandra's own count
(already compared in step 12) and is **non-zero**:

```bash
$EDB trino sql "SELECT count(*) FROM cqlite.${KEYSPACE}.${TABLE}"   # must equal Cassandra count, not 0
```

A silent 0 (or short count) with a non-empty table = #2228 regression → capture +
file.

### 12c. #2193 verdict — tiny-table read

```bash
"$CLUSTER_DIR/cqlite/bin/verify.sh" "$KEYSPACE" tiny
$EDB trino sql "SELECT * FROM cqlite.${KEYSPACE}.tiny"
```

- **3 rows → close #2193** (note round-5 digest `289f97e0…`).
- **Still fails** → #2193's error surfacing should now make it loud. Capture ALL of:
  ```bash
  # server-side ERROR line (RUST_LOG=info suffices per #2193 fix)
  kubectl logs -l easydblab.com/kit=cqlite-flight --all-containers --prefix --since=5m \
    | grep -iE "flight rpc failed|error|encode|arrow"
  # cqlite_errors_total (may be lazily registered — note if absent)
  source "$CLUSTER_DIR/env.sh"
  with-proxy curl -s "http://control0:8428/api/v1/query?query=cqlite_errors_total" | jq -r ".data.result[]"
  # client-side: Trino runtime arrow-java version on the coordinator classpath
  kubectl exec deployment/trino-coordinator -- bash -c 'ls /usr/lib/trino/lib | grep -i arrow'
  ```
  Post the arrow-java version + client stack trace + server ERROR line on #2193
  and #2286 (the remaining axes are environmental: x86_64 build, runtime
  arrow-java, live gRPC).

### 12d. #2157 verdict — LIMIT runtime (the #2264 fix)

Round 4: `LIMIT 5` ran 418s → ABANDONED, `in_flight` gauge stuck at 8. #2264
(cancellation-aware backpressure) must fix this.

```bash
# Expect low SECONDS, not minutes/ABANDONED. Watch cqlite_rpc_in_flight in a 2nd shell.
time $EDB trino sql "SELECT * FROM cqlite.${KEYSPACE}.${TABLE} LIMIT 5"
time $EDB trino sql "SELECT * FROM cqlite.${KEYSPACE}.${TABLE} LIMIT 100"
# point-read predicate pushdown — EXPLAIN must show a NON-EMPTY filterJson
$EDB trino sql "EXPLAIN SELECT * FROM cqlite.${KEYSPACE}.${TABLE} WHERE key = '<a-real-key>'"
```

**Metrics sanity (the #2264 gauge-leak signature):** after each LIMIT query
completes, `cqlite_rpc_in_flight` must return to **0** (round 4 it was stuck at 8):

```bash
source "$CLUSTER_DIR/env.sh"
with-proxy curl -s "http://control0:8428/api/v1/query?query=cqlite_rpc_in_flight" | jq -r ".data.result[].value[1]"
```

- **`LIMIT 5` in seconds + `in_flight` returns to 0 → close #2157.**
- **Still slow/ABANDONED on `289f97e0…`** → live #2264 field escalation: capture
  the `cqlite_rpc_phase_duration_seconds` split (which phase holds the time) +
  `cqlite_rpc_in_flight_ratio` during the hang. Post on #2157 + #2286.

### 12e. #2229 verdict — unsupported column hide-and-warn — NEW round 5

Query the `mixed` table from 8c. The supported columns must return; an
unsupported column must be hidden with a warning, not poison the whole table:

```bash
$EDB trino sql "DESCRIBE cqlite.${KEYSPACE}.mixed"        # which columns are exposed
$EDB trino sql "SELECT key, label FROM cqlite.${KEYSPACE}.mixed"   # supported cols return
$EDB trino sql "SELECT * FROM cqlite.${KEYSPACE}.mixed"   # TIME maps, or col hidden + warns
```

- **Supported columns return + TIME maps (or is hidden with a warning) → #2229 PASS.**
- **Whole-table error / `mixed` unqueryable → #2229 FAIL** → capture the
  coordinator error + `DESCRIBE` output. Post on #2229 + #2286.

### 13a. Run (a) — default `snapshot` mode loadtest

```bash
$EDB trino-loadtest-trino start --ks "$KEYSPACE" --tbl "$TABLE" \
  --threads 8 --duration 120 --traceparent
```

Watch transient per-query snapshots on **all** replica nodes (a second #2227
signal — snapshots should appear cluster-wide, not only on the Sidecar's node):

```bash
watch -n2 "$EDB cassandra nt listsnapshots"
```

```bash
$EDB trino-loadtest-trino stop
```

### 13b. Run (b) — `--read-mode live`: compaction-stress weak-spot hunt

Flip to live by reinstalling the overlay:

```bash
$EDB kit install trino-cqlite --connector-version 0.13.4 --flight-port 8815 \
  --sidecar-uri "http://${DB0_IP}:9043" --trino-image-tag 481 --read-mode live
$EDB cqlite start
grep cqlite.read-mode "$CLUSTER_DIR/cqlite/trino-catalog.properties"   # → cqlite.read-mode=live
```

Repeat the targeted verdict reads (12a–12e) in **live** mode — #2157/#2193's
closure criteria in #2286 are defined on `live` mode, so both modes must be
exercised:

```bash
$EDB trino sql "SELECT * FROM cqlite.${KEYSPACE}.tiny"              # #2193 in live
time $EDB trino sql "SELECT * FROM cqlite.${KEYSPACE}.${TABLE} LIMIT 5"   # #2157 in live
$EDB trino sql "SELECT count(*) FROM cqlite.${KEYSPACE}.${TABLE}"  # #2227/#2228 in live
```

Then race compaction against an in-flight scan — concurrent write churn + read load:

```bash
$EDB cassandra stress start --name cqlite-live-churn --tags "phase=live-hunt" \
  -- KeyValue -d 10m --threads 20
$EDB trino-loadtest-trino start --ks "$KEYSPACE" --tbl "$TABLE" \
  --threads 16 --duration 300 --traceparent
for i in 1 2 3; do sleep 60; $EDB cassandra nt flush "$KEYSPACE"; done
$EDB trino-loadtest-trino stop
$EDB cassandra stress stop cqlite-live-churn --force
```

### 13c. #2241 verdict — split replica failover (killed flight pod) — NEW round 5

#2241 fixed a single down Flight endpoint failing the whole query at RF≥2. Kill
ONE flight pod, then run a full-ring query: it must **succeed** via failover to
another replica. Only an all-replicas-down case should fail loudly.

```bash
# Snapshot the pod set, kill exactly one
kubectl get pods -l easydblab.com/kit=cqlite-flight -o wide
VICTIM=$(kubectl get pods -l easydblab.com/kit=cqlite-flight -o name | head -1)
kubectl delete "$VICTIM" --wait=false     # DaemonSet will recreate; query the gap
# Immediately (before recreate) run a full-ring scan — expect SUCCESS via failover
$EDB trino sql "SELECT count(*) FROM cqlite.${KEYSPACE}.${TABLE}"
```

- **Query succeeds while one pod is down → #2241 PASS** (splits that targeted the
  dead endpoint fell back to another replica).
- **Whole query fails → #2241 FAIL** → capture the coordinator error naming the
  dead endpoint + confirm the other 2 pods were Ready. Post on #2241 + #2286.

Let the DaemonSet recreate the victim and confirm it's back on digest `289f97e0…`
before continuing:

```bash
kubectl rollout status daemonset -l easydblab.com/kit=cqlite-flight --timeout=120s
```

Revert to `snapshot` mode if re-running Phase 3a:

```bash
$EDB kit install trino-cqlite --connector-version 0.13.4 --flight-port 8815 \
  --sidecar-uri "http://${DB0_IP}:9043" --trino-image-tag 481 --read-mode snapshot
$EDB cqlite start
```

---

## Phase 4: OBSERVE / VERIFY

### 14. Confirm `cqlite_rpc_*` metric names + mid-query movement

```bash
source "$CLUSTER_DIR/env.sh"
with-proxy curl -s "http://control0:8428/api/v1/label/__name__/values" | grep cqlite_rpc
with-proxy curl -s "http://control0:8428/api/v1/label/__name__/values" \
  | grep -E "cqlite_rpc_rows_total|cqlite_rpc_bytes_total|cqlite_rpc_phase_duration_seconds|cqlite_rpc_in_flight"
```

During a long scan (e.g. the step-12 `count(*)`), in a second shell watch rows
climb and phase durations split — and confirm `in_flight` is non-zero *during* and
0 *after* (the #2264 signature):

```bash
watch -n2 'source "$CLUSTER_DIR/env.sh"; \
  echo "rows:"; with-proxy curl -s "http://control0:8428/api/v1/query?query=cqlite_rpc_rows_total" | jq -r ".data.result[].value[1]"; \
  echo "in_flight:"; with-proxy curl -s "http://control0:8428/api/v1/query?query=cqlite_rpc_in_flight" | jq -r ".data.result[].value[1]"; \
  echo "--- phase durations ---"; \
  with-proxy curl -s "http://control0:8428/api/v1/query?query=cqlite_rpc_phase_duration_seconds_sum" | jq -r ".data.result[] | \"\(.metric.phase)=\(.value[1])\""'
```

Verdict: rows non-zero + climbing; phase durations attribute time to
`resolve|merge_setup|stream`; `in_flight` drains to 0 after each query. A stall
with all time in one phase, zero rows, and a stuck gauge = round-4 symptom
persisting → capture + file on #2264/#2157.

Post-compaction reconciliation deltas:

```bash
source "$CLUSTER_DIR/env.sh"
for m in cqlite_merge_rows_in cqlite_merge_rows_out cqlite_compaction_tombstones_suppressed; do
  echo "$m:"; with-proxy curl -s "http://control0:8428/api/v1/query?query=$m" | jq -r ".data.result[].value[1]"
done
```

### 14c. Presence-oracle soak (OPT-IN, P0-if-nonzero)

`cqlite_read_bloom_false_negatives` is off by default. To run the correctness
soak, enable it on ONE pod and drive read load:

```bash
POD=$(kubectl get pods -l easydblab.com/kit=cqlite-flight -o name | head -1)
kubectl set env "$POD" CQLITE_VERIFY_PRESENCE_ORACLE=1   # restarts that one pod
# ... drive a Phase 3 read run, then:
with-proxy curl -s "http://control0:8428/api/v1/query?query=cqlite_read_bloom_false_negatives" | jq -r ".data.result[].value[1]"
```

**Any `> 0` is a P0 finding — report on #2286 immediately** (a false negative
means a live row was silently skipped). Zero across the soak is the pass.

### 15. Grafana dashboard, traces, profiles

- **Grafana** (`http://<control-node-private-ip>:3000`): open **CQLite Flight —
  RPC Metrics**, set `$cluster`/`$service_name` (`cqlite-flight`), confirm all 5
  panel rows populate during Phase 3 runs.
- **Tempo:** both Phase 3 runs used `--traceparent`; confirm a trace spans client
  (Trino) → `cqlite-flight` → `cqlite-core` and `span.rpc.method` is filterable.
- **Pyroscope:** datasource Pyroscope, `service_name=trino`, confirm CPU/alloc
  load concentrated in the `cqlite_flight` plugin path (not idle/GC-only) during
  Phase 3.

### 18. Confirm snapshot lifecycle cleanup

```bash
$EDB cassandra nt listsnapshots
```

Expected: empty of `cqlite-*` entries once both Phase 3 runs stop — every
`cqlite-<queryId>` snapshot should have been deleted by Trino's `cleanupQuery`
hook. **With #2227's per-replica-host snapshots, verify cleanup on ALL 3 nodes,
not just the Sidecar's** — a leak on a non-Sidecar node is a new #2227-adjacent
finding. A growing `cqlite-*` list after runs stop = leak-class finding → capture
+ file.

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

Confirm AWS is clean after teardown (no lingering instances/volumes billing).

---

## Phase 5: FAILURE TRIAGE → ISSUES ON #2286

Every anomaly from Phases 3–4 becomes a comment on **epic #2286** (per-issue
detail may additionally go on the adjudicated issue). easy-db-lab-side defects:
**comment-only upstream** on `rustyrazorblade/easy-db-lab` (never fixed in the
cqlite tree). These are oracle-driven parity/correctness bugs — plain GitHub
comments + pinned reproduction, **not** OpenSpec changes.

| Failure class | Symptom / trigger | Data to capture |
|---|---|---|
| **#2227 multi-node snapshot `NotFound`** | Snapshot-mode query fails / short count with splits on non-Sidecar hosts | Failing split ticket JSON (snapshot name + host), which host lacked the snapshot dir, `nt listsnapshots` on all 3 nodes, flight pod log on the failing host |
| **#2264 LIMIT hang** | `LIMIT` query slow/ABANDONED, `cqlite_rpc_in_flight` stuck > 0 after completion | `cqlite_rpc_phase_duration_seconds` split, `cqlite_rpc_in_flight`/`_ratio` during + after, Trino task state, flight pod log |
| **#2193 tiny decode** | `tiny` read fails `Failed to read message` | Server ERROR line (RUST_LOG=info), `cqlite_errors_total`, coordinator arrow-java version, client stack trace |
| **#2228 silent 0-row** | Full-range `SELECT *` returns 0 / short on a non-empty table | The query, Cassandra `count(*)` vs Trino count, the ticket's token range |
| **#2229 table poisoned** | A table with an unsupported column is wholly unqueryable | `DESCRIBE`, coordinator error, which column, whether TIME mapped |
| **#2241 failover** | Query fails when one flight pod is down at RF≥2 | Coordinator error naming the dead endpoint, Ready-state of the other 2 pods, RF of the keyspace |
| **Live-mode compaction race** | Errors/resets during a scan overlapping forced flush/compaction | Flight pod logs (`--since=30m`), `cqlite_errors_total by (cqlite_error_category)`, Tempo trace, ticket JSON; whether it repros in snapshot mode |
| **Wrong row counts (snapshot vs live vs CQL)** | Step 12 counts disagree across modes | SSTable file set at mismatch time, snapshot name, full query; which mode disagreed and by how much |
| **OOM / memory growth** | `kubectl top pod` unbounded growth or pod restart | `kubectl top pod` samples across the run, restart counts, Pyroscope `alloc` flame graph |

---

## Scale notes

| Parameter | 3-node baseline (this plan) | Bigger run |
|---|---|---|
| DB nodes | 3 × `i4i.xlarge` | 6-9 × `i4i.xlarge` |
| App nodes | 1 × `m5.xlarge` | 2-3 × `m5.xlarge` (`install trino --workers N`) |
| Phase 1 write | `--threads 50 -d 15m` | `--threads 200 -d 1h`+ |
| Phase 1 flush interval | every 120s × 5 | every 60s, or drop `min_compaction_threshold` |
| Phase 3b write churn | `--threads 20 -d 10m` | `--threads 100 -d 30m`+ |
| Phase 3 read | `--threads 8-16`, `120/300s` | `--threads 32-64`, `600s`+ |

## Validation Checklist (post filled to #2286)

- [ ] Maven Central has `in.mcfad:cqlite-trino:0.13.4` (0.1)
- [ ] `kit source add` + named `kit install` used for all three kits, not `--from` (0.2)
- [ ] `trino` installed `--version 481`; #2116 `web-ui.*` workaround applied; workers Ready (0.3)
- [ ] SOCKS vars exported; `trino sql` works (note whether #737 merged, 0.5)
- [ ] `cqlite-flight` DaemonSet: one Running pod per db node, **digest == `289f97e0…`** (Step 4)
- [ ] `detect-multidisk` decision reviewed on every pod; observability line present (Step 4)
- [ ] RF ≥ 2 confirmed on the stress keyspace (#2227 precondition, Step 8)
- [ ] **#2227 verdict:** default snapshot full-ring scan returns complete count, splits touch non-Sidecar hosts (12a)
- [ ] **#2228 verdict:** full-range `SELECT *` count == Cassandra count, non-zero (12b)
- [ ] **#2193 verdict:** `tiny` returns 3 rows (close) OR ERROR line + arrow-java version + client trace captured (12c)
- [ ] **#2157 verdict:** `LIMIT 5` in seconds + `cqlite_rpc_in_flight` drains to 0 (close) OR field escalation captured (12d)
- [ ] **#2229 verdict:** `mixed` serves supported columns + TIME maps/warns, not poisoned (12e)
- [ ] **#2241 verdict:** query succeeds with one flight pod killed at RF≥2 (13c)
- [ ] Both `snapshot` AND `live` modes exercised for #2193/#2157 (closure criteria are on live)
- [ ] `cqlite_rpc_*` names confirmed live; rows climb mid-query; phase durations split; `in_flight` drains (14)
- [ ] Presence-oracle soak: `cqlite_read_bloom_false_negatives == 0` (any >0 → P0 on #2286) (14c)
- [ ] Grafana panels populate; Tempo client→flight→core trace exists; Pyroscope shows plugin load (15)
- [ ] `nt listsnapshots` empty of `cqlite-*` on ALL 3 nodes after runs stop (18)
- [ ] Every anomaly has a #2286 comment or explicit "not reproducible" note

## Notes

- Never overwrite `cassandra.patch.yaml`; `cassandra use 5.0` generates it.
- `stress start` requires `--` before workload args when passing `--name`/`--tags`.
- The `trino-cqlite` overlay's `post-workload-start/stop` hooks fire on any kit
  start/stop and re-run `bin/reapply-plugin-patch.sh` (cheap, idempotent).
- If `cassandra cql` returns `No node was available` despite `nt status` all `UN`,
  the Sidecar needs more time — wait 30s and retry.
- `count(*)` on the big keyvalue table can time out at LOCAL_QUORUM (normal) — if
  so, use a bounded `LIMIT` scan for the #2227/#2228 multi-node checks and note it.
- Long `easy-db-lab` commands hit a 2-min shell cap — background them.
