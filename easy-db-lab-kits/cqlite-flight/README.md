# cqlite-flight kit (easy-db-lab)

Deploys the published [`cqlite-flight`](../../cqlite-flight/README.md) Arrow Flight
server as a **DaemonSet co-located with Cassandra** — one pod per `type=db` node,
each reading that node's own local SSTables read-only. This is the data plane the
`trino-connector` talks to.

## What it deploys

A single `DaemonSet/cqlite-flight` in the `default` namespace:

- **`nodeSelector: {type: db}`** — schedules exactly one pod per Cassandra node,
  matching the pattern used by the Cassandra sidecar and OTel collector DaemonSets
  already in this lab.
- **`hostNetwork: true`** — the Flight gRPC port is bound directly on the node's
  network namespace, so it is reachable from Trino (or any other in-cluster pod)
  at `<db-node-private-ip>:<flight-port>` — the same node-IP-addressing model the
  connector already needs from Sidecar-based topology discovery. It also means
  `localhost:4317` on the pod reaches the **node-local** OTel collector DaemonSet
  with no extra Service/DNS hop (see [Observability](#observability)).
- **Read-only hostPath mount** of the Cassandra data directory (default
  `/mnt/db1/cassandra/data`, matching `cassandra.service`'s
  `data_file_directories`), mounted at `/data` in the container and passed to
  `--data-dir`.
- **Image**: `ghcr.io/pmcfadin/cqlite-flight:<tag>` (default `latest`).

Default read path is the **live** Cassandra data directory — this kit does not
select a Sidecar snapshot. Snapshot-consistent reads (a stable file set while
compaction runs underneath) are driven by the **trino-connector's Flight ticket**
(`"snapshot": "<name>"`), not by this kit. See the
[flight ticket contract](../../cqlite-flight/README.md#flight-ticket-contract).

No `dashboards/` are shipped by this change — auto-discovery will pick up any
`.json` files placed there later without any kit.yaml change.

## Args

| Flag | Variable | Default | Description |
|------|----------|---------|--------------|
| `--tag` | `TAG` | `latest` | `cqlite-flight` image tag. |
| `--flight-port` | `FLIGHT_PORT` | `8815` | Arrow Flight gRPC port (host + container, via `hostNetwork`). |
| `--data-dir` | `CASSANDRA_DATA_DIR` | `/mnt/db1/cassandra/data` | Host path to the Cassandra data dir, mounted read-only. |
| `--data-gid` | `CASSANDRA_DATA_GID` | `999` | Host GID that owns the Cassandra data dir; added as a pod `supplementalGroup`. |
| `--otel-endpoint` | `OTEL_ENDPOINT` | `http://localhost:4317` | OTLP gRPC endpoint (defaults to the node-local collector). |

## Installing out-of-tree

**The issue's assumed syntax, `easy-db-lab kit install <kit> --from <dir>`, does
not exist.** Verified against `InstallTemplateResolver.kt` / `commands/kit/Install.kt`
in the easy-db-lab source: the real ad-hoc flag set is `--from <dir> --kit <name>
--size <size>`, and — more importantly — **`--from` never wires a kit's declared
`args:`**. `Install.execute()` calls `renderAndWrite(source, kitName, storageSize)`
with no `extraVars`, so only the fixed `TemplateVariables` set (`CLUSTER_NAME`,
`KIT_NAME`, `DB_NODE_IPS`, `KUBECONFIG`, etc.) gets substituted — **not**
`__TAG__`, `__FLIGHT_PORT__`, `__CASSANDRA_DATA_DIR__`, `__CASSANDRA_DATA_GID__`,
or `__OTEL_ENDPOINT__`. `--from` also skips the `type: db` node-pool guard and any
typed `install:` steps (irrelevant here — this kit has none).

**Recommended path — register the kits directory as a source, then install by name**
(this wires args correctly, exactly like a built-in kit):

```bash
easy-db-lab kit source add cqlite /path/to/cqlite/easy-db-lab-kits
easy-db-lab kit install cqlite-flight --tag v0.13.0 --flight-port 8815
easy-db-lab cqlite-flight start
```

**Fallback — ad-hoc `--from`** (only if you don't want to register a source).
This scaffolds the kit with placeholders **unresolved**; you must hand-edit the
written files before running `start`:

```bash
easy-db-lab kit install --from /path/to/cqlite/easy-db-lab-kits/cqlite-flight \
  --kit cqlite-flight --size 0Gi
# Edit the scaffolded files to replace remaining __TAG__ / __FLIGHT_PORT__ /
# __CASSANDRA_DATA_DIR__ / __CASSANDRA_DATA_GID__ / __OTEL_ENDPOINT__ tokens:
#   <workdir>/cqlite-flight/daemonset.yaml
#   <workdir>/cqlite-flight/bin/start.sh   (only __TAG__/__FLIGHT_PORT__/__CASSANDRA_DATA_DIR__ appear here, for the echo lines)
easy-db-lab cqlite-flight start
```

## uid 10001 and the Cassandra data dir

The published image runs as a fixed non-root user (`uid 10001`, `useradd -r -u
10001 flight` — see `cqlite-flight/Dockerfile`). On the host, Cassandra's data
directory is owned by `cassandra:cassandra` at `uid/gid 999`
(`useradd -m -u 999 cassandra` in `packer/cassandra/install/install_cassandra.sh`,
"to match the cassandra-sidecar container image"). The manifest does **not**
override `runAsUser`/`runAsGroup` (the image's own `USER flight` directive
stands); it adds `supplementalGroups: [<data-gid>]` (default `999`) to the pod
security context so the uid-10001 process can read files owned by gid 999.

This assumes the host directory is at least group-readable
(`rwxr-x---` or looser) for gid 999. `fsGroup` was deliberately **not** used —
Kubernetes' `fsGroup` recursive-chown does not apply to `hostPath` volumes, so it
would silently do nothing here; `supplementalGroups` is the correct mechanism for
a pre-existing hostPath tree. **Open risk**: the exact host directory permissions
were not verified against a live cluster (no cluster was available while
authoring this kit) — if a real run shows permission-denied errors, either loosen
the host directory's group permissions or pass `--data-gid` to match whatever GID
actually owns it.

## Observability

`CQLITE_OTEL_ENABLED=true`, `CQLITE_OTEL_ENDPOINT` (default
`http://localhost:4317`), `CQLITE_OTEL_PROTOCOL=grpc`, and
`CQLITE_OTEL_SERVICE_NAME=cqlite-flight` are set as container env vars — see
`cqlite_core::observability::config` for the full `CQLITE_OTEL_*` contract.
Metrics/traces are **OTLP-push**; no Prometheus scrape config or `metrics:` block
is declared in `kit.yaml` (there is nothing for the OTel DaemonSet to scrape).

The default endpoint relies on this pod running `hostNetwork: true` **on the same
node** as the lab's OTel collector DaemonSet (`otel-collector-config.yaml`, OTLP
gRPC receiver on `0.0.0.0:4317`), so `localhost:4317` reaches it directly — the
same idiom the Cassandra sidecar DaemonSet already uses
(`OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317`). Override `--otel-endpoint`
if that topology ever changes (e.g. a control-node-only collector reachable via
the in-cluster `otel-collector.default.svc.cluster.local:4317` Service).

## Lifecycle

```bash
easy-db-lab cqlite-flight start   # kubectl apply the DaemonSet, wait for rollout
easy-db-lab cqlite-flight stop    # kubectl delete daemonset -l easydblab.com/kit=cqlite-flight
easy-db-lab cqlite-flight status  # built-in kit status command
```

`bin/start.sh` / `bin/stop.sh` are plain scripts (no typed `start:`/`stop:` steps
in `kit.yaml`), following the `sysbench`/`trino` bin-script pattern — this also
keeps them working identically regardless of which install path was used.
`stop.sh` deletes strictly by label selector (`easydblab.com/kit=cqlite-flight`),
never by resource name, matching the issue's requirement.

## Open risks for a real lab run

- **Host directory permissions** for the Cassandra data dir were not verified
  live (see [uid 10001](#uid-10001-and-the-cassandra-data-dir) above).
- **Trino reachability** assumes the trino-connector resolves each replica's
  `cqlite-flight` endpoint as `<db-node-private-ip>:<flight-port>` (consistent
  with the connector's stated Sidecar-based topology discovery). If the
  connector instead expects a stable DNS name, swap `hostNetwork` for a headless
  `Service` — not done here since it doesn't match the "replica's endpoint"
  model described in `cqlite-flight/README.md`.
- **No `kubectl apply --dry-run=client` against a live lab cluster** was
  possible while authoring this kit (no cluster available); it was validated
  against a throwaway local k3s control plane instead (see PR/validation notes).
  Re-run the dry-run against the real lab cluster before first use.
- **GHCR image pull**: the kit assumes the `ghcr.io/pmcfadin/cqlite-flight`
  package is public (no `imagePullSecrets` wired) per the image's own README.
