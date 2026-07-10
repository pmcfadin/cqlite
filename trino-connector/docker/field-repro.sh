#!/usr/bin/env bash
# Field-shaped local reproduction harness (issue #2289). SEPARATE from
# `e2e-test.sh` (untouched, still the fast 32-assert smoke check) — this script
# is slower and exists to close the gap between "e2e-test.sh is green" and
# "the field hit a real bug e2e-test.sh could never see": it loads field-scale
# data (>=100k partitions), snapshots via the real Sidecar API, and runs two
# PINNED checks for #2264 (LIMIT-5 mid-stream cancel / do_get channel
# saturation) and #2193 (tiny nb-1-big LZ4 decode through arrow-java).
#
# Usage:
#   trino-connector/docker/field-repro.sh
#   FLIGHT_PLATFORM=linux/amd64 trino-connector/docker/field-repro.sh
#   trino-connector/docker/field-repro.sh --inject-failure=tiny   # see below
#
# Exit code 0 = both pinned checks passed (or, for --inject-failure, the
# capture-on-fail mechanism fired as expected).
#
# ── Tiering (read before escalating off a laptop) ───────────────────────────
# 1. Docker (THIS script) — fast local discovery/iteration. Single node, single
#    process per role. Good for: does the bug reproduce at all, does a fix
#    change the observed behavior, decode/framing correctness.
# 2. A single x86_64 EC2 instance running this SAME compose stack — reach for
#    this ONLY if step 1 leaves an arch-sensitivity question open (e.g. a
#    decode bug that might be x86_64-vs-arm64-dependent, or FLIGHT_PLATFORM
#    emulation timing looks suspicious). Still single-node.
# 3. The 3-node kit (`easy-db-lab-kits/`, epic #2103) — the ONLY tier that can
#    adjudicate multi-node semantics: per-host snapshot placement (#2227),
#    replica failover (#2241), or any claim that depends on more than one
#    Cassandra node. A single-node Docker or EC2 run CANNOT adjudicate those —
#    say so explicitly rather than over-claiming a single-node result.
#
# ── Honest instrumentation note (read before trusting the #2264 evidence) ──
# This compose stack wires NO OTel collector, so `cqlite-flight`'s real
# `cqlite.rpc.in_flight` gauge and `obs::in_flight_level()` accessor are not
# reachable from outside the process here. The #2264 check below uses the
# best LOCAL proxy signal instead: (a) whether the query returns within the
# timeout bound at all, and (b) the crate's debug-level `tracing` lines
# (`do_get phase completed`, `token-range SSTable prune`) captured via
# `RUST_LOG=info,cqlite_flight=debug` (see docker-compose.field-repro-override.yml).
# This is a documented proxy, not the real metric — do not read it as
# equivalent to a live Prometheus/OTel assertion.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DOCKER_DIR="$ROOT/trino-connector/docker"
COMPOSE=(docker compose -f "$DOCKER_DIR/docker-compose.yml" -f "$DOCKER_DIR/docker-compose.field-repro-override.yml")
FAILURES=0

# `FLIGHT_PLATFORM` toggle (deliverable 4): default to whatever the Docker
# daemon's own OS/Arch is (a documented no-op override), so
# `docker-compose.field-repro-override.yml`'s `platform: ${FLIGHT_PLATFORM}`
# always has a value BEFORE any `${COMPOSE[@]}` call (even `down`, which still
# parses the whole compose file). `FLIGHT_PLATFORM=linux/amd64` on a
# non-amd64 host cross-builds cqlite-flight under QEMU emulation — a
# decode/framing check only; NOT a throughput/backpressure-timing claim
# (documented at file top).
if [[ -z "${FLIGHT_PLATFORM:-}" ]]; then
  native_os_arch="$(docker version -f '{{.Server.Os}}/{{.Server.Arch}}' 2>/dev/null || echo linux/amd64)"
  export FLIGHT_PLATFORM="$native_os_arch"
fi
echo "── FLIGHT_PLATFORM=$FLIGHT_PLATFORM (native unless overridden) ──"

# `--inject-failure=<check>` deliberately makes ONE check fail (a query against
# a table that does not exist) so the capture-on-fail path can be proven to
# actually fire end-to-end, without depending on the real #2264/#2193 bugs
# reproducing on this run (the field-vs-local shape gaps this issue documents
# mean neither bug is guaranteed to reproduce locally every run). Not part of
# a normal run.
INJECT_FAILURE=""
for arg in "$@"; do
  case "$arg" in
    --inject-failure=*) INJECT_FAILURE="${arg#--inject-failure=}" ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

# ── timeout discipline (issue #2233, same pattern as e2e-test.sh) ───────────
QUERY_TIMEOUT_SECS=180
TIMEOUT_BIN="$(command -v timeout || command -v gtimeout || true)"
run_with_timeout() {
  local secs="$1"; shift
  if [[ -n "$TIMEOUT_BIN" ]]; then
    "$TIMEOUT_BIN" "$secs" "$@"
    return $?
  fi
  "$@" &
  local pid=$! waited=0
  while kill -0 "$pid" 2>/dev/null; do
    if (( waited >= secs )); then
      kill -9 "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      return 124
    fi
    sleep 1
    waited=$((waited + 1))
  done
  wait "$pid"
}

log() { echo "── $* ──"; }

# Runs a query and returns its output; returns 124 on timeout WITHOUT exiting
# the script (unlike e2e-test.sh's `trino()`, which treats a timeout as fatal)
# — a #2264-class hang timing out IS the expected/interesting outcome here,
# not a harness bug, so callers decide what a timeout means.
trino_query() {
  local out rc=0
  set +e
  out="$(run_with_timeout "$QUERY_TIMEOUT_SECS" "${COMPOSE[@]}" exec -T trino trino --execute "$1" 2>&1)"
  rc=$?
  set -e
  echo "$out"
  return "$rc"
}

# ── artifact capture-on-fail (issue #2289 deliverable 3) ────────────────────
ARTIFACTS_ROOT="$DOCKER_DIR/field-repro-artifacts"
# Flight's :8815 traffic is captured by a throwaway tcpdump CONTAINER joined to
# Cassandra's network namespace (cqlite-flight runs `network_mode:
# service:cassandra`, so it listens on 0.0.0.0:8815 IN that namespace). This is
# deliberately container-based rather than a host-side `tcpdump -i br-<id>`
# (issue #2289 arm64/macOS run, 2026-07-09): on Docker Desktop for Mac the
# compose bridge lives inside the LinuxKit VM, so NO `br-*` interface exists on
# the host and a host tcpdump silently captures nothing. Capturing from inside
# the shared netns (exactly the issue's "tcpdump ... inside/alongside the
# flight container" wording) works identically on Linux and macOS and needs no
# `sudo`. The image is overridable for air-gapped hosts; if it cannot be run
# the run continues WITHOUT a pcap (never aborts).
TCPDUMP_IMAGE="${FIELD_REPRO_TCPDUMP_IMAGE:-nicolaka/netshoot:latest}"
CASSANDRA_CID=""
resolve_cassandra_cid() {
  CASSANDRA_CID="$("${COMPOSE[@]}" ps -q cassandra 2>/dev/null | head -1)" || true
}

# Starts a tcpdump capture of Flight traffic (port 8815) in a container sharing
# Cassandra's network namespace, writing to /cap.pcap inside that container.
# Prints the capture container's name (the handle stop_tcpdump/capture use) on
# stdout, or nothing if it could not be started — a capture failure must never
# abort the run.
start_tcpdump() {
  local name="$1"
  [[ -z "$CASSANDRA_CID" ]] && return 0
  local cname="field-repro-tcpdump-${name}"
  docker rm -f "$cname" >/dev/null 2>&1 || true
  if ! docker run -d --name "$cname" --network "container:$CASSANDRA_CID" \
      "$TCPDUMP_IMAGE" tcpdump -i any -U -w /cap.pcap port 8815 \
      >/tmp/field-repro-tcpdump.log 2>&1; then
    docker rm -f "$cname" >/dev/null 2>&1 || true
    return 0
  fi
  sleep 0.5
  if [[ "$(docker inspect -f '{{.State.Running}}' "$cname" 2>/dev/null)" == "true" ]]; then
    echo "$cname"
  else
    docker rm -f "$cname" >/dev/null 2>&1 || true
  fi
}

# Stops the capture container, copies /cap.pcap out to the host path $2 (if a
# handle was given), and removes the container. No sudo — the copied file is
# owned by the invoking user.
stop_tcpdump() {
  local cname="$1" pcap="${2:-}"
  [[ -z "$cname" ]] && return 0
  docker kill -s INT "$cname" >/dev/null 2>&1 || true
  sleep 0.5
  if [[ -n "$pcap" ]]; then
    docker cp "$cname:/cap.pcap" "$pcap" >/dev/null 2>&1 || true
  fi
  docker rm -f "$cname" >/dev/null 2>&1 || true
}

# Saves diagnostics for one failing check into a timestamped artifacts dir:
# cqlite-flight debug logs, the port-8815 pcap (if captured), and the most
# recent Trino query's JSON record (issue #2289 deliverable 3 / #2286 parity).
capture_artifacts() {
  local check_name="$1" pcap="$2"
  local artdir="$ARTIFACTS_ROOT/$(date -u +%Y%m%dT%H%M%SZ)-${check_name}"
  mkdir -p "$artdir"
  "${COMPOSE[@]}" logs cqlite-flight --no-color > "$artdir/cqlite-flight-debug.log" 2>&1 || true
  if [[ -n "$pcap" && -f "$pcap" ]]; then
    # The pcap was copied out of the capture container by `docker cp` and is
    # already owned by us — no `sudo chown` needed (issue #2289 arm64/macOS).
    cp "$pcap" "$artdir/flight-8815.pcap" 2>/dev/null || true
  fi
  "${COMPOSE[@]}" exec -T trino trino --output-format JSON \
    --execute "SELECT query_id, state, query FROM system.runtime.queries ORDER BY created DESC LIMIT 5" \
    > "$artdir/trino-recent-queries.json" 2>&1 || true
  echo "captured failure artifacts: $artdir" >&2
  ls -la "$artdir" >&2
}

# Wraps a check function with tcpdump-during + capture-on-fail-after. `$1` is
# a label used for both the pcap filename and the artifacts subdir.
run_check() {
  local name="$1"; shift
  local pcap="/tmp/field-repro-${name}.pcap"
  rm -f "$pcap" 2>/dev/null || true
  local tcpdump_handle=""
  tcpdump_handle="$(start_tcpdump "$name")"
  set +e
  "$@"
  local rc=$?
  set -e
  stop_tcpdump "$tcpdump_handle" "$pcap"
  if [[ $rc -ne 0 ]]; then
    echo "FAIL: $name"
    FAILURES=$((FAILURES + 1))
    capture_artifacts "$name" "$pcap"
  else
    echo "PASS: $name"
    # The pcap (if any) was `docker cp`'d out and is owned by us — a plain
    # `rm -f` suffices (no privileged deletion / set -e abort footgun).
    rm -f "$pcap" 2>/dev/null || true
  fi
  return 0
}

cleanup() {
  log "tearing down"
  "${COMPOSE[@]}" --profile loadtest down -v --remove-orphans || true
}
trap cleanup EXIT

log "clean slate (reproducible run)"
"${COMPOSE[@]}" --profile loadtest down -v --remove-orphans || true
mkdir -p "$ARTIFACTS_ROOT"

log "build connector plugin (shared with e2e-test.sh's build step)"
PLUGIN_DIR="$ROOT/trino-connector/build/plugin/cqlite_flight"
rm -rf "$PLUGIN_DIR"
(cd "$ROOT/trino-connector" && ./gradlew --no-daemon installPlugin) || true
if ! ls "$PLUGIN_DIR"/*.jar >/dev/null 2>&1; then
  log "host Gradle produced no plugin; building in a JDK 25 container"
  docker run --rm \
    -v "$ROOT/trino-connector":/work -w /work \
    -v cqlite-gradle-cache:/root/.gradle \
    eclipse-temurin:25-jdk \
    ./gradlew --no-daemon --console=plain installPlugin
fi
ls "$PLUGIN_DIR"/*.jar >/dev/null 2>&1 || { echo "plugin build failed: no jar in $PLUGIN_DIR"; exit 1; }

log "bring up stack (builds cqlite-flight image; waits for healthy deps)"
"${COMPOSE[@]}" up -d --build

resolve_cassandra_cid
if [[ -n "$CASSANDRA_CID" ]]; then
  log "resolved cassandra container ($CASSANDRA_CID) — tcpdump capture container will share its netns"
else
  log "WARNING: could not resolve the cassandra container id — capture-on-fail will skip the pcap"
fi

# ── bounded resolve-wait polling (issue #2233 discipline applied to a RETRY
# LOOP, not just a single query — issue #2289 dry run, 2026-07-09: a resolve
# loop built on `trino_query()` (which by design never exits fatally on a
# timeout, so the #2264 check can observe a hang rather than aborting on it)
# inherited that "keep retrying forever" behavior, so a slow/failing attempt
# could legitimately run up to iteration_count * 180s (~108 min) — the exact
# unbounded-wait failure mode #2233 exists to prevent. Every polling loop
# below now has BOTH a short per-attempt timeout AND a hard wall-clock
# deadline on the whole loop, and fails loudly (exit 1) if exceeded — no
# silent infinite retry. The resolve QUERIES themselves were also switched
# from `count(*)` (a full-table scan — actively wrong for a "is this table
# resolved yet" check against a >=100k-partition table) to a `LIMIT 1`, the
# cheapest possible resolve probe. ─────────────────────────────────────────
RESOLVE_ATTEMPT_TIMEOUT_SECS=30
RESOLVE_OVERALL_TIMEOUT_SECS=300
wait_for_resolve() {
  local label="$1" query="$2"
  local start now elapsed out rc
  start="$(date +%s)"
  while true; do
    set +e
    out="$(run_with_timeout "$RESOLVE_ATTEMPT_TIMEOUT_SECS" "${COMPOSE[@]}" exec -T trino trino --execute "$query" 2>&1)"
    rc=$?
    set -e
    if [[ $rc -eq 0 ]]; then
      log "$label resolved"
      return 0
    fi
    now="$(date +%s)"
    elapsed=$((now - start))
    if (( elapsed >= RESOLVE_OVERALL_TIMEOUT_SECS )); then
      echo "FATAL: $label did not resolve within ${RESOLVE_OVERALL_TIMEOUT_SECS}s (last attempt rc=$rc): $out" >&2
      return 1
    fi
    sleep 5
  done
}

log "wait for Trino to accept queries"
wait_for_resolve "Trino" "SELECT 1" || exit 1

# ── load field-shaped data ───────────────────────────────────────────────
source "$DOCKER_DIR/field-repro-load.sh"
load_tiny_table "$ROOT" "${COMPOSE[@]}"
load_big_keyvalue_table "${COMPOSE[@]}"

log "wait for the connector to resolve fieldrepro.tiny via Sidecar"
wait_for_resolve "fieldrepro.tiny" "SELECT * FROM cqlite.fieldrepro.tiny LIMIT 1" || exit 1

log "wait for the connector to resolve loadtest.keyvalue"
wait_for_resolve "loadtest.keyvalue" "SELECT key FROM cqlite.loadtest.keyvalue LIMIT 1" || exit 1

# ── snapshot-completeness + degraded-schema observation (deliverable 1) ────
source "$DOCKER_DIR/field-repro-snapshot.sh"
observe_snapshot_listing "${COMPOSE[@]}"
observe_degraded_schema_warn "${COMPOSE[@]}"

# ── the two pinned repro checks (deliverable 2) ─────────────────────────────
source "$DOCKER_DIR/field-repro-checks.sh"

if [[ -n "$INJECT_FAILURE" ]]; then
  log "--inject-failure=$INJECT_FAILURE: proving capture-on-fail actually fires"
  run_check "inject-failure-$INJECT_FAILURE" check_inject_failure
else
  run_check "2264-limit5-midstream-cancel" check_2264_limit5_midstream_cancel
  run_check "2193-tiny-decode-native" check_2193_tiny_decode
fi

echo
if [[ $FAILURES -eq 0 ]]; then
  echo "✅ FIELD-REPRO PASSED"
else
  echo "❌ FIELD-REPRO: $FAILURES check(s) failed (see field-repro-artifacts/ above)"
  exit 1
fi
