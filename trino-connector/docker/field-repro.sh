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
# Exit codes:
#   0 = both pinned checks passed (or, for --inject-failure, the capture-on-
#       fail mechanism fired as expected AND was proven end-to-end, pcap
#       included).
#   1 = a check failed / capture-on-fail was demonstrably broken.
#   2 = usage error (unknown argument).
#   3 = --inject-failure ONLY: SKIPPED — no tcpdump capture container could
#       be started on this host this run (image pull failure, air-gapped
#       host, arch mismatch), so the pcap half of capture-on-fail was never
#       exercised. capture_artifacts still writes a debug log and a
#       trino-recent-queries.json file on this path, but their CONTENT is
#       deliberately NOT strictly vetted here (issue #2289 roborev finding,
#       job 1600: the strict content checks apply only on the PROVEN path —
#       gating them behind the capture-started check keeps exit 3 reachable
#       even when the Trino metadata query ALSO timed out on a stalled
#       host). This is DISTINCT from both PASS(0) and FAIL(1) — never
#       conflate a host that couldn't prove the mechanism with a host that
#       proved it.
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

# ── timeout discipline (issue #2233, same pattern as e2e-test.sh) — placed
# FIRST, before anything else in this file, so `run_with_timeout` is
# available to EVERY subsequent command including the `FLIGHT_PLATFORM`
# auto-detect immediately below (issue #2289 roborev finding, job 1595: this
# block used to sit further down, so the `docker version` autodetect call ran
# unbounded before `run_with_timeout` even existed yet). ─────────────────────
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

# ── shared timeout tiers (issue #2289 roborev finding, job 1595 CLASS SWEEP:
# every external-network or potentially-blocking command in the five harness
# files must run under SOME bound). Grouped into a small number of reused
# tiers — see this round's commit message / the roborev-response report for
# the full per-call-site inventory — rather than one bespoke constant per call
# site, to keep this file's length manageable. ───────────────────────────────
NETWORK_PULL_TIMEOUT_SECS=300              # docker compose pull; any `docker run` whose image may not be cached yet (tcpdump/curl/JDK-builder images)
STACK_UP_TIMEOUT_SECS=900                  # docker compose up -d --build: may pull Cassandra/Sidecar/Trino AND build cqlite-flight; generous for a cold QEMU (amd64-emulated) build
DOCKER_CTL_TIMEOUT_SECS=30                 # fast local docker/compose control-plane calls against an already-running daemon: ps, inspect, rm, kill, cp, logs
CASSANDRA_CTL_TIMEOUT_SECS=60              # local `exec` calls against the Cassandra container that normally finish quickly (nodetool flush/tablestats, find) but could stall if Cassandra itself is unresponsive
TEARDOWN_TIMEOUT_SECS=120                  # docker compose down
GRADLE_HOST_BUILD_TIMEOUT_SECS=600         # host-side ./gradlew installPlugin (may cold-download Gradle/deps)
GRADLE_CONTAINER_BUILD_TIMEOUT_SECS=900    # containerized JDK 25 fallback build (cold image pull + cold Gradle/deps download + build)
PCAP_READ_TIMEOUT_SECS=60                  # reading back an already-captured pcap (image is guaranteed cached from this same run's earlier capture)
TCPDUMP_EXIT_WAIT_TIMEOUT_SECS=15          # docker wait for the capture container to actually exit after SIGINT, before copying its pcap out (issue #2289 roborev finding, job 1599) — generous for tcpdump to flush+close a short-lived capture, bounded so a container that never exits can't hang teardown
TCPDUMP_START_TIMEOUT_SECS=15              # bounded poll for the capture container to reach Running (or definitively exit/fail) after `docker run -d` (issue #2289 roborev finding, job 1602) — replaces a single fixed 0.5s sleep + one-shot check, which could false-negative a container that just hadn't started yet under host load

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

# `FLIGHT_PLATFORM` toggle (deliverable 4): default to whatever the Docker
# daemon's own OS/Arch is (a documented no-op override), so
# `docker-compose.field-repro-override.yml`'s `platform: ${FLIGHT_PLATFORM}`
# always has a value BEFORE any `${COMPOSE[@]}` call (even `down`, which still
# parses the whole compose file). `FLIGHT_PLATFORM=linux/amd64` on a
# non-amd64 host cross-builds cqlite-flight under QEMU emulation — a
# decode/framing check only; NOT a throughput/backpressure-timing claim
# (documented at file top).
if [[ -z "${FLIGHT_PLATFORM:-}" ]]; then
  native_os_arch="$(run_with_timeout "$DOCKER_CTL_TIMEOUT_SECS" docker version -f '{{.Server.Os}}/{{.Server.Arch}}' 2>/dev/null || echo linux/amd64)"
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
  CASSANDRA_CID="$(run_with_timeout "$DOCKER_CTL_TIMEOUT_SECS" "${COMPOSE[@]}" ps -q cassandra 2>/dev/null | head -1)" || true
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
  run_with_timeout "$DOCKER_CTL_TIMEOUT_SECS" docker rm -f "$cname" >/dev/null 2>&1 || true
  # `docker run` implicitly pulls `$TCPDUMP_IMAGE` if not already cached — a
  # real network operation, bounded at the NETWORK_PULL tier (issue #2289
  # roborev finding, job 1595 class sweep).
  if ! run_with_timeout "$NETWORK_PULL_TIMEOUT_SECS" docker run -d --name "$cname" --network "container:$CASSANDRA_CID" \
      "$TCPDUMP_IMAGE" tcpdump -i any -U -w /cap.pcap port 8815 \
      >/tmp/field-repro-tcpdump.log 2>&1; then
    run_with_timeout "$DOCKER_CTL_TIMEOUT_SECS" docker rm -f "$cname" >/dev/null 2>&1 || true
    return 0
  fi
  # Bounded poll, NOT a single fixed 0.5s sleep + one-shot check (issue
  # #2289 roborev finding, job 1602): a single check right after `docker run
  # -d` returns could race a slow container start (image extraction, cgroup
  # setup under host load) and wrongly classify a container that just hadn't
  # reached Running YET as unavailable — a needless SKIP on a host that would
  # have worked fine with a moment more patience. Poll `.State.Status` every
  # 0.5s up to TCPDUMP_START_TIMEOUT_SECS: `running` -> confirmed success
  # (return immediately, don't wait out the rest of the bound); `exited`/
  # `dead`/inspect-failed(container gone) -> DEFINITIVELY not coming up,
  # break immediately rather than polling a dead container until timeout;
  # anything else (`created`, `restarting`, ...) -> still starting, keep
  # polling. Guarded per the round-7/8 (job 1598/1599) `|| var=$?`-free
  # pattern — `run_with_timeout`'s own return code is irrelevant here (we key
  # off the printed status text, defaulting to empty on any failure), so a
  # bare `|| state=""` suffices without needing a separate rc capture.
  local max_iterations=$((TCPDUMP_START_TIMEOUT_SECS * 2)) i=0 state=""
  while (( i < max_iterations )); do
    state="$(run_with_timeout "$DOCKER_CTL_TIMEOUT_SECS" docker inspect -f '{{.State.Status}}' "$cname" 2>/dev/null)" || state=""
    case "$state" in
      running)
        echo "$cname"
        return 0
        ;;
      exited | dead | "")
        break
        ;;
      *)
        sleep 0.5
        i=$((i + 1))
        ;;
    esac
  done
  run_with_timeout "$DOCKER_CTL_TIMEOUT_SECS" docker rm -f "$cname" >/dev/null 2>&1 || true
}

# Stops the capture container, copies /cap.pcap out to the host path $2 (if a
# handle was given), and removes the container. No sudo — the copied file is
# owned by the invoking user.
#
# Waits for CONFIRMED container exit (`docker wait`) before copying, rather
# than a fixed sleep (issue #2289 roborev finding, job 1599): a fixed 0.5s
# sleep after SIGINT is not a completion signal — on a slow/loaded host
# tcpdump may not have flushed + closed /cap.pcap yet, so the copied-out pcap
# could be truncated/empty, producing flaky capture-on-fail proof and false
# negatives in --inject-failure. `docker wait` blocks until the container has
# actually exited (tcpdump received SIGINT, flushed its write buffer, and
# terminated), bounded at TCPDUMP_EXIT_WAIT_TIMEOUT_SECS so a container that
# never exits can't hang teardown. Guarded per the round-7 (job 1598)
# set +e-free `|| rc=$?` pattern — this script runs under `set -euo
# pipefail`, and a bare failing substitution here would abort the whole
# script raw instead of reaching a deterministic message.
#
# On a timed-out/failed wait, the copy is SKIPPED entirely (never "copy a
# maybe-truncated pcap and call it proof") — the pcap then simply does not
# exist at the destination path, which the EXISTING downstream
# "flight-8815.pcap missing" check (in the --inject-failure verdict and
# capture_artifacts's normal-failure path) already turns into a correctly
# FAILED/BROKEN verdict, so no separate hard-failure plumbing is needed here.
stop_tcpdump() {
  local cname="$1" pcap="${2:-}"
  [[ -z "$cname" ]] && return 0
  run_with_timeout "$DOCKER_CTL_TIMEOUT_SECS" docker kill -s INT "$cname" >/dev/null 2>&1 || true
  local wait_rc=0
  local exit_code
  exit_code="$(run_with_timeout "$TCPDUMP_EXIT_WAIT_TIMEOUT_SECS" docker wait "$cname" 2>/dev/null)" || wait_rc=$?
  if [[ $wait_rc -ne 0 || -z "$exit_code" ]]; then
    echo "WARNING: stop_tcpdump: '$cname' did not confirm exit within ${TCPDUMP_EXIT_WAIT_TIMEOUT_SECS}s after SIGINT (rc=$wait_rc) — the pcap may be truncated; SKIPPING the copy rather than presenting possibly-incomplete data as proof" >&2
  elif [[ -n "$pcap" ]]; then
    run_with_timeout "$DOCKER_CTL_TIMEOUT_SECS" docker cp "$cname:/cap.pcap" "$pcap" >/dev/null 2>&1 || true
  fi
  run_with_timeout "$DOCKER_CTL_TIMEOUT_SECS" docker rm -f "$cname" >/dev/null 2>&1 || true
}

# Bound for the post-failure Trino metadata query below — deliberately SHORT
# (well under QUERY_TIMEOUT_SECS): when the FAILING check IS a hang/server
# stall (the exact #2264 scenario this harness exists to catch), an unbounded
# diagnostics query would itself hang, so "capture diagnostics on failure"
# would silently become "hang a second time instead of capturing anything"
# (issue #2289 roborev finding, job 1590).
CAPTURE_METADATA_TIMEOUT_SECS=20

# Set by `capture_artifacts` to the EXACT artifacts dir it just created — the
# `--inject-failure` self-test verifies THIS path, not a glob of the newest
# `*-inject-failure-*` dir (issue #2289 roborev finding, job 1590: a glob can
# be satisfied by a stale dir left over from a PRIOR run even if the current
# run produced nothing new).
LAST_ARTIFACT_DIR=""

# Saves diagnostics for one failing check into a timestamped artifacts dir:
# cqlite-flight debug logs, the port-8815 pcap (if captured), and the most
# recent Trino query's JSON record (issue #2289 deliverable 3 / #2286 parity).
capture_artifacts() {
  local check_name="$1" pcap="$2"
  local artdir="$ARTIFACTS_ROOT/$(date -u +%Y%m%dT%H%M%SZ)-${check_name}"
  mkdir -p "$artdir"
  LAST_ARTIFACT_DIR="$artdir"
  run_with_timeout "$DOCKER_CTL_TIMEOUT_SECS" "${COMPOSE[@]}" logs cqlite-flight --no-color > "$artdir/cqlite-flight-debug.log" 2>&1 || true
  if [[ -n "$pcap" && -f "$pcap" ]]; then
    # The pcap was copied out of the capture container by `docker cp` and is
    # already owned by us — no `sudo chown` needed (issue #2289 arm64/macOS).
    cp "$pcap" "$artdir/flight-8815.pcap" 2>/dev/null || true
  fi
  # Bounded + never fatal: on timeout/error, write a short fallback note
  # instead of leaving an empty/truncated JSON file, and keep going — the
  # debug log + pcap are still real diagnostics even if Trino itself can't
  # answer right now.
  #
  # `ORDER BY created DESC` restored (issue #2289 roborev finding, job 1602,
  # overriding job 1598's no-sort choice): `created` is EMPIRICALLY VERIFIED
  # correct for the PINNED image (`trinodb/trino:481`, this compose stack's
  # exact version — `docker run -d trinodb/trino:481` + `DESCRIBE
  # system.runtime.queries` + this exact query run standalone, 2026-07-10:
  # the column IS `created`, `timestamp(3) with time zone`; `create_time`
  # does not exist, "Column 'create_time' cannot be resolved"). Deterministic
  # newest-first ordering is worth the (already-disproven) fragility concern
  # for THIS pinned image — do not re-litigate either direction without new
  # evidence against `trinodb/trino:481` specifically.
  if run_with_timeout "$CAPTURE_METADATA_TIMEOUT_SECS" "${COMPOSE[@]}" exec -T trino trino --output-format JSON \
      --execute "SELECT query_id, state, query FROM system.runtime.queries ORDER BY created DESC LIMIT 5" \
      > "$artdir/trino-recent-queries.json" 2>&1; then
    :
  else
    local trino_meta_rc=$?
    echo "trino-recent-queries.json unavailable (rc=${trino_meta_rc}, bound ${CAPTURE_METADATA_TIMEOUT_SECS}s) — Trino itself may be stalled by the same failure being captured; see cqlite-flight-debug.log instead" \
      > "$artdir/trino-recent-queries.json"
  fi
  echo "captured failure artifacts: $artdir" >&2
  ls -la "$artdir" >&2
}

# Wraps a check function with tcpdump-during + capture-on-fail-after. `$1` is
# a label used for both the pcap filename and the artifacts subdir.
#
# `LAST_CAPTURE_STARTED` (global, issue #2289 roborev finding, 2026-07-10):
# whether THIS invocation's capture container actually started, not merely
# whether a Cassandra container id was resolvable. `CASSANDRA_CID` being
# non-empty only means a capture COULD be attempted — the capture container
# itself can still fail to start (image pull failure on an air-gapped host,
# arch mismatch, docker run error), and the old check-on-`CASSANDRA_CID`
# would then wrongly demand a pcap that was never possible. The final
# `--inject-failure` verdict reads this flag instead.
LAST_CAPTURE_STARTED=0
run_check() {
  local name="$1"; shift
  local pcap="/tmp/field-repro-${name}.pcap"
  rm -f "$pcap" 2>/dev/null || true
  local tcpdump_handle=""
  tcpdump_handle="$(start_tcpdump "$name")"
  if [[ -n "$tcpdump_handle" ]]; then
    LAST_CAPTURE_STARTED=1
  else
    LAST_CAPTURE_STARTED=0
  fi
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
  run_with_timeout "$TEARDOWN_TIMEOUT_SECS" "${COMPOSE[@]}" --profile loadtest down -v --remove-orphans || true
}
trap cleanup EXIT

log "clean slate (reproducible run)"
run_with_timeout "$TEARDOWN_TIMEOUT_SECS" "${COMPOSE[@]}" --profile loadtest down -v --remove-orphans || true
mkdir -p "$ARTIFACTS_ROOT"

# Belt-and-suspenders alongside `LAST_ARTIFACT_DIR` (issue #2289 roborev
# finding, job 1590): clear any stale `*-inject-failure-*` dirs from a PRIOR
# `--inject-failure` run before this one starts, so nothing left over on disk
# could ever be mistaken for THIS run's evidence even by future code that
# re-introduces a glob.
if [[ -n "$INJECT_FAILURE" ]]; then
  rm -rf "$ARTIFACTS_ROOT"/*-inject-failure-* 2>/dev/null || true
fi

log "build connector plugin (shared with e2e-test.sh's build step)"
PLUGIN_DIR="$ROOT/trino-connector/build/plugin/cqlite_flight"
rm -rf "$PLUGIN_DIR"
# Host-side Gradle may cold-download Gradle itself + dependencies over the
# network on a fresh checkout — bounded at the GRADLE_HOST_BUILD tier (issue
# #2289 roborev finding, job 1595 class sweep). Run via `bash -c` so the `cd`
# stays scoped to this one invocation without needing a real subshell (which
# `run_with_timeout`'s positional-arg exec cannot wrap directly).
run_with_timeout "$GRADLE_HOST_BUILD_TIMEOUT_SECS" bash -c "cd \"$ROOT/trino-connector\" && ./gradlew --no-daemon installPlugin" || true
if ! ls "$PLUGIN_DIR"/*.jar >/dev/null 2>&1; then
  log "host Gradle produced no plugin; building in a JDK 25 container"
  # Cold image pull + cold Gradle/deps download + build — the most
  # network-exposed step in the whole harness, bounded generously.
  run_with_timeout "$GRADLE_CONTAINER_BUILD_TIMEOUT_SECS" docker run --rm \
    -v "$ROOT/trino-connector":/work -w /work \
    -v cqlite-gradle-cache:/root/.gradle \
    eclipse-temurin:25-jdk \
    ./gradlew --no-daemon --console=plain installPlugin
fi
ls "$PLUGIN_DIR"/*.jar >/dev/null 2>&1 || { echo "plugin build failed: no jar in $PLUGIN_DIR"; exit 1; }

log "bring up stack (builds cqlite-flight image; waits for healthy deps)"
# May pull Cassandra/Sidecar/Trino images AND build cqlite-flight (a genuine
# Rust compile, slower still under FLIGHT_PLATFORM=linux/amd64 QEMU emulation)
# — bounded generously at the STACK_UP tier rather than left unbounded.
run_with_timeout "$STACK_UP_TIMEOUT_SECS" "${COMPOSE[@]}" up -d --build

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

log "wait for the connector to resolve cassandra_easy_stress.keyvalue via Sidecar"
wait_for_resolve "cassandra_easy_stress.keyvalue" "SELECT * FROM cqlite.cassandra_easy_stress.keyvalue LIMIT 1" || exit 1

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
  # Label carries the flight platform so the #2193 arch-sensitivity data point
  # (native arm64 vs FLIGHT_PLATFORM=linux/amd64) is unambiguous in any
  # capture-on-fail artifacts dir — not always "native" (issue #2289 amd64 run,
  # 2026-07-10: the old hardcoded "-native" suffix mislabeled the emulated run).
  run_check "2193-tiny-decode-${FLIGHT_PLATFORM//\//-}" check_2193_tiny_decode
fi

echo
# `--inject-failure` is a SELF-TEST of the capture-on-fail mechanism, so its
# verdict is inverted from a normal run: the forced check failure is EXPECTED,
# and success means the capture path produced a usable artifacts dir (debug
# logs + a Flight pcap). This makes the documented "exit 0 = capture fired as
# expected" contract at the top of this file actually hold (issue #2289 arm64
# run, 2026-07-10: the old path exited 1 on --inject-failure, contradicting
# that contract and making the self-test's own exit code meaningless).
if [[ -n "$INJECT_FAILURE" ]]; then
  # Verify the EXACT dir `capture_artifacts` created for THIS invocation
  # (`LAST_ARTIFACT_DIR`, set inside `capture_artifacts`), not a glob of the
  # newest `*-inject-failure-*` dir — a glob can be satisfied by a stale dir
  # left over from a prior run even when this run produced nothing new
  # (issue #2289 roborev finding, job 1590). `check_inject_failure` always
  # returns 1 by design, so `run_check` always calls `capture_artifacts` and
  # `LAST_ARTIFACT_DIR` is always set for a genuinely-completed self-test run.
  latest="$LAST_ARTIFACT_DIR"
  if [[ -z "$latest" || ! -d "$latest" ]]; then
    echo "❌ CAPTURE-ON-FAIL BROKEN: no artifacts dir was recorded for this run (LAST_ARTIFACT_DIR unset — capture_artifacts did not run as expected)"
    exit 1
  fi
  missing=()
  [[ -f "$latest/cqlite-flight-debug.log" ]] || missing+=("cqlite-flight-debug.log")
  [[ -f "$latest/trino-recent-queries.json" ]] || missing+=("trino-recent-queries.json")
  if [[ ${#missing[@]} -ne 0 ]]; then
    echo "❌ CAPTURE-ON-FAIL BROKEN: $latest is missing: ${missing[*]}"
    exit 1
  fi
  trino_json="$latest/trino-recent-queries.json"
  # Issue #2289 roborev finding (job 1600): the STRICT trino_json content
  # validation (placeholder/empty/JSON-Lines-parse, added job 1597) used to
  # run BEFORE this capture-started gate. When tcpdump never started AND the
  # bounded post-failure Trino metadata query ALSO timed out (a genuinely
  # stalled/degraded host — exactly the scenario the SKIP(3) exit code exists
  # to describe honestly), trino_json would be the TIMEOUT/ERROR PLACEHOLDER,
  # so the strict check fired FIRST and hard-FAILed (exit 1) — the
  # documented exit 3 SKIP path was UNREACHABLE on a stalled host, silently
  # breaking the exit-code contract in the file header. The strict
  # content-of-proof checks apply ONLY on the PROVEN path (capture started);
  # gate them behind `LAST_CAPTURE_STARTED` so the no-capture path still
  # reaches exit 3 regardless of whether the JSON half also failed.
  #
  # Issue #2289 roborev finding (job 1595): when THIS check's capture
  # container never started (`LAST_CAPTURE_STARTED=0` — image pull failure,
  # air-gapped host, arch mismatch), the pcap requirement below was
  # correctly SKIPPED (job 1592's fix), but execution then fell straight
  # through to the "✅ CAPTURE-ON-FAIL PROVEN" banner regardless — an
  # air-gapped/first-run host would "pass" this self-test without the pcap
  # half of capture-on-fail ever being exercised. A capture-less run is a
  # DISTINCT, non-PROVEN outcome: print an explicit SKIP verdict and exit
  # with a DEDICATED non-zero code (3 — never 0/PASS, never 1/FAIL, so a
  # caller can tell "the mechanism could not be exercised on this host" apart
  # from both "it works" and "it's broken").
  if [[ "$LAST_CAPTURE_STARTED" -ne 1 ]]; then
    echo "⚠️  CAPTURE-ON-FAIL SKIPPED: no tcpdump capture container could be started on this host this run (see the capture-start warning earlier in this run's output) — cqlite-flight-debug.log and a trino-recent-queries.json file were both written ($latest), but neither the pcap half NOR the strict trino-recent-queries.json content check were exercised on this run (the latter is intentionally skipped here too — full artifact proof only applies on the PROVEN path), so this is not proof the capture path works end-to-end. Re-run with a working \$TCPDUMP_IMAGE / network access to actually prove it." >&2
    exit 3
  fi
  # Issue #2289 roborev finding (job 1597): PROVEN-path-only from here on —
  # this verdict verified the debug log + pcap but NEVER checked
  # `trino-recent-queries.json` — `capture_artifacts` can silently fall back
  # to a TIMEOUT/ERROR PLACEHOLDER note (job 1590's never-fatal fallback,
  # written when the bounded post-failure Trino metadata query itself times
  # out or errors) and this self-test would still exit 0 claiming full proof
  # despite that one artifact never having been genuinely captured. Every
  # artifact this self-test advertises as proof (the 'debug-log + pcap +
  # trino-recent-queries.json' line below) must be POSITIVELY verified as
  # real content, not merely present-on-disk.
  if grep -qF "trino-recent-queries.json unavailable" "$trino_json"; then
    echo "❌ CAPTURE-ON-FAIL BROKEN: $trino_json is the TIMEOUT/ERROR FALLBACK PLACEHOLDER (Trino did not answer within \${CAPTURE_METADATA_TIMEOUT_SECS}s this run) — not a real capture. This self-test requires genuine Trino query-metadata evidence, never a placeholder note."
    exit 1
  fi
  if [[ ! -s "$trino_json" ]]; then
    echo "❌ CAPTURE-ON-FAIL BROKEN: $trino_json is empty"
    exit 1
  fi
  # Trino's `--output-format JSON` emits JSON LINES (one object per row), not
  # a single top-level document — validate every non-empty line parses.
  # python3 gives a real parse; without it, fall back to a structural
  # heuristic rather than silently skipping the check on hosts without
  # python3 (still a POSITIVE check, just a weaker one).
  if command -v python3 >/dev/null 2>&1; then
    if ! python3 -c '
import json, sys
with open(sys.argv[1]) as f:
    lines = [l for l in f if l.strip()]
if not lines:
    sys.exit(1)
for line in lines:
    json.loads(line)
' "$trino_json" 2>/dev/null; then
      echo "❌ CAPTURE-ON-FAIL BROKEN: $trino_json is not valid JSON Lines (truncated/garbage — not a genuine Trino query-metadata capture)"
      exit 1
    fi
  elif ! awk 'NF{ if ($0 !~ /^\{.*\}$/) { exit 1 } } END { exit 0 }' "$trino_json"; then
    echo "❌ CAPTURE-ON-FAIL BROKEN: $trino_json does not look like well-formed JSON Lines (no python3 available on this host for a stricter parse check)"
    exit 1
  fi
  [[ -f "$latest/flight-8815.pcap" ]] || missing+=("flight-8815.pcap")
  if [[ ${#missing[@]} -ne 0 ]]; then
    echo "❌ CAPTURE-ON-FAIL BROKEN: $latest is missing: ${missing[*]}"
    exit 1
  fi
  # Count packets via the SAME containerized tcpdump image the capture itself
  # used (`$TCPDUMP_IMAGE`), never a host-installed `tcpdump` binary (issue
  # #2289 roborev finding, job 1592: relying on `command -v tcpdump` meant a
  # host WITHOUT the binary silently left `pkts="?"` and the check passed
  # regardless of actual content). 0 packets is a hard self-test FAILURE —
  # the capture path ran but recorded nothing useful, which is exactly the
  # false-pass this self-test exists to prevent. Bounded at PCAP_READ_TIMEOUT
  # (the image is guaranteed cached from this same run's earlier capture, so
  # this bound only needs to cover reading the file back, not a pull).
  #
  # Guarded with explicit set +e/set -e (issue #2289 roborev finding, job
  # 1598): this script runs under `set -euo pipefail`, and `wc -l`/`tr -d ' '`
  # virtually always exit 0 regardless of what came before them in the pipe —
  # but `pipefail` still propagates an UPSTREAM failure (`run_with_timeout`
  # timing out, or `tcpdump` erroring on a truncated/unreadable pcap) as the
  # pipeline's overall exit status. On a bare top-level assignment, that
  # would trigger `errexit` and abort the whole script RAW, before ever
  # reaching the deterministic `pkts == 0` failure message below. Capture the
  # real exit code explicitly instead.
  pkts_rc=0
  pkts="$(run_with_timeout "$PCAP_READ_TIMEOUT_SECS" docker run --rm -v "$latest":/cap:ro "$TCPDUMP_IMAGE" tcpdump -r /cap/flight-8815.pcap 2>/dev/null | wc -l | tr -d ' ')" || pkts_rc=$?
  if [[ "$pkts_rc" -ne 0 ]]; then
    echo "❌ CAPTURE-ON-FAIL BROKEN: reading back $latest/flight-8815.pcap failed (rc=$pkts_rc, bound ${PCAP_READ_TIMEOUT_SECS}s) — the pcap may be truncated, corrupted, or unreadable, which is not evidence the capture path actually recorded Flight traffic"
    exit 1
  fi
  pkts="${pkts:-0}"
  if [[ "$pkts" -eq 0 ]]; then
    echo "❌ CAPTURE-ON-FAIL BROKEN: $latest/flight-8815.pcap has 0 packets even though the capture container started — a zero-packet pcap is not evidence the capture path actually recorded Flight traffic"
    exit 1
  fi
  echo "✅ CAPTURE-ON-FAIL PROVEN: forced failure produced $latest"
  echo "   (cqlite-flight-debug.log + flight-8815.pcap [${pkts} pkts] + trino-recent-queries.json)"
  exit 0
fi

if [[ $FAILURES -eq 0 ]]; then
  echo "✅ FIELD-REPRO PASSED"
else
  echo "❌ FIELD-REPRO: $FAILURES check(s) failed (see field-repro-artifacts/ above)"
  exit 1
fi
