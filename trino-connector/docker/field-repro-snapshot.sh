#!/usr/bin/env bash
# Sidecar snapshot observation for field-repro.sh (issue #2289, deliverable 1).
# Sourced by field-repro.sh; expects `log()`/`ARTIFACTS_ROOT` already defined.
set -euo pipefail

# Issue #2289 roborev finding (job 1601): a FIXED snapshot name was never
# cleared before use — an interrupted prior run (crashed harness, killed
# host, etc.) could leave STALE contents at this exact name, and a run whose
# own PUT then failed (or raced) would still scan and report on that stale
# data as if it were fresh, producing a FALSE observation. Run-unique via a
# PID + wall-clock-seconds suffix (a throwaway diagnostic snapshot name
# inside a shell script — no wall-clock-race objection applies the way it
# would for a pinned test assertion) so no two runs, even overlapping ones,
# can ever collide; using the SAME unique name consistently for create/scan/
# delete below also means the scanned snapshot is INHERENTLY the one this
# run created (nothing else on disk could match). See
# `cleanup_stale_leftover_snapshots` for the complementary best-effort
# disk-hygiene sweep of OLDER runs' abandoned snapshots.
SNAPSHOT_NAME="fieldrepro-manual-check-$$-$(date +%s)"
STALE_SNAPSHOT_PREFIX="fieldrepro-manual-check"

# Best-effort cleanup of any snapshot(s) left behind by a PRIOR interrupted
# run under the shared `fieldrepro-manual-check*` prefix (this run's OWN
# `$SNAPSHOT_NAME` is unique and can never match anything already on disk, so
# this sweep only ever touches leftovers from EARLIER runs). Discovers stale
# tag names via the same on-disk `find` pattern already used for the real
# listing scan below, then clears each via `nodetool clearsnapshot` (more
# direct/reliable than round-tripping through the Sidecar HTTP DELETE route
# for an unknown number of possibly-orphaned tags). Never fatal — a failure
# here is disk hygiene, not correctness.
cleanup_stale_leftover_snapshots() {
  local -a compose=("$@")
  local stale_names
  stale_names="$(run_with_timeout "$CASSANDRA_CTL_TIMEOUT_SECS" "${compose[@]}" exec -T cassandra \
    find /var/lib/cassandra/data/loadtest -maxdepth 3 -type d -name "${STALE_SNAPSHOT_PREFIX}*" 2>/dev/null \
    | xargs -n1 -r basename 2>/dev/null | sort -u)" || true
  if [[ -n "$stale_names" ]]; then
    log "clearing stale ${STALE_SNAPSHOT_PREFIX}* snapshot leftover(s) from a prior interrupted run: $(echo "$stale_names" | tr '\n' ' ')"
    while IFS= read -r name; do
      [[ -n "$name" ]] || continue
      run_with_timeout "$CASSANDRA_CTL_TIMEOUT_SECS" "${compose[@]}" exec -T cassandra nodetool clearsnapshot -t "$name" loadtest >/dev/null 2>&1 || true
    done <<< "$stale_names"
  fi
}

# Creates a Sidecar snapshot of `loadtest.keyvalue` via the EXACT same
# HTTP route the connector's own `SidecarClient` uses in `snapshot` read mode
# (`PUT /api/v1/keyspaces/{ks}/tables/{table}/snapshots/{name}` —
# `trino-connector/src/main/java/in/mcfad/cqlite/flight/sidecar/SidecarClient.java`),
# records the resulting on-disk snapshot directory listing as a harness
# artifact, and DELETEs it afterward (mirroring the connector's per-query
# cleanup). We hit the Sidecar directly here (rather than racing the
# automatic per-query create/delete a real `SELECT` triggers) so the listing
# capture is deterministic instead of depending on catching a transient
# snapshot mid-query.
#
# This is the harness's vehicle for the suspected snapshot-completeness gap
# (#2264 Phase C candidate): the field's Sidecar-created snapshots have been
# observed missing `Index.db`/`Summary.db`/`Filter.db` (Data.db + header
# only). If the LOCAL snapshot here has all components, that divergence is
# surfaced LOUDLY below — it is not silently treated as "passing".
# Issue #2289 roborev finding (job 1595 class sweep): every external-network
# or potentially-blocking command in this file now runs under
# `run_with_timeout`, reusing field-repro.sh's shared tiers
# (NETWORK_PULL_TIMEOUT_SECS for the curl-image `docker run`s that hit the
# Sidecar HTTP API, CASSANDRA_CTL_TIMEOUT_SECS / DOCKER_CTL_TIMEOUT_SECS for
# local exec/control-plane calls) — those globals are defined in
# field-repro.sh before this file is sourced.
observe_snapshot_listing() {
  local -a compose=("$@")
  log "Sidecar snapshot API: PUT loadtest.keyvalue snapshot '$SNAPSHOT_NAME' (same route as SidecarClient.createSnapshot)"
  local cassandra_cid
  # `|| true` (issue #2289 roborev finding, job 1598 class sweep): mirrors
  # field-repro.sh's `resolve_cassandra_cid()`, which already guards the
  # identical lookup this way. Without it, a genuine compose/daemon error
  # here (not just "cassandra isn't up yet", which the `-z` check below
  # already handles gracefully) would abort the whole script RAW under
  # `set -e`, instead of falling through to the intended graceful
  # "could not resolve ... skipping" WARNING path.
  cassandra_cid="$(run_with_timeout "$DOCKER_CTL_TIMEOUT_SECS" "${compose[@]}" ps -q cassandra 2>/dev/null)" || true
  if [[ -z "$cassandra_cid" ]]; then
    echo "WARNING: could not resolve the cassandra container id — skipping snapshot-listing observation" >&2
    return 0
  fi

  cleanup_stale_leftover_snapshots "${compose[@]}"

  local put_status
  put_status="$(run_with_timeout "$NETWORK_PULL_TIMEOUT_SECS" docker run --rm --network "container:$cassandra_cid" curlimages/curl:latest \
    -s -o /dev/null -w '%{http_code}' -X PUT \
    "http://172.42.0.2:9043/api/v1/keyspaces/loadtest/tables/keyvalue/snapshots/$SNAPSHOT_NAME" || echo "curl-failed")"
  log "Sidecar snapshot PUT -> HTTP $put_status"

  local artdir="$ARTIFACTS_ROOT/snapshot-listing"
  mkdir -p "$artdir"
  local listing_file="$artdir/keyvalue-snapshot-listing.txt"

  # Issue #2289 roborev finding (job 1596): the PUT's HTTP status was logged
  # but never CHECKED — a failed PUT (Sidecar down, auth, 5xx, curl itself
  # erroring/timing out) still fell through to scanning the filesystem and
  # could report a "missing components" divergence finding for a snapshot
  # that was NEVER CREATED this run (or stale leftovers from a PRIOR run),
  # poisoning the #2295 evidence with a fabricated result. Require an
  # unambiguous 2xx before treating the listing as meaningful; on anything
  # else, record an explicit SKIPPED/FAILED artifact and return WITHOUT
  # scanning the filesystem or emitting any divergence finding.
  if [[ ! "$put_status" =~ ^2[0-9][0-9]$ ]]; then
    echo "SIDECAR SNAPSHOT PUT FAILED (HTTP $put_status, expected 2xx) — SKIPPING the snapshot-listing observation entirely: with no confirmed-created snapshot, scanning the filesystem now could only surface stale data from a PRIOR run or nothing at all, either of which would be a FABRICATED divergence finding, not real evidence." >&2
    echo "SKIPPED: Sidecar snapshot PUT returned HTTP $put_status (expected 2xx) — no listing scanned, no divergence finding emitted this run" > "$listing_file"
    # Best-effort cleanup even on a failed/ambiguous PUT — idempotent no-op
    # if nothing was actually created.
    run_with_timeout "$NETWORK_PULL_TIMEOUT_SECS" docker run --rm --network "container:$cassandra_cid" curlimages/curl:latest \
      -s -o /dev/null -X DELETE \
      "http://172.42.0.2:9043/api/v1/keyspaces/loadtest/tables/keyvalue/snapshots/$SNAPSHOT_NAME" || true
    return 0
  fi

  run_with_timeout "$CASSANDRA_CTL_TIMEOUT_SECS" "${compose[@]}" exec -T cassandra \
    find /var/lib/cassandra/data/loadtest -path "*/snapshots/$SNAPSHOT_NAME/*" -type f \
    > "$listing_file" 2>&1 || true
  log "snapshot dir listing recorded: $listing_file"
  cat "$listing_file" >&2

  local missing=()
  for component in Index.db Summary.db Filter.db; do
    if ! grep -q "$component" "$listing_file"; then
      missing+=("$component")
    fi
  done
  if [[ ${#missing[@]} -eq 0 ]]; then
    echo "FINDING: local Sidecar snapshot includes ALL of Index.db/Summary.db/Filter.db." >&2
    echo "         The field's snapshots have been observed WITHOUT these (Data.db + header only)." >&2
    echo "         This is a DIVERGENCE from the field, not a pass — see #2264's Phase C" >&2
    echo "         (snapshot-completeness) candidate finding. Do not treat this as evidence" >&2
    echo "         the field defect doesn't exist; it means this local harness cannot" >&2
    echo "         reproduce the missing-component shape without deliberately recreating it." >&2
  else
    echo "FINDING: local Sidecar snapshot is ALSO missing: ${missing[*]} — this MATCHES the" >&2
    echo "         field observation. Snapshot-completeness may be reproducible locally;" >&2
    echo "         file/link a dedicated issue per #2289's Phase-C guidance." >&2
  fi

  run_with_timeout "$NETWORK_PULL_TIMEOUT_SECS" docker run --rm --network "container:$cassandra_cid" curlimages/curl:latest \
    -s -o /dev/null -X DELETE \
    "http://172.42.0.2:9043/api/v1/keyspaces/loadtest/tables/keyvalue/snapshots/$SNAPSHOT_NAME" || true
}

# Greps cqlite-flight's own debug/warn logs for the exact WARN string
# (`cqlite-core/src/schema/mod.rs`) emitted when the SSTable header's
# SerializationHeader carries zero partition-key-tagged columns — the
# "degraded header-extracted schema" path noted in the issue. Reports
# honestly either way; a non-reproduction is NOT silently treated as a pass.
observe_degraded_schema_warn() {
  local -a compose=("$@")
  local needle="No partition keys found in SSTable header"
  local artdir="$ARTIFACTS_ROOT/degraded-schema-warn"
  mkdir -p "$artdir"
  local logfile="$artdir/cqlite-flight-logs.txt"
  run_with_timeout "$DOCKER_CTL_TIMEOUT_SECS" "${compose[@]}" logs cqlite-flight --no-color > "$logfile" 2>&1 || true
  if grep -qF "$needle" "$logfile"; then
    echo "REPRODUCES LOCALLY: '$needle' WARN found in cqlite-flight logs ($logfile)." >&2
    grep -F "$needle" "$logfile" >&2
  else
    echo "DOES NOT REPRODUCE LOCALLY: '$needle' WARN not found in cqlite-flight logs ($logfile)." >&2
    echo "This is a documented gap, not a fabricated pass — the field's cassandra-easy-stress" >&2
    echo "table shape may differ from what this harness's loader produces." >&2
  fi
}
