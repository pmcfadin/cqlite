#!/usr/bin/env bash
# Sidecar snapshot observation for field-repro.sh (issue #2289, deliverable 1).
# Sourced by field-repro.sh; expects `log()`/`ARTIFACTS_ROOT` already defined.
set -euo pipefail

SNAPSHOT_NAME="fieldrepro-manual-check"

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
  cassandra_cid="$(run_with_timeout "$DOCKER_CTL_TIMEOUT_SECS" "${compose[@]}" ps -q cassandra)"
  if [[ -z "$cassandra_cid" ]]; then
    echo "WARNING: could not resolve the cassandra container id — skipping snapshot-listing observation" >&2
    return 0
  fi

  local put_status
  put_status="$(run_with_timeout "$NETWORK_PULL_TIMEOUT_SECS" docker run --rm --network "container:$cassandra_cid" curlimages/curl:latest \
    -s -o /dev/null -w '%{http_code}' -X PUT \
    "http://172.42.0.2:9043/api/v1/keyspaces/loadtest/tables/keyvalue/snapshots/$SNAPSHOT_NAME" || echo "curl-failed")"
  log "Sidecar snapshot PUT -> HTTP $put_status"

  local artdir="$ARTIFACTS_ROOT/snapshot-listing"
  mkdir -p "$artdir"
  local listing_file="$artdir/keyvalue-snapshot-listing.txt"
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
