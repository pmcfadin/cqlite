#!/usr/bin/env bash
# Field-shaped data loading for field-repro.sh (issue #2289, deliverable 1).
# Sourced by field-repro.sh; expects `log()` already defined by the caller.
set -euo pipefail

# `cassandra_easy_stress.keyvalue`: 3 rows, exactly ONE flush -> ONE
# `nb-1-big` SSTable, LZ4 — the EXACT shape pinned by the committed Flight
# decode oracle (`cqlite-flight/src/test_fixtures.rs`'s
# `KEYVALUE_DDL`/`KEYVALUE_ROWS`), not a hand-made lookalike (issue #2289
# roborev finding, 2026-07-10 — see field-repro-data.cql's header comment).
# Issue #2289 roborev finding (job 1595 class sweep): `exec`s against the
# Cassandra container are LOCAL (no image pull), but a stalled/unresponsive
# Cassandra could still hang the cqlsh load or the nodetool flush
# indefinitely — bounded at the CASSANDRA_CTL tier (generous enough for a
# 3-row load, short enough to fail loudly rather than hang the harness).
CQLSH_TINY_LOAD_TIMEOUT_SECS=120

load_tiny_table() {
  local root="$1"; shift
  local -a compose=("$@")
  log "load tiny fixture (3 rows, single flush -> one nb-1-big SSTable, LZ4, cassandra_easy_stress.keyvalue oracle shape)"
  run_with_timeout "$CQLSH_TINY_LOAD_TIMEOUT_SECS" "${compose[@]}" exec -T cassandra cqlsh 172.42.0.2 < "$root/trino-connector/docker/field-repro-data.cql"
  run_with_timeout "$CASSANDRA_CTL_TIMEOUT_SECS" "${compose[@]}" exec -T cassandra nodetool flush cassandra_easy_stress
}

# `loadtest.keyvalue`: >=100k partitions across >=2 SSTable generations, via
# `cassandra-easy-stress` with an EXPLICIT, bounded operation count
# (`-n <count> --readrate 0`) — NOT `--populate` (issue #2289 dry run,
# 2026-07-09: `--populate 60000 -n 0` did NOT stop after populating; `-n 0`
# is apparently "unbounded main workload", not "no extra ops", and the run
# kept going as a live mixed read/write KeyValue stress test for 500k+
# operations before anything downstream noticed. Verified fix in isolation
# against a throwaway single-node Cassandra: `-n 1000 --readrate 0
# --partitiongenerator sequence` executes EXACTLY 1000 write-only ops and
# exits promptly ("Stress complete"); two batches with distinct `--id`s
# produced exactly 2000 non-colliding partitions, no more, no less).
# `--partitiongenerator sequence` avoids random-collision ambiguity in the
# partition count; a distinct `--id` per batch gives each its own
# partition-key namespace, so two batches can never collide.
#
# Falls back to a cqlsh INSERT loop ONLY when the stress image is CONFIRMED
# genuinely nonexistent in the registry (`docker compose pull <service>`
# fails with an unambiguous "manifest unknown" / "not found" / "no such
# image" error) — issue #2289 roborev finding (job 1592, narrowed further by
# job 1596): a prior revision fell back on ANY nonzero `docker compose run`
# exit, then (job 1592) narrowed that to ANY nonzero `docker compose pull`
# exit — but a pull can ALSO fail on a timeout, an unreachable daemon, an
# auth/registry-access problem, or any other transient environment issue,
# none of which mean "this image doesn't exist". Conflating those with the
# genuinely-missing-image case would mask real regressions (daemon health,
# network policy, credential drift) behind a silent, slower, shape-changing
# fallback. `classify_pull_failure` (below) inspects BOTH the exit code
# (124 = timeout is NEVER a fallback trigger) AND the captured pull output
# against an ALLOWLIST of unambiguous "does not exist" error strings; only a
# match falls back — everything else (including "pull access denied ...
# repository does not exist or may require 'docker login'", Docker's single
# ambiguous message for BOTH auth failures and missing images, deliberately
# NOT allowlisted) FAILS the run loudly with the real error text. Once the
# image is confirmed pullable, any subsequent run failure is treated as a
# real error and FAILS FAST (no fallback, no silent mixing) after cleaning up
# the in-flight container.
#
# Allowlist patterns (case-insensitive), each unambiguously meaning "this
# exact image/tag does not exist in the registry" and nothing else — verified
# against Docker's actual wording for this failure mode:
#   "manifest unknown"                    — registry has no manifest for the tag
#   "manifest for .* not found"           — `docker pull`'s own not-found phrasing
#   "no such image"                       — local-reference-only variant
# Deliberately EXCLUDED: "pull access denied" / "repository does not exist or
# may require" (Docker's overloaded auth-OR-missing message — ambiguous, so
# treated as a real error per the roborev finding's explicit "auth ...
# ⇒ FAIL" guidance), any DNS/connection/daemon-unreachable text, and anything
# unrecognized.
PULL_NOT_FOUND_PATTERN='manifest unknown|manifest for .* not found|no such image'

# Classifies a failed `docker compose pull`'s outcome. Args: $1 = exit code,
# $2 = path to the captured pull output. Prints one of `not-found` (legitimate
# fallback trigger) or `real-error` (must FAIL loudly, never fall back).
classify_pull_failure() {
  local rc="$1" outfile="$2"
  if [[ "$rc" -eq 124 ]]; then
    echo "real-error"
    return 0
  fi
  if grep -Eiq "$PULL_NOT_FOUND_PATTERN" "$outfile" 2>/dev/null; then
    echo "not-found"
  else
    echo "real-error"
  fi
}

# Every invocation is wrapped in `run_with_timeout` (issue #2233 discipline
# extended to data loading, not just queries — a stuck/misconfigured stress
# run must fail loudly in minutes, not hang the whole harness): a batch that
# does not finish within `LOAD_BATCH_TIMEOUT_SECS` aborts the run rather than
# silently retrying forever.
#
# `--replication` is left at the image's default even though this is a single
# node: CL=ONE writes only need one ack, so an RF > 1 keyspace on a one-node
# ring still writes fine (matches the existing `docker-compose.yml` loadtest
# profile's cassandra-easy-stress service, which makes the same assumption).
BIG_TABLE_PARTITIONS_PER_BATCH=60000
BIG_TABLE_BATCHES=2
LOAD_BATCH_TIMEOUT_SECS=300

# Removes any cassandra-easy-stress container left in-flight by a killed/timed
# -out `docker compose run` — `--rm` only fires on a clean client-side exit,
# and `run_with_timeout` may have SIGKILLed the `docker compose run` CLI
# itself without stopping the server-side container it was attached to.
#
# Scoped to THIS compose project ONLY via `${compose[@]} ps` — issue #2289
# roborev finding (job 1595): a prior revision ALSO queried a raw
# `docker ps --filter label=com.docker.compose.service=cassandra-easy-stress`
# with NO project filter, which would match — and `docker rm -f` — a
# same-named service container from ANY OTHER compose project on the same
# host (e.g. another lane's stack, or a future project reusing the service
# name), a destructive blast-radius leak outside this run. Verified live
# (issue #2289, 2026-07-09): `docker compose ps -a -q <service>` DOES list
# one-off `run --rm` containers (not just long-lived `up` services), so the
# raw cross-project `docker ps` fallback was unnecessary as well as unsafe —
# `${compose[@]} ps`, which is inherently scoped to the exact `-f` compose
# files this script passes (hence this run's project), is sufficient alone.
cleanup_stress_containers() {
  local -a compose=("$@")
  local ids_raw
  ids_raw="$(run_with_timeout "$DOCKER_CTL_TIMEOUT_SECS" "${compose[@]}" --profile loadtest ps -a -q cassandra-easy-stress 2>/dev/null || true)"
  if [[ -n "$ids_raw" ]]; then
    # Read into an array and pass directly to `docker rm -f` (NOT `xargs`,
    # which would fork a new process that cannot see this shell's
    # `run_with_timeout` function unless explicitly exported) — one bounded
    # call covering all in-flight IDs from this project.
    local -a ids=()
    while IFS= read -r id; do
      [[ -n "$id" ]] && ids+=("$id")
    done <<< "$ids_raw"
    log "cleaning up in-flight cassandra-easy-stress container(s) in THIS compose project: ${ids[*]}"
    run_with_timeout "$DOCKER_CTL_TIMEOUT_SECS" docker rm -f "${ids[@]}" >/dev/null 2>&1 || true
  fi
}

# Records which loader actually produced `loadtest.keyvalue`'s rows as a
# harness artifact (issue #2289 roborev finding, job 1592's "at minimum say
# loudly which loader produced the data" bar) — durable beyond stdout, so a
# later reader of `field-repro-artifacts/` can tell without re-reading the
# full run log.
record_loader_provenance() {
  local loader="$1"
  mkdir -p "$ARTIFACTS_ROOT"
  echo "$loader" > "$ARTIFACTS_ROOT/loadtest-keyvalue-loader.txt"
  log "loadtest.keyvalue loader: $loader"
}

load_big_keyvalue_table() {
  local -a compose=("$@")
  log "load field-shaped big table: loadtest.keyvalue, target >=$((BIG_TABLE_PARTITIONS_PER_BATCH * BIG_TABLE_BATCHES)) partitions across $BIG_TABLE_BATCHES flushes"

  log "probing cassandra-easy-stress image availability (docker compose pull, bound ${NETWORK_PULL_TIMEOUT_SECS}s, no workload run yet)"
  local pull_outfile pull_rc
  pull_outfile="$(mktemp)"
  set +e
  run_with_timeout "$NETWORK_PULL_TIMEOUT_SECS" "${compose[@]}" --profile loadtest pull cassandra-easy-stress >"$pull_outfile" 2>&1
  pull_rc=$?
  set -e

  if [[ "$pull_rc" -eq 0 ]]; then
    rm -f "$pull_outfile"
    for batch in $(seq 1 "$BIG_TABLE_BATCHES"); do
      local id
      id="$(printf 'fr%02d' "$batch")"
      log "cassandra-easy-stress batch $batch/$BIG_TABLE_BATCHES (id=$id, $BIG_TABLE_PARTITIONS_PER_BATCH write-only ops, bound ${LOAD_BATCH_TIMEOUT_SECS}s)"
      # Captured via explicit set +e/set -e, NOT `if ! cmd; then $?`: inside an
      # `if ! cmd; then` branch, `$?` reflects the NEGATION's own exit status
      # (always 0 there), not the real `cmd` exit code — this would have
      # silently reported `rc=0` for every real failure/timeout.
      local batch_rc
      set +e
      run_with_timeout "$LOAD_BATCH_TIMEOUT_SECS" "${compose[@]}" --profile loadtest run --rm cassandra-easy-stress \
          run KeyValue --host 172.42.0.2 --port 9042 --dc dc1 --keyspace loadtest \
          --id "$id" -n "$BIG_TABLE_PARTITIONS_PER_BATCH" --readrate 0 \
          --partitiongenerator sequence
      batch_rc=$?
      set -e
      if [[ $batch_rc -ne 0 ]]; then
        echo "FATAL: cassandra-easy-stress batch $batch failed or timed out (rc=$batch_rc, bound ${LOAD_BATCH_TIMEOUT_SECS}s). The image WAS pullable, so this is a real workload/config error or a genuine hang — NOT falling back (a fallback here could silently mix a partially-written stress batch with fallback-loader rows in the SAME table)." >&2
        cleanup_stress_containers "${compose[@]}"
        record_loader_provenance "cassandra-easy-stress (FAILED at batch $batch/$BIG_TABLE_BATCHES, rc=$batch_rc — loadtest.keyvalue may contain partial/mixed data from batch(es) 1..$((batch - 1)) only)"
        exit 1
      fi
      run_with_timeout "$CASSANDRA_CTL_TIMEOUT_SECS" "${compose[@]}" exec -T cassandra nodetool flush loadtest
    done
    record_loader_provenance "cassandra-easy-stress ($BIG_TABLE_BATCHES batch(es), $BIG_TABLE_PARTITIONS_PER_BATCH ops each)"
  else
    local pull_class
    pull_class="$(classify_pull_failure "$pull_rc" "$pull_outfile")"
    if [[ "$pull_class" == "not-found" ]]; then
      log "cassandra-easy-stress image confirmed NOT FOUND in the registry (unambiguous manifest/not-found error, rc=$pull_rc) — falling back to a cqlsh INSERT loop (SLOW, last resort)"
      record_loader_provenance "cqlsh-fallback (cassandra-easy-stress image confirmed not-found: $(head -3 "$pull_outfile" | tr '\n' ' '))"
      rm -f "$pull_outfile"
      load_big_keyvalue_table_fallback "${compose[@]}"
    else
      # Any other pull failure (timeout rc=124, daemon-unreachable, auth, or
      # anything not on the not-found allowlist) is a REAL environment
      # problem — FAIL loudly with the actual error text, never silently
      # fall back (issue #2289 roborev finding, job 1596).
      echo "FATAL: cassandra-easy-stress image pull failed (rc=$pull_rc) with an error NOT recognized as 'image genuinely does not exist' — treating this as a real environment problem (daemon/network/auth/timeout), NOT falling back. Full pull output:" >&2
      cat "$pull_outfile" >&2
      record_loader_provenance "cassandra-easy-stress (PULL FAILED, unclassified error rc=$pull_rc — NOT falling back to avoid masking a real environment problem; see run log for the full pull output)"
      rm -f "$pull_outfile"
      exit 1
    fi
  fi

  # `nodetool tablestats` can comma-format the estimate (e.g. "121,940") — issue
  # #2289 roborev finding: `grep -oE '[0-9]+'` on a comma-formatted number
  # splits it into separate digit-group tokens ("121", "940"), and `tail -1`
  # silently picks the LAST group ("940"), not the real value, which would
  # false-FATAL a genuinely >=100k-partition table. Strip commas from the whole
  # line BEFORE extracting digits so a comma-formatted estimate parses as one
  # number. Common tail for BOTH the cassandra-easy-stress success path AND
  # the not-found fallback path above (the real-error path already `exit 1`s
  # and never reaches here).
  local estimate
  estimate="$(run_with_timeout "$CASSANDRA_CTL_TIMEOUT_SECS" "${compose[@]}" exec -T cassandra nodetool tablestats loadtest.keyvalue 2>&1 \
    | grep -i "Number of partitions" | tr -d ',' | grep -oE '[0-9]+' | tail -1 || true)"
  log "loadtest.keyvalue partition estimate (nodetool tablestats): ${estimate:-unknown}"
  if [[ -z "$estimate" ]] || (( estimate < 100000 )); then
    echo "FATAL: loadtest.keyvalue has fewer than 100k partitions (estimate: ${estimate:-unknown}) — the #2264 check would be vacuous" >&2
    exit 1
  fi
}

# Bound for the cqlsh fallback's bulk INSERT batch (up to
# BIG_TABLE_PARTITIONS_PER_BATCH * BIG_TABLE_BATCHES = 120k statements) —
# deliberately generous since this path is already documented as "SLOW, last
# resort" (issue #2289 roborev finding, job 1595 class sweep: still must be
# SOME bound, not unbounded, even on the intentionally-slow degraded path).
CQLSH_FALLBACK_LOAD_TIMEOUT_SECS=1800

# Fallback loader: a plain cqlsh batch-INSERT loop. Deliberately simple
# (correctness over speed) — this path only runs when the stress image itself
# could not be pulled/run, which is already a degraded environment.
load_big_keyvalue_table_fallback() {
  local -a compose=("$@")
  local total=$((BIG_TABLE_PARTITIONS_PER_BATCH * BIG_TABLE_BATCHES))
  local tmp_cql
  tmp_cql="$(mktemp)"
  {
    echo "CREATE KEYSPACE IF NOT EXISTS loadtest WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
    echo "CREATE TABLE IF NOT EXISTS loadtest.keyvalue (key text PRIMARY KEY, value text);"
    for ((i = 0; i < total; i++)); do
      echo "INSERT INTO loadtest.keyvalue (key, value) VALUES ('fallback-$i', 'v');"
    done
  } > "$tmp_cql"
  run_with_timeout "$CQLSH_FALLBACK_LOAD_TIMEOUT_SECS" "${compose[@]}" exec -T cassandra cqlsh 172.42.0.2 < "$tmp_cql"
  rm -f "$tmp_cql"
  run_with_timeout "$CASSANDRA_CTL_TIMEOUT_SECS" "${compose[@]}" exec -T cassandra nodetool flush loadtest
}
