#!/usr/bin/env bash
# Field-shaped data loading for field-repro.sh (issue #2289, deliverable 1).
# Sourced by field-repro.sh; expects `log()` already defined by the caller.
set -euo pipefail

# `fieldrepro.tiny`: 3 rows, exactly ONE flush -> ONE `nb-1-big` SSTable, LZ4 —
# the precise shape #2193's arrow-java decode check needs.
load_tiny_table() {
  local root="$1"; shift
  local -a compose=("$@")
  log "load tiny fixture (3 rows, single flush -> one nb-1-big SSTable, LZ4)"
  "${compose[@]}" exec -T cassandra cqlsh 172.42.0.2 < "$root/trino-connector/docker/field-repro-data.cql"
  "${compose[@]}" exec -T cassandra nodetool flush fieldrepro
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
# partition-key namespace, so two batches can never collide. Falls back to a
# cqlsh INSERT loop if the stress image cannot be pulled/run (network-
# restricted environment) — the acceptance criterion is "enough partitions to
# saturate the do_get channel", not the specific loader, and the fallback is
# intentionally slower so it is a true last resort, not the default path.
#
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

load_big_keyvalue_table() {
  local -a compose=("$@")
  log "load field-shaped big table: loadtest.keyvalue, target >=$((BIG_TABLE_PARTITIONS_PER_BATCH * BIG_TABLE_BATCHES)) partitions across $BIG_TABLE_BATCHES flushes"
  local stress_ok=1
  for batch in $(seq 1 "$BIG_TABLE_BATCHES"); do
    local id
    id="$(printf 'fr%02d' "$batch")"
    log "cassandra-easy-stress batch $batch/$BIG_TABLE_BATCHES (id=$id, $BIG_TABLE_PARTITIONS_PER_BATCH write-only ops, bound ${LOAD_BATCH_TIMEOUT_SECS}s)"
    if ! run_with_timeout "$LOAD_BATCH_TIMEOUT_SECS" "${compose[@]}" --profile loadtest run --rm cassandra-easy-stress \
        run KeyValue --host 172.42.0.2 --port 9042 --dc dc1 --keyspace loadtest \
        --id "$id" -n "$BIG_TABLE_PARTITIONS_PER_BATCH" --readrate 0 \
        --partitiongenerator sequence; then
      echo "cassandra-easy-stress batch $batch did not finish within ${LOAD_BATCH_TIMEOUT_SECS}s or errored" >&2
      stress_ok=0
      break
    fi
    "${compose[@]}" exec -T cassandra nodetool flush loadtest
  done

  if [[ "$stress_ok" -eq 0 ]]; then
    log "WARNING: cassandra-easy-stress unavailable — falling back to a cqlsh INSERT loop (SLOW, last resort)"
    load_big_keyvalue_table_fallback "${compose[@]}"
  fi

  local estimate
  estimate="$("${compose[@]}" exec -T cassandra nodetool tablestats loadtest.keyvalue 2>&1 \
    | grep -i "Number of partitions" | grep -oE '[0-9]+' | tail -1 || true)"
  log "loadtest.keyvalue partition estimate (nodetool tablestats): ${estimate:-unknown}"
  if [[ -z "$estimate" ]] || (( estimate < 100000 )); then
    echo "FATAL: loadtest.keyvalue has fewer than 100k partitions (estimate: ${estimate:-unknown}) — the #2264 check would be vacuous" >&2
    exit 1
  fi
}

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
  "${compose[@]}" exec -T cassandra cqlsh 172.42.0.2 < "$tmp_cql"
  rm -f "$tmp_cql"
  "${compose[@]}" exec -T cassandra nodetool flush loadtest
}
