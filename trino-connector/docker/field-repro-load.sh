#!/usr/bin/env bash
# Field-shaped data loading for field-repro.sh (issue #2289, deliverable 1).
# Sourced by field-repro.sh; expects `log()` already defined by the caller.
set -euo pipefail

# `cassandra_easy_stress.keyvalue`: 3 rows, exactly ONE flush -> ONE
# `nb-1-big` SSTable, LZ4 — the EXACT shape pinned by the committed Flight
# decode oracle (`cqlite-flight/src/test_fixtures.rs`'s
# `KEYVALUE_DDL`/`KEYVALUE_ROWS`), not a hand-made lookalike (issue #2289
# roborev finding, 2026-07-10 — see field-repro-data.cql's header comment).
load_tiny_table() {
  local root="$1"; shift
  local -a compose=("$@")
  log "load tiny fixture (3 rows, single flush -> one nb-1-big SSTable, LZ4, cassandra_easy_stress.keyvalue oracle shape)"
  "${compose[@]}" exec -T cassandra cqlsh 172.42.0.2 < "$root/trino-connector/docker/field-repro-data.cql"
  "${compose[@]}" exec -T cassandra nodetool flush cassandra_easy_stress
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
# Falls back to a cqlsh INSERT loop ONLY when the stress image itself is not
# PULLABLE (network-restricted environment) — issue #2289 roborev finding
# (job 1592): a prior revision fell back on ANY nonzero `docker compose run`
# exit, which conflated a genuinely missing image with real workload/config
# errors AND `LOAD_BATCH_TIMEOUT_SECS` kills, silently masking real defects
# and risking a partially-written stress batch mixing with fallback-loader
# rows in the SAME table. The image-availability check
# (`docker compose pull <service>`) runs BEFORE any workload attempt and is
# the ONLY trigger for the fallback; once the image is confirmed pullable, any
# subsequent run failure is treated as a real error and FAILS FAST (no
# fallback, no silent mixing) after cleaning up the in-flight container.
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

# Removes any cassandra-easy-stress container left in-flight by a killed/timed
# -out `docker compose run` — `--rm` only fires on a clean client-side exit,
# and `run_with_timeout` may have SIGKILLed the `docker compose run` CLI
# itself without stopping the server-side container it was attached to.
# Queried TWO ways for robustness across compose versions: `compose ps` scoped
# to this project/service, and a raw `docker ps` label filter (one-off `run`
# containers aren't always listed by `compose ps`) — deduped, best-effort,
# never fatal.
cleanup_stress_containers() {
  local -a compose=("$@")
  local ids
  ids="$( { "${compose[@]}" --profile loadtest ps -a -q cassandra-easy-stress 2>/dev/null; \
            docker ps -a -q --filter "label=com.docker.compose.service=cassandra-easy-stress" 2>/dev/null; } \
          | sort -u)"
  if [[ -n "$ids" ]]; then
    log "cleaning up in-flight cassandra-easy-stress container(s): $(echo "$ids" | tr '\n' ' ')"
    echo "$ids" | xargs -r docker rm -f >/dev/null 2>&1 || true
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

  log "probing cassandra-easy-stress image availability (docker compose pull, no workload run yet)"
  if ! "${compose[@]}" --profile loadtest pull cassandra-easy-stress >/dev/null 2>&1; then
    log "WARNING: cassandra-easy-stress image is NOT pullable (network-restricted environment) — falling back to a cqlsh INSERT loop (SLOW, last resort)"
    record_loader_provenance "cqlsh-fallback (cassandra-easy-stress image unavailable)"
    load_big_keyvalue_table_fallback "${compose[@]}"
  else
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
      "${compose[@]}" exec -T cassandra nodetool flush loadtest
    done
    record_loader_provenance "cassandra-easy-stress ($BIG_TABLE_BATCHES batch(es), $BIG_TABLE_PARTITIONS_PER_BATCH ops each)"
  fi

  # `nodetool tablestats` can comma-format the estimate (e.g. "121,940") — issue
  # #2289 roborev finding: `grep -oE '[0-9]+'` on a comma-formatted number
  # splits it into separate digit-group tokens ("121", "940"), and `tail -1`
  # silently picks the LAST group ("940"), not the real value, which would
  # false-FATAL a genuinely >=100k-partition table. Strip commas from the whole
  # line BEFORE extracting digits so a comma-formatted estimate parses as one
  # number.
  local estimate
  estimate="$("${compose[@]}" exec -T cassandra nodetool tablestats loadtest.keyvalue 2>&1 \
    | grep -i "Number of partitions" | tr -d ',' | grep -oE '[0-9]+' | tail -1 || true)"
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
