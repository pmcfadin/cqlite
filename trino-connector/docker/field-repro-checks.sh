#!/usr/bin/env bash
# The two pinned repro checks for field-repro.sh (issue #2289, deliverable 2),
# plus the `--inject-failure` self-test of the capture-on-fail path. Sourced
# by field-repro.sh; expects `log()`/`trino_query()`/`COMPOSE`/`ARTIFACTS_ROOT`
# already defined by the caller.
set -euo pipefail

# A hung query only ties up the Trino CLI client; a SHORTER bound than the
# script-wide QUERY_TIMEOUT_SECS lets the #2264 check reach its "did it hang"
# verdict without burning the full 180s budget on the primary attempt.
LIMIT5_TIMEOUT_SECS=25
# Bound for the post-hang drain probe (issue #2289 acceptance: "in-flight
# count returns to 0 within a bound (<=30s)"). Since the live gauge is not
# reachable from outside the container in this compose stack (see
# field-repro.sh's top-of-file honesty note), a trivial follow-up query
# returning promptly is the proxy for "the server, not just this one query,
# recovered" — a real full-server lockup would make even `SELECT 1` hang.
DRAIN_BOUND_SECS=30

# #2264 pinned check: `SELECT * FROM keyvalue LIMIT 5` against the >=100k
# partition table. PASS means EITHER (a) it returns promptly with the correct
# row count (the bug does not reproduce on this run's shape — an honest
# negative result, not a vacuous one: `loadtest.keyvalue` genuinely has
# >=100k partitions, verified by `load_big_keyvalue_table`'s tablestats
# assertion), OR (b) it hangs past LIMIT5_TIMEOUT_SECS (reproducing the bug)
# but a follow-up trivial query still drains within DRAIN_BOUND_SECS (the
# server itself recovered even though this one query was stuck — this is
# still recorded as a FAIL below so the reproduction is visible, not hidden).
check_2264_limit5_midstream_cancel() {
  log "#2264 check: SELECT * FROM loadtest.keyvalue LIMIT 5 (bound ${LIMIT5_TIMEOUT_SECS}s)"
  local start end elapsed out rc
  start="$(date +%s)"
  set +e
  out="$(run_with_timeout "$LIMIT5_TIMEOUT_SECS" "${COMPOSE[@]}" exec -T trino trino --output-format CSV \
    --execute 'SELECT count(*) FROM (SELECT * FROM cqlite.loadtest.keyvalue LIMIT 5)' 2>&1)"
  rc=$?
  set -e
  end="$(date +%s)"
  elapsed=$((end - start))

  # Best-effort proxy evidence (issue #2289's own documented substitute for a
  # live in_flight gauge): the crate's debug-level phase/prune lines.
  local evidence="$ARTIFACTS_ROOT/2264-proxy-evidence.log"
  mkdir -p "$ARTIFACTS_ROOT"
  "${COMPOSE[@]}" logs cqlite-flight --no-color 2>&1 \
    | grep -E "do_get phase completed|token-range SSTable prune" > "$evidence" || true
  log "proxy evidence (do_get phase timings / SSTable prune counts): $evidence"
  tail -20 "$evidence" >&2 || true

  if [[ $rc -eq 0 ]]; then
    log "LIMIT 5 returned in ${elapsed}s: $out"
    if [[ "$out" == '"5"' ]]; then
      log "#2264: does NOT reproduce on this run (query returned correct row count promptly)"
      return 0
    fi
    echo "FAIL: LIMIT 5 returned wrong row count: $out" >&2
    return 1
  fi

  if [[ $rc -eq 124 ]]; then
    echo "#2264 REPRODUCES: LIMIT 5 hung past ${LIMIT5_TIMEOUT_SECS}s — killed the client mid-stream (this IS the repro)." >&2
    log "checking whether the server drains: a trivial SELECT 1 within ${DRAIN_BOUND_SECS}s"
    if run_with_timeout "$DRAIN_BOUND_SECS" "${COMPOSE[@]}" exec -T trino trino --execute "SELECT 1" >/dev/null 2>&1; then
      echo "PARTIAL: the server drained/recovered within ${DRAIN_BOUND_SECS}s even though LIMIT 5 hung — the do_get accounting did not wedge the whole server, but the query-level hang itself IS the #2264 defect." >&2
    else
      echo "FAIL: the server did NOT recover within ${DRAIN_BOUND_SECS}s — full lockup, not just one stuck query." >&2
    fi
    return 1
  fi

  echo "FAIL: LIMIT 5 query errored (rc=$rc): $out" >&2
  return 1
}

# #2193 pinned check: `SELECT * FROM tiny` decodes through arrow-java and
# returns exactly 3 rows (the real bug surface was a full-row decode
# failure — `Failed to read message` — not merely a wrong count, so this
# reads every column of every row, not just `count(*)`).
check_2193_tiny_decode() {
  log "#2193 check: SELECT * FROM fieldrepro.tiny (full arrow-java decode, expect 3 rows)"
  local out rc rows
  set +e
  out="$(run_with_timeout "$QUERY_TIMEOUT_SECS" "${COMPOSE[@]}" exec -T trino trino --output-format CSV \
    --execute 'SELECT * FROM cqlite.fieldrepro.tiny' 2>&1)"
  rc=$?
  set -e
  if [[ $rc -eq 124 ]]; then
    echo "FAIL: SELECT * FROM tiny timed out after ${QUERY_TIMEOUT_SECS}s" >&2
    return 1
  fi
  if [[ $rc -ne 0 ]]; then
    echo "FAIL: SELECT * FROM tiny errored (rc=$rc): $out" >&2
    return 1
  fi
  # Trino's CSV output quotes every field regardless of type (verified live:
  # `"1","a"` for an `int` id column) — count non-empty lines, not a
  # digit-anchored pattern (which would silently match zero rows and produce
  # a false FAIL).
  rows="$(echo "$out" | grep -c '.')"
  log "decoded output:\n$out"
  if [[ "$rows" -eq 3 ]]; then
    log "#2193: does not reproduce on this run (3 rows decoded correctly)"
    return 0
  fi
  echo "FAIL: expected 3 decoded rows, got $rows. Output: $out" >&2
  return 1
}

# `--inject-failure` self-test: deliberately queries a nonexistent table so
# `run_check`'s capture-on-fail path can be proven to actually fire (pcap +
# debug logs + Trino query JSON land in field-repro-artifacts/), independent
# of whether the real #2264/#2193 bugs happen to reproduce on this run.
check_inject_failure() {
  log "--inject-failure: querying a nonexistent table to deliberately force a failure"
  "${COMPOSE[@]}" exec -T trino trino --execute 'SELECT * FROM cqlite.fieldrepro.does_not_exist' 2>&1 || true
  return 1
}
