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
  local start end elapsed out rc rows
  start="$(date +%s)"
  set +e
  # Issue #2289 roborev finding: wrapping the query as
  # `SELECT count(*) FROM (SELECT * FROM ... LIMIT 5)` strips the TOP-LEVEL
  # over-satisfied LIMIT that actually triggers Trino's mid-stream cancel — the
  # inner LIMIT 5 is satisfied inside a subquery Trino can plan/execute without
  # ever issuing the split-cancel this check exists to exercise. Run the exact
  # repro query verbatim; row count is read from its own CSV output.
  out="$(run_with_timeout "$LIMIT5_TIMEOUT_SECS" "${COMPOSE[@]}" exec -T trino trino --output-format CSV \
    --execute 'SELECT * FROM cqlite.loadtest.keyvalue LIMIT 5' 2>&1)"
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
    # Same CSV-quoting caveat as the #2193 check: count non-empty lines, not a
    # digit-anchored pattern.
    rows="$(echo "$out" | grep -c '.')"
    log "LIMIT 5 returned ${rows} row(s) in ${elapsed}s: $out"
    if [[ "$rows" -eq 5 ]]; then
      log "#2264: does NOT reproduce on this run (query returned correct row count promptly)"
      return 0
    fi
    echo "FAIL: LIMIT 5 returned wrong row count: $rows (expected 5). Output: $out" >&2
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

# #2193 pinned check: `SELECT * FROM cassandra_easy_stress.keyvalue` decodes
# through arrow-java and returns exactly the 3 pinned rows. This is the EXACT
# canonical shape (`key text PRIMARY KEY, value text`, 1 pk, 0 ck — see
# field-repro-data.cql's header comment) pinned by the committed
# `keyvalue.flightdata` Flight-level decode golden, not a lookalike (issue
# #2289 roborev finding, 2026-07-10: an earlier `id int, val text` shape could
# pass this check while the real #2193 TEXT-column decode regression was still
# present). The real bug surface was a full-row decode failure — `Failed to
# read message` — not merely a wrong count, so this reads every column of
# every row.
#
# Issue #2289 roborev finding (job 1590): counting rows alone lets silent
# decode CORRUPTION pass — e.g. a byte-swap, truncation, or column-shift that
# still happens to emit 3 lines. Assert the DECODED VALUES match the pinned
# oracle EXACTLY: the same `("k1","1"),("k2","2"),("k3","3")` rows
# `cqlite-flight/src/test_fixtures.rs`'s `KEYVALUE_ROWS` pins — the identical
# fixture the committed `keyvalue.flightdata` golden was generated from (this
# bash check reads the values back through the FULL production stack: real
# cqlite-flight Rust producer -> real Flight wire bytes -> real arrow-java
# decode -> real Trino connector -> real Trino CLI, which the Java-only
# `FlightDataGoldenDecodeTest` golden comparison does not exercise end-to-end;
# comparing byte-for-byte against the golden itself would need a JVM decode
# tool this bash harness does not have). Row order is the server's TOKEN
# order, not insertion order (per that fixture's own comment), so compare as
# a SORTED SET, not positionally.
check_2193_tiny_decode() {
  log "#2193 check: SELECT * FROM cassandra_easy_stress.keyvalue (full arrow-java decode, expect the exact pinned k1/k2/k3 rows, oracle shape)"
  local out rc expected actual
  set +e
  out="$(run_with_timeout "$QUERY_TIMEOUT_SECS" "${COMPOSE[@]}" exec -T trino trino --output-format CSV \
    --execute 'SELECT * FROM cqlite.cassandra_easy_stress.keyvalue' 2>&1)"
  rc=$?
  set -e
  if [[ $rc -eq 124 ]]; then
    echo "FAIL: SELECT * FROM cassandra_easy_stress.keyvalue timed out after ${QUERY_TIMEOUT_SECS}s" >&2
    return 1
  fi
  if [[ $rc -ne 0 ]]; then
    echo "FAIL: SELECT * FROM cassandra_easy_stress.keyvalue errored (rc=$rc): $out" >&2
    return 1
  fi
  log "decoded output:\n$out"
  # Trino's CSV output quotes every field regardless of type (e.g. `"k1","1"`
  # for the text/text keyvalue shape). `sort` makes the comparison
  # order-independent (see the function doc comment above).
  expected="$(printf '"k1","1"\n"k2","2"\n"k3","3"\n' | sort)"
  actual="$(echo "$out" | sort)"
  if [[ "$actual" == "$expected" ]]; then
    log "#2193: does not reproduce on this run (decoded values match the pinned k1/k2/k3 oracle exactly)"
    return 0
  fi
  echo "FAIL: decoded rows do not match the pinned oracle." >&2
  echo "  expected (any order): $(echo "$expected" | tr '\n' ' ')" >&2
  echo "  got:                  $(echo "$out" | tr '\n' ' ')" >&2
  return 1
}

# `--inject-failure` self-test: proves `run_check`'s capture-on-fail path fires
# and yields USEFUL artifacts (a NON-EMPTY pcap + debug logs + Trino query JSON
# in field-repro-artifacts/), independent of whether the real #2264/#2193 bugs
# reproduce on this run. Two deliberate steps, in order:
#   1. a REAL `SELECT * FROM cassandra_easy_stress.keyvalue` — this drives
#      genuine Flight do_get traffic on :8815 WHILE the tcpdump capture
#      container is running, so the copied-out pcap actually contains Flight
#      packets (issue #2289 arm64 run, 2026-07-10: a bare nonexistent-table
#      query short-circuits in Trino's planner before any do_get, producing a
#      valid-but-EMPTY pcap that is weak evidence the capture works).
#   2. then a nonexistent-table query to force the non-zero return that trips
#      capture-on-fail.
check_inject_failure() {
  log "--inject-failure step 1/2: real SELECT * FROM cassandra_easy_stress.keyvalue (drives genuine :8815 Flight traffic for the pcap)"
  "${COMPOSE[@]}" exec -T trino trino --execute 'SELECT * FROM cqlite.cassandra_easy_stress.keyvalue' 2>&1 || true
  log "--inject-failure step 2/2: querying a nonexistent table to deliberately force a failure"
  "${COMPOSE[@]}" exec -T trino trino --execute 'SELECT * FROM cqlite.fieldrepro.does_not_exist' 2>&1 || true
  return 1
}
