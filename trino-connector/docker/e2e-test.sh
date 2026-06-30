#!/usr/bin/env bash
# Full end-to-end integration test: builds the Trino plugin and the cqlite-flight
# image, brings up Cassandra + Sidecar + cqlite-flight + Trino, loads data,
# flushes to SSTables, and asserts query results through the connector.
#
# Usage: trino-connector/docker/e2e-test.sh
# Exit code 0 = all assertions passed.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/trino-connector/docker/docker-compose.yml")
FAILURES=0

log()  { echo "── $* ──"; }
trino() { "${COMPOSE[@]}" exec -T trino trino --execute "$1" 2>&1; }

assert_eq() {
  local desc="$1" expected="$2" actual="$3"
  if [[ "$actual" == "$expected" ]]; then
    echo "PASS: $desc"
  else
    echo "FAIL: $desc — expected [$expected], got [$actual]"
    FAILURES=$((FAILURES + 1))
  fi
}

cleanup() { log "tearing down"; "${COMPOSE[@]}" --profile loadtest down -v --remove-orphans || true; }
trap cleanup EXIT

log "clean slate (reproducible run)"
"${COMPOSE[@]}" --profile loadtest down -v --remove-orphans || true

log "build connector plugin (JDK 25 toolchain)"
PLUGIN_DIR="$ROOT/trino-connector/build/plugin/cqlite_flight"
# Prefer the host Gradle (CI runs on a JDK 21 host). When no usable host JDK is
# present (e.g. a dev box without Java), fall back to a JDK 25 container so the
# e2e is still runnable. A persistent volume caches the Gradle dist + deps.
# Verify the plugin jar actually materialised — some host "java" shims exit 0
# without running, so an exit code alone is not proof of a build.
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

log "wait for Trino to accept queries"
for i in $(seq 1 60); do
  if "${COMPOSE[@]}" exec -T trino trino --execute "SELECT 1" >/dev/null 2>&1; then break; fi
  sleep 5
  [[ $i -eq 60 ]] && { echo "Trino did not become ready"; exit 1; }
done

log "load data + flush to SSTables"
"${COMPOSE[@]}" exec -T cassandra cqlsh 172.42.0.2 < "$ROOT/trino-connector/docker/e2e-data.cql"
"${COMPOSE[@]}" exec -T cassandra nodetool flush analytics

log "wait for the connector to resolve the table via Sidecar (CQL session warmup)"
for i in $(seq 1 36); do
  if "${COMPOSE[@]}" exec -T trino trino --execute \
       "SELECT count(*) FROM cqlite.analytics.events" >/dev/null 2>&1; then break; fi
  sleep 5
  [[ $i -eq 36 ]] && { echo "connector never resolved analytics.events (Sidecar not ready)"; exit 1; }
done

log "assert query results through the cqlite_flight connector"
assert_eq "events row count"        '"5"'                                   "$(trino 'SELECT count(*) FROM cqlite.analytics.events')"
assert_eq "predicate score>25 count" '"3"'                                  "$(trino 'SELECT count(*) FROM cqlite.analytics.events WHERE score > 25')"
assert_eq "projection + filter"     '"carol"'                               "$(trino 'SELECT name FROM cqlite.analytics.events WHERE id = 3')"
assert_eq "aggregate sum(score)"    '"150"'                                 "$(trino 'SELECT sum(score) FROM cqlite.analytics.events')"
assert_eq "uuid renders as text"    '"alpha"'                               "$(trino "SELECT label FROM cqlite.analytics.typed WHERE id = '11111111-1111-1111-1111-111111111111'")"
assert_eq "timestamp renders"       '"2024-01-01 00:00:00.000 UTC"'         "$(trino "SELECT created FROM cqlite.analytics.typed WHERE label = 'alpha'")"

# Nested predicate pushdown (#834): OR / NOT / parenthesized groups must return
# the same answer as a Trino-side filter. Rows: alice(10,t) bob(20,f) carol(30,t)
# dave(40,f) erin(50,t).
# (score>25 AND active) OR name='bob' -> carol, erin, bob = 3.
assert_eq "nested (AND)-OR predicate" '"3"'                                  "$(trino "SELECT count(*) FROM cqlite.analytics.events WHERE (score > 25 AND active = true) OR name = 'bob'")"
# NOT (score>25) -> alice, bob = 2.
assert_eq "NOT predicate"           '"2"'                                    "$(trino 'SELECT count(*) FROM cqlite.analytics.events WHERE NOT (score > 25)')"
# OR with IS NULL (no null actives) -> only erin(50) = 1.
assert_eq "OR with IS NULL"         '"1"'                                    "$(trino 'SELECT count(*) FROM cqlite.analytics.events WHERE score > 45 OR active IS NULL')"

# Aggregation pushdown (#841): global count/sum already asserted above; add
# min/max/avg (avg exercises the SumDouble+Count decomposition) and GROUP BY (the
# single-finalize-split path). Scores: 10,20,30,40,50.
assert_eq "aggregate min(score)"    '"10"'                                   "$(trino 'SELECT min(score) FROM cqlite.analytics.events')"
assert_eq "aggregate max(score)"    '"50"'                                   "$(trino 'SELECT max(score) FROM cqlite.analytics.events')"
# Integer avg now pushes via SumDouble (no i64 overflow, matches Trino) — #902.
assert_eq "aggregate avg(score)"    '"30.0"'                                 "$(trino 'SELECT avg(score) FROM cqlite.analytics.events')"
# Float/double min/max/avg push too (#896): NaN ordering matches Trino. ratios:
# 1.5,2.5,3.5,4.5,5.5 -> min 1.5, max 5.5, avg 3.5.
assert_eq "aggregate min(ratio)"    '"1.5"'                                  "$(trino 'SELECT min(ratio) FROM cqlite.analytics.events')"
assert_eq "aggregate max(ratio)"    '"5.5"'                                  "$(trino 'SELECT max(ratio) FROM cqlite.analytics.events')"
assert_eq "aggregate avg(ratio)"    '"3.5"'                                  "$(trino 'SELECT avg(ratio) FROM cqlite.analytics.events')"
# GROUP BY active -> two groups; assert the group count is 2 (deterministic scalar).
assert_eq "group-by group count"    '"2"'                                    "$(trino 'SELECT count(*) FROM (SELECT active FROM cqlite.analytics.events GROUP BY active)')"
# Aggregation + predicate: sum(score) WHERE score > 25 -> 30+40+50 = 120.
assert_eq "agg + predicate"         '"120"'                                  "$(trino 'SELECT sum(score) FROM cqlite.analytics.events WHERE score > 25')"

# AUTOMATIC aggregation-pushdown cardinality gate (issue #944). The `readings`
# table has 2 partitions × 5 clustering rows = 10 rows. The connector surfaces
# authoritative partition_count=2 / total_rows=10 over the Flight table_stats
# action; the DDL-driven gate then:
#   GROUP BY device      -> ratio ≈ 0.2 (< 0.5) -> PUSH (low cardinality)
#   GROUP BY device, ts  -> ratio ≈ 1.0 (> 0.5) -> DECLINE (high cardinality)
# Globals always push regardless of the gate.
log "wait for the connector to resolve analytics.readings"
for i in $(seq 1 36); do
  if "${COMPOSE[@]}" exec -T trino trino --execute \
       "SELECT count(*) FROM cqlite.analytics.readings" >/dev/null 2>&1; then break; fi
  sleep 5
  [[ $i -eq 36 ]] && { echo "connector never resolved analytics.readings"; exit 1; }
done

# Results are identical whether or not the aggregate is pushed (pushdown is a pure
# optimization), so assert the answers first.
assert_eq "readings row count"      '"10"'                                   "$(trino 'SELECT count(*) FROM cqlite.analytics.readings')"
# GROUP BY device -> 2 groups; sum(value) per device: dev1=150, dev2=165.
assert_eq "low-card group count"    '"2"'                                    "$(trino 'SELECT count(*) FROM (SELECT device FROM cqlite.analytics.readings GROUP BY device)')"
# GROUP BY device, ts -> 10 groups (full row uniqueness).
assert_eq "high-card group count"   '"10"'                                   "$(trino 'SELECT count(*) FROM (SELECT device, ts FROM cqlite.analytics.readings GROUP BY device, ts)')"

# Now assert the GATE via EXPLAIN: when an aggregate is pushed into the scan,
# Trino's PushAggregationIntoTableScan removes the Aggregate node (the scan emits
# the aggregate). When NOT pushed, an "Aggregate" plan node remains above the scan.
explain() { "${COMPOSE[@]}" exec -T trino trino --execute "EXPLAIN (TYPE LOGICAL) $1" 2>&1; }
assert_contains() {
  local desc="$1" needle="$2" haystack="$3"
  if grep -qi -- "$needle" <<<"$haystack"; then echo "PASS: $desc";
  else echo "FAIL: $desc — plan did not contain [$needle]"; echo "$haystack"; FAILURES=$((FAILURES + 1)); fi
}
assert_absent() {
  local desc="$1" needle="$2" haystack="$3"
  if grep -qi -- "$needle" <<<"$haystack"; then
    echo "FAIL: $desc — plan unexpectedly contained [$needle]"; echo "$haystack"; FAILURES=$((FAILURES + 1));
  else echo "PASS: $desc"; fi
}

# Low-cardinality GROUP BY device: PUSHED -> no residual Aggregate node above scan.
LOWCARD_PLAN="$(explain 'SELECT device, sum(value) FROM cqlite.analytics.readings GROUP BY device')"
assert_absent  "low-card GROUP BY is pushed (no Aggregate node)" 'Aggregate' "$LOWCARD_PLAN"
# High-cardinality GROUP BY device, ts: DECLINED -> Aggregate node remains in Trino.
HIGHCARD_PLAN="$(explain 'SELECT device, ts, sum(value) FROM cqlite.analytics.readings GROUP BY device, ts')"
assert_contains "high-card GROUP BY left to Trino (Aggregate node present)" 'Aggregate' "$HIGHCARD_PLAN"
# Global aggregate always pushes regardless of the gate -> no Aggregate node.
GLOBAL_PLAN="$(explain 'SELECT sum(value) FROM cqlite.analytics.readings')"
assert_absent  "global aggregate is pushed (no Aggregate node)" 'Aggregate' "$GLOBAL_PLAN"

# Proof it reads SSTables (not live CQL): an unflushed row must be invisible.
log "assert SSTable semantics (memtable invisible until flush)"
"${COMPOSE[@]}" exec -T cassandra cqlsh 172.42.0.2 \
  -e "INSERT INTO analytics.events (id,name,score,active) VALUES (99,'ghost',1,true);"
assert_eq "unflushed row invisible" '"5"'                                   "$(trino 'SELECT count(*) FROM cqlite.analytics.events')"
"${COMPOSE[@]}" exec -T cassandra nodetool flush analytics
assert_eq "row visible after flush" '"6"'                                   "$(trino 'SELECT count(*) FROM cqlite.analytics.events')"

echo
if [[ $FAILURES -eq 0 ]]; then
  echo "✅ E2E PASSED"
else
  echo "❌ E2E FAILED ($FAILURES assertion(s))"
  exit 1
fi
