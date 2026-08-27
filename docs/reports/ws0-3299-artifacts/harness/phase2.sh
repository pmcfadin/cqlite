#!/usr/bin/env bash
# #3299 PHASE 2 — the Flight `do_get` arm on Corpus B, at S=1 and S=6 only.
#
# WHY THIS EXISTS. AC4 needs "remaining to target", which needs both the target
# (bare scan, phase 1) and where we are today (`do_get`). The only box-level
# `do_get` figure in existence is #3217's 1,076,917 rows/s on CORPUS A (LZ4,
# 196.09 B/row); phase 1's target is CORPUS B (uncompressed, 693.69 B/row).
# Dividing across those is forbidden — 3.5x the bytes per row and no per-row
# decompression are two large opposite-signed effects on the measured quantity.
# So `do_get` is measured on Corpus B, in the same session, at TWO points only.
#
# NOT a 25-point grid: the full `do_get` C(N) curve is #3217's deliverable and
# already exists. No acceptance criterion asks for it again on Corpus B.
#
# THE ALIGNED WINDOW HERE IS #3224's, VERBATIM AND UNMODIFIED: perf runs the
# loadgen as its OWN CHILD, so the counted interval IS the row-producing
# interval — numerator and denominator share one window by construction, with no
# rate assumption and nothing to attribute. Phase 1 needed its own machinery only
# because it had N independent worker PROCESSES and no single child to wrap; here
# there is exactly one loadgen process, which is the case #3224's convention was
# written for. The server is warmed by a separate uncounted invocation first.
#
# CORE ALLOCATION — the client set is CONSTANT across S, on purpose.
#   server = the first S complete sibling groups
#   client = the LAST TWO complete sibling groups, at EVERY S
# Holding the client constant is what makes `do_get`'s OWN S=1->S=6 slope
# internally valid: if the client shrank as the server grew, the arm's slope
# would confound server scaling with client starvation. It also matches
# #3217/#3224's convention (a constant 2-physical-core client), which is what
# makes the Corpus-B-vs-Corpus-A `do_get` comparison same-convention and
# same-arm — the one comparison in phase 2 that carries no asymmetry caveat.
#
# THE ASYMMETRY, STATED NOT HIDDEN. Bare-scan S=6 ran 6 cores pinned with 2 IDLE
# and no client; `do_get` S=6 runs 6 serving with those same 2 BUSY driving load.
# Those are not identical machine states, so the CROSS-ARM slope comparison is
# not a controlled A/B. It is still the right thing to measure: the deployment
# bar itself is asymmetric (real `do_get` has clients; bare scan does not), each
# arm's own marginal efficiency is self-normalised and internally valid, and the
# `do_get`-B-vs-A comparison is clean. The report states this rather than
# implying a controlled comparison.
set -Eeuo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../../.." && pwd)"
CONTAIN="$REPO/test-data/scripts/perf-run-contained.sh"
GUARDS="$HERE/guards.py"

CORPUS=/data/ws0-3096
KEYSPACE=ws0
TABLE=events
RESULTS=""
S_LIST="1,6"
RAMP=""                 # concurrency per rep; default resolved per S below
REPS=3
STEP_DURATION=60s
PORT=18815
MEM_CAP=24G
CLIENT_CORES=2
SERVER_BIN="$REPO/target/release/cqlite-flight"
LOADGEN_BIN="$REPO/target/release/flight-loadgen"
# `full` is the shape that corresponds to a bare scan (`SELECT * FROM ws0.events`).
# The loadgen's DEFAULT is `mixed`, which would measure a different workload and
# make the cross-arm ratio meaningless.
SHAPE=full

usage() { sed -n '2,40p' "${BASH_SOURCE[0]}" >&2; exit 2; }
while [[ $# -gt 0 ]]; do
  case "$1" in
    --results) RESULTS="$2"; shift 2 ;;
    --corpus) CORPUS="$2"; shift 2 ;;
    --s-list) S_LIST="$2"; shift 2 ;;
    --ramp) RAMP="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    --step-duration) STEP_DURATION="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --shape) SHAPE="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "unknown arg: $1" >&2; usage ;;
  esac
done
[[ -n "$RESULTS" ]] || { echo "--results is required" >&2; usage; }
for b in "$SERVER_BIN" "$LOADGEN_BIN"; do
  [[ -x "$b" ]] || { echo "FATAL: $b not built. Build BEFORE the box is quiet, never during a rep." >&2; exit 2; }
done
mkdir -p "$RESULTS"; RESULTS="$(cd "$RESULTS" && pwd)"

# --- topology, from sysfs (never assumed) -------------------------------------
SIB_MAP="$RESULTS/siblings.map"; : > "$SIB_MAP"
for c in /sys/devices/system/cpu/cpu[0-9]*; do
  echo "${c##*/cpu} $(cat "$c/topology/thread_siblings_list")" >> "$SIB_MAP"
done
sort -n -o "$SIB_MAP" "$SIB_MAP"
mapfile -t CORE_GROUPS < <(awk '{print $2}' "$SIB_MAP" | sort -u -t, -k1,1n)
PHYS=${#CORE_GROUPS[@]}
# Client = the LAST CLIENT_CORES groups, constant at every S.
CLIENT_CPUS="$(printf '%s\n' "${CORE_GROUPS[@]}" | tail -n "$CLIENT_CORES" | paste -sd,)"
echo "[phase2] $PHYS physical cores; client (CONSTANT) = $CLIENT_CPUS"

# --- ticket template: the DDL travels in the TICKET, not a server flag --------
# `cqlite-flight` has no --schema: service.rs `parse_schema(ticket)` parses the
# CQL DDL carried by each request and caches it. So Corpus B's schema is injected
# here, and the server needs no change to serve an uncompressed corpus.
DDL_FILE="$CORPUS/$KEYSPACE-$TABLE.cql"
[[ -r "$DDL_FILE" ]] || DDL_FILE="$CORPUS/ws0-events.cql"
[[ -r "$DDL_FILE" ]] || { echo "FATAL: no DDL at $DDL_FILE" >&2; exit 2; }
TICKET="$RESULTS/ticket-template.json"
python3 - "$DDL_FILE" "$KEYSPACE" "$TABLE" "$TICKET" <<'PY'
import json, sys
ddl = open(sys.argv[1]).read()
json.dump({"keyspace": sys.argv[2], "table": sys.argv[3], "ddl": ddl,
           "snapshot": None, "token_ranges": None, "limit": None},
          open(sys.argv[4], "w"), indent=2)
PY
echo "[phase2] ticket template <- $DDL_FILE"

start_server() { # $1 = server cpu list, $2 = log
  "$CONTAIN" --mem "$MEM_CAP" --swap 0 -- \
    taskset -c "$1" "$SERVER_BIN" --data-dir "$CORPUS" --port "$PORT" > "$2" 2>&1 &
  SERVER_WRAPPER=$!
  for _ in $(seq 1 120); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then exec 3<&- 3>&-; return 0; fi
    sleep 0.5
  done
  echo "FATAL: server did not listen on $PORT within 60s (see $2)" >&2; return 1
}
stop_server() {
  # Kill by PID, never by name: `pkill -x cqlite-flight` cannot match a name over
  # the kernel's 15-char comm limit, and `pkill -f <pat>` matches the killer's own
  # shell. Both report success having killed nothing.
  pkill -P "${SERVER_WRAPPER:-0}" 2>/dev/null || true
  kill "${SERVER_WRAPPER:-0}" 2>/dev/null || true
  wait "${SERVER_WRAPPER:-0}" 2>/dev/null || true
}
trap stop_server EXIT

IFS=',' read -r -a S_VALUES <<< "$S_LIST"
: > "$RESULTS/manifest.jsonl"
for S in "${S_VALUES[@]}"; do
  (( S + CLIENT_CORES <= PHYS )) || { echo "FATAL: S=$S plus a $CLIENT_CORES-core client exceeds $PHYS physical cores" >&2; exit 2; }
  SERVER_CPUS="$(printf '%s\n' "${CORE_GROUPS[@]}" | head -n "$S" | paste -sd,)"
  python3 "$GUARDS" cpuset --s "$S" --cpus "$SERVER_CPUS" --siblings "$SIB_MAP" --headroom-cores 0
  # Disjointness is verified, not assumed: `perf stat -C <server>` would
  # otherwise count client work as engine work.
  python3 - "$SERVER_CPUS" "$CLIENT_CPUS" <<'PY'
import sys
a = set(sys.argv[1].split(",")); b = set(sys.argv[2].split(","))
if a & b:
    sys.exit(f"FATAL: server and client CPU sets OVERLAP on {sorted(a & b)}")
PY
  N="${RAMP:-$(( S * 4 ))}"
  echo "[phase2] S=$S server=$SERVER_CPUS client=$CLIENT_CPUS N=$N"
  start_server "$SERVER_CPUS" "$RESULTS/server-s${S}.log"

  # WARMUP: uncounted, and it doubles as the recon's outstanding residual check —
  # a non-zero do_get against Corpus B, confirmed BEFORE any measurement, because
  # a 0-row do_get presents as a very fast one.
  taskset -c "$CLIENT_CPUS" "$LOADGEN_BIN" --endpoint "http://127.0.0.1:$PORT" \
    --ticket-template "$TICKET" --ramp "$N" --step-duration 20s --shape "$SHAPE" \
    --round "warmup-s$S" --out "$RESULTS/s${S}-warmup.jsonl" > "$RESULTS/s${S}-warmup.log" 2>&1
  python3 "$GUARDS" flight-step --jsonl "$RESULTS/s${S}-warmup.jsonl"

  for (( rep=1; rep<=REPS; rep++ )); do
    RD="$RESULTS/s${S}-n${N}-rep${rep}"; mkdir "$RD"
    # ALIGNED (#3224): perf is the PARENT of the loadgen, so the counted interval
    # is the row-producing interval. -C counts the SERVER cores only.
    perf stat -x, -o "$RD/perf.csv" -C "$SERVER_CPUS" \
      -e instructions,cycles,L1-dcache-loads,L1-dcache-load-misses,task-clock \
      -- taskset -c "$CLIENT_CPUS" "$LOADGEN_BIN" \
           --endpoint "http://127.0.0.1:$PORT" --ticket-template "$TICKET" \
           --ramp "$N" --step-duration "$STEP_DURATION" --shape "$SHAPE" \
           --round "s${S}-rep${rep}" --out "$RD/step.jsonl" > "$RD/loadgen.log" 2>&1
    python3 "$GUARDS" perf-csv --csv "$RD/perf.csv"
    python3 "$GUARDS" flight-step --jsonl "$RD/step.jsonl"
    echo "{\"arm\":\"do_get\",\"s\":$S,\"n\":$N,\"rep\":$rep,\"rundir\":\"$RD\",\"server_cpus\":\"$SERVER_CPUS\",\"client_cpus\":\"$CLIENT_CPUS\"}" >> "$RESULTS/manifest.jsonl"
    echo "[phase2] S=$S rep=$rep OK"
  done
  stop_server
done
echo "[phase2] done -> $RESULTS/manifest.jsonl"
