#!/usr/bin/env bash
# #3299 PHASE 2 — the Flight `do_get` arm on Corpus B. ONE point per invocation.
#
# WHY ONE POINT PER INVOCATION rather than an S sweep: phase 2's scope is not a
# curve. It is (1) a servability smoke, (2) a client-bound FALSIFICATION at S=6,
# and (3) `do_get` at S=1 on the rig's own calibrated core split. Those three
# want DIFFERENT core allocations, so the allocation is an explicit argument and
# is printed into every artifact, rather than derived by a rule that would have
# to encode three exceptions. The launcher can see exactly what will run.
#
# THE VALIDITY PROBLEM THIS SHAPE EXISTS TO RESPECT (recon, adopted):
# `ws0-baseline.sh` ships `SERVER_CPUS="2,10"` (ONE physical core) against
# `CLIENT_CPUS="4,12,5,13,6,14,7,15"` (FOUR) — a 1:4 server:client ratio chosen
# by whoever calibrated the rig. A `do_get` S=6 point on 8 physical cores would
# run 6:2, a 12x swing, and a 2-core client driving a 6-core server is far below
# what that author thought a ONE-core server needed. If such a point is
# client-bound it is not a measurement of `do_get` at all — and the error
# direction UNDERSTATES `do_get`, OVERSTATING the bare-scan gap, which flatters
# the very lever this issue calibrates. Hence: no S=6 figure is published unless
# the falsification below clears it.
#
# ALIGNED WINDOW — #3224's convention, verbatim: perf runs the loadgen as its OWN
# CHILD, so the counted interval IS the row-producing interval. Numerator and
# denominator share one window by construction; nothing is attributed and no rate
# is assumed. (Phase 1 needed bespoke machinery only because it had N independent
# worker processes and no single child to wrap.)
#
# USAGE — the three phase-2 invocations, in order:
#
#   # 1. servability + S=1 at the rig's CALIBRATED 1:4 split (server 1 core, client 4)
#   phase2.sh --results D --label s1-rigsplit \
#             --server-cpus 2,10 --client-cpus 4,12,5,13,6,14,7,15 --n-list 1,2,4,8
#
#   # 2/3. the client-bound FALSIFICATION at S=6: identical server, client halved
#   phase2.sh --results D --label s6-client2 \
#             --server-cpus 0,8,1,9,2,10,3,11,4,12,5,13 --client-cpus 6,14,7,15 --n-list 24
#   phase2.sh --results D --label s6-client1 \
#             --server-cpus 0,8,1,9,2,10,3,11,4,12,5,13 --client-cpus 7,15     --n-list 24
#   #    aggregate moves materially => CLIENT-BOUND, the S=6 number is VOID
#   #    aggregate does not move     => that objection is falsified; only the
#   #                                   machine-state asymmetry remains, and it is disclosable
#   python3 phase2-compare.py --results D --a s6-client2 --b s6-client1
set -Eeuo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../../.." && pwd)"
CONTAIN="$REPO/test-data/scripts/perf-run-contained.sh"
GUARDS="$HERE/guards.py"

CORPUS=/data/ws0-3096
KEYSPACE=ws0; TABLE=events
RESULTS=""; LABEL=""; SERVER_CPUS=""; CLIENT_CPUS=""; N_LIST=""
REPS=3; STEP_DURATION=60s; PORT=18815; MEM_CAP=24G
SERVER_BIN="$REPO/target/release/cqlite-flight"
LOADGEN_BIN="$REPO/target/release/flight-loadgen"
# `full` matches the bare scan's `SELECT * FROM ws0.events`. The loadgen's DEFAULT
# is `mixed`, which measures a different workload and would void the cross-arm ratio.
SHAPE=full

usage() { sed -n '2,45p' "${BASH_SOURCE[0]}" >&2; exit 2; }
while [[ $# -gt 0 ]]; do
  case "$1" in
    --results) RESULTS="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --server-cpus) SERVER_CPUS="$2"; shift 2 ;;
    --client-cpus) CLIENT_CPUS="$2"; shift 2 ;;
    --n-list) N_LIST="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    --step-duration) STEP_DURATION="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --corpus) CORPUS="$2"; shift 2 ;;
    --shape) SHAPE="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "unknown arg: $1" >&2; usage ;;
  esac
done
for req in RESULTS LABEL SERVER_CPUS CLIENT_CPUS N_LIST; do
  [[ -n "${!req}" ]] || { echo "--${req,,} is required" >&2; usage; }
done
for b in "$SERVER_BIN" "$LOADGEN_BIN"; do
  [[ -x "$b" ]] || { echo "FATAL: $b not built. Build while the box is QUIET, never during a rep." >&2; exit 2; }
done
mkdir -p "$RESULTS"; RESULTS="$(cd "$RESULTS" && pwd)"

# --- topology + core-set verification -----------------------------------------
SIB_MAP="$RESULTS/siblings.map"; : > "$SIB_MAP"
for c in /sys/devices/system/cpu/cpu[0-9]*; do
  echo "${c##*/cpu} $(cat "$c/topology/thread_siblings_list")" >> "$SIB_MAP"
done
sort -n -o "$SIB_MAP" "$SIB_MAP"
# Both sets must be complete sibling groups: a half-populated core measures a
# different machine. `--headroom-cores 0` because phase 2 deliberately uses the
# whole box (server + client), unlike the bare-scan arm which kept 2 cores idle —
# a difference §9.2 of the report discloses rather than hides.
SCOUNT="$(python3 -c 'import sys;print(len(set(sys.argv[1].split(","))) // 2)' "$SERVER_CPUS")"
CCOUNT="$(python3 -c 'import sys;print(len(set(sys.argv[1].split(","))) // 2)' "$CLIENT_CPUS")"
python3 "$GUARDS" cpuset --s "$SCOUNT" --cpus "$SERVER_CPUS" --siblings "$SIB_MAP" --headroom-cores 0
python3 "$GUARDS" cpuset --s "$CCOUNT" --cpus "$CLIENT_CPUS" --siblings "$SIB_MAP" --headroom-cores 0
# Disjointness is VERIFIED, not assumed: `perf stat -C <server>` would otherwise
# count the loadgen's work as engine work.
python3 - "$SERVER_CPUS" "$CLIENT_CPUS" <<'PY'
import sys
a, b = set(sys.argv[1].split(",")), set(sys.argv[2].split(","))
if a & b:
    sys.exit(f"FATAL: server and client CPU sets OVERLAP on {sorted(a & b)} — "
             f"perf -C <server> would count client work as engine work")
PY
echo "[phase2] $LABEL  server=$SERVER_CPUS (${SCOUNT} cores)  client=$CLIENT_CPUS (${CCOUNT} cores)"

# --- ticket template: the DDL travels in the TICKET, not a server flag --------
# `cqlite-flight` has no --schema: `service.rs:424 parse_schema(ticket)` parses the
# CQL DDL carried by each request and caches it. So Corpus B's schema is injected
# here and the server needs no change to serve an uncompressed corpus.
DDL_FILE="$CORPUS/ws0-events.cql"
[[ -r "$DDL_FILE" ]] || { echo "FATAL: no DDL at $DDL_FILE" >&2; exit 2; }
TICKET="$RESULTS/ticket-template.json"
python3 - "$DDL_FILE" "$KEYSPACE" "$TABLE" "$TICKET" <<'PY'
import json, sys
json.dump({"keyspace": sys.argv[2], "table": sys.argv[3], "ddl": open(sys.argv[1]).read(),
           "snapshot": None, "token_ranges": None, "limit": None},
          open(sys.argv[4], "w"), indent=2)
PY

SERVER_WRAPPER=""
stop_server() {
  # By PID, never by name. `pkill -x cqlite-flight` CANNOT match — the kernel's
  # comm is 15 chars and the name is longer, so it reports success having killed
  # nothing. `pkill -f <pat>` matches the killer's own shell. Both leave orphans
  # that hold cores and silently corrupt the NEXT rep.
  [[ -n "$SERVER_WRAPPER" ]] || return 0
  pkill -P "$SERVER_WRAPPER" 2>/dev/null || true
  kill "$SERVER_WRAPPER" 2>/dev/null || true
  wait "$SERVER_WRAPPER" 2>/dev/null || true
  SERVER_WRAPPER=""
}
trap stop_server EXIT

"$CONTAIN" --mem "$MEM_CAP" --swap 0 -- \
  taskset -c "$SERVER_CPUS" "$SERVER_BIN" --data-dir "$CORPUS" --port "$PORT" \
  > "$RESULTS/$LABEL-server.log" 2>&1 &
SERVER_WRAPPER=$!
for _ in $(seq 1 120); do
  (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && { exec 3<&- 3>&-; break; }
  sleep 0.5
done
(exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null || {
  echo "FATAL: server did not listen on $PORT within 60s (see $RESULTS/$LABEL-server.log)" >&2; exit 2; }
exec 3<&- 3>&- || true

# --- SERVABILITY SMOKE: uncounted, and MANDATORY ------------------------------
# A 0-row `do_get` presents as an extremely FAST one — a server answering
# NotFound completes every request immediately — so "it ran" proves nothing and
# a row count must be observed BEFORE any rep is measured. #3224 shipped exactly
# this failure (2,258,606 NotFounds behind `discovered 0 tables`, rc=0).
FIRST_N="${N_LIST%%,*}"
taskset -c "$CLIENT_CPUS" "$LOADGEN_BIN" --endpoint "http://127.0.0.1:$PORT" \
  --ticket-template "$TICKET" --ramp "$FIRST_N" --step-duration 20s --shape "$SHAPE" \
  --round "smoke-$LABEL" --out "$RESULTS/$LABEL-smoke.jsonl" > "$RESULTS/$LABEL-smoke.log" 2>&1
python3 "$GUARDS" flight-step --jsonl "$RESULTS/$LABEL-smoke.jsonl"
echo "[phase2] servability smoke PASSED (non-zero rows observed against Corpus B)"

IFS=',' read -r -a NS <<< "$N_LIST"
for N in "${NS[@]}"; do
  for (( rep=1; rep<=REPS; rep++ )); do
    RD="$RESULTS/${LABEL}-n${N}-rep${rep}"
    [[ -e "$RD" ]] && { echo "FATAL: $RD exists; refusing to reuse or delete a results dir" >&2; exit 2; }
    mkdir "$RD"
    # ALIGNED: perf is the PARENT of the loadgen, counting the SERVER cores only.
    perf stat -x, -o "$RD/perf.csv" -C "$SERVER_CPUS" \
      -e instructions,cycles,L1-dcache-loads,L1-dcache-load-misses,task-clock \
      -- taskset -c "$CLIENT_CPUS" "$LOADGEN_BIN" \
           --endpoint "http://127.0.0.1:$PORT" --ticket-template "$TICKET" \
           --ramp "$N" --step-duration "$STEP_DURATION" --shape "$SHAPE" \
           --round "${LABEL}-n${N}-rep${rep}" --out "$RD/step.jsonl" > "$RD/loadgen.log" 2>&1
    python3 "$GUARDS" perf-csv --csv "$RD/perf.csv"
    python3 "$GUARDS" flight-step --jsonl "$RD/step.jsonl"
    printf '{"arm":"do_get","label":"%s","n":%s,"rep":%s,"rundir":"%s","server_cpus":"%s","client_cpus":"%s","server_cores":%s,"client_cores":%s,"shape":"%s"}\n' \
      "$LABEL" "$N" "$rep" "$RD" "$SERVER_CPUS" "$CLIENT_CPUS" "$SCOUNT" "$CCOUNT" "$SHAPE" >> "$RESULTS/manifest.jsonl"
    echo "[phase2] $LABEL N=$N rep=$rep OK"
  done
done
stop_server
echo "[phase2] $LABEL done -> $RESULTS/manifest.jsonl"
