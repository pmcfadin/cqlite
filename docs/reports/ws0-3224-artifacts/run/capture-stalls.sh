#!/usr/bin/env bash
# #3224 arm (d) — group C: DIRECTLY MEASURED memory-stall cycles.
#
# WHY THIS ARM EXISTS
# ------------------
# AC4 wants the cycles/row delta ATTRIBUTED, with the residual as a number.
# The classical way to do that is to charge each miss counter at a modelled
# penalty:  attributed = SUM over counters of  d(misses/row) x penalty_cycles.
# That product is only as good as the penalty, and the penalty is the weakest
# link in the whole chain:
#
#   * an UNLOADED serial-chase latency (what run/penalty-probe.sh measures)
#     assumes ZERO memory-level parallelism, so it OVERCHARGES every miss that
#     actually overlapped another miss;
#   * dividing by an assumed MLP UNDERCHARGES if the guess is high;
#   * a vendor figure is not this silicon under this load at all.
#
# #3217 charged modelled penalties and landed ~87% UNATTRIBUTED, and reporting
# that honestly is what made it usable. But on THIS host we do not have to model
# it: `cycle_activity.stalls_l3_miss` counts EXECUTION-STALL CYCLES WHILE AN L3
# MISS IS OUTSTANDING, in hardware, per pinned CPU. It is the quantity the
# penalty product is trying to estimate, measured instead of inferred — and it
# is inherently MLP-correct, because two overlapping misses stalling the same
# cycle are ONE stalled cycle, which is exactly what a cycles/row accounting
# needs to add up.
#
# So this arm charges the MEASURED stall cycles and keeps the modelled product
# as a cross-check. Where the two disagree, the measurement wins and the report
# says by how much.
#
# Group C (verified 100.00% enabled on this host under load before running):
#   cycles, instructions, task-clock,
#   cycle_activity.stalls_l3_miss    <- memory-stall cycles, THE attribution term
#   cycle_activity.stalls_l2_miss    <- L2-miss stalls (superset: includes L3 hits)
#   cycle_activity.stalls_total      <- all execution stalls; the denominator that
#                                       says what fraction of the decay is memory
#   l1d_pend_miss.pending            <- SUM of outstanding L1D misses per cycle
#   l1d_pend_miss.pending_cycles     <- cycles with >=1 outstanding
#                                       => MLP = pending / pending_cycles, so the
#                                          modelled charge can be MLP-corrected
#                                          with a MEASURED divisor, not a guess.
#
# It reuses harness/common.sh unchanged, so every guard that governs the primary
# arms governs this one: sysctls re-asserted per capture, server/client sets
# verified as COMPLETE SMT sibling groups on ONE NUMA node and disjoint, warmth
# verified via /proc/<pid>/io read_bytes delta == 0, occupancy fail-closed on an
# empty or partial-scan run, and the client-saturation ceiling.
#
# ALIGNED convention only. perf runs the loadgen as its own child, so the counted
# interval and the row-producing interval are ONE interval by construction — no
# rate assumption, which is what the primary arms established as the sound basis.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/../harness/ws0env.sh"
source "$HERE/../harness/common.sh"

LABEL="${1:?label}"; S="${2:?S}"; N="${3:?N}"
STEP_SECS="${4:?step_secs}"; REP="${5:?rep}"; OUT="${6:?outdir}"
CORPUS_ROWS="${CORPUS_ROWS:-3999890}"

CORE_EVENTS_C="cycles,instructions,task-clock,cycle_activity.stalls_l3_miss,cycle_activity.stalls_l2_miss,cycle_activity.stalls_total,l1d_pend_miss.pending,l1d_pend_miss.pending_cycles"

mkdir -p "$OUT"
ws0_require_inputs
ws0_assert_sysctl

SERVER_CPUS="$(ws0_server_cpus_for_s "$S")"
CLIENT_CPUS="$WS0_CLIENT_CPUS_DEFAULT"
ws0_assert_cpuset_on_node      "server(S=$S)" "$SERVER_CPUS"
ws0_assert_cpuset_on_node      "client"       "$CLIENT_CPUS"
ws0_assert_full_physical_cores "server(S=$S)" "$SERVER_CPUS" "$S"
ws0_assert_full_physical_cores "client"       "$CLIENT_CPUS" 2
ws0_assert_sets_disjoint       "$SERVER_CPUS" "$CLIENT_CPUS"

SRV_LOG="$OUT/server-stalls.log"
ws0_start_server "$SERVER_CPUS" bypass "$SRV_LOG"
SRV_PID="$WS0_SERVER_PID"
cleanup() { ws0_stop_server "${SRV_PID:-}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

ws0_warm_prepass "$CLIENT_CPUS" "${WARM_SECS:-45}" "$OUT/warm-stalls.log"

IO_BEFORE="$(ws0_proc_io_json "$SRV_PID")"
CLIENT_BUSY_BEFORE="$(ws0_cpuset_busy_secs "$CLIENT_CPUS")"
WALL_BEFORE="$(date +%s.%N)"

ws0_log "[$LABEL rep$REP] arm(d) ALIGNED groupC (stalls_l3_miss / MLP)"
perf stat -x, -C "$SERVER_CPUS" -e "$CORE_EVENTS_C" \
  -o "$OUT/perf-coreC-aligned.csv" -- \
  taskset -c "$CLIENT_CPUS" "$WS0_LOADGEN_BIN" \
    --endpoint "$WS0_ENDPOINT" --ticket-template "$WS0_TICKET_TPL" \
    --shape full --ramp "$N" --step-duration "${STEP_SECS}s" \
    --seed "$WS0_SEED" --round "${LABEL}-alignedC" \
    --out "$OUT/step-alignedC.jsonl" > "$OUT/loadgen-alignedC.log" 2>&1
RC_C=$?                                   # captured IMMEDIATELY, before any $( )

WALL_AFTER="$(date +%s.%N)"
IO_AFTER="$(ws0_proc_io_json "$SRV_PID")"
CLIENT_BUSY_AFTER="$(ws0_cpuset_busy_secs "$CLIENT_CPUS")"

ws0_stop_server "$SRV_PID"; trap - EXIT INT TERM

# NOTE: this heredoc is QUOTED ('PY'), so bash performs NO substitution inside
# it and the python source cannot be mangled. Values arrive as argv instead.
# capture-endpoint.sh uses an UNQUOTED heredoc and had to warn against writing
# backticks in its own docstrings; passing argv removes the hazard entirely.
python3 - "$OUT" "$LABEL" "$REP" "$S" "$N" "$STEP_SECS" "$SERVER_CPUS" \
  "$CLIENT_CPUS" "$CORPUS_ROWS" "$RC_C" "$IO_BEFORE" "$IO_AFTER" \
  "$CLIENT_BUSY_BEFORE" "$CLIENT_BUSY_AFTER" "$WALL_BEFORE" "$WALL_AFTER" \
  "$CORE_EVENTS_C" "$WS0_CLIENT_SAT_THRESHOLD" "$WS0_BUSY_FRACTION_FLOOR" <<'PY'
import json, sys, os
(out, label, rep, S, N, step, scpus, ccpus, corpus, rc, io0, io1,
 cb0, cb1, w0, w1, events, satmax, busyfloor) = sys.argv[1:20]
rows = int(corpus)

def last(p):
    try:
        recs = [json.loads(l) for l in open(p) if l.strip()]
        return recs[-1] if recs else None
    except OSError:
        return None

s = last(os.path.join(out, 'step-alignedC.jsonl'))
if s is None:
    sys.exit("FATAL: no step record — the loadgen produced nothing")

# Occupancy: fail closed on an empty or partial-scan run. `rt % rows == 0` alone
# is NOT enough, because 0 % rows == 0 passed a run that measured nothing.
rt = s['rows_total']; ok = s['requests_ok']
err = s.get('requests_error', 0); unav = s.get('requests_unavailable', 0)
d = s['duration_s']; n = s['target_concurrency']
p50 = s['latency_ms']['p50'] / 1000.0
occ = {
    'rows_total': rt,
    'rows_positive': rt > 0,
    'whole_scans': rt / rows,
    'rows_total_is_exact_multiple_of_corpus': (rt > 0 and rt % rows == 0),
    'requests_ok': ok, 'requests_error': err, 'requests_unavailable': unav,
    'duration_s': d, 'rows_per_s_step': s['rows_per_s'],
    'busy_fraction_estimate': (ok * p50 / n / d) if (n and d) else None,
    # THE BUSY FRACTION IS GATED HERE TOO (roborev round 5 finding #2). The primary
    # capture applied WS0_BUSY_FRACTION_FLOOR to every occupancy arm while this one --
    # the arm that supplies the HEADLINE attribution -- only recorded the estimate.
    # A largely idle stalls arm could therefore pass validation and carry the
    # measured attribution, which is the fix landing in one file and not its sibling:
    # the shape that produced findings in three consecutive rounds. Not computable
    # (n or d zero) is a FAILURE, not a pass.
    'busy_fraction_floor': float(busyfloor),
    'busy_fraction_ok': bool((ok * p50 / n / d) >= float(busyfloor)) if (n and d)
                        else False,
    'ok': bool(rt > 0 and rt % rows == 0 and err == 0 and unav == 0 and ok > 0
               and (n and d) and (ok * p50 / n / d) >= float(busyfloor)),
}

doc = {
    'schema': 'ws0-3224.stalls/v1',
    'label': label, 'rep': int(rep),
    'S_physical_cores': int(S), 'N_streams': int(N),
    'step_secs': float(step), 'server_cpus': scpus, 'client_cpus': ccpus,
    'corpus_rows': rows, 'event_group_C': events,
    'rc': {'alignedC': int(rc)},
    'convention': 'aligned (perf runs the loadgen as its own child)',
    'step': s, 'occupancy': {'alignedC': occ},
    'server_io_before': json.loads(io0), 'server_io_after': json.loads(io1),
}
rb0 = doc['server_io_before'].get('read_bytes')
rb1 = doc['server_io_after'].get('read_bytes')
doc['warm_read_bytes_delta'] = (rb1 - rb0) if (rb0 is not None and rb1 is not None) else None
doc['warm_verified_zero_disk_reads'] = (doc['warm_read_bytes_delta'] == 0)

wall = float(w1) - float(w0)
ncli = len({c for p in ccpus.split(',')
            for c in (range(int(p.split('-')[0]), int(p.split('-')[1]) + 1)
                      if '-' in p else [int(p)])})
busy = float(cb1) - float(cb0)
doc['client_cpu_count'] = ncli
doc['client_utilisation'] = (busy / (wall * ncli)) if wall and ncli else None
doc['client_saturation_gate_pass'] = (doc['client_utilisation'] is not None
                                      and doc['client_utilisation'] <= float(satmax))

with open(os.path.join(out, 'meta-stalls.json'), 'w') as fh:
    fh.write(json.dumps(doc, indent=1) + '\n')

print('  meta-stalls -> %s/meta-stalls.json' % out)
print('  warm read_bytes delta: %s | client util: %.4f | gate: %s'
      % (doc['warm_read_bytes_delta'], doc['client_utilisation'] or -1,
         'PASS' if doc['client_saturation_gate_pass'] else 'FAIL'))
print('  occupancy[alignedC]: rows=%d whole_scans=%.3f exact=%s err=%d '
      'busy_frac=%.4f rows/s=%.0f ok=%s'
      % (occ['rows_total'], occ['whole_scans'],
         occ['rows_total_is_exact_multiple_of_corpus'], occ['requests_error'],
         occ['busy_fraction_estimate'] or -1, occ['rows_per_s_step'], occ['ok']))

bad = []
if not occ['ok']:
    bad.append('occupancy')
if not doc['warm_verified_zero_disk_reads']:
    bad.append('warmth (read_bytes delta=%s)' % doc['warm_read_bytes_delta'])
if not doc['client_saturation_gate_pass']:
    bad.append('client saturation (util=%s)' % doc['client_utilisation'])
if int(rc) != 0:
    bad.append('perf rc=%s' % rc)
if bad:
    sys.exit('FATAL: validity gates FAILED: %s' % bad)
print('  ALL VALIDITY GATES PASS')
PY
RC_META=$?
ws0_log "[$LABEL rep$REP] arm(d) done rc(alignedC=$RC_C meta=$RC_META)"
[ "$RC_C" -eq 0 ] && [ "$RC_META" -eq 0 ] || exit 1
exit 0
