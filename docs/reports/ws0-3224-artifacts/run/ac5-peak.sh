#!/usr/bin/env bash
# #3224 AC5 — achievable memory bandwidth on THIS host, at the engine's binding.
#
# RUNBOOK step 8: the peak must be measured "pinned and NUMA-bound exactly like
# the engine arms, because a peak measured across both sockets is not the ceiling
# the engine faces."
#
# That is not a theoretical caution. The positive control's ADVISORY unpinned
# 128-thread triad reported 285.382 GB/s @24B on one run and 131.865 GB/s @24B on
# the next, minutes apart on an idle box — 2.16x apart. Neither is a ceiling; both
# are contention artefacts. So the AC5 figure comes from HERE, at the S=6/N=16
# server binding, and never from that advisory line.
#
# `cache-hostile stream` is a STREAM-TRIAD-CLASS reference, NOT the vendor STREAM
# benchmark, and the report must say so. It reports two byte bases and the report
# must name which one it quotes:
#   24 B/element  architectural (2 reads + 1 write per element)
#   32 B/element  including read-for-ownership (the write's line is fetched first)
# A bare GB/s with no basis named is not a usable number.
#
# We also count the triad's OWN DRAM traffic with uncore_imc, as an independent
# cross-check that the byte accounting is right (RUNBOOK step 8.2: "Cross-check
# the peak against a second source if cheap").
set -uo pipefail
OUT="${1:-/data/ws0/ac5}"
BIN="${BIN:-/data/ws0/positive-control-run2/cache-hostile}"
SERVER_CPUS="${SERVER_CPUS:-0-5,64-69}"    # the S=6/N=16 server set
NODE="${NODE:-0}"
THREADS="${THREADS:-12}"                    # hw threads in that set
STREAM_MIB="${STREAM_MIB:-4096}"
ITERS="${ITERS:-10}"

mkdir -p "$OUT"
[ -x "$BIN" ] || { echo "FATAL: cache-hostile not at $BIN"; exit 2; }
sudo -n sysctl -q -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 >/dev/null 2>&1 || true
[ "$(cat /proc/sys/kernel/perf_event_paranoid)" = "-1" ] || { echo "FATAL: paranoid != -1"; exit 2; }

UNCORE="$(python3 -c "
print(','.join('uncore_imc_%d/cas_count_%s/' % (i,k) for i in range(12) for k in ('read','write')))")"

echo "== AC5 peak, pinned to $SERVER_CPUS on node $NODE, threads=$THREADS, ${STREAM_MIB} MiB, iters=$ITERS" \
  | tee "$OUT/summary.txt"

# The engine binding, verbatim: numactl node-bind + membind, then taskset.
numactl --cpunodebind="$NODE" --membind="$NODE" \
  taskset -c "$SERVER_CPUS" \
  perf stat -x, --per-socket -a -e "$UNCORE" -o "$OUT/perf-uncore-triad.csv" -- \
  "$BIN" stream --stream-mib "$STREAM_MIB" --threads "$THREADS" --iters "$ITERS" \
  > "$OUT/stream.txt" 2>&1
RC=$?
echo "rc=$RC" | tee -a "$OUT/summary.txt"
cat "$OUT/stream.txt" | tee -a "$OUT/summary.txt"

# Independent cross-check from the IMC counters over the SAME run.
python3 - "$OUT/perf-uncore-triad.csv" "$OUT/stream.txt" | tee -a "$OUT/summary.txt" <<'PY'
import re, sys
csv, stxt = sys.argv[1], sys.argv[2]
per = {'S0': 0.0, 'S1': 0.0}
elapsed = None
for line in open(csv):
    line = line.strip()
    if not line or line.startswith('#'):
        continue
    f = line.split(',')
    if len(f) < 7:
        continue
    sock, val, unit, ev, enabled = f[0], f[2], f[3], f[4], f[6]
    try:
        v = float(val)
    except ValueError:
        continue
    if float(enabled) < 99.0:
        sys.exit("FATAL: %s %s only %s%% enabled" % (sock, ev, enabled))
    if sock in per and 'cas_count' in ev:
        per[sock] += v            # already MiB; do NOT multiply by 64 again
    if elapsed is None:
        try: elapsed = float(f[5]) / 1e9
        except (ValueError, IndexError): pass
tot = per['S0'] + per['S1']
print()
print("-- IMC cross-check of the triad's own DRAM traffic (independent of the")
print("   triad's internal byte accounting) --")
print("   window (perf run_time)  : %.3f s" % (elapsed or float('nan')))
print("   S0 cas total           : %.1f MiB" % per['S0'])
print("   S1 cas total (far)     : %.1f MiB" % per['S1'])
if tot:
    print("   far-socket fraction    : %.4f  (membind=node0 should keep this near 0)"
          % (per['S1'] / tot))
if elapsed:
    print("   measured DRAM traffic  : %.2f GB/s" % (tot * 1048576 / 1e9 / elapsed))
print()
print("   NOTE: this is a STREAM-TRIAD-CLASS reference, not the vendor STREAM")
print("   benchmark. Quote which byte basis (24 B/elem architectural, or 32 B/elem")
print("   including read-for-ownership) any published figure uses.")
PY
