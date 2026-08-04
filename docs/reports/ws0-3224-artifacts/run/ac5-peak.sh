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
#
# THIS BLOCK ALSO SETTLES A QUESTION THE WHOLE BANDWIDTH CLAIM RESTS ON.
# perf exposes uncore_imc_0..11, all with cpumask=0,32, and per socket EIGHT of
# them report a near-identical non-zero value while FOUR read exactly 0.0. Two
# readings fit that equally well:
#   (a) 8 POPULATED CHANNELS, near-identical because DRAM interleaving is uniform
#       -> the per-instance values must be SUMMED;
#   (b) 8 DUPLICATE REPORTS of one socket-level aggregate
#       -> summing would overcount by 8x.
# `sum/max = 7.996` is consistent with BOTH, so it cannot decide, and every GB/s
# figure in this report differs by 8x depending on which is true.
#
# The triad decides it by BYTE ACCOUNTING, which needs no timing at all: it moves
# a known number of bytes (elements x iters x basis, plus the init pass), so the
# ratio IMC_total / expected_total is ~1 under (a) and ~8 under (b).
#
# Note the timing subtlety this avoids: the triad reports its GB/s from the BEST
# iteration, while perf's counters cover the WHOLE window including the
# single-threaded init. Those two are not comparable as rates, so the rate
# comparison is reported for information and the BYTE ratio carries the verdict.
python3 - "$OUT/perf-uncore-triad.csv" "$OUT/stream.txt" | tee -a "$OUT/summary.txt" <<'PY'
import sys
csv, stxt = sys.argv[1], sys.argv[2]
per = {'S0': {}, 'S1': {}}
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
        per[sock][ev] = v         # already MiB; do NOT multiply by 64 again
    if elapsed is None:
        try:
            elapsed = float(f[5]) / 1e9
        except (ValueError, IndexError):
            pass

st = {}
for line in open(stxt):
    if '=' in line:
        k, _, v = line.strip().partition('=')
        st[k] = v

tot_mib = sum(sum(d.values()) for d in per.values())
print()
print("-- IMC cross-check of the triad's own DRAM traffic --")
print("   window (perf run_time)  : %.3f s" % (elapsed or float('nan')))
for sock in ('S0', 'S1'):
    vals = list(per[sock].values())
    nz = [v for v in vals if v > 0]
    print("   %s: %d instances, %d non-zero, sum %.1f MiB (per-instance min %.1f "
          "max %.1f)" % (sock, len(vals), len(nz), sum(vals),
                         min(nz) if nz else 0.0, max(nz) if nz else 0.0))
if tot_mib:
    print("   far-socket fraction     : %.4f  (membind=node0 should keep this near 0)"
          % (sum(per['S1'].values()) / tot_mib))

try:
    n = float(st['elements']); iters = float(st['iters'])
    best = float(st['best_iter_s']); init = float(st['init_s'])
    # Steady-state traffic: `iters` passes at 32 B/element (2 reads + the written
    # line's read-for-ownership + the writeback). The init pass writes all three
    # arrays once, so charge it at 3 arrays x 8 B x n, x2 for RFO + writeback.
    steady = 32.0 * n * iters
    init_bytes = 2.0 * 3.0 * 8.0 * n
    expected = steady + init_bytes
    measured = tot_mib * 1048576.0
    ratio = measured / expected if expected else float('nan')
    print()
    print("   -- byte accounting: is the per-instance sum right? --")
    print("   elements %.0f, iters %.0f" % (n, iters))
    print("   expected steady traffic  : %.1f GB (32 B/elem x elements x iters)"
          % (steady / 1e9))
    print("   expected init traffic    : %.1f GB (3 arrays written once, RFO+WB)"
          % (init_bytes / 1e9))
    print("   expected TOTAL           : %.1f GB" % (expected / 1e9))
    print("   IMC measured TOTAL       : %.1f GB" % (measured / 1e9))
    print("   ratio measured/expected  : %.3f" % ratio)
    if 0.6 <= ratio <= 1.6:
        print("   VERDICT: ~1x  -> the 8 non-zero instances per socket are DISTINCT")
        print("            CHANNELS and summing them is CORRECT.")
    elif 6.0 <= ratio <= 10.0:
        print("   VERDICT: ~8x  -> the instances are DUPLICATE reports of one")
        print("            socket aggregate. EVERY GB/s figure derived by summing")
        print("            them is 8x too high and MUST be divided by the non-zero")
        print("            instance count. Fix the derivation before publishing.")
    else:
        print("   VERDICT: INDETERMINATE (ratio %.3f matches neither ~1x nor ~8x)."
              % ratio)
        print("            Do NOT publish a bandwidth figure until this is resolved.")
    print()
    print("   rates, for information only (NOT the verdict): the triad's own GB/s")
    print("   comes from its BEST iteration (%.6f s) while the IMC counters cover"
          % best)
    print("   the WHOLE %.3f s window including a %.3f s single-threaded init, so"
          % (elapsed or float('nan'), init))
    print("   the IMC window-average rate is necessarily the lower of the two.")
    if elapsed:
        print("   IMC window-average      : %.2f GB/s" % (measured / 1e9 / elapsed))
    print("   triad best-iteration     : basis24 %s GB/s | basis32 %s GB/s"
          % (st.get('gbps_basis24', '?'), st.get('gbps_basis32', '?')))
    print("   steady-state IMC equivalent at the best iteration rate:")
    print("                            : %.2f GB/s" % (32.0 * n / best / 1e9))
except (KeyError, ValueError) as exc:
    print("   byte accounting UNAVAILABLE (%s) — the channels-vs-duplicates"
          % exc)
    print("   question is then UNRESOLVED and no bandwidth figure may be published.")

print()
print("   NOTE: this is a STREAM-TRIAD-CLASS reference, not the vendor STREAM")
print("   benchmark. Quote which byte basis (24 B/elem architectural, or 32 B/elem")
print("   including read-for-ownership) any published figure uses.")
PY
