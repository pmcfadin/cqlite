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
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../harness/guards.sh
source "$HERE/../harness/guards.sh"
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
RC=$?   # captured IMMEDIATELY, before any command substitution
echo "rc=$RC" | tee -a "$OUT/summary.txt"
cat "$OUT/stream.txt" | tee -a "$OUT/summary.txt"
# A nonzero rc used to be PRINTED and then walked past (roborev finding #6, PR
# #3286): if a partial CSV happened to exist, the analysis below would run on it,
# find enough rows to reach a verdict, and the script would exit 0 — publishing an
# AC5 figure from a triad that had failed. AC5 is a DISCHARGED acceptance
# criterion, so a fail-open here certifies a claim against a measurement that did
# not complete.
ws0_guard_rc "AC5 triad+uncore capture" "$RC" \
  "See $OUT/stream.txt. A partial CSV must not be analysed: a bandwidth ceiling derived from an incomplete run is not a ceiling." \
  || exit 1

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
# NOTE ON THE REDIRECTION ORDER — this was a real bug, caught on first execution.
# Written as `python3 - args | tee -a file <<'PY'`, bash attaches the heredoc to the
# LAST command of the pipeline, i.e. to *tee*: tee then copies the Python SOURCE
# into summary.txt while python3 reads an empty stdin and computes nothing. The
# original form of this block had exactly that shape and had never been executed,
# so the defect sat latent in a committed script. The heredoc must be attached to
# python3 and the pipe applied after it.
# The analysis lives in run/ac5-analyse.py, not in a heredoc here. Two reasons,
# both from roborev finding #6 (PR #3286): its failure paths had to become real
# exits rather than printed prose, and a heredoc cannot be handed a crafted
# indeterminate input by a test. Its EXIT CODE is the AC5 verdict.
python3 "$HERE/ac5-analyse.py" "$OUT/perf-uncore-triad.csv" "$OUT/stream.txt" \
  | tee -a "$OUT/summary.txt"
ARC=${PIPESTATUS[0]}
ws0_guard_rc "AC5 byte accounting (run/ac5-analyse.py)" "$ARC" \
  "The channels-vs-duplicates question is unresolved or resolved against the derivation, so no bandwidth figure may be published from $OUT." \
  || exit 1
echo "AC5 peak: byte accounting resolved; artefacts in $OUT" | tee -a "$OUT/summary.txt"
