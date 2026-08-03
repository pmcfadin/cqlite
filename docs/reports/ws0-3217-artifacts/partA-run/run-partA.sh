#!/usr/bin/env bash
# Part A chain: S=1 (AC2 control) first, then S=2/4/6, then the merge reference points.
set -u
source /data/ws0/ws0env.sh
cd "$WT/docs/reports/ws0-3217-artifacts/harness" || exit 1
PROG=/data/ws0/logs/driver/partA-progress.txt
: > "$PROG"

mkdir -p /data/ws0/logs/ctxt
taskset -c 6,7,14,15 nohup /data/ws0/ctxt-sampler.sh /data/ws0/logs/ctxt/threads.jsonl >/dev/null 2>&1 &
SAMPLER=$!
echo "sampler pid $SAMPLER" >> "$PROG"
trap 'kill '"$SAMPLER"' 2>/dev/null' EXIT

run() { # label  s-spec  ramp  step  reps  path
  echo "$(date -u +%FT%TZ) START $1" >> "$PROG"
  ./sweep.sh "$1" "$2" 6,7,14,15 "$3" "$4" "$5" "$6" \
    > "/data/ws0/logs/driver/$1.out" 2>&1 < /dev/null
  # rc MUST be captured BEFORE any other command substitution: $(date ...) runs a
  # subshell and OVERWRITES $?, so `echo "$(date) END rc=$?"` always logs rc=0.
  # That bug shipped in this run's driver logs (#3217 P1) - a failed step was
  # indistinguishable from a clean one in the only retained progress ledger.
  local rc=$?
  echo "$(date -u +%FT%TZ) END   $1 rc=$rc" >> "$PROG"
}

run cn-s1 s1 1,2,4,8,16 120 3 bypass
run cn-s2 s2 1,2,4,8,16 120 3 bypass
run cn-s4 s4 1,2,4,8,16 120 3 bypass
run cn-s6 s6 1,2,4,8,16 120 3 bypass
run cn-s1-merge-n1 s1 1 120 3 merge
run cn-s6-merge-n1 s6 1 120 3 merge

echo "$(date -u +%FT%TZ) ALL-DONE" >> "$PROG"
