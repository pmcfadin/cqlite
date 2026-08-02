#!/usr/bin/env bash
# Waits for the main chain, then re-runs S=1 with the per-TID AC5 sidecar live.
# cn-s1 ran BEFORE the sidecar existed, so its voluntary/involuntary split is absent.
# Identical config to cn-s1 (s1, 120s, 3 reps, bypass), so it is simultaneously an
# independent reproduction of the AC2 control, not just an AC5 top-up.
set -u
source /data/ws0/ws0env.sh
cd "$WT/docs/reports/ws0-3217-artifacts/harness" || exit 1
PROG=/data/ws0/logs/driver/partA-progress.txt
while ! grep -q ALL-DONE "$PROG" 2>/dev/null; do sleep 15; done
echo "$(date -u +%FT%TZ) START cn-s1-ac5" >> "$PROG"
./sweep.sh cn-s1-ac5 s1 6,7,14,15 1,2,4,8,16 120 3 bypass \
  > /data/ws0/logs/driver/cn-s1-ac5.out 2>&1 < /dev/null
echo "$(date -u +%FT%TZ) END   cn-s1-ac5 rc=$?" >> "$PROG"
echo "$(date -u +%FT%TZ) FOLLOWON-DONE" >> "$PROG"
