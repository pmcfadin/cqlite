#!/usr/bin/env bash
set -u
source /data/ws0/ws0env.sh
cd "$WT/docs/reports/ws0-3217-artifacts/harness" || exit 1
export WS0_WARM_SECS=0 WS0_STEADY_PRE_SECS=10 WS0_TAIL_SECS=5
./profile-oncpu.sh oncpu-smoke s6 16 10 bypass
echo "SMOKE-RC=$?"
