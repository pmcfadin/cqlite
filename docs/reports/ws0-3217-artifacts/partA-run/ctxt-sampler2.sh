#!/usr/bin/env bash
# AC5 sidecar v2. v1 summed voluntary/nonvoluntary over the LIVE thread set, which is
# NOT a monotone counter: tokio creates and retires threads (observed 14 -> 75 -> 21),
# so a retiring thread's accumulated count leaves the sum and the delta goes NEGATIVE.
# v2 records PER-TID counters so the analysis can sum per-TID deltas over the window.
OUT="$1"
while true; do
  pid=$(pgrep -x cqlite-flight | head -1)
  if [ -n "$pid" ]; then
    python3 - "$pid" >> "$OUT" 2>/dev/null <<'PY'
import glob, json, os, sys, time
pid = sys.argv[1]; tids = {}
for p in glob.glob("/proc/%s/task/*/status" % pid):
    tid = os.path.basename(os.path.dirname(p)); v = n = None
    try:
        for line in open(p):
            if line.startswith("voluntary_ctxt_switches"): v = int(line.split(":")[1])
            elif line.startswith("nonvoluntary_ctxt_switches"): n = int(line.split(":")[1])
    except OSError: continue
    if v is not None and n is not None: tids[tid] = [v, n]
print(json.dumps({"ts_unix_ms": int(time.time()*1000), "pid": int(pid), "tids": tids}))
PY
  fi
  sleep 2
done
