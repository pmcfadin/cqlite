#!/usr/bin/env bash
# AC5 sidecar: sum voluntary/nonvoluntary ctxt switches over ALL server threads.
# /proc/<pid>/status is main-thread only and reads 0; /proc/<pid>/task/*/status is the real figure.
# Pinned to the CLIENT set so it never steals server cycles (inflating client util is the safe direction).
OUT="$1"
while true; do
  pid=$(pgrep -x cqlite-flight | head -1)
  if [ -n "$pid" ]; then
    python3 - "$pid" >> "$OUT" 2>/dev/null <<'PY'
import glob, json, sys, time
pid = sys.argv[1]; v = n = t = 0
for p in glob.glob("/proc/%s/task/*/status" % pid):
    try:
        for line in open(p):
            if line.startswith("voluntary_ctxt_switches"): v += int(line.split(":")[1])
            elif line.startswith("nonvoluntary_ctxt_switches"): n += int(line.split(":")[1])
    except OSError: continue
    t += 1
print(json.dumps({"ts_unix_ms": int(time.time()*1000), "pid": int(pid),
                  "threads": t, "vol": v, "nonvol": n}))
PY
  fi
  sleep 2
done
