#!/usr/bin/env bash
# Poor-man's Java profiler: repeated jstack, keep only RUNNABLE worker threads,
# tally their TOP frame. Java JIT frames are invisible to `perf` without a
# perf-map agent, so this is the only Java-level view available on this box.
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
PID=$1; N=${2:-120}; OUT=$3
: > "$OUT"
for i in $(seq 1 "$N"); do
  $JAVA_HOME/bin/jstack "$PID" 2>/dev/null >> "$OUT"
  sleep 0.15
done
