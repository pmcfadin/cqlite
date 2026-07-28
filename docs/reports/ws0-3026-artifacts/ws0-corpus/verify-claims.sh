#!/usr/bin/env bash
# WS0 empirical verification of two source-derived claims (CQLite #3026).
#
#   Claim A: file_cache_enabled (the ChunkCache) defaults to FALSE.
#   Claim B: the scan path issues 256 KiB preads, not one 16 KiB pread per chunk.
#
# Run as: bash verify-claims.sh   (needs passwordless sudo for drop_caches + bpftrace)
set -uo pipefail

CAS=/home/ubuntu/ws0/apache-cassandra-5.0.8
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
HERE=/home/ubuntu/ws0/ws0-corpus
OUT=${1:-$HERE/claims-evidence.txt}
: > "$OUT"
log() { echo "$@" | tee -a "$OUT"; }

# Identify the daemon by the process LISTENING on the native port, not by a cmdline
# grep: `setsid nohup bin/cassandra` leaves a bash wrapper whose cmdline also matches
# "CassandraDaemon", and that wrapper has all-zero /proc/<pid>/io counters.
PID=$(sudo -n ss -ltnpH "sport = :9042" 2>/dev/null | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$PID" ] || PID=$(pgrep -x java | while read -r p; do
      grep -qa "org.apache.cassandra.service.CassandraDaemon" "/proc/$p/cmdline" 2>/dev/null && echo "$p"; done | head -1)
log "daemon RSS (KB)       : $(awk '/VmRSS/{print $2}' /proc/$PID/status)"
log "daemon pid            : $PID"
log "kernel                : $(uname -r)"
log "max_hw_sectors_kb     : $(cat /sys/block/nvme0n1/queue/max_hw_sectors_kb)"
log "max_sectors_kb        : $(cat /sys/block/nvme0n1/queue/max_sectors_kb)"
log "read_ahead_kb         : $(cat /sys/block/nvme0n1/queue/read_ahead_kb)"
log ""

# ============================== CLAIM A ======================================
log "================ CLAIM A: file_cache_enabled / ChunkCache ================"
log "--- evidence A1: system_views.settings on the LIVE node ---"
python3 "$HERE/cql.py" "SELECT name, value FROM system_views.settings WHERE name IN \
 ('file_cache_enabled','file_cache_size','file_cache_round_up','disk_access_mode', \
  'buffer_pool_use_heap_if_exhausted')" 2>/dev/null | tee -a "$OUT"

log ""
log "--- evidence A2: nodetool info cache lines (Info.java prints a 'Chunk Cache' line"
log "    ONLY if the Cache/ChunkCache MBean is registered; it swallows"
log "    InstanceNotFoundException, so absence == chunk cache not instantiated) ---"
"$CAS/bin/nodetool" info | grep -iE "Cache" | tee -a "$OUT"
if "$CAS/bin/nodetool" info | grep -qi "Chunk Cache"; then
  log "A2 VERDICT: 'Chunk Cache' line PRESENT -> chunk cache is ON"
else
  log "A2 VERDICT: 'Chunk Cache' line ABSENT -> chunk cache MBean not registered -> OFF"
fi

log ""
log "--- evidence A3: ChunkCache metric MBeans registered in the live JVM ---"
sudo -n env JAVA_HOME="$JAVA_HOME" "$CAS/bin/nodetool" sjk mxdump 2>/dev/null \
  | grep -c "ChunkCache" | sed 's/^/ChunkCache MBean name occurrences: /' | tee -a "$OUT" \
  || log "(sjk mxdump unavailable)"

log ""
log "--- evidence A4: startup Config dump ---"
grep -o "file_cache_enabled=[a-z]*" /home/ubuntu/ws0/cassandra-logs/stdout.log | head -1 | tee -a "$OUT"
grep -o "compressed_read_ahead_buffer_size=[0-9A-Za-z]*" /home/ubuntu/ws0/cassandra-logs/stdout.log | head -1 | tee -a "$OUT"
grep "DiskAccessMode is" /home/ubuntu/ws0/cassandra-logs/stdout.log | tail -1 | tee -a "$OUT"
grep "Global buffer pool limit" /home/ubuntu/ws0/cassandra-logs/stdout.log | tail -1 | tee -a "$OUT"

# ============================== CLAIM B ======================================
log ""
log "================ CLAIM B: read sizes during a COLD full scan ============="

io_snap() { grep -E "^(rchar|read_bytes|syscr|wchar|write_bytes|syscw)" "/proc/$PID/io"; }

log "--- /proc/$PID/io BEFORE (raw counters, reported separately; NEVER divided) ---"
io_snap | tee -a "$OUT"
io_snap > /tmp/io_before.txt

log ""
log "--- dropping page cache ---"
sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null; sleep 2
free -h | sed -n 2p | tee -a "$OUT"

log ""
log "--- starting bpftrace (syscall + block level) ---"
sudo -n bpftrace "$HERE/trace-scan.bt" "$PID" > /tmp/bt.out 2>/tmp/bt.err &
BTPID=$!
for i in $(seq 1 40); do grep -q TRACING /tmp/bt.out 2>/dev/null && break; sleep 1; done
grep -q TRACING /tmp/bt.out || { log "bpftrace failed to start:"; cat /tmp/bt.err | tee -a "$OUT"; }

log "--- running cold full scan ---"
python3 "$HERE/fullscan.py" 512 2>/dev/null | tee -a "$OUT"

log ""
log "--- stopping bpftrace ---"
sudo -n pkill -INT -f "bpftrace.*trace-scan.bt" 2>/dev/null
wait $BTPID 2>/dev/null
sed 's/^/  /' /tmp/bt.out | tee -a "$OUT"

log ""
log "--- /proc/$PID/io AFTER (raw counters) ---"
io_snap | tee -a "$OUT"
io_snap > /tmp/io_after.txt

log ""
log "--- /proc/$PID/io DELTA over the scan (each counter separately; NOT divided) ---"
python3 - <<'PY' | tee -a "$OUT"
def rd(p):
    return {k: int(v) for k, v in (l.split(": ") for l in open(p).read().split("\n") if ": " in l)}
b, a = rd("/tmp/io_before.txt"), rd("/tmp/io_after.txt")
for k in ("rchar", "syscr", "read_bytes", "wchar", "syscw", "write_bytes"):
    if k in a:
        print(f"  delta {k:<12}= {a[k]-b[k]}")
print("  NOTE: rchar counts bytes returned to userspace by read/pread syscalls;")
print("        read_bytes counts bytes actually fetched from the block device;")
print("        syscr counts read-family syscalls. They measure DIFFERENT layers and")
print("        rchar/syscr is NOT a per-syscall request size (a prior effort's bogus")
print("        '~59 KB/syscall' came from exactly that division).")
PY

log ""
log "--- Data.db size for comparison with device read_bytes ---"
ls -l /home/ubuntu/ws0/cassandra-data/data/ws0/events-*/*-Data.db | tee -a "$OUT"
log ""
log "evidence written to $OUT"
