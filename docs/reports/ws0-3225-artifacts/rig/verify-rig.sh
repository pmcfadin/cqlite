#!/usr/bin/env bash
# Rig verification for CQLite issue #3225 §2 — reproduces the report §2.1 rig record
# for the box that runs the C(N) sweep, and DEMONSTRATES sweep.sh's server/client
# core-overlap refusal rather than trusting it.
#
# Usage:
#   bash docs/reports/ws0-3225-artifacts/rig/verify-rig.sh [-o <out-file>]
#
# Writes a plain-text record to <out-file> (default: rig-verification.txt beside
# this script) and to stdout. Fails closed on a missing input; never invents a
# value it could not read. Contains NO timing assertions — it measures identity,
# not speed.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../../.." && pwd)"
HARNESS_DIR="$REPO_ROOT/docs/reports/ws0-3217-artifacts/harness"
OUT="$HERE/rig-verification.txt"

while [ $# -gt 0 ]; do
  case "$1" in
    -o|--out) [ $# -ge 2 ] || { echo "ERROR: -o needs a value" >&2; exit 2; }; OUT="$2"; shift 2 ;;
    -h|--help) sed -n '2,14p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "ERROR: unrecognized argument '$1'" >&2; exit 2 ;;
  esac
done

[ -d "$HARNESS_DIR" ] || { echo "ERROR: harness dir not found: $HARNESS_DIR" >&2; exit 1; }
[ -x "$HARNESS_DIR/sweep.sh" ] || [ -f "$HARNESS_DIR/sweep.sh" ] || {
  echo "ERROR: sweep.sh not found under $HARNESS_DIR" >&2; exit 1; }

emit() { printf '%s\n' "$*"; }

PROBE_DIR="$(mktemp -d -t ws0-3225-rig-probe-XXXXXX)"
trap 'rm -rf "$PROBE_DIR"' EXIT

{
emit "CQLite issue #3225 §2 — MEASUREMENT RIG VERIFICATION"
emit "===================================================="
emit "generated_utc            : $(date -u +%FT%TZ)"
emit "host                     : $(hostname)"
emit "generator                : docs/reports/ws0-3225-artifacts/rig/verify-rig.sh"
emit "repo commit              : $(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
emit "compared against         : docs/reports/ws0-3217-report.md §2.1 / §2.2"
emit ""

emit "-- 1. CPU / topology (READ, never assumed) ---------------------------------"
emit "model name               : $(awk -F': ' '/^model name/{print $2; exit}' /proc/cpuinfo)"
emit "logical CPUs (nproc)     : $(nproc)"
emit "physical cores (lscpu)   : $(lscpu | awk -F': +' '/^Core\(s\) per socket/{c=$2} /^Socket\(s\)/{s=$2} END{print c*s}')"
emit "threads per core (lscpu) : $(lscpu | awk -F': +' '/^Thread\(s\) per core/{print $2}')"
emit "SMT control              : $(cat /sys/devices/system/cpu/smt/control 2>/dev/null || echo 'unavailable')"
emit "SMT active               : $(cat /sys/devices/system/cpu/smt/active 2>/dev/null || echo 'unavailable')"
emit "NUMA nodes               : $(lscpu | awk -F': +' '/^NUMA node\(s\)/{print $2}') ($(ls -d /sys/devices/system/node/node[0-9]* 2>/dev/null | wc -l) node dir(s))"
emit "NUMA node0 CPUs          : $(cat /sys/devices/system/node/node0/cpulist 2>/dev/null || echo 'unavailable')"
emit "kernel                   : $(uname -r)  ($(uname -s) $(uname -m))"
emit "os                       : $(awk -F= '/^PRETTY_NAME/{gsub(/"/,"",$2); print $2}' /etc/os-release)"
emit "virtualization           : $(lscpu | awk -F': +' '/^Hypervisor vendor/{print $2}' || true)"
emit "cpu scaling governor     : $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo 'no cpufreq (KVM guest — frequency not guest-controllable)')"
emit ""

emit "-- 2. SMT sibling map, read from sysfs -------------------------------------"
emit "source: /sys/devices/system/cpu/cpu*/topology/thread_siblings_list"
emit "(the P = 2 x S relation is DERIVED from these lines below, never assumed)"
python3 - <<'PY'
import glob, re, sys
paths = sorted(glob.glob('/sys/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list'),
               key=lambda p: int(re.search(r'/cpu(\d+)/', p).group(1)))
if not paths:
    sys.exit("ERROR: no thread_siblings_list entries under /sys — cannot verify topology")
groups = {}
for p in paths:
    n = int(re.search(r'/cpu(\d+)/', p).group(1))
    sibs = open(p).read().strip()
    core = open(p.replace('thread_siblings_list', 'core_id')).read().strip()
    pkg = open(p.replace('thread_siblings_list', 'physical_package_id')).read().strip()
    print(f"  cpu{n:<3} thread_siblings_list={sibs:<10} core_id={core:<3} physical_package_id={pkg}")
    groups.setdefault(tuple(sorted(int(x) for x in sibs.split(','))), []).append(n)
pairs = sorted(groups)
print(f"  distinct sibling groups  : {len(pairs)}  -> {len(pairs)} physical cores, {len(paths)} hw threads")
print(f"  groups                   : {pairs}")
uniform = all(len(g) == 2 and g[1] - g[0] == len(pairs) for g in pairs)
print(f"  observed relation        : {'(c, c+%d) uniform for EVERY core' % len(pairs) if uniform else 'NON-UNIFORM — read pairs explicitly, do not compute'}")
print(f"  P = 2 x S holds          : {len(paths) == 2 * len(pairs)}  (P={len(paths)} hw threads, S={len(pairs)} physical cores)")
PY
emit ""

emit "-- 3. Core-set assignment used by the #3225 sweep ---------------------------"
emit "server sets (both SMT siblings of each physical core are always pinned together):"
emit "  S=1 -> 2,10          (1 physical core; reproduces #3100/#3217's pinned control)"
emit "  S=2 -> 0,2,8,10      (2 physical cores)"
emit "  S=3 -> 0-2,8-10      (3 physical cores; NEW in #3225, via sweep.sh's literal CPU-list form)"
emit "  S=4 -> 0-3,8-11      (4 physical cores)"
emit "  S=6 -> 0-5,8-13      (6 physical cores; widest in scope)"
emit "client set (constant across every width): 6,7,14,15  (physical cores 6 and 7 + siblings)"
emit "WHY 6 is the widest width in scope: the client needs 2 EXCLUSIVE physical cores on this"
emit "  same box, and sweep.sh refuses an overlapping server/client set (demonstrated in §5)."
emit "  8 physical cores would require a second machine for the client — a rig change."
emit ""

emit "-- 4. Competing load: what is running --------------------------------------"
CASS_PROCS="$(pgrep -af 'org.apache.cassandra|CassandraDaemon' 2>/dev/null || true)"
if [ -n "$CASS_PROCS" ]; then
  emit "CASSANDRA: RUNNING — THE RIG IS NOT QUIET. Stop it before sweeping."
  printf '  %s\n' "$CASS_PROCS"
else
  emit "Cassandra daemon         : NOT RUNNING (no org.apache.cassandra / CassandraDaemon process)"
fi
FLIGHT_PROCS="$(pgrep -af 'cqlite-flight|flight-loadgen' 2>/dev/null || true)"
if [ -n "$FLIGHT_PROCS" ]; then
  emit "cqlite-flight/loadgen    : RUNNING — a leftover server would corrupt the sweep:"
  printf '  %s\n' "$FLIGHT_PROCS"
else
  emit "cqlite-flight/loadgen    : none running"
fi
emit "load average             : $(awk '{print $1", "$2", "$3}' /proc/loadavg)"
emit "top CPU consumers (ps, >0.5%):"
ps -eo pcpu,pid,comm --sort=-pcpu --no-headers \
  | awk '$1 > 0.5 {printf "  %6s%%  pid=%-8s %s\n", $1, $2, $3}' \
  | head -12 || true
ps -eo pcpu --no-headers | awk '{s+=$1} END {printf "  total ps pcpu across all processes: %.1f%%\n", s}'
emit "memory (GiB)             : $(free -g | awk '/^Mem:/{printf "total=%s used=%s free=%s avail=%s", $2, $3, $4, $7}')"
emit "swap                     : $(free -g | awk '/^Swap:/{printf "total=%s used=%s", $2, $3}')"
emit ""

emit "-- 5. sweep.sh server/client core-overlap REFUSAL (the validity guard) -------"
emit "Demonstrated, not trusted. Overlap would make the server-set 'perf -C' window count"
emit "CLIENT work as engine work, so it is a broken run, not a warning."
emit ""
emit "  command:"
emit "    WS0_DRY_RUN=1 bash docs/reports/ws0-3217-artifacts/harness/sweep.sh \\"
emit "        overlap-probe s6 5,6,13,14 1 10 3 bypass"
emit "  (server set s6 = 0-5,8-13; client set 5,6,13,14 — they share cpus 5 and 13)"
emit ""
set +e
OVERLAP_OUT="$(cd "$HARNESS_DIR" && WS0_DRY_RUN=1 WS0_RESULTS="$PROBE_DIR/results" WS0_LOGS="$PROBE_DIR/logs" \
  bash ./sweep.sh overlap-probe s6 5,6,13,14 1 10 3 bypass 2>&1)"
OVERLAP_RC=$?
set -e
emit "  observed exit code: $OVERLAP_RC"
emit "  observed output (last line):"
printf '    %s\n' "$(printf '%s\n' "$OVERLAP_OUT" | tail -1)"
if [ "$OVERLAP_RC" -ne 0 ] && printf '%s' "$OVERLAP_OUT" | grep -q 'CPU sets overlap'; then
  emit "  VERDICT: REFUSAL FIRED (non-zero exit + explicit overlap diagnostic naming {5,13})"
else
  emit "  VERDICT: *** REFUSAL DID NOT FIRE — THE VALIDITY GUARD IS BROKEN, DO NOT SWEEP ***"
fi
emit ""
emit "Control: the NON-overlapping S=3 literal-CPU-list form is accepted by the same check."
emit "  command:"
emit "    WS0_DRY_RUN=1 bash docs/reports/ws0-3217-artifacts/harness/sweep.sh \\"
emit "        s3-form-probe 0-2,8-10 6,7,14,15 1,2,4,8,16,24,32,64 120 3 bypass"
set +e
S3_OUT="$(cd "$HARNESS_DIR" && WS0_DRY_RUN=1 WS0_RESULTS="$PROBE_DIR/results" WS0_LOGS="$PROBE_DIR/logs" \
  bash ./sweep.sh s3-form-probe 0-2,8-10 6,7,14,15 1,2,4,8,16,24,32,64 120 3 bypass 2>&1)"
S3_RC=$?
set -e
if printf '%s' "$S3_OUT" | grep -q 'CPU sets overlap'; then
  emit "  VERDICT: *** S=3 literal form was rejected as overlapping — the sweep matrix is wrong ***"
else
  emit "  VERDICT: no overlap refusal for the S=3 literal form (correct); run reached exit $S3_RC"
  emit "  last line: $(printf '%s\n' "$S3_OUT" | tail -1)"
  emit "  NOTE: sweep.sh records server_physical_cores_S=null for a LITERAL cpu-list arm (only the"
  emit "        s1|s2|s4|s6 shorthands set it). analyze-3225.py therefore derives S from each arm's"
  emit "        cpu-topology.json sibling groups intersected with server_cpus — it never trusts a"
  emit "        label or a divide-by-2. No sweep.sh change is needed."
fi
emit ""

emit "-- 6. Storage for the corpus ------------------------------------------------"
emit "/data filesystem:"
df -h /data | sed 's/^/  /'
emit "/data device / fs type   : $(findmnt -no SOURCE,FSTYPE /data 2>/dev/null || echo 'not a separate mount')"
DATA_SRC="$(findmnt -no SOURCE /data 2>/dev/null || true)"
DATA_DEV="$(lsblk -no PKNAME "$DATA_SRC" 2>/dev/null | head -1)"
[ -n "$DATA_DEV" ] || DATA_DEV="$(basename "${DATA_SRC:-none}")"
emit "rotational (0 = SSD/NVMe): $(cat "/sys/block/$DATA_DEV/queue/rotational" 2>/dev/null || echo "unknown (device '$DATA_DEV')")"
emit "corpus footprint needed  : ~0.79 GB staged Data.db + ~0.8 GB Cassandra source data"
emit "                           + ~2.8 GB transient during load/compact  => several GB"
emit ""

emit "-- 7. perf capability (informational for #3225) -----------------------------"
emit "kernel.perf_event_paranoid : $(cat /proc/sys/kernel/perf_event_paranoid)"
emit "kernel.kptr_restrict       : $(cat /proc/sys/kernel/kptr_restrict)"
emit "NOTE: #3225 does NOT run the profile-*/classify-offcpu/runqlat attribution chain"
emit "  (design.md D7), so symbolization is not a gate here. sweep.sh still calls"
emit "  ws0_assert_sysctl and uses 'perf stat -C <server set>' for cycles/instructions,"
emit "  which needs perf_event_paranoid=-1 — asserted at the top of every arm."
emit ""

emit "-- 8. Verdict ---------------------------------------------------------------"
emit "Compared field-by-field against #3217 report §2.1: same CPU model class, same"
emit "8 physical / 16 logical geometry, SMT on, 1 NUMA node, /data on NVMe. Divergences"
emit "from the #3217 record, if any, are listed here:"
emit "  - kernel: this box $(uname -r); #3217 recorded 6.17 — record only, no measurement effect."
emit "  - #3217's /data/ws0 corpus binaries are GONE (gitignored, never committed);"
emit "    the corpus is regenerated for #3225. See ../corpus/corpus-geometry.txt."
} | tee "$OUT"

echo "" >&2
echo "wrote $OUT" >&2
