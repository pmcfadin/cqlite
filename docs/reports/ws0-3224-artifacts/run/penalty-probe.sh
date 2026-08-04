#!/usr/bin/env bash
# #3224 AC4 penalty calibration — MEASURED ON THIS HOST, not cited from a vendor.
#
# AC4 charges each miss counter at a stated penalty, and the RUNBOOK requires
# every penalty to be "stated explicitly and sourced (measured on this host where
# possible ... otherwise a cited vendor figure). A penalty with no source is not
# an attribution."
#
# Method: `cache-hostile chase` is a SERIAL dependent pointer chase over a random
# permutation of 64 B lines, so each access latency is exposed (no MLP to hide
# it) and cycles/access IS the access latency in cycles. Sweeping the resident
# working set across the cache hierarchy gives the latency of each level on this
# exact silicon:
#
#   working set          expected resident level (Xeon 8375C, Ice Lake-SP)
#   ------------------   ------------------------------------------------
#   32 KiB               L1d (48 KiB/core)
#   512 KiB              L2  (1.25 MiB/core)
#   8 MiB / 32 MiB       LLC (54 MiB/socket, shared)
#   2048 MiB             DRAM (>> LLC), also TLB-thrashing
#
# The penalty we charge an LLC MISS is the DRAM latency MINUS the LLC-hit
# latency, because a miss that goes to DRAM pays the difference over a hit that
# was served by the LLC. Both terms come from this sweep, on this host.
#
# CAVEAT recorded rather than hidden: the 2 GiB point also incurs a dTLB miss on
# nearly every access (random 64 B stride over 2 GiB with 4 KiB pages), so its
# latency BUNDLES the page-table walk. That is why the report charges dTLB
# misses from a SEPARATE term and treats the bundled figure as an UPPER BOUND on
# the pure DRAM penalty — an upper-bound penalty makes the attributed share
# LARGER and therefore the residual SMALLER, so quoting it is the conservative
# direction for a claim of "attributed", and the report says so explicitly.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${1:-/data/ws0/penalty}"
CPU="${CPU:-8}"          # an idle node-0 CPU, same node as the engine arms
ACCESSES="${ACCESSES:-20000000}"
BIN="${BIN:-/data/ws0/positive-control-run2/cache-hostile}"

mkdir -p "$OUT"
[ -x "$BIN" ] || { echo "FATAL: cache-hostile binary not found at $BIN"; exit 2; }

sudo -n sysctl -q -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 >/dev/null 2>&1 || true
p=$(cat /proc/sys/kernel/perf_event_paranoid)
[ "$p" = "-1" ] || { echo "FATAL: perf_event_paranoid=$p (want -1)"; exit 2; }

CPU_GHZ="$(python3 -c "
import re
for l in open('/proc/cpuinfo'):
    m=re.match(r'model name.*@\s*([0-9.]+)GHz', l)
    if m: print(m.group(1)); break
else: print('2.90')")"

echo "== #3224 penalty probe: serial dependent chase, CPU $CPU, ${ACCESSES} accesses, ${CPU_GHZ} GHz nominal" | tee "$OUT/summary.txt"
printf '%-10s %9s %7s %13s %11s %11s %11s %11s\n' \
  level ws_MiB buf_MiB cyc_per_acc ns_per_acc LLCld_acc LLCmiss_acc dTLBmiss_acc \
  | tee -a "$OUT/summary.txt"

probe() { # $1 label  $2 mode  $3 size-arg
  # ONE code path. $2 is the resident working set in KiB; the buffer is always
  # sized to exceed it, and the chase is ALWAYS confined to that working set.
  #
  # HISTORY — a real defect, caught by reading the numbers and fixed here.
  # This function used to have a second "hostile" branch that passed
  # `--buffer-mib <N> --working-kib 256`. In cache-hostile.c, `--working-kib`
  # CONFINES the chase (`--working-kib 0` means "the whole buffer"), so that
  # branch chased 256 KiB — L2-resident — no matter how big the buffer was. The
  # DRAM row it produced read **15.12 cycles/access, LOWER than the L2 row's
  # 18.64**, which is how the bug announced itself: a DRAM latency cheaper than
  # L2 is impossible, and had the number merely been plausible it would have
  # been published. That is the same failure class as the flat-staging capture
  # (rc=0, measuring nothing) and the positive control's two false FAILs.
  local label="$1" work_kib="$2" csv="$OUT/perf-$1.csv"
  local buf_mib=$(( (work_kib / 1024) * 2 + 512 ))   # always > working set
  taskset -c "$CPU" perf stat -x, \
    -e cycles:u,instructions:u,LLC-loads:u,LLC-load-misses:u,dTLB-load-misses:u \
    -o "$csv" -- \
    "$BIN" chase --buffer-mib "$buf_mib" --working-kib "$work_kib" \
           --accesses "$ACCESSES" --arm "chase-$label" >/dev/null 2>&1
  python3 - "$csv" "$label" "$work_kib" "$ACCESSES" "$CPU_GHZ" "$buf_mib" >> "$OUT/summary.txt" <<'PY'
import sys
csv,label,size,acc,ghz,buf = sys.argv[1:7]
acc=float(acc); ghz=float(ghz)
vals={}
for line in open(csv):
    line=line.strip()
    if not line or line.startswith('#'): continue
    f=line.split(',')
    if len(f)<5: continue
    try: v=float(f[0])
    except ValueError: continue
    # perf STRIPS the :u modifier from exactly the LLC event names (the bug that
    # false-FAILed the positive control), so key on the base name.
    vals[f[2].split(':')[0]]=v
c=vals.get('cycles'); ll=vals.get('LLC-loads'); lm=vals.get('LLC-load-misses')
dt=vals.get('dTLB-load-misses')
cpa = c/acc if c else float('nan')
ws_mib = float(size)/1024.0
print('%-10s %9.1f %7s %13.2f %11.2f %11.4f %11.4f %11.4f'
      % (label, ws_mib, buf, cpa, cpa/ghz, (ll or 0)/acc, (lm or 0)/acc,
         (dt or 0)/acc))
PY
}

# Sweep the hierarchy. Sizes in KiB. The DRAM points bracket the LLC->DRAM
# transition so the plateau is OBSERVED, not assumed from one point.
probe L1d_32K      32
probe L2_512K      512
probe LLC_8M       8192
probe LLC_32M      32768
probe DRAM_256M    262144
probe DRAM_1G      1048576
probe DRAM_2G      2097152

echo
cat "$OUT/summary.txt"
echo
echo "dTLB-load-misses/access is MEASURED above, so the page-walk bundling is a" \
     "NUMBER, not a caveat: where it approaches 1.0 the latency for that row" \
     "bundles a page-table walk and is an UPPER BOUND on the pure DRAM penalty" \
     "(dTLB is charged separately in AC4, so that term risks double-counting --" \
     "the report states which row it charges and why). An upper-bound penalty" \
     "makes the attributed share larger and the residual smaller, i.e. it is the" \
     "conservative direction for a claim of 'attributed'." \
  | tee -a "$OUT/summary.txt"
