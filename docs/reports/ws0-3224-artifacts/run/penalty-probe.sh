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
# the pure DRAM penalty.
#
# DIRECTION OF CONSERVATISM — this comment used to state it BACKWARDS, and the
# correction is worth keeping visible. It claimed an upper-bound penalty was "the
# conservative direction for a claim of attributed" because it makes the residual
# smaller. That is exactly wrong: a LARGER attributed share FLATTERS the
# hypothesis that the decay is explained, so an inflated penalty is the
# ANTI-conservative direction. Report §5.4 measures the consequence — the
# zero-MLP charge accounts for >100% of a delta it is meant to explain part of —
# and derive.py's route-2 block states the rule correctly. Which is why the
# MEASURED stall counter, not this table, is the headline attribution.
#
# ------------------------------------------------------------------------------
# WINDOW GATING — the fix for roborev finding #2 (PR #3286), and the reason this
# script's earlier output must not be used to derive a penalty.
#
# This probe used to invoke perf as a plain wrapper, with neither `-D` nor a
# control FIFO, while cache-hostile defaults to `delay_s = 10.0` and calls
# wait_for_window(). perf therefore counted FROM PROCESS START, so the
# identity-fill + Sattolo permutation build was inside the measured interval. That
# build walks the WORKING SET, so it added ~29 instructions PER NODE — measured at
# 28.91/28.99/29.01/29.01/29.02 across LLC_8M..DRAM_2G, i.e. constant across five
# orders of magnitude, which is what identifies it. cycles/access then varied with
# working set for a reason that had nothing to do with access latency, which is
# the one quantity this probe exists to isolate.
#
# The nanosleep in wait_for_window() is NOT the contaminant: `cycles:u` counts no
# user cycles while the process is descheduled. The contaminant is init WORK.
#
# Fixed by gating the window exactly around the chase with perf's control FIFO
# (`-D -1 --control fifo:<ctl>,<ack>`, cache-hostile writes enable/disable), which
# is what positive-control.sh has always done and is strictly better than a `-D`
# delay: it excludes exit-time address-space teardown as well as init, and both are
# working-set-dependent. run/penalty-window-check.py then VERIFIES the gate held,
# per row, from the artefacts — so a silently-failed handshake cannot publish a
# latency either.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../harness/guards.sh
source "$HERE/../harness/guards.sh"
OUT="${1:-/data/ws0/penalty}"
CPU="${CPU:-8}"          # an idle node-0 CPU, same node as the engine arms
ACCESSES="${ACCESSES:-20000000}"
BIN="${BIN:-/data/ws0/positive-control-run2/cache-hostile}"
CTL="$OUT/perf-ctl.fifo"; ACK="$OUT/perf-ack.fifo"

mkdir -p "$OUT"
[ -x "$BIN" ] || { echo "FATAL: cache-hostile binary not found at $BIN"; exit 2; }
trap 'rm -f "$CTL" "$ACK"' EXIT INT TERM

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
  # The window is gated to the chase by the control FIFO, NOT by perf's default
  # "count the whole process". See the header block: without this, init is counted.
  rm -f "$CTL" "$ACK"; mkfifo "$CTL" "$ACK" \
    || { echo "FATAL: cannot create control FIFOs under $OUT" >&2; exit 2; }
  taskset -c "$CPU" perf stat -x, \
    -e cycles:u,instructions:u,LLC-loads:u,LLC-load-misses:u,dTLB-load-misses:u \
    -D -1 --control "fifo:$CTL,$ACK" \
    -o "$csv" -- \
    "$BIN" chase --buffer-mib "$buf_mib" --working-kib "$work_kib" \
           --accesses "$ACCESSES" --arm "chase-$label" \
           --ctl-fifo "$CTL" --ack-fifo "$ACK" > "$OUT/run-$label.log" 2>&1
  local rc=$?   # captured IMMEDIATELY, before any command substitution
  # A failed probe used to be silently ignored (stdout and stderr both went to
  # /dev/null and rc was never read), so a partial count could still be published
  # as a latency. cache-hostile exits 4 on init_overrun and dies on a failed FIFO
  # handshake; both must stop the sweep, not decorate it.
  ws0_guard_rc "penalty probe row '$label' (working set ${work_kib} KiB)" "$rc" \
    "See $OUT/run-$label.log. A row whose measured section did not run cannot yield an access latency." \
    || exit 1
  python3 - "$csv" "$label" "$work_kib" "$ACCESSES" "$CPU_GHZ" "$buf_mib" >> "$OUT/summary.txt" <<'PY'
import math, sys
csv,label,size,acc,ghz,buf = sys.argv[1:7]
acc=float(acc); ghz=float(ghz)

# EVERY REQUIRED EVENT MUST BE PRESENT, NUMERIC, FINITE AND UNMULTIPLEXED BEFORE A
# ROW IS EMITTED (roborev round 2 finding #4).
#
# This parser used to reach for each counter with `vals.get(...)` and paper over the
# result: a missing or `<not supported>` `cycles` became `None` and then
# `cpa = nan`, printed into the summary table as a latency; a missing LLC or dTLB
# counter became `(ll or 0)/acc`, i.e. a confident 0.0000 per access. And the
# enabled percentage was never read at all, so a multiplexed estimate was
# indistinguishable from a count. run/penalty-window-check.py validates only
# `instructions`, so nothing else in the pipeline would have caught it either — the
# script could exit 0 with an unusable cycles or miss measurement in the published
# table.
#
# `(x or 0)` is worth naming as its own hazard: it maps BOTH "absent" and "genuinely
# zero" onto 0, so it destroys the very distinction the #3217 silent-instrument
# lesson turns on. A real 0 is a finding; an absent counter is a failure. They must
# not print the same.
REQUIRED = ('cycles', 'instructions', 'LLC-loads', 'LLC-load-misses',
            'dTLB-load-misses')
MUX_MIN = 99.0

vals={}; enabled={}
for line in open(csv):
    line=line.strip()
    if not line or line.startswith('#'): continue
    f=line.split(',')
    if len(f)<5: continue
    # perf STRIPS the :u modifier from exactly the LLC event names (the bug that
    # false-FAILed the positive control), so key on the base name.
    name=f[2].split(':')[0]
    try: v=float(f[0])
    except ValueError:
        # Keep the unreadable token so the diagnosis can name it rather than
        # reporting the event as merely absent.
        vals.setdefault(name, f[0]); continue
    vals[name]=v
    try: enabled[name]=float(f[4])
    except ValueError: enabled[name]=None

problems=[]
for name in REQUIRED:
    if name not in vals:
        problems.append('%s ABSENT from the CSV' % name); continue
    v=vals[name]
    if not isinstance(v, float):
        problems.append('%s reads %r (not a number)' % (name, v)); continue
    if not math.isfinite(v):
        problems.append('%s is non-finite (%r)' % (name, v)); continue
    e=enabled.get(name)
    if e is None:
        problems.append('%s has an unreadable enabled%% — an unverifiable count is '
                        'not a usable one' % name); continue
    if e < MUX_MIN:
        problems.append('%s only %.2f%% enabled (floor %.0f%%): a MULTIPLEXED '
                        'ESTIMATE, not a count' % (name, e, MUX_MIN))
if problems:
    sys.exit("FATAL: penalty probe row '%s' (%s KiB working set) cannot be "
             "published:\n  - %s\nRemedy: split the event group so nothing "
             "multiplexes, and confirm this host programs all five events "
             "(positive-control.sh's event probe reports which)."
             % (label, size, '\n  - '.join(problems)))

c=vals['cycles']; ll=vals['LLC-loads']; lm=vals['LLC-load-misses']
dt=vals['dTLB-load-misses']
if c <= 0:
    sys.exit("FATAL: penalty probe row '%s' counted %r cycles. A chase of %d "
             "accesses cannot take zero cycles; this is a failed capture, not a "
             "measurement of zero." % (label, c, int(acc)))
cpa = c/acc
ws_mib = float(size)/1024.0
print('%-10s %9.1f %7s %13.2f %11.2f %11.4f %11.4f %11.4f'
      % (label, ws_mib, buf, cpa, cpa/ghz, ll/acc, lm/acc, dt/acc))
PY
  # The parser above now REFUSES a row it cannot certify, so its exit code has to be
  # read. Left unchecked it would be the same fail-open one layer down: the row simply
  # would not appear in the table while the sweep carried on to the next level, and a
  # penalty would be computed from whichever rows happened to survive.
  local prc=$?
  ws0_guard_rc "penalty probe row '$label' counter validation" "$prc" \
    "See $csv. Every required counter must be present, finite and >= 99% enabled before a latency is published." \
    || exit 1
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

# VERIFY the gate held, per row, from the artefacts just written. A FIFO handshake
# that silently failed would leave the window ungated and produce plausible,
# wrong latencies — the "plausible output from a broken instrument" class this
# whole issue indicts #3217 for. Fail closed: no summary may be presented as a
# penalty table unless this passes.
python3 "$HERE/penalty-window-check.py" "$OUT" "$ACCESSES" | tee -a "$OUT/summary.txt"
WRC=${PIPESTATUS[0]}
ws0_guard_rc "penalty-probe window-gate check" "$WRC" \
  "cycles/access in $OUT are NOT access latencies; do not derive a penalty from them." \
  || exit 1

echo
cat "$OUT/summary.txt"
echo
echo "dTLB-load-misses/access is MEASURED above, so the page-walk bundling is a" \
     "NUMBER, not a caveat: where it approaches 1.0 the latency for that row" \
     "bundles a page-table walk and is an UPPER BOUND on the pure DRAM penalty" \
     "(dTLB is charged separately in AC4, so that term risks double-counting --" \
     "the report states which row it charges and why). An upper-bound penalty" \
     "makes the attributed share LARGER and the residual SMALLER, which FLATTERS" \
     "the hypothesis: it is the ANTI-conservative direction, not the conservative" \
     "one. This table is therefore a CROSS-CHECK; the headline attribution is the" \
     "measured cycle_activity.stalls_l3_miss delta." \
  | tee -a "$OUT/summary.txt"
