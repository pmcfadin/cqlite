#!/usr/bin/env bash
# Cold-vs-warm A/B for issue #2824 (madvise(WILLNEED) on the Auto-mmap scan plane).
#
# WHAT THIS MEASURES, AND WHAT IT DOES NOT
# ----------------------------------------
# Runs `ws0-scan-bench` over a fixed corpus twice per labelled binary per round:
# a FLOOR run (`--setup-only`, cold), a COLD single-pass run (page cache dropped
# immediately before it) and a WARM single-pass run (cache left resident). Each phase is timed by its OWN
# `/usr/bin/time`, so every recorded number belongs to exactly one phase.
# Two binaries -> a baseline-vs-patched A/B.
#
# It does NOT produce issue #2824's acceptance-criterion number. Four limits, all
# recorded into the output so a reader of the artifact alone cannot miss them:
#
#   1. AC1 asks for cold-p99 on a cold **i4i** scan. This script measures whatever
#      host it runs on and records the instance type. On anything other than an
#      i4i the i4i magnitude is UNMEASURED.
#   2. `ws0-scan-bench` reports **whole-scan wall seconds** per pass, not a
#      within-scan latency distribution. There is no p99 to be had from it; what
#      is delivered is cold whole-scan wall time and its spread across rounds.
#   3. The ws0.events fixture is **uncompressed** (#1406), so it does not exercise
#      the compressed-chunk read path a field scan uses.
#   4. Wall clock is contended on a shared box. Arms are therefore ALTERNATED
#      within each round and the order is FLIPPED on even rounds, so a monotonic
#      drift in background load cannot be attributed to one arm.
#
# The fixture is CQLite-written and CQLite-read: a PERFORMANCE fixture only, never
# a correctness oracle (#3042).
#
# The load-bearing signal here is not wall clock but **major page faults** (`%F`
# from `/usr/bin/time`): MADV_WILLNEED's whole mechanism is converting synchronous
# major faults on the reading thread into kernel-initiated async read-ahead, so a
# real effect must show up as a major-fault reduction on the cold read.
#
# `%F` is per-process, so it is immune to the neighbours — but it is NOT isolated
# to the scan mapping. It counts every major fault the process takes, including
# faulting in its own executable and shared libraries, and because the global page
# cache is dropped first those are cold too. Reporting it unqualified would
# attribute process-startup faults to the scan.
#
# So each arm also runs a **FLOOR** phase: the same binary, cold, with
# `--setup-only` — it opens the reader and reads the index/summary but performs no
# scan. Its fault count is the non-scan cost of starting this process on a cold
# cache, and `scan_major_faults = cold - floor` is the scan-attributable figure.
# That is an ESTIMATE, not true per-mapping accounting (`/usr/bin/time` cannot do
# per-mapping), and the raw `major_faults` column is kept beside it so the
# subtraction is always visible rather than baked in.
#
# TWO ENVIRONMENT CONTROLS, both load-bearing:
#
# * `CQLITE_DISK_ACCESS_MODE=mmap` and `CQLITE_PREFETCH` are pinned for every phase.
#   Both are read from the environment by the reader (`prefetch_mode_via_env`) and
#   OVERRIDE the compiled default, so an inherited value would give both arms the
#   same explicit policy — an A/B that runs cleanly and compares nothing. Pinning
#   the access mode also stops the backend resolving to direct I/O and bypassing
#   the mmap path under test entirely. Both are recorded in `host.txt`.
#
# * The FLOOR phase runs at `CQLITE_PREFETCH=off`, deliberately, and this is a
#   correctness fix rather than a detail. At `auto` the patched binary issues
#   `MADV_WILLNEED` over the whole file during the floor run; that read-ahead is
#   ASYNCHRONOUS and outlives the process (it fills the file's page cache, which is
#   not process-scoped), while `sync` does not wait for reads and `drop_caches`
#   skips pages still under I/O. The floor would therefore pre-warm the very cold
#   phase it precedes, and only for the patched arm. At `off` neither binary issues
#   any advice, so the floor performs only synchronous index/summary reads that
#   complete before it exits and are cleanly dropped.
#
#   THE COST OF THAT CHOICE, STATED RATHER THAN GLOSSED: the floor is measured at
#   `off` and the cold phase runs at `auto`, so
#       scan_major_faults = cold(auto) - floor(off)
#                         = scan(auto) + [setup(auto) - setup(off)]
#   and the bracketed residual is NOT zero for an arm whose `auto` setup issues
#   advice — this harness's own advice census shows exactly that, `WILLNEED=1` on a
#   `--setup-only` run at `auto`. The residual is arm-asymmetric and of the same
#   order as the difference being measured, so `scan_major_faults` bounds the scan
#   cost, it does not resolve it. An earlier revision of this comment claimed the
#   `off` floor "makes the floor arm-independent"; that was contradicted by the
#   census in this same file and is withdrawn. Treat a small between-arm difference
#   in this column as unresolved, never as a signal.
#
# Requires: passwordless sudo for /proc/sys/vm/drop_caches (checked, fail-closed).
set -euo pipefail
# GNU `time` renders %e per the locale; a decimal comma would shift every CSV column
# after it. Pin before any measurement is taken or parsed.
export LC_ALL=C

CORPUS=""; ROUNDS=4; OUT=""
declare -a BINARIES=()

usage() {
  cat <<'USAGE'
usage: cold-warm-ab.sh --corpus <dir> --out <dir> --bin <label>=<path> --bin <label>=<path>
                       [--rounds N]

  --corpus   corpus directory (see tools/ws0-corpus-gen/README.md)
  --out      directory for the recorded artifacts (created)
  --bin      a labelled ws0-scan-bench binary; give it twice for an A/B
  --rounds   alternating rounds; must be EVEN and >= 2 (default 4)
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --corpus) CORPUS="${2:?--corpus needs a value}"; shift 2 ;;
    --out)    OUT="${2:?--out needs a value}"; shift 2 ;;
    --bin)    BINARIES+=("${2:?--bin needs label=path}"); shift 2 ;;
    --rounds) ROUNDS="${2:?--rounds needs a value}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "cold-warm-ab: unrecognized argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[ -n "$CORPUS" ] || { echo "cold-warm-ab: --corpus is required" >&2; exit 2; }
[ -n "$OUT" ]    || { echo "cold-warm-ab: --out is required" >&2; exit 2; }
# EXACTLY two arms, and an EVEN round count. The drift control here is order
# reversal on even rounds, and that only balances under both conditions:
#   - with an odd round count the first-listed arm takes (R+1)/2 first positions
#     against the other's (R-1)/2, so a monotonic background drift is not cancelled;
#   - with more than two arms, reversing pins every MIDDLE arm to the same position
#     in every round, so those arms get no drift control at all.
# Rejecting is deliberate rather than warning-and-continuing: an unbalanced schedule
# still produces a plausible-looking CSV, and the whole point of alternating is that
# a confound must not be attributable to one arm.
if [ "${#BINARIES[@]}" -ne 2 ]; then
  echo "cold-warm-ab: exactly two --bin arms are required (got ${#BINARIES[@]}); the order-reversal drift control does not balance otherwise" >&2
  exit 2
fi
# A round count that is not a positive integer would complete "successfully" having
# recorded nothing — a vacuous pass wearing a green exit code.
case "$ROUNDS" in
  ''|*[!0-9]*) echo "cold-warm-ab: --rounds must be a positive integer, got '$ROUNDS'" >&2; exit 2 ;;
esac
ROUNDS=$((10#$ROUNDS))   # "08" is digit-only but octal to $(( )); normalise before arithmetic
[ "$ROUNDS" -ge 2 ] || { echo "cold-warm-ab: --rounds must be at least 2, got '$ROUNDS'" >&2; exit 2; }
if [ $(( ROUNDS % 2 )) -ne 0 ]; then
  echo "cold-warm-ab: --rounds must be EVEN so each arm runs first equally often, got '$ROUNDS'" >&2
  exit 2
fi
[ -d "$CORPUS" ] || { echo "cold-warm-ab: corpus not a directory: $CORPUS" >&2; exit 2; }
[ -x /usr/bin/time ] || { echo "cold-warm-ab: /usr/bin/time is required for major-fault counts" >&2; exit 3; }
command -v python3 >/dev/null 2>&1 || { echo "cold-warm-ab: python3 is required to validate each phase's JSON" >&2; exit 3; }

# The label is interpolated into BOTH a filename and a CSV field, so it is
# constrained to a charset safe for both, and required unique. Unvalidated:
# a duplicate silently overwrites another arm's artifacts and makes its CSV rows
# indistinguishable, a comma corrupts the CSV, and a slash writes outside --out.
_seen_labels=""; _seen_digests=""
for entry in "${BINARIES[@]}"; do
  label="${entry%%=*}"; path="${entry#*=}"
  if [ "$label" = "$entry" ] || [ -z "$label" ] || [ -z "$path" ]; then
    echo "cold-warm-ab: --bin needs label=path, got: $entry" >&2; exit 2
  fi
  case "$label" in
    *[!A-Za-z0-9._-]*) echo "cold-warm-ab: --bin label must match [A-Za-z0-9._-]+, got '$label'" >&2; exit 2 ;;
    .|..)              echo "cold-warm-ab: --bin label '$label' is not a usable filename" >&2; exit 2 ;;
  esac
  case " $_seen_labels " in
    *" $label "*) echo "cold-warm-ab: duplicate --bin label '$label'; labels must be unique" >&2; exit 2 ;;
  esac
  _seen_labels="$_seen_labels $label"
  [ -x "$path" ] || { echo "cold-warm-ab: not executable: $path" >&2; exit 2; }
  _d=$(sha256sum "$path" | cut -d' ' -f1)
  case " $_seen_digests " in
    *" $_d "*) echo "cold-warm-ab: both --bin arms are the SAME binary (sha256 $_d); that A/B compares nothing" >&2; exit 2 ;;
  esac
  _seen_digests="$_seen_digests $_d"
done

# A reused directory keeps stale per-phase artifacts from a previous run beside
# freshly-truncated aggregate files — an artifact set that looks complete and is
# internally inconsistent. Refuse rather than silently mix generations.
if [ -e "$OUT" ]; then
  if [ ! -d "$OUT" ]; then
    echo "cold-warm-ab: --out exists and is not a directory: $OUT" >&2; exit 2
  fi
  if [ -n "$(ls -A "$OUT" 2>/dev/null)" ]; then
    echo "cold-warm-ab: --out is not empty: $OUT" >&2
    echo "cold-warm-ab: refusing to mix artifact generations; remove it or pass a fresh path." >&2
    exit 2
  fi
fi
# Fail CLOSED on drop_caches: a "cold" arm that silently ran warm is worse than no
# measurement, because in the output it is indistinguishable from a real one.
#
# Placed AFTER all static validation. This probe drops the SYSTEM-WIDE page cache,
# which on a shared box costs every other workload on it — so a rejectable
# invocation (bad label, duplicate binary, non-empty --out) must be rejected on
# argument inspection alone, before anything global is disturbed.
if ! sudo -n sh -c 'echo 1 > /proc/sys/vm/drop_caches' 2>/dev/null; then
  echo "cold-warm-ab: cannot drop the page cache (passwordless sudo required)." >&2
  echo "cold-warm-ab: refusing to run — a warm run labelled COLD is not a measurement." >&2
  exit 3
fi

mkdir -p "$OUT"

# Device discovery runs BEFORE anything is claimed, because the i4i verdict depends
# on it: an i4i instance whose corpus sits on EBS has NOT measured the i4i criterion.
_dev=$(findmnt -no SOURCE --target "$CORPUS" 2>/dev/null || true)
[ -n "$_dev" ] || _dev=UNKNOWN
# `lsblk -no PKNAME` EXITS 0 AND PRINTS NOTHING for a whole disk (no parent), so a
# `|| basename` fallback never fires and every whole-disk device silently recorded
# UNKNOWN — an unmeasured value taking the permissive branch. Resolve affirmatively.
_base=$(lsblk -no PKNAME "$_dev" 2>/dev/null | head -1 | tr -d ' ' || true)
[ -n "$_base" ] || _base=$(basename "$_dev")
_model=UNKNOWN; _ra=UNKNOWN
if [ -r "/sys/block/${_base}/device/model" ]; then
  _model=$(tr -s ' ' < "/sys/block/${_base}/device/model" | sed 's/ *$//')
fi
if [ -r "/sys/block/${_base}/queue/read_ahead_kb" ]; then
  _ra=$(cat "/sys/block/${_base}/queue/read_ahead_kb")
fi
# Positive identification only. An unrecognised model is NOT local storage for the
# purposes of this claim — the permissive branch here would be a false MEASURED.
case "$_model" in
  *"Instance Storage"*) _local_store=yes ;;
  UNKNOWN)              _local_store=unknown ;;
  *)                    _local_store=no ;;
esac
IMDS_TOKEN=$(curl -sf -m 3 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token 2>/dev/null || true)
INSTANCE=$(curl -sf -m 3 -H "X-aws-ec2-metadata-token: ${IMDS_TOKEN}" http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || true)
# A garbled IMDS body must not be recorded as an instance type. Only accept the
# shape AWS actually emits; anything else is UNKNOWN, which takes the non-i4i arm.
case "$INSTANCE" in
  *[!a-z0-9.-]*)       INSTANCE=UNKNOWN ;;   # rejects newlines and every other stray byte
  [a-z0-9]*.[a-z0-9]*) : ;;
  *)                   INSTANCE=UNKNOWN ;;
esac

{
  echo "host: $(hostname)"
  echo "instance-type: $INSTANCE"
  # AC1's clause is "a cold i4i scan". That needs an i4i instance AND the corpus on
  # its LOCAL NVMe: an i4i reading from EBS measures EBS. Both are required, and an
  # unidentified device is never read as satisfying either.
  if [ "$_local_store" = yes ] && case "$INSTANCE" in i4i.*) true ;; *) false ;; esac; then
    echo "i4i-magnitude: MEASURED (i4i instance '$INSTANCE' AND corpus on local instance storage: '$_model')"
  elif case "$INSTANCE" in i4i.*) true ;; *) false ;; esac; then
    echo "i4i-magnitude: UNMEASURED (instance is i4i, but the corpus device is '$_model' [local-instance-storage=$_local_store], not positively identified local NVMe) — AC1's i4i clause is NOT satisfied by this run"
  else
    echo "i4i-magnitude: UNMEASURED (this host is '$INSTANCE', not an i4i) — AC1's i4i clause is NOT satisfied by this run"
  fi
  echo "kernel: $(uname -r)"
  echo "cores: $(nproc)"
  echo "mem-total-kb: $(awk '/MemTotal/{print $2}' /proc/meminfo)"
  echo "loadavg-at-start: $(cut -d' ' -f1-3 /proc/loadavg)"
  echo "corpus: $CORPUS"
  echo "corpus-bytes: $(du -sb "$CORPUS" | cut -f1)"
  echo "corpus-device: $_dev"
  echo "corpus-device-base: $_base"
  echo "corpus-device-model: $_model"
  echo "corpus-device-local-instance-storage: $_local_store"
  echo "corpus-device-read_ahead_kb: $_ra"
  if [ "$_model" = UNKNOWN ] || [ "$_ra" = UNKNOWN ]; then
    echo "corpus-device-note: NOT MEASURED — a read-ahead A/B whose device is unknown cannot be interpreted; treat the result as UNATTRIBUTED"
  fi
  echo "phases-per-arm-per-round: floor (--setup-only, cache dropped) + cold (cache dropped, --passes 1) + warm (cache resident, --passes 1), each timed separately"
  echo "rounds: $ROUNDS  (even by requirement; arm order reverses on even rounds, so each arm runs first exactly $((ROUNDS/2)) times)"
  echo "primary-signal: scan-attributable major page faults = cold(major_faults) - floor(major_faults)"
  echo "primary-signal-note: %F is per-process (immune to neighbours) but NOT isolated to the scan mapping; the floor phase estimates the non-scan startup cost. The subtraction is an ESTIMATE, not per-mapping accounting."
  echo "secondary-signal: wall seconds (contended; medians only)"
  echo "limit-p99: UNAVAILABLE — ws0-scan-bench reports whole-scan wall seconds, not a per-operation latency distribution; there is no p99 in this data on any host"
  echo "limit-fixture-compression: none (#1406) — the compressed-chunk read path is NOT exercised"
  echo "limit-attribution-residual: scan_major_faults = cold(auto) - floor(off) carries an arm-asymmetric [setup(auto)-setup(off)] residual of the same order as the difference; it BOUNDS the scan cost, it does not resolve it"
  echo "limit-cross-arm-readahead: the warm phase runs at auto, so an arm that issues whole-file WILLNEED can leave read-ahead in flight past the next drop_caches; see drain-* below"
  echo "env-pinned-floor: CQLITE_PREFETCH=off CQLITE_DISK_ACCESS_MODE=mmap"
  echo "env-pinned-cold-warm: CQLITE_PREFETCH=auto CQLITE_DISK_ACCESS_MODE=mmap"
  echo "env-inherited-cqlite: $(env | grep -E '^CQLITE_' | sort | tr '\n' ' ' | sed 's/ $//')"
  echo "started-utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  for entry in "${BINARIES[@]}"; do
    echo "arm: ${entry%%=*}  sha256=$(sha256sum "${entry#*=}" | cut -d' ' -f1)"
  done
  echo "run: INCOMPLETE (in progress or aborted)"
} | tee "$OUT/host.txt"
# Replaced with COMPLETE at the end; an aborted run therefore leaves a NEGATIVE
# statement rather than well-formed CSVs that look finished.
trap 'sed -i "s/^run: INCOMPLETE .*$/run: ABORTED (exit $?)/" "$OUT/host.txt" 2>/dev/null || true' EXIT

echo "round,arm,phase,wall_secs,max_rss_kb,major_faults,minor_faults" > "$OUT/summary.csv"
echo "round,arm,floor_major_faults,cold_major_faults,scan_major_faults" > "$OUT/scan-attributable.csv"
echo "round,arm,where,drain_state" > "$OUT/drain.csv"

# One `/usr/bin/time` invocation per PHASE, each a single-pass run, so every
# recorded number is attributable to exactly one phase. An earlier version wrapped
# `--passes N` in ONE invocation, which summed cold+warm into a column the header
# called cold — a mislabel that would have propagated into the artifact.
# Bounded drain of in-flight device I/O before dropping the cache.
#
# `sync` flushes writes and does NOT wait for reads, and `drop_caches` skips pages
# still under I/O — so read-ahead issued by a previous phase (or by the other arm's
# warm phase, which runs at `auto`) can land in the page cache AFTER the drop and
# pre-warm a run labelled cold. This polls the device's in-flight counter to zero
# with a bounded timeout and RECORDS the outcome; it never silently assumes it
# drained. On a device shared with other workloads it may legitimately never reach
# zero, which is why the result is reported rather than enforced.
DRAIN_STATE=UNKNOWN
# First observed rows/cells/table-dirs; every later phase must match it exactly.
EXPECTED_SHAPE=""
drain_and_drop() {
  local round="$1" label="$2" where="$3" i=0 inflight
  DRAIN_STATE=NOT-ATTEMPTED
  if [ -r "/sys/block/${_base}/stat" ]; then
    DRAIN_STATE=TIMEOUT
    while [ "$i" -lt 100 ]; do
      inflight=$(awk '{print $9}' "/sys/block/${_base}/stat" 2>/dev/null || echo "")
      if [ -z "$inflight" ]; then DRAIN_STATE=UNREADABLE; break; fi
      if [ "$inflight" -eq 0 ] 2>/dev/null; then DRAIN_STATE=DRAINED; break; fi
      i=$((i+1)); sleep 0.1
    done
  fi
  sync; sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'
  echo "$round,$label,$where,$DRAIN_STATE" >> "$OUT/drain.csv"
  [ "$DRAIN_STATE" = DRAINED ] || echo "  [drain] $where: $DRAIN_STATE (device busy or unreadable; this run may not be fully cold)"
}

# Set by run_phase so the caller can derive the scan-attributable fault count.
LAST_MAJF=""

run_phase() {
  local round="$1" label="$2" path="$3" phase="$4" prefetch="$5"; shift 5
  # `floor` is --setup-only: it deliberately performs no scan, so it has no passes
  # to validate. Every scanning phase is validated below.
  local tm="$OUT/${label}.round${round}.${phase}.time"
  local log="$OUT/${label}.round${round}.${phase}.json"
  if ! /usr/bin/time -f "%e %M %F %R" -o "$tm" \
      env CQLITE_PREFETCH="$prefetch" CQLITE_DISK_ACCESS_MODE=mmap \
      "$path" --corpus "$CORPUS" "$@" > "$log" 2> "$OUT/${label}.round${round}.${phase}.err"; then
    echo "cold-warm-ab: round $round arm $label phase $phase FAILED; see $OUT/${label}.round${round}.${phase}.err" >&2
    exit 4
  fi
  # A zero exit is not a measurement. Require the emitted JSON to be well-formed,
  # to report a NON-ZERO row count (the "0-rows-when-present" failure this repo
  # forbids), and to report the SAME rows/cells/table-dirs as every other scanning
  # phase in the run — two arms scanning different work would otherwise produce a
  # complete-looking A/B that compares nothing.
  #
  # `floor` is exempt BY CONSTRUCTION, not by oversight: it runs --setup-only and
  # performs no scan, so it emits no passes to validate.
  if [ "$phase" != floor ]; then
    local shape
    if ! shape=$(python3 - "$log" <<'PYEOF'
import json,sys
try:
    d=json.load(open(sys.argv[1]))
except Exception as e:
    print(f"UNPARSEABLE {e}"); sys.exit(1)
ps=d.get("passes") or []
if not ps: print("NO-PASSES"); sys.exit(1)
rows=sum(int(p.get("rows",0)) for p in ps)
cells=sum(int(p.get("cells",0)) for p in ps)
if rows<=0: print(f"ZERO-ROWS rows={rows}"); sys.exit(1)
print(f"rows={rows} cells={cells} tables={','.join(sorted(d.get('table_dirs_ingested') or []))}")
PYEOF
    ); then
      echo "cold-warm-ab: round $round arm $label phase $phase produced no usable measurement: $shape" >&2
      exit 5
    fi
    if [ -z "$EXPECTED_SHAPE" ]; then
      EXPECTED_SHAPE="$shape"
      echo "work-shape: $shape" >> "$OUT/host.txt"
    elif [ "$shape" != "$EXPECTED_SHAPE" ]; then
      echo "cold-warm-ab: round $round arm $label phase $phase scanned DIFFERENT work than the rest of this run" >&2
      echo "cold-warm-ab:   expected: $EXPECTED_SHAPE" >&2
      echo "cold-warm-ab:   observed: $shape" >&2
      echo "cold-warm-ab: an A/B over different work compares nothing." >&2
      exit 5
    fi
  fi
  local wall rss majf minf
  read -r wall rss majf minf < "$tm"
  echo "$round,$label,$phase,$wall,$rss,$majf,$minf" >> "$OUT/summary.csv"
  echo "  [$phase] wall=${wall}s max_rss=${rss}kB major_faults=$majf minor_faults=$minf"
  LAST_MAJF="$majf"
}

for round in $(seq 1 "$ROUNDS"); do
  order=("${BINARIES[@]}")
  if [ $(( round % 2 )) -eq 0 ]; then
    rev=(); for (( i=${#BINARIES[@]}-1; i>=0; i-- )); do rev+=("${BINARIES[$i]}"); done; order=("${rev[@]}")
  fi
  for entry in "${order[@]}"; do
    label="${entry%%=*}"; path="${entry#*=}"
    echo "--- round $round arm $label ---"
    # FLOOR: same binary, cold, opens the reader but does not scan, at
    # CQLITE_PREFETCH=off so it issues no read-ahead that could outlive it and
    # pre-warm the cold phase below. Its faults are the non-scan cost of starting
    # this process on a cold cache.
    drain_and_drop "$round" "$label" pre-floor
    run_phase "$round" "$label" "$path" floor off --setup-only
    local_floor="$LAST_MAJF"
    drain_and_drop "$round" "$label" pre-cold
    run_phase "$round" "$label" "$path" cold auto --passes 1
    local_cold="$LAST_MAJF"
    echo "$round,$label,$local_floor,$local_cold,$(( local_cold - local_floor ))" >> "$OUT/scan-attributable.csv"
    echo "  [scan-attributable] major_faults=$(( local_cold - local_floor )) (cold $local_cold - floor $local_floor)"
    # No drop here: the corpus is now resident, so this run is the WARM arm.
    run_phase "$round" "$label" "$path" warm auto --passes 1
  done
done

# ADVICE CENSUS — evidence that the two arms actually differ in the advice they
# issue, recorded rather than assumed. Deliberately runs AFTER every measurement:
# it executes at CQLITE_PREFETCH=auto, so on a patched-style arm it issues real
# whole-file read-ahead, and doing it earlier would pre-warm a later cold phase.
# It records what each arm DOES; it asserts nothing about what each arm SHOULD do,
# because the labels are the caller's and this harness does not know their meaning.
{
  echo "# madvise census per arm, --setup-only, CQLITE_PREFETCH=auto CQLITE_DISK_ACCESS_MODE=mmap"
  echo "# counts are SUCCESSFUL calls (returned 0); failures appear as (+N FAILED)"
  if ! command -v strace >/dev/null 2>&1; then
    echo "UNAVAILABLE: strace is not installed — the advice census was NOT taken."
    echo "UNAVAILABLE: this run carries no evidence that the arms differ in issued advice."
  else
    for entry in "${BINARIES[@]}"; do
      label="${entry%%=*}"; path="${entry#*=}"
      st="$OUT/${label}.advice.strace"
      if env CQLITE_PREFETCH=auto CQLITE_DISK_ACCESS_MODE=mmap \
           strace -f -e trace=madvise -o "$st" "$path" --corpus "$CORPUS" --setup-only >/dev/null 2>&1; then
        # Count SUCCESSES, not attempts. `madvise` failure is non-fatal to the
        # reader (it is logged and swallowed), so a run where every
        # `MADV_WILLNEED` returned -1 would have no effective prefetch at all
        # while a naive `grep -c MADV_WILLNEED` still reported 1. Counting the
        # attempt would make an ineffective run indistinguishable from a working
        # one — in the artifact whose whole job is to evidence that the arms
        # differ. Failures are counted separately and reported, never folded in.
        _adv=UNMEASURED
        if [ -r "$st" ]; then
          _adv=$(awk '
            # strace -f splits a syscall interrupted by another thread into an
            # "<unfinished ...>" line and a "<... madvise resumed>) = R" line, so
            # neither half alone carries BOTH the advice name and the return value.
            # Counting complete lines only reports every split call as a failure —
            # measured: 2 of 4 real MADV_DONTNEED calls. Pair them by pid.
            /<unfinished \.\.\.>/ && /MADV_/ {
              for (i = 1; i <= NF; i++) if ($i ~ /^MADV_/) { a = $i; gsub(/[),]/, "", a) }
              pending[$1] = a; next
            }
            /<\.\.\. madvise resumed>/ {
              a = pending[$1]
              if (a == "") { unpaired++; next }
              delete pending[$1]
              if ($0 ~ /=[ ]*0[ ]*$/) s[a]++; else f[a]++
              next
            }
            /MADV_/ {
              for (i = 1; i <= NF; i++) if ($i ~ /^MADV_/) { a = $i; gsub(/[),]/, "", a) }
              if ($0 ~ /=[ ]*0[ ]*$/) s[a]++; else f[a]++
            }
            END {
              split("MADV_WILLNEED MADV_RANDOM MADV_SEQUENTIAL MADV_DONTNEED", k, " ")
              out = ""
              for (i = 1; i <= 4; i++) {
                n = k[i]; sub(/^MADV_/, "", n)
                out = out sprintf("%s=%d", n, s[k[i]] + 0)
                if (f[k[i]] + 0 > 0) out = out sprintf("(+%d FAILED)", f[k[i]])
                out = out " "
              }
              d = 0; for (x in pending) d++
              if (unpaired + 0 > 0 || d > 0) out = out sprintf("[UNRECONCILED unpaired=%d dangling=%d]", unpaired + 0, d)
              print out
            }' "$st")
          [ -n "$_adv" ] || _adv=UNMEASURED
        fi
        printf '%s: %s\n' "$label" "$_adv"
      else
        echo "$label: CENSUS FAILED — no evidence recorded for this arm"
      fi
    done
    echo "# note: MADV_DONTNEED here is the runtime releasing thread stacks, not the reader."
  fi
} | tee "$OUT/advice-census.txt"

# Order matters: append the closing metadata while the abort trap is STILL ARMED,
# then flip the sentinel, and only then disarm. Clearing the trap first would let a
# failed append exit non-zero with host.txt already claiming COMPLETE.
{
  echo "finished-utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "loadavg-at-end: $(cut -d' ' -f1-3 /proc/loadavg)"
} | tee -a "$OUT/host.txt"
# Replace the sentinel in place — appending would leave BOTH `run:` lines and a
# reader grepping `run:` could not tell which one is current.
sed -i 's/^run: INCOMPLETE .*$/run: COMPLETE/' "$OUT/host.txt"
trap - EXIT
echo "artifacts: $OUT"
