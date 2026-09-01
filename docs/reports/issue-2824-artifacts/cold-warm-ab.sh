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
# Requires: passwordless sudo for /proc/sys/vm/drop_caches (checked, fail-closed).
set -euo pipefail

CORPUS=""; ROUNDS=5; OUT=""
declare -a BINARIES=()

usage() {
  cat <<'USAGE'
usage: cold-warm-ab.sh --corpus <dir> --out <dir> --bin <label>=<path> --bin <label>=<path>
                       [--rounds N]

  --corpus   corpus directory (see tools/ws0-corpus-gen/README.md)
  --out      directory for the recorded artifacts (created)
  --bin      a labelled ws0-scan-bench binary; give it twice for an A/B
  --rounds   alternating rounds over the whole arm set (default 5)
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
[ "${#BINARIES[@]}" -ge 2 ] || { echo "cold-warm-ab: an A/B needs at least two --bin arms" >&2; exit 2; }
# A round count that is not a positive integer would complete "successfully" having
# recorded nothing — a vacuous pass wearing a green exit code.
case "$ROUNDS" in
  ''|*[!0-9]*) echo "cold-warm-ab: --rounds must be a positive integer, got '$ROUNDS'" >&2; exit 2 ;;
esac
[ "$ROUNDS" -ge 1 ] || { echo "cold-warm-ab: --rounds must be at least 1, got '$ROUNDS'" >&2; exit 2; }
[ -d "$CORPUS" ] || { echo "cold-warm-ab: corpus not a directory: $CORPUS" >&2; exit 2; }
[ -x /usr/bin/time ] || { echo "cold-warm-ab: /usr/bin/time is required for major-fault counts" >&2; exit 3; }

# Fail CLOSED on drop_caches: a "cold" arm that silently ran warm is worse than no
# measurement, because in the output it is indistinguishable from a real one.
if ! sudo -n sh -c 'echo 1 > /proc/sys/vm/drop_caches' 2>/dev/null; then
  echo "cold-warm-ab: cannot drop the page cache (passwordless sudo required)." >&2
  echo "cold-warm-ab: refusing to run — a warm run labelled COLD is not a measurement." >&2
  exit 3
fi

# The label is interpolated into BOTH a filename and a CSV field, so it is
# constrained to a charset safe for both, and required unique. Unvalidated:
# a duplicate silently overwrites another arm's artifacts and makes its CSV rows
# indistinguishable, a comma corrupts the CSV, and a slash writes outside --out.
_seen_labels=""
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
done

mkdir -p "$OUT"

# Device discovery runs BEFORE anything is claimed, because the i4i verdict depends
# on it: an i4i instance whose corpus sits on EBS has NOT measured the i4i criterion.
_dev=$(findmnt -no SOURCE --target "$CORPUS" 2>/dev/null || true)
[ -n "$_dev" ] || _dev=UNKNOWN
# `lsblk -no PKNAME` EXITS 0 AND PRINTS NOTHING for a whole disk (no parent), so a
# `|| basename` fallback never fires and every whole-disk device silently recorded
# UNKNOWN — an unmeasured value taking the permissive branch. Resolve affirmatively.
_base=$(lsblk -no PKNAME "$_dev" 2>/dev/null | head -1 | tr -d ' ')
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
  [a-z0-9]*.[a-z0-9]*) : ;;
  *) INSTANCE=UNKNOWN ;;
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
  echo "rounds: $ROUNDS  (arm order alternates on even rounds)"
  echo "primary-signal: scan-attributable major page faults = cold(major_faults) - floor(major_faults)"
  echo "primary-signal-note: %F is per-process (immune to neighbours) but NOT isolated to the scan mapping; the floor phase estimates the non-scan startup cost. The subtraction is an ESTIMATE, not per-mapping accounting."
  echo "secondary-signal: wall seconds (contended; medians only)"
  echo "started-utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  for entry in "${BINARIES[@]}"; do
    echo "arm: ${entry%%=*}  sha256=$(sha256sum "${entry#*=}" | cut -d' ' -f1)"
  done
} | tee "$OUT/host.txt"

echo "round,arm,phase,wall_secs,max_rss_kb,major_faults,minor_faults" > "$OUT/summary.csv"
echo "round,arm,floor_major_faults,cold_major_faults,scan_major_faults" > "$OUT/scan-attributable.csv"

# One `/usr/bin/time` invocation per PHASE, each a single-pass run, so every
# recorded number is attributable to exactly one phase. An earlier version wrapped
# `--passes N` in ONE invocation, which summed cold+warm into a column the header
# called cold — a mislabel that would have propagated into the artifact.
# Set by run_phase so the caller can derive the scan-attributable fault count.
LAST_MAJF=""

run_phase() {
  local round="$1" label="$2" path="$3" phase="$4"; shift 4
  local tm="$OUT/${label}.round${round}.${phase}.time"
  local log="$OUT/${label}.round${round}.${phase}.json"
  if ! /usr/bin/time -f "%e %M %F %R" -o "$tm" \
      "$path" --corpus "$CORPUS" "$@" > "$log" 2> "$OUT/${label}.round${round}.${phase}.err"; then
    echo "cold-warm-ab: round $round arm $label phase $phase FAILED; see $OUT/${label}.round${round}.${phase}.err" >&2
    exit 4
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
    # FLOOR: same binary, cold, opens the reader but does not scan. Its faults are
    # the non-scan cost of starting this process on a cold cache.
    sync; sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    run_phase "$round" "$label" "$path" floor --setup-only
    local_floor="$LAST_MAJF"
    sync; sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    run_phase "$round" "$label" "$path" cold --passes 1
    local_cold="$LAST_MAJF"
    echo "$round,$label,$local_floor,$local_cold,$(( local_cold - local_floor ))" >> "$OUT/scan-attributable.csv"
    echo "  [scan-attributable] major_faults=$(( local_cold - local_floor )) (cold $local_cold - floor $local_floor)"
    # No drop here: the corpus is now resident, so this run is the WARM arm.
    run_phase "$round" "$label" "$path" warm --passes 1
  done
done

{
  echo "finished-utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "loadavg-at-end: $(cut -d' ' -f1-3 /proc/loadavg)"
} | tee -a "$OUT/host.txt"
echo "artifacts: $OUT"
