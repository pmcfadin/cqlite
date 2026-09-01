#!/usr/bin/env bash
# Cold-vs-warm A/B for issue #2824 (madvise(WILLNEED) on the Auto-mmap scan plane).
#
# WHAT THIS MEASURES, AND WHAT IT DOES NOT
# ----------------------------------------
# Runs `ws0-scan-bench` over a fixed corpus once per labelled binary, with the
# page cache DROPPED before every run. Pass 1 of each run is the COLD arm; passes
# 2..N are WARM. Two binaries -> a baseline-vs-patched A/B.
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
# from `/usr/bin/time`), which is per-process and therefore immune to the
# neighbours. MADV_WILLNEED's whole mechanism is converting synchronous major
# faults on the reading thread into kernel-initiated async read-ahead, so a real
# effect must show up as a major-fault reduction on the cold pass. Wall clock is
# reported alongside it as corroboration, not as the primary evidence.
#
# Requires: passwordless sudo for /proc/sys/vm/drop_caches (checked, fail-closed).
set -euo pipefail

CORPUS=""; PASSES=2; ROUNDS=5; OUT=""
declare -a BINARIES=()

usage() {
  cat <<'USAGE'
usage: cold-warm-ab.sh --corpus <dir> --out <dir> --bin <label>=<path> --bin <label>=<path>
                       [--passes N] [--rounds N]

  --corpus   corpus directory (see tools/ws0-corpus-gen/README.md)
  --out      directory for the recorded artifacts (created)
  --bin      a labelled ws0-scan-bench binary; give it twice for an A/B
  --passes   scan passes per run; pass 1 is COLD, 2..N are WARM (default 2)
  --rounds   alternating rounds over the whole arm set (default 5)
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --corpus) CORPUS="${2:?--corpus needs a value}"; shift 2 ;;
    --out)    OUT="${2:?--out needs a value}"; shift 2 ;;
    --bin)    BINARIES+=("${2:?--bin needs label=path}"); shift 2 ;;
    --passes) PASSES="${2:?--passes needs a value}"; shift 2 ;;
    --rounds) ROUNDS="${2:?--rounds needs a value}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "cold-warm-ab: unrecognized argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[ -n "$CORPUS" ] || { echo "cold-warm-ab: --corpus is required" >&2; exit 2; }
[ -n "$OUT" ]    || { echo "cold-warm-ab: --out is required" >&2; exit 2; }
[ "${#BINARIES[@]}" -ge 2 ] || { echo "cold-warm-ab: an A/B needs at least two --bin arms" >&2; exit 2; }
[ -d "$CORPUS" ] || { echo "cold-warm-ab: corpus not a directory: $CORPUS" >&2; exit 2; }
[ -x /usr/bin/time ] || { echo "cold-warm-ab: /usr/bin/time is required for major-fault counts" >&2; exit 3; }

# Fail CLOSED on drop_caches: a "cold" arm that silently ran warm is worse than no
# measurement, because in the output it is indistinguishable from a real one.
if ! sudo -n sh -c 'echo 1 > /proc/sys/vm/drop_caches' 2>/dev/null; then
  echo "cold-warm-ab: cannot drop the page cache (passwordless sudo required)." >&2
  echo "cold-warm-ab: refusing to run — a warm run labelled COLD is not a measurement." >&2
  exit 3
fi

for entry in "${BINARIES[@]}"; do
  label="${entry%%=*}"; path="${entry#*=}"
  if [ "$label" = "$entry" ] || [ -z "$label" ] || [ -z "$path" ]; then
    echo "cold-warm-ab: --bin needs label=path, got: $entry" >&2; exit 2
  fi
  [ -x "$path" ] || { echo "cold-warm-ab: not executable: $path" >&2; exit 2; }
done

mkdir -p "$OUT"
INSTANCE=$(curl -s -m 3 -H "X-aws-ec2-metadata-token: $(curl -s -m 3 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token 2>/dev/null)" http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || echo UNKNOWN)

{
  echo "host: $(hostname)"
  echo "instance-type: $INSTANCE"
  case "$INSTANCE" in
    i4i.*) echo "i4i-magnitude: MEASURED (this host is an i4i)" ;;
    *)     echo "i4i-magnitude: UNMEASURED (this host is '$INSTANCE', not an i4i) — AC1's i4i clause is NOT satisfied by this run" ;;
  esac
  echo "kernel: $(uname -r)"
  echo "cores: $(nproc)"
  echo "mem-total-kb: $(awk '/MemTotal/{print $2}' /proc/meminfo)"
  echo "loadavg-at-start: $(cut -d' ' -f1-3 /proc/loadavg)"
  echo "corpus: $CORPUS"
  echo "corpus-bytes: $(du -sb "$CORPUS" | cut -f1)"
  echo "passes-per-run: $PASSES  (pass 1 = COLD, 2..$PASSES = WARM)"
  echo "rounds: $ROUNDS  (arm order alternates on even rounds)"
  echo "primary-signal: major page faults (per-process, contention-immune)"
  echo "secondary-signal: wall seconds (contended; medians only)"
  echo "started-utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  for entry in "${BINARIES[@]}"; do
    echo "arm: ${entry%%=*}  sha256=$(sha256sum "${entry#*=}" | cut -d' ' -f1)"
  done
} | tee "$OUT/host.txt"

echo "round,arm,wall_secs,max_rss_kb,major_faults,minor_faults" > "$OUT/summary.csv"

for round in $(seq 1 "$ROUNDS"); do
  # Flip arm order on even rounds so monotonic background drift cannot be
  # attributed to whichever arm happens to run first.
  order=("${BINARIES[@]}")
  if [ $(( round % 2 )) -eq 0 ]; then
    rev=(); for (( i=${#BINARIES[@]}-1; i>=0; i-- )); do rev+=("${BINARIES[$i]}"); done; order=("${rev[@]}")
  fi
  for entry in "${order[@]}"; do
    label="${entry%%=*}"; path="${entry#*=}"
    sync; sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    log="$OUT/${label}.round${round}.json"
    tm="$OUT/${label}.round${round}.time"
    echo "--- round $round arm $label (page cache dropped) ---"
    if ! /usr/bin/time -f "%e %M %F %R" -o "$tm" \
        "$path" --corpus "$CORPUS" --passes "$PASSES" > "$log" 2> "$OUT/${label}.round${round}.err"; then
      echo "cold-warm-ab: round $round arm $label FAILED; see $OUT/${label}.round${round}.err" >&2
      exit 4
    fi
    read -r wall rss majf minf < "$tm"
    echo "$round,$label,$wall,$rss,$majf,$minf" >> "$OUT/summary.csv"
    echo "  wall=${wall}s max_rss=${rss}kB major_faults=$majf minor_faults=$minf"
  done
done

{
  echo "finished-utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "loadavg-at-end: $(cut -d' ' -f1-3 /proc/loadavg)"
} | tee -a "$OUT/host.txt"
echo "artifacts: $OUT"
